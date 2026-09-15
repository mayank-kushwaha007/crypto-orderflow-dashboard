import json
import os
import threading
import time
from collections import deque
import websocket
import pandas as pd
import numpy as np

import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# =====================================================================
# CONFIGURATION
# =====================================================================
SOCKET_URL = "wss://public-socket.india.delta.exchange" 
SYMBOL = "BTCUSD"
MAX_HISTORY = 40        # Optimized timeline length for vertical mobile viewports
REFRESH_RATE_MS = 1000  # Refresh interval (1000ms = 1 second)
MSG_LOG_LIMIT = 15      # Raw frames dumped at startup, for diagnosing the feed

class MobileTerminalEngine:
    def __init__(self):
        self.lock = threading.Lock()
        self.order_book = {"bids": {}, "asks": {}}
        
        self.prev_best_bid_price = None
        self.prev_best_bid_size = 0.0
        self.prev_best_ask_price = None
        self.prev_best_ask_size = 0.0
        self.cumulative_ofi = 0.0
        
        # Microscopic dynamic memory stacks
        self.timestamps = deque(maxlen=MAX_HISTORY)
        self.prices = deque(maxlen=MAX_HISTORY)
        self.ofi_history = deque(maxlen=MAX_HISTORY)
        self.ofi_steps = deque(maxlen=MAX_HISTORY)
        
        # Feed diagnostics
        self.msg_seen = 0
        self.type_counts = {}
        self.ws_state = "starting"

        self.opens = deque(maxlen=MAX_HISTORY)
        self.highs = deque(maxlen=MAX_HISTORY)
        self.lows = deque(maxlen=MAX_HISTORY)
        self.closes = deque(maxlen=MAX_HISTORY)

mobile_pipeline = MobileTerminalEngine()

# =====================================================================
# BACKGROUND DATA INGESTION MATRIX
# =====================================================================
def parse_level(level):
    """Delta sends a level either as ["price", "size"] or as
    {"limit_price": "...", "size": ...}. Handle both, return (price, size)."""
    if isinstance(level, dict):
        return float(level.get("limit_price", level.get("price"))), float(level.get("size", 0))
    return float(level[0]), float(level[1])


def apply_levels(side, levels):
    book = mobile_pipeline.order_book[side]
    for level in levels:
        try:
            p, s = parse_level(level)
        except (TypeError, ValueError, KeyError, IndexError):
            continue
        if s == 0: book.pop(p, None)
        else: book[p] = s


def update_metrics():
    """Recompute OFI from the current top of book. Caller must hold the lock."""
    if not (mobile_pipeline.order_book["bids"] and mobile_pipeline.order_book["asks"]):
        return

    best_bid = max(mobile_pipeline.order_book["bids"].keys())
    best_bid_sz = mobile_pipeline.order_book["bids"][best_bid]
    best_ask = min(mobile_pipeline.order_book["asks"].keys())
    best_ask_sz = mobile_pipeline.order_book["asks"][best_ask]
    
    mid_price = (best_bid + best_ask) / 2.0

    dBid = best_bid_sz if mobile_pipeline.prev_best_bid_price is None or best_bid > mobile_pipeline.prev_best_bid_price else (best_bid_sz - mobile_pipeline.prev_best_bid_size if best_bid == mobile_pipeline.prev_best_bid_price else -mobile_pipeline.prev_best_bid_size)
    dAsk = best_ask_sz if mobile_pipeline.prev_best_ask_price is None or best_ask < mobile_pipeline.prev_best_ask_price else (best_ask_sz - mobile_pipeline.prev_best_ask_size if best_ask == mobile_pipeline.prev_best_ask_price else -mobile_pipeline.prev_best_ask_size)
    
    step_ofi = dBid - dAsk
    mobile_pipeline.cumulative_ofi += step_ofi

    now = pd.Timestamp.now()
    mobile_pipeline.timestamps.append(now)
    mobile_pipeline.prices.append(mid_price)
    mobile_pipeline.ofi_history.append(mobile_pipeline.cumulative_ofi)
    mobile_pipeline.ofi_steps.append(step_ofi)

    mobile_pipeline.opens.append(mobile_pipeline.closes[-1] if mobile_pipeline.closes else mid_price)
    mobile_pipeline.highs.append(max(mid_price, mobile_pipeline.opens[-1]))
    mobile_pipeline.lows.append(min(mid_price, mobile_pipeline.opens[-1]))
    mobile_pipeline.closes.append(mid_price)

    mobile_pipeline.prev_best_bid_price = best_bid
    mobile_pipeline.prev_best_bid_size = best_bid_sz
    mobile_pipeline.prev_best_ask_price = best_ask
    mobile_pipeline.prev_best_ask_size = best_ask_sz


def on_message(ws, message):
    data = json.loads(message)
    msg_type = data.get("type")

    with mobile_pipeline.lock:
        mobile_pipeline.msg_seen += 1
        seen = mobile_pipeline.msg_seen
        mobile_pipeline.type_counts[msg_type] = mobile_pipeline.type_counts.get(msg_type, 0) + 1

    # Dump the opening frames verbatim so the real payload shape is visible in logs.
    if seen <= MSG_LOG_LIMIT:
        print(f"[WS RAW {seen}] {message[:400]}", flush=True)

    if msg_type == "l2_updates":
        with mobile_pipeline.lock:
            if data.get("action") == "snapshot":
                mobile_pipeline.order_book["bids"].clear()
                mobile_pipeline.order_book["asks"].clear()

            apply_levels("bids", data.get("bids") or [])
            apply_levels("asks", data.get("asks") or [])
            update_metrics()

    elif msg_type == "l2_orderbook":
        # Full depth snapshot; Delta names the sides buy/sell on this channel.
        with mobile_pipeline.lock:
            mobile_pipeline.order_book["bids"].clear()
            mobile_pipeline.order_book["asks"].clear()

            apply_levels("bids", data.get("buy") or data.get("bids") or [])
            apply_levels("asks", data.get("sell") or data.get("asks") or [])
            update_metrics()

    if seen % 200 == 0:
        with mobile_pipeline.lock:
            print(f"[WS STATS] seen={seen} types={mobile_pipeline.type_counts} "
                  f"bids={len(mobile_pipeline.order_book['bids'])} "
                  f"asks={len(mobile_pipeline.order_book['asks'])} "
                  f"points={len(mobile_pipeline.timestamps)}", flush=True)


def on_open(ws):
    channels = [
        {"name": "l2_updates", "symbols": [SYMBOL]},
        {"name": "l2_orderbook", "symbols": [SYMBOL]},
    ]
    ws.send(json.dumps({"type": "subscribe", "payload": {"channels": channels}}))
    mobile_pipeline.ws_state = "connected"
    print(f"[WS] connected to {SOCKET_URL}, subscribed to "
          f"{[c['name'] for c in channels]} for {SYMBOL}", flush=True)

def on_error(ws, error):
    mobile_pipeline.ws_state = f"error: {type(error).__name__}: {error}"[:90]
    print(f"[WS ERROR] {type(error).__name__}: {error}", flush=True)


def on_close(ws, status_code, msg):
    mobile_pipeline.ws_state = f"closed (code={status_code})"
    print(f"[WS CLOSED] code={status_code} msg={msg}", flush=True)


def ws_forever():
    while True:
        try:
            ws = websocket.WebSocketApp(
                SOCKET_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as exc:
            print(f"[WS LOOP ERROR] {exc}", flush=True)
        mobile_pipeline.ws_state = "reconnecting"
        print("[WS] reconnecting in 5s...", flush=True)
        time.sleep(5)


def run_ws():
    wst = threading.Thread(target=ws_forever)
    wst.daemon = True
    wst.start()

run_ws()

# =====================================================================
# DASH PRESENTATION CONTAINER SETUP
# =====================================================================
app = dash.Dash(__name__, title=f"TradingView Mobile Terminal")
server = app.server

app.layout = html.Div(
    style={"backgroundColor": "#131722", "color": "#d1d4dc", "fontFamily": "sans-serif", "padding": "5px"},
    children=[
        html.Div(
            style={"display": "flex", "justifyContent": "space-between", "borderBottom": "1px solid #2a2e39", "padding": "8px", "fontSize": "13px"},
            children=[
                html.Span(f"📊 {SYMBOL} • 1S • DELTA", style={"fontWeight": "bold", "color": "#f2f3f5"}),
                html.Div(id="mobile-ticker-feed", style={"fontWeight": "bold"})
            ]
        ),
        dcc.Graph(id="mobile-master-chart", config={"displayModeBar": False, "scrollZoom": True}),
        dcc.Interval(id="mobile-pulse-clock", interval=REFRESH_RATE_MS, n_intervals=0)
    ]
)

# =====================================================================
# RENDERING PIPELINE CONTROLLER CALLBACK
# =====================================================================
@app.callback(
    [Output("mobile-ticker-feed", "children"),
     Output("mobile-ticker-feed", "style"),
     Output("mobile-master-chart", "figure")],
    [Input("mobile-pulse-clock", "n_intervals")]
)
def refresh_mobile_view(n):
    with mobile_pipeline.lock:
        bids = dict(mobile_pipeline.order_book["bids"])
        asks = dict(mobile_pipeline.order_book["asks"])
        current_ofi = mobile_pipeline.cumulative_ofi
        
        times = list(mobile_pipeline.timestamps)
        op, hi, lo, cl = list(mobile_pipeline.opens), list(mobile_pipeline.highs), list(mobile_pipeline.lows), list(mobile_pipeline.closes)
        ofi_steps_list = list(mobile_pipeline.ofi_steps)

        ws_state = mobile_pipeline.ws_state
        frames = mobile_pipeline.msg_seen
        type_counts = dict(mobile_pipeline.type_counts)

    if not times:
        seen = ", ".join(f"{k}x{v}" for k, v in sorted(type_counts.items(), key=lambda kv: -kv[1]))
        diag = f"WS {ws_state} | frames {frames} | book {len(bids)}/{len(asks)}"
        if seen: diag += f" | {seen}"
        return diag, {"color": "#db8c02", "fontSize": "11px"}, go.Figure().update_layout(template="plotly_dark")

    last_price = cl[-1]
    ticker_color = "#089981" if ofi_steps_list[-1] >= 0 else "#f23645"
    ticker_text = f"P: ${last_price:,.1f} | OFI: {current_ofi:+,.0f}"

    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, 
        vertical_spacing=0.03, row_heights=[0.80, 0.20]
    )

    fig.add_trace(go.Candlestick(
        x=times, open=op, high=hi, low=lo, close=cl,
        increasing_line_color='#089981', decreasing_line_color='#f23645',
        increasing_fillcolor='#089981', decreasing_fillcolor='#f23645',
        name="Price"
    ), row=1, col=1)

    if bids and asks:
        sorted_bids = sorted(bids.items(), key=lambda x: x[0], reverse=True)[:8]
        sorted_asks = sorted(asks.items(), key=lambda x: x[0])[:8]
        
        max_size = max([s for p, s in sorted_bids + sorted_asks] + [1.0])
        
        for price, size in sorted_bids:
            scaled_marker = int((size / max_size) * 28) + 6
            fig.add_trace(go.Scatter(
                x=[times[-1]], y=[price], mode="markers",
                marker=dict(size=scaled_marker, color="#089981", opacity=0.35, symbol="square"),
                showlegend=False, hoverinfo="skip"
            ), row=1, col=1)

        for price, size in sorted_asks:
            scaled_marker = int((size / max_size) * 28) + 6
            fig.add_trace(go.Scatter(
                x=[times[-1]], y=[price], mode="markers",
                marker=dict(size=scaled_marker, color="#f23645", opacity=0.35, symbol="square"),
                showlegend=False, hoverinfo="skip"
            ), row=1, col=1)

    colors = ['#089981' if val >= 0 else '#f23645' for val in ofi_steps_list]
    fig.add_trace(go.Bar(
        x=times, y=ofi_steps_list,
        marker_color=colors, name="OFI Tracker"
    ), row=2, col=1)

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="#131722",
        plot_bgcolor="#131722",
        xaxis_rangeslider_visible=False,
        height=540,  
        margin=dict(l=8, r=40, t=5, b=5), 
        showlegend=False,
        uirevision='constant' 
    )

    fig.update_xaxes(showgrid=True, gridcolor="#2a2e39", showticklabels=False, row=1, col=1)
    fig.update_xaxes(showgrid=True, gridcolor="#2a2e39", tickfont=dict(size=10), row=2, col=1)
    
    fig.update_yaxes(
        showgrid=True, gridcolor="#2a2e39", 
        side="right", tickfont=dict(size=10), 
        autorange=True, row=1, col=1
    )
    fig.update_yaxes(
        showgrid=True, gridcolor="#2a2e39", 
        side="right", tickfont=dict(size=8), 
        autorange=True, row=2, col=1
    )

    return ticker_text, {"color": ticker_color}, fig

# =====================================================================
# CLOUD PRODUCTION SERVICE DEPLOYMENT RUN ENGINE
# =====================================================================
if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=int(os.environ.get("PORT", 8050)))