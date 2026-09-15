import json
import os
import urllib.request
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
# Tried in order. A connection that yields no book data is abandoned for the
# next one, so a wrong endpoint self-corrects instead of sitting there connected.
SOCKET_URLS = [
    "wss://socket.india.delta.exchange",
    "wss://socket.delta.exchange",
    "wss://public-socket.india.delta.exchange",
]
STALL_SECONDS = 25      # No book data this long after connecting -> try the next URL

# REST fallback. The websocket needs a channel subscription the venue can refuse;
# this endpoint needs none, so it keeps the chart alive when the socket will not.
REST_URL = "https://api.india.delta.exchange/v2/l2orderbook/{symbol}?depth=20"
REST_AFTER = 10         # Seconds without websocket book data before polling REST
REST_INTERVAL = 1.0
SYMBOL = "BTCUSD"
MAX_HISTORY = 40        # Optimized timeline length for vertical mobile viewports
REFRESH_RATE_MS = 1000  # Refresh interval (1000ms = 1 second)
BUCKET = "1s"           # Candles aggregate every update within one wall-clock second
STALE_AFTER = 5         # Seconds without a book update before the ticker says so

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
        
        # Candle currently being accumulated for the present second
        self.cur_sec = None
        self.cur_open = None
        self.cur_high = None
        self.cur_low = None
        self.cur_close = None
        self.cur_ofi = 0.0

        self.ws_state = "starting"
        self.url_index = 0
        self.connected_at = 0.0
        self.data_seen = False
        self.last_ws_data = 0.0
        self.last_update = 0.0          # wall clock of the last book update, any source
        self.ws = None

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


def flush_bucket():
    """Close the accumulating second and push it onto the history. Lock held."""
    p = mobile_pipeline
    if p.cur_sec is None or p.cur_close is None:
        return
    p.timestamps.append(p.cur_sec)
    p.opens.append(p.cur_open)
    p.highs.append(p.cur_high)
    p.lows.append(p.cur_low)
    p.closes.append(p.cur_close)
    p.prices.append(p.cur_close)
    p.ofi_steps.append(p.cur_ofi)
    p.ofi_history.append(p.cumulative_ofi)


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

    # Fold this update into the current second rather than emitting a point per
    # message: the feed bursts many updates per second, which collapses the
    # timeline to milliseconds and makes every candle degenerate.
    sec = pd.Timestamp.now().floor(BUCKET)
    p = mobile_pipeline

    if p.cur_sec is None:
        p.cur_sec = sec
        p.cur_open = p.closes[-1] if p.closes else mid_price
        p.cur_high = p.cur_low = mid_price
        p.cur_ofi = 0.0
    elif sec != p.cur_sec:
        flush_bucket()
        p.cur_sec = sec
        p.cur_open = p.cur_close
        p.cur_high = p.cur_low = mid_price
        p.cur_ofi = 0.0

    p.cur_high = max(p.cur_high, mid_price)
    p.cur_low = min(p.cur_low, mid_price)
    p.cur_close = mid_price
    p.cur_ofi += step_ofi
    p.last_update = time.time()

    mobile_pipeline.prev_best_bid_price = best_bid
    mobile_pipeline.prev_best_bid_size = best_bid_sz
    mobile_pipeline.prev_best_ask_price = best_ask
    mobile_pipeline.prev_best_ask_size = best_ask_sz


def on_message(ws, message):
    data = json.loads(message)
    msg_type = data.get("type")

    if msg_type == "l2_updates":
        with mobile_pipeline.lock:
            if data.get("action") == "snapshot":
                mobile_pipeline.order_book["bids"].clear()
                mobile_pipeline.order_book["asks"].clear()

            apply_levels("bids", data.get("bids") or [])
            apply_levels("asks", data.get("asks") or [])
            update_metrics()
            mobile_pipeline.data_seen = True
            mobile_pipeline.last_ws_data = time.time()

    elif msg_type == "l2_orderbook":
        # Full depth snapshot; Delta names the sides buy/sell on this channel.
        with mobile_pipeline.lock:
            mobile_pipeline.order_book["bids"].clear()
            mobile_pipeline.order_book["asks"].clear()

            apply_levels("bids", data.get("buy") or data.get("bids") or [])
            apply_levels("asks", data.get("sell") or data.get("asks") or [])
            update_metrics()
            mobile_pipeline.data_seen = True
            mobile_pipeline.last_ws_data = time.time()


def on_open(ws):
    # Sent as separate frames: if the venue rejects one channel name, the
    # other still gets through rather than the whole subscribe failing.
    for name in ("l2_updates", "l2_orderbook"):
        payload = {"type": "subscribe", "payload": {"channels": [{"name": name, "symbols": [SYMBOL]}]}}
        ws.send(json.dumps(payload))
        print(f"[WS] sent subscribe for {name}:{SYMBOL}", flush=True)
    mobile_pipeline.ws_state = "connected"
    print(f"[WS] connected to {SOCKET_URLS[mobile_pipeline.url_index]}", flush=True)

def on_error(ws, error):
    mobile_pipeline.ws_state = f"error: {type(error).__name__}: {error}"[:90]
    print(f"[WS ERROR] {type(error).__name__}: {error}", flush=True)


def on_close(ws, status_code, msg):
    mobile_pipeline.ws_state = f"closed (code={status_code})"
    print(f"[WS CLOSED] code={status_code} msg={msg}", flush=True)


def poll_rest():
    """Poll the REST order book whenever the websocket is not delivering."""
    url = REST_URL.format(symbol=SYMBOL)
    while True:
        time.sleep(REST_INTERVAL)
        if time.time() - mobile_pipeline.last_ws_data < REST_AFTER:
            continue                      # socket is healthy, leave it alone
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                payload = json.loads(resp.read().decode())
        except Exception as exc:
            print(f"[REST ERROR] {type(exc).__name__}: {exc}", flush=True)
            continue

        result = payload.get("result") or {}
        with mobile_pipeline.lock:
            mobile_pipeline.order_book["bids"].clear()
            mobile_pipeline.order_book["asks"].clear()
            apply_levels("bids", result.get("buy") or [])
            apply_levels("asks", result.get("sell") or [])
            update_metrics()


def watchdog():
    """Abandon an endpoint that connects but never delivers book data."""
    while True:
        time.sleep(5)
        stalled = (not mobile_pipeline.data_seen
                   and mobile_pipeline.ws_state == "connected"
                   and time.time() - mobile_pipeline.connected_at > STALL_SECONDS)
        if stalled and mobile_pipeline.ws is not None:
            print(f"[WS] no data after {STALL_SECONDS}s, trying next endpoint", flush=True)
            try: mobile_pipeline.ws.close()
            except Exception: pass


def ws_forever():
    while True:
        url = SOCKET_URLS[mobile_pipeline.url_index]
        try:
            ws = websocket.WebSocketApp(
                url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            mobile_pipeline.ws = ws
            mobile_pipeline.connected_at = time.time()
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as exc:
            print(f"[WS LOOP ERROR] {exc}", flush=True)

        # Nothing usable came from this endpoint; move to the next one.
        if not mobile_pipeline.data_seen:
            mobile_pipeline.url_index = (mobile_pipeline.url_index + 1) % len(SOCKET_URLS)
        mobile_pipeline.ws_state = "reconnecting"
        print("[WS] reconnecting in 5s...", flush=True)
        time.sleep(5)


def run_ws():
    for target in (ws_forever, watchdog, poll_rest):
        t = threading.Thread(target=target)
        t.daemon = True
        t.start()

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

        # Include the second still being accumulated, so the newest candle grows
        # live instead of appearing only once the second has closed.
        if mobile_pipeline.cur_sec is not None and mobile_pipeline.cur_close is not None:
            times.append(mobile_pipeline.cur_sec)
            op.append(mobile_pipeline.cur_open)
            hi.append(mobile_pipeline.cur_high)
            lo.append(mobile_pipeline.cur_low)
            cl.append(mobile_pipeline.cur_close)
            ofi_steps_list.append(mobile_pipeline.cur_ofi)

        ws_state = mobile_pipeline.ws_state
        age = time.time() - mobile_pipeline.last_update if mobile_pipeline.last_update else None

    if not times:
        return (f"BUFFERING · {ws_state}", {"color": "#db8c02"},
                go.Figure().update_layout(template="plotly_dark"))

    last_price = cl[-1]
    ticker_color = "#089981" if ofi_steps_list[-1] >= 0 else "#f23645"
    ticker_text = f"P: ${last_price:,.1f} | OFI: {current_ofi:+,.0f}"

    # Without this a dead feed is indistinguishable from a quiet market: the
    # page keeps redrawing the same last candle and looks alive.
    if age is not None and age > STALE_AFTER:
        ticker_text += f" | STALE {age:,.0f}s"
        ticker_color = "#db8c02"

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

    pad = pd.Timedelta(seconds=1)
    xr = [times[0] - pad, times[-1] + pad]
    fig.update_xaxes(showgrid=True, gridcolor="#2a2e39", showticklabels=False,
                     range=xr, row=1, col=1)
    fig.update_xaxes(showgrid=True, gridcolor="#2a2e39", tickfont=dict(size=10),
                     range=xr, row=2, col=1)
    
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