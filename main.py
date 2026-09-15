import json
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
# FIXED: Using the required public channel endpoint for L2 order book data
SOCKET_URL = "wss://public-socket.india.delta.exchange"
SYMBOL = "BTCUSD"
MAX_HISTORY = 60       
REFRESH_RATE_MS = 1000  

class TradingViewEngine:
    def __init__(self):
        self.lock = threading.Lock()
        self.order_book = {"bids": {}, "asks": {}}
        
        self.prev_best_bid_price = None
        self.prev_best_bid_size = 0.0
        self.prev_best_ask_price = None
        self.prev_best_ask_size = 0.0
        self.cumulative_ofi = 0.0
        
        # FIXED: Pre-seeding history arrays with placeholder data so the chart framework doesn't crash on boot
        now = pd.Timestamp.now()
        self.timestamps = deque([now - pd.Timedelta(seconds=i) for i in range(MAX_HISTORY)][::-1], maxlen=MAX_HISTORY)
        self.prices = deque([90000.0] * MAX_HISTORY, maxlen=MAX_HISTORY)
        self.ofi_history = deque([0.0] * MAX_HISTORY, maxlen=MAX_HISTORY)
        self.ofi_steps = deque([0.0] * MAX_HISTORY, maxlen=MAX_HISTORY)
        
        self.opens = deque([90000.0] * MAX_HISTORY, maxlen=MAX_HISTORY)
        self.highs = deque([90000.0] * MAX_HISTORY, maxlen=MAX_HISTORY)
        self.lows = deque([90000.0] * MAX_HISTORY, maxlen=MAX_HISTORY)
        self.closes = deque([90000.0] * MAX_HISTORY, maxlen=MAX_HISTORY)

engine_pipeline = TradingViewEngine()

# =====================================================================
# DATA EXTRACTION & ANALYSIS DAEMON
# =====================================================================
def on_message(ws, message):
    data = json.loads(message)
    if "type" in data and data["type"] == "l2_updates" and "bids" in data:
        with engine_pipeline.lock:
            for bid in data.get("bids", []):
                p, s = float(bid[0]), float(bid[1])
                if s == 0: engine_pipeline.order_book["bids"].pop(p, None)
                else: engine_pipeline.order_book["bids"][p] = s

            for ask in data.get("asks", []):
                p, s = float(ask[0]), float(ask[1])
                if s == 0: engine_pipeline.order_book["asks"].pop(p, None)
                else: engine_pipeline.order_book["asks"][p] = s

            if engine_pipeline.order_book["bids"] and engine_pipeline.order_book["asks"]:
                best_bid = max(engine_pipeline.order_book["bids"].keys())
                best_bid_sz = engine_pipeline.order_book["bids"][best_bid]
                best_ask = min(engine_pipeline.order_book["asks"].keys())
                best_ask_sz = engine_pipeline.order_book["asks"][best_ask]
                
                mid_price = (best_bid + best_ask) / 2.0

                dBid = best_bid_sz if engine_pipeline.prev_best_bid_price is None or best_bid > engine_pipeline.prev_best_bid_price else (best_bid_sz - engine_pipeline.prev_best_bid_size if best_bid == engine_pipeline.prev_best_bid_price else -engine_pipeline.prev_best_bid_size)
                dAsk = best_ask_sz if engine_pipeline.prev_best_ask_price is None or best_ask < engine_pipeline.prev_best_ask_price else (best_ask_sz - engine_pipeline.prev_best_ask_size if best_ask == engine_pipeline.prev_best_ask_price else -engine_pipeline.prev_best_ask_size)
                
                step_ofi = dBid - dAsk
                engine_pipeline.cumulative_ofi += step_ofi

                now = pd.Timestamp.now()
                engine_pipeline.timestamps.append(now)
                engine_pipeline.prices.append(mid_price)
                engine_pipeline.ofi_history.append(engine_pipeline.cumulative_ofi)
                engine_pipeline.ofi_steps.append(step_ofi)

                # Generate candles organically onto layout arrays
                engine_pipeline.opens.append(engine_pipeline.closes[-1] if engine_pipeline.closes else mid_price)
                engine_pipeline.highs.append(max(mid_price, engine_pipeline.opens[-1]))
                engine_pipeline.lows.append(min(mid_price, engine_pipeline.opens[-1]))
                engine_pipeline.closes.append(mid_price)

                engine_pipeline.prev_best_bid_price = best_bid
                engine_pipeline.prev_best_bid_size = best_bid_sz
                engine_pipeline.prev_best_ask_price = best_ask
                engine_pipeline.prev_best_ask_size = best_ask_sz

def on_open(ws):
    subscribe_payload = {"type": "subscribe", "payload": {"channels": [{"name": "l2_updates", "symbols": [SYMBOL]}]}}
    ws.send(json.dumps(subscribe_payload))

def start_ws():
    ws = websocket.WebSocketApp(SOCKET_URL, on_open=on_open, on_message=on_message)
    wst = threading.Thread(target=ws.run_forever)
    wst.daemon = True
    wst.start()

start_ws()

# =====================================================================
# TRADINGVIEW UI DESIGN INTERFACE
# =====================================================================
app = dash.Dash(__name__, title=f"TradingView Advanced Orderflow Suite")
server = app.server

app.layout = html.Div(
    style={"backgroundColor": "#131722", "color": "#d1d4dc", "fontFamily": "Trebuchet MS, sans-serif", "padding": "15px"},
    children=[
        html.Div(
            style={"display": "flex", "justifyContent": "space-between", "borderBottom": "1px solid #2a2e39", "paddingBottom": "10px", "marginBottom": "15px"},
            children=[
                html.Span(f"📊 {SYMBOL} PERPETUAL FUTURE • 1S • DELTA EXCHANGE", style={"fontSize": "16px", "fontWeight": "bold", "color": "#f2f3f5"}),
                html.Div(id="tv-ticker-bar", style={"fontWeight": "bold"})
            ]
        ),
        dcc.Graph(id="tradingview-master-chart", config={"displayModeBar": True, "scrollZoom": True}),
        dcc.Interval(id="tv-pulse", interval=REFRESH_RATE_MS, n_intervals=0)
    ]
)

# =====================================================================
# REALTIME RENDERING CALLBACK
# =====================================================================
@app.callback(
    [Output("tv-ticker-bar", "children"),
     Output("tv-ticker-bar", "style"),
     Output("tradingview-master-chart", "figure")],
    [Input("tv-pulse", "n_intervals")]
)
def refresh_tradingview_terminal(n):
    with engine_pipeline.lock:
        bids = dict(engine_pipeline.order_book["bids"])
        asks = dict(engine_pipeline.order_book["asks"])
        current_ofi = engine_pipeline.cumulative_ofi
        
        times = list(engine_pipeline.timestamps)
        op, hi, lo, cl = list(engine_pipeline.opens), list(engine_pipeline.highs), list(engine_pipeline.lows), list(engine_pipeline.closes)
        ofi_steps_list = list(engine_pipeline.ofi_steps)

    last_p = cl[-1]
    ticker_color = "#089981" if ofi_steps_list[-1] >= 0 else "#f23645"
    ticker_text = f"LAST: ${last_p:,.1f} | CUMULATIVE OFI: {current_ofi:+,.1f}"

    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, 
        vertical_spacing=0.05, row_heights=[0.75, 0.25]
    )

    fig.add_trace(go.Candlestick(
        x=times, open=op, high=hi, low=lo, close=cl,
        increasing_line_color='#089981', decreasing_line_color='#f23645',
        increasing_fillcolor='#089981', decreasing_fillcolor='#f23645',
        name="Price Action"
    ), row=1, col=1)

    if bids and asks:
        sorted_bids = sorted(bids.items(), key=lambda x: x, reverse=True)[:10]
        sorted_asks = sorted(asks.items(), key=lambda x: x)[:10]
        
        for price, size in sorted_bids:
            fig.add_trace(go.Scatter(
                x=[times[-1]], y=[price], mode="markers",
                marker=dict(size=min(size * 4, 35), color="#089981", opacity=0.35, symbol="square"),
                hoverinfo="text", hovertext=f"BUY CLUSTER<br>Price: ${price}<br>Vol: {size}",
                showlegend=False
            ), row=1, col=1)

        for price, size in sorted_asks:
            fig.add_trace(go.Scatter(
                x=[times[-1]], y=[price], mode="markers",
                marker=dict(size=min(size * 4, 35), color="#f23645", opacity=0.35, symbol="square"),
                hoverinfo="text", hovertext=f"SELL CLUSTER<br>Price: ${price}<br>Vol: {size}",
                showlegend=False
            ), row=1, col=1)

    colors = ['#089981' if val >= 0 else '#f23645' for val in ofi_steps_list]
    fig.add_trace(go.Bar(
        x=times, y=ofi_steps_list,
        marker_color=colors, name="OFI Volume"
    ), row=2, col=1)

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="#131722",
        plot_bgcolor="#131722",
        xaxis_rangeslider_visible=False,
        height=650,
        margin=dict(l=50, r=20, t=10, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )

    fig.update_xaxes(showgrid=True, gridcolor="#2a2e39", row=1, col=1)
    fig.update_xaxes(showgrid=True, gridcolor="#2a2e39", row=2, col=1)
    fig.update_yaxes(showgrid=True, gridcolor="#2a2e39", title_text="Price ($)", row=1, col=1)
    fig.update_yaxes(showgrid=True, gridcolor="#2a2e39", title_text="OFI Imbalance", row=2, col=1)

    return ticker_text, {"color": ticker_color}, fig

if __name__ == "__main__":
    app.run_server(debug=False, host="0.0.0.0", port=8050)
