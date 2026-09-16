import json
import os
import urllib.request
import threading
import time
from collections import deque
import websocket
import pandas as pd
import numpy as np

import storage

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
TICKER_URL = "https://api.india.delta.exchange/v2/tickers/{symbol}"
REST_INTERVAL = 0.5     # REST is the primary source, polled continuously
REST_MAX_BACKOFF = 8.0  # Failures back off to here, then recover on success

# Render kills a free instance after ~15 minutes without INBOUND traffic.
# Outbound calls do not count, so the service requests its own public URL.
_BASE_URL = os.environ.get("RENDER_EXTERNAL_URL") or os.environ.get("KEEPALIVE_URL", "")
KEEPALIVE_URL = (_BASE_URL.rstrip("/") + "/health") if _BASE_URL else ""
KEEPALIVE_EVERY = 600   # 10 minutes, comfortably inside the 15 minute window

# The layout is rendered per request, so reloading the page is a real refresh.
# This drives live updates where the in-place callback is not reaching the
# browser. Set to 0 to disable it and rely on the callback alone.
AUTO_REFRESH_SECONDS = int(os.environ.get("AUTO_REFRESH_SECONDS", "5"))

# Persistence. Unset DATABASE_URL and everything below degrades to the previous
# in-memory-only behaviour rather than failing.
DATABASE_URL = os.environ.get("DATABASE_URL", "")
DOM_ROWS = 10           # Depth levels shown in the bid/ask table
SYMBOL = "BTCUSD"
MAX_HISTORY = 40        # Optimized timeline length for vertical mobile viewports
REFRESH_RATE_MS = 500   # Browser redraw interval, so the open candle moves live
BUCKET = "1s"           # Candles aggregate every update within one wall-clock second
# Candles are kept in UTC and converted for display only, so what is stored stays
# unambiguous while the axis reads in the viewer's own time.
DISPLAY_TZ = os.environ.get("DISPLAY_TZ", "Asia/Kolkata")
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
        self.session_day = None         # UTC day the cumulative total belongs to
        
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
        self.last_ws_data = 0.0
        self.conn_started = 0.0
        self.rest_error = ""
        self.ltp_error = ""
        self.callbacks = 0              # renders served, to tell client from server
        self.last_callback = 0.0
        self.render_error = ""
        self.ltp = None                 # last traded price, from the ticker endpoint
        self.prev_ltp = None
        self.last_update = 0.0          # wall clock of the last book update, any source
        self.ws = None

        self.opens = deque(maxlen=MAX_HISTORY)
        self.highs = deque(maxlen=MAX_HISTORY)
        self.lows = deque(maxlen=MAX_HISTORY)
        self.closes = deque(maxlen=MAX_HISTORY)

mobile_pipeline = MobileTerminalEngine()
store = storage.Storage(DATABASE_URL, SYMBOL)

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

    store.record(p.cur_sec.to_pydatetime(), p.cur_open, p.cur_high, p.cur_low,
                 p.cur_close, p.cur_ofi, p.cumulative_ofi)


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

    # Cumulative OFI is anchored to the UTC day. Measured from process start it
    # restarts at an invisible moment and is not comparable between days; a
    # session total is. The open second is closed against the old day's running
    # figure before the reset, so no bar records a total from the wrong session.
    day = pd.Timestamp.now(tz="UTC").floor("D")
    p = mobile_pipeline
    if p.session_day is not None and day != p.session_day:
        flush_bucket()
        p.cur_sec = None
        p.cumulative_ofi = 0.0
        print(f"[OFI] new UTC session {day.date()}, cumulative reset to 0", flush=True)
    p.session_day = day

    p.cumulative_ofi += step_ofi

    # Fold this update into the current second rather than emitting a point per
    # message: the feed bursts many updates per second, which collapses the
    # timeline to milliseconds and makes every candle degenerate.
    sec = pd.Timestamp.now(tz="UTC").floor(BUCKET)

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
            mobile_pipeline.last_ws_data = time.time()

    elif msg_type == "l2_orderbook":
        # Full depth snapshot; Delta names the sides buy/sell on this channel.
        with mobile_pipeline.lock:
            mobile_pipeline.order_book["bids"].clear()
            mobile_pipeline.order_book["asks"].clear()

            apply_levels("bids", data.get("buy") or data.get("bids") or [])
            apply_levels("asks", data.get("sell") or data.get("asks") or [])
            update_metrics()
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


def fetch_ltp(headers):
    """Last traded price. Independent of the book, so it survives book failures."""
    try:
        req = urllib.request.Request(TICKER_URL.format(symbol=SYMBOL), headers=headers)
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = (json.loads(resp.read().decode()).get("result") or {})
        for key in ("close", "last_price", "mark_price", "spot_price"):
            if result.get(key) not in (None, ""):
                ltp = float(result[key])
                mobile_pipeline.ltp_error = ""
                if ltp != mobile_pipeline.ltp:
                    mobile_pipeline.prev_ltp = mobile_pipeline.ltp
                mobile_pipeline.ltp = ltp
                return
    except Exception as exc:
        mobile_pipeline.ltp_error = f"{type(exc).__name__}: {exc}"[:70]
        print(f"[LTP ERROR] {type(exc).__name__}: {exc}", flush=True)


def poll_rest():
    """Poll the REST order book whenever the websocket is not delivering."""
    url = REST_URL.format(symbol=SYMBOL)
    headers = {"Accept": "application/json", "User-Agent": "orderflow-dashboard/1.0"}
    delay = REST_INTERVAL
    while True:
        # Everything is inside the guard: this is the last line of defence, and a
        # thread that dies here takes the fallback down for the process lifetime.
        try:
            time.sleep(delay)
            fetch_ltp(headers)

            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=10) as resp:
                payload = json.loads(resp.read().decode())

            result = payload.get("result") or {}
            with mobile_pipeline.lock:
                mobile_pipeline.order_book["bids"].clear()
                mobile_pipeline.order_book["asks"].clear()
                apply_levels("bids", result.get("buy") or [])
                apply_levels("asks", result.get("sell") or [])
                update_metrics()
            mobile_pipeline.rest_error = ""
            delay = REST_INTERVAL
        except Exception as exc:
            delay = min(delay * 2, REST_MAX_BACKOFF)
            mobile_pipeline.rest_error = f"{type(exc).__name__}: {exc}"[:70]
            print(f"[REST ERROR] retry in {delay:.1f}s: {type(exc).__name__}: {exc}", flush=True)


def watchdog():
    """Abandon an endpoint that stops delivering, whether or not it ever did."""
    while True:
        try:
            time.sleep(5)
            last = max(mobile_pipeline.last_ws_data, mobile_pipeline.connected_at)
            stalled = (mobile_pipeline.ws_state == "connected"
                       and time.time() - last > STALL_SECONDS)
            if stalled and mobile_pipeline.ws is not None:
                print(f"[WS] silent for {STALL_SECONDS}s, dropping the connection", flush=True)
                try: mobile_pipeline.ws.close()
                except Exception: pass
        except Exception as exc:
            print(f"[WATCHDOG ERROR] {exc}", flush=True)


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
            mobile_pipeline.conn_started = time.time()
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as exc:
            print(f"[WS LOOP ERROR] {exc}", flush=True)

        # Judge this connection alone: an endpoint that delivered before but
        # sent nothing this time must still be rotated away from.
        if mobile_pipeline.last_ws_data < mobile_pipeline.conn_started:
            mobile_pipeline.url_index = (mobile_pipeline.url_index + 1) % len(SOCKET_URLS)
        mobile_pipeline.ws_state = "reconnecting"
        print("[WS] reconnecting in 5s...", flush=True)
        time.sleep(5)


def keepalive():
    """Request our own public URL so Render sees inbound traffic and stays up.

    Only inbound requests count towards Render's idle timer, so calls out to the
    exchange do not help. This cannot wake an instance that has already been put
    to sleep -- nothing running inside it is left to make the call -- so it keeps
    a live instance alive rather than resurrecting a dead one.
    """
    if not KEEPALIVE_URL:
        _retired.add("keepalive")       # deliberate exit, not a crash to restart
        print("[KEEPALIVE] no RENDER_EXTERNAL_URL or KEEPALIVE_URL set; disabled", flush=True)
        return
    headers = {"User-Agent": "orderflow-dashboard-keepalive/1.0"}
    while True:
        time.sleep(KEEPALIVE_EVERY)
        try:
            req = urllib.request.Request(KEEPALIVE_URL, headers=headers)
            with urllib.request.urlopen(req, timeout=20) as resp:
                print(f"[KEEPALIVE] {KEEPALIVE_URL} -> {resp.status}", flush=True)
        except Exception as exc:
            print(f"[KEEPALIVE ERROR] {type(exc).__name__}: {exc}", flush=True)


WORKERS = (("ws", ws_forever), ("watchdog", watchdog), ("rest", poll_rest),
           ("keepalive", keepalive))
_threads = {}
_retired = set()        # workers that finished on purpose and must not be respawned
_threads_lock = threading.Lock()


def restore_history():
    """Reload the last candles so a restart resumes rather than starts over."""
    rows = store.load_recent(MAX_HISTORY)
    if not rows:
        return
    p = mobile_pipeline
    with p.lock:
        if p.timestamps:                 # a live process already has better data
            return
        for ts, o, h, l, c, step, cum in rows:
            p.timestamps.append(pd.Timestamp(ts).tz_convert("UTC"))
            p.opens.append(o); p.highs.append(h); p.lows.append(l); p.closes.append(c)
            p.prices.append(c)
            p.ofi_steps.append(step)
            p.ofi_history.append(cum)
        # Resume the total only within the same UTC day; across a boundary the
        # session has ended and today starts from zero.
        today = pd.Timestamp.now(tz="UTC").floor("D")
        last_day = pd.Timestamp(rows[-1][0]).tz_convert("UTC").floor("D")
        p.session_day = today
        p.cumulative_ofi = rows[-1][6] if last_day == today else 0.0
    print(f"[DB] restored {len(rows)} candles, cumulative OFI resumes at "
          f"{p.cumulative_ofi:+,.0f} for UTC {p.session_day.date()}", flush=True)


def ensure_workers():
    """Start the feed threads in THIS process, and restart any that have died.

    Threads do not survive fork(). Under `gunicorn --preload` the module is
    imported once in the master and the workers are forked from it, so threads
    started at import exist only in the master: every worker then serves the
    snapshot captured at fork time and never updates again. Calling this from
    the callback as well as at import means whichever process answers requests
    is always the one running the feed.
    """
    if store.enabled and not store._started:
        store.start()
        # Off the request path: restore_history() connects and queries, and a slow
        # or unreachable database would otherwise stall every callback behind it.
        threading.Thread(target=restore_history, name="restore", daemon=True).start()

    with _threads_lock:
        for name, target in WORKERS:
            if name in _retired:
                continue
            t = _threads.get(name)
            if t is None or not t.is_alive():
                t = threading.Thread(target=target, name=name, daemon=True)
                t.start()
                _threads[name] = t
                print(f"[THREADS] started {name} in pid {os.getpid()}", flush=True)


ensure_workers()

# =====================================================================
# DASH PRESENTATION CONTAINER SETUP
# =====================================================================
CELL = {"padding": "3px 10px", "fontVariantNumeric": "tabular-nums",
        "fontFamily": "ui-monospace, Menlo, monospace", "fontSize": "12px"}
HEAD = dict(CELL, color="#787b86", fontSize="10px", letterSpacing="0.06em",
            borderBottom="1px solid #2a2e39", textAlign="right")


def waiting_figure(ws_state, rest_error, ltp_error):
    """Hold the page's shape and say why it is empty, rather than collapsing."""
    live = sum(1 for t in _threads.values() if t.is_alive())
    lines = [f"waiting for market data  ·  pid {os.getpid()}  ·  {live}/{len(WORKERS)} feed threads",
             f"websocket: {ws_state}"]
    lines.append(f"order book: {rest_error}" if rest_error else "order book: polling")
    lines.append(f"last price: {ltp_error}" if ltp_error else "last price: polling")

    fig = go.Figure()
    fig.add_annotation(text="<br>".join(lines), showarrow=False, xref="paper", yref="paper",
                       x=0.5, y=0.5, align="left",
                       font=dict(size=12, color="#787b86", family="ui-monospace, monospace"))
    fig.update_layout(template="plotly_dark", paper_bgcolor="#131722", plot_bgcolor="#131722",
                      height=540, margin=dict(l=8, r=40, t=5, b=5), showlegend=False)
    fig.update_xaxes(visible=False)
    fig.update_yaxes(visible=False)
    return fig


def format_ltp(ltp, prev_ltp):
    """Big traded price, tinted and signed against the previous print."""
    base = {"fontSize": "28px", "fontWeight": "bold", "fontVariantNumeric": "tabular-nums"}
    if ltp is None:
        return "—", dict(base, color="#787b86"), ""
    if prev_ltp is None or ltp == prev_ltp:
        return f"${ltp:,.1f}", dict(base, color="#d1d4dc"), ""
    up = ltp > prev_ltp
    diff = ltp - prev_ltp
    colour = "#089981" if up else "#f23645"
    return (f"${ltp:,.1f}", dict(base, color=colour),
            html.Span(f"{'▲' if up else '▼'} {abs(diff):,.1f}", style={"color": colour}))


def dom_table(bids, asks):
    """Bid and ask ladders side by side, deepest liquidity shaded strongest."""
    top_bids = sorted(bids.items(), key=lambda x: x[0], reverse=True)[:DOM_ROWS]
    top_asks = sorted(asks.items(), key=lambda x: x[0])[:DOM_ROWS]
    if not top_bids and not top_asks:
        return None

    biggest = max([sz for _, sz in top_bids + top_asks] + [1.0])
    rows = []
    for i in range(max(len(top_bids), len(top_asks))):
        bp, bs = top_bids[i] if i < len(top_bids) else ("", "")
        ap, asz = top_asks[i] if i < len(top_asks) else ("", "")
        b_shade = f"rgba(8,153,129,{0.06 + 0.34 * (bs / biggest):.3f})" if bs != "" else "transparent"
        a_shade = f"rgba(242,54,69,{0.06 + 0.34 * (asz / biggest):.3f})" if asz != "" else "transparent"
        rows.append(html.Tr([
            html.Td(f"{bs:,.0f}" if bs != "" else "",
                    style=dict(CELL, textAlign="right", color="#9fb0ad", backgroundColor=b_shade)),
            html.Td(f"{bp:,.1f}" if bp != "" else "",
                    style=dict(CELL, textAlign="right", color="#089981", fontWeight="bold",
                               backgroundColor=b_shade)),
            html.Td(f"{ap:,.1f}" if ap != "" else "",
                    style=dict(CELL, textAlign="left", color="#f23645", fontWeight="bold",
                               backgroundColor=a_shade)),
            html.Td(f"{asz:,.0f}" if asz != "" else "",
                    style=dict(CELL, textAlign="left", color="#c2a0a3", backgroundColor=a_shade)),
        ]))

    return html.Table(
        style={"width": "100%", "borderCollapse": "collapse", "tableLayout": "fixed"},
        children=[
            html.Thead(html.Tr([
                html.Th("BID SIZE", style=HEAD),
                html.Th("BID", style=HEAD),
                html.Th("ASK", style=dict(HEAD, textAlign="left")),
                html.Th("ASK SIZE", style=dict(HEAD, textAlign="left")),
            ])),
            html.Tbody(rows),
        ])



app = dash.Dash(__name__, title=f"TradingView Mobile Terminal")
server = app.server


@server.route("/health")
def health():
    """Cheap liveness probe for an external pinger, and a status readout.

    Serving this is far lighter than rendering the whole page, and it doubles as
    the hook that starts this worker's feed: a ping keeps the process both awake
    and collecting, not merely awake.
    """
    ensure_workers()
    with mobile_pipeline.lock:
        bids = len(mobile_pipeline.order_book["bids"])
        asks = len(mobile_pipeline.order_book["asks"])
        last_update = mobile_pipeline.last_update
        ltp = mobile_pipeline.ltp
        ws_state = mobile_pipeline.ws_state
        rest_error = mobile_pipeline.rest_error

    age = round(time.time() - last_update, 1) if last_update else None
    payload = {
        "status": "ok" if age is not None and age < STALE_AFTER else "stale",
        "symbol": SYMBOL,
        "session_day": str(mobile_pipeline.session_day.date()) if mobile_pipeline.session_day else None,
        "ltp": ltp,
        "book": {"bids": bids, "asks": asks},
        "seconds_since_update": age,
        "websocket": ws_state,
        "rest_error": rest_error or None,
        "pid": os.getpid(),
        "threads": sorted(n for n, t in _threads.items() if t.is_alive()),
        "callbacks": mobile_pipeline.callbacks,
        "seconds_since_callback": (round(time.time() - mobile_pipeline.last_callback, 1)
                                   if mobile_pipeline.last_callback else None),
        "render_error": mobile_pipeline.render_error or None,
        "dash_version": dash.__version__,
        "storage": {"enabled": store.enabled, "written": store.written,
                    "dropped": store.dropped, "error": store.error or None},
    }
    return json.dumps(payload), 200, {"Content-Type": "application/json"}

def serve_layout():
    """Rendered on every page load, so a reload always reflects current state.

    Built once at import, the seeded values froze at process start: the panel
    read "websocket: starting" indefinitely and a reload could never show live
    data, however healthy the feed was. As a function it also means the page is
    useful even when the update callback is not reaching the browser.
    """
    ticker, ticker_style, fig, ltp, ltp_style, delta, table = refresh_mobile_view(0)

    return html.Div(
    style={"backgroundColor": "#131722", "color": "#d1d4dc", "fontFamily": "sans-serif", "padding": "5px"},
    children=[
        html.Div(
            style={"display": "flex", "justifyContent": "space-between", "borderBottom": "1px solid #2a2e39", "padding": "8px", "fontSize": "13px"},
            children=[
                html.Span(f"📊 {SYMBOL} • 1S • DELTA", style={"fontWeight": "bold", "color": "#f2f3f5"}),
                html.Div(id="mobile-ticker-feed", children=ticker,
                         style=dict(ticker_style, fontWeight="bold"))
            ]
        ),
        html.Div(
            style={"display": "flex", "alignItems": "baseline", "gap": "10px",
                   "padding": "10px 8px 6px"},
            children=[
                html.Span("LTP", style={"color": "#787b86", "fontSize": "11px",
                                        "letterSpacing": "0.08em"}),
                html.Span(id="ltp-value", children=ltp, style=ltp_style),
                html.Span(id="ltp-delta", children=delta,
                          style={"fontSize": "12px", "fontVariantNumeric": "tabular-nums"}),
            ]
        ),
        dcc.Graph(id="mobile-master-chart", figure=fig,
                  config={"displayModeBar": False, "scrollZoom": True}),
        html.Div(id="dom-table", children=table, style={"padding": "4px 8px 12px"}),
        dcc.Interval(id="mobile-pulse-clock", interval=REFRESH_RATE_MS, n_intervals=0)
    ]
    )

# =====================================================================
# RENDERING PIPELINE CONTROLLER CALLBACK
# =====================================================================
@app.callback(
    [Output("mobile-ticker-feed", "children"),
     Output("mobile-ticker-feed", "style"),
     Output("mobile-master-chart", "figure"),
     Output("ltp-value", "children"),
     Output("ltp-value", "style"),
     Output("ltp-delta", "children"),
     Output("dom-table", "children")],
    [Input("mobile-pulse-clock", "n_intervals")]
)
def refresh_mobile_view(n):
    try:
        return _render(n)
    except Exception as exc:
        # Raising here means Dash sends no update at all, and the page sits on
        # whatever it last drew with nothing to say why. Show the fault instead.
        mobile_pipeline.render_error = f"{type(exc).__name__}: {exc}"[:120]
        print(f"[RENDER ERROR] {type(exc).__name__}: {exc}", flush=True)
        msg = f"RENDER ERROR · {type(exc).__name__}: {exc}"[:140]
        return (msg, {"color": "#f23645", "fontSize": "11px"},
                waiting_figure(msg, "", ""), "—",
                {"fontSize": "28px", "fontWeight": "bold", "color": "#787b86"}, "", None)


def _render(n):
    mobile_pipeline.callbacks += 1
    mobile_pipeline.last_callback = time.time()
    ensure_workers()        # a forked worker starts its own feed on first request

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
        rest_error = mobile_pipeline.rest_error
        ltp_error = mobile_pipeline.ltp_error
        ltp, prev_ltp = mobile_pipeline.ltp, mobile_pipeline.prev_ltp

    # Converted here, before anything is plotted: the series is kept in UTC and
    # only the axis reads in local time.
    times = [t.tz_convert(DISPLAY_TZ) for t in times]

    ltp_text, ltp_style, ltp_delta = format_ltp(ltp, prev_ltp)
    table = dom_table(bids, asks)

    if not times:
        return (f"WAITING · {ws_state}", {"color": "#db8c02"},
                waiting_figure(ws_state, rest_error, ltp_error),
                ltp_text, ltp_style, ltp_delta, table)

    last_price = cl[-1]
    ticker_color = "#089981" if ofi_steps_list[-1] >= 0 else "#f23645"
    ticker_text = f"P: ${last_price:,.1f} | OFI(D): {current_ofi:+,.0f}"

    # Without this a dead feed is indistinguishable from a quiet market: the
    # page keeps redrawing the same last candle and looks alive.
    if age is not None and age > STALE_AFTER:
        ticker_text += f" | STALE {age:,.0f}s"
        if rest_error: ticker_text += f" | REST {rest_error}"
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

    return (ticker_text, {"color": ticker_color}, fig,
            ltp_text, ltp_style, ltp_delta, table)

# Assigned here rather than beside the definition: Dash evaluates the callable
# immediately to validate it, and it renders through refresh_mobile_view below.
if AUTO_REFRESH_SECONDS > 0:
    app.index_string = app.index_string.replace(
        "{%metas%}",
        f'{{%metas%}}\n        <meta http-equiv="refresh" content="{AUTO_REFRESH_SECONDS}">')

app.layout = serve_layout


# =====================================================================
# CLOUD PRODUCTION SERVICE DEPLOYMENT RUN ENGINE
# =====================================================================
if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=int(os.environ.get("PORT", 8050)))