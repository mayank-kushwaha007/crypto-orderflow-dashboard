import gzip
import json
import math
import os
import urllib.request
import threading
import time
from collections import deque, OrderedDict
import websocket
import pandas as pd

import storage

import dash
from dash import dcc, html
from flask import request
from dash.dependencies import Input, Output
import plotly.graph_objects as go
from plotly.utils import PlotlyJSONEncoder
from plotly.subplots import make_subplots

# =====================================================================
# CONFIGURATION
# =====================================================================
# Tried in order. A connection that yields no book data is abandoned for the
# next one, so a wrong endpoint self-corrects.
SOCKET_URLS = [
    "wss://socket.india.delta.exchange",
    "wss://socket.delta.exchange",
    "wss://public-socket.india.delta.exchange",
]
STALL_SECONDS = 25      # No book data this long after connecting -> try the next URL

# REST fallback. The websocket needs a channel subscription the venue can refuse;
# this endpoint needs none.
REST_URL = "https://api.india.delta.exchange/v2/l2orderbook/{symbol}?depth=20"
TICKER_URL = "https://api.india.delta.exchange/v2/tickers/{symbol}"
REST_INTERVAL = 0.5     # REST is the primary source, polled continuously
REST_MAX_BACKOFF = 8.0  # Failures back off to here, then recover on success

# Executed trades, which the book channels do not carry: they carry resting
# orders. A footprint is built from what actually traded and on which side the
# aggressor stood, so it needs this feed and nothing else will do. Public and
# unauthenticated - it is get_public_trades() in Delta's own REST client.
TRADES_URL = "https://api.india.delta.exchange/v2/trades/{symbol}"
TRADES_INTERVAL = 1.0
# Falling back to mid prices after this long without a trade keeps the candles
# drawing when the trade feed is the thing that broke.
TRADE_FRESH = 30.0

# Render kills a free instance after ~15 minutes without INBOUND traffic.
# Outbound calls do not count, so the service requests its own public URL.
_BASE_URL = os.environ.get("RENDER_EXTERNAL_URL") or os.environ.get("KEEPALIVE_URL", "")
KEEPALIVE_URL = (_BASE_URL.rstrip("/") + "/health") if _BASE_URL else ""
KEEPALIVE_EVERY = 600   # 10 minutes, comfortably inside the 15 minute window

# The layout is rendered per request, so a reload is a real refresh. It drives
# updates only where the in-place callback is not arriving; 0 disables it.
AUTO_REFRESH_SECONDS = int(os.environ.get("AUTO_REFRESH_SECONDS", "5"))

# Persistence. Unset means storage goes inert, not that anything fails.
DATABASE_URL = os.environ.get("DATABASE_URL", "")

# Render sets RENDER_GIT_COMMIT on every deploy, so the page can report which
# commit it is running.
GIT_COMMIT = (os.environ.get("RENDER_GIT_COMMIT")
              or os.environ.get("GIT_COMMIT", "local"))[:7]
DOM_ROWS = 10           # Depth levels shown in the bid/ask table
SYMBOL = "BTCUSD"
MAX_HISTORY = 40        # Optimized timeline length for vertical mobile viewports
# Browser update interval. A gzipped frame is ~1.6KB, so 5000ms costs about
# 0.32KB/s.
REFRESH_RATE_MS = int(os.environ.get("REFRESH_RATE_MS", "5000"))
# Abort a hung frame fetch: four intervals, capped so a slow cadence cannot
# hold a dead request slot for twenty seconds.
FETCH_TIMEOUT_MS = min(10000, max(4000, REFRESH_RATE_MS * 4))
# A callback this recent means in-place updates are working. Must exceed one
# interval, or the check flaps between ticks and re-arms the reload tag.
CALLBACK_FRESH = max(5.0, REFRESH_RATE_MS / 1000.0 * 2)
STALL_RELOAD = 8        # Consecutive failed fetches before reloading the page
BUCKET = "1s"           # Candles aggregate every update within one wall-clock second
# Candles are kept in UTC and converted for display only, so stored data stays
# unambiguous across DST and deployments.
DISPLAY_TZ = os.environ.get("DISPLAY_TZ", "Asia/Kolkata")
STALE_AFTER = 5         # Seconds without a book update before the ticker says so

# Footprint. Each bar is one FOOTPRINT_BUCKET of trades, split into rows
# showing sell volume x buy volume at that price.
FOOTPRINT_BUCKET = os.environ.get("FOOTPRINT_BUCKET", "5min")
# Row height. "auto" sizes it from what the instrument actually did, so the
# chart reads the same on a $77,000 future and a $0.60 alt without being told
# which it is. A number here overrides that and is used as given.
FOOTPRINT_TICK = os.environ.get("FOOTPRINT_TICK", "auto")
FOOTPRINT_ROWS_TARGET = int(os.environ.get("FOOTPRINT_ROWS_TARGET", "14"))
# Re-snap only when the ideal row height is this far from the one in use. The
# grid rescaling under the reader costs more than a slightly wrong row does,
# and bars either side of a rescale are not comparable.
TICK_HYSTERESIS = float(os.environ.get("TICK_HYSTERESIS", "1.5"))
# Trades are stored at their exact price and bucketed into rows only when the
# chart is drawn, so changing the row height re-buckets the whole history
# rather than leaving old bars on the old grid. This caps the distinct prices
# one bar will hold, since that is what the store costs.
FOOTPRINT_MAX_LEVELS = int(os.environ.get("FOOTPRINT_MAX_LEVELS", "5000"))
# Thresholds for the per-bar read. These are judgement calls, not measured
# edges: retune them against your own instrument before trusting a flag.
FP_IMBALANCE = float(os.environ.get("FP_IMBALANCE", "0.35"))    # |delta|/volume to call a side
FP_ABSORB_POS = float(os.environ.get("FP_ABSORB_POS", "0.35"))  # close this near the wrong end
# One bar's range is a real measurement but a bad estimate of the chart's, so
# below this many bars the row height comes from the price level instead. One
# bar of a quiet minute gave $2 rows on a $85,000 instrument.
FP_TICK_MIN_BARS = int(os.environ.get("FP_TICK_MIN_BARS", "3"))
FP_TICK_SEED = float(os.environ.get("FP_TICK_SEED", "0.0005"))  # of price, per row
try:
    FIXED_TICK = float(FOOTPRINT_TICK)
    if FIXED_TICK <= 0: FIXED_TICK = 0.0
except ValueError:
    FIXED_TICK = 0.0    # "auto", or anything unparseable: size it from the data
# Bars are wide: at 400px twelve of them overlap into unreadable mush, five do
# not. The browser sends its width with the frame request and the server picks.
FOOTPRINT_BARS = int(os.environ.get("FOOTPRINT_BARS", "12"))
FOOTPRINT_BARS_NARROW = int(os.environ.get("FOOTPRINT_BARS_NARROW", "5"))
NARROW_PX = int(os.environ.get("NARROW_PX", "600"))
TRADE_DEDUPE = 4000     # Recent trade keys kept, so a re-poll cannot double count
# The footprint is the chart being read now, so it gets the height. The OFI
# panel keeps its shape as a strip: the footprint already draws the candles,
# so what is left worth seeing there is the OFI bar and whether it agrees.
FOOTPRINT_HEIGHT = int(os.environ.get("FOOTPRINT_HEIGHT", "620"))
OFI_STRIP_HEIGHT = int(os.environ.get("OFI_STRIP_HEIGHT", "190"))

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
        self.callbacks = 0              # frames served to the browser
        self.last_callback = 0.0
        self.render_error = ""
        self.ltp = None                 # last traded price, from the ticker endpoint
        self.prev_ltp = None
        self.last_update = 0.0          # wall clock of the last book update, any source
        self.ws = None

        # Executed trades, for the footprint and for real candle prices.
        self.footprint = OrderedDict()  # bar ts -> {levels, ohlc, buy, sell}
        self.trade_price = None
        self.last_trade = 0.0           # wall clock of the last trade seen
        self.trades_seen = 0
        self.trade_error = ""
        self._trade_keys = set()
        self._trade_order = deque(maxlen=TRADE_DEDUPE)
        self.logged_trade = False       # the first raw payload goes to the log once
        # Held row height, keyed by bar count: a phone and a laptop see
        # different spans, so they must not fight over one value.
        self.fp_tick = {}

        self.opens = deque(maxlen=MAX_HISTORY)
        self.highs = deque(maxlen=MAX_HISTORY)
        self.lows = deque(maxlen=MAX_HISTORY)
        self.closes = deque(maxlen=MAX_HISTORY)

mobile_pipeline = MobileTerminalEngine()
store = storage.Storage(DATABASE_URL, SYMBOL)

# =====================================================================
# DATA INGESTION
# =====================================================================
def parse_level(level):
    """Delta sends a level either as ["price", "size"] or as
    {"limit_price": "...", "size": ...}. Handle both, return (price, size)."""
    if isinstance(level, dict):
        return float(level.get("limit_price", level.get("price"))), float(level.get("size", 0))
    return float(level[0]), float(level[1])


def apply_levels(side, levels, book=None):
    if book is None: book = mobile_pipeline.order_book[side]
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


def touch_candle(sec, price):
    """Fold a price into the current second, rolling the candle over at the
    boundary. The single owner of the 1s candle, called both by the book and by
    the trade feed. Caller must hold the lock."""
    p = mobile_pipeline
    if p.cur_sec is None:
        p.cur_sec = sec
        p.cur_open = p.closes[-1] if p.closes else price
        p.cur_high = p.cur_low = price
        p.cur_ofi = 0.0
    elif sec != p.cur_sec:
        flush_bucket()
        p.cur_sec = sec
        p.cur_open = p.cur_close
        p.cur_high = p.cur_low = price
        p.cur_ofi = 0.0

    p.cur_high = max(p.cur_high, price)
    p.cur_low = min(p.cur_low, price)
    p.cur_close = price


def trades_fresh():
    """True while the trade feed is delivering. Candle prices come from trades
    when it is and from the book mid when it is not, so the chart keeps drawing
    if the trade feed is the component that fails."""
    last = mobile_pipeline.last_trade
    return bool(last) and (time.time() - last) < TRADE_FRESH


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

    # Cumulative OFI is a UTC daily session total. The open second is closed
    # against the old day's running figure before the reset, so no bar records a
    # total from the wrong session.
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
    # message: the feed bursts many updates per second. The price is the last
    # traded one where trades are arriving, and the book mid otherwise.
    sec = pd.Timestamp.now(tz="UTC").floor(BUCKET)
    price = p.trade_price if (trades_fresh() and p.trade_price) else mid_price
    touch_candle(sec, price)

    p.cur_ofi += step_ofi
    p.last_update = time.time()

    mobile_pipeline.prev_best_bid_price = best_bid
    mobile_pipeline.prev_best_bid_size = best_bid_sz
    mobile_pipeline.prev_best_ask_price = best_ask
    mobile_pipeline.prev_best_ask_size = best_ask_sz


def trade_time(raw):
    """Delta stamps trades in microseconds. Accept seconds and milliseconds too
    rather than trusting a magnitude that has not been confirmed on this venue."""
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return pd.Timestamp.now(tz="UTC")
    if v > 1e17: v = v / 1e9         # nanoseconds
    elif v > 1e14: v = v / 1e6       # microseconds
    elif v > 1e11: v = v / 1e3       # milliseconds
    return pd.Timestamp(v, unit="s", tz="UTC")


def parse_trade(t):
    """One executed trade as (timestamp, price, size, is_buy).

    The exact field names on this venue are not confirmable from here, so every
    plausible spelling is accepted the same way parse_level accepts both level
    shapes. is_buy means the aggressor bought: it lifted the ask.
    """
    price = float(t.get("price", t.get("p")))
    size = float(t.get("size", t.get("s", t.get("volume", 0))))

    buyer, seller = t.get("buyer_role"), t.get("seller_role")
    side = str(t.get("side", "")).lower()
    if buyer == "taker" or seller == "maker": is_buy = True
    elif seller == "taker" or buyer == "maker": is_buy = False
    elif side in ("buy", "b"): is_buy = True
    elif side in ("sell", "s"): is_buy = False
    # Delta's l2 convention elsewhere in this file is buy/sell for bid/ask.
    elif t.get("buyer_role") is None and t.get("is_buyer_maker") is not None:
        is_buy = not bool(t.get("is_buyer_maker"))
    else:
        raise ValueError("no side field")

    ts = trade_time(t.get("timestamp", t.get("created_at", t.get("time"))))
    if size <= 0:
        raise ValueError("non-positive size")
    return ts, price, size, is_buy


def record_trade(ts, price, size, is_buy):
    """Fold one trade into the footprint and into the live candle. Lock held."""
    p = mobile_pipeline
    bar = ts.floor(FOOTPRINT_BUCKET)
    c = p.footprint.get(bar)
    if c is None:
        c = {"levels": {}, "open": price, "high": price, "low": price,
             "close": price, "buy": 0.0, "sell": 0.0}
        p.footprint[bar] = c
        # Keep a little more than the widest view asks for, so switching to a
        # laptop does not show a chart that has to refill.
        while len(p.footprint) > FOOTPRINT_BARS + 4:
            p.footprint.popitem(last=False)

    level = c["levels"].get(price)
    if level is None and len(c["levels"]) < FOOTPRINT_MAX_LEVELS:
        level = c["levels"][price] = [0.0, 0.0]          # [sell, buy]
    # Past the cap the level accounting is skipped, but everything below it
    # still runs: the trade is real and the candle and liveness depend on it.
    if level is not None:
        level[1 if is_buy else 0] += size
    c["buy" if is_buy else "sell"] += size
    c["high"] = max(c["high"], price)
    c["low"] = min(c["low"], price)
    c["close"] = price

    p.trade_price = price
    p.last_trade = time.time()
    p.trades_seen += 1
    touch_candle(ts.floor(BUCKET), price)


def ingest_trades(raw_list):
    """Apply a batch of raw trades, skipping ones already counted.

    A REST poll returns a window that overlaps the previous one, so without the
    dedupe every poll would inflate the footprint by whatever it re-read.
    """
    p = mobile_pipeline
    fresh = 0
    for t in raw_list:
        if not isinstance(t, dict):
            continue
        try:
            ts, price, size, is_buy = parse_trade(t)
        except (TypeError, ValueError, KeyError):
            continue
        key = t.get("id") or t.get("trade_id") or (t.get("timestamp"), price, size, is_buy)
        if key in p._trade_keys:
            continue
        if len(p._trade_order) == p._trade_order.maxlen:
            p._trade_keys.discard(p._trade_order[0])
        p._trade_order.append(key)
        p._trade_keys.add(key)
        record_trade(ts, price, size, is_buy)
        fresh += 1
    return fresh


def log_first_trade(payload):
    """Print one raw payload so the real field names can be read off the Render
    logs. The venue's trade schema could not be confirmed from the sandbox."""
    if mobile_pipeline.logged_trade:
        return
    mobile_pipeline.logged_trade = True
    print(f"[TRADES] first raw payload: {json.dumps(payload)[:600]}", flush=True)


def poll_trades():
    """Poll executed trades. The book poller cannot serve this: it returns
    resting orders, and a footprint needs fills."""
    url = TRADES_URL.format(symbol=SYMBOL)
    headers = {"Accept": "application/json", "User-Agent": "orderflow-dashboard/1.0"}
    delay = TRADES_INTERVAL
    while True:
        try:
            time.sleep(delay)
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=10) as resp:
                payload = json.loads(resp.read().decode())

            result = payload.get("result")
            if isinstance(result, dict):
                result = result.get("trades") or result.get("result") or []
            log_first_trade(result[:2] if isinstance(result, list) else payload)

            with mobile_pipeline.lock:
                fresh = ingest_trades(result or [])
            if fresh:
                mobile_pipeline.trade_error = ""
            delay = TRADES_INTERVAL
        except Exception as exc:
            delay = min(delay * 2, REST_MAX_BACKOFF)
            mobile_pipeline.trade_error = f"{type(exc).__name__}: {exc}"[:70]
            print(f"[TRADES ERROR] retry in {delay:.1f}s: {type(exc).__name__}: {exc}",
                  flush=True)


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

    elif msg_type in ("all_trades", "trades", "recent_trade"):
        # Lower latency than the poller where the venue accepts the channel. The
        # channel name is not confirmable from the sandbox, so both spellings are
        # subscribed and whichever arrives is handled.
        batch = data.get("trades")
        if not isinstance(batch, list):
            batch = [data]
        log_first_trade(batch[:2])
        with mobile_pipeline.lock:
            ingest_trades(batch)

    elif msg_type == "l2_orderbook":
        # Full depth snapshot; Delta names the sides buy/sell on this channel.
        # Parsed into a scratch book for the same reason as the REST poller: an
        # empty snapshot must not wipe a working one.
        bids, asks = {}, {}
        apply_levels("bids", data.get("buy") or data.get("bids") or [], bids)
        apply_levels("asks", data.get("sell") or data.get("asks") or [], asks)
        if not (bids and asks):
            return
        with mobile_pipeline.lock:
            mobile_pipeline.order_book["bids"] = bids
            mobile_pipeline.order_book["asks"] = asks
            update_metrics()
            mobile_pipeline.last_ws_data = time.time()


def on_open(ws):
    # Separate frames, so a rejected channel name does not fail the other.
    for name in ("l2_updates", "l2_orderbook", "all_trades", "trades"):
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
        # Everything inside the guard: a thread that dies here takes the
        # fallback down for the process lifetime.
        try:
            time.sleep(delay)
            fetch_ltp(headers)

            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=10) as resp:
                payload = json.loads(resp.read().decode())

            result = payload.get("result") or {}
            # Parse into a scratch book first. Clearing the live one and then
            # finding the response carried no levels left an empty book, and an
            # empty book makes update_metrics return before it touches
            # last_update - so the feed went stale with rest_error cleared to ""
            # and nothing anywhere said why. Raising puts it on the error path,
            # which backs off, logs, and shows the reason in the ticker.
            bids, asks = {}, {}
            apply_levels("bids", result.get("buy") or [], bids)
            apply_levels("asks", result.get("sell") or [], asks)
            if not (bids and asks):
                raise ValueError("no levels in response: %.90s" % json.dumps(payload))
            with mobile_pipeline.lock:
                mobile_pipeline.order_book["bids"] = bids
                mobile_pipeline.order_book["asks"] = asks
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
        # sent nothing this time is still rotated away from.
        if mobile_pipeline.last_ws_data < mobile_pipeline.conn_started:
            mobile_pipeline.url_index = (mobile_pipeline.url_index + 1) % len(SOCKET_URLS)
        mobile_pipeline.ws_state = "reconnecting"
        print("[WS] reconnecting in 5s...", flush=True)
        time.sleep(5)


def keepalive():
    """Request our own public URL so Render sees inbound traffic and stays up.

    Only inbound requests reset Render's idle timer, so calls out to the exchange
    do not help. It keeps a live instance alive; it cannot wake a sleeping one,
    since nothing inside it is left to make the call.
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
           ("trades", poll_trades), ("keepalive", keepalive))
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

    Threads do not survive fork(), so threads started at import exist only in a
    `gunicorn --preload` master and every forked worker serves a frozen
    snapshot. Calling this from the request path as well as at import keeps the
    process that answers requests the one running the feed.
    """
    if store.enabled and not store._started:
        store.start()
        # Off the request path: a slow or unreachable database would otherwise
        # stall every callback behind it.
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
# PRESENTATION
# =====================================================================
CELL = {"padding": "3px 10px", "fontVariantNumeric": "tabular-nums",
        "fontFamily": "ui-monospace, Menlo, monospace", "fontSize": "12px"}
HEAD = dict(CELL, color="#787b86", fontSize="10px", letterSpacing="0.06em",
            borderBottom="1px solid #2a2e39", textAlign="right")


def waiting_figure(ws_state, rest_error, ltp_error, height=FOOTPRINT_HEIGHT):
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
                      height=height, margin=dict(l=8, r=40, t=5, b=5), showlegend=False,
                      dragmode=False)
    fig.update_xaxes(visible=False, fixedrange=True)
    fig.update_yaxes(visible=False, fixedrange=True)
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


# 1 / 2 / 2.5 / 5 x 10^k. Rows land on prices a person recognises - 25, 50,
# 250 - rather than on 37.4, which is what span/rows alone would give.
NICE_TICKS = (1.0, 2.0, 2.5, 5.0, 10.0)


def nice_tick(raw):
    """Snap a raw row height up to the next round number."""
    if not raw or raw <= 0 or not math.isfinite(raw):
        return 1.0
    mag = 10.0 ** math.floor(math.log10(raw))
    for step in NICE_TICKS:
        if raw <= step * mag * 1.000001:
            return step * mag
    return 10.0 * mag


def choose_tick(span, current):
    """Row height for a price span, holding the previous one where it is close.

    The target is a row count, not a row size: FOOTPRINT_ROWS_TARGET rows are
    what fits the screen whatever the instrument costs, so the same code reads
    on a $77,000 future and a $0.60 alt. Without hysteresis the grid would
    rescale on every ordinary swing in volatility and bars either side of the
    change would not be comparable, so an established tick is kept until the
    ideal one is TICK_HYSTERESIS away in either direction.
    """
    if span <= 0 or not math.isfinite(span):
        return current or 1.0
    raw = span / FOOTPRINT_ROWS_TARGET
    if not current:
        return nice_tick(raw)
    # Compare the UNSNAPPED ideal against what is held. Snapping first would
    # make the threshold meaningless: two snapped values are already a whole
    # rung apart, so any drift across a rung boundary would clear any ratio.
    ratio = max(raw / current, current / raw)
    return nice_tick(raw) if ratio >= TICK_HYSTERESIS else current


def fmt_tick(t):
    """Row height for the header, without trailing noise on sub-unit ticks."""
    return f"{t:,.0f}" if t >= 1 else f"{t:g}"


def bucket_levels(levels, tick):
    """Exact traded prices folded into display rows of `tick`."""
    grid = {}
    for price, (sell, buy) in levels.items():
        row = round(math.floor(price / tick) * tick, 8)
        cell = grid.get(row)
        if cell is None: cell = grid[row] = [0.0, 0.0]
        cell[0] += sell
        cell[1] += buy
    return grid


def read_bar(bar, prev):
    """A short read of one footprint bar: (label, flagged, why).

    The labels describe what the bar did. A flag marks the two cases worth
    stopping on, and it is a prompt to look, NOT a signal to trade - these are
    conventional readings with thresholds picked by hand, never backtested
    here, and a bar means little without the level it is trading against.

    ABS  absorption. One side was clearly the aggressor and price closed at the
         opposite end anyway, so that aggression was filled by someone. The
         arrow points where the absorbing side would have price go.
    DIV  divergence. A higher high on weaker buying than the bar before, or a
         lower low on weaker selling: the extension is not backed by flow.
    BUY  buyers were the aggressors and price agreed. SELL the mirror.
    BAL  neither side dominant.
    """
    vol = bar["buy"] + bar["sell"]
    if vol <= 0:
        return "—", False, ""
    skew = (bar["buy"] - bar["sell"]) / vol
    rng = bar["high"] - bar["low"]
    pos = (bar["close"] - bar["low"]) / rng if rng > 0 else 0.5

    if skew >= FP_IMBALANCE and pos <= FP_ABSORB_POS:
        return "ABS↓", True, "buying absorbed at the highs"
    if skew <= -FP_IMBALANCE and pos >= 1 - FP_ABSORB_POS:
        return "ABS↑", True, "selling absorbed at the lows"

    if prev is not None:
        d, pd_ = bar["buy"] - bar["sell"], prev["buy"] - prev["sell"]
        if bar["high"] > prev["high"] and d < pd_ and d > 0 and pd_ > 0:
            return "DIV↓", True, "higher high on weaker buying"
        if bar["low"] < prev["low"] and d > pd_ and d < 0 and pd_ < 0:
            return "DIV↑", True, "lower low on weaker selling"

    if skew >= FP_IMBALANCE: return "BUY", False, ""
    if skew <= -FP_IMBALANCE: return "SELL", False, ""
    return "BAL", False, ""


def read_bars(keys, bars):
    """read_bar over the visible bars, each against the one before it."""
    out, prev = [], None
    for k in keys:
        out.append(read_bar(bars[k], prev))
        prev = bars[k]
    return out


def fp_num(v):
    """Cell volumes, short. A column is about 70px at phone width, so a pair
    like "1,040 x 1,521" runs past the cell and over the price axis; "1.0k x
    1.5k" does not."""
    if v >= 999_500: return f"{v / 1e6:.1f}M"
    if v >= 1000: return f"{v / 1000:.1f}k"
    return f"{v:,.0f}"


def age_text(seconds):
    """A gap in units a person reads at a glance. "13,247s" does not say
    "three and a half hours" without arithmetic."""
    if seconds < 90: return f"{seconds:.0f}s"
    if seconds < 5400: return f"{seconds / 60:.0f}m"
    return f"{seconds / 3600:.1f}h"


def footprint_figure(bars, trade_error, tick, stale=0.0):
    """Per-bar, per-price-row sell x buy volume, candles drawn over the top.

    Reading it. Each cell is `sell x buy` for that price row: volume that hit
    the bid, then volume that lifted the ask. Green means buyers were the
    aggressors there, red means sellers. What the chart is for:

    - Absorption. A row with heavy volume where price then refuses to continue
      is someone large filling against the move. Heavy buying at the top of a
      bar that closes weak means those buyers were fed; the level above is
      defended. This is the reason to have a footprint at all, and it is
      invisible to OFI, which nets it to zero.
    - Imbalance. Compare a row diagonally against its neighbour, bid to ask:
      a run of rows imbalanced the same way marks where one side was in
      control, and those edges tend to matter again on a revisit.
    - Point of control. The fattest row in a bar is where the volume agreed on
      value. Price leaving it quickly and not returning is acceptance; price
      keeping coming back to it is a range.
    - Delta divergence. The bar under the grid is buy minus sell for the whole
      bar. Price making a new high while that bar prints lower than the last
      high means the move is running on thin offers, not on buying.
    - Exhaustion. A tiny cell at the extreme of a bar - almost all one side,
      almost no volume - is the aggressor running out, not breaking out.

    None of this is a signal on its own. The footprint says what happened at a
    price; it does not say what happens next, and a single bar rarely means
    anything without the level it is trading against.

    A Heatmap carries the numbers because it is one trace for the whole grid
    rather than one per cell, and its texttemplate puts the pair inside each
    box. Its x axis is categorical, so go.Candlestick cannot share it and the
    bodies and wicks are line segments instead. This is the arrangement the
    public OrderflowChart project settled on, and the reason is the same.
    """
    if not bars:
        why = f"trades: {trade_error}" if trade_error else "waiting for the first trades"
        return waiting_figure(why, "", ""), "FOOTPRINT · no trades yet"

    keys = sorted(bars)
    labels = [k.tz_convert(DISPLAY_TZ).strftime("%H:%M") for k in keys]
    grids = {k: bucket_levels(bars[k]["levels"], tick) for k in keys}
    rows = sorted({r for g in grids.values() for r in g})

    xs, ys, zs, texts = [], [], [], []
    for label, k in zip(labels, keys):
        levels = grids[k]
        for row in rows:
            sell, buy = levels.get(row, (0.0, 0.0))
            xs.append(label)
            ys.append(row)
            if not (sell or buy):
                zs.append(None)
                texts.append("")
                continue
            # Imbalance colours the cell: +1 all buying, -1 all selling.
            zs.append((buy - sell) / (buy + sell))
            texts.append(f"{fp_num(sell)} x {fp_num(buy)}")

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        vertical_spacing=0.02, row_heights=[0.82, 0.18])
    fig.add_trace(go.Heatmap(
        x=xs, y=ys, z=zs, text=texts, texttemplate="%{text}",
        textfont={"size": 9, "family": "ui-monospace, monospace"},
        colorscale=[[0.0, "#5c1a20"], [0.5, "#1c2230"], [1.0, "#0d4a3e"]],
        zmid=0, showscale=False, xgap=2, ygap=1, hoverinfo="skip",
    ), row=1, col=1)

    for label, k in zip(labels, keys):
        c = bars[k]
        colour = "#089981" if c["close"] >= c["open"] else "#f23645"
        fig.add_trace(go.Scatter(x=[label, label], y=[c["low"], c["high"]], mode="lines",
                                 line=dict(color=colour, width=1),
                                 hoverinfo="skip", showlegend=False), row=1, col=1)
        fig.add_trace(go.Scatter(x=[label, label], y=[c["open"], c["close"]], mode="lines",
                                 line=dict(color=colour, width=5),
                                 hoverinfo="skip", showlegend=False), row=1, col=1)

    deltas = [bars[k]["buy"] - bars[k]["sell"] for k in keys]
    fig.add_trace(go.Bar(x=labels, y=deltas, hoverinfo="skip", showlegend=False,
                         marker_color=["#089981" if d >= 0 else "#f23645" for d in deltas]),
                  row=2, col=1)

    # The read of each bar goes under its time, and a flagged one gets a mark
    # above its high. Both are drawn from read_bars, so the label and the flag
    # can never disagree about what the bar did.
    reads = read_bars(keys, bars)
    ticktext = []
    for label, (name, flagged, _why) in zip(labels, reads):
        ticktext.append(f"{label}<br>{'⚑ ' if flagged else ''}{name}")
    for label, k, (name, flagged, why) in zip(labels, keys, reads):
        if not flagged: continue
        up = name.endswith("↑")
        fig.add_annotation(x=label, y=bars[k]["high"], text="⚑", showarrow=False,
                           yshift=12, font=dict(size=13,
                           color="#089981" if up else "#f23645"),
                           hovertext=why, row=1, col=1)

    # b=22, not the main chart's 5: the bar times sit on this axis and 5px
    # clips them against whatever is drawn underneath.
    # dragmode=False as well as fixedrange: fixedrange takes zoom and pan off
    # the axes but Plotly still installs its touch drag layer, which swallows a
    # swipe, so the page could not be scrolled past the chart on a phone.
    fig.update_layout(template="plotly_dark", paper_bgcolor="#131722",
                      plot_bgcolor="#131722", height=FOOTPRINT_HEIGHT,
                      margin=dict(l=8, r=40, t=5, b=34), showlegend=False,
                      dragmode=False, uirevision="footprint")
    # Dragging a chart on a phone otherwise pans it instead of scrolling the
    # page, which makes the page hard to move around. fixedrange takes zoom and
    # pan off both axes, so the touch reaches the page.
    fig.update_yaxes(side="right", tickfont=dict(size=9), gridcolor="#2a2e39",
                     fixedrange=True, row=1, col=1)
    fig.update_yaxes(side="right", tickfont=dict(size=8), gridcolor="#2a2e39",
                     fixedrange=True, row=2, col=1)
    fig.update_xaxes(fixedrange=True, row=1, col=1)
    fig.update_xaxes(tickfont=dict(size=9), gridcolor="#2a2e39", fixedrange=True,
                     tickmode="array", tickvals=labels, ticktext=ticktext,
                     row=2, col=1)

    total = sum(bars[k]["buy"] + bars[k]["sell"] for k in keys)
    name, flagged, why = reads[-1]
    now = f"⚑ {name} · {why}" if flagged else name
    head = (f"FOOTPRINT · {FOOTPRINT_BUCKET} · ${fmt_tick(tick)} rows · "
            f"sell x buy · Δ {deltas[-1]:+,.0f} · vol {total:,.0f} · {now}")
    # A stale feed leaves the last bars on screen looking current. Say it here,
    # on the chart being read, not only in the ticker above it.
    if stale > STALE_AFTER:
        head = f"NO TRADES FOR {age_text(stale)} · showing {len(keys)} bar(s) · " + head
    elif len(keys) < 3:
        head = f"FILLING · {len(keys)} bar(s) so far · " + head
    return fig, head


def callbacks_arriving():
    """True when the in-place update callback has run recently enough to drive
    the page on its own."""
    last = mobile_pipeline.last_callback
    return bool(last) and (time.time() - last) < CALLBACK_FRESH


# A reload puts the reader back at the top of the page, which on a phone means
# losing the chart they had scrolled to. Both reloads here are involuntary - the
# refresh tag and the stall recovery - so the position is carried across.
# Re-applied as the graphs render, since the page is not full height until then.
SCROLL_KEEP = """
<script>
(function () {
  var KEY = 'of-scroll';
  function save() { try { sessionStorage.setItem(KEY, String(window.scrollY)); } catch (e) {} }
  function restore() {
    try {
      var y = parseInt(sessionStorage.getItem(KEY) || '0', 10);
      if (!y) { return; }
      var tries = 0;
      var t = setInterval(function () {
        if (window.scrollY < y) { window.scrollTo(0, y); }
        if (++tries > 20) { clearInterval(t); }
      }, 100);
    } catch (e) {}
  }
  window.addEventListener('scroll', save, {passive: true});
  if (document.readyState === 'loading') {
    window.addEventListener('DOMContentLoaded', restore);
  } else { restore(); }
})();
</script>
"""


def reload_timer():
    """The fallback reload, as a timer the page can cancel.

    This was a <meta http-equiv="refresh"> tag. Once the browser has parsed one
    it is armed and cannot be called off - removing the element does nothing -
    so a page that turned out to be updating perfectly well still reloaded, and
    every reload threw the reader back to the top. A timer does the same job
    and the first frame that lands clears it.
    """
    return (f'<script>window._ofReload = setTimeout(function () '
            f'{{ location.reload(); }}, {AUTO_REFRESH_SECONDS * 1000});</script>')


class LiveDash(dash.Dash):
    """Arms the fallback reload only while the update callback is not arriving.

    Where the callback works the timer is absent, or is cleared by the first
    frame, and the page updates in place as Dash intends. The server only sees
    its own worker's callback counter, so the browser clearing the timer is the
    authority: under more than one gunicorn worker the worker rendering the page
    may never have served a frame.
    """

    def interpolate_index(self, **kwargs):
        doc = super().interpolate_index(**kwargs)
        if AUTO_REFRESH_SECONDS > 0 and not callbacks_arriving():
            doc = doc.replace("<head>", "<head>" + reload_timer(), 1)
        return doc.replace("<head>", "<head>" + SCROLL_KEEP, 1)


app = LiveDash(__name__, title="TradingView Mobile Terminal")
server = app.server


@server.after_request
def compress(response):
    """Gzip every text response, not just the frame.

    A page reload is ~26KB uncompressed and ~6.8KB gzipped, which on a 3KB/s
    link is the difference between nine seconds of blank screen and two.
    """
    if (response.direct_passthrough
            or response.status_code < 200 or response.status_code >= 300
            or "Content-Encoding" in response.headers
            or "gzip" not in (request.headers.get("Accept-Encoding") or "")):
        return response

    ctype = response.headers.get("Content-Type", "")
    if not any(t in ctype for t in ("text/", "json", "javascript")):
        return response

    body = response.get_data()
    if len(body) < 500:          # below this the header overhead is not worth it
        return response

    response.set_data(gzip.compress(body, 6))
    response.headers["Content-Encoding"] = "gzip"
    response.headers["Content-Length"] = len(response.get_data())
    response.headers.add("Vary", "Accept-Encoding")
    return response


@server.route("/api/frame")
def api_frame():
    """Everything the page needs, as one GET."""
    mobile_pipeline.callbacks += 1
    mobile_pipeline.last_callback = time.time()

    width = 0
    try:
        width = int(request.args.get("w", 0))
    except (TypeError, ValueError):
        width = 0
    (ticker, ticker_style, fig, ltp, ltp_style, delta, table,
     fp_fig, fp_head) = _safe_render(0, width)
    # The template is ~8KB, static, and already established by the first render.
    fig_json = fig.to_plotly_json()
    fig_json.get("layout", {}).pop("template", None)
    fp_json = fp_fig.to_plotly_json()
    fp_json.get("layout", {}).pop("template", None)

    payload = {
        "clock": frame_clock(),
        "ticker": ticker,
        "ticker_style": dict(ticker_style, fontWeight="bold"),
        "figure": fig_json,
        "ltp": ltp,
        "ltp_style": ltp_style,
        "ltp_delta": delta,
        "table": table,
        "footprint": fp_json,
        "footprint_head": fp_head,
    }
    # PlotlyJSONEncoder recurses into nested components, which is what the table
    # needs. to_plotly_json() converts only the outermost one.
    return json.dumps(payload, cls=PlotlyJSONEncoder), 200, {
        "Content-Type": "application/json", "Cache-Control": "no-store"}


@server.route("/health")
def health():
    """Cheap liveness probe for an external pinger, and a status readout.

    Far lighter than rendering the page, and it starts this worker's feed, so a
    ping keeps the process collecting rather than merely awake.
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
        "commit": GIT_COMMIT,
        "session_day": str(mobile_pipeline.session_day.date()) if mobile_pipeline.session_day else None,
        "ltp": ltp,
        "book": {"bids": bids, "asks": asks},
        "seconds_since_update": age,
        "websocket": ws_state,
        "rest_error": rest_error or None,
        "trades": {"seen": mobile_pipeline.trades_seen,
                   "bars": len(mobile_pipeline.footprint),
                   "fresh": trades_fresh(),
                   "seconds_since_trade": (round(time.time() - mobile_pipeline.last_trade, 1)
                                           if mobile_pipeline.last_trade else None),
                   "error": mobile_pipeline.trade_error or None,
                   "tick": (FIXED_TICK or None) if FIXED_TICK
                           else dict(mobile_pipeline.fp_tick)},
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

def frame_clock():
    """Server time this frame was built. It advances only when a frame actually
    reaches the page, which separates a stalled feed from a stalled transport."""
    return pd.Timestamp.now(tz="UTC").tz_convert(DISPLAY_TZ).strftime("%H:%M:%S")


def callback_badge():
    """One short string saying whether Dash's in-place updates are reaching the
    browser. Rendered server-side on every page load, so it is visible without
    opening /health or a console."""
    n = mobile_pipeline.callbacks
    last = mobile_pipeline.last_callback
    if not last:
        return f"cb {n} · never · {GIT_COMMIT}"
    return f"cb {n} · {time.time() - last:.1f}s · {GIT_COMMIT}"


def serve_layout():
    """Rendered on every page load, so a reload always reflects current state.

    Must stay a callable: built once at import it would freeze at the values
    seeded at process start. It also keeps the page useful where the update
    callback is not reaching the browser.
    """
    (ticker, ticker_style, fig, ltp, ltp_style, delta, table,
     fp_fig, fp_head) = _safe_render(0)

    return html.Div(
    style={"backgroundColor": "#131722", "color": "#d1d4dc", "fontFamily": "sans-serif", "padding": "5px"},
    children=[
        html.Div(
            style={"display": "flex", "justifyContent": "space-between", "borderBottom": "1px solid #2a2e39", "padding": "8px", "fontSize": "13px"},
            children=[
                html.Span(f"📊 {SYMBOL} • 1S • DELTA", style={"fontWeight": "bold", "color": "#f2f3f5"}),
                html.Span(id="frame-clock", children=frame_clock(),
                          style={"fontSize": "11px", "color": "#787b86",
                                 "fontFamily": "ui-monospace, monospace"}),
                html.Span(callback_badge(), style={"fontSize": "10px", "color": "#787b86",
                                                   "fontFamily": "ui-monospace, monospace"}),
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
        html.Div(id="footprint-head", children=fp_head,
                 style={"padding": "6px 8px 2px", "fontSize": "11px",
                        "color": "#787b86", "fontFamily": "ui-monospace, monospace"}),
        # The height is pinned in CSS as well as in the figure. Replacing a
        # figure re-renders the graph, and for that moment the container has no
        # height: the document collapses, the browser clamps scrollY to 0, and
        # every frame threw the reader back to the top of the page.
        dcc.Graph(id="footprint-chart", figure=fp_fig,
                  style={"height": f"{FOOTPRINT_HEIGHT}px"},
                  config={"displayModeBar": False, "scrollZoom": False}),
        html.Div("OFI · 1s steps · price", style={"padding": "8px 8px 2px",
                 "fontSize": "10px", "color": "#787b86",
                 "fontFamily": "ui-monospace, monospace",
                 "borderTop": "1px solid #2a2e39"}),
        dcc.Graph(id="mobile-master-chart", figure=fig,
                  style={"height": f"{OFI_STRIP_HEIGHT}px"},
                  config={"displayModeBar": False, "scrollZoom": False}),
        html.Div(id="dom-table", children=table, style={"padding": "4px 8px 12px"}),
        dcc.Interval(id="mobile-pulse-clock", interval=REFRESH_RATE_MS, n_intervals=0)
    ]
    )

# =====================================================================
# UPDATE CALLBACK
# =====================================================================
# Updates run clientside, fetching /api/frame over GET, rather than through the
# server callback's POST to _dash-update-component. Both are ordinary Dash and
# behave identically where the POST path works.
app.clientside_callback(
    """
    function(n) {
        var blank = Array(10).fill(window.dash_clientside.no_update);

        // One request in flight at a time. The interval fires whether or not
        // the last frame arrived, so on a slow link requests otherwise pile up
        // and saturate the connection they are waiting on.
        if (window._ofBusy) { return blank; }
        window._ofBusy = true;

        // Bound each attempt, so a request that never settles releases the slot.
        var ctl = new AbortController();
        var timer = setTimeout(function () { ctl.abort(); }, %(timeout)d);

        // The width picks the footprint bar count: twelve bars overlap at 400px.
        var url = '/api/frame?w=' + Math.round(window.innerWidth || 0);
        return fetch(url, {cache: 'no-store', signal: ctl.signal})
            .then(function (r) { return r.ok ? r.json() : Promise.reject(r.status); })
            .then(function (d) {
                clearTimeout(timer);
                window._ofBusy = false;
                window._ofStall = 0;
                // A frame landed, so in-place updates work and the reload tag
                // is wrong. The server only infers this from its own callback
                // counter, which is per worker: under more than one gunicorn
                // worker the one rendering the page may never have served a
                // frame, so it emitted the tag forever and the page reloaded
                // every AUTO_REFRESH_SECONDS, throwing away the scroll
                // position each time. The browser knows for certain.
                if (window._ofReload) {
                    clearTimeout(window._ofReload);
                    window._ofReload = null;
                }
                return [d.ticker, d.ticker_style, d.figure,
                        d.ltp, d.ltp_style, d.ltp_delta, d.table, d.clock,
                        d.footprint, d.footprint_head];
            })
            .catch(function () {
                clearTimeout(timer);
                window._ofBusy = false;
                // The reload fallback stands down while frames are arriving, so
                // a client that stalls afterwards has nothing else to recover it.
                window._ofStall = (window._ofStall || 0) + 1;
                if (window._ofStall >= %(stall)d) { window._ofStall = 0; location.reload(); }
                return blank;
            });
    }
    """ % {"timeout": FETCH_TIMEOUT_MS, "stall": STALL_RELOAD},
    [Output("mobile-ticker-feed", "children"),
     Output("mobile-ticker-feed", "style"),
     Output("mobile-master-chart", "figure"),
     Output("ltp-value", "children"),
     Output("ltp-value", "style"),
     Output("ltp-delta", "children"),
     Output("dom-table", "children"),
     Output("frame-clock", "children"),
     Output("footprint-chart", "figure"),
     Output("footprint-head", "children")],
    [Input("mobile-pulse-clock", "n_intervals")],
)


def _safe_render(n, width=0):
    try:
        return _render(n, width)
    except Exception as exc:
        # Raising sends no update at all and the page sits on its last draw with
        # nothing to say why. Show the fault instead.
        mobile_pipeline.render_error = f"{type(exc).__name__}: {exc}"[:120]
        print(f"[RENDER ERROR] {type(exc).__name__}: {exc}", flush=True)
        msg = f"RENDER ERROR · {type(exc).__name__}: {exc}"[:140]
        return (msg, {"color": "#f23645", "fontSize": "11px"},
                waiting_figure(msg, "", "", OFI_STRIP_HEIGHT), "—",
                {"fontSize": "28px", "fontWeight": "bold", "color": "#787b86"}, "", None,
                waiting_figure(msg, "", ""), "FOOTPRINT · unavailable")


def _render(n, width=0):
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
        trade_error = mobile_pipeline.trade_error
        # A narrow viewport gets fewer bars: twelve of them overlap at 400px.
        want = FOOTPRINT_BARS_NARROW if 0 < width < NARROW_PX else FOOTPRINT_BARS
        fp_keys = sorted(mobile_pipeline.footprint)[-want:]
        bars = {k: {"levels": dict(mobile_pipeline.footprint[k]["levels"]),
                    **{f: mobile_pipeline.footprint[k][f]
                       for f in ("open", "high", "low", "close", "buy", "sell")}}
                for k in fp_keys}

    # Converted before anything is plotted: the series is kept in UTC and only
    # the axis reads in local time.
    times = [t.tz_convert(DISPLAY_TZ) for t in times]

    ltp_text, ltp_style, ltp_delta = format_ltp(ltp, prev_ltp)
    table = dom_table(bids, asks)
    # Row height from the visible span, holding the previous one where close.
    # A number in FOOTPRINT_TICK overrides it and is used exactly as given.
    if FIXED_TICK:
        tick = FIXED_TICK
    else:
        lows = [b["low"] for b in bars.values()]
        highs = [b["high"] for b in bars.values()]
        held = mobile_pipeline.fp_tick.get(want)
        if len(bars) < FP_TICK_MIN_BARS:
            # Too few bars to measure a range from; size off the price instead
            # and do not store it, so the first real span still decides.
            seed = highs[-1] if highs else (mobile_pipeline.trade_price or 0.0)
            if held: tick = held                    # a real span already decided
            elif seed > 0: tick = nice_tick(seed * FP_TICK_SEED)
            else: tick = 1.0
        else:
            span = max(highs) - min(lows)
            tick = choose_tick(span, held)
            mobile_pipeline.fp_tick[want] = tick
    stale = (time.time() - mobile_pipeline.last_trade) if mobile_pipeline.last_trade else 0.0
    fp_fig, fp_head = footprint_figure(bars, trade_error, tick, stale)

    if not times:
        return (f"WAITING · {ws_state}", {"color": "#db8c02"},
                waiting_figure(ws_state, rest_error, ltp_error, OFI_STRIP_HEIGHT),
                ltp_text, ltp_style, ltp_delta, table, fp_fig, fp_head)

    last_price = cl[-1]
    ticker_color = "#089981" if ofi_steps_list[-1] >= 0 else "#f23645"
    ticker_text = f"P: ${last_price:,.1f} | OFI(D): {current_ofi:+,.0f}"

    # Without this a dead feed looks like a quiet market: the page keeps
    # redrawing the same last candle.
    if age is not None and age > STALE_AFTER:
        ticker_text += f" | STALE {age:,.0f}s"
        if rest_error: ticker_text += f" | REST {rest_error}"
        ticker_color = "#db8c02"

    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True,
        vertical_spacing=0.04, row_heights=[0.62, 0.38]
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
        height=OFI_STRIP_HEIGHT,
        margin=dict(l=8, r=40, t=5, b=18),
        showlegend=False,
        dragmode=False,
        uirevision='constant'
    )

    pad = pd.Timedelta(seconds=1)
    xr = [times[0] - pad, times[-1] + pad]
    fig.update_xaxes(showgrid=True, gridcolor="#2a2e39", showticklabels=False,
                     range=xr, fixedrange=True, row=1, col=1)
    fig.update_xaxes(showgrid=True, gridcolor="#2a2e39", tickfont=dict(size=8),
                     range=xr, fixedrange=True, row=2, col=1)

    fig.update_yaxes(
        showgrid=True, gridcolor="#2a2e39",
        side="right", tickfont=dict(size=8),
        autorange=True, fixedrange=True, row=1, col=1
    )
    fig.update_yaxes(
        showgrid=True, gridcolor="#2a2e39",
        side="right", tickfont=dict(size=8),
        autorange=True, fixedrange=True, row=2, col=1
    )

    return (ticker_text, {"color": ticker_color}, fig,
            ltp_text, ltp_style, ltp_delta, table, fp_fig, fp_head)

# Assigned after the callback, not beside serve_layout: Dash evaluates the
# callable immediately and it renders through _safe_render above.
app.layout = serve_layout


# =====================================================================
# LOCAL RUN
# =====================================================================
if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=int(os.environ.get("PORT", 8050)))