# crypto-orderflow-dashboard

A Dash app for order flow imbalance (OFI) and DOM level-2 order cluster analysis,
fed by Delta Exchange's public `l2_updates` websocket channel. Deployed on Render.

- `main.py` — websocket and REST ingestion, OFI accounting, Dash layout and callback.
- `storage.py` — optional Postgres persistence for completed candles.
- `requirements.txt` — dependencies (unpinned).
- No test suite, no CI.

## Environment

- `DATABASE_URL` — Postgres connection string. **Unset is a supported mode**: storage
  goes inert and the app behaves exactly as it did before. Never make persistence
  load-bearing for the live chart.
- `AUTO_REFRESH_SECONDS` — fallback reload interval, default 5, `0` disables. Armed
  **only while the update callback is not arriving**, and cleared by the first frame
  that lands. It is a `setTimeout`, **not a `<meta http-equiv="refresh">`**: once a
  browser has parsed a meta refresh it is armed and cannot be called off — removing the
  element does nothing — so a page that turned out to be updating fine still reloaded
  every 5s and threw the reader back to the top. Do not put the tag back.
- `REFRESH_RATE_MS` — browser update interval, default 5000. A gzipped frame is ~1.6KB,
  so this needs about 0.32KB/s, roughly 1.2MB/hour; 1000 is five times both. Two values
  derive from it and must keep their relationship: `FETCH_TIMEOUT_MS` is four intervals
  capped at 10s, and `CALLBACK_FRESH` is two intervals — shorter than one interval and
  `callbacks_arriving()` flaps between ticks, re-arming the reload tag on a page that is
  updating perfectly well.
- `FOOTPRINT_BUCKET` — footprint bar period, default `5min`. `FOOTPRINT_BARS` (12) and
  `FOOTPRINT_BARS_NARROW` (5) are the bar counts above and below `NARROW_PX` (600) of
  viewport width.
- `FOOTPRINT_TICK` — price row height, default `auto`: sized from what the instrument
  actually did, so the chart reads the same on a $77,000 future and a $0.60 alt. A
  number here overrides it and is used exactly as given. `FOOTPRINT_ROWS_TARGET` (14) is
  what auto aims for, `TICK_HYSTERESIS` (1.5) how far the ideal must drift before the
  grid re-snaps, `FOOTPRINT_MAX_LEVELS` (5000) the distinct prices one bar will hold.
- `SIGNAL_EVERY` (3600) / `SIGNAL_BUCKET` (`15min`) / `SIGNAL_LOOKBACK_H` (48) /
  `SIGNAL_HORIZONS` (`15,60`) / `SIGNAL_MIN_N` (30) — the hourly scan. `FP_RECORD_EVERY`
  (20) is the write sweep, `FP_MINUTE_KEEP` (240) the 1m bars held in memory.
- `FP_TICK_MIN_BARS` (3) / `FP_TICK_SEED` (0.0005) — below that many bars there is no
  range worth measuring, so the row height comes from the price level instead. One bar
  of a quiet minute sized rows at `$2` on an `$84,858` instrument.
- `FP_IMBALANCE` (0.35) / `FP_ABSORB_POS` (0.35) — thresholds for the per-bar read in
  `read_bar`. Judgement calls, never backtested here; retune per instrument.
- `FOOTPRINT_HEIGHT` (620) / `OFI_STRIP_HEIGHT` (190) — the footprint is the chart being
  read, so it is first and tall; the OFI panel sits under it as a strip. The footprint
  already draws the candles, so the strip is there for the OFI bar and whether it agrees.
- `DISPLAY_TZ` — timezone for chart axis labels, default `Asia/Kolkata`. Display only.
- `RENDER_EXTERNAL_URL` — set by Render; the keepalive requests it every 10 minutes.
  Only inbound traffic resets Render's idle timer, so calls to the exchange do not
  keep the instance up.

## Working agreement

The repository owner set this up deliberately. Follow it unless they say otherwise.

**Agree the change, then ship it end to end.**

1. **Before changing logic, explain and get a yes.** Describe what is wrong, what you
   would change, and show the diff. Do not alter trading logic, OFI maths or chart
   behaviour on your own initiative. This applies to logic — not to the mechanics of
   shipping it.
2. **Once they agree, do the whole delivery without asking again**: commit, push,
   open the pull request, merge it. No "shall I push?", no "here's the link to click".
   Report what landed when it is done.
3. Render auto-deploys from `main`, so a merge is a deploy to their live site.
   Treat every merge as reaching production.

## Verifying before you push

There are no tests, so nothing will catch a mistake after the fact. Verify by
replaying synthetic Delta-shaped messages through `on_message` and asserting the
pipeline fills, then confirm the Dash callback returns a real figure rather than the
`BUFFERING ENGINE...` placeholder. Cover at least: `action=snapshot`, an incremental
size change, a size-0 deletion, and a malformed level.

Reproduce the reported failure first, then show the same case passing.

Outbound network access to `delta.exchange` is blocked from Claude Code sandboxes,
so the live feed cannot be reached from here. Anything endpoint-specific has to be
confirmed from the Render logs instead — do not guess at socket URLs.

## Things that have already bitten this project

- **Missing index subscripts.** A Delta L2 level is `["price", "size"]`, or an object
  with `limit_price`/`size`. `float()` on the level itself raises `TypeError`.
  `websocket-client` swallows exceptions raised inside `on_message`, logs them and
  keeps the socket open — so the app looks connected while discarding every message,
  and the chart sits on `BUFFERING ENGINE...` forever. If data never arrives, read the
  Render logs before touching anything else.
- **`app.run_server()` was removed in Dash 3.x.** Use `app.run()`.
- **Multiple gunicorn workers each keep their own order book** and their own websocket
  connection, so consecutive refreshes read from different books. This app wants
  `--workers 1`.
- **Threads do not survive `fork()`.** Under `gunicorn --preload` the module is
  imported in the master and the workers are forked from it, so threads started at
  import exist only in the master and every worker serves a frozen snapshot. Start
  them through `ensure_workers()`, which the callback and `/health` both call.
- **Liveness must not depend on the component that fails.** The REST poller once
  stood down whenever the socket had "recently" delivered, so a socket that went
  quiet froze the book while the poller judged it healthy.
- **A parseable response with no levels used to stall the feed in silence.** The
  poller cleared the book, applied nothing, and set `rest_error = ""`. An empty book
  makes `update_metrics` return before it touches `last_update`, so the chart went
  stale for hours with no error in the ticker, in `/health`, or in the logs. Both the
  poller and the `l2_orderbook` handler now parse into a scratch book and keep the
  last good one unless both sides come back non-empty. **Never clear the live book
  before the replacement has parsed.**

## Conventions

- Match the existing style in `main.py`, including its single-line `if x: ...` bodies
  and banner comments. Do not reformat surrounding code as part of a fix.
- Never commit `__pycache__/` or `.pyc` files.
- Keep each change minimal and scoped to what was agreed.

## The footprint needs trades, not the book

The `l2_*` channels carry **resting orders**. A footprint is per-price-row executed
volume split by which side the aggressor was on, so it is built from a separate feed:
`GET /v2/trades/{symbol}`, public and unauthenticated (it is `get_public_trades()` in
Delta's own REST client), polled by the `trades` worker. The websocket `all_trades` /
`trades` channels are subscribed as well and handled if they arrive.

**The trade schema is not confirmed.** `docs.delta.exchange` is egress-blocked from
Claude Code sandboxes, so `parse_trade` accepts every plausible spelling — `buyer_role`,
`seller_role`, `side`, `is_buyer_maker` — and `trade_time` accepts seconds through
nanoseconds. The first raw payload is printed once as `[TRADES] first raw payload:`.
**Read it in the Render logs and narrow the parser to what the venue actually sends**
rather than leaving it guessing forever.

A REST poll returns a window overlapping the previous one, so `ingest_trades` dedupes on
the trade id, or on `(timestamp, price, size, side)` where there is none. Without it
every poll would inflate the footprint by whatever it re-read.

Candle prices come from trades while `trades_fresh()` holds and from the book mid
otherwise, so the chart keeps drawing when the trade feed is the component that fails.
`touch_candle()` is the single owner of the 1s candle; OFI is computed from the book in
`update_metrics()` and is unaffected by any of this.

`footprint_figure`'s docstring carries the note on what to infer from the chart —
absorption, imbalance, point of control, delta divergence, exhaustion. Keep it there;
it is the part a reader of this code most needs and least gets from the code itself.

**Levels are stored at the exact traded price and bucketed into rows only when the
chart is drawn.** Bucketing at ingest would bake the row height into the history, so a
tick change would strand old bars on the old grid; `bucket_levels` at draw time
re-buckets everything at once. Cost is the distinct prices a bar holds, capped by
`FOOTPRINT_MAX_LEVELS` — past the cap level accounting is skipped but the rest of
`record_trade` still runs, because the candle and `trades_fresh()` depend on it.

`choose_tick` compares the **unsnapped** ideal row height against the one in use.
Snapping first makes the threshold meaningless: two snapped values are already a whole
rung apart, so any drift over a rung boundary clears any ratio. The held tick is kept
per bar count, since a phone and a laptop see different spans and must not fight over
one value.

Both charts have `fixedrange=True` on every axis, `scrollZoom: False` **and
`dragmode=False`**. `fixedrange` alone was not enough: Plotly still installs its touch
drag layer, which swallowed the swipe, so the page could not be scrolled past a chart
on a phone.

Scroll position is saved to `sessionStorage` and restored by `SCROLL_KEEP`, re-applied
as the graphs render because the page is not full height until then. Both reloads here
are involuntary — the fallback timer and the stall recovery — and losing the reader's
place on a phone costs more than the reload buys.

The footprint header says when the feed is dead (`NO TRADES FOR 3.7h · showing 1
bar(s)`) or still filling. Stale bars otherwise sit there looking current, and the
count is what tells you a thin chart is a thin feed rather than a layout change.

`read_bar` gives each bar a short label under its time on the axis, and flags the two
cases worth stopping on with a mark above the bar: absorption (one side clearly the
aggressor, price closed at the opposite end) and divergence (a higher high on weaker
buying, or a lower low on weaker selling). Absorption outranks divergence on the same
bar. **These are prompts to look, not signals**: conventional readings with hand-picked
thresholds, never backtested in this repo.

The footprint is a `go.Heatmap` — one trace for the whole grid, with `texttemplate`
putting `sell x buy` inside each cell, shortened by `fp_num` to `1.5k` above a thousand
because a column is ~70px at phone width and a full-width pair runs over the price axis. Its x axis is categorical, so `go.Candlestick`
cannot share it and the bodies and wicks are `go.Scatter` segments. This is the
arrangement the public OrderflowChart project uses, for the same reason. Its bottom
margin is 22px, not the main chart's 5, or the bar times clip.

## Recording and the signal scan

Footprints are recorded at **1 minute**, whatever `FOOTPRINT_BUCKET` displays. The
display bucket is a display choice; what is stored must not be. 1m rolls up to
3/5/15/30/60 **exactly** — `merge_bars` sums levels per price, maxes the high, mins the
low, takes the first open and last close, and every one of those is additive or
associative, so a 15m bar merged from 15 stored 1m bars is the same bar as one built
from the trades. That is proven in the scratchpad rollup test, not assumed. Aggregation
is one-way: nothing recovers a finer bucket than the one stored.

Levels are stored as JSONB keyed by the **exact traded price**. Pre-bucketing to
display rows would cap how fine any later analysis could go. One row per bar rather
than one per level: measured at ~7MB/day against ~25MB/day.

`record_minutes` writes completed bars on a sweep, never on the ingest path —
`record_trade` holds the lock and a slow database must never stall the feed. The newest
bar is left alone because it is still open, and a failed write keeps `saved=False` for
the next sweep.

`scan_signals` runs every `SIGNAL_EVERY`, rolls the stored minutes up to
`SIGNAL_BUCKET`, writes a row for each flagged bar, then resolves rows old enough to
have an answer against the price that actually came. **What accumulates is a forward
test**: the outcome was not known when the row was written. Scanning stored history for
whichever rule looks best would fit the noise, and on one instrument over a few days it
would always find something. `fwd_move` is in basis points, signed so positive means
the read was right. Below `SIGNAL_MIN_N` resolved occurrences the scan reports
`too few (n/N)` instead of a hit rate. **Keep it that way** — the number is what
invites belief, and belief is what this is meant to withhold until it is earned.

Both workers retire themselves when `DATABASE_URL` is unset, and every storage call is
a clean no-op, so the live chart is unchanged.

## OFI semantics

Cumulative OFI is a **UTC daily session total**, not a since-startup figure. It
resets at 00:00 UTC, and a restart resumes the stored total only within the same
UTC day. The header labels it `OFI(D)`. Per-second OFI steps are unaffected.

## How updates reach the browser

A clientside callback on the interval fetches `/api/frame` over **GET** and writes the
result into the page. The server callback's POST to `_dash-update-component` was not
reaching the Render deployment, while every GET did; this is ordinary Dash either way
and behaves identically where that path works.

`/api/frame` must serialise with `PlotlyJSONEncoder`, which is what Dash uses for
layouts. `to_plotly_json()` converts only the outermost component, and a `default=str`
fallback then turns the children into text, so the table arrives as a string.

Every text response is gzipped by an `after_request` hook, not just the frame. When
only the frame was compressed a page reload cost ~26KB — 16KB of HTML plus 9KB of
layout — which is nine seconds on a 3KB/s link, and the stall recovery reloads the
page, so the recovery manufactured the gaps it was meant to repair. Compressed the
same reload is ~6.8KB, about two seconds.

The frame is gzipped, which matters more than it sounds: the ladder repeats the same
inline style on all 40 cells, so 21KB of JSON compresses to about 1.6KB. On a 4KB/s
mobile link that is 0.4s per frame rather than 5.2s, which is the difference between
updating and appearing frozen. The static Plotly template is stripped for the same
reason — it is ~8KB, identical every frame, and already established by the first render.

The fetch keeps one request in flight at a time and aborts after `FETCH_TIMEOUT_MS`.
The interval fires whether or not the last frame arrived, so without the guard requests
pile up on a slow link and saturate the connection they are waiting on — a twelve second
stall produced twenty-four overlapping fetches in a browser test, four with the guard.
A hung fetch does **not** freeze Dash permanently; that was tested against the code
without the timeout and it recovered too, so do not reach for that explanation.

The header carries a `frame-clock` showing when the server built the frame. It advances
only when a frame actually lands, which separates a stalled feed from a stalled
transport at a glance.

## Diagnosing the update path

The running commit is reported as `GIT_COMMIT`, from Render's `RENDER_GIT_COMMIT`, in
`/health` and in the header badge. Compare it against `main` to tell whether a deploy
actually landed, rather than inferring it from behaviour.

The header carries a `cb N · Xs · <commit>` badge, rendered server-side on every page load:
callbacks received from the browser, and how long since the last one. `cb 0 · never`
means the browser's POSTs are not arriving at all; a rising count means Dash is
updating in place. Only the Dash callback increments it — `serve_layout` renders the
same view server-side and must never count, or the badge reports updates as arriving
when the browser has sent nothing.

## Timestamps

Candles are kept in UTC throughout — in memory and in Postgres — and converted to
`DISPLAY_TZ` (default `Asia/Kolkata`) only when the axis is drawn. Keep it that
way: storing local time makes stored data ambiguous across DST and deployments.

## Where a frame's time goes

Measured, median of 50, before the footprint: ingest and OFI aggregation 0.46ms (0.9%),
building the Plotly figure 44.6ms (92.4%), JSON 3.0ms, gzip 0.2ms. The footprint adds
30-60ms to the build and takes the gzipped frame from 1.6KB to 2.5KB at 5 bars and
3.1KB at 12 — about a second on a 3KB/s link, against a 5s cadence. The aggregation is scalar arithmetic on
a handful of floats — numba or similar would optimise under 1% of the work. If frame
build ever needs to be faster, the target is Plotly object construction: consolidating
the sixteen per-level DOM scatter traces into two measured 24.5ms to 19.4ms.

Server CPU has never been the cause of a stall. At ~48ms a frame it cannot produce a
gap of seconds; look at transport and at reload cost instead.

## Known rough edges (left deliberately — do not "fix" unprompted)

- One data point is appended per websocket message, not per second, so the `1S` label
  and the 40-point window are message-based rather than time-based.
- The footprint starts empty after a restart and takes `FOOTPRINT_BUCKET` × bars of
  trading to fill. It is not persisted; only candles are.
