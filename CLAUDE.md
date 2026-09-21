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
- `AUTO_REFRESH_SECONDS` — fallback reload interval, default 5, `0` disables. The tag
  is emitted **only while the update callback is not arriving**, so where Dash works
  normally — a laptop, a local run — it is absent and the page updates in place. The
  decision is per page load and corrects itself in both directions.
- `REFRESH_RATE_MS` — browser update interval, default 5000. A gzipped frame is ~1.6KB,
  so this needs about 0.32KB/s, roughly 1.2MB/hour; 1000 is five times both. Two values
  derive from it and must keep their relationship: `FETCH_TIMEOUT_MS` is four intervals
  capped at 10s, and `CALLBACK_FRESH` is two intervals — shorter than one interval and
  `callbacks_arriving()` flaps between ticks, re-arming the reload tag on a page that is
  updating perfectly well.
- `FOOTPRINT_BUCKET` / `FOOTPRINT_TICK` — footprint bar period and price row height,
  default `5min` and `50`. `FOOTPRINT_BARS` (12) and `FOOTPRINT_BARS_NARROW` (5) are the
  bar counts above and below `NARROW_PX` (600) of viewport width.
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

The footprint is a `go.Heatmap` — one trace for the whole grid, with `texttemplate`
putting `sell x buy` inside each cell, shortened by `fp_num` to `1.5k` above a thousand
because a column is ~70px at phone width and a full-width pair runs over the price axis. Its x axis is categorical, so `go.Candlestick`
cannot share it and the bodies and wicks are `go.Scatter` segments. This is the
arrangement the public OrderflowChart project uses, for the same reason. Its bottom
margin is 22px, not the main chart's 5, or the bar times clip.

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
