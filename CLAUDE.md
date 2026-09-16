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
- `REFRESH_RATE_MS` — browser update interval, default 500.
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

## Known rough edges (left deliberately — do not "fix" unprompted)

- One data point is appended per websocket message, not per second, so the `1S` label
  and the 40-point window are message-based rather than time-based.
- Candles are synthesised from consecutive mid prices, so they are degenerate by
  construction.
