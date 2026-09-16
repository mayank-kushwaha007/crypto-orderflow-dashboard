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
- `AUTO_REFRESH_SECONDS` — page auto-reload interval, default 5, `0` disables. The
  layout is rendered per request, so a reload is a real refresh; this drives live
  updates where the in-place callback is not reaching the browser.
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

## Timestamps

Chart timestamps are the server's clock, which is UTC on Render. A viewer in
IST sees candles 5h30m "behind" their phone; that is the timezone, not stale data.

## Known rough edges (left deliberately — do not "fix" unprompted)

- One data point is appended per websocket message, not per second, so the `1S` label
  and the 40-point window are message-based rather than time-based.
- Candles are synthesised from consecutive mid prices, so they are degenerate by
  construction.
