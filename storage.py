"""Durable storage for completed candles.

Everything the dashboard collects lives in memory, so a restart - a deploy, a
crash, a free instance spinning down - loses it. This writes each completed
second to Postgres and reads the series back on startup, so the chart resumes
and cumulative OFI keeps accumulating across restarts.

Writes are queued and flushed in batches by a background thread, so a slow or
unreachable database can never stall ingestion. Nothing here raises into the
caller, and with DATABASE_URL unset the whole module is inert.
"""

import os
import queue
import threading
import time

try:
    import psycopg
except ImportError:                      # the app must still run without the driver
    psycopg = None

BATCH = 30              # Rows per insert
FLUSH_SECONDS = 5.0     # Write a partial batch rather than holding rows forever
QUEUE_MAX = 5000        # Bounded: a database outage must not grow memory without limit
RETENTION_HOURS = 48    # Second resolution kept this long, then rolled up to minutes
PRUNE_EVERY = 3600      # Roll up and prune once an hour

SCHEMA = """
CREATE TABLE IF NOT EXISTS candles (
    symbol    text        NOT NULL,
    ts        timestamptz NOT NULL,
    open      double precision,
    high      double precision,
    low       double precision,
    close     double precision,
    ofi_step  double precision,
    ofi_cum   double precision,
    PRIMARY KEY (symbol, ts)
);
-- One row per minute per symbol. Levels are the exact traded prices, not
-- display rows: pre-bucketing would cap how fine any later analysis can go,
-- and 1m rolls up to 3/5/15/30/60 exactly because every field of a footprint
-- is additive or associative. JSONB rather than a row per level because the
-- same data costs 7MB a day this way and 25MB the other.
CREATE TABLE IF NOT EXISTS footprint_1m (
    symbol    text        NOT NULL,
    ts        timestamptz NOT NULL,
    open      double precision,
    high      double precision,
    low       double precision,
    close     double precision,
    buy       double precision,
    sell      double precision,
    levels    jsonb,
    PRIMARY KEY (symbol, ts)
);
-- Signals are written when they fire and resolved later, so what is stored is
-- a forward test: the outcome was not known when the row was created. Scanning
-- stored history for rules instead would fit the noise.
CREATE TABLE IF NOT EXISTS signals (
    symbol    text        NOT NULL,
    ts        timestamptz NOT NULL,
    kind      text        NOT NULL,
    horizon_m integer     NOT NULL,
    price     double precision,
    delta     double precision,
    volume    double precision,
    fwd_price double precision,
    fwd_move  double precision,
    resolved  boolean     NOT NULL DEFAULT false,
    PRIMARY KEY (symbol, ts, kind, horizon_m)
);
CREATE INDEX IF NOT EXISTS signals_open ON signals (symbol, resolved, ts);
CREATE TABLE IF NOT EXISTS candles_1m (
    symbol    text        NOT NULL,
    ts        timestamptz NOT NULL,
    open      double precision,
    high      double precision,
    low       double precision,
    close     double precision,
    ofi_step  double precision,
    ofi_cum   double precision,
    PRIMARY KEY (symbol, ts)
);
"""

INSERT = """
INSERT INTO candles (symbol, ts, open, high, low, close, ofi_step, ofi_cum)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (symbol, ts) DO UPDATE SET
    high = GREATEST(candles.high, EXCLUDED.high),
    low  = LEAST(candles.low, EXCLUDED.low),
    close = EXCLUDED.close,
    ofi_step = EXCLUDED.ofi_step,
    ofi_cum = EXCLUDED.ofi_cum
"""

# Fold expiring seconds into minute bars before deleting them, so history thins
# with age instead of disappearing.
ROLLUP = """
INSERT INTO candles_1m (symbol, ts, open, high, low, close, ofi_step, ofi_cum)
SELECT symbol,
       date_trunc('minute', ts),
       (array_agg(open  ORDER BY ts))[1],
       max(high),
       min(low),
       (array_agg(close ORDER BY ts DESC))[1],
       sum(ofi_step),
       (array_agg(ofi_cum ORDER BY ts DESC))[1]
FROM candles
WHERE symbol = %s AND ts < now() - make_interval(hours => %s)
GROUP BY symbol, date_trunc('minute', ts)
ON CONFLICT (symbol, ts) DO NOTHING
"""

PRUNE = "DELETE FROM candles WHERE symbol = %s AND ts < now() - make_interval(hours => %s)"

FP_INSERT = """
INSERT INTO footprint_1m (symbol, ts, open, high, low, close, buy, sell, levels)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (symbol, ts) DO UPDATE SET
    high = GREATEST(footprint_1m.high, EXCLUDED.high),
    low  = LEAST(footprint_1m.low, EXCLUDED.low),
    close = EXCLUDED.close,
    buy = EXCLUDED.buy,
    sell = EXCLUDED.sell,
    levels = EXCLUDED.levels
"""

FP_RECENT = """
SELECT ts, open, high, low, close, buy, sell, levels
FROM footprint_1m WHERE symbol = %s AND ts >= %s ORDER BY ts
"""

SIG_INSERT = """
INSERT INTO signals (symbol, ts, kind, horizon_m, price, delta, volume)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (symbol, ts, kind, horizon_m) DO NOTHING
"""

SIG_OPEN = """
SELECT ts, kind, horizon_m, price FROM signals
WHERE symbol = %s AND NOT resolved AND ts < %s ORDER BY ts LIMIT 500
"""

SIG_RESOLVE = """
UPDATE signals SET fwd_price = %s, fwd_move = %s, resolved = true
WHERE symbol = %s AND ts = %s AND kind = %s AND horizon_m = %s
"""

# Sample size first. A hit rate on nine occurrences is not a hit rate.
SIG_STATS = """
SELECT kind, horizon_m, count(*) AS n,
       avg(fwd_move) AS mean_move,
       stddev_samp(fwd_move) AS sd_move,
       avg(CASE WHEN fwd_move > 0 THEN 1.0 ELSE 0.0 END) AS hit_rate
FROM signals WHERE symbol = %s AND resolved
GROUP BY kind, horizon_m ORDER BY kind, horizon_m
"""

# Close of the 1m bar nearest at or after a target time, for resolving.
FP_PRICE_AT = """
SELECT ts, close FROM footprint_1m
WHERE symbol = %s AND ts >= %s ORDER BY ts LIMIT 1
"""

RECENT = """
SELECT ts, open, high, low, close, ofi_step, ofi_cum
FROM candles WHERE symbol = %s ORDER BY ts DESC LIMIT %s
"""


class Storage:
    def __init__(self, dsn, symbol):
        self.dsn = dsn or ""
        self.symbol = symbol
        self.enabled = bool(self.dsn) and psycopg is not None
        self.error = ""
        self.written = 0
        self.dropped = 0
        self.fp_written = 0
        self.signals_found = 0
        self._q = queue.Queue(maxsize=QUEUE_MAX)
        self._conn = None
        self._started = False
        self._lock = threading.Lock()

        if dsn and psycopg is None:
            self.error = "psycopg not installed"
            print("[DB] DATABASE_URL is set but psycopg is not installed", flush=True)

    # -- connection ------------------------------------------------------
    def _connect(self):
        """Return a live connection, reconnecting if the previous one died."""
        if self._conn is not None and not self._conn.closed:
            return self._conn
        self._conn = psycopg.connect(self.dsn, autocommit=True, connect_timeout=10)
        with self._conn.cursor() as cur:
            cur.execute(SCHEMA)
        return self._conn

    def start(self):
        """Idempotent, and safe to call from every worker process."""
        if not self.enabled:
            return
        with self._lock:
            if self._started:
                return
            self._started = True
        threading.Thread(target=self._writer, name="db-writer", daemon=True).start()

    # -- write path ------------------------------------------------------
    def record(self, ts, o, h, l, c, ofi_step, ofi_cum):
        """Queue a finished candle. Never blocks, never raises."""
        if not self.enabled:
            return
        try:
            self._q.put_nowait((self.symbol, ts, o, h, l, c, ofi_step, ofi_cum))
        except queue.Full:
            self.dropped += 1       # prefer losing history to stalling the feed

    def _writer(self):
        last_prune = time.time()
        while True:
            batch = self._drain()
            if batch:
                self._flush(batch)
            if time.time() - last_prune > PRUNE_EVERY:
                last_prune = time.time()
                self._prune()

    def _drain(self):
        """Collect up to BATCH rows, waiting no longer than FLUSH_SECONDS."""
        batch = []
        deadline = time.time() + FLUSH_SECONDS
        while len(batch) < BATCH:
            timeout = deadline - time.time()
            if timeout <= 0:
                break
            try:
                batch.append(self._q.get(timeout=timeout))
            except queue.Empty:
                break
        return batch

    def _flush(self, batch):
        try:
            conn = self._connect()
            with conn.cursor() as cur:
                cur.executemany(INSERT, batch)
            self.written += len(batch)
            self.error = ""
        except Exception as exc:
            self.error = f"{type(exc).__name__}: {exc}"[:120]
            print(f"[DB ERROR] write failed, {len(batch)} rows lost: {exc}", flush=True)
            self._reset()

    def _prune(self):
        try:
            conn = self._connect()
            with conn.cursor() as cur:
                cur.execute(ROLLUP, (self.symbol, RETENTION_HOURS))
                cur.execute(PRUNE, (self.symbol, RETENTION_HOURS))
                print(f"[DB] rolled up and pruned {cur.rowcount} rows older than "
                      f"{RETENTION_HOURS}h", flush=True)
        except Exception as exc:
            self.error = f"{type(exc).__name__}: {exc}"[:120]
            print(f"[DB ERROR] prune failed: {exc}", flush=True)
            self._reset()

    def _reset(self):
        try:
            if self._conn is not None:
                self._conn.close()
        except Exception:
            pass
        self._conn = None

    # -- footprint and signals -------------------------------------------
    # These go straight to the database rather than through the candle queue:
    # they are written once a minute and once an hour, not once a second, and
    # the caller is already a background thread. Still never raises.
    def record_footprint(self, ts, bar, levels_json):
        if not self.enabled:
            return False
        try:
            conn = self._connect()
            with conn.cursor() as cur:
                cur.execute(FP_INSERT, (self.symbol, ts, bar["open"], bar["high"],
                                        bar["low"], bar["close"], bar["buy"],
                                        bar["sell"], levels_json))
            self.fp_written += 1
            return True
        except Exception as exc:
            self.error = f"{type(exc).__name__}: {exc}"[:120]
            print(f"[DB ERROR] footprint write failed: {exc}", flush=True)
            self._reset()
            return False

    def load_footprint(self, since):
        """1m bars at or after `since`, oldest first. Empty on any failure."""
        if not self.enabled:
            return []
        try:
            conn = self._connect()
            with conn.cursor() as cur:
                cur.execute(FP_RECENT, (self.symbol, since))
                return cur.fetchall()
        except Exception as exc:
            self.error = f"{type(exc).__name__}: {exc}"[:120]
            print(f"[DB ERROR] footprint load failed: {exc}", flush=True)
            self._reset()
            return []

    def record_signal(self, ts, kind, horizon_m, price, delta, volume):
        """Insert a signal. Returns True only if this one was new."""
        if not self.enabled:
            return False
        try:
            conn = self._connect()
            with conn.cursor() as cur:
                cur.execute(SIG_INSERT, (self.symbol, ts, kind, horizon_m,
                                         price, delta, volume))
                return cur.rowcount > 0
        except Exception as exc:
            self.error = f"{type(exc).__name__}: {exc}"[:120]
            print(f"[DB ERROR] signal write failed: {exc}", flush=True)
            self._reset()
            return False

    def open_signals(self, before):
        if not self.enabled:
            return []
        try:
            conn = self._connect()
            with conn.cursor() as cur:
                cur.execute(SIG_OPEN, (self.symbol, before))
                return cur.fetchall()
        except Exception as exc:
            self.error = f"{type(exc).__name__}: {exc}"[:120]
            self._reset()
            return []

    def price_at(self, when):
        """Close of the first 1m bar at or after `when`, or None."""
        if not self.enabled:
            return None
        try:
            conn = self._connect()
            with conn.cursor() as cur:
                cur.execute(FP_PRICE_AT, (self.symbol, when))
                row = cur.fetchone()
            return row[1] if row else None
        except Exception as exc:
            self.error = f"{type(exc).__name__}: {exc}"[:120]
            self._reset()
            return None

    def resolve_signal(self, ts, kind, horizon_m, fwd_price, fwd_move):
        if not self.enabled:
            return
        try:
            conn = self._connect()
            with conn.cursor() as cur:
                cur.execute(SIG_RESOLVE, (fwd_price, fwd_move, self.symbol,
                                          ts, kind, horizon_m))
        except Exception as exc:
            self.error = f"{type(exc).__name__}: {exc}"[:120]
            self._reset()

    def signal_stats(self):
        if not self.enabled:
            return []
        try:
            conn = self._connect()
            with conn.cursor() as cur:
                cur.execute(SIG_STATS, (self.symbol,))
                return cur.fetchall()
        except Exception as exc:
            self.error = f"{type(exc).__name__}: {exc}"[:120]
            self._reset()
            return []

    # -- read path -------------------------------------------------------
    def load_recent(self, limit):
        """Most recent candles, oldest first. Empty list on any failure."""
        if not self.enabled:
            return []
        try:
            conn = self._connect()
            with conn.cursor() as cur:
                cur.execute(RECENT, (self.symbol, limit))
                rows = cur.fetchall()
            return list(reversed(rows))
        except Exception as exc:
            self.error = f"{type(exc).__name__}: {exc}"[:120]
            print(f"[DB ERROR] load failed: {exc}", flush=True)
            self._reset()
            return []
