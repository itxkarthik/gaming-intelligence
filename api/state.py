"""Shared process state for the API: connections, background feeds, readers.

Both main.py (REST/WS) and dashboard.py (HTML views + Datastar SSE) import
from here — one Kafka event feed, one offset sampler, one alert listener, one
async Redis client and one asyncpg pool per process, and ONE set of Redis
readers so the REST API and the dashboard can never disagree.

Started/stopped by the FastAPI lifespan hook in main.py (nothing runs at
import time).
"""

import asyncio
import itertools
import json
import logging
import os
import threading
import time
from collections import deque
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

import asyncpg
import redis
import redis.asyncio as aioredis
from kafka import KafkaConsumer, TopicPartition

log = logging.getLogger("api.state")

VERSION = "1.4"  # product version: OpenAPI `version` and the UI brand badge

# ─── Configuration ──────────────────────────────────────────────────────────

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_TIMEOUT_SECONDS = float(os.getenv("REDIS_TIMEOUT_SECONDS", "2"))
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
# The pipeline's input topics: the live feed reads them and the throughput
# KPIs count only them (the `alerts` topic is pipeline OUTPUT).
FEED_TOPICS = ("gameplay_events", "player_events", "server_metrics")

# PostgreSQL: alert history written by the Go alert engine (Phase 4).
# Defaults match the compose network; host-run needs POSTGRES_PORT=5433
# (docker-compose maps 5433->5432 to avoid a host port collision).
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "gaming")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "gaming_dev")
POSTGRES_DB = os.getenv("POSTGRES_DB", "gaming_platform")
PG_DSN = (f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
          f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")

HEARTBEAT_SECONDS = 15

# ─── Shared thresholds / contracts (Spark writers + Go engine) ──────────────
# server_health writes `status` HEALTHY (>=80) / DEGRADED (>=50) / CRITICAL;
# the dashboard colours by that field so it can never disagree with Spark.
SERVER_HEALTHY = "HEALTHY"
SERVER_STATUS_CLASS = {"HEALTHY": "ok", "DEGRADED": "warn", "CRITICAL": "bad"}
# cheat_detection: flag at suspicion_effective >= 0.70; >= 0.85 is CRITICAL.
SUSPICION_FLAG = 0.70
SUSPICION_CRITICAL = 0.85
# match_quality: BALANCED >= 70 / UNBALANCED >= 40 / STOMPED < 40.
MATCH_BALANCED_MIN = 70.0
MATCH_UNBALANCED_MIN = 40.0
# A match hash rewritten within this window counts as "recently scored"
# (session windows emit once the session closes — this is NOT "live").
MATCH_RECENT_SECONDS = 90
# A server_metrics sample older than this no longer counts toward live matches.
LIVE_MATCH_MAX_AGE_SECONDS = 30
ALERT_TYPES = ("SERVER_DEGRADED", "CHEAT_DETECTED", "MATCH_QUALITY_LOW",
               "SMURF_DETECTED", "BEHAVIOR_ANOMALY")
ALERT_SEVERITIES = ("CRITICAL", "WARNING")
ALERTS_CHANNEL = "alerts:stream"
ALERT_LOG_SIZE = 100

# Throughput sampler cadence: one Kafka offset sample every 5 s, kept 120 s
# (the overview chart's span); rates are averaged over the newest 60 s.
SAMPLE_INTERVAL_SECONDS = 5
SAMPLE_RETENTION_SECONDS = 120
RATE_WINDOW_SECONDS = 60
SAMPLER_STALE_SECONDS = 3 * SAMPLE_INTERVAL_SECONDS

# Errors that mean "a backing service is unavailable" (-> HTTP 503).
UNAVAILABLE_ERRORS = (redis.RedisError, OSError, asyncpg.PostgresError)


def as_float(value: Any, default: float = 0.0) -> float:
    """Redis hashes store strings; tolerate missing/garbage values."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


class FailureStreak:
    """Log a background failure once per streak (and its recovery once),
    instead of one WARN line every retry tick."""

    def __init__(self, name: str):
        self.name = name
        self.failing = False

    def fail(self, err: BaseException, *, traceback: bool = False):
        if not self.failing:
            self.failing = True
            log.warning("%s failing: %r (retrying quietly until it recovers)",
                        self.name, err, exc_info=err if traceback else None)

    def ok(self):
        if self.failing:
            self.failing = False
            log.warning("%s recovered", self.name)


# ─── Redis (async, bounded by socket timeouts) ──────────────────────────────

_redis: aioredis.Redis | None = None


def redis_client() -> aioredis.Redis:
    """Process-wide async client. Socket timeouts bound every call, so a
    hung Redis yields redis.TimeoutError (-> 503) instead of a stuck loop."""
    global _redis
    if _redis is None:
        _redis = aioredis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True,
            socket_timeout=REDIS_TIMEOUT_SECONDS,
            socket_connect_timeout=REDIS_TIMEOUT_SECONDS,
            health_check_interval=30)
    return _redis


async def close_redis():
    global _redis
    if _redis is not None:
        await _redis.aclose()
        _redis = None


async def _pipeline(build: Callable[[Any], Any]) -> list[Any]:
    """One round trip for many commands (non-transactional pipeline)."""
    async with redis_client().pipeline(transaction=False) as pipe:
        build(pipe)
        return await pipe.execute()


async def _hgetall_many(keys: list[str]) -> list[dict[str, str]]:
    if not keys:
        return []
    return await _pipeline(lambda p: [p.hgetall(k) for k in keys])


# ─── Readers (the ONLY Redis readers; REST and dashboard share them) ────────
# Spark keeps the membership sets (players:flagged, players:smurf,
# behavior:anomalies) as lifetime sets; the CURRENT state lives in each
# entity hash, so every reader filters on the hash, not on set membership.

def _is_flagged(profile: dict[str, str]) -> bool:
    return profile.get("flagged") == "true"


def _is_smurf(smurf: dict[str, str]) -> bool:
    return smurf.get("status") == "SMURF"


def _is_behavior_anomaly(behavior: dict[str, str]) -> bool:
    return behavior.get("anomaly") == "1"


def effective_suspicion(profile: dict[str, str]) -> float:
    base = as_float(profile.get("suspicion_score"))
    return as_float(profile.get("suspicion_effective"), base)


async def read_servers() -> list[dict[str, str]]:
    ids = sorted(await redis_client().smembers("servers:active"))
    return [d for d in await _hgetall_many([f"server:{i}" for i in ids]) if d]


async def read_server(server_id: str) -> dict[str, str]:
    return await redis_client().hgetall(f"server:{server_id}")


async def read_flagged_players() -> list[dict[str, str]]:
    """Players whose latest cheat evaluation is flagged, worst first."""
    ids = sorted(await redis_client().smembers("players:flagged"))
    rows = [d for d in await _hgetall_many([f"player:{i}" for i in ids])
            if _is_flagged(d)]
    rows.sort(key=effective_suspicion, reverse=True)
    return rows


async def read_smurfs() -> list[dict[str, str]]:
    """Players whose latest smurf evaluation says SMURF, most likely first."""
    ids = sorted(await redis_client().smembers("players:smurf"))
    rows = []
    for pid, data in zip(ids, await _hgetall_many([f"player:smurf:{i}" for i in ids]),
                         strict=True):
        if _is_smurf(data):
            rows.append({**data, "player_id": pid})
    rows.sort(key=lambda s: as_float(s.get("probability")), reverse=True)
    return rows


async def read_behavior_anomalies() -> list[dict[str, str]]:
    """Current CUSUM behavior shifts. `anomalies` (lifetime count) lives in
    behavior:cusum:{id}; the rest of the row in player:behavior:{id}."""
    ids = sorted(await redis_client().smembers("behavior:anomalies"))
    keys = [k for i in ids for k in (f"player:behavior:{i}", f"behavior:cusum:{i}")]
    data = await _hgetall_many(keys)
    rows = []
    for n, pid in enumerate(ids):
        behavior, cusum = data[2 * n], data[2 * n + 1]
        if _is_behavior_anomaly(behavior):
            rows.append({**behavior, "player_id": pid,
                         "anomalies": cusum.get("anomalies", "0")})
    return rows


async def _count_current(set_key: str, hash_fmt: str, field: str, value: str) -> int:
    ids = list(await redis_client().smembers(set_key))
    if not ids:
        return 0
    vals = await _pipeline(lambda p: [p.hget(hash_fmt.format(i), field) for i in ids])
    return sum(1 for v in vals if v == value)


async def current_counts() -> dict[str, int]:
    """Current (not lifetime) flagged / smurf / behavior-shift counts."""
    flagged, smurf, anomalies = await asyncio.gather(
        _count_current("players:flagged", "player:{}", "flagged", "true"),
        _count_current("players:smurf", "player:smurf:{}", "status", "SMURF"),
        _count_current("behavior:anomalies", "player:behavior:{}", "anomaly", "1"))
    return {"cheat_flagged": flagged, "smurf": smurf, "behavior_anomalies": anomalies}


async def read_player(player_id: str) -> dict[str, Any] | None:
    """Combined live profile, or None when no job has written this player."""
    profile, smurf, behavior, cusum = await _hgetall_many([
        f"player:{player_id}", f"player:smurf:{player_id}",
        f"player:behavior:{player_id}", f"behavior:cusum:{player_id}"])
    if not (profile or smurf or behavior or cusum):
        return None
    return {
        "player_id": player_id,
        "combat": profile or None,
        "smurf_evaluation": smurf or None,
        "behavior": behavior or None,
        "cusum": cusum or None,
        "flags": {
            "cheat_flagged": _is_flagged(profile),
            "smurf_flagged": _is_smurf(smurf),
            "behavior_anomaly": _is_behavior_anomaly(behavior),
        },
    }


async def read_match(match_id: str) -> dict[str, Any]:
    data = await redis_client().hgetall(f"match:{match_id}")
    if data:
        data["quality_score"] = as_float(data.get("quality_score"))
    return data


async def read_recent_matches(limit: int = 40,
                              recent_seconds: int = MATCH_RECENT_SECONDS) -> dict[str, Any]:
    """Most recently scored matches (newest first).

    Match ids come from the `matches:timeline` zset ordered by update time.
    Each row's `recent` = rewritten within `recent_seconds` (the session
    window closed and was scored then — not a live match).
    """
    r = redis_client()
    if limit <= 0:
        tracked = await r.zcard("matches:quality")
        return {"rows": [], "tracked": tracked, "recent": 0}
    now = time.time()
    recent_since = now - recent_seconds
    top, tracked, recent = await _pipeline(lambda p: (
        p.zrevrange("matches:timeline", 0, max(0, limit - 1), withscores=True),
        p.zcard("matches:quality"),
        p.zcount("matches:timeline", recent_since, now),
    ))
    rows = []
    ids = [match_id for match_id, _ in top]
    for (match_id, updated), data in zip(top,
                                         await _hgetall_many([f"match:{i}" for i in ids]), strict=True):
        if data:
            data["quality_score"] = as_float(data.get("quality_score"))
            data["recent"] = updated is not None and 0 <= now - updated <= recent_seconds
            rows.append(data)
    return {"rows": rows, "tracked": tracked, "recent": recent}


async def read_match_summary() -> dict[str, Any]:
    """Status tiers + decile histogram straight from the zset (ZCOUNT, no
    per-match reads)."""
    hi, lo = MATCH_BALANCED_MIN, MATCH_UNBALANCED_MIN

    def build(p):
        p.zcard("matches:quality")
        p.zcount("matches:quality", hi, "+inf")
        p.zcount("matches:quality", lo, f"({hi}")
        p.zcount("matches:quality", "-inf", f"({lo}")
        for d in range(10):
            p.zcount("matches:quality", "-inf" if d == 0 else d * 10,
                     "+inf" if d == 9 else f"({(d + 1) * 10}")

    res = await _pipeline(build)
    return {"tracked": res[0], "balanced": res[1], "unbalanced": res[2],
            "stomped": res[3], "histogram": res[4:]}


async def tournament_snapshot() -> dict[str, Any]:
    """Live aggregates: server health, match quality, flag counts, alerts.
    Redis errors propagate (callers answer 503) — never fake zeros."""
    servers = await read_servers()
    worst, scored_total = await _pipeline(lambda p: (
        p.zrange("matches:quality", 0, 0, withscores=True), p.zcard("matches:quality")))
    counts = await current_counts()
    health = [as_float(s.get("health_score")) for s in servers]
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "servers": {
            "active": len(servers),
            "avg_health": round(sum(health) / len(health), 1) if health else None,
            "degraded": sum(1 for s in servers if s.get("status") != SERVER_HEALTHY),
        },
        "matches": {
            "scored_total": scored_total,
            "live": feed.live_matches(),
            "worst": {"match_id": worst[0][0], "score": worst[0][1]} if worst else None,
        },
        "players": counts,
        "alerts": {"recent_count": len(alert_log)},
    }


# ─── PostgreSQL (alert history) ─────────────────────────────────────────────

_pg_pool: asyncpg.Pool | None = None
_pg_pool_lock = asyncio.Lock()


async def get_pg_pool() -> asyncpg.Pool:
    """Lazily created asyncpg pool (min 1 / max 5 connections).

    asyncpg.Pool has no is_closed() (that lives on Connection) — the pool
    recovers per-acquire, so None-check is the whole guard. A failed create
    leaves it None, so the next request retries.
    """
    global _pg_pool
    if _pg_pool is None:
        async with _pg_pool_lock:
            if _pg_pool is None:
                _pg_pool = await asyncpg.create_pool(
                    PG_DSN, min_size=1, max_size=5, timeout=3, command_timeout=5)
    return _pg_pool


async def close_pg_pool():
    global _pg_pool
    if _pg_pool is not None:
        await _pg_pool.close()
        _pg_pool = None


def _alert_row(rec: asyncpg.Record) -> dict[str, Any]:
    d = dict(rec)
    if isinstance(d.get("details"), str):  # asyncpg leaves jsonb as str
        try:
            d["details"] = json.loads(d["details"])
        except ValueError:
            pass
    return d


async def fetch_alert_history(limit: int = 50, offset: int = 0,
                              alert_type: str | None = None,
                              severity: str | None = None) -> dict[str, Any]:
    """Paginated alert_history rows (newest first) + total for the filter."""
    pool = await get_pg_pool()
    where = ("WHERE ($1::text IS NULL OR alert_type = $1) "
             "AND ($2::text IS NULL OR severity = $2)")
    rows = await pool.fetch(
        "SELECT id, alert_id, alert_type, severity, entity_type, "
        f"entity_id, message, details, ts, received_at FROM alert_history {where} "
        "ORDER BY id DESC LIMIT $3 OFFSET $4",
        alert_type, severity, limit, offset)
    total = await pool.fetchval(f"SELECT count(*) FROM alert_history {where}",
                                alert_type, severity)
    return {
        "alerts": [_alert_row(row) for row in rows],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


# ─── Pipeline throughput (Kafka log offsets) ────────────────────────────────
# The sampler thread owns its own kafka-python consumer (not thread-safe, so
# nothing else touches it); request handlers only read the samples it leaves
# behind under _samples_lock.

_samples_lock = threading.Lock()
_samples: list[dict[str, Any]] = []   # {"ts", "end": {topic: n}, "retained": {topic: n}}
_sampler_thread: threading.Thread | None = None
_sampler_stop = threading.Event()


def _sample_offsets(c: KafkaConsumer) -> dict[str, Any]:
    end_total: dict[str, int] = {}
    retained: dict[str, int] = {}
    for topic in sorted(t for t in c.topics() if not t.startswith("__")):
        parts = [TopicPartition(topic, p) for p in (c.partitions_for_topic(topic) or [])]
        if not parts:
            end_total[topic] = retained[topic] = 0
            continue
        beg = c.beginning_offsets(parts)
        end = c.end_offsets(parts)
        end_total[topic] = sum(end.get(tp, 0) for tp in parts)
        retained[topic] = sum(end.get(tp, 0) - beg.get(tp, 0) for tp in parts)
    return {"ts": time.time(), "end": end_total, "retained": retained}


def _throughput_sampler():
    streak = FailureStreak("throughput sampler")
    consumer: KafkaConsumer | None = None
    try:
        while not _sampler_stop.is_set():
            try:
                if consumer is None:
                    # No subscription: metadata + offset queries only.
                    consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                                             group_id=None, enable_auto_commit=False)
                sample = _sample_offsets(consumer)
                with _samples_lock:
                    _samples.append(sample)
                    cutoff = sample["ts"] - SAMPLE_RETENTION_SECONDS
                    while _samples and _samples[0]["ts"] < cutoff:
                        _samples.pop(0)
                streak.ok()
            except Exception as e:  # noqa: BLE001 - kafka-python raises many unrelated types
                streak.fail(e)
                if consumer is not None:
                    _close_quietly(consumer)
                    consumer = None
            _sampler_stop.wait(SAMPLE_INTERVAL_SECONDS)
    finally:
        if consumer is not None:
            _close_quietly(consumer)


def _close_quietly(consumer: KafkaConsumer):
    try:
        consumer.close()
    except Exception as e:  # noqa: BLE001 - best-effort close on shutdown
        log.debug("kafka consumer close: %r", e)


def start_throughput_sampler():
    global _sampler_thread
    if _sampler_thread is None:
        _sampler_stop.clear()
        _sampler_thread = threading.Thread(target=_throughput_sampler, daemon=True,
                                           name="throughput-sampler")
        _sampler_thread.start()


def stop_throughput_sampler(timeout: float = 5.0):
    global _sampler_thread
    _sampler_stop.set()
    if _sampler_thread is not None:
        _sampler_thread.join(timeout)
        _sampler_thread = None


def _input_rate(s0: dict[str, Any], s1: dict[str, Any]) -> float | None:
    dt = s1["ts"] - s0["ts"]
    if dt < 1.0:
        return None
    delta = sum(s1["end"].get(t, 0) - s0["end"].get(t, 0) for t in FEED_TOPICS)
    return round(max(delta, 0) / dt, 1)


def clock(ts: float) -> str:
    return time.strftime("%H:%M:%S", time.localtime(ts))


def sampler_age() -> float | None:
    """Seconds since the last successful Kafka sample (None = never)."""
    with _samples_lock:
        last = _samples[-1]["ts"] if _samples else None
    return None if last is None else time.time() - last


def throughput_snapshot() -> dict[str, Any]:
    """Input-topic rate averaged over the samples of the last 60 s (oldest
    to newest real sample — no synthetic 'now' point), plus the latest 5 s
    rate. Never touches Kafka. Rates are None until two samples exist."""
    with _samples_lock:
        samples = list(_samples)
    now = time.time()
    window_samples = [s for s in samples if s["ts"] >= now - RATE_WINDOW_SECONDS]
    rate, window = None, 0.0
    if len(window_samples) >= 2:
        first, last = window_samples[0], window_samples[-1]
        window = round(last["ts"] - first["ts"], 1)
        rate = _input_rate(first, last)
    latest = samples[-1] if samples else None
    return {
        "events_per_sec": rate,
        "window_seconds": window,
        "samples": len(window_samples),
        "latest_events_per_sec": _input_rate(samples[-2], samples[-1]) if len(samples) >= 2 else None,
        "latest_sample_time": clock(latest["ts"]) if latest else None,
        "input_topics": list(FEED_TOPICS),
        "ingested_total": sum(latest["end"].get(t, 0) for t in FEED_TOPICS) if latest else 0,
        "per_topic_total": dict(latest["retained"]) if latest else {},
        "generated_at": datetime.now(UTC).isoformat(),
    }


def throughput_timeline() -> list[dict[str, Any]]:
    """5 s input-topic rates over the retained ~120 s of samples, as
    {time: 'HH:MM:SS', eps: float} chart points (time = sample time)."""
    with _samples_lock:
        samples = list(_samples)
    out: list[dict[str, Any]] = []
    for s0, s1 in itertools.pairwise(samples):
        rate = _input_rate(s0, s1)
        if rate is not None:
            out.append({"time": clock(s1["ts"]), "eps": rate})
    return out


# ─── Shared broadcast (one publisher task, N subscribers) ───────────────────
# The HTTP mirror of what Kafka already gives us: each frame is computed ONCE
# per tick and the identical frame is fanned out to every connected window,
# so two browser tabs always show the same numbers at the same second.
# Subscribers receive the last frame of each kind immediately on connect
# (instant paint), a slow client drops its oldest queued frame instead of
# stalling the others, and the publisher task lives only while at least one
# client is watching. A crashing publisher is logged and restarted with
# backoff instead of silently freezing the stream.

Publisher = Callable[["Broadcaster"], Awaitable[None]]


class Broadcaster:
    def __init__(self, name: str, factory: Publisher | None = None):
        self.name = name
        self._factory = factory
        self._subs: dict[asyncio.Queue, None] = {}   # dict used as ordered set
        self._last: dict[str, Any] = {}              # kind -> most recent frame
        self._task: asyncio.Task | None = None
        self._streak = FailureStreak(f"publisher {name!r}")

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=64)
        self._subs[q] = None
        if self._factory is not None and (self._task is None or self._task.done()):
            self._task = asyncio.create_task(self._supervise(), name=f"publisher-{self.name}")
        return q

    def unsubscribe(self, q: asyncio.Queue):
        self._subs.pop(q, None)
        if not self._subs:
            if self._task is not None:
                self._task.cancel()
                self._task = None
            self._last.clear()   # never replay frames from a dead publisher

    async def _supervise(self):
        assert self._factory is not None
        backoff = 1.0
        while True:
            started = time.monotonic()
            try:
                await self._factory(self)
                raise RuntimeError("publisher returned")
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 - a publisher must survive any render error
                self._streak.fail(e, traceback=not isinstance(e, UNAVAILABLE_ERRORS))
            if time.monotonic() - started > 60:
                backoff = 1.0                    # it had been healthy for a while
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)

    def publish(self, kind: str, frame: Any):
        self._streak.ok()
        self._last[kind] = frame
        for q in list(self._subs):
            if q.full():
                try:
                    q.get_nowait()               # drop oldest: never stall peers
                except asyncio.QueueEmpty:
                    pass
            q.put_nowait(frame)

    def frames(self) -> list[Any]:
        """Cached frames for instant initial paint of a new subscriber."""
        return list(self._last.values())


# ─── Go alert engine stream (alerts:stream) ─────────────────────────────────
# One listener task per process keeps the last N engine alerts (seeded from
# Postgres alert_history, then live from the Redis channel) and fans each raw
# payload out on `alert_bus` (WS /ws/alerts, dashboard SSE).

class AlertLog:
    def __init__(self, size: int = ALERT_LOG_SIZE):
        self._items: deque[dict[str, Any]] = deque(maxlen=size)

    def __len__(self) -> int:
        return len(self._items)

    def add(self, alert: dict[str, Any]) -> bool:
        aid = alert.get("alert_id")
        if aid and any(a.get("alert_id") == aid for a in self._items):
            return False                  # engine is at-least-once
        self._items.appendleft(alert)
        return True

    def merge(self, alerts: list[dict[str, Any]]):
        merged = {a.get("alert_id"): a for a in alerts}
        merged.update({a.get("alert_id"): a for a in self._items})
        ordered = sorted(merged.values(), key=lambda a: as_float(a.get("timestamp")), reverse=True)
        self._items.clear()
        self._items.extend(ordered[: self._items.maxlen])

    def recent(self, n: int) -> list[dict[str, Any]]:
        return list(self._items)[:n]


alert_log = AlertLog()
alert_bus = Broadcaster("alerts:stream")


def _normalized(rec: dict[str, Any]) -> dict[str, Any]:
    """alert_history row -> the engine's alerts:stream payload shape."""
    return {
        "alert_id": rec["alert_id"], "alert_type": rec["alert_type"],
        "severity": rec["severity"], "entity_type": rec["entity_type"],
        "entity_id": rec["entity_id"], "message": rec["message"],
        "details": rec["details"], "timestamp": int(rec["ts"].timestamp() * 1000),
    }


async def _seed_alert_log() -> bool:
    try:
        hist = await fetch_alert_history(limit=ALERT_LOG_SIZE)
    except UNAVAILABLE_ERRORS as e:
        log.warning("alert log: Postgres seed failed (%r); live alerts only for now", e)
        return False
    alert_log.merge([_normalized(r) for r in hist["alerts"]])
    return True


async def run_alert_listener():
    """Subscribe to alerts:stream (reconnecting with backoff) and re-seed
    from Postgres after every (re)subscribe so outage gaps are backfilled."""
    streak = FailureStreak("alerts:stream listener")
    backoff = 1.0
    while True:
        pubsub = redis_client().pubsub()
        try:
            await pubsub.subscribe(ALERTS_CHANNEL)
            seeded = await _seed_alert_log()
            next_seed = time.monotonic() + 30
            streak.ok()
            backoff = 1.0
            while True:
                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if msg is None:
                    if not seeded and time.monotonic() >= next_seed:
                        seeded = await _seed_alert_log()
                        next_seed = time.monotonic() + 30
                    continue
                if msg.get("type") != "message":
                    continue
                try:
                    alert = json.loads(msg["data"])
                except ValueError:
                    log.warning("alerts:stream: dropping non-JSON payload")
                    continue
                if isinstance(alert, dict) and alert_log.add(alert):
                    alert_bus.publish("alert", msg["data"])
        except asyncio.CancelledError:
            raise
        except UNAVAILABLE_ERRORS as e:
            streak.fail(e)
        finally:
            try:
                await pubsub.aclose()
            except UNAVAILABLE_ERRORS:
                pass
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 30.0)


# ─── Kafka → fan-out live event feed (WS + Datastar SSE) ────────────────────

class EventFeed:
    """Single shared Kafka consumer broadcasting processed events to every
    connected /ws/live and dashboard SSE client. Retains a rolling buffer
    of the most recent events so new connections and initial page renders
    are immediately populated, and the latest server_metrics per server
    (live active-match count).

    ASSIGN-ONLY: partitions are assigned manually (seeded at tail-25) and
    the consumer stays at the tail. NEVER call subscribe() on it —
    kafka-python 2.x raises IllegalStateError (assign and subscribe are
    mutually exclusive). Topics missing at startup are re-checked every few
    seconds and added when they appear; re-assigning a superset keeps the
    positions of partitions already assigned.
    """

    SEED_PER_PARTITION = 25
    TOPIC_RECHECK_SECONDS = 5.0

    def __init__(self, topics: tuple[str, ...] = FEED_TOPICS, history: int = 40):
        self._topics = topics
        self._queues: dict[asyncio.Queue, asyncio.AbstractEventLoop] = {}
        self._lock = threading.Lock()
        self._history: deque[str] = deque(maxlen=history)
        self._server_matches: dict[str, tuple[int, float]] = {}
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    def start(self):
        with self._lock:
            if self._thread is None:
                self._stop.clear()
                self._thread = threading.Thread(target=self._run, daemon=True,
                                                name="kafka-event-feed")
                self._thread.start()

    def stop(self, timeout: float = 5.0):
        self._stop.set()
        thread, self._thread = self._thread, None
        if thread is not None:
            thread.join(timeout)

    def register(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=256)
        with self._lock:
            self._queues[q] = asyncio.get_running_loop()
        return q

    def unregister(self, q: asyncio.Queue):
        with self._lock:
            self._queues.pop(q, None)

    def recent_events(self) -> list[str]:
        with self._lock:
            return list(self._history)

    def live_matches(self, max_age: float = LIVE_MATCH_MAX_AGE_SECONDS) -> int:
        """Sum of `active_matches` over servers with a fresh server_metrics
        sample (event time within max_age)."""
        cutoff = time.time() - max_age
        with self._lock:
            return sum(n for n, ts in self._server_matches.values() if ts >= cutoff)

    @staticmethod
    def _drop_put(q: asyncio.Queue, text: str):
        if q.full():
            try:
                q.get_nowait()
            except asyncio.QueueEmpty:
                pass
        q.put_nowait(text)

    def _dispatch(self, rec) -> None:
        if rec.value is None:          # tombstone: nothing to show
            return
        try:
            event = json.loads(rec.value)
        except ValueError:
            event = {"raw": rec.value.decode("utf-8", errors="replace")}
        text = json.dumps({
            "type": "event",
            "topic": rec.topic,
            "offset": rec.offset,
            "timestamp": rec.timestamp,   # Kafka record time (ms)
            "event": event,
        })
        with self._lock:
            self._history.append(text)
            if rec.topic == "server_metrics" and isinstance(event, dict) and event.get("server_id"):
                ts_ms = as_float(event.get("timestamp"), as_float(rec.timestamp))
                self._server_matches[str(event["server_id"])] = (
                    int(as_float(event.get("active_matches"))), ts_ms / 1000)
            targets = list(self._queues.items())
        for q, loop in targets:
            if not loop.is_closed():
                loop.call_soon_threadsafe(self._drop_put, q, text)

    def _assign_new(self, consumer: KafkaConsumer,
                    assigned: set[TopicPartition]) -> set[TopicPartition]:
        new = {TopicPartition(t, p)
               for t in self._topics
               for p in consumer.partitions_for_topic(t)
               if TopicPartition(t, p) not in assigned}
        if not new:
            return assigned
        full = assigned | new
        consumer.assign(sorted(full))
        end = consumer.end_offsets(sorted(new))
        for tp in new:
            consumer.seek(tp, max(0, end.get(tp, 0) - self.SEED_PER_PARTITION))
        return full

    def _run(self):
        streak = FailureStreak("kafka event feed")
        waiting_logged: set[str] = set()
        while not self._stop.is_set():
            consumer: KafkaConsumer | None = None
            try:
                consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                                         group_id=None, enable_auto_commit=False)
                assigned: set[TopicPartition] = set()
                next_check = 0.0
                while not self._stop.is_set():
                    if time.monotonic() >= next_check:
                        have = {tp.topic for tp in assigned}
                        if have != set(self._topics):
                            assigned = self._assign_new(consumer, assigned)
                            missing = set(self._topics) - {tp.topic for tp in assigned}
                            if missing - waiting_logged:
                                log.warning("event feed: waiting for topics %s", sorted(missing))
                            waiting_logged = missing
                        next_check = time.monotonic() + self.TOPIC_RECHECK_SECONDS
                    if not assigned:
                        self._stop.wait(1.0)
                        continue
                    batch = consumer.poll(timeout_ms=1000)
                    streak.ok()
                    records = [r for recs in batch.values() for r in recs]
                    records.sort(key=lambda r: (r.timestamp or 0, r.offset))
                    for rec in records:
                        self._dispatch(rec)
            except Exception as e:  # noqa: BLE001 - kafka-python raises many unrelated types
                streak.fail(e)
                self._stop.wait(3.0)
            finally:
                if consumer is not None:
                    _close_quietly(consumer)


feed = EventFeed()
