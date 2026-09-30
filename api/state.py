"""Shared process state for the API: connections, background feed, sampler.

Both main.py (REST/WS) and dashboard.py (HTML views + Datastar SSE) import
from here — one Kafka event feed, one offset sampler, one asyncpg pool per
process, no circular imports.
"""

import asyncio
import json
import os
import sys
import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import redis

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
FEED_TOPICS = ["gameplay_events", "player_events", "server_metrics"]

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


_redis_client = None


def get_redis_client():
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    return _redis_client


async def close_pg_pool():
    global _pg_pool
    if _pg_pool is not None:
        await _pg_pool.close()
        _pg_pool = None


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


# ─── PostgreSQL (alert history) ─────────────────────────────────────────────

_pg_pool: Optional[Any] = None
_pg_pool_lock = asyncio.Lock()


async def get_pg_pool():
    """Lazily created asyncpg pool (min 1 / max 5 connections).

    asyncpg.Pool has no is_closed() (that lives on Connection) — the pool
    recovers per-acquire, so None-check is the whole guard.
    """
    global _pg_pool
    if _pg_pool is None:
        async with _pg_pool_lock:
            if _pg_pool is None:
                import asyncpg
                _pg_pool = await asyncpg.create_pool(PG_DSN, min_size=1, max_size=5)
    return _pg_pool


async def fetch_alert_history(limit: int = 50, offset: int = 0,
                              alert_type: Optional[str] = None,
                              severity: Optional[str] = None) -> Dict[str, Any]:
    """Paginated alert_history rows (newest first) + total for the filter."""
    def _as_dict(rec) -> Dict[str, Any]:
        d = dict(rec)
        if isinstance(d.get("details"), str):  # asyncpg leaves jsonb as str
            try:
                d["details"] = json.loads(d["details"])
            except ValueError:
                pass
        return d

    pool = await get_pg_pool()
    filters = ("WHERE ($3::text IS NULL OR alert_type = $3) "
               "AND ($4::text IS NULL OR severity = $4)")
    rows = await pool.fetch(
        "SELECT id, alert_id, alert_type, severity, entity_type, "
        "entity_id, message, details, ts, received_at "
        f"FROM alert_history {filters} "
        "ORDER BY id DESC LIMIT $1 OFFSET $2",
        limit, offset, alert_type, severity)
    total = await pool.fetchval(
        "SELECT count(*) FROM alert_history "
        "WHERE ($1::text IS NULL OR alert_type = $1) "
        "AND ($2::text IS NULL OR severity = $2)",
        alert_type, severity)
    return {
        "alerts": [_as_dict(row) for row in rows],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


# ─── Pipeline throughput (Kafka log offsets) ────────────────────────────────

# kafka-python's KafkaConsumer is NOT thread-safe: creation and every offset
# query must happen under one lock. Only the background sampler queries
# Kafka — request handlers read the samples it leaves behind.
_throughput_lock = threading.Lock()
_throughput_samples: List[Dict[str, Any]] = []
_throughput_consumer = None
_throughput_started = False
_throughput_last: Dict[str, int] = {}


def _get_throughput_consumer_locked():
    """Caller must hold _throughput_lock."""
    global _throughput_consumer
    if _throughput_consumer is None:
        from kafka import KafkaConsumer
        # No subscription: we only use metadata + offset queries.
        _throughput_consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=None,
            enable_auto_commit=False,
        )
    return _throughput_consumer


def _topic_counts() -> Dict[str, int]:
    """Offset snapshot. Caller must hold _throughput_lock (single-threaded
    access to the shared consumer)."""
    from kafka import TopicPartition
    c = _get_throughput_consumer_locked()
    counts = {}
    for topic in sorted(t for t in c.topics() if not t.startswith("__")):
        parts = [TopicPartition(topic, p) for p in (c.partitions_for_topic(topic) or [])]
        if not parts:
            counts[topic] = 0
            continue
        beg = c.beginning_offsets(parts)
        end = c.end_offsets(parts)
        counts[topic] = sum(end.get(tp, 0) - beg.get(tp, 0) for tp in parts)
    return counts


def _throughput_sampler():
    """Background sampler: records per-topic log totals every 5 s so readers
    can compute a real events/sec rate over the last 60 s."""
    while True:
        try:
            with _throughput_lock:
                counts = _topic_counts()
                _throughput_last.clear()
                _throughput_last.update(counts)
                _throughput_samples.append({"ts": time.time(), "totals": counts})
                cutoff = time.time() - 120
                while len(_throughput_samples) > 2 and _throughput_samples[0]["ts"] < cutoff:
                    _throughput_samples.pop(0)
        except Exception as e:
            print(f"[WARN] throughput sampler: {e}", file=sys.stderr, flush=True)
        time.sleep(5)


def start_throughput_sampler():
    global _throughput_started
    if not _throughput_started:
        _throughput_started = True
        threading.Thread(target=_throughput_sampler, daemon=True,
                         name="throughput-sampler").start()


def throughput_snapshot() -> Dict[str, Any]:
    """Rate over the last ~60 s from sampler samples (never touches Kafka —
    sampler thread owns the consumer). Returns rate=None before warm-up."""
    with _throughput_lock:
        samples = [s for s in _throughput_samples if s["ts"] >= time.time() - 60]
        current = dict(_throughput_last)
    rate = None
    window = 0.0
    if samples:
        enriched = samples + [{"ts": time.time(), "totals": current}]
        first, last = enriched[0], enriched[-1]
        window = round(last["ts"] - first["ts"], 1)
        if window >= 2.0:
            delta = sum(last["totals"].get(t, 0) - first["totals"].get(t, 0)
                        for t in set(last["totals"]) | set(first["totals"]))
            rate = round(max(delta, 0) / window, 1)
    return {
        "events_per_sec": rate,
        "window_seconds": window,
        "samples": len(samples),
        "per_topic_total": current,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def throughput_timeline() -> List[Dict[str, Any]]:
    """Returns sliding 60s timeline of {time: 'HH:MM:SS', eps: float} points for charts."""
    with _throughput_lock:
        samples = list(_throughput_samples)
    out: List[Dict[str, Any]] = []
    for i in range(1, len(samples)):
        s0, s1 = samples[i-1], samples[i]
        dt = s1["ts"] - s0["ts"]
        if dt >= 1.0:
            delta = sum(s1["totals"].get(t, 0) - s0["totals"].get(t, 0)
                        for t in set(s1["totals"]) | set(s0["totals"]))
            rate = round(max(0.0, delta) / dt, 1)
            time_str = time.strftime("%H:%M:%S", time.localtime(s1["ts"]))
            out.append({"time": time_str, "eps": rate})
    return out


# ─── Shared SSE broadcast (one publisher task, N subscribers) ────────────────
# The HTTP mirror of what Kafka already gives us: each frame is computed ONCE
# per tick and the identical frame is fanned out to every connected window,
# so two browser tabs always show the same numbers at the same second.
# Subscribers receive the last frame of each kind immediately on connect
# (instant paint), a slow client drops its oldest queued frame instead of
# stalling the others, and the publisher task lives only while at least one
# client is watching.

class Broadcaster:
    def __init__(self):
        self._subs: Dict[asyncio.Queue, None] = {}   # dict used as ordered set
        self._last: Dict[str, Any] = {}              # kind -> most recent frame
        self._task: Optional[asyncio.Task] = None
        self._factory: Optional[Any] = None

    def attach(self, factory) -> "Broadcaster":
        self._factory = factory
        return self

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=64)
        self._subs[q] = None
        if (self._task is None or self._task.done()) and self._factory is not None:
            self._task = asyncio.create_task(self._factory(self))
        return q

    def unsubscribe(self, q: asyncio.Queue):
        self._subs.pop(q, None)
        if not self._subs and self._task is not None:
            self._task.cancel()
            self._task = None

    def publish(self, kind: str, frame: Any):
        self._last[kind] = frame
        for q in list(self._subs):
            try:
                q.put_nowait(frame)
            except asyncio.QueueFull:
                try:
                    q.get_nowait()               # drop oldest: never stall peers
                except asyncio.QueueEmpty:
                    pass
                try:
                    q.put_nowait(frame)
                except asyncio.QueueFull:
                    pass

    def frames(self) -> List[Any]:
        """Cached frames for instant initial paint of a new subscriber."""
        return list(self._last.values())


_sse_buses: Dict[str, Broadcaster] = {}


def sse_bus(name: str, factory) -> Broadcaster:
    """Registry: exactly one Broadcaster + publisher task per SSE endpoint."""
    bus = _sse_buses.get(name)
    if bus is None:
        bus = _sse_buses[name] = Broadcaster()
    return bus.attach(factory)


# ─── Kafka → fan-out live event feed (WS + Datastar SSE) ────────────────────

class EventFeed:
    """Single shared Kafka consumer broadcasting processed events to every
    connected /ws/live and dashboard SSE client. Retains a rolling buffer
    of the most recent events so new connections and initial page renders
    are immediately populated."""

    def __init__(self):
        self._queues: set = set()
        self._started = False
        self._lock = threading.Lock()
        self._history: deque = deque(maxlen=40)

    def start(self):
        with self._lock:
            if not self._started:
                self._started = True
                threading.Thread(target=self._run, daemon=True,
                                 name="kafka-event-feed").start()

    def register(self) -> asyncio.Queue:
        loop = asyncio.get_running_loop()
        q: asyncio.Queue = asyncio.Queue(maxsize=256)
        with self._lock:
            self._queues.add((q, loop))
            if not self._started:
                self._started = True
                threading.Thread(target=self._run, daemon=True,
                                 name="kafka-event-feed").start()
        return q

    def unregister(self, q: asyncio.Queue):
        with self._lock:
            self._queues = {pair for pair in self._queues if pair[0] is not q}

    def recent_events(self) -> List[str]:
        with self._lock:
            return list(self._history)

    @staticmethod
    def _drop_put(q: asyncio.Queue, text: str):
        if q.full():
            try:
                q.get_nowait()
            except asyncio.QueueEmpty:
                pass
        q.put_nowait(text)

    def _run(self):
        from kafka import KafkaConsumer, TopicPartition
        while True:
            try:
                consumer = KafkaConsumer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    group_id=None,
                    enable_auto_commit=False,
                )
                # Seed latest recent records on startup so table is never blank
                try:
                    tps = []
                    for t in FEED_TOPICS:
                        parts = consumer.partitions_for_topic(t)
                        if parts:
                            for p in parts:
                                tps.append(TopicPartition(t, p))
                    if tps:
                        consumer.assign(tps)
                        end_offsets = consumer.end_offsets(tps)
                        for tp in tps:
                            consumer.seek(tp, max(0, end_offsets.get(tp, 0) - 25))
                        initial_records = consumer.poll(timeout_ms=1000, max_records=60)
                        all_recs = []
                        for tp, rec_list in initial_records.items():
                            all_recs.extend(rec_list)
                        all_recs.sort(key=lambda r: (r.timestamp or 0, r.offset))
                        for r in all_recs:
                            try:
                                ev = json.loads(r.value)
                            except (ValueError, TypeError):
                                ev = {"raw": r.value.decode("utf-8", errors="replace")}
                            text = json.dumps({
                                "type": "event",
                                "topic": r.topic,
                                "offset": r.offset,
                                "event": ev,
                            })
                            with self._lock:
                                self._history.append(text)
                    # Seed done on an ASSIGNED consumer: it stays at the tail and
                    # keeps receiving new records from there. NEVER call
                    # subscribe() on the same consumer — kafka-python 2.x raises
                    # IllegalStateError (assign and subscribe are mutually
                    # exclusive) and the feed loop would crash every 3 s.
                except Exception as seed_err:
                    print(f"[DEBUG] kafka seed note: {seed_err}", file=sys.stderr, flush=True)

                for msg in consumer:
                    try:
                        event = json.loads(msg.value)
                    except (ValueError, TypeError):
                        event = {"raw": msg.value.decode("utf-8", errors="replace")}
                    text = json.dumps({
                        "type": "event",
                        "topic": msg.topic,
                        "offset": msg.offset,
                        "event": event,
                    })
                    with self._lock:
                        self._history.append(text)
                        targets = list(self._queues)
                    for q, loop in targets:
                        if not loop.is_closed():
                            loop.call_soon_threadsafe(self._drop_put, q, text)
            except Exception as e:
                print(f"[WARN] kafka event feed: {e}", file=sys.stderr, flush=True)
                time.sleep(3)


feed = EventFeed()
feed.start()


# ─── Tournament aggregates (REST + dashboard share this) ────────────────────

def tournament_snapshot() -> Dict[str, Any]:
    """Live aggregates: server health, match quality, flag counts, alerts."""
    servers: List[Dict[str, Any]] = []
    health_scores: List[float] = []
    worst = []
    scored_total = 0
    cheat_flagged = 0
    smurf = 0
    behavior_anomalies = 0
    recent_alerts = 0
    try:
        r = get_redis_client()
        for s_id in r.smembers("servers:active"):
            data = r.hgetall(f"server:{s_id}")
            if data:
                servers.append(data)
        health_scores = [_float(s.get("health_score")) for s in servers]
        worst = r.zrange("matches:quality", 0, 0, withscores=True)
        scored_total = r.zcard("matches:quality")
        cheat_flagged = r.scard("players:flagged")
        smurf = r.scard("players:smurf")
        behavior_anomalies = r.scard("behavior:anomalies")
        recent_alerts = r.llen("alerts:recent")
    except Exception:
        pass

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "servers": {
            "active": len(servers),
            "avg_health": round(sum(health_scores) / len(health_scores), 1) if health_scores else None,
            "degraded": sum(1 for h in health_scores if h < 50.0),
        },
        "matches": {
            "scored_total": scored_total,
            "worst": {"match_id": worst[0][0], "score": worst[0][1]} if worst else None,
        },
        "players": {
            "cheat_flagged": cheat_flagged,
            "smurf": smurf,
            "behavior_anomalies": behavior_anomalies,
        },
        "alerts": {"recent_count": recent_alerts},
    }
