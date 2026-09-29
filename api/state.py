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
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

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


def get_redis_client():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)


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


# ─── Kafka → fan-out live event feed (WS + Datastar SSE) ────────────────────

class EventFeed:
    """Single shared Kafka consumer broadcasting processed events to every
    connected /ws/live and dashboard SSE client. Slow clients drop oldest
    frames (backpressure by design — dashboards want freshness, not
    completeness)."""

    def __init__(self):
        self._queues: set = set()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._started = False
        self._lock = threading.Lock()

    def register(self) -> asyncio.Queue:
        loop = asyncio.get_running_loop()
        q: asyncio.Queue = asyncio.Queue(maxsize=256)
        with self._lock:
            self._queues.add(q)
            if not self._started:
                self._started = True
                self._loop = loop
                threading.Thread(target=self._run, daemon=True,
                                 name="kafka-event-feed").start()
        return q

    def unregister(self, q: asyncio.Queue):
        with self._lock:
            self._queues.discard(q)

    @staticmethod
    def _drop_put(q: asyncio.Queue, text: str):
        if q.full():
            try:
                q.get_nowait()
            except asyncio.QueueEmpty:
                pass
        q.put_nowait(text)

    def _run(self):
        from kafka import KafkaConsumer
        while True:
            try:
                consumer = KafkaConsumer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    group_id=None,
                    enable_auto_commit=False,
                    auto_offset_reset="latest",
                )
                consumer.subscribe(FEED_TOPICS)
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
                    loop = self._loop
                    if loop is None:
                        continue
                    for q in list(self._queues):
                        loop.call_soon_threadsafe(self._drop_put, q, text)
            except Exception as e:
                print(f"[WARN] kafka event feed: {e}", file=sys.stderr, flush=True)
                time.sleep(3)


feed = EventFeed()


# ─── Tournament aggregates (REST + dashboard share this) ────────────────────

def tournament_snapshot() -> Dict[str, Any]:
    """Live aggregates: server health, match quality, flag counts, alerts."""
    r = get_redis_client()
    servers = []
    for s_id in r.smembers("servers:active"):
        data = r.hgetall(f"server:{s_id}")
        if data:
            servers.append(data)
    health_scores = [_float(s.get("health_score")) for s in servers]

    worst = r.zrange("matches:quality", 0, 0, withscores=True)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "servers": {
            "active": len(servers),
            "avg_health": round(sum(health_scores) / len(health_scores), 1) if health_scores else None,
            "degraded": sum(1 for h in health_scores if h < 50.0),
        },
        "matches": {
            "scored_total": r.zcard("matches:quality"),
            "worst": {"match_id": worst[0][0], "score": worst[0][1]} if worst else None,
        },
        "players": {
            "cheat_flagged": r.scard("players:flagged"),
            "smurf": r.scard("players:smurf"),
            "behavior_anomalies": r.scard("behavior:anomalies"),
        },
        "alerts": {"recent_count": r.llen("alerts:recent")},
    }
