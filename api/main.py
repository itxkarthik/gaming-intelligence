"""FastAPI Application Entrypoint for Gaming Intelligence Platform."""

import asyncio
import json
import os
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import redis
import redis.asyncio as aioredis

app = FastAPI(
    title="Gaming Intelligence Platform API",
    description="Real-Time Competitive Gaming Analytics, Cheat Detection, and Server Health Monitoring",
    version="1.1.0"
)

# Enable CORS for dashboard development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
FEED_TOPICS = ["gameplay_events", "player_events", "server_metrics"]
HEARTBEAT_SECONDS = 15


def get_redis_client():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


# ─── Basic routes ───────────────────────────────────────────────────────────

@app.get("/")
async def root():
    return {
        "status": "online",
        "service": "gaming-intelligence-api",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@app.get("/health")
async def health_check():
    redis_status = "unavailable"
    try:
        r = get_redis_client()
        if r.ping():
            redis_status = "connected"
    except Exception:
        redis_status = "disconnected"

    return {
        "status": "healthy" if redis_status == "connected" else "degraded",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "components": {
            "api": "ready",
            "redis": redis_status,
            "kafka": "configured"
        }
    }


# ─── Servers ────────────────────────────────────────────────────────────────

@app.get("/api/v1/servers")
async def list_servers():
    """Returns real-time health metrics for all active game servers."""
    try:
        r = get_redis_client()
        server_ids = r.smembers("servers:active")
        results = []
        for s_id in server_ids:
            data = r.hgetall(f"server:{s_id}")
            if data:
                results.append(data)
        return {"servers": sorted(results, key=lambda x: x.get("server_id", ""))}
    except Exception as e:
        return {"servers": [], "error": str(e)}


@app.get("/api/v1/servers/{server_id}/health")
def get_server_health(server_id: str):
    """Returns the latest health snapshot for one server."""
    r = get_redis_client()
    data = r.hgetall(f"server:{server_id}")
    if not data:
        raise HTTPException(status_code=404, detail=f"server '{server_id}' not found")
    data["health_score"] = _float(data.get("health_score"))
    return data


# ─── Matches ────────────────────────────────────────────────────────────────

@app.get("/api/v1/matches/active")
def list_active_matches(live_window_seconds: int = 90):
    """Returns matches currently tracked in Redis, most recently updated first.

    A match whose quality window was updated within `live_window_seconds`
    is flagged active: true (session-window jobs refresh them continuously
    while a stream runs).
    """
    r = get_redis_client()
    now_ms = int(time.time() * 1000)
    matches = []
    for key in r.scan_iter("match:*"):
        if ":" in key[len("match:"):]:  # skip match:meta style keys if any
            continue
        data = r.hgetall(key)
        if not data:
            continue
        updated_ms = int(_float(data.get("updated_at"), 0)) * 1000
        data["active"] = 0 <= (now_ms - updated_ms) <= live_window_seconds * 1000
        data["quality_score"] = _float(data.get("quality_score"))
        matches.append(data)
    matches.sort(key=lambda m: m.get("updated_at", ""), reverse=True)
    return {
        "matches": matches[:50],
        "total": len(matches),
        "active": sum(1 for m in matches if m["active"]),
    }


@app.get("/api/v1/matches/{match_id}/quality")
def get_match_quality(match_id: str):
    """Returns the quality assessment (score, status, kills) for one match."""
    r = get_redis_client()
    data = r.hgetall(f"match:{match_id}")
    if not data:
        raise HTTPException(status_code=404, detail=f"match '{match_id}' not found")
    score = r.zscore("matches:quality", match_id)
    data["quality_score"] = _float(data.get("quality_score"), _float(score))
    return data


# ─── Players ────────────────────────────────────────────────────────────────

@app.get("/api/v1/players/flagged")
async def list_flagged_players():
    """Returns players flagged by real-time cheat detection."""
    try:
        r = get_redis_client()
        player_ids = r.smembers("players:flagged")
        results = []
        for p_id in player_ids:
            data = r.hgetall(f"player:{p_id}")
            if data:
                results.append(data)
        return {"flagged_players": results}
    except Exception as e:
        return {"flagged_players": [], "error": str(e)}


@app.get("/api/v1/players/{player_id}/profile")
def get_player_profile(player_id: str):
    """Returns the combined live profile: combat stats, smurf evaluation,
    and behavior-change status for one player."""
    r = get_redis_client()
    profile = r.hgetall(f"player:{player_id}")
    smurf = r.hgetall(f"player:smurf:{player_id}")
    behavior = r.hgetall(f"player:behavior:{player_id}")
    if not profile and not smurf and not behavior:
        raise HTTPException(status_code=404, detail=f"player '{player_id}' not found")
    return {
        "player_id": player_id,
        "combat": profile or None,
        "smurf_evaluation": smurf or None,
        "behavior": behavior or None,
        "flags": {
            "cheat_flagged": r.sismember("players:flagged", player_id),
            "smurf_flagged": r.sismember("players:smurf", player_id),
            "behavior_anomaly": r.sismember("behavior:anomalies", player_id),
        },
    }


@app.get("/api/v1/players/{player_id}/suspicion")
def get_player_suspicion(player_id: str):
    """Returns suspicion scores for one player: base suspicion, behavior
    boosted suspicion, IsolationForest score, and flags."""
    r = get_redis_client()
    data = r.hgetall(f"player:{player_id}")
    if not data:
        raise HTTPException(status_code=404, detail=f"player '{player_id}' not found")
    base = _float(data.get("suspicion_score"))
    effective = _float(data.get("suspicion_effective"), base)
    return {
        "player_id": player_id,
        "suspicion_score": base,
        "suspicion_effective": effective,
        "behavior_boost": round(effective - base, 6),
        "iforest_score": _float(data.get("iforest_score"), 0.0),
        "iforest_flag": data.get("iforest_flag") == "true",
        "behavior_anomaly": data.get("behavior_anomaly") == "true",
        "flagged": r.sismember("players:flagged", player_id),
        "updated_at": data.get("updated_at"),
    }


# ─── Alerts ─────────────────────────────────────────────────────────────────

@app.get("/api/v1/alerts/recent")
async def list_recent_alerts(limit: int = 20):
    """Returns recent alerts generated by streaming analytics."""
    limit = max(1, min(limit, 100))  # clamp: limit<=0 would make lrange return the whole list
    try:
        r = get_redis_client()
        raw_alerts = r.lrange("alerts:recent", 0, limit - 1)
        alerts = [json.loads(a) for a in raw_alerts]
        return {"alerts": alerts}
    except Exception as e:
        return {"alerts": [], "error": str(e)}


# ─── Tournament / aggregate stats ───────────────────────────────────────────

@app.get("/api/v1/tournament/live")
def tournament_live():
    """Returns live tournament aggregates: server health, match quality,
    flagged/smurf/anomaly counts, and recent alert volume."""
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


# ─── Pipeline throughput (Kafka log offsets) ────────────────────────────────

_throughput_lock = threading.Lock()
_throughput_samples: List[Dict[str, Any]] = []
_throughput_consumer = None
_throughput_started = False


def _get_throughput_consumer():
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
    from kafka import TopicPartition
    c = _get_throughput_consumer()
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
    """Background sampler: records per-topic log totals every 5 s so the
    endpoint can compute a real events/sec rate over the last 60 s."""
    while True:
        try:
            counts = _topic_counts()
            with _throughput_lock:
                _throughput_samples.append({"ts": time.time(), "totals": counts})
                cutoff = time.time() - 120
                while len(_throughput_samples) > 2 and _throughput_samples[0]["ts"] < cutoff:
                    _throughput_samples.pop(0)
        except Exception as e:
            print(f"[WARN] throughput sampler: {e}", file=sys.stderr, flush=True)
        time.sleep(5)


@app.on_event("startup")
def _start_throughput_sampler():
    global _throughput_started
    if not _throughput_started:
        _throughput_started = True
        threading.Thread(target=_throughput_sampler, daemon=True,
                         name="throughput-sampler").start()


@app.get("/api/v1/stats/throughput")
def pipeline_throughput():
    """Returns per-topic event totals and the pipeline's observed
    events/sec rate over a sliding ~60 s window of Kafka log offsets."""
    try:
        current = _topic_counts()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Kafka unavailable: {e}")

    with _throughput_lock:
        samples = [s for s in _throughput_samples if s["ts"] >= time.time() - 60]
        samples = samples + [{"ts": time.time(), "totals": current}]

    rate = None
    window = 0.0
    if len(samples) >= 2:
        first, last = samples[0], samples[-1]
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


# ─── Kafka → WebSocket live event feed ──────────────────────────────────────

class EventFeed:
    """Single shared Kafka consumer broadcasting processed events to every
    connected /ws/live client. Slow clients drop oldest frames (backpressure
    by design — dashboards want freshness, not completeness)."""

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


@app.websocket("/ws/live")
async def ws_live(websocket: WebSocket):
    """Real-time event feed: streams processed Kafka events (gameplay,
    player events, server metrics) as they arrive. Heartbeat frame every
    {HEARTBEAT_SECONDS}s keeps proxies from idling the connection out."""
    await websocket.accept()
    q = feed.register()
    try:
        while True:
            try:
                frame = await asyncio.wait_for(q.get(), timeout=HEARTBEAT_SECONDS)
                await websocket.send_text(frame)
            except asyncio.TimeoutError:
                await websocket.send_text(json.dumps(
                    {"type": "heartbeat",
                     "ts": datetime.now(timezone.utc).isoformat()}))
    except (WebSocketDisconnect, RuntimeError):
        pass
    finally:
        feed.unregister(q)


@app.websocket("/ws/alerts")
async def ws_alerts(websocket: WebSocket):
    """Alert notifications: forwards every alert the Go alert engine
    forwards (published on Redis channel `alerts:stream`) in real time."""
    await websocket.accept()
    r = aioredis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    pubsub = r.pubsub()
    await pubsub.subscribe("alerts:stream")
    last_beat = time.monotonic()
    try:
        while True:
            msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if msg and msg.get("type") == "message":
                await websocket.send_text(msg["data"])
                last_beat = time.monotonic()
            elif time.monotonic() - last_beat >= HEARTBEAT_SECONDS:
                await websocket.send_text(json.dumps(
                    {"type": "heartbeat",
                     "ts": datetime.now(timezone.utc).isoformat()}))
                last_beat = time.monotonic()
    except (WebSocketDisconnect, RuntimeError):
        pass
    finally:
        try:
            await pubsub.unsubscribe("alerts:stream")
            await pubsub.aclose()
            await r.aclose()
        except Exception:
            pass
