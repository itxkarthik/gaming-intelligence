"""FastAPI Application Entrypoint for Gaming Intelligence Platform.

REST + WebSocket surface (Phase 2/4). Shared process state (Redis, Kafka
event feed, throughput sampler, PostgreSQL pool) lives in state.py; the
HTML dashboard (Phase 5, Datastar) lives in dashboard.py.
"""

import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import redis.asyncio as aioredis

import state
from state import HEARTBEAT_SECONDS, _float, get_redis_client
from dashboard import router as dashboard_router

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

app.include_router(dashboard_router)

# Dashboard assets (Phase 5)
import os as _os
app.mount("/static",
          StaticFiles(directory=_os.path.join(
              _os.path.dirname(_os.path.abspath(__file__)), "static")),
          name="static")


# ─── Basic routes ───────────────────────────────────────────────────────────

@app.get("/api/v1/status")
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


@app.get("/api/v1/alerts/history")
async def alert_history(limit: int = 50, offset: int = 0,
                        alert_type: Optional[str] = None,
                        severity: Optional[str] = None):
    """Paginated alert history from PostgreSQL (written by the Go alert
    engine) — complements /alerts/recent (Redis, latest 100 only).
    Filters: alert_type (SERVER_DEGRADED, CHEAT_DETECTED, ...) and
    severity (WARNING, CRITICAL)."""
    limit = max(1, min(limit, 200))
    offset = max(0, offset)
    try:
        return await state.fetch_alert_history(limit, offset, alert_type, severity)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"PostgreSQL unavailable: {e}")


# ─── Tournament / aggregate stats ───────────────────────────────────────────

@app.get("/api/v1/tournament/live")
def tournament_live():
    """Returns live tournament aggregates: server health, match quality,
    flagged/smurf/anomaly counts, and recent alert volume."""
    return state.tournament_snapshot()


# ─── Pipeline throughput (Kafka log offsets) ────────────────────────────────

@app.on_event("startup")
def _start_throughput_sampler():
    state.start_throughput_sampler()


@app.on_event("shutdown")
async def _shutdown():
    await state.close_pg_pool()


@app.get("/api/v1/stats/throughput")
def pipeline_throughput():
    """Returns per-topic event totals and the pipeline's observed
    events/sec rate over a sliding ~60 s window of Kafka log offsets."""
    snap = state.throughput_snapshot()
    if snap["samples"] == 0:
        raise HTTPException(status_code=503,
                            detail="throughput sampler not ready (Kafka unreachable or API just started)")
    return snap


# ─── Kafka → WebSocket live event feed ──────────────────────────────────────

@app.websocket("/ws/live")
async def ws_live(websocket: WebSocket):
    """Real-time event feed: streams processed Kafka events (gameplay,
    player events, server metrics) as they arrive. Heartbeat frame every
    {HEARTBEAT_SECONDS}s keeps proxies from idling the connection out."""
    await websocket.accept()
    q = state.feed.register()
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
        state.feed.unregister(q)


@app.websocket("/ws/alerts")
async def ws_alerts(websocket: WebSocket):
    """Alert notifications: forwards every alert the Go alert engine
    forwards (published on Redis channel `alerts:stream`) in real time."""
    await websocket.accept()
    r = aioredis.Redis(host=state.REDIS_HOST, port=state.REDIS_PORT, decode_responses=True)
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
