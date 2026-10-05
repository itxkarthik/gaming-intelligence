"""FastAPI Application Entrypoint for Gaming Intelligence Platform.

REST + WebSocket surface (Phase 2/4). Shared process state (Redis readers,
Kafka event feed, throughput sampler, alert listener, PostgreSQL pool) lives
in state.py; the HTML dashboard (Phase 5, Datastar) lives in dashboard.py.
Same-origin only: the dashboard is served by this process, so no CORS.
"""

import asyncio
import contextlib
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import UTC, datetime

import asyncpg
import redis
from fastapi import (
    FastAPI,
    HTTPException,
    Query,
    Request,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

import state
from dashboard import router as dashboard_router
from dashboard import unavailable_page
from state import HEARTBEAT_SECONDS, as_float

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("api")


@asynccontextmanager
async def lifespan(_app: FastAPI):
    state.feed.start()
    state.start_throughput_sampler()
    listener = asyncio.create_task(state.run_alert_listener(), name="alert-listener")
    try:
        yield
    finally:
        listener.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await listener
        # Both threads close their Kafka consumers on the way out.
        await asyncio.gather(asyncio.to_thread(state.feed.stop),
                             asyncio.to_thread(state.stop_throughput_sampler))
        await state.close_redis()
        await state.close_pg_pool()


app = FastAPI(
    title="Gaming Intelligence Platform API",
    description="Real-Time Competitive Gaming Analytics, Cheat Detection, and Server Health Monitoring",
    version=state.VERSION,
    lifespan=lifespan,
)

app.include_router(dashboard_router)
app.mount("/static",
          StaticFiles(directory=os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")),
          name="static")


async def _service_unavailable(request: Request, exc: Exception):
    """Redis / PostgreSQL failures -> 503 (JSON for the API, a page for the
    dashboard). Details go to the log, never to the client."""
    service = ("Redis" if isinstance(exc, redis.RedisError)
               else "PostgreSQL" if isinstance(exc, asyncpg.PostgresError | OSError)
               else "backing service")
    log.warning("%s %s -> 503 (%s): %r", request.method, request.url.path, service, exc)
    if request.url.path.startswith("/api/") or request.url.path == "/health":
        return JSONResponse({"detail": f"{service} unavailable"}, status_code=503)
    return unavailable_page(request, service)


for _exc in state.UNAVAILABLE_ERRORS:
    app.add_exception_handler(_exc, _service_unavailable)


def _heartbeat() -> str:
    return json.dumps({"type": "heartbeat", "ts": datetime.now(UTC).isoformat()})


# ─── Basic routes ───────────────────────────────────────────────────────────

@app.get("/api/v1/status")
async def root():
    return {
        "status": "online",
        "service": "gaming-intelligence-api",
        "timestamp": datetime.now(UTC).isoformat()
    }


@app.get("/health")
async def health_check():
    """Redis = live PING; Kafka = freshness of the background offset sampler
    (connected if its last successful sample is under 15 s old)."""
    try:
        redis_ok = bool(await state.redis_client().ping())
    except state.UNAVAILABLE_ERRORS:
        redis_ok = False
    age = state.sampler_age()
    kafka = ("unavailable" if age is None
             else "connected" if age <= state.SAMPLER_STALE_SECONDS else "stale")
    return {
        "status": "healthy" if redis_ok and kafka == "connected" else "degraded",
        "timestamp": datetime.now(UTC).isoformat(),
        "components": {
            "api": "ready",
            "redis": "connected" if redis_ok else "disconnected",
            "kafka": kafka,
        }
    }


# ─── Servers ────────────────────────────────────────────────────────────────

@app.get("/api/v1/servers")
async def list_servers():
    """Returns real-time health metrics for all active game servers."""
    return {"servers": await state.read_servers()}


@app.get("/api/v1/servers/{server_id}/health")
async def get_server_health(server_id: str):
    """Returns the latest health snapshot for one server."""
    data = await state.read_server(server_id)
    if not data:
        raise HTTPException(status_code=404, detail=f"server '{server_id}' not found")
    data["health_score"] = as_float(data.get("health_score"))
    return data


# ─── Matches ────────────────────────────────────────────────────────────────

@app.get("/api/v1/matches/active")
async def list_active_matches(live_window_seconds: int = Query(state.MATCH_RECENT_SECONDS,
                                                               ge=0, le=86_400)):
    """Returns the 50 most recently SCORED matches, newest first.

    Match quality comes from append-mode session windows, which emit when a
    match's session closes — so a row's `active` flag means "scored within
    `live_window_seconds`" (the match recently ended), and `active` at the
    top level counts those rows. `live_matches` is the real in-progress
    count: the sum of server_metrics `active_matches` over servers that
    reported in the last 30 s. `total` = all matches ever scored.
    """
    res = await state.read_recent_matches(limit=50, recent_seconds=live_window_seconds)
    for m in res["rows"]:
        m["active"] = m.pop("recent")
    return {
        "matches": res["rows"],
        "total": res["tracked"],
        "active": res["recent"],
        "live_matches": state.feed.live_matches(),
    }


@app.get("/api/v1/matches/{match_id}/quality")
async def get_match_quality(match_id: str):
    """Returns the quality assessment (score, status, kills) for one match."""
    data = await state.read_match(match_id)
    if not data:
        raise HTTPException(status_code=404, detail=f"match '{match_id}' not found")
    return data


# ─── Players ────────────────────────────────────────────────────────────────

@app.get("/api/v1/players/flagged")
async def list_flagged_players():
    """Returns players whose LATEST cheat evaluation is flagged (the
    players:flagged set is lifetime; each hash's `flagged` field is current)."""
    return {"flagged_players": await state.read_flagged_players()}


@app.get("/api/v1/players/{player_id}/profile")
async def get_player_profile(player_id: str):
    """Returns the combined live profile: combat stats, smurf evaluation,
    behavior-change status, CUSUM state and current flags for one player."""
    profile = await state.read_player(player_id)
    if profile is None:
        raise HTTPException(status_code=404, detail=f"player '{player_id}' not found")
    return profile


@app.get("/api/v1/players/{player_id}/suspicion")
async def get_player_suspicion(player_id: str):
    """Returns suspicion scores for one player: base suspicion, behavior
    boosted suspicion, IsolationForest score (null until scored), and flags."""
    data = await state.redis_client().hgetall(f"player:{player_id}")
    if not data:
        raise HTTPException(status_code=404, detail=f"player '{player_id}' not found")
    base = as_float(data.get("suspicion_score"))
    effective = state.effective_suspicion(data)
    return {
        "player_id": player_id,
        "suspicion_score": base,
        "suspicion_effective": effective,
        "behavior_boost": round(effective - base, 6),
        "iforest_score": as_float(data["iforest_score"]) if data.get("iforest_score") else None,
        "iforest_flag": data.get("iforest_flag") == "true",
        "behavior_anomaly": data.get("behavior_anomaly") == "true",
        "flagged": data.get("flagged") == "true",
        "updated_at": data.get("updated_at"),
    }


# ─── Alerts ─────────────────────────────────────────────────────────────────

@app.get("/api/v1/alerts/recent")
async def list_recent_alerts(limit: int = 20):
    """Returns the latest alerts forwarded by the Go alert engine (post
    dedup/rate-limit; live from Redis `alerts:stream`, seeded from
    PostgreSQL at startup), newest first."""
    limit = max(1, min(limit, state.ALERT_LOG_SIZE))
    return {"alerts": state.alert_log.recent(limit)}


@app.get("/api/v1/alerts/history")
async def alert_history(limit: int = 50, offset: int = 0,
                        alert_type: str | None = None,
                        severity: str | None = None):
    """Paginated alert history from PostgreSQL (written by the Go alert
    engine) — complements /alerts/recent (in-memory, latest 100 only).
    Filters: alert_type (SERVER_DEGRADED, CHEAT_DETECTED, ...) and
    severity (WARNING, CRITICAL)."""
    limit = max(1, min(limit, 200))
    offset = max(0, offset)
    return await state.fetch_alert_history(limit, offset, alert_type, severity)


# ─── Tournament / aggregate stats ───────────────────────────────────────────

@app.get("/api/v1/tournament/live")
async def tournament_live():
    """Returns live tournament aggregates: server health, match quality,
    current flagged/smurf/anomaly counts, and recent engine alert volume."""
    return await state.tournament_snapshot()


# ─── Pipeline throughput (Kafka log offsets) ────────────────────────────────

@app.get("/api/v1/stats/throughput")
async def pipeline_throughput():
    """Returns the pipeline's events/sec over the input topics (sliding
    ~60 s window of Kafka log-end offsets, plus the latest 5 s rate),
    total events ingested into the input topics, and per-topic RETAINED
    record counts (end − beginning offsets, all topics)."""
    snap = state.throughput_snapshot()
    if snap["samples"] == 0:
        raise HTTPException(status_code=503,
                            detail="throughput sampler not ready (Kafka unreachable or API just started)")
    return snap


# ─── WebSockets ─────────────────────────────────────────────────────────────

async def _pump(websocket: WebSocket, q: asyncio.Queue):
    """Forward queued frames; a heartbeat frame after HEARTBEAT_SECONDS of
    silence keeps proxies from idling the connection out."""
    try:
        while True:
            try:
                frame = await asyncio.wait_for(q.get(), timeout=HEARTBEAT_SECONDS)
            except TimeoutError:
                frame = _heartbeat()
            await websocket.send_text(frame)
    except (WebSocketDisconnect, RuntimeError, OSError):
        pass


@app.websocket("/ws/live")
async def ws_live(websocket: WebSocket):
    """Real-time event feed: streams processed Kafka events (gameplay,
    player events, server metrics) as they arrive, from the one shared
    consumer. Heartbeat frame every 15 s (HEARTBEAT_SECONDS) when idle."""
    await websocket.accept()
    q = state.feed.register()
    try:
        await _pump(websocket, q)
    finally:
        state.feed.unregister(q)


@app.websocket("/ws/alerts")
async def ws_alerts(websocket: WebSocket):
    """Alert notifications: every alert the Go alert engine publishes on
    Redis `alerts:stream`, fanned out from the one shared listener.
    Heartbeat frame every 15 s (HEARTBEAT_SECONDS) when idle."""
    await websocket.accept()
    q = state.alert_bus.subscribe()
    try:
        await _pump(websocket, q)
    finally:
        state.alert_bus.unsubscribe(q)
