"""FastAPI Application Entrypoint for Gaming Intelligence Platform."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime

app = FastAPI(
    title="Gaming Intelligence Platform API",
    description="Real-Time Competitive Gaming Analytics, Cheat Detection, and Server Health Monitoring",
    version="1.0.0"
)

# Enable CORS for dashboard development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {
        "status": "online",
        "service": "gaming-intelligence-api",
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "phase": "Phase 0 (Environment & Scaffolding)",
        "components": {
            "api": "ready",
            "redis": "configured",
            "postgres": "configured"
        }
    }

# Phase 4 will mount the following routers:
# - app.include_router(servers.router, prefix="/api/v1/servers", tags=["Servers"])
# - app.include_router(matches.router, prefix="/api/v1/matches", tags=["Matches"])
# - app.include_router(players.router, prefix="/api/v1/players", tags=["Players"])
# - app.include_router(alerts.router, prefix="/api/v1/alerts", tags=["Alerts"])
# - app.include_router(websocket.router, prefix="/ws", tags=["WebSockets"])
