"""
FastAPI application main file.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.core.config import settings
from app.routers import general, markets, traders, positions, orders, pnl, profile_stats, activity, trades, leaderboard, closed_positions, scoring, trade_history, dashboard, auth, websocket, marketing
from app.db.session import init_db
import os
import logging

logger = logging.getLogger(__name__)

app = FastAPI(
    title=settings.API_TITLE,
    version=settings.API_VERSION,
    description=settings.API_DESCRIPTION,
    docs_url="/docs",
    redoc_url="/redoc"
)

# Configure CORS
# Allow requests from frontend domains
allowed_origins = [
    "https://polymarket-ui-one.vercel.app",  # Current frontend domain
    "https://polymarket-uimain.vercel.app",   # Alternative frontend domain
    "https://polyrating.com",
    "https://www.polyrating.com",
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://localhost:5174",
    "http://127.0.0.1:5174",
    "http://localhost:5175",
    "http://127.0.0.1:5175",
    "http://127.0.0.1:3000",
]

# Add production origins from env
import os
env_origins = os.getenv("ALLOW_ORIGINS", "")
if env_origins:
    allowed_origins.extend([origin.strip() for origin in env_origins.split(",")])

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH", "HEAD"],
    allow_headers=["*"],
    expose_headers=["*"],
    max_age=600,  # Cache preflight requests for 10 minutes
)

@app.on_event("startup")
async def startup_event():
    """Initialize database on application startup."""
    await init_db()
    
    # Activity broadcaster: fetches Polymarket live trades and pushes to /ws/activity.
    # It runs either (1) on startup if ENABLE_ACTIVITY_BROADCASTER=true, or
    # (2) when the first client connects to the WebSocket (e.g. Dashboard "Live Feed").
    # Requires outbound internet (gamma-api + data-api + CLOB WS). If DNS/network fails,
    # it backs off to avoid log spam (see activity_broadcaster backoff).
    enable_broadcaster = os.getenv("ENABLE_ACTIVITY_BROADCASTER", "false").lower() == "true"

    if enable_broadcaster:
        from app.services.activity_broadcaster import broadcaster
        import asyncio
        asyncio.create_task(broadcaster.start())
        logger.info("🚀 Activity broadcaster auto-start enabled")
    else:
        logger.info("⏸️  Activity broadcaster disabled (set ENABLE_ACTIVITY_BROADCASTER=true to enable)")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup tasks on application shutdown."""
    from app.services.activity_broadcaster import broadcaster
    await broadcaster.stop()

# Include routers
app.include_router(auth.router)
app.include_router(dashboard.router, prefix="/dashboard", tags=["Dashboard"])
app.include_router(general.router)
app.include_router(markets.router)
app.include_router(traders.router)
app.include_router(positions.router)
app.include_router(orders.router)
app.include_router(pnl.router)
app.include_router(profile_stats.router)
app.include_router(activity.router)
app.include_router(trades.router)
app.include_router(leaderboard.router)
app.include_router(closed_positions.router)
app.include_router(scoring.router)
app.include_router(trade_history.router)
app.include_router(dashboard.router)
app.include_router(websocket.router)  # WebSocket for real-time activity feed
app.include_router(marketing.router)

