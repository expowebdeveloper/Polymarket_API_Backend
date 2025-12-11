"""
FastAPI application main file.
"""

from fastapi import FastAPI
from app.core.config import settings
from app.routers import general, markets, analytics, traders, positions, orders, pnl, profile_stats, activity, trades, leaderboard, closed_positions, scoring
from app.db.session import init_db

app = FastAPI(
    title=settings.API_TITLE,
    version=settings.API_VERSION,
    description=settings.API_DESCRIPTION,
    docs_url="/docs",
    redoc_url="/redoc"
)

@app.on_event("startup")
async def startup_event():
    """Initialize database on application startup."""
    await init_db()

# Include routers
app.include_router(general.router)
# app.include_router(markets.router)
# app.include_router(analytics.router)
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

