"""
FastAPI application main file.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.core.config import settings
from app.routers import general, markets, analytics, traders, positions, orders, pnl, profile_stats, activity, trades, leaderboard, closed_positions, scoring, trade_history
from app.db.session import init_db

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
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://localhost:3000",
    "http://127.0.0.1:3000",
]

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

# Include routers
app.include_router(general.router)
app.include_router(markets.router)
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
app.include_router(trade_history.router)

