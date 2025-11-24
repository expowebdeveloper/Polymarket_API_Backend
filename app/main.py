"""
FastAPI application main file.
"""

from fastapi import FastAPI
from app.core.config import settings
from app.routers import general, markets, analytics, traders

app = FastAPI(
    title=settings.API_TITLE,
    version=settings.API_VERSION,
    description=settings.API_DESCRIPTION,
    docs_url="/docs",
    redoc_url="/redoc"
)

# Include routers
app.include_router(general.router)
app.include_router(markets.router)
app.include_router(analytics.router)
app.include_router(traders.router)

