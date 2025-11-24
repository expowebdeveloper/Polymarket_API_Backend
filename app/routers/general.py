"""General API routes."""

from fastapi import APIRouter
from app.schemas.general import HealthResponse
from app.core.config import settings
from app.core.constants import STATUS_HEALTHY

router = APIRouter(tags=["General"])


@router.get("/", response_model=dict)
async def root():
    """Root endpoint with API information."""
    return {
        "message": settings.API_TITLE,
        "version": settings.API_VERSION,
        "endpoints": {
            "/health": "Health check endpoint",
            "/markets": "Get all resolved markets",
            "/analytics?wallet=<address>": "Get analytics for a specific wallet",
            "/docs": "Swagger UI documentation",
            "/redoc": "ReDoc documentation"
        }
    }


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    return HealthResponse(
        status=STATUS_HEALTHY,
        version=settings.API_VERSION,
        service=settings.API_TITLE
    )

