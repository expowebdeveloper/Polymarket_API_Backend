"""General API routes."""

from fastapi import APIRouter, Query, HTTPException, status
from typing import Optional
from app.schemas.general import HealthResponse, ErrorResponse
from app.core.config import settings
from app.core.constants import STATUS_HEALTHY
from app.services.data_fetcher import fetch_user_leaderboard_data

router = APIRouter(tags=["General"])


@router.get("/", response_model=dict)
async def root():
    """Root endpoint with API information."""
    return {
        "message": settings.API_TITLE,
        "version": settings.API_VERSION,
        "endpoints": {
            "/health": "Health check endpoint",
            # "/markets": "Get all resolved markets",
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


@router.get(
    "/user/leaderboard",
    responses={
        400: {"model": ErrorResponse, "description": "Invalid wallet address"},
        404: {"model": ErrorResponse, "description": "User not found in leaderboard"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Get user leaderboard data",
    description="Fetch user data from Polymarket leaderboard API including username, xUsername, profileImage, volume, etc."
)
async def get_user_leaderboard_data(
    user: str = Query(
        ...,
        description="Wallet address to fetch leaderboard data for (must be 42 characters starting with 0x)",
        example="0x4fd9856c1cd3b014846c301174ec0b9e93b1a49e",
        min_length=42,
        max_length=42
    ),
    category: Optional[str] = Query(
        "overall",
        description="Category filter (overall, politics, etc.)",
        example="overall"
    )
):
    """
    Get user leaderboard data from Polymarket API.
    
    Returns user information including:
    - userName
    - xUsername
    - profileImage
    - volume
    - pnl
    - rank
    - verifiedBadge
    """
    # Validate wallet address
    if not user.startswith("0x") or len(user) != 42:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid wallet address format: {user}. Must be 42 characters starting with 0x"
        )
    
    try:
        user_data = fetch_user_leaderboard_data(user, category=category)
        
        if not user_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User not found in leaderboard for address: {user}"
            )
        
        return user_data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching user leaderboard data: {str(e)}"
        )

