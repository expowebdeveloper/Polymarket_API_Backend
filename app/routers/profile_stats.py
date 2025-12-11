"""Profile stats API routes."""

from fastapi import APIRouter, HTTPException, Query, status, Depends
from typing import Optional
from app.schemas.profile_stats import ProfileStatsResponse
from app.schemas.general import ErrorResponse
from app.services.profile_stats_service import fetch_and_save_profile_stats, get_profile_stats_from_db
from app.db.session import get_db
from sqlalchemy.ext.asyncio import AsyncSession
from decimal import Decimal

router = APIRouter(prefix="/profile/stats", tags=["Profile Stats"])


def validate_wallet(wallet_address: str) -> bool:
    """Validate wallet address format."""
    if not wallet_address:
        return False
    if not wallet_address.startswith("0x"):
        return False
    if len(wallet_address) != 42:
        return False
    try:
        int(wallet_address[2:], 16)
        return True
    except:
        return False


@router.get(
    "",
    response_model=ProfileStatsResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid wallet address"},
        404: {"model": ErrorResponse, "description": "Profile stats not found"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Fetch and save profile stats",
    description="Fetch profile statistics from Polymarket API and save them to the database"
)
async def fetch_and_save_profile_stats_endpoint(
    proxyAddress: str = Query(
        ...,
        description="Wallet address to fetch stats for (must be 42 characters starting with 0x)",
        example="0x17db3fcd93ba12d38382a0cade24b200185c5f6d",
        min_length=42,
        max_length=42
    ),
    username: Optional[str] = Query(
        None,
        description="Optional username",
        example="fengdubiying"
    ),
    db: AsyncSession = Depends(get_db)
):
    """
    Fetch profile statistics from Polymarket API and save them to the database.
    
    This endpoint:
    1. Validates the wallet address format
    2. Fetches profile stats from https://polymarket.com/api/profile/stats
    3. Saves the stats to the database (updates if already exists)
    4. Returns the profile stats
    
    Args:
        proxyAddress: Wallet address (query parameter)
        username: Optional username (query parameter)
        db: Database session (injected)
    
    Returns:
        ProfileStatsResponse with trades, largestWin, views, and joinDate
    """
    if not validate_wallet(proxyAddress):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid wallet address format: {proxyAddress}. Must be 42 characters starting with 0x"
        )
    
    try:
        # Fetch profile stats from API and save to database
        stats_data, saved_stats = await fetch_and_save_profile_stats(
            db, proxyAddress, username=username
        )
        
        if not stats_data or not saved_stats:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Profile stats not found for address: {proxyAddress}"
            )
        
        return ProfileStatsResponse(
            proxy_address=saved_stats.proxy_address,
            username=saved_stats.username,
            trades=saved_stats.trades,
            largest_win=saved_stats.largest_win,
            views=saved_stats.views,
            join_date=saved_stats.join_date
        )
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching and saving profile stats: {str(e)}"
        )


@router.get(
    "/from-db",
    response_model=ProfileStatsResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid wallet address"},
        404: {"model": ErrorResponse, "description": "Profile stats not found in database"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Get profile stats from database",
    description="Retrieve profile statistics from the database (without fetching from API)"
)
async def get_profile_stats_from_db_endpoint(
    proxyAddress: str = Query(
        ...,
        description="Wallet address to get stats for (must be 42 characters starting with 0x)",
        example="0x17db3fcd93ba12d38382a0cade24b200185c5f6d",
        min_length=42,
        max_length=42
    ),
    username: Optional[str] = Query(
        None,
        description="Optional username",
        example="fengdubiying"
    ),
    db: AsyncSession = Depends(get_db)
):
    """
    Get profile statistics from the database.
    
    This endpoint retrieves profile stats that were previously saved to the database.
    Use the main /profile/stats endpoint to fetch fresh data from the API.
    
    Args:
        proxyAddress: Wallet address (query parameter)
        username: Optional username (query parameter)
        db: Database session (injected)
    
    Returns:
        ProfileStatsResponse with trades, largestWin, views, and joinDate
    """
    if not validate_wallet(proxyAddress):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid wallet address format: {proxyAddress}. Must be 42 characters starting with 0x"
        )
    
    try:
        # Get profile stats from database
        profile_stats = await get_profile_stats_from_db(db, proxyAddress, username=username)
        
        if not profile_stats:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Profile stats not found in database for address: {proxyAddress}"
            )
        
        return ProfileStatsResponse(
            proxy_address=profile_stats.proxy_address,
            username=profile_stats.username,
            trades=profile_stats.trades,
            largest_win=profile_stats.largest_win,
            views=profile_stats.views,
            join_date=profile_stats.join_date
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving profile stats from database: {str(e)}"
        )

