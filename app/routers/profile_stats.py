"""Profile stats API routes."""

from fastapi import APIRouter, HTTPException, Query, status, Depends
from typing import Optional, List
from app.schemas.profile_stats import ProfileStatsResponse, EnhancedProfileStatsResponse
from app.schemas.general import ErrorResponse
from app.services.profile_stats_service import (
    fetch_and_save_profile_stats,
    get_profile_stats_from_db,
    get_enhanced_profile_stats,
    search_trader_by_username_or_wallet
)
from app.services.leaderboard_service import (
    get_unique_wallet_addresses,
    calculate_trader_metrics_with_time_filter,
    calculate_scores_and_rank
)
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
        import traceback
        print(f"ValueError in profile stats: {e}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        import traceback
        print(f"Error fetching and saving profile stats for {proxyAddress}: {e}")
        print(traceback.format_exc())
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


@router.get(
    "/enhanced",
    response_model=EnhancedProfileStatsResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid search query"},
        404: {"model": ErrorResponse, "description": "Trader not found"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Get enhanced profile stats",
    description="Get comprehensive profile statistics including scoring, streaks, and all metrics"
)
async def get_enhanced_profile_stats_endpoint(
    wallet: Optional[str] = Query(
        None,
        description="Wallet address to get stats for",
        example="0x17db3fcd93ba12d38382a0cade24b200185c5f6d"
    ),
    username: Optional[str] = Query(
        None,
        description="Username to search for",
        example="fengdubiying"
    ),
    search: Optional[str] = Query(
        None,
        description="Search by username or wallet address",
        example="fengdubiying"
    ),
    db: AsyncSession = Depends(get_db)
):
    """
    Get enhanced profile statistics with scoring, streaks, and all metrics.
    
    You can search by:
    - wallet: Direct wallet address
    - username: Username parameter
    - search: Search query (will try to match username or wallet)
    
    Returns comprehensive stats including:
    - Final Score, Top %, Ranking Tag
    - Longest/Current Winning Streaks
    - Biggest Win, Worst Loss, Maximum Stake
    - Portfolio Value, Average Stake Value
    """
    wallet_address = None
    
    # Determine wallet address from parameters
    if wallet:
        if not validate_wallet(wallet):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid wallet address format: {wallet}"
            )
        wallet_address = wallet
    elif search:
        # Search for trader
        wallet_address = await search_trader_by_username_or_wallet(db, search)
        if not wallet_address:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Trader not found: {search}"
            )
    elif username:
        # Search by username
        wallet_address = await search_trader_by_username_or_wallet(db, username)
        if not wallet_address:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Trader not found with username: {username}"
            )
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Must provide either wallet, username, or search parameter"
        )
    
    try:
        stats = await get_enhanced_profile_stats(db, wallet_address, username)
        
        if not stats:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Profile stats not found for: {wallet_address}"
            )
        
        return EnhancedProfileStatsResponse(**stats)
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        print(f"Error getting enhanced profile stats: {e}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving enhanced profile stats: {str(e)}"
        )


@router.get(
    "/top-traders",
    response_model=List[EnhancedProfileStatsResponse],
    responses={
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Get top traders",
    description="Get top N traders ranked by final score"
)
async def get_top_traders_endpoint(
    limit: int = Query(
        3,
        ge=1,
        le=100,
        description="Number of top traders to return"
    ),
    db: AsyncSession = Depends(get_db)
):
    """
    Get top traders ranked by final score.
    
    Returns the top N traders with all enhanced profile stats.
    """
    try:
        # Get all wallets
        wallets = await get_unique_wallet_addresses(db)
        
        # Calculate metrics for all traders
        all_metrics = []
        for wallet in wallets:
            try:
                metrics = await calculate_trader_metrics_with_time_filter(db, wallet, period='all')
                if metrics and metrics.get('total_trades', 0) > 0:
                    all_metrics.append(metrics)
            except Exception:
                continue
        
        if not all_metrics:
            return []
        
        # Calculate scores and rank
        scored_traders = calculate_scores_and_rank(all_metrics)
        scored_traders.sort(key=lambda x: x.get('final_score', 0), reverse=True)
        
        # Get top N traders
        top_traders = scored_traders[:limit]
        
        # Get enhanced stats for each
        results = []
        for idx, trader in enumerate(top_traders, 1):
            wallet_addr = trader.get('wallet_address')
            if wallet_addr:
                stats = await get_enhanced_profile_stats(db, wallet_addr)
                if stats:
                    stats['rank'] = idx
                    results.append(EnhancedProfileStatsResponse(**stats))
        
        return results
    except Exception as e:
        import traceback
        print(f"Error getting top traders: {e}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving top traders: {str(e)}"
        )

