"""Traders API routes."""

from fastapi import APIRouter, HTTPException, Query, Path, status
from typing import Optional
from app.schemas.traders import (
    TraderBasicInfo,
    TraderDetail,
    TradersListResponse,
    TraderTradesResponse,
    LeaderboardTrader,
    LeaderboardTradersResponse
)
from app.schemas.markets import PaginationInfo
from app.schemas.general import ErrorResponse
from app.services.trader_service import (
    get_traders_analytics_from_db,
    get_trader_basic_info,
    get_trader_detail,
    get_traders_list as fetch_traders_list
)
from app.services.data_fetcher import fetch_resolved_markets, fetch_trades_for_wallet, fetch_traders_from_leaderboard

router = APIRouter(prefix="/traders", tags=["Traders"])


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


from fastapi import APIRouter, HTTPException, Query, Path, status, BackgroundTasks

@router.post(
    "/sync",
    summary="Sync traders to database",
    description="Fetch traders from Leaderboard API and save/update them in the database. Done in background. Set limit=0 to sync all available traders."
)
async def sync_traders(
    background_tasks: BackgroundTasks,
    limit: int = Query(50, ge=0, description="Number of traders to sync (use 0 for all)")
):
    """Trigger synchronization of traders from API to Database."""
    from app.services.trader_service import sync_traders_to_db
    
    # Trigger sync in background
    background_tasks.add_task(sync_traders_to_db, limit)
    
    return {"message": f"Sync started in background for {'all' if limit == 0 else limit} traders", "limit": limit}


@router.get(
    "/analytics",
    summary="Get database leaderboards analytics",
    description="Get aggregated analytics and leaderboards from local database"
)
async def get_db_analytics():
    """Get full leaderboard analytics from database."""
    try:
        data = await get_traders_analytics_from_db()
        return data
    except Exception as e:
         raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generating analytics: {str(e)}"
        )


@router.get(
    "",
    response_model=TradersListResponse,
    summary="Get list of traders",
    description="Get a list of traders from the database with basic information"
)
async def get_traders(
    limit: int = Query(50, ge=1, le=100, description="Maximum number of traders to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination")
):
    """
    Get a list of traders from the database.
    """
    from app.services.trader_service import get_traders_from_db
    try:
        traders = await get_traders_from_db(limit=limit, offset=offset)
        return TradersListResponse(
            count=len(traders),
            traders=[TraderBasicInfo(**trader) for trader in traders]
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching traders list: {str(e)}"
        )


@router.get(
    "/leaderboard",
    response_model=LeaderboardTradersResponse,
    summary="Get traders from Polymarket Leaderboard",
    description="Fetch traders from Polymarket Leaderboard API with ranking and stats"
)
async def get_leaderboard_traders(
    category: str = Query("overall", description="Category filter: 'overall', 'politics', 'sports', etc."),
    time_period: str = Query("all", description="Time period: 'all', '1m', '3m', '6m', '1y'"),
    order_by: str = Query("VOL", description="Sort by: 'VOL', 'PNL', 'ROI'"),
    limit: int = Query(50, ge=1, le=100, description="Maximum number of traders to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination")
):
    """
    Get traders from Polymarket Leaderboard API.
    
    This endpoint fetches traders directly from Polymarket's leaderboard API,
    which includes ranking, volume, PnL, ROI, and other performance metrics.
    """
    try:
        traders, pagination_dict = await fetch_traders_from_leaderboard(
            category=category,
            time_period=time_period,
            order_by=order_by,
            limit=limit,
            offset=offset
        )
        
        pagination_info = None
        if pagination_dict:
            pagination_info = PaginationInfo(
                limit=pagination_dict["limit"],
                offset=pagination_dict["offset"],
                total=pagination_dict["total"],
                has_more=pagination_dict["has_more"]
            )
        
        return LeaderboardTradersResponse(
            count=len(traders),
            traders=[LeaderboardTrader(**trader) for trader in traders],
            pagination=pagination_info
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching traders from leaderboard: {str(e)}"
        )


@router.get(
    "/{wallet}",
    response_model=TraderDetail,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid wallet address"},
        404: {"model": ErrorResponse, "description": "Trader not found"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Get trader details",
    description="Get detailed information and analytics for a specific trader"
)
async def get_trader(
    wallet: str = Path(..., description="Wallet address of the trader")
):
    """
    Get detailed trader information including full analytics.
    
    Returns:
    - Basic trader info (wallet, trades, positions)
    - Performance metrics (wins, losses, PnL, win rate)
    - Final score
    - Category breakdown
    - Trade dates
    """
    if not validate_wallet(wallet):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid wallet address format: {wallet}. Must be 42 characters starting with 0x"
        )
    
    try:
        trader_data = await get_trader_detail(wallet)
        
        # Check if trader has any data
        if trader_data.get("total_trades", 0) == 0:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No trades found for trader {wallet}"
            )
        
        # Validate and return the data
        try:
            return TraderDetail(**trader_data)
        except Exception as validation_error:
            # If validation fails, log the error and return a more helpful message
            print(f"⚠ Validation error for trader {wallet}: {validation_error}")
            print(f"  Trader data keys: {list(trader_data.keys())}")
            print(f"  Trader data: {trader_data}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Data validation error: {str(validation_error)}. Trader data may be incomplete."
            )
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        print(f"✗ Error fetching trader data for {wallet}: {e}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching trader data: {str(e)}"
        )


@router.get(
    "/{wallet}/basic",
    response_model=TraderBasicInfo,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid wallet address"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Get trader basic info",
    description="Get basic information about a trader (faster than full details)"
)
async def get_trader_basic(
    wallet: str = Path(..., description="Wallet address of the trader")
):
    """
    Get basic trader information without full analytics calculation.
    Faster than the full details endpoint.
    """
    if not validate_wallet(wallet):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid wallet address format: {wallet}. Must be 42 characters starting with 0x"
        )
    
    try:
        markets = fetch_resolved_markets()
        trader_data = await get_trader_basic_info(wallet, markets)
        return TraderBasicInfo(**trader_data)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching trader basic info: {str(e)}"
        )


@router.get(
    "/{wallet}/trades",
    response_model=TraderTradesResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid wallet address"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Get trader trades",
    description="Get all trades for a specific trader"
)
async def get_trader_trades(
    wallet: str = Path(..., description="Wallet address of the trader"),
    limit: Optional[int] = Query(None, ge=1, le=1000, description="Maximum number of trades to return")
):
    """
    Get all trades for a specific trader.
    
    Returns raw trade data for the specified wallet address.
    """
    if not validate_wallet(wallet):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid wallet address format: {wallet}. Must be 42 characters starting with 0x"
        )
    
    try:
        trades = fetch_trades_for_wallet(wallet)
        
        if limit:
            trades = trades[:limit]
        
        return TraderTradesResponse(
            wallet_address=wallet,
            count=len(trades),
            trades=trades
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching trader trades: {str(e)}"
        )

