"""Traders API routes."""

from fastapi import APIRouter, HTTPException, Query, Path, status
from typing import Optional
from app.schemas.traders import (
    TraderBasicInfo,
    TraderDetail,
    TradersListResponse,
    TraderTradesResponse
)
from app.schemas.general import ErrorResponse
from app.services.trader_service import (
    get_trader_basic_info,
    get_trader_detail,
    get_traders_list as fetch_traders_list
)
from app.services.data_fetcher import fetch_resolved_markets, fetch_trades_for_wallet

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


@router.get(
    "",
    response_model=TradersListResponse,
    summary="Get list of traders",
    description="Get a list of traders extracted from markets with basic information"
)
async def get_traders(
    limit: int = Query(50, ge=1, le=100, description="Maximum number of traders to return")
):
    """
    Get a list of traders with basic information.
    
    This endpoint:
    1. Fetches resolved markets
    2. Extracts unique trader wallet addresses from market trades
    3. Returns basic information for each trader
    """
    try:
        traders = fetch_traders_list(limit=limit)
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
        trader_data = get_trader_detail(wallet)
        
        # Check if trader has any data
        if trader_data.get("total_trades", 0) == 0:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No trades found for trader {wallet}"
            )
        
        return TraderDetail(**trader_data)
    except HTTPException:
        raise
    except Exception as e:
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
        trader_data = get_trader_basic_info(wallet, markets)
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

