"""Trades API routes."""

from fastapi import APIRouter, HTTPException, Query, status, Depends
from typing import Optional
from app.schemas.trades import TradesListResponse, TradeResponse
from app.schemas.general import ErrorResponse
from app.services.trade_service import fetch_and_save_trades, get_trades_from_db
from app.services.trade_data_processor import process_and_insert_trade_data
from app.db.session import get_db
from sqlalchemy.ext.asyncio import AsyncSession
from decimal import Decimal

router = APIRouter(prefix="/trades", tags=["Trades"])


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
    response_model=TradesListResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid wallet address"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Fetch and save user trades",
    description="Fetch user trades from Polymarket API and save them to the database"
)
async def fetch_and_save_trades_endpoint(
    user: str = Query(
        ...,
        description="Wallet address to fetch trades for (must be 42 characters starting with 0x)",
        example="0xdbade4c82fb72780a0db9a38f821d8671aba9c95",
        min_length=42,
        max_length=42
    ),
    db: AsyncSession = Depends(get_db)
):
    """
    Fetch user trades from Polymarket Data API and save them to the database.
    
    This endpoint:
    1. Validates the wallet address format
    2. Fetches trades from https://data-api.polymarket.com/trades?user={wallet}
    3. Saves each trade to the database (updates if already exists)
    4. Returns the fetched trades
    
    Args:
        user: Wallet address (query parameter)
        db: Database session (injected)
    
    Returns:
        TradesListResponse with wallet address, count, and list of trades
    """
    if not validate_wallet(user):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid wallet address format: {user}. Must be 42 characters starting with 0x"
        )
    
    try:
        # Fetch trades from API and save to database
        trades_data, saved_count = await fetch_and_save_trades(db, user)
        
        # Convert to response format
        trades_response = []
        for trade in trades_data:
            trades_response.append(TradeResponse(
                proxy_wallet=trade.get("proxyWallet", user),
                side=trade.get("side", ""),
                asset=str(trade.get("asset", "")),
                condition_id=trade.get("conditionId", ""),
                size=Decimal(str(trade.get("size", 0))),
                price=Decimal(str(trade.get("price", 0))),
                timestamp=trade.get("timestamp", 0),
                title=trade.get("title"),
                slug=trade.get("slug"),
                icon=trade.get("icon"),
                event_slug=trade.get("eventSlug"),
                outcome=trade.get("outcome"),
                outcome_index=trade.get("outcomeIndex"),
                name=trade.get("name"),
                pseudonym=trade.get("pseudonym"),
                bio=trade.get("bio"),
                profile_image=trade.get("profileImage"),
                profile_image_optimized=trade.get("profileImageOptimized"),
                transaction_hash=trade.get("transactionHash", ""),
            ))
        
        return TradesListResponse(
            wallet_address=user,
            count=len(trades_response),
            trades=trades_response
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching and saving trades: {str(e)}"
        )


@router.get(
    "/from-db",
    response_model=TradesListResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid wallet address"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Get user trades from database",
    description="Retrieve user trades from the database (without fetching from API)"
)
async def get_trades_from_db_endpoint(
    user: str = Query(
        ...,
        description="Wallet address to get trades for (must be 42 characters starting with 0x)",
        example="0xdbade4c82fb72780a0db9a38f821d8671aba9c95",
        min_length=42,
        max_length=42
    ),
    side: Optional[str] = Query(
        None,
        description="Filter by side (BUY/SELL)"
    ),
    limit: Optional[int] = Query(
        None,
        ge=1,
        description="Maximum number of trades to return"
    ),
    db: AsyncSession = Depends(get_db)
):
    """
    Get user trades from the database with optional filters.
    
    This endpoint retrieves trades that were previously saved to the database.
    Use the main /trades endpoint to fetch fresh data from the API.
    
    Args:
        user: Wallet address (query parameter)
        side: Filter by side - BUY or SELL (optional)
        limit: Maximum number of trades to return (optional)
        db: Database session (injected)
    
    Returns:
        TradesListResponse with wallet address, count, and list of trades
    """
    if not validate_wallet(user):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid wallet address format: {user}. Must be 42 characters starting with 0x"
        )
    
    if side and side.upper() not in ["BUY", "SELL"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Side must be either 'BUY' or 'SELL'"
        )
    
    try:
        # Get trades from database
        trades = await get_trades_from_db(db, user, side=side, limit=limit)
        
        # Convert to response format
        trades_response = []
        for trade in trades:
            trades_response.append(TradeResponse(
                proxy_wallet=trade.proxy_wallet,
                side=trade.side,
                asset=trade.asset,
                condition_id=trade.condition_id,
                size=trade.size,
                price=trade.price,
                timestamp=trade.timestamp,
                title=trade.title,
                slug=trade.slug,
                icon=trade.icon,
                event_slug=trade.event_slug,
                outcome=trade.outcome,
                outcome_index=trade.outcome_index,
                name=trade.name,
                pseudonym=trade.pseudonym,
                bio=trade.bio,
                profile_image=trade.profile_image,
                profile_image_optimized=trade.profile_image_optimized,
                transaction_hash=trade.transaction_hash,
            ))
        
        return TradesListResponse(
            wallet_address=user,
            count=len(trades_response),
            trades=trades_response
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving trades from database: {str(e)}"
        )


@router.post(
    "/process",
    responses={
        400: {"model": ErrorResponse, "description": "Invalid wallet address"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Process and insert trade data",
    description="Read trade data, clean it (remove duplicates, fix missing values), calculate PnL, and insert into database with Traders, Trades, and Aggregated Metrics"
)
async def process_trade_data_endpoint(
    user: str = Query(
        ...,
        description="Wallet address to process trades for (must be 42 characters starting with 0x)",
        example="0xdbade4c82fb72780a0db9a38f821d8671aba9c95",
        min_length=42,
        max_length=42
    ),
    db: AsyncSession = Depends(get_db)
):
    """
    Process and insert trade data into database.
    
    This endpoint:
    1. Fetches trade data from API
    2. Cleans data (removes duplicates, fixes missing values)
    3. Calculates entry/exit prices and PnL
    4. Creates/updates Trader record
    5. Inserts trades into database (linked to Trader ID)
    6. Calculates and inserts aggregated metrics
    
    Args:
        user: Wallet address (query parameter)
        db: Database session (injected)
    
    Returns:
        Dictionary with processing results including metrics
    """
    if not validate_wallet(user):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid wallet address format: {user}. Must be 42 characters starting with 0x"
        )
    
    try:
        result = await process_and_insert_trade_data(db, user)
        return result
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error processing trade data: {str(e)}"
        )


@router.post(
    "/sync",
    responses={
        400: {"model": ErrorResponse, "description": "Invalid wallet address"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Optimized trade sync",
    description="Sync only new trades from Polymarket API (incremental update)."
)
async def sync_trades_endpoint(
    user: str = Query(
        ...,
        description="Wallet address to sync trades for",
        min_length=42,
        max_length=42
    ),
    db: AsyncSession = Depends(get_db)
):
    """
    Sync only new trades from Polymarket API.
    
    This endpoint:
    1. Checks the latest trade timestamp in the DB.
    2. Fetches trades from API in batches.
    3. Stops fetching when it sees trades older than the DB timestamp.
    4. Saves only the new trades.
    """
    if not validate_wallet(user):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid wallet address format: {user}"
        )
    
    try:
        # Get latest timestamp from DB
        from app.services.trade_service import get_latest_trade_timestamp, sync_trades_since_timestamp
        
        latest_ts = await get_latest_trade_timestamp(db, user)
        
        # Sync new trades
        saved_count = await sync_trades_since_timestamp(db, user, min_timestamp=latest_ts)
        
        return {
            "message": "Sync completed",
            "wallet": user,
            "new_trades_saved": saved_count,
            "previous_latest_timestamp": latest_ts
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error syncing trades: {str(e)}"
        )
