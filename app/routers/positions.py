"""Positions API routes."""

from fastapi import APIRouter, HTTPException, Query, status, Depends
from typing import List, Optional
from app.schemas.positions import PositionsListResponse, PositionResponse
from app.schemas.general import ErrorResponse
from app.services.position_service import fetch_and_save_positions, get_positions_from_db
from app.db.session import get_db
from sqlalchemy.ext.asyncio import AsyncSession
from decimal import Decimal

router = APIRouter(prefix="/positions", tags=["Positions"])


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
    response_model=PositionsListResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid wallet address"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Fetch and save positions",
    description="Fetch positions for a wallet address from Polymarket API and save them to the database"
)
async def fetch_and_save_positions_endpoint(
    user: str = Query(
        ...,
        description="Wallet address to fetch positions for (must be 42 characters starting with 0x)",
        example="0x17db3fcd93ba12d38382a0cade24b200185c5f6d",
        min_length=42,
        max_length=42
    ),
    sortBy: Optional[str] = Query(
        None,
        description="Sort field (e.g., 'CURRENT', 'INITIAL', 'PNL')",
        example="CURRENT"
    ),
    sortDirection: Optional[str] = Query(
        None,
        description="Sort direction ('ASC' or 'DESC')",
        example="DESC"
    ),
    sizeThreshold: Optional[float] = Query(
        None,
        description="Minimum size threshold",
        example=0.1
    ),
    limit: Optional[int] = Query(
        None,
        ge=1,
        description="Maximum number of positions to return",
        example=50
    ),
    offset: Optional[int] = Query(
        None,
        ge=0,
        description="Offset for pagination",
        example=0
    ),
    db: AsyncSession = Depends(get_db)
):
    """
    Fetch positions for a wallet address from Polymarket Data API and save them to the database.
    
    This endpoint:
    1. Validates the wallet address format
    2. Fetches positions from https://data-api.polymarket.com/positions with optional filters
    3. Saves each position to the database (updates if already exists)
    4. Returns the fetched positions
    
    Args:
        user: Wallet address (query parameter)
        sortBy: Sort field (e.g., "CURRENT", "INITIAL", "PNL") - optional
        sortDirection: Sort direction ("ASC" or "DESC") - optional
        sizeThreshold: Minimum size threshold (e.g., 0.1) - optional
        limit: Maximum number of positions to return - optional
        offset: Offset for pagination - optional
        db: Database session (injected)
    
    Returns:
        PositionsListResponse with wallet address, count, and list of positions
    """
    if not validate_wallet(user):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid wallet address format: {user}. Must be 42 characters starting with 0x"
        )
    
    try:
        # Fetch positions from API and save to database
        positions_data, saved_count = await fetch_and_save_positions(
            db, user,
            sort_by=sortBy,
            sort_direction=sortDirection,
            size_threshold=sizeThreshold,
            limit=limit,
            offset=offset
        )
        
        # Convert to response format
        positions_response = []
        for pos in positions_data:
            positions_response.append(PositionResponse(
                proxy_wallet=pos.get("proxyWallet", user),
                asset=str(pos.get("asset", "")),
                condition_id=pos.get("conditionId", ""),
                size=Decimal(str(pos.get("size", 0))),
                avg_price=Decimal(str(pos.get("avgPrice", 0))),
                initial_value=Decimal(str(pos.get("initialValue", 0))),
                current_value=Decimal(str(pos.get("currentValue", 0))),
                cash_pnl=Decimal(str(pos.get("cashPnl", 0))),
                percent_pnl=Decimal(str(pos.get("percentPnl", 0))),
                total_bought=Decimal(str(pos.get("totalBought", 0))),
                realized_pnl=Decimal(str(pos.get("realizedPnl", 0))),
                percent_realized_pnl=Decimal(str(pos.get("percentRealizedPnl", 0))),
                cur_price=Decimal(str(pos.get("curPrice", 0))),
                redeemable=pos.get("redeemable", False),
                mergeable=pos.get("mergeable", False),
                title=pos.get("title"),
                slug=pos.get("slug"),
                icon=pos.get("icon"),
                event_id=pos.get("eventId"),
                event_slug=pos.get("eventSlug"),
                outcome=pos.get("outcome"),
                outcome_index=pos.get("outcomeIndex"),
                opposite_outcome=pos.get("oppositeOutcome"),
                opposite_asset=str(pos.get("oppositeAsset", "")) if pos.get("oppositeAsset") else None,
                end_date=pos.get("endDate"),
                negative_risk=pos.get("negativeRisk", False),
            ))
        
        return PositionsListResponse(
            wallet_address=user,
            count=len(positions_response),
            positions=positions_response
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching and saving positions: {str(e)}"
        )


@router.get(
    "/from-db",
    response_model=PositionsListResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid wallet address"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Get positions from database",
    description="Retrieve positions for a wallet address from the database (without fetching from API)"
)
async def get_positions_from_db_endpoint(
    user: str = Query(
        ...,
        description="Wallet address to get positions for (must be 42 characters starting with 0x)",
        example="0x554ad2bc8a8f372d7e3376918fcb6e284387859a",
        min_length=42,
        max_length=42
    ),
    db: AsyncSession = Depends(get_db)
):
    """
    Get positions for a wallet address from the database.
    
    This endpoint retrieves positions that were previously saved to the database.
    Use the main /positions endpoint to fetch fresh data from the API.
    
    Args:
        user: Wallet address (query parameter)
        db: Database session (injected)
    
    Returns:
        PositionsListResponse with wallet address, count, and list of positions
    """
    if not validate_wallet(user):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid wallet address format: {user}. Must be 42 characters starting with 0x"
        )
    
    try:
        # Get positions from database
        positions = await get_positions_from_db(db, user)
        
        # Convert to response format
        positions_response = []
        for pos in positions:
            positions_response.append(PositionResponse(
                proxy_wallet=pos.proxy_wallet,
                asset=pos.asset,
                condition_id=pos.condition_id,
                size=pos.size,
                avg_price=pos.avg_price,
                initial_value=pos.initial_value,
                current_value=pos.current_value,
                cash_pnl=pos.cash_pnl,
                percent_pnl=pos.percent_pnl,
                total_bought=pos.total_bought,
                realized_pnl=pos.realized_pnl,
                percent_realized_pnl=pos.percent_realized_pnl,
                cur_price=pos.cur_price,
                redeemable=pos.redeemable,
                mergeable=pos.mergeable,
                title=pos.title,
                slug=pos.slug,
                icon=pos.icon,
                event_id=pos.event_id,
                event_slug=pos.event_slug,
                outcome=pos.outcome,
                outcome_index=pos.outcome_index,
                opposite_outcome=pos.opposite_outcome,
                opposite_asset=pos.opposite_asset,
                end_date=pos.end_date,
                negative_risk=pos.negative_risk,
            ))
        
        return PositionsListResponse(
            wallet_address=user,
            count=len(positions_response),
            positions=positions_response
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving positions from database: {str(e)}"
        )

