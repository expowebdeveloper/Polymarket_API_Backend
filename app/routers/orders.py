"""Orders API routes."""

from fastapi import APIRouter, HTTPException, Query, status, Depends
from typing import Optional, List
from app.schemas.orders import OrdersListResponse, OrderResponse, PaginationInfo
from app.schemas.general import ErrorResponse
from app.services.order_service import fetch_and_save_orders, get_orders_from_db
from app.db.session import get_db
from sqlalchemy.ext.asyncio import AsyncSession
from decimal import Decimal

router = APIRouter(prefix="/orders", tags=["Orders"])


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
    response_model=OrdersListResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid parameters"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Fetch and save orders",
    description="Fetch orders from Dome API and save them to the database"
)
async def fetch_and_save_orders_endpoint(
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of orders to fetch"),
    status: Optional[str] = Query(None, description="Order status filter (e.g., 'closed', 'open')"),
    market_slug: Optional[str] = Query(None, description="Filter by market slug"),
    user: Optional[str] = Query(None, description="Filter by wallet address"),
    db: AsyncSession = Depends(get_db)
):
    """
    Fetch orders from Dome API and save them to the database.
    
    This endpoint:
    1. Fetches orders from https://api.domeapi.io/v1/polymarket/orders
    2. Saves each order to the database (updates if already exists based on order_hash)
    3. Returns the fetched orders with pagination info
    
    Args:
        limit: Maximum number of orders to fetch (1-1000)
        status: Order status filter (optional)
        market_slug: Filter by market slug (optional)
        user: Wallet address filter (optional)
        db: Database session (injected)
    
    Returns:
        OrdersListResponse with orders list and pagination info
    """
    if user and not validate_wallet(user):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid wallet address format: {user}. Must be 42 characters starting with 0x"
        )
    
    try:
        # Fetch orders from API and save to database
        orders_data, saved_count, pagination = await fetch_and_save_orders(
            db, limit=limit, status=status, market_slug=market_slug, user=user
        )
        
        # Convert to response format
        orders_response = []
        for order in orders_data:
            orders_response.append(OrderResponse(
                token_id=str(order.get("token_id", "")),
                token_label=order.get("token_label", ""),
                side=order.get("side", ""),
                market_slug=order.get("market_slug", ""),
                condition_id=order.get("condition_id", ""),
                shares=Decimal(str(order.get("shares", 0))),
                price=Decimal(str(order.get("price", 0))),
                tx_hash=order.get("tx_hash", ""),
                title=order.get("title"),
                timestamp=order.get("timestamp", 0),
                order_hash=order.get("order_hash", ""),
                user=order.get("user", ""),
                taker=order.get("taker", ""),
                shares_normalized=Decimal(str(order.get("shares_normalized", 0))),
            ))
        
        pagination_info = None
        if pagination:
            pagination_info = PaginationInfo(
                limit=pagination.get("limit", limit),
                offset=pagination.get("offset", 0),
                total=pagination.get("total", len(orders_response)),
                has_more=pagination.get("has_more", False)
            )
        
        return OrdersListResponse(
            count=len(orders_response),
            orders=orders_response,
            pagination=pagination_info
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching and saving orders: {str(e)}"
        )


@router.get(
    "/from-db",
    response_model=OrdersListResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid parameters"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Get orders from database",
    description="Retrieve orders from the database (without fetching from API)"
)
async def get_orders_from_db_endpoint(
    user: Optional[str] = Query(None, description="Filter by wallet address"),
    market_slug: Optional[str] = Query(None, description="Filter by market slug"),
    side: Optional[str] = Query(None, description="Filter by side (BUY/SELL)"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of orders to return"),
    db: AsyncSession = Depends(get_db)
):
    """
    Get orders from the database with optional filters.
    
    This endpoint retrieves orders that were previously saved to the database.
    Use the main /orders endpoint to fetch fresh data from the API.
    
    Args:
        user: Filter by wallet address (optional)
        market_slug: Filter by market slug (optional)
        side: Filter by side - BUY or SELL (optional)
        limit: Maximum number of orders to return
        db: Database session (injected)
    
    Returns:
        OrdersListResponse with orders list
    """
    if user and not validate_wallet(user):
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
        # Get orders from database
        orders = await get_orders_from_db(
            db, user=user, market_slug=market_slug, side=side.upper() if side else None, limit=limit
        )
        
        # Convert to response format
        orders_response = []
        for order in orders:
            orders_response.append(OrderResponse(
                token_id=order.token_id,
                token_label=order.token_label,
                side=order.side,
                market_slug=order.market_slug,
                condition_id=order.condition_id,
                shares=order.shares,
                price=order.price,
                tx_hash=order.tx_hash,
                title=order.title,
                timestamp=order.timestamp,
                order_hash=order.order_hash,
                user=order.user,
                taker=order.taker,
                shares_normalized=order.shares_normalized,
            ))
        
        return OrdersListResponse(
            count=len(orders_response),
            orders=orders_response,
            pagination=None
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving orders from database: {str(e)}"
        )

