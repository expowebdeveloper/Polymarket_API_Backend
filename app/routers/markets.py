"""Markets API routes."""

from fastapi import APIRouter, HTTPException, status, Query
from typing import Optional
from app.schemas.markets import MarketsResponse, PaginationInfo
from app.services.data_fetcher import fetch_markets, fetch_market_orders

router = APIRouter(prefix="/markets", tags=["Markets"])


@router.get("", response_model=MarketsResponse)
async def get_markets(
    status: str = Query("active", description="Market status: 'active', 'resolved', 'closed', etc."),
    limit: Optional[int] = Query(20, ge=1, le=100, description="Maximum number of markets to return"),
    offset: Optional[int] = Query(0, ge=0, description="Offset for pagination")
):
    """Fetch markets from Polymarket API with pagination."""
    try:
        markets, pagination_dict = fetch_markets(status=status, limit=limit, offset=offset)
        
        pagination_info = None
        if pagination_dict:
            pagination_info = PaginationInfo(
                limit=pagination_dict["limit"],
                offset=pagination_dict["offset"],
                total=pagination_dict["total"],
                has_more=pagination_dict["has_more"]
            )
        
        return MarketsResponse(
            count=len(markets),
            markets=markets,
            pagination=pagination_info
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching markets: {str(e)}"
        )


@router.get("/orders")
async def get_market_orders(
    market_slug: str = Query(..., description="Market slug identifier"),
    limit: Optional[int] = Query(100, ge=1, le=1000, description="Maximum number of orders to return"),
    offset: Optional[int] = Query(0, ge=0, description="Offset for pagination")
):
    """Fetch orders for a specific market from DomeAPI."""
    try:
        result = fetch_market_orders(market_slug=market_slug, limit=limit, offset=offset)
        return result
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching market orders: {str(e)}"
        )

