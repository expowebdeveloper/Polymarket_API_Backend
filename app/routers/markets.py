"""Markets API routes."""

from fastapi import APIRouter, HTTPException, status, Query
from typing import Optional, Dict, Any
from app.schemas.markets import MarketsResponse, PaginationInfo
from app.services.data_fetcher import fetch_markets, fetch_market_orders, fetch_market_by_slug

router = APIRouter(prefix="/markets", tags=["Markets"])


@router.get("", response_model=MarketsResponse)
async def get_markets(
    status: str = Query("active", description="Market status: 'active', 'resolved', 'closed', etc."),
    limit: Optional[int] = Query(50, ge=1, le=100, description="Maximum number of markets to return (default: 50)"),
    offset: Optional[int] = Query(0, ge=0, description="Offset for pagination"),
    tag_slug: Optional[str] = Query(None, description="Filter by tag slug (e.g., 'sports', 'politics', 'crypto')")
):
    """Fetch markets from Polymarket Gamma API with pagination (async)."""
    try:
        markets, pagination_dict = await fetch_markets(status=status, limit=limit, offset=offset, tag_slug=tag_slug)
        
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
    """Fetch orders for a specific market from Polymarket Data API only."""
    try:
        result = fetch_market_orders(market_slug=market_slug, limit=limit, offset=offset)
        return result
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching market orders: {str(e)}"
        )


@router.get("/{market_slug}")
async def get_market_details(market_slug: str) -> Dict[str, Any]:
    """Fetch market details by slug from Polymarket API only (async)."""
    try:
        market = await fetch_market_by_slug(market_slug)
        if not market:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Market '{market_slug}' not found"
            )
        return market
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching market details: {str(e)}"
        )

