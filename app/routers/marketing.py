"""Marketing API routes for live market data."""

from fastapi import APIRouter, HTTPException, status, Query
from typing import Optional, List, Any
from app.schemas.markets import MarketsResponse, PaginationInfo
from app.services.data_fetcher import fetch_markets

router = APIRouter(prefix="/marketing", tags=["Marketing"])

@router.get("/markets", response_model=MarketsResponse)
async def get_live_markets(
    limit: int = Query(50, ge=1, le=100, description="Maximum number of markets to return (default: 50)"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    tag_slug: Optional[str] = Query(None, description="Filter by tag slug (e.g., 'sports', 'politics', 'crypto')")
):
    """
    Fetch LIVE markets directly from Polymarket API (Gamma) with pagination.
    Bypasses the local database for fresh data.
    """
    try:
        # Fetch directly from data fetcher service (Gamma API)
        # We assume fetch_markets handles the API calls efficiently
        markets, pagination = await fetch_markets(
            status="active",
            limit=limit,
            offset=offset,
            tag_slug=tag_slug
        )
        
        # Transform keys if necessary to match MarketsResponse schema/frontend expectations
        # The data_fetcher returns a list of dictionaries.
        # We need to ensure consistency with what the frontend expects (camelCase vs snake_case).
        # MarketsResponse expects simple List[Any] for markets, so dicts are fine.
        
        # Construct PaginationInfo
        # Data fetcher returns a dict with pagination info
        pagination_info = PaginationInfo(
            limit=pagination.get("limit", limit),
            offset=pagination.get("offset", offset),
            total=pagination.get("total", 0),
            has_more=pagination.get("has_more", False)
        )
        
        return MarketsResponse(
            count=len(markets),
            markets=markets,
            pagination=pagination_info
        )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching live marketing data: {str(e)}"
        )
