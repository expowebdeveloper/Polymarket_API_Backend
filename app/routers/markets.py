"""Markets API routes."""

from fastapi import APIRouter, HTTPException, status
from app.schemas.markets import MarketsResponse
from app.services.data_fetcher import fetch_resolved_markets

router = APIRouter(prefix="/markets", tags=["Markets"])


@router.get("", response_model=MarketsResponse)
async def get_markets():
    """Fetch all resolved markets from Polymarket API."""
    try:
        markets = fetch_resolved_markets()
        return MarketsResponse(
            count=len(markets),
            markets=markets
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching markets: {str(e)}"
        )

