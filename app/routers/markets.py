"""Markets API routes."""

from fastapi import APIRouter, HTTPException, status, Query, BackgroundTasks
from typing import Optional, Dict, Any
from app.schemas.markets import MarketsResponse, PaginationInfo
from app.services.data_fetcher import fetch_markets, fetch_market_orders, fetch_market_by_slug
from app.services.market_service import update_all_markets

router = APIRouter(prefix="/markets", tags=["Markets"])


@router.post("/update")
async def trigger_market_update(background_tasks: BackgroundTasks):
    """
    Manually trigger an update of all markets.
    This runs in the background.
    """
    background_tasks.add_task(update_all_markets)
    return {"message": "Market update job started in the background."}


@router.get("", response_model=MarketsResponse)
async def get_markets(
    status: str = Query("active", description="Market status: 'active', 'resolved', 'closed', etc."),
    limit: Optional[int] = Query(50, ge=1, le=100, description="Maximum number of markets to return (default: 50)"),
    offset: Optional[int] = Query(0, ge=0, description="Offset for pagination"),
    tag_slug: Optional[str] = Query(None, description="Filter by tag slug (e.g., 'sports', 'politics', 'crypto')")
):
    """Fetch markets from local database with pagination."""
    try:
        from app.db.session import AsyncSessionLocal
        from app.db.models import Market
        from sqlalchemy.future import select
        from sqlalchemy import func, desc

        async with AsyncSessionLocal() as session:
            # Build query
            query = select(Market)
            
            # Filter by status
            if status:
                query = query.where(Market.status == status)
                
            # Filter by tag (basic implementation - improved search would need specific DB design)
            if tag_slug:
                query = query.where(Market.tags.ilike(f"%{tag_slug}%"))
            
            # Count total
            count_query = select(func.count()).select_from(query.subquery())
            total_result = await session.execute(count_query)
            total = total_result.scalar()
            
            # Apply sorting (Volume desc) and pagination
            query = query.order_by(desc(Market.volume))
            query = query.offset(offset).limit(limit)
            
            # Execute
            result = await session.execute(query)
            db_markets = result.scalars().all()
            
            # Convert to dictionary format expected by schema
            markets_list = []
            for m in db_markets:
                # Deserialize JSON fields
                try:
                    import json
                    outcome_prices = json.loads(m.outcome_prices) if m.outcome_prices else {}
                    tags_list = json.loads(m.tags) if m.tags else []
                except:
                    outcome_prices = {}
                    tags_list = []

                markets_list.append({
                    "id": m.id,
                    "slug": m.slug,
                    "question": m.question,
                    "description": m.description,
                    "status": m.status,
                    "end_date": m.end_date.isoformat() if m.end_date else None,
                    "creation_date": m.creation_date.isoformat() if m.creation_date else None,
                    "volume": float(m.volume) if m.volume else 0,
                    "liquidity": float(m.liquidity) if m.liquidity else 0,
                    "openInterest": float(m.open_interest) if m.open_interest else 0,
                    "image": m.image,
                    "icon": m.icon,
                    "category": m.category,
                    "tags": tags_list,
                    "outcomePrices": outcome_prices
                })

            pagination_info = PaginationInfo(
                limit=limit,
                offset=offset,
                total=total or 0,
                has_more=(offset + limit) < (total or 0)
            )
            
            return MarketsResponse(
                count=len(markets_list),
                markets=markets_list,
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
    limit: Optional[int] = Query(5000, ge=1, le=100000, description="Maximum number of orders to return"),
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

