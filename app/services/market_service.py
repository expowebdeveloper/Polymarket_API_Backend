import asyncio
import json
from datetime import datetime
from typing import List, Dict, Any, Optional
from sqlalchemy.future import select
from sqlalchemy.exc import IntegrityError
from app.db.session import AsyncSessionLocal
from app.db.models import Market
from app.services.data_fetcher import fetch_markets

async def update_all_markets():
    """
    Fetches all markets (active, closed, resolved) from Polymarket and updates the database.
    """
    print(f"[{datetime.utcnow()}] Starting market update job...")
    
    statuses = ["active", "closed", "resolved"]
    
    async with AsyncSessionLocal() as session:
        total_updated = 0
        
        for status in statuses:
            print(f"Fetching {status} markets...")
            offset = 0
            limit = 100 
            
            while True:
                markets, pagination = await fetch_markets(status=status, limit=limit, offset=offset)
                
                if not markets:
                    break
                
                for market_data in markets:
                    await upsert_market(session, market_data)
                    total_updated += 1
                
                if not pagination.get("has_more"):
                    break
                
                offset += limit
                
                # Small delay to respect rate limits
                await asyncio.sleep(0.1)

        await session.commit()
        print(f"[{datetime.utcnow()}] Market update job completed. Updated {total_updated} markets.")

async def upsert_market(session, market_data: Dict[str, Any]):
    """
    Insert or update a market in the database.
    """
    market_id = market_data.get("id")
    if not market_id:
        return

    # Parse dates
    end_date = parse_date(market_data.get("endDate") or market_data.get("end_date"))
    creation_date = parse_date(market_data.get("creationDate") or market_data.get("createdAt"))
    
    # Serialize complex types
    outcome_prices = json.dumps(market_data.get("outcomePrices", {}))
    tags = json.dumps(market_data.get("tags", []))

    # Check if market exists
    stmt = select(Market).where(Market.id == market_id)
    result = await session.execute(stmt)
    existing_market = result.scalar_one_or_none()

    if existing_market:
        # Update existing
        existing_market.slug = market_data.get("slug")
        existing_market.question = market_data.get("question")
        existing_market.description = market_data.get("description")
        existing_market.status = market_data.get("status")
        existing_market.end_date = end_date
        existing_market.creation_date = creation_date
        existing_market.volume = market_data.get("volume")
        existing_market.liquidity = market_data.get("liquidity")
        existing_market.open_interest = market_data.get("openInterest")
        existing_market.image = market_data.get("image")
        existing_market.icon = market_data.get("icon")
        existing_market.category = market_data.get("category")
        existing_market.tags = tags
        existing_market.outcome_prices = outcome_prices
        existing_market.last_updated_at = datetime.utcnow()
    else:
        # Create new
        new_market = Market(
            id=market_id,
            slug=market_data.get("slug"),
            question=market_data.get("question") or "",
            description=market_data.get("description"),
            status=market_data.get("status"),
            end_date=end_date,
            creation_date=creation_date,
            volume=market_data.get("volume", 0),
            liquidity=market_data.get("liquidity", 0),
            open_interest=market_data.get("openInterest", 0),
            image=market_data.get("image"),
            icon=market_data.get("icon"),
            category=market_data.get("category"),
            tags=tags,
            outcome_prices=outcome_prices,
            last_updated_at=datetime.utcnow()
        )
        session.add(new_market)

def parse_date(date_str: Optional[str]) -> Optional[datetime]:
    if not date_str:
        return None
    try:
        # Handle ISO format variations
        # Replace Z with +00:00 for parsing, then convert to naive UTC
        dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        return dt.replace(tzinfo=None)
    except ValueError:
        return None
