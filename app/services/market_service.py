import asyncio
import json
from datetime import datetime
from typing import List, Dict, Any, Optional
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.future import select
from sqlalchemy.exc import IntegrityError
from app.db.session import AsyncSessionLocal
from app.db.models import Market
from app.services.data_fetcher import fetch_markets

async def update_all_markets():
    """
    Fetches all markets (active, closed, resolved) from Polymarket and updates the database.
    Optimized for speed using bulk upserts and concurrent fetching.
    """
    start_time = datetime.utcnow()
    print(f"[{start_time}] Starting optimized market update job...")
    
    statuses = ["active", "closed", "resolved"]
    
    # Process each status concurrently
    tasks = [process_market_status(status) for status in statuses]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    total_updated = 0
    for result in results:
        if isinstance(result, int):
            total_updated += result
        else:
            print(f"Error in market update task: {result}")

    end_time = datetime.utcnow()
    duration = (end_time - start_time).total_seconds()
    print(f"[{end_time}] Market update job completed. Updated {total_updated} markets in {duration:.2f} seconds.")

async def process_market_status(status: str) -> int:
    """
    Fetch and process markets for a specific status.
    Returns the number of markets processed.
    """
    print(f"Starting fetch for {status} markets...")
    total_processed = 0
    offset = 0
    limit = 100 
    batch_size = 100  # Reduced batch size to 100 to avoid DBAPI errors
    market_batch = []
    
    async with AsyncSessionLocal() as session:
        while True:
            try:
                markets, pagination = await fetch_markets(status=status, limit=limit, offset=offset)
            except Exception as e:
                print(f"Error fetching markets at offset {offset}: {e}")
                # Try to skip this batch and continue? Or retry? 
                # For now, let's break to avoid infinite loops if API is down
                break
            
            if not markets:
                break
            
            market_batch.extend(markets)
            
            # Process batch if full
            if len(market_batch) >= batch_size:
                try:
                    await bulk_upsert_markets(session, market_batch)
                    await session.commit()  # Commit incrementally
                    total_processed += len(market_batch)
                except Exception as e:
                    print(f"Error upserting batch of {len(market_batch)} markets: {e}")
                    await session.rollback()
                finally:
                    market_batch = []
            
            if not pagination.get("has_more"):
                break
            
            offset += limit
            
            # Small delay to respect rate limits
            await asyncio.sleep(0.05)
        
        # Process remaining markets in batch
        if market_batch:
            try:
                await bulk_upsert_markets(session, market_batch)
                await session.commit()  # Commit remainder
                total_processed += len(market_batch)
            except Exception as e:
                print(f"Error upserting final batch of {len(market_batch)} markets: {e}")
                await session.rollback()
    
    print(f"Completed {status} markets. Processed: {total_processed}")
    return total_processed

async def bulk_upsert_markets(session, markets_data: List[Dict[str, Any]]):
    """
    Bulk insert or update markets using PostgreSQL ON CONFLICT DO UPDATE.
    """
    if not markets_data:
        return

    values_list = []
    for market_data in markets_data:
        market_id = market_data.get("id")
        if not market_id:
            continue
            
        # Parse dates
        end_date = parse_date(market_data.get("endDate") or market_data.get("end_date"))
        creation_date = parse_date(market_data.get("creationDate") or market_data.get("createdAt"))
        
        # Serialize complex types
        outcome_prices = json.dumps(market_data.get("outcomePrices", {}))
        tags = json.dumps(market_data.get("tags", []))
        
        values_list.append({
            "id": market_id,
            "slug": market_data.get("slug"),
            "question": market_data.get("question") or "",
            "description": market_data.get("description"),
            "status": market_data.get("status"),
            "end_date": end_date,
            "creation_date": creation_date,
            "volume": float(market_data.get("volume", 0)),
            "liquidity": float(market_data.get("liquidity", 0)),
            "open_interest": float(market_data.get("openInterest", 0)),
            "image": market_data.get("image"),
            "icon": market_data.get("icon"),
            "category": market_data.get("category"),
            "tags": tags,
            "outcome_prices": outcome_prices,
            "last_updated_at": datetime.utcnow(),
            "created_at": datetime.utcnow() # This is ignored on update due to business logic, but needed for insert
        })

    if not values_list:
        return

    stmt = insert(Market).values(values_list)
    
    # Define the columns to update on conflict
    update_dict = {
        "slug": stmt.excluded.slug,
        "question": stmt.excluded.question,
        "description": stmt.excluded.description,
        "status": stmt.excluded.status,
        "end_date": stmt.excluded.end_date,
        "creation_date": stmt.excluded.creation_date,
        "volume": stmt.excluded.volume,
        "liquidity": stmt.excluded.liquidity,
        "open_interest": stmt.excluded.open_interest,
        "image": stmt.excluded.image,
        "icon": stmt.excluded.icon,
        "category": stmt.excluded.category,
        "tags": stmt.excluded.tags,
        "outcome_prices": stmt.excluded.outcome_prices,
        "last_updated_at": datetime.utcnow()
    }
    
    # Perform upsert
    upsert_stmt = stmt.on_conflict_do_update(
        index_elements=['id'],
        set_=update_dict
    )
    
    await session.execute(upsert_stmt)

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
