"""
Script to fetch daily volume leaderboard data from Polymarket API and store in database.

This script:
1. Fetches data from https://data-api.polymarket.com/v1/leaderboard?timePeriod=day&orderBy=VOL&offset=0
2. Stores it in daily_volume_leaderboard table
3. Handles pagination to fetch all records
"""

import asyncio
import json
import sys
from typing import Dict, List, Optional
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from app.core.config import settings
from app.db.models import DailyVolumeLeaderboard, Base
from app.services.data_fetcher import async_client


# Configuration
API_BASE_URL = "https://data-api.polymarket.com/v1/leaderboard"
TIME_PERIOD = "day"
ORDER_BY = "VOL"
LIMIT = 50  # Records per page (API maximum is 50)
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds


async def check_table_exists(engine) -> bool:
    """Check if daily_volume_leaderboard table exists."""
    async with engine.begin() as conn:
        result = await conn.execute(
            text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'daily_volume_leaderboard'
                )
            """)
        )
        return result.scalar()


async def create_table(engine):
    """Create daily_volume_leaderboard table if it doesn't exist."""
    table_exists = await check_table_exists(engine)
    
    if not table_exists:
        print("Creating daily_volume_leaderboard table...")
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all, tables=[DailyVolumeLeaderboard.__table__])
        print("✅ Table created successfully!")
    else:
        print("✅ Table daily_volume_leaderboard already exists")


async def fetch_leaderboard_page(
    time_period: str = TIME_PERIOD,
    order_by: str = ORDER_BY,
    limit: int = LIMIT,
    offset: int = 0,
    retry_count: int = 0
) -> Optional[List[Dict]]:
    """
    Fetch a single page of leaderboard data from the API.
    
    Args:
        time_period: Time period filter (day)
        order_by: Order by metric (VOL)
        limit: Number of records per page
        offset: Offset for pagination
        retry_count: Current retry attempt
    
    Returns:
        List of trader records or None if failed after retries
    """
    try:
        params = {
            "timePeriod": time_period,
            "orderBy": order_by,
            "limit": limit,
            "offset": offset
        }
        
        response = await async_client.get(API_BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()
        
        # Handle list response
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            # Try to extract data from dict
            if "data" in data:
                return data["data"]
            elif "traders" in data:
                return data["traders"]
            else:
                print(f"⚠️  Warning: Unexpected response format at offset {offset}")
                return []
        else:
            print(f"⚠️  Warning: API returned unexpected data type at offset {offset}: {type(data)}")
            return []
            
    except Exception as e:
        if retry_count < MAX_RETRIES:
            print(f"⚠️  Error fetching offset {offset} (attempt {retry_count + 1}/{MAX_RETRIES}): {e}")
            await asyncio.sleep(RETRY_DELAY * (retry_count + 1))
            return await fetch_leaderboard_page(
                time_period, order_by, limit, offset, retry_count + 1
            )
        else:
            print(f"❌ Failed to fetch offset {offset} after {MAX_RETRIES} retries: {e}")
            return None


async def process_trader_record(session: AsyncSession, trader_data: Dict, fetched_at: datetime) -> bool:
    """
    Process a single trader record: insert into database.
    
    Args:
        session: Database session
        trader_data: Raw trader data from API
        fetched_at: Timestamp when data was fetched
    
    Returns:
        True if successful, False otherwise
    """
    try:
        # Extract wallet address
        wallet_address = (
            trader_data.get("proxyWallet") or 
            trader_data.get("proxy_wallet") or
            trader_data.get("wallet_address") or 
            trader_data.get("wallet") or
            trader_data.get("address")
        )
        
        if not wallet_address or not wallet_address.startswith("0x") or len(wallet_address) != 42:
            return False
        
        wallet_address = wallet_address.lower()
        
        # Extract fields
        rank = trader_data.get("rank")
        name = trader_data.get("userName") or trader_data.get("name")
        pseudonym = trader_data.get("xUsername") or trader_data.get("pseudonym")
        profile_image = trader_data.get("profileImage") or trader_data.get("profile_image")
        pnl = trader_data.get("pnl")
        volume = trader_data.get("vol") or trader_data.get("volume")
        verified_badge = trader_data.get("verifiedBadge") or trader_data.get("verified_badge", False)
        
        # Convert to appropriate types
        if pnl is not None:
            pnl = float(pnl)
        if volume is not None:
            volume = float(volume)
        if rank is not None:
            rank = int(rank)
        
        # Store full API response as JSON
        raw_data = json.dumps(trader_data)
        
        # Insert record
        await session.execute(
            text("""
                INSERT INTO daily_volume_leaderboard 
                (wallet_address, rank, name, pseudonym, profile_image, pnl, volume, 
                 verified_badge, raw_data, fetched_at, created_at, updated_at)
                VALUES 
                (:wallet, :rank, :name, :pseudonym, :profile_image, :pnl, :volume,
                 :verified_badge, :raw_data, :fetched_at, :created_at, :updated_at)
            """),
            {
                "wallet": wallet_address,
                "rank": rank,
                "name": name,
                "pseudonym": pseudonym,
                "profile_image": profile_image,
                "pnl": pnl,
                "volume": volume,
                "verified_badge": verified_badge if verified_badge is not None else False,
                "raw_data": raw_data,
                "fetched_at": fetched_at,
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow()
            }
        )
        return True
            
    except Exception as e:
        print(f"⚠️  Error processing trader record: {e}")
        return False


async def fetch_all_leaderboard_data(session: AsyncSession):
    """
    Fetch all leaderboard data from the API with pagination.
    
    Args:
        session: Database session
    """
    offset = 0
    total_fetched = 0
    total_inserted = 0
    failed_offsets = []
    fetched_at = datetime.utcnow()  # Use same timestamp for all records in this run
    
    print(f"\n🚀 Starting to fetch daily volume leaderboard data...")
    print(f"📋 Configuration: timePeriod={TIME_PERIOD}, orderBy={ORDER_BY}, limit={LIMIT}\n")
    
    while True:
        print(f"📥 Fetching offset {offset}...", end=" ")
        
        # Fetch page
        traders = await fetch_leaderboard_page(
            time_period=TIME_PERIOD,
            order_by=ORDER_BY,
            limit=LIMIT,
            offset=offset
        )
        
        if traders is None:
            # Failed after retries
            failed_offsets.append(offset)
            print(f"❌ Failed")
            offset += LIMIT
            continue
        
        if len(traders) == 0:
            print(f"✅ No more data (empty response)")
            break
        
        print(f"✅ Fetched {len(traders)} traders")
        
        total_fetched += len(traders)
        
        # Process each trader
        batch_inserted = 0
        for trader in traders:
            if await process_trader_record(session, trader, fetched_at):
                batch_inserted += 1
                total_inserted += 1
        
        # Commit batch
        try:
            await session.commit()
            print(f"   💾 Committed: {batch_inserted} inserted")
        except Exception as e:
            print(f"   ❌ Error committing batch: {e}")
            await session.rollback()
        
        # Stop if we got fewer records than requested (definitely no more data)
        if len(traders) < LIMIT:
            print(f"✅ Reached end of data (returned {len(traders)} < {LIMIT})")
            break
        
        # Move to next page
        offset += LIMIT
        
        # Small delay to avoid rate limiting
        await asyncio.sleep(0.5)
    
    # Print final statistics
    print(f"\n{'='*60}")
    print(f"📊 FINAL STATISTICS")
    print(f"{'='*60}")
    print(f"Total traders fetched:     {total_fetched}")
    print(f"Total records inserted:     {total_inserted}")
    print(f"Final offset reached:       {offset}")
    print(f"Failed offsets:             {len(failed_offsets)}")
    if failed_offsets:
        print(f"Failed offset list:         {failed_offsets[:10]}{'...' if len(failed_offsets) > 10 else ''}")
    print(f"{'='*60}\n")
    
    return {
        "total_fetched": total_fetched,
        "total_inserted": total_inserted,
        "failed_offsets": failed_offsets
    }


async def main():
    """Main function to run the ingestion script."""
    print("="*60)
    print("Polymarket Daily Volume Leaderboard Data Ingestion Script")
    print("="*60)
    
    # Create database engine
    engine = create_async_engine(
        settings.DATABASE_URL,
        echo=False
    )
    
    try:
        # Check and create table
        await create_table(engine)
        
        # Create session
        AsyncSessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=engine,
            class_=AsyncSession
        )
        
        async with AsyncSessionLocal() as session:
            # Check current database state
            result = await session.execute(
                text("SELECT COUNT(*) FROM daily_volume_leaderboard")
            )
            existing_count = result.scalar()
            
            print(f"📊 Current database state:")
            print(f"   Total records: {existing_count}\n")
            
            # Fetch all data
            stats = await fetch_all_leaderboard_data(session)
            
            # Verify final count
            result = await session.execute(
                text("SELECT COUNT(*) FROM daily_volume_leaderboard")
            )
            total_in_db = result.scalar()
            
            print(f"\n✅ Final database state:")
            print(f"   Total records: {total_in_db}")
            print(f"   New records added: {total_in_db - existing_count}")
        
        print("\n✅ Script completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
