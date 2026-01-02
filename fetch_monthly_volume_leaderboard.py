"""
Script to fetch monthly volume leaderboard data from Polymarket API and store in database.
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
from app.db.models import MonthlyVolumeLeaderboard, Base
from app.services.data_fetcher import async_client


# Configuration
API_BASE_URL = "https://data-api.polymarket.com/v1/leaderboard"
TIME_PERIOD = "month"
ORDER_BY = "VOL"
LIMIT = 50  # Records per page
MAX_RETRIES = 3
RETRY_DELAY = 2


async def check_table_exists(engine) -> bool:
    """Check if monthly_volume_leaderboard table exists."""
    async with engine.begin() as conn:
        result = await conn.execute(
            text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'monthly_volume_leaderboard'
                )
            """)
        )
        return result.scalar()


async def create_table(engine):
    """Create monthly_volume_leaderboard table if it doesn't exist."""
    table_exists = await check_table_exists(engine)
    
    if not table_exists:
        print("Creating monthly_volume_leaderboard table...")
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all, tables=[MonthlyVolumeLeaderboard.__table__])
        print("✅ Table created successfully!")
    else:
        print("✅ Table monthly_volume_leaderboard already exists")


async def fetch_leaderboard_page(
    time_period: str = TIME_PERIOD,
    order_by: str = ORDER_BY,
    limit: int = LIMIT,
    offset: int = 0,
    retry_count: int = 0
) -> Optional[List[Dict]]:
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
        
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            if "data" in data:
                return data["data"]
            elif "traders" in data:
                return data["traders"]
            else:
                return []
        else:
            return []
            
    except Exception as e:
        if retry_count < MAX_RETRIES:
            print(f"⚠️ Error fetching offset {offset} (attempt {retry_count + 1}/{MAX_RETRIES}): {e}")
            await asyncio.sleep(RETRY_DELAY * (retry_count + 1))
            return await fetch_leaderboard_page(
                time_period, order_by, limit, offset, retry_count + 1
            )
        else:
            print(f"❌ Failed to fetch offset {offset}: {e}")
            return None


async def process_trader_record(session: AsyncSession, trader_data: Dict, fetched_at: datetime) -> bool:
    try:
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
        
        rank = trader_data.get("rank")
        name = trader_data.get("userName") or trader_data.get("name")
        pseudonym = trader_data.get("xUsername") or trader_data.get("pseudonym")
        profile_image = trader_data.get("profileImage") or trader_data.get("profile_image")
        pnl = trader_data.get("pnl")
        volume = trader_data.get("vol") or trader_data.get("volume")
        verified_badge = trader_data.get("verifiedBadge") or trader_data.get("verified_badge", False)
        
        if pnl is not None: pnl = float(pnl)
        if volume is not None: volume = float(volume)
        if rank is not None: rank = int(rank)
        
        raw_data = json.dumps(trader_data)
        
        await session.execute(
            text("""
                INSERT INTO monthly_volume_leaderboard 
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
        print(f"⚠️ Error processing record: {e}")
        return False


async def fetch_all_leaderboard_data(session: AsyncSession):
    offset = 0
    total_fetched = 0
    total_inserted = 0
    fetched_at = datetime.utcnow()
    
    print(f"\n🚀 Fetching monthly volume leaderboard data...")
    
    while True:
        traders = await fetch_leaderboard_page(offset=offset)
        
        if traders is None or len(traders) == 0:
            break
        
        print(f"📥 Fetched offset {offset} ({len(traders)} traders)")
        total_fetched += len(traders)
        
        batch_inserted = 0
        for trader in traders:
            if await process_trader_record(session, trader, fetched_at):
                batch_inserted += 1
                total_inserted += 1
        
        try:
            await session.commit()
        except Exception as e:
            print(f"❌ Commit error: {e}")
            await session.rollback()
        
        if len(traders) < LIMIT:
            break
        
        offset += LIMIT
        await asyncio.sleep(0.5)
    
    print(f"\n✅ Stats: Fetched {total_fetched}, Inserted {total_inserted}")
    return total_inserted


async def main():
    engine = create_async_engine(settings.DATABASE_URL)
    try:
        await create_table(engine)
        AsyncSessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
        async with AsyncSessionLocal() as session:
            await fetch_all_leaderboard_data(session)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
