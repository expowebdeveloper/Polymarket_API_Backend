
import asyncio
import sys
import os
import json
from sqlalchemy import text, select, func
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

# Add current directory to path to import app
sys.path.append(os.getcwd())

from app.core.config import settings
from app.db.models import TraderLeaderboard, TraderClosedPosition, TraderFetchCheckpoint
from app.services.data_fetcher import async_client

# Re-implement simplified fetch to compare
API_BASE_URL = f"{settings.POLYMARKET_DATA_API_URL}/v1/closed-positions"

async def fetch_api_stats(wallet_address: str):
    print(f"\n--- API Check for {wallet_address} ---")
    url = API_BASE_URL
    params = {
        "user": wallet_address,
        "sortBy": "timestamp",
        "sortDirection": "DESC",
        "limit": 10
    }
    
    try:
        response = await async_client.get(url, params=params)
        if response.status_code != 200:
            print(f"API Error: {response.status_code} - {response.text}")
            return
            
        data = response.json()
        print(f"API returned {len(data)} items in first page (limit=10).")
        if data:
            print(f"Latest position timestamp: {data[0].get('timestamp')}")
            print(f"Latest position slug: {data[0].get('slug')}")
    except Exception as e:
        print(f"API Exception: {e}")

async def debug_user(user_identifier: str):
    print(f"Debugging user identifier: {user_identifier}")
    
    engine = create_async_engine(settings.DATABASE_URL, echo=False)
    AsyncSessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    
    async with AsyncSessionLocal() as session:
        # Find trader
        query = select(TraderLeaderboard).where(
            (TraderLeaderboard.wallet_address == user_identifier) | 
            (TraderLeaderboard.name == user_identifier) | 
            (TraderLeaderboard.pseudonym == user_identifier)
        )
        result = await session.execute(query)
        trader = result.scalars().first()
        
        if not trader:
            print(f"❌ User not found in TraderLeaderboard with identifier '{user_identifier}'")
            # Try partial match
            print("Searching for partial matches...")
            query = select(TraderLeaderboard).where(
                (TraderLeaderboard.name.ilike(f"%{user_identifier}%")) | 
                (TraderLeaderboard.pseudonym.ilike(f"%{user_identifier}%"))
            )
            result = await session.execute(query)
            matches = result.scalars().all()
            if matches:
                print(f"Found {len(matches)} partial matches:")
                for m in matches:
                    print(f" - ID: {m.id}, Name: {m.name}, Pseudonym: {m.pseudonym}, Wallet: {m.wallet_address}")
            return

        print(f"✅ Found User: ID={trader.id}, Name={trader.name}, Pseudonym={trader.pseudonym}")
        print(f"   Wallet: {trader.wallet_address}")
        
        # Check Checkpoints
        ckpt_query = select(TraderFetchCheckpoint).where(TraderFetchCheckpoint.trader_id == trader.id)
        result = await session.execute(ckpt_query)
        checkpoint = result.scalars().first()
        
        if checkpoint:
            print(f"\n--- Checkpoint Data ---")
            print(f"Last Fetch At: {checkpoint.last_fetch_at}")
            print(f"Last Closed Position Timestamp: {checkpoint.last_closed_position_timestamp}")
            print(f"Total Fetched: {checkpoint.total_closed_positions_fetched}")
            print(f"Status: {checkpoint.fetch_status}")
            print(f"Error Message: {checkpoint.error_message}")
        else:
            print(f"\n❌ No Checkpoint found for this trader.")

        # Check DB Positions
        pos_count_query = select(func.count(TraderClosedPosition.id)).where(TraderClosedPosition.trader_id == trader.id)
        result = await session.execute(pos_count_query)
        db_count = result.scalar()
        print(f"\n--- DB Positions ---")
        print(f"Total positions in DB: {db_count}")
        
        # Check latest position in DB
        latest_pos_query = select(TraderClosedPosition).where(TraderClosedPosition.trader_id == trader.id).order_by(TraderClosedPosition.timestamp.desc()).limit(1)
        result = await session.execute(latest_pos_query)
        latest_pos = result.scalars().first()
        if latest_pos:
            print(f"Latest DB Position Timestamp: {latest_pos.timestamp}")
            print(f"Latest DB Position Slug: {latest_pos.slug}")

        # Compare with API
        await fetch_api_stats(trader.wallet_address)

    await engine.dispose()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python debug_kch123.py <username_or_wallet>")
        sys.exit(1)
    
    asyncio.run(debug_user(sys.argv[1]))
