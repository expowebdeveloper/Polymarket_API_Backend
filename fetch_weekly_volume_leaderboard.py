"""
Script to fetch weekly volume leaderboard data from Polymarket API and store in database.

This script:
1. Fetches data from https://data-api.polymarket.com/v1/leaderboard?timePeriod=week&orderBy=VOL&offset=0
2. Stores it in weekly_volume_leaderboard table
3. Clears table before each run to ensure data freshness
4. Handles pagination to fetch all records
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
from app.db.models import WeeklyVolumeLeaderboard, Base
from app.services.data_fetcher import async_client
from app.services.polymarket_service import PolymarketService
from app.services.live_leaderboard_service import transform_stats_for_scoring
from app.services.leaderboard_service import calculate_scores_and_rank


# Configuration
API_BASE_URL = "https://data-api.polymarket.com/v1/leaderboard"
TIME_PERIOD = "week"
ORDER_BY = "VOL"
LIMIT = 50  # Records per page (API maximum is 50)
MAX_RETRIES = 3
RETRY_DELAY = 10
SCORING_LIMIT = 200  # Calculate scores for top 200 traders
CONCURRENCY_LIMIT = 5
# seconds


async def check_table_exists(engine) -> bool:
    """Check if weekly_volume_leaderboard table exists."""
    async with engine.begin() as conn:
        result = await conn.execute(
            text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'weekly_volume_leaderboard'
                )
            """)
        )
        return result.scalar()


async def create_table(engine):
    """Create weekly_volume_leaderboard table if it doesn't exist."""
    table_exists = await check_table_exists(engine)
    
    if not table_exists:
        print("Creating weekly_volume_leaderboard table...")
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all, tables=[WeeklyVolumeLeaderboard.__table__])
        print("✅ Table created successfully!")
    else:
        print("✅ Table weekly_volume_leaderboard already exists")


async def clear_table(session: AsyncSession):
    """Clear all records from weekly_volume_leaderboard table."""
    print("🧹 Clearing existing data from weekly_volume_leaderboard...")
    await session.execute(text("TRUNCATE TABLE weekly_volume_leaderboard"))
    await session.commit()
    print("✅ Table cleared")


async def fetch_leaderboard_page(
    time_period: str = TIME_PERIOD,
    order_by: str = ORDER_BY,
    limit: int = LIMIT,
    offset: int = 0,
    retry_count: int = 0
) -> Optional[List[Dict]]:
    """
    Fetch a single page of leaderboard data from the API.
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
        
        # Prepare data dict
        data = {
            "wallet": wallet_address,
            "rank": rank,
            "name": name,
            "pseudonym": pseudonym,
            "profile_image": profile_image,
            "pnl": pnl,
            "volume": volume,
            "roi": trader_data.get("roi", 0.0),
            "win_rate": trader_data.get("win_rate", 0.0),
            "total_trades": trader_data.get("total_trades", 0),
            "total_trades_with_pnl": trader_data.get("total_trades_with_pnl", 0),
            "winning_trades": trader_data.get("winning_trades", 0),
            "total_stakes": trader_data.get("total_stakes", 0.0),
            "score_win_rate": trader_data.get("score_win_rate", 0.0),
            "score_roi": trader_data.get("score_roi", 0.0),
            "score_pnl": trader_data.get("score_pnl", 0.0),
            "score_risk": trader_data.get("score_risk", 0.0),
            "final_score": trader_data.get("final_score", 0.0),
            "w_shrunk": trader_data.get("W_shrunk"),
            "roi_shrunk": trader_data.get("roi_shrunk"),
            "pnl_shrunk": trader_data.get("pnl_shrunk"),
            "verified_badge": verified_badge if verified_badge is not None else False,
            "raw_data": raw_data,
            "fetched_at": fetched_at,
            "updated_at": datetime.utcnow()
        }

        # Check if record exists
        result = await session.execute(
            text("SELECT id FROM weekly_volume_leaderboard WHERE wallet_address = :wallet"),
            {"wallet": wallet_address}
        )
        existing = result.fetchone()

        if existing:
            # Update existing record
            await session.execute(
                text("""
                    UPDATE weekly_volume_leaderboard 
                    SET rank = :rank,
                        name = :name,
                        pseudonym = :pseudonym,
                        profile_image = :profile_image,
                        pnl = :pnl,
                        volume = :volume,
                        roi = :roi,
                        win_rate = :win_rate,
                        total_trades = :total_trades,
                        total_trades_with_pnl = :total_trades_with_pnl,
                        winning_trades = :winning_trades,
                        total_stakes = :total_stakes,
                        score_win_rate = :score_win_rate,
                        score_roi = :score_roi,
                        score_pnl = :score_pnl,
                        score_risk = :score_risk,
                        final_score = :final_score,
                        w_shrunk = :w_shrunk,
                        roi_shrunk = :roi_shrunk,
                        pnl_shrunk = :pnl_shrunk,
                        verified_badge = :verified_badge,
                        raw_data = :raw_data,
                        fetched_at = :fetched_at,
                        updated_at = :updated_at
                    WHERE wallet_address = :wallet
                """),
                data
            )
            return False
        else:
            # Insert new record
            data["created_at"] = datetime.utcnow()
            await session.execute(
                text("""
                    INSERT INTO weekly_volume_leaderboard 
                    (wallet_address, rank, name, pseudonym, profile_image, pnl, volume, 
                     roi, win_rate, total_trades, total_trades_with_pnl, winning_trades, total_stakes,
                     score_win_rate, score_roi, score_pnl, score_risk, final_score,
                     w_shrunk, roi_shrunk, pnl_shrunk,
                     verified_badge, raw_data, fetched_at, created_at, updated_at)
                    VALUES 
                    (:wallet, :rank, :name, :pseudonym, :profile_image, :pnl, :volume,
                     :roi, :win_rate, :total_trades, :total_trades_with_pnl, :winning_trades, :total_stakes,
                     :score_win_rate, :score_roi, :score_pnl, :score_risk, :final_score,
                     :w_shrunk, :roi_shrunk, :pnl_shrunk,
                     :verified_badge, :raw_data, :fetched_at, :created_at, :updated_at)
                """),
                data
            )
            return True
            
    except Exception as e:
        print(f"⚠️  Error processing trader record: {e}")
        return False


async def fetch_all_leaderboard_data(session: AsyncSession):
    """
    Fetch all leaderboard data from the API with pagination.
    """
    offset = 0
    total_fetched = 0
    total_inserted = 0
    failed_offsets = []
    fetched_at = datetime.utcnow()  # Use same timestamp for all records in this run
    
    print(f"\n🚀 Starting to fetch weekly volume leaderboard data...")
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
    print(f"{'='*60}\n")
    
    print(f"\n✅ Stats: Fetched {total_fetched}, Inserted {total_inserted}")
    
    # --- Scoring Phase ---
    # Now that we have all traders, let's calculate scores for the top ones
    if total_inserted > 0:
        print(f"\n🏆 Calculating scores for top {min(total_inserted, SCORING_LIMIT)} traders...")
        
        # Get top traders from DB
        result = await session.execute(
            text("SELECT wallet_address, rank, name, pseudonym, profile_image, pnl, volume, raw_data FROM weekly_volume_leaderboard ORDER BY volume DESC LIMIT :limit"),
            {"limit": SCORING_LIMIT}
        )
        rows = result.fetchall()
        
        semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
        
        async def fetch_trader_stats(row):
            async with semaphore:
                try:
                    wallet = row.wallet_address
                    # Fetch stats for a week
                    stats = await PolymarketService.calculate_portfolio_stats(wallet, time_period="week")
                    if stats is None:
                        return None
                    
                    transformed = transform_stats_for_scoring(stats)
                    if transformed:
                        # Preserve info from row
                        transformed["name"] = row.name
                        transformed["pseudonym"] = row.pseudonym
                        transformed["profile_image"] = row.profile_image
                        transformed["rank"] = row.rank
                        transformed["pnl"] = float(row.pnl) if row.pnl is not None else 0.0
                        transformed["vol"] = float(row.volume) if row.volume is not None else 0.0
                        transformed["raw_data_orig"] = row.raw_data
                    return transformed
                except Exception as e:
                    print(f"⚠️ Error fetching stats for {row.wallet_address}: {e}")
                    return None

        tasks = [fetch_trader_stats(row) for row in rows]
        scored_results_raw = await asyncio.gather(*tasks)
        
        # Filter valid results
        valid_results = [r for r in scored_results_raw if r is not None]
        
        if valid_results:
            # Calculate scores using the population
            ranked_results = calculate_scores_and_rank(valid_results)
            
            print(f"💾 Updating {len(ranked_results)} traders with calculated scores...")
            
            for trader in ranked_results:
                await session.execute(
                    text("""
                        UPDATE weekly_volume_leaderboard 
                        SET roi = :roi, 
                            win_rate = :win_rate, 
                            total_trades = :total_trades,
                            total_trades_with_pnl = :total_trades_with_pnl,
                            winning_trades = :winning_trades,
                            total_stakes = :total_stakes,
                            score_win_rate = :score_win_rate,
                            score_roi = :score_roi,
                            score_pnl = :score_pnl,
                            score_risk = :score_risk,
                            final_score = :final_score,
                            w_shrunk = :w_shrunk,
                            roi_shrunk = :roi_shrunk,
                            pnl_shrunk = :pnl_shrunk
                        WHERE wallet_address = :wallet
                    """),
                    {
                        "wallet": trader["wallet_address"],
                        "roi": trader.get("roi", 0.0),
                        "win_rate": trader.get("win_rate", 0.0),
                        "total_trades": trader.get("total_trades", 0),
                        "total_trades_with_pnl": trader.get("total_trades_with_pnl", 0),
                        "winning_trades": trader.get("winning_trades", 0),
                        "total_stakes": trader.get("total_stakes", 0.0),
                        "score_win_rate": trader.get("score_win_rate", 0.0),
                        "score_roi": trader.get("score_roi", 0.0),
                        "score_pnl": trader.get("score_pnl", 0.0),
                        "score_risk": trader.get("score_risk", 0.0),
                        "final_score": trader.get("final_score", 0.0),
                        "w_shrunk": trader.get("W_shrunk"),
                        "roi_shrunk": trader.get("roi_shrunk"),
                        "pnl_shrunk": trader.get("pnl_shrunk")
                    }
                )
            
            await session.commit()
            print("✅ Weekly Volume Leaderboard updated with advanced scores!")
    
    return total_inserted


async def main():
    """Main function to run the ingestion script."""
    print("="*60)
    print("Polymarket Weekly Volume Leaderboard Data Ingestion Script")
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
            # Clear existing data
            await clear_table(session)
            
            # Fetch all data
            stats = await fetch_all_leaderboard_data(session)
            
            # Verify final count
            result = await session.execute(
                text("SELECT COUNT(*) FROM weekly_volume_leaderboard")
            )
            total_in_db = result.scalar()
            
            print(f"\n✅ Final database state:")
            print(f"   Total records: {total_in_db}")
        
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
