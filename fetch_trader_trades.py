"""
Script to fetch trades for all traders with checkpoint tracking.

This script:
1. Tracks the last fetched timestamp for each trader
2. Only fetches new trades since the last checkpoint
3. For new traders, fetches all historical trades
4. Implements rate limiting to avoid API blocks
5. Supports resuming from where it left off if interrupted
"""

import asyncio
import argparse
import sys
import json
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from app.core.config import settings
from app.db.models import TraderLeaderboard, TraderTrade, Base
from app.services.data_fetcher import async_client
import time

# Configuration
API_BASE_URL = f"{settings.POLYMARKET_DATA_API_URL}/trades"
FETCH_LIMIT = 100  # API max is 100
MAX_RETRIES = 3
BASE_DELAY = 0.7  # Base delay between API calls (seconds)
MAX_DELAY = 30  # Maximum delay for exponential backoff
BATCH_COMMIT_SIZE = 10  # Commit every N traders


async def fetch_trades_since_timestamp(
    wallet_address: str,
    since_timestamp: Optional[int] = None,
    limit: Optional[int] = None
) -> List[Dict]:
    """
    Fetch trades for a wallet, optionally since a specific timestamp.
    
    Args:
        wallet_address: Ethereum wallet address
        since_timestamp: Only fetch trades newer than this timestamp (None = fetch all)
        limit: Maximum number of trades to fetch (None = fetch all)
    
    Returns:
        List of trade dictionaries
    """
    try:
        url = API_BASE_URL
        params = {
            "user": wallet_address,
            "limit": FETCH_LIMIT
        }
        
        all_trades = []
        offset = 0
        
        while True:
            params["offset"] = offset
            
            response = await async_client.get(url, params=params)
            response.raise_for_status()
            trades = response.json()
            
            if not trades or not isinstance(trades, list):
                break
            
            # Filter by timestamp if specified
            if since_timestamp is not None:
                new_trades = []
                for trade in trades:
                    trade_timestamp = trade.get("timestamp")
                    if trade_timestamp and trade_timestamp > since_timestamp:
                        new_trades.append(trade)
                    # Note: trades are usually sorted DESC by timestamp
                    # but we continue fetching to be safe
                
                all_trades.extend(new_trades)
            else:
                all_trades.extend(trades)
            
            # Check if we've fetched enough or reached the end
            if len(trades) < FETCH_LIMIT:
                break
            
            if limit and len(all_trades) >= limit:
                return all_trades[:limit]
            
            offset += FETCH_LIMIT
            
            # Small delay to avoid rate limiting
            await asyncio.sleep(0.3)
        
        return all_trades
        
    except Exception as e:
        print(f"  ✗ Error fetching trades for {wallet_address}: {e}")
        raise


async def save_trades(
    session: AsyncSession,
    trader_id: int,
    trades: List[Dict]
) -> int:
    """
    Save trades to the database.
    
    Returns:
        Number of new trades inserted
    """
    inserted_count = 0
    
    for trade in trades:
        # Use a savepoint for each trade to handle errors gracefully
        savepoint = await session.begin_nested()
        
        try:
            # Extract fields
            asset = trade.get("asset")
            transaction_hash = trade.get("transactionHash") or trade.get("transaction_hash")
            timestamp = trade.get("timestamp")
            
            if not asset or not transaction_hash or not timestamp:
                await savepoint.rollback()
                continue
            
            # Check if already exists
            result = await session.execute(
                text("""
                    SELECT id FROM trader_trades 
                    WHERE trader_id = :trader_id 
                    AND transaction_hash = :tx_hash 
                    AND timestamp = :timestamp
                    AND asset = :asset
                """),
                {
                    "trader_id": trader_id,
                    "tx_hash": transaction_hash,
                    "timestamp": timestamp,
                    "asset": asset
                }
            )
            
            if result.fetchone():
                await savepoint.rollback()
                continue  # Already exists
            
            # Insert new trade
            await session.execute(
                text("""
                    INSERT INTO trader_trades (
                        trader_id, side, asset, condition_id, size, price, timestamp,
                        title, slug, icon, event_slug, outcome, outcome_index,
                        name, pseudonym, bio, profile_image, profile_image_optimized,
                        transaction_hash, raw_data, created_at, updated_at
                    ) VALUES (
                        :trader_id, :side, :asset, :condition_id, :size, :price, :timestamp,
                        :title, :slug, :icon, :event_slug, :outcome, :outcome_index,
                        :name, :pseudonym, :bio, :profile_image, :profile_image_optimized,
                        :transaction_hash, :raw_data, :created_at, :updated_at
                    )
                """),
                {
                    "trader_id": trader_id,
                    "side": trade.get("side"),
                    "asset": asset,
                    "condition_id": trade.get("conditionId") or trade.get("condition_id"),
                    "size": trade.get("size"),
                    "price": trade.get("price"),
                    "timestamp": timestamp,
                    "title": trade.get("title"),
                    "slug": trade.get("slug"),
                    "icon": trade.get("icon"),
                    "event_slug": trade.get("eventSlug") or trade.get("event_slug"),
                    "outcome": trade.get("outcome"),
                    "outcome_index": trade.get("outcomeIndex") or trade.get("outcome_index"),
                    "name": trade.get("name"),
                    "pseudonym": trade.get("pseudonym"),
                    "bio": trade.get("bio"),
                    "profile_image": trade.get("profileImage") or trade.get("profile_image"),
                    "profile_image_optimized": trade.get("profileImageOptimized") or trade.get("profile_image_optimized"),
                    "transaction_hash": transaction_hash,
                    "raw_data": json.dumps(trade),
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow()
                }
            )
            
            await savepoint.commit()
            inserted_count += 1
            
        except Exception as e:
            await savepoint.rollback()
            # Only print first few errors to avoid spam
            if inserted_count < 3:
                print(f"  ⚠️ Error saving trade: {e}")
            continue
    
    return inserted_count


async def get_checkpoint(session: AsyncSession, trader_id: int) -> Optional[Dict]:
    """Get checkpoint for a trader from trader_fetch_checkpoints table."""
    try:
        result = await session.execute(
            text("""
                SELECT last_closed_position_timestamp, fetch_status, last_fetch_at
                FROM trader_fetch_checkpoints 
                WHERE trader_id = :trader_id
            """),
            {"trader_id": trader_id}
        )
        row = result.fetchone()
        if row:
            return {
                "last_trade_timestamp": row[0],  # Reuse this field for trades
                "fetch_status": row[1],
                "last_fetch_at": row[2]
            }
        return None
    except:
        return None


async def update_checkpoint(
    session: AsyncSession,
    trader_id: int,
    wallet_address: str,
    latest_timestamp: Optional[int],
    total_new_trades: int
):
    """Update or create checkpoint for a trader (reusing trader_fetch_checkpoints table)."""
    try:
        # Check if checkpoint exists
        result = await session.execute(
            text("SELECT id FROM trader_fetch_checkpoints WHERE trader_id = :trader_id"),
            {"trader_id": trader_id}
        )
        existing = result.fetchone()
        
        if existing:
            # Update existing checkpoint (use last_closed_position_timestamp for trades too)
            await session.execute(
                text("""
                    UPDATE trader_fetch_checkpoints 
                    SET last_closed_position_timestamp = :timestamp,
                        last_fetch_at = :fetch_at,
                        updated_at = :updated_at
                    WHERE trader_id = :trader_id
                """),
                {
                    "trader_id": trader_id,
                    "timestamp": latest_timestamp,
                    "fetch_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow()
                }
            )
        else:
            # Create new checkpoint
            await session.execute(
                text("""
                    INSERT INTO trader_fetch_checkpoints (
                        trader_id, wallet_address, last_closed_position_timestamp,
                        last_fetch_at, total_closed_positions_fetched, fetch_status,
                        retry_count, created_at, updated_at
                    ) VALUES (
                        :trader_id, :wallet_address, :timestamp, :fetch_at,
                        0, 'completed', 0, :created_at, :updated_at
                    )
                """),
                {
                    "trader_id": trader_id,
                    "wallet_address": wallet_address,
                    "timestamp": latest_timestamp,
                    "fetch_at": datetime.utcnow(),
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow()
                }
            )
    except Exception as e:
        print(f"  ⚠️ Error updating checkpoint: {e}")


async def process_trader(
    session: AsyncSession,
    trader_id: int,
    wallet_address: str,
    checkpoint: Optional[Dict],
    retry_count: int = 0
) -> Tuple[int, str]:
    """
    Process a single trader's trades.
    
    Returns:
        Tuple of (new_trades_count, status)
    """
    try:
        # Determine if this is a new trader or incremental fetch
        since_timestamp = None
        if checkpoint and checkpoint.get("last_trade_timestamp"):
            since_timestamp = checkpoint["last_trade_timestamp"]
            print(f"  📥 Incremental fetch (since timestamp {since_timestamp})")
        else:
            print(f"  📥 Full fetch (new trader)")
        
        # Fetch trades
        trades = await fetch_trades_since_timestamp(
            wallet_address,
            since_timestamp=since_timestamp
        )
        
        if not trades:
            print(f"  ✓ No new trades")
            return 0, "completed"
        
        # Save trades
        inserted_count = await save_trades(session, trader_id, trades)
        
        # Find latest timestamp
        latest_timestamp = max(t.get("timestamp", 0) for t in trades)
        
        # Update checkpoint
        await update_checkpoint(
            session, trader_id, wallet_address,
            latest_timestamp, inserted_count
        )
        
        print(f"  ✓ Saved {inserted_count} new trades (fetched {len(trades)} total)")
        return inserted_count, "completed"
        
    except Exception as e:
        error_msg = str(e)
        print(f"  ✗ Error: {error_msg}")
        
        if retry_count < MAX_RETRIES:
            delay = min(BASE_DELAY * (2 ** retry_count), MAX_DELAY)
            print(f"  ⏳ Retrying in {delay}s... (attempt {retry_count + 1}/{MAX_RETRIES})")
            await asyncio.sleep(delay)
            return await process_trader(session, trader_id, wallet_address, checkpoint, retry_count + 1)
        else:
            return 0, "error"


async def main(limit: Optional[int] = None, force: bool = False, dry_run: bool = False):
    """Main function to fetch trades for all traders."""
    print("=" * 70)
    print("Polymarket Trader Trades Fetcher (with Checkpoints)")
    print("=" * 70)
    
    # Create database engine
    engine = create_async_engine(settings.DATABASE_URL, echo=False)
    
    try:
        # Create tables if they don't exist
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        
        print("✅ Database tables ready\n")
        
        # Create session
        AsyncSessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=engine,
            class_=AsyncSession
        )
        
        async with AsyncSessionLocal() as session:
            # Fetch all traders
            result = await session.execute(
                text("SELECT id, wallet_address FROM trader_leaderboard ORDER BY id")
            )
            traders = result.fetchall()
            
            if limit:
                traders = traders[:limit]
            
            print(f"📊 Found {len(traders)} traders to process\n")
            
            if dry_run:
                print("🔍 DRY RUN MODE - No actual API calls or database changes\n")
                for trader_id, wallet_address in traders[:5]:
                    print(f"Would process: {wallet_address}")
                return
            
            # Statistics
            total_new_trades = 0
            total_processed = 0
            total_errors = 0
            total_skipped = 0
            start_time = time.time()
            
            # Process traders
            for idx, (trader_id, wallet_address) in enumerate(traders, 1):
                print(f"\n[{idx}/{len(traders)}] Processing {wallet_address[:10]}...")
                
                checkpoint = await get_checkpoint(session, trader_id)
                
                # Skip if recently fetched and not forcing
                if not force and checkpoint:
                    status = checkpoint.get("fetch_status")
                    last_fetch = checkpoint.get("last_fetch_at")
                    
                    if status == "completed" and last_fetch:
                        # Skip if fetched within last 24 hours
                        if isinstance(last_fetch, datetime):
                            age = datetime.utcnow() - last_fetch
                            if age < timedelta(hours=24):
                                print(f"  ⏭️  Skipped (fetched {age.seconds // 3600}h ago)")
                                total_skipped += 1
                                continue
                
                # Process trader
                new_trades, status = await process_trader(
                    session, trader_id, wallet_address, checkpoint
                )
                
                total_new_trades += new_trades
                total_processed += 1
                
                if status == "error":
                    total_errors += 1
                
                # Commit every BATCH_COMMIT_SIZE traders
                if idx % BATCH_COMMIT_SIZE == 0:
                    await session.commit()
                    print(f"\n  💾 Committed batch (processed {idx} traders)")
                
                # Rate limiting delay
                await asyncio.sleep(BASE_DELAY)
                
                # Progress update every 50 traders
                if idx % 50 == 0:
                    elapsed = time.time() - start_time
                    rate = idx / elapsed if elapsed > 0 else 0
                    remaining = len(traders) - idx
                    eta = remaining / rate if rate > 0 else 0
                    
                    print(f"\n📊 Progress: {idx}/{len(traders)} traders")
                    print(f"   New trades: {total_new_trades}")
                    print(f"   Errors: {total_errors}")
                    print(f"   Rate: {rate:.1f} traders/sec")
                    print(f"   ETA: {eta/60:.1f} minutes\n")
            
            # Final commit
            await session.commit()
            
            # Final statistics
            elapsed = time.time() - start_time
            print("\n" + "=" * 70)
            print("📊 FINAL STATISTICS")
            print("=" * 70)
            print(f"Total traders processed:     {total_processed}")
            print(f"Total traders skipped:       {total_skipped}")
            print(f"Total new trades saved:      {total_new_trades}")
            print(f"Total errors:                {total_errors}")
            print(f"Total time:                  {elapsed/60:.1f} minutes")
            print(f"Average rate:                {total_processed/elapsed:.2f} traders/sec")
            print("=" * 70)
            
            print("\n✅ Script completed successfully!")
            
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch trades for traders with checkpoint tracking")
    parser.add_argument("--limit", type=int, help="Limit number of traders to process")
    parser.add_argument("--force", action="store_true", help="Force re-fetch even if recently fetched")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without making changes")
    
    args = parser.parse_args()
    
    asyncio.run(main(limit=args.limit, force=args.force, dry_run=args.dry_run))
