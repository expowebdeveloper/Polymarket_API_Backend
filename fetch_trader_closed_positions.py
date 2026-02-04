"""
Script to fetch closed positions for all traders with checkpoint tracking.

This script:
1. Tracks the last fetched timestamp for each trader
2. Only fetches new closed positions since the last checkpoint
3. For new traders, fetches all historical closed positions
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
from app.db.models import TraderLeaderboard, TraderClosedPosition, TraderFetchCheckpoint, Base
from app.services.data_fetcher import async_client
import time

# Configuration
API_BASE_URL = f"{settings.POLYMARKET_DATA_API_URL}/v1/closed-positions"
FETCH_LIMIT = 50  # API max is 50
MAX_RETRIES = 3
BASE_DELAY = 0.7  # Base delay between API calls (seconds)
MAX_DELAY = 30  # Maximum delay for exponential backoff
BATCH_COMMIT_SIZE = 10  # Commit every N traders
PARALLEL_LIMIT = 3  # Max concurrent traders to process


async def fetch_closed_positions_since_timestamp(
    wallet_address: str,
    since_timestamp: Optional[int] = None,
    limit: Optional[int] = None
) -> List[Dict]:
    """
    Fetch closed positions for a wallet, optionally since a specific timestamp.
    
    Args:
        wallet_address: Ethereum wallet address
        since_timestamp: Only fetch positions newer than this timestamp (None = fetch all)
        limit: Maximum number of positions to fetch (None = fetch all)
    
    Returns:
        List of closed position dictionaries
    """
    try:
        url = API_BASE_URL
        params = {
            "user": wallet_address,
            "sortBy": "timestamp",
            "sortDirection": "DESC",
            "limit": FETCH_LIMIT
        }
        
        all_positions = []
        offset = 0
        
        while True:
            params["offset"] = offset
            
            response = await async_client.get(url, params=params)
            response.raise_for_status()
            positions = response.json()
            
            if not positions or not isinstance(positions, list):
                break
            
            # Filter by timestamp if specified
            if since_timestamp is not None:
                new_positions = []
                for pos in positions:
                    pos_timestamp = pos.get("timestamp")
                    if pos_timestamp and pos_timestamp > since_timestamp:
                        new_positions.append(pos)
                    else:
                        # Since sorted DESC, we can stop when we hit older positions
                        return all_positions + new_positions
                
                all_positions.extend(new_positions)
            else:
                all_positions.extend(positions)
            
            # Check if we've fetched enough or reached the end
            if len(positions) < FETCH_LIMIT:
                break
            
            if limit and len(all_positions) >= limit:
                return all_positions[:limit]
            
            offset += FETCH_LIMIT
            
            # Small delay to avoid rate limiting
            await asyncio.sleep(0.3)
        
        return all_positions
        
    except Exception as e:
        print(f"  ✗ Error fetching closed positions for {wallet_address}: {e}")
        raise


async def save_closed_positions(
    session: AsyncSession,
    trader_id: int,
    positions: List[Dict]
) -> int:
    """
    Save closed positions to the database.
    
    Returns:
        Number of new positions inserted
    """
    inserted_count = 0
    
    for pos in positions:
        # Use a savepoint for each position to handle errors gracefully
        savepoint = await session.begin_nested()
        
        try:
            # Extract fields
            asset = pos.get("asset")
            condition_id = pos.get("conditionId") or pos.get("condition_id")
            timestamp = pos.get("timestamp")
            
            if not asset or not condition_id or not timestamp:
                await savepoint.rollback()
                continue
            
            # Check if already exists
            result = await session.execute(
                text("""
                    SELECT id FROM trader_closed_positions 
                    WHERE trader_id = :trader_id 
                    AND asset = :asset 
                    AND condition_id = :condition_id 
                    AND timestamp = :timestamp
                """),
                {
                    "trader_id": trader_id,
                    "asset": asset,
                    "condition_id": condition_id,
                    "timestamp": timestamp
                }
            )
            
            if result.fetchone():
                await savepoint.rollback()
                continue  # Already exists
            
            # Insert new position
            await session.execute(
                text("""
                    INSERT INTO trader_closed_positions (
                        trader_id, asset, condition_id, avg_price, total_bought,
                        realized_pnl, cur_price, title, slug, icon, event_slug,
                        outcome, outcome_index, opposite_outcome, opposite_asset,
                        end_date, timestamp, raw_data, created_at, updated_at
                    ) VALUES (
                        :trader_id, :asset, :condition_id, :avg_price, :total_bought,
                        :realized_pnl, :cur_price, :title, :slug, :icon, :event_slug,
                        :outcome, :outcome_index, :opposite_outcome, :opposite_asset,
                        :end_date, :timestamp, :raw_data, :created_at, :updated_at
                    )
                """),
                {
                    "trader_id": trader_id,
                    "asset": asset,
                    "condition_id": condition_id,
                    "avg_price": pos.get("avgPrice") or pos.get("avg_price"),
                    "total_bought": pos.get("totalBought") or pos.get("total_bought"),
                    "realized_pnl": pos.get("realizedPnl") or pos.get("realized_pnl"),
                    "cur_price": pos.get("curPrice") or pos.get("cur_price"),
                    "title": pos.get("title"),
                    "slug": pos.get("slug"),
                    "icon": pos.get("icon"),
                    "event_slug": pos.get("eventSlug") or pos.get("event_slug"),
                    "outcome": pos.get("outcome"),
                    "outcome_index": pos.get("outcomeIndex") or pos.get("outcome_index"),
                    "opposite_outcome": pos.get("oppositeOutcome") or pos.get("opposite_outcome"),
                    "opposite_asset": pos.get("oppositeAsset") or pos.get("opposite_asset"),
                    "end_date": pos.get("endDate") or pos.get("end_date"),
                    "timestamp": timestamp,
                    "raw_data": json.dumps(pos),
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
                print(f"  ⚠️ Error saving position: {e}")
            continue
    
    return inserted_count


async def update_checkpoint(
    session: AsyncSession,
    trader_id: int,
    wallet_address: str,
    latest_timestamp: Optional[int],
    total_new_positions: int,
    status: str = "completed",
    error_message: Optional[str] = None
):
    """Update or create checkpoint for a trader."""
    try:
        # Check if checkpoint exists
        result = await session.execute(
            text("SELECT id, total_closed_positions_fetched FROM trader_fetch_checkpoints WHERE trader_id = :trader_id"),
            {"trader_id": trader_id}
        )
        existing = result.fetchone()
        
        if existing:
            # Update existing checkpoint
            checkpoint_id, prev_total = existing
            new_total = (prev_total or 0) + total_new_positions
            
            await session.execute(
                text("""
                    UPDATE trader_fetch_checkpoints 
                    SET last_closed_position_timestamp = :timestamp,
                        last_fetch_at = :fetch_at,
                        total_closed_positions_fetched = :total,
                        fetch_status = :status,
                        error_message = :error,
                        updated_at = :updated_at
                    WHERE trader_id = :trader_id
                """),
                {
                    "trader_id": trader_id,
                    "timestamp": latest_timestamp,
                    "fetch_at": datetime.utcnow(),
                    "total": new_total,
                    "status": status,
                    "error": error_message,
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
                        error_message, retry_count, created_at, updated_at
                    ) VALUES (
                        :trader_id, :wallet_address, :timestamp, :fetch_at,
                        :total, :status, :error, 0, :created_at, :updated_at
                    )
                """),
                {
                    "trader_id": trader_id,
                    "wallet_address": wallet_address,
                    "timestamp": latest_timestamp,
                    "fetch_at": datetime.utcnow(),
                    "total": total_new_positions,
                    "status": status,
                    "error": error_message,
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
    Process a single trader's closed positions.
    
    Returns:
        Tuple of (new_positions_count, status)
    """
    try:
        # Determine if this is a new trader or incremental fetch
        since_timestamp = None
        if checkpoint and checkpoint.get("last_closed_position_timestamp"):
            since_timestamp = checkpoint["last_closed_position_timestamp"]
            print(f"  📥 Incremental fetch (since timestamp {since_timestamp})")
        else:
            print(f"  📥 Full fetch (new trader)")
        
        # Fetch closed positions
        positions = await fetch_closed_positions_since_timestamp(
            wallet_address,
            since_timestamp=since_timestamp
        )
        
        if not positions:
            print(f"  ✓ No new positions")
            await update_checkpoint(session, trader_id, wallet_address, since_timestamp, 0, "completed")
            return 0, "completed"
        
        # Save positions
        inserted_count = await save_closed_positions(session, trader_id, positions)
        
        # Find latest timestamp
        latest_timestamp = max(pos.get("timestamp", 0) for pos in positions)
        
        # Update checkpoint
        await update_checkpoint(
            session, trader_id, wallet_address,
            latest_timestamp, inserted_count, "completed"
        )
        
        print(f"  ✓ Saved {inserted_count} new positions (fetched {len(positions)} total)")
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
            await update_checkpoint(session, trader_id, wallet_address, None, 0, "error", error_msg)
            return 0, "error"


async def main(limit: Optional[int] = None, force: bool = False, dry_run: bool = False, user_identifier: Optional[str] = None):
    """Main function to fetch closed positions for all traders."""
    print("=" * 70)
    print("Polymarket Trader Closed Positions Fetcher (with Checkpoints)")
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
            # Fetch traders
            if user_identifier:
                print(f"🔍 Looking for user: {user_identifier}")
                # Try to find by wallet, name, or pseudonym
                result = await session.execute(
                    text("""
                        SELECT id, wallet_address FROM trader_leaderboard 
                        WHERE wallet_address = :id 
                           OR name = :id 
                           OR pseudonym = :id
                    """),
                    {"id": user_identifier}
                )
                traders = result.fetchall()
                
                if not traders:
                    # Try partial match if no exact match
                    result = await session.execute(
                        text("""
                            SELECT id, wallet_address FROM trader_leaderboard 
                            WHERE name ILIKE :id 
                               OR pseudonym ILIKE :id
                        """),
                        {"id": f"%{user_identifier}%"}
                    )
                    traders = result.fetchall()
                
                if not traders:
                    print(f"❌ User '{user_identifier}' not found in leaderboard")
                    return
            else:
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
            
            # Fetch existing checkpoints
            result = await session.execute(
                text("""
                    SELECT trader_id, last_closed_position_timestamp, fetch_status, last_fetch_at
                    FROM trader_fetch_checkpoints
                """)
            )
            checkpoints = {row[0]: {
                "last_closed_position_timestamp": row[1],
                "fetch_status": row[2],
                "last_fetch_at": row[3]
            } for row in result.fetchall()}
            
            # Statistics
            total_new_positions = 0
            total_processed = 0
            total_errors = 0
            total_skipped = 0
            start_time = time.time()
            
            # Process traders
            for idx, (trader_id, wallet_address) in enumerate(traders, 1):
                print(f"\n[{idx}/{len(traders)}] Processing {wallet_address[:10]}...")
                
                checkpoint = checkpoints.get(trader_id)
                
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
                new_positions, status = await process_trader(
                    session, trader_id, wallet_address, checkpoint
                )
                
                total_new_positions += new_positions
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
                    print(f"   New positions: {total_new_positions}")
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
            print(f"Total new positions saved:   {total_new_positions}")
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
    parser = argparse.ArgumentParser(description="Fetch closed positions for traders with checkpoint tracking")
    parser.add_argument("--limit", type=int, help="Limit number of traders to process")
    parser.add_argument("--force", action="store_true", help="Force re-fetch even if recently fetched")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without making changes")
    parser.add_argument("--user", type=str, help="Specific user (wallet, name, or pseudonym) to process")
    
    args = parser.parse_args()
    
    asyncio.run(main(limit=args.limit, force=args.force, dry_run=args.dry_run, user_identifier=args.user))
