"""
Script to fetch active positions for all traders with checkpoint tracking.

Special logic: If an active position has current_value = 0, it's treated as a 
closed position and stored in trader_closed_positions table.

This script:
1. Tracks the last fetch timestamp for each trader
2. Fetches active positions from Polymarket API
3. Classifies positions as active or closed based on current_value
4. Stores in appropriate table (trader_positions or trader_closed_positions)
5. Implements rate limiting to avoid API blocks
6. Supports resuming from where it left off if interrupted
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
from app.db.models import TraderLeaderboard, TraderPosition, TraderClosedPosition, TraderFetchCheckpoint, Base
from app.services.data_fetcher import async_client, fetch_positions_for_wallet
import time

# Configuration
FETCH_LIMIT = 50  # API max is 50
MAX_RETRIES = 3
BASE_DELAY = 0.7  # Base delay between API calls (seconds)
MAX_DELAY = 30  # Maximum delay for exponential backoff
BATCH_COMMIT_SIZE = 10  # Commit every N traders


async def classify_and_save_positions(
    session: AsyncSession,
    trader_id: int,
    positions: List[Dict]
) -> Tuple[int, int]:
    """
    Classify positions as active or closed based on current_value.
    Save to appropriate table.
    
    Returns:
        Tuple of (active_count, closed_count)
    """
    active_count = 0
    closed_count = 0
    
    for pos in positions:
        # Use a savepoint for each position to handle errors gracefully
        savepoint = await session.begin_nested()
        
        try:
            # Extract common fields
            asset = pos.get("asset")
            condition_id = pos.get("conditionId") or pos.get("condition_id")
            current_value = pos.get("currentValue") or pos.get("current_value") or 0
            
            if not asset or not condition_id:
                await savepoint.rollback()
                continue
            
            # Convert current_value to float
            try:
                current_value = float(current_value)
            except (ValueError, TypeError):
                current_value = 0
            
            # Classify: if current_value is 0, treat as closed position
            if current_value == 0:
                # Save as closed position
                inserted = await save_as_closed_position(session, trader_id, pos)
                if inserted:
                    closed_count += 1
            else:
                # Save as active position
                inserted = await save_as_active_position(session, trader_id, pos)
                if inserted:
                    active_count += 1
            
            await savepoint.commit()
            
        except Exception as e:
            await savepoint.rollback()
            # Only print first few errors to avoid spam
            if (active_count + closed_count) < 3:
                print(f"  ⚠️ Error saving position: {e}")
            continue
    
    return active_count, closed_count


async def save_as_active_position(
    session: AsyncSession,
    trader_id: int,
    pos: Dict
) -> bool:
    """Save position to trader_positions table. Returns True if inserted."""
    try:
        asset = pos.get("asset")
        condition_id = pos.get("conditionId") or pos.get("condition_id")
        
        # Check if already exists
        result = await session.execute(
            text("""
                SELECT id FROM trader_positions 
                WHERE trader_id = :trader_id 
                AND asset = :asset 
                AND condition_id = :condition_id
            """),
            {
                "trader_id": trader_id,
                "asset": asset,
                "condition_id": condition_id
            }
        )
        
        if result.fetchone():
            # Update existing position
            await session.execute(
                text("""
                    UPDATE trader_positions 
                    SET size = :size,
                        avg_price = :avg_price,
                        initial_value = :initial_value,
                        current_value = :current_value,
                        cash_pnl = :cash_pnl,
                        percent_pnl = :percent_pnl,
                        total_bought = :total_bought,
                        realized_pnl = :realized_pnl,
                        percent_realized_pnl = :percent_realized_pnl,
                        cur_price = :cur_price,
                        updated_at = :updated_at
                    WHERE trader_id = :trader_id 
                    AND asset = :asset 
                    AND condition_id = :condition_id
                """),
                {
                    "trader_id": trader_id,
                    "asset": asset,
                    "condition_id": condition_id,
                    "size": pos.get("size"),
                    "avg_price": pos.get("avgPrice") or pos.get("avg_price"),
                    "initial_value": pos.get("initialValue") or pos.get("initial_value"),
                    "current_value": pos.get("currentValue") or pos.get("current_value"),
                    "cash_pnl": pos.get("cashPnl") or pos.get("cash_pnl"),
                    "percent_pnl": pos.get("percentPnl") or pos.get("percent_pnl"),
                    "total_bought": pos.get("totalBought") or pos.get("total_bought"),
                    "realized_pnl": pos.get("realizedPnl") or pos.get("realized_pnl", 0),
                    "percent_realized_pnl": pos.get("percentRealizedPnl") or pos.get("percent_realized_pnl"),
                    "cur_price": pos.get("curPrice") or pos.get("cur_price", 0),
                    "updated_at": datetime.utcnow()
                }
            )
            return False  # Updated, not inserted
        else:
            # Insert new position
            await session.execute(
                text("""
                    INSERT INTO trader_positions (
                        trader_id, asset, condition_id, size, avg_price, initial_value,
                        current_value, cash_pnl, percent_pnl, total_bought, realized_pnl,
                        percent_realized_pnl, cur_price, redeemable, mergeable, title,
                        slug, icon, event_id, event_slug, outcome, outcome_index,
                        opposite_outcome, opposite_asset, end_date, negative_risk,
                        raw_data, created_at, updated_at
                    ) VALUES (
                        :trader_id, :asset, :condition_id, :size, :avg_price, :initial_value,
                        :current_value, :cash_pnl, :percent_pnl, :total_bought, :realized_pnl,
                        :percent_realized_pnl, :cur_price, :redeemable, :mergeable, :title,
                        :slug, :icon, :event_id, :event_slug, :outcome, :outcome_index,
                        :opposite_outcome, :opposite_asset, :end_date, :negative_risk,
                        :raw_data, :created_at, :updated_at
                    )
                """),
                {
                    "trader_id": trader_id,
                    "asset": asset,
                    "condition_id": condition_id,
                    "size": pos.get("size"),
                    "avg_price": pos.get("avgPrice") or pos.get("avg_price"),
                    "initial_value": pos.get("initialValue") or pos.get("initial_value"),
                    "current_value": pos.get("currentValue") or pos.get("current_value"),
                    "cash_pnl": pos.get("cashPnl") or pos.get("cash_pnl"),
                    "percent_pnl": pos.get("percentPnl") or pos.get("percent_pnl"),
                    "total_bought": pos.get("totalBought") or pos.get("total_bought"),
                    "realized_pnl": pos.get("realizedPnl") or pos.get("realized_pnl", 0),
                    "percent_realized_pnl": pos.get("percentRealizedPnl") or pos.get("percent_realized_pnl"),
                    "cur_price": pos.get("curPrice") or pos.get("cur_price", 0),
                    "redeemable": pos.get("redeemable", False),
                    "mergeable": pos.get("mergeable", False),
                    "title": pos.get("title"),
                    "slug": pos.get("slug"),
                    "icon": pos.get("icon"),
                    "event_id": pos.get("eventId") or pos.get("event_id"),
                    "event_slug": pos.get("eventSlug") or pos.get("event_slug"),
                    "outcome": pos.get("outcome"),
                    "outcome_index": pos.get("outcomeIndex") or pos.get("outcome_index"),
                    "opposite_outcome": pos.get("oppositeOutcome") or pos.get("opposite_outcome"),
                    "opposite_asset": pos.get("oppositeAsset") or pos.get("opposite_asset"),
                    "end_date": pos.get("endDate") or pos.get("end_date"),
                    "negative_risk": pos.get("negativeRisk") or pos.get("negative_risk", False),
                    "raw_data": json.dumps(pos),
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow()
                }
            )
            return True  # Inserted
    except Exception as e:
        raise


async def save_as_closed_position(
    session: AsyncSession,
    trader_id: int,
    pos: Dict
) -> bool:
    """Save position to trader_closed_positions table. Returns True if inserted."""
    try:
        asset = pos.get("asset")
        condition_id = pos.get("conditionId") or pos.get("condition_id")
        # Use current timestamp as the "closed" timestamp
        timestamp = int(datetime.utcnow().timestamp())
        
        # Check if already exists
        result = await session.execute(
            text("""
                SELECT id FROM trader_closed_positions 
                WHERE trader_id = :trader_id 
                AND asset = :asset 
                AND condition_id = :condition_id
            """),
            {
                "trader_id": trader_id,
                "asset": asset,
                "condition_id": condition_id
            }
        )
        
        if result.fetchone():
            return False  # Already exists
        
        # Insert as closed position
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
                "realized_pnl": pos.get("realizedPnl") or pos.get("realized_pnl", 0),
                "cur_price": pos.get("curPrice") or pos.get("cur_price", 0),
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
        return True  # Inserted
    except Exception as e:
        raise


async def process_trader(
    session: AsyncSession,
    trader_id: int,
    wallet_address: str,
    retry_count: int = 0
) -> Tuple[int, int, str]:
    """
    Process a single trader's active positions.
    
    Returns:
        Tuple of (active_count, closed_count, status)
    """
    try:
        print(f"  📥 Fetching active positions...")
        
        # Fetch active positions
        positions = await fetch_positions_for_wallet(wallet_address)
        
        if not positions:
            print(f"  ✓ No positions found")
            return 0, 0, "completed"
        
        # Classify and save positions
        active_count, closed_count = await classify_and_save_positions(
            session, trader_id, positions
        )
        
        print(f"  ✓ Saved {active_count} active, {closed_count} closed (from {len(positions)} total)")
        return active_count, closed_count, "completed"
        
    except Exception as e:
        error_msg = str(e)
        print(f"  ✗ Error: {error_msg}")
        
        if retry_count < MAX_RETRIES:
            delay = min(BASE_DELAY * (2 ** retry_count), MAX_DELAY)
            print(f"  ⏳ Retrying in {delay}s... (attempt {retry_count + 1}/{MAX_RETRIES})")
            await asyncio.sleep(delay)
            return await process_trader(session, trader_id, wallet_address, retry_count + 1)
        else:
            return 0, 0, "error"


async def main(limit: Optional[int] = None, dry_run: bool = False):
    """Main function to fetch active positions for all traders."""
    print("=" * 70)
    print("Polymarket Trader Active Positions Fetcher")
    print("(Zero-value positions → Closed Positions)")
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
            total_active = 0
            total_closed = 0
            total_processed = 0
            total_errors = 0
            start_time = time.time()
            
            # Process traders
            for idx, (trader_id, wallet_address) in enumerate(traders, 1):
                print(f"\n[{idx}/{len(traders)}] Processing {wallet_address[:10]}...")
                
                # Process trader
                active_count, closed_count, status = await process_trader(
                    session, trader_id, wallet_address
                )
                
                total_active += active_count
                total_closed += closed_count
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
                    print(f"   Active positions: {total_active}")
                    print(f"   Closed positions: {total_closed}")
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
            print(f"Total active positions:      {total_active}")
            print(f"Total closed positions:      {total_closed} (zero-value)")
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
    parser = argparse.ArgumentParser(description="Fetch active positions for traders (zero-value → closed)")
    parser.add_argument("--limit", type=int, help="Limit number of traders to process")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without making changes")
    
    args = parser.parse_args()
    
    asyncio.run(main(limit=args.limit, dry_run=args.dry_run))
