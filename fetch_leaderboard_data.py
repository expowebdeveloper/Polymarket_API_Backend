"""
Script to fetch all traders from Polymarket leaderboard API and store in database.

This script:
1. Checks if trader_leaderboard table exists, creates it if not
2. Fetches all traders from the API with pagination
3. Handles errors and retries
4. Inserts new records or updates existing ones
5. Logs progress and statistics
"""

import asyncio
import json
import sys
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from app.core.config import settings
from app.db.models import TraderLeaderboard, Base
from app.services.data_fetcher import async_client


# Configuration
API_BASE_URL = "https://data-api.polymarket.com/v1/leaderboard"
LIMIT = 50  # Records per page (API maximum is 50)
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds


async def check_table_exists(engine) -> bool:
    """Check if trader_leaderboard table exists."""
    async with engine.begin() as conn:
        result = await conn.execute(
            text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'trader_leaderboard'
                )
            """)
        )
        return result.scalar()


async def create_table(engine):
    """Create trader_leaderboard table if it doesn't exist."""
    # Check if table exists
    table_exists = await check_table_exists(engine)
    
    if not table_exists:
        print("Creating trader_leaderboard table...")
        async with engine.begin() as conn:
            # Create table using SQLAlchemy metadata
            await conn.run_sync(Base.metadata.create_all, tables=[TraderLeaderboard.__table__])
        print("✅ Table created successfully!")
    else:
        print("✅ Table trader_leaderboard already exists")


async def fetch_leaderboard_page(
    time_period: str = "all",
    order_by: str = "PNL",
    category: str = "overall",
    limit: int = 50,
    offset: int = 0,
    retry_count: int = 0
) -> Optional[Tuple[List[Dict], Dict]]:
    """
    Fetch a single page of leaderboard data from the API.
    
    Args:
        time_period: Time period filter (all)
        order_by: Order by metric (PNL)
        category: Category filter (overall)
        limit: Number of records per page
        offset: Offset for pagination
        retry_count: Current retry attempt
    
    Returns:
        Tuple of (list of trader records, pagination dict) or None if failed after retries
        Pagination dict contains: limit, offset, total, has_more
    """
    try:
        params = {
            "timePeriod": time_period,
            "orderBy": order_by,
            "category": category,
            "limit": limit,
            "offset": offset
        }
        
        response = await async_client.get(API_BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()
        
        # Debug: Print response type for first request
        if offset == 0:
            print(f"\n   🔍 Debug: API response type: {type(data)}")
            if isinstance(data, list):
                print(f"   🔍 Debug: Response is a list with {len(data)} items")
            elif isinstance(data, dict):
                print(f"   🔍 Debug: Response is a dict with keys: {list(data.keys())}")
        
        # Handle different response formats
        traders = []
        pagination = {
            "limit": limit,
            "offset": offset,
            "total": 0,
            "has_more": False
        }
        
        if isinstance(data, list):
            # Direct list response (most common format)
            traders = data
            # If we got exactly the limit, assume there might be more
            # Only stop if we get fewer than the limit
            pagination["has_more"] = len(traders) >= limit
            pagination["total"] = offset + len(traders) if len(traders) < limit else None
        elif isinstance(data, dict):
            # Response with pagination metadata
            if "data" in data or "traders" in data:
                traders = data.get("data") or data.get("traders") or []
            else:
                # Might be a list in the response
                traders = data if isinstance(data, list) else []
            
            # Extract pagination info if available
            if "pagination" in data:
                pag_info = data["pagination"]
                pagination.update({
                    "limit": pag_info.get("limit", limit),
                    "offset": pag_info.get("offset", offset),
                    "total": pag_info.get("total", 0),
                    "has_more": pag_info.get("has_more", False)
                })
            else:
                # Estimate based on response size
                pagination["has_more"] = len(traders) == limit
        else:
            print(f"⚠️  Warning: API returned unexpected data type at offset {offset}: {type(data)}")
            return ([], pagination)
        
        return (traders, pagination)
            
    except Exception as e:
        if retry_count < MAX_RETRIES:
            print(f"⚠️  Error fetching offset {offset} (attempt {retry_count + 1}/{MAX_RETRIES}): {e}")
            await asyncio.sleep(RETRY_DELAY * (retry_count + 1))  # Exponential backoff
            return await fetch_leaderboard_page(
                time_period, order_by, category, limit, offset, retry_count + 1
            )
        else:
            print(f"❌ Failed to fetch offset {offset} after {MAX_RETRIES} retries: {e}")
            return None


async def process_trader_record(session: AsyncSession, trader_data: Dict) -> Tuple[bool, bool]:
    """
    Process a single trader record: insert if new, update if exists.
    
    Args:
        session: Database session
        trader_data: Raw trader data from API
    
    Returns:
        Tuple of (was_inserted, was_updated)
    """
    try:
        # Extract wallet address (primary key)
        # API uses "user" or "proxyWallet" - check "user" first as per data_fetcher.py
        wallet_address = (
            trader_data.get("user") or
            trader_data.get("proxyWallet") or 
            trader_data.get("proxy_wallet") or
            trader_data.get("wallet_address") or 
            trader_data.get("wallet") or
            trader_data.get("address")
        )
        
        if not wallet_address or not wallet_address.startswith("0x") or len(wallet_address) != 42:
            # Log first few failures for debugging
            if not hasattr(process_trader_record, '_debug_logged'):
                print(f"   ⚠️  Warning: Invalid wallet address in trader data. Keys: {list(trader_data.keys())[:10]}")
                process_trader_record._debug_logged = True
            return False, False
        
        wallet_address = wallet_address.lower()
        
        # Extract fields
        rank = trader_data.get("rank")
        name = trader_data.get("userName") or trader_data.get("name")
        pseudonym = trader_data.get("xUsername") or trader_data.get("pseudonym")
        profile_image = trader_data.get("profileImage") or trader_data.get("profile_image")
        pnl = trader_data.get("pnl")
        volume = trader_data.get("vol") or trader_data.get("volume")
        roi = trader_data.get("roi")
        win_rate = trader_data.get("winRate") or trader_data.get("win_rate")
        trades_count = trader_data.get("totalTrades") or trader_data.get("trades_count") or trader_data.get("trades")
        verified_badge = trader_data.get("verifiedBadge") or trader_data.get("verified_badge", False)
        
        # Convert to appropriate types
        if pnl is not None:
            pnl = float(pnl)
        if volume is not None:
            volume = float(volume)
        if roi is not None:
            roi = float(roi)
        if win_rate is not None:
            win_rate = float(win_rate)
        if trades_count is not None:
            trades_count = int(trades_count)
        if rank is not None:
            rank = int(rank)
        
        # Store full API response as JSON
        raw_data = json.dumps(trader_data)
        
        # Check if record exists
        result = await session.execute(
            text("SELECT id FROM trader_leaderboard WHERE wallet_address = :wallet"),
            {"wallet": wallet_address}
        )
        existing = result.fetchone()
        
        # Prepare data dict
        data = {
            "wallet": wallet_address,
            "rank": rank,
            "name": name,
            "pseudonym": pseudonym,
            "profile_image": profile_image,
            "pnl": pnl,
            "volume": volume,
            "roi": roi,
            "win_rate": win_rate,
            "trades_count": trades_count,
            "verified_badge": verified_badge if verified_badge is not None else False,
            "raw_data": raw_data,
            "updated_at": datetime.utcnow()
        }
        
        if existing:
            # Update existing record
            await session.execute(
                text("""
                    UPDATE trader_leaderboard 
                    SET rank = :rank,
                        name = :name,
                        pseudonym = :pseudonym,
                        profile_image = :profile_image,
                        pnl = :pnl,
                        volume = :volume,
                        roi = :roi,
                        win_rate = :win_rate,
                        trades_count = :trades_count,
                        verified_badge = :verified_badge,
                        raw_data = :raw_data,
                        updated_at = :updated_at
                    WHERE wallet_address = :wallet
                """),
                data
            )
            return False, True
        else:
            # Insert new record
            data["created_at"] = datetime.utcnow()
            await session.execute(
                text("""
                    INSERT INTO trader_leaderboard 
                    (wallet_address, rank, name, pseudonym, profile_image, pnl, volume, 
                     roi, win_rate, trades_count, verified_badge, raw_data, created_at, updated_at)
                    VALUES 
                    (:wallet, :rank, :name, :pseudonym, :profile_image, :pnl, :volume,
                     :roi, :win_rate, :trades_count, :verified_badge, :raw_data, :created_at, :updated_at)
                """),
                data
            )
            return True, False
            
    except Exception as e:
        print(f"⚠️  Error processing trader record: {e}")
        return False, False


async def fetch_all_leaderboard_data(session: AsyncSession):
    """
    Fetch all leaderboard data from the API with pagination.
    
    Args:
        session: Database session
    """
    offset = 0
    total_fetched = 0
    total_inserted = 0
    total_updated = 0
    failed_offsets = []
    consecutive_no_new = 0  # Track consecutive batches with no new records
    MAX_CONSECUTIVE_NO_NEW = 10  # Stop after 10 consecutive batches with no new records
    seen_wallets_this_run = set()  # Track wallets seen in this run to detect API cycling
    
    print(f"\n🚀 Starting to fetch leaderboard data...")
    print(f"📋 Configuration: limit={LIMIT}, timePeriod=all, orderBy=PNL, category=overall\n")
    
    while True:
        print(f"📥 Fetching offset {offset}...", end=" ")
        
        # Fetch page
        result = await fetch_leaderboard_page(
            time_period="all",
            order_by="PNL",
            category="overall",
            limit=LIMIT,
            offset=offset
        )
        
        if result is None:
            # Failed after retries
            failed_offsets.append(offset)
            print(f"❌ Failed")
            offset += LIMIT
            continue
        
        traders, pagination = result
        
        if len(traders) == 0:
            print(f"✅ No more data (empty response)")
            break
        
        print(f"✅ Fetched {len(traders)} traders", end="")
        if pagination.get("total"):
            print(f" (Total available: {pagination['total']})", end="")
        print()
        
        # Debug: Show sample wallet addresses from this batch (first 3)
        if traders:
            sample_wallets = []
            for t in traders[:3]:
                wallet = (
                    t.get("user") or
                    t.get("proxyWallet") or 
                    t.get("proxy_wallet") or
                    t.get("wallet_address") or 
                    t.get("wallet")
                )
                if wallet:
                    sample_wallets.append(wallet[:10] + "..." if len(wallet) > 10 else wallet)
            if sample_wallets:
                print(f"   📋 Sample wallets: {', '.join(sample_wallets)}")
        
        total_fetched += len(traders)
        
        # Process each trader
        batch_inserted = 0
        batch_updated = 0
        batch_skipped = 0
        batch_duplicates_in_response = 0  # Track duplicates within the same API response
        
        # Extract wallet addresses from this batch to check for duplicates
        batch_wallets = []
        invalid_wallets = 0
        for trader in traders:
            wallet = (
                trader.get("user") or
                trader.get("proxyWallet") or 
                trader.get("proxy_wallet") or
                trader.get("wallet_address") or 
                trader.get("wallet")
            )
            if wallet and wallet.startswith("0x") and len(wallet) == 42:
                wallet_lower = wallet.lower()
                batch_wallets.append(wallet_lower)
                if wallet_lower in seen_wallets_this_run:
                    batch_duplicates_in_response += 1
                seen_wallets_this_run.add(wallet_lower)
            else:
                invalid_wallets += 1
        
        # Check for duplicates within this batch
        if len(batch_wallets) != len(set(batch_wallets)):
            duplicates_in_batch = len(batch_wallets) - len(set(batch_wallets))
            print(f"   ⚠️  Warning: {duplicates_in_batch} duplicate wallet(s) within this API response!")
        
        if invalid_wallets > 0:
            print(f"   ⚠️  Warning: {invalid_wallets} trader(s) with invalid/missing wallet address")
        
        # Every 100 batches, show statistics
        if (offset // LIMIT) % 100 == 0 and offset > 0:
            print(f"   📊 Progress: {len(seen_wallets_this_run)} unique wallets seen so far at offset {offset}")
        
        # Process each trader
        for trader in traders:
            inserted, updated = await process_trader_record(session, trader)
            if inserted:
                batch_inserted += 1
                total_inserted += 1
            elif updated:
                batch_updated += 1
                total_updated += 1
            else:
                batch_skipped += 1  # Invalid wallet address or other issue
        
        # Commit batch
        try:
            await session.commit()
            status_msg = f"   💾 Committed: {batch_inserted} inserted, {batch_updated} updated"
            if batch_skipped > 0:
                status_msg += f", {batch_skipped} skipped"
            if batch_duplicates_in_response > 0:
                status_msg += f" (⚠️ {batch_duplicates_in_response} duplicates from API)"
            print(status_msg)
        except Exception as e:
            print(f"   ❌ Error committing batch: {e}")
            await session.rollback()
        
        # Check if we're seeing wallets we've already processed (API cycling)
        if batch_duplicates_in_response > 0:
            print(f"   ⚠️  API appears to be cycling - {batch_duplicates_in_response} wallet(s) already seen in this run")
            # Show which wallets are duplicates
            if batch_duplicates_in_response <= 5:
                duplicate_list = []
                for trader in traders:
                    wallet = (
                        trader.get("user") or
                        trader.get("proxyWallet") or 
                        trader.get("proxy_wallet") or
                        trader.get("wallet_address") or 
                        trader.get("wallet")
                    )
                    if wallet and wallet.startswith("0x") and len(wallet) == 42:
                        wallet_lower = wallet.lower()
                        if wallet_lower in seen_wallets_this_run and wallet_lower not in duplicate_list:
                            duplicate_list.append(wallet_lower)
                            if len(duplicate_list) >= 3:
                                break
                if duplicate_list:
                    print(f"      Example duplicates: {', '.join([w[:10] + '...' for w in duplicate_list[:3]])}")
        
        # Check if we got any new records
        if batch_inserted == 0:
            consecutive_no_new += 1
            if consecutive_no_new >= MAX_CONSECUTIVE_NO_NEW:
                print(f"\n⚠️  Stopping: {MAX_CONSECUTIVE_NO_NEW} consecutive batches with no new records")
                print(f"   Total unique wallets seen in this run: {len(seen_wallets_this_run)}")
                print(f"   This suggests we've fetched all unique traders from the API")
                print(f"   The API may only have ~{len(seen_wallets_this_run)} unique traders")
                break
        else:
            consecutive_no_new = 0  # Reset counter if we got new records
        
        # Check if we should continue using pagination metadata
        has_more = pagination.get("has_more", False)
        
        # Stop if we got fewer records than requested (definitely no more data)
        if len(traders) < LIMIT:
            print(f"✅ Reached end of data (returned {len(traders)} < {LIMIT})")
            break
        
        # Stop if API explicitly says no more data
        if not has_more and len(traders) == LIMIT:
            # Got full page but API says no more - this shouldn't happen, but handle it
            print(f"⚠️  Got full page but has_more=false, continuing to verify...")
            # Continue one more time to verify
        
        # If has_more is False and we got less than limit, definitely stop
        if not has_more:
            print(f"✅ Reached end of data (has_more=false)")
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
    print(f"Total records updated:       {total_updated}")
    print(f"Unique wallets in this run: {len(seen_wallets_this_run)}")
    print(f"Final offset reached:       {offset}")
    print(f"Failed offsets:             {len(failed_offsets)}")
    if failed_offsets:
        print(f"Failed offset list:         {failed_offsets[:10]}{'...' if len(failed_offsets) > 10 else ''}")
    if consecutive_no_new >= MAX_CONSECUTIVE_NO_NEW:
        print(f"\n⚠️  Note: Stopped early due to {MAX_CONSECUTIVE_NO_NEW} consecutive batches with no new records")
        print(f"   This likely means all unique traders have been fetched")
    print(f"{'='*60}\n")
    
    return {
        "total_fetched": total_fetched,
        "total_inserted": total_inserted,
        "total_updated": total_updated,
        "failed_offsets": failed_offsets
    }


async def main():
    """Main function to run the ingestion script."""
    print("="*60)
    print("Polymarket Leaderboard Data Ingestion Script")
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
                text("SELECT COUNT(*) FROM trader_leaderboard")
            )
            existing_count = result.scalar()
            
            result_unique = await session.execute(
                text("SELECT COUNT(DISTINCT wallet_address) FROM trader_leaderboard")
            )
            existing_unique = result_unique.scalar()
            
            print(f"📊 Current database state:")
            print(f"   Total records: {existing_count}")
            print(f"   Unique wallets: {existing_unique}\n")
            
            # Fetch all data
            stats = await fetch_all_leaderboard_data(session)
            
            # Verify final count
            result = await session.execute(
                text("SELECT COUNT(*) FROM trader_leaderboard")
            )
            total_in_db = result.scalar()
            
            # Check for unique wallet addresses
            result_unique = await session.execute(
                text("SELECT COUNT(DISTINCT wallet_address) FROM trader_leaderboard")
            )
            unique_wallets = result_unique.scalar()
            
            print(f"\n✅ Final database state:")
            print(f"   Total records: {total_in_db}")
            print(f"   Unique wallet addresses: {unique_wallets}")
            print(f"   New records added: {total_in_db - existing_count}")
            print(f"   New unique wallets: {unique_wallets - existing_unique}")
            
            if total_in_db != unique_wallets:
                print(f"⚠️  Warning: {total_in_db - unique_wallets} duplicate wallet addresses found!")
            
            # Check if we hit the limit
            if stats["total_inserted"] == 0 and stats["total_updated"] > 0:
                print(f"\n⚠️  ANALYSIS:")
                print(f"   The API appears to only have ~{unique_wallets} unique traders.")
                print(f"   After offset ~{unique_wallets // LIMIT * LIMIT}, the API cycles through the same traders.")
                print(f"   This is why you see '0 inserted, 50 updated' - all traders already exist in DB.")
                print(f"   Polymarket leaderboard likely only tracks traders with significant activity.")
        
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
