"""
Optimized script to fetch trades from API and store into database efficiently.
Features:
- Batch processing for API calls and DB operations
- Retry logic with exponential backoff
- Bulk insert/upsert operations
- Progress tracking
- Error recovery
- Connection pooling
"""

import asyncio
import sys
import logging
from typing import List, Dict, Optional, Tuple
from decimal import Decimal
from datetime import datetime
import time

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import text

from app.db.models import Trade, Trader
from app.db.session import get_async_session
from app.services.data_fetcher import fetch_user_trades

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
BATCH_SIZE = 500  # Number of trades to process per batch
DB_BATCH_SIZE = 1000  # Number of trades to insert per DB batch
MAX_RETRIES = 3
RETRY_DELAY_BASE = 1  # Base delay in seconds for exponential backoff
API_PARALLEL_BATCH = 15  # Parallel API requests
DB_COMMIT_INTERVAL = 5  # Commit every N batches


class TradeFetcher:
    """Optimized trade fetcher with batch processing and error handling."""
    
    def __init__(self, session: AsyncSession):
        self.session = session
        self.stats = {
            'total_fetched': 0,
            'total_saved': 0,
            'total_errors': 0,
            'batches_processed': 0,
            'start_time': time.time()
        }
    
    async def fetch_trades_with_retry(
        self,
        wallet_address: str,
        limit: Optional[int] = None,
        offset: int = 0,
        max_retries: int = MAX_RETRIES
    ) -> List[Dict]:
        """
        Fetch trades with retry logic and exponential backoff.
        
        Args:
            wallet_address: Wallet address to fetch trades for
            limit: Maximum number of trades to fetch
            offset: Pagination offset
            max_retries: Maximum number of retry attempts
        
        Returns:
            List of trade dictionaries
        """
        for attempt in range(max_retries):
            try:
                trades = await fetch_user_trades(wallet_address, limit=limit, offset=offset)
                return trades
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Failed to fetch trades after {max_retries} attempts: {e}")
                    self.stats['total_errors'] += 1
                    raise
                
                delay = RETRY_DELAY_BASE * (2 ** attempt)
                logger.warning(f"Attempt {attempt + 1} failed, retrying in {delay}s: {e}")
                await asyncio.sleep(delay)
        
        return []
    
    def prepare_trade_dict(self, trade_data: Dict, wallet_address: str) -> Dict:
        """
        Prepare trade dictionary for database insertion.
        
        Args:
            trade_data: Raw trade data from API
            wallet_address: Wallet address
        
        Returns:
            Prepared trade dictionary
        """
        return {
            "proxy_wallet": trade_data.get("proxyWallet", wallet_address),
            "side": (trade_data.get("side", "BUY")).upper(),
            "asset": str(trade_data.get("asset", "")),
            "condition_id": trade_data.get("conditionId", ""),
            "size": Decimal(str(trade_data.get("size", 0))),
            "price": Decimal(str(trade_data.get("price", 0))),
            "timestamp": int(trade_data.get("timestamp", 0)),
            "title": trade_data.get("title"),
            "slug": trade_data.get("slug"),
            "icon": trade_data.get("icon"),
            "event_slug": trade_data.get("eventSlug"),
            "outcome": trade_data.get("outcome"),
            "outcome_index": trade_data.get("outcomeIndex"),
            "name": trade_data.get("name"),
            "pseudonym": trade_data.get("pseudonym"),
            "bio": trade_data.get("bio"),
            "profile_image": trade_data.get("profileImage"),
            "profile_image_optimized": trade_data.get("profileImageOptimized"),
            "transaction_hash": trade_data.get("transactionHash", ""),
        }
    
    async def bulk_upsert_trades(
        self,
        trades: List[Dict],
        wallet_address: str
    ) -> int:
        """
        Bulk upsert trades using PostgreSQL's ON CONFLICT.
        This is much faster than individual inserts.
        
        Args:
            trades: List of trade dictionaries
            wallet_address: Wallet address
        
        Returns:
            Number of trades saved
        """
        if not trades:
            return 0
        
        try:
            # Prepare all trade dictionaries
            trade_dicts = [self.prepare_trade_dict(trade, wallet_address) for trade in trades]
            
            # Bulk insert with conflict handling
            stmt = pg_insert(Trade).values(trade_dicts)
            stmt = stmt.on_conflict_do_update(
                constraint="uq_trade_unique",
                set_={
                    "side": stmt.excluded.side,
                    "size": stmt.excluded.size,
                    "price": stmt.excluded.price,
                    "title": stmt.excluded.title,
                    "slug": stmt.excluded.slug,
                    "icon": stmt.excluded.icon,
                    "event_slug": stmt.excluded.event_slug,
                    "outcome": stmt.excluded.outcome,
                    "outcome_index": stmt.excluded.outcome_index,
                    "name": stmt.excluded.name,
                    "pseudonym": stmt.excluded.pseudonym,
                    "bio": stmt.excluded.bio,
                    "profile_image": stmt.excluded.profile_image,
                    "profile_image_optimized": stmt.excluded.profile_image_optimized,
                    "updated_at": text("NOW()"),
                }
            )
            
            result = await self.session.execute(stmt)
            await self.session.commit()
            
            saved_count = len(trade_dicts)
            logger.info(f"✓ Bulk upserted {saved_count} trades for {wallet_address}")
            return saved_count
            
        except Exception as e:
            logger.error(f"Error in bulk upsert: {e}")
            await self.session.rollback()
            
            # Fallback: try smaller batches
            return await self._fallback_insert(trades, wallet_address)
    
    async def _fallback_insert(
        self,
        trades: List[Dict],
        wallet_address: str
    ) -> int:
        """
        Fallback method: insert trades in smaller batches if bulk insert fails.
        
        Args:
            trades: List of trade dictionaries
            wallet_address: Wallet address
        
        Returns:
            Number of trades saved
        """
        saved_count = 0
        small_batch_size = 100
        
        for i in range(0, len(trades), small_batch_size):
            batch = trades[i:i + small_batch_size]
            try:
                trade_dicts = [self.prepare_trade_dict(trade, wallet_address) for trade in batch]
                
                stmt = pg_insert(Trade).values(trade_dicts)
                stmt = stmt.on_conflict_do_update(
                    constraint="uq_trade_unique",
                    set_={
                        "side": stmt.excluded.side,
                        "size": stmt.excluded.size,
                        "price": stmt.excluded.price,
                        "title": stmt.excluded.title,
                        "slug": stmt.excluded.slug,
                        "icon": stmt.excluded.icon,
                        "event_slug": stmt.excluded.event_slug,
                        "outcome": stmt.excluded.outcome,
                        "outcome_index": stmt.excluded.outcome_index,
                        "name": stmt.excluded.name,
                        "pseudonym": stmt.excluded.pseudonym,
                        "bio": stmt.excluded.bio,
                        "profile_image": stmt.excluded.profile_image,
                        "profile_image_optimized": stmt.excluded.profile_image_optimized,
                        "updated_at": text("NOW()"),
                    }
                )
                
                await self.session.execute(stmt)
                await self.session.commit()
                saved_count += len(batch)
                
            except Exception as e:
                logger.error(f"Error in fallback batch insert: {e}")
                await self.session.rollback()
                # Try individual inserts for this batch
                for trade in batch:
                    try:
                        trade_dict = self.prepare_trade_dict(trade, wallet_address)
                        stmt = pg_insert(Trade).values(**trade_dict)
                        stmt = stmt.on_conflict_do_update(
                            constraint="uq_trade_unique",
                            set_={
                                "side": stmt.excluded.side,
                                "size": stmt.excluded.size,
                                "price": stmt.excluded.price,
                                "updated_at": text("NOW()"),
                            }
                        )
                        await self.session.execute(stmt)
                        await self.session.commit()
                        saved_count += 1
                    except Exception as inner_e:
                        logger.error(f"Failed to save individual trade: {inner_e}")
                        self.stats['total_errors'] += 1
        
        return saved_count
    
    async def get_or_create_trader(self, wallet_address: str, trade_data: Optional[Dict] = None) -> Trader:
        """
        Get or create trader record.
        
        Args:
            wallet_address: Wallet address
            trade_data: Optional trade data to extract trader info
        
        Returns:
            Trader object
        """
        # Check if trader exists
        stmt = text("SELECT id FROM traders WHERE wallet_address = :wallet_address")
        result = await self.session.execute(stmt, {"wallet_address": wallet_address})
        trader_id = result.scalar_one_or_none()
        
        if trader_id:
            # Trader exists, return a minimal Trader object (we only need the ID)
            trader = Trader()
            trader.id = trader_id
            trader.wallet_address = wallet_address
            return trader
        
        # Create new trader
        trader_dict = {
            "wallet_address": wallet_address,
            "name": trade_data.get("name") if trade_data else None,
            "pseudonym": trade_data.get("pseudonym") if trade_data else None,
            "bio": trade_data.get("bio") if trade_data else None,
            "profile_image": trade_data.get("profileImage") if trade_data else None,
            "profile_image_optimized": trade_data.get("profileImageOptimized") if trade_data else None,
        }
        
        stmt = text("""
            INSERT INTO traders (wallet_address, name, pseudonym, bio, profile_image, profile_image_optimized, created_at, updated_at)
            VALUES (:wallet_address, :name, :pseudonym, :bio, :profile_image, :profile_image_optimized, NOW(), NOW())
            ON CONFLICT (wallet_address) DO UPDATE SET
                name = COALESCE(EXCLUDED.name, traders.name),
                pseudonym = COALESCE(EXCLUDED.pseudonym, traders.pseudonym),
                bio = COALESCE(EXCLUDED.bio, traders.bio),
                profile_image = COALESCE(EXCLUDED.profile_image, traders.profile_image),
                profile_image_optimized = COALESCE(EXCLUDED.profile_image_optimized, traders.profile_image_optimized),
                updated_at = NOW()
            RETURNING id
        """)
        
        result = await self.session.execute(stmt, trader_dict)
        trader_id = result.scalar_one()
        await self.session.commit()
        
        trader = Trader()
        trader.id = trader_id
        trader.wallet_address = wallet_address
        return trader
    
    async def process_wallet(
        self,
        wallet_address: str,
        limit: Optional[int] = None
    ) -> Dict:
        """
        Process trades for a single wallet address.
        
        Args:
            wallet_address: Wallet address to process
            limit: Optional limit on number of trades to fetch
        
        Returns:
            Dictionary with processing results
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing trades for: {wallet_address}")
        logger.info(f"{'='*60}")
        
        wallet_start_time = time.time()
        wallet_stats = {
            'wallet_address': wallet_address,
            'fetched': 0,
            'saved': 0,
            'errors': 0,
            'batches': 0
        }
        
        try:
            # Step 1: Fetch all trades (with retry logic)
            logger.info(f"Fetching trades from API...")
            all_trades = await self.fetch_trades_with_retry(wallet_address, limit=limit)
            
            if not all_trades:
                logger.warning(f"No trades found for {wallet_address}")
                return {
                    **wallet_stats,
                    'error': 'No trades found'
                }
            
            wallet_stats['fetched'] = len(all_trades)
            self.stats['total_fetched'] += len(all_trades)
            logger.info(f"✓ Fetched {len(all_trades)} trades from API")
            
            # Step 2: Get or create trader
            first_trade = all_trades[0] if all_trades else {}
            trader = await self.get_or_create_trader(wallet_address, first_trade)
            logger.info(f"✓ Trader ID: {trader.id}")
            
            # Step 3: Process trades in batches for database insertion
            logger.info(f"Processing {len(all_trades)} trades in batches of {DB_BATCH_SIZE}...")
            
            for i in range(0, len(all_trades), DB_BATCH_SIZE):
                batch = all_trades[i:i + DB_BATCH_SIZE]
                batch_num = (i // DB_BATCH_SIZE) + 1
                total_batches = (len(all_trades) + DB_BATCH_SIZE - 1) // DB_BATCH_SIZE
                
                logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} trades)...")
                
                try:
                    saved = await self.bulk_upsert_trades(batch, wallet_address)
                    wallet_stats['saved'] += saved
                    wallet_stats['batches'] += 1
                    self.stats['total_saved'] += saved
                    self.stats['batches_processed'] += 1
                    
                    # Progress update
                    progress = (i + len(batch)) / len(all_trades) * 100
                    logger.info(f"  Progress: {progress:.1f}% ({i + len(batch)}/{len(all_trades)})")
                    
                except Exception as e:
                    logger.error(f"Error processing batch {batch_num}: {e}")
                    wallet_stats['errors'] += 1
                    self.stats['total_errors'] += 1
                    # Continue with next batch instead of failing completely
                    continue
            
            elapsed = time.time() - wallet_start_time
            logger.info(f"\n✓ Completed processing for {wallet_address}")
            logger.info(f"  Fetched: {wallet_stats['fetched']} trades")
            logger.info(f"  Saved: {wallet_stats['saved']} trades")
            logger.info(f"  Batches: {wallet_stats['batches']}")
            logger.info(f"  Errors: {wallet_stats['errors']}")
            logger.info(f"  Time: {elapsed:.2f}s ({wallet_stats['saved']/elapsed:.1f} trades/sec)")
            
            return {
                **wallet_stats,
                'success': True,
                'elapsed_time': elapsed
            }
            
        except Exception as e:
            logger.error(f"Error processing wallet {wallet_address}: {e}", exc_info=True)
            wallet_stats['errors'] += 1
            self.stats['total_errors'] += 1
            return {
                **wallet_stats,
                'error': str(e)
            }


async def main():
    """Main function to process trades for one or more wallet addresses."""
    if len(sys.argv) < 2:
        print("Usage: python fetch_trades_optimized.py <wallet_address> [wallet_address2] ...")
        print("Example: python fetch_trades_optimized.py 0xdbade4c82fb72780a0db9a38f821d8671aba9c95")
        sys.exit(1)
    
    wallet_addresses = sys.argv[1:]
    
    # Use existing database session
    async with AsyncSessionLocal() as session:
        try:
            fetcher = TradeFetcher(session)
            
            # Process each wallet
            results = []
            for wallet_address in wallet_addresses:
                result = await fetcher.process_wallet(wallet_address)
                results.append(result)
            
            # Print summary
            total_time = time.time() - fetcher.stats['start_time']
            print(f"\n{'='*60}")
            print("SUMMARY")
            print(f"{'='*60}")
            print(f"Total wallets processed: {len(wallet_addresses)}")
            print(f"Total trades fetched: {fetcher.stats['total_fetched']}")
            print(f"Total trades saved: {fetcher.stats['total_saved']}")
            print(f"Total batches processed: {fetcher.stats['batches_processed']}")
            print(f"Total errors: {fetcher.stats['total_errors']}")
            print(f"Total time: {total_time:.2f}s")
            if fetcher.stats['total_saved'] > 0:
                print(f"Average speed: {fetcher.stats['total_saved']/total_time:.1f} trades/sec")
            print(f"{'='*60}\n")
            
            # Print per-wallet results
            for result in results:
                if result.get('success'):
                    print(f"✓ {result['wallet_address']}: {result['saved']} trades saved in {result.get('elapsed_time', 0):.2f}s")
                else:
                    print(f"✗ {result['wallet_address']}: Error - {result.get('error', 'Unknown error')}")
        
        except Exception as e:
            logger.error(f"Error in main: {e}", exc_info=True)
            raise
        finally:
            break  # Exit after first session


if __name__ == "__main__":
    asyncio.run(main())
