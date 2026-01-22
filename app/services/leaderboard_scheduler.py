"""
Background scheduler for recalculating leaderboard entries periodically.
Runs every 6-7 hours to keep the database up to date.
"""

import asyncio
from datetime import datetime, timedelta
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import AsyncSessionLocal
from app.services.leaderboard_storage_service import calculate_and_store_leaderboard_entries
import logging

logger = logging.getLogger(__name__)

# Global flag to track if scheduler is running
_scheduler_running = False
_last_run_time = None

async def recalculate_leaderboard_job():
    """
    Background job to recalculate and store all leaderboard entries.
    This should be called periodically (every 6-7 hours).
    """
    global _scheduler_running, _last_run_time
    
    if _scheduler_running:
        logger.warning("Leaderboard recalculation already in progress, skipping...")
        return
    
    _scheduler_running = True
    start_time = datetime.now()
    
    try:
        logger.info("🔄 Starting scheduled leaderboard recalculation...")
        
        async with AsyncSessionLocal() as session:
            # Calculate and store all leaderboard entries
            stats = await calculate_and_store_leaderboard_entries(
                session,
                wallet_addresses=None,  # Process all traders
                max_traders=None  # No limit
            )
            
            _last_run_time = datetime.now()
            duration = (_last_run_time - start_time).total_seconds()
            
            logger.info(f"✅ Leaderboard recalculation complete!")
            logger.info(f"   Processed: {stats['processed']}")
            logger.info(f"   Created: {stats['created']}")
            logger.info(f"   Updated: {stats['updated']}")
            logger.info(f"   Errors: {stats['errors']}")
            logger.info(f"   Duration: {duration:.2f} seconds")
            
            return stats
    
    except Exception as e:
        logger.error(f"❌ Error in leaderboard recalculation: {e}", exc_info=True)
        raise
    
    finally:
        _scheduler_running = False


async def start_periodic_recalculation(interval_hours: float = 6.5):
    """
    Start a background task that recalculates leaderboard every N hours.
    Does NOT run on startup; first run is after the first interval.
    
    Args:
        interval_hours: Interval in hours between recalculations (default: 6.5)
    """
    logger.info(f"📅 Starting periodic leaderboard recalculation (every {interval_hours} hours, no run on startup)")
    
    async def periodic_task():
        while True:
            try:
                # Wait for the interval first, then run (no immediate run on startup)
                await asyncio.sleep(interval_hours * 3600)  # Convert hours to seconds
                await recalculate_leaderboard_job()
            except Exception as e:
                logger.error(f"Error in periodic leaderboard recalculation: {e}", exc_info=True)
    
    # Start the task in the background using the current event loop
    try:
        loop = asyncio.get_event_loop()
        loop.create_task(periodic_task())
    except RuntimeError:
        # If no event loop is running, create one
        asyncio.create_task(periodic_task())


def get_last_run_time() -> Optional[datetime]:
    """Get the last time the leaderboard was recalculated."""
    return _last_run_time


def is_scheduler_running() -> bool:
    """Check if the scheduler is currently running."""
    return _scheduler_running



