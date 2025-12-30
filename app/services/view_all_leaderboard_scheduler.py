"""
Background scheduler for recalculating view-all leaderboard entries periodically.
Runs every 2 hours to keep the database up to date.
"""

import asyncio
from datetime import datetime
from typing import Optional
from app.db.session import AsyncSessionLocal
from app.services.view_all_leaderboard_storage import calculate_and_store_view_all_leaderboard
import logging

logger = logging.getLogger(__name__)

# Global flag to track if scheduler is running
_scheduler_running = False
_last_run_time = None


async def recalculate_view_all_leaderboard_job():
    """
    Background job to recalculate and store view-all leaderboard entries.
    This should be called periodically (every 2 hours).
    """
    global _scheduler_running, _last_run_time
    
    if _scheduler_running:
        logger.warning("View-all leaderboard recalculation already in progress, skipping...")
        return
    
    _scheduler_running = True
    start_time = datetime.now()
    
    try:
        logger.info("🔄 Starting scheduled view-all leaderboard recalculation...")
        
        async with AsyncSessionLocal() as session:
            # Calculate and store view-all leaderboard entries
            stats = await calculate_and_store_view_all_leaderboard(
                session,
                time_period="all",
                order_by="PNL",
                limit=500
            )
            
            _last_run_time = datetime.now()
            duration = (_last_run_time - start_time).total_seconds()
            
            logger.info(f"✅ View-all leaderboard recalculation complete!")
            logger.info(f"   Wallets fetched: {stats['wallets_fetched']}")
            logger.info(f"   Processed: {stats['processed']}")
            logger.info(f"   Created: {stats['created']}")
            logger.info(f"   Updated: {stats['updated']}")
            logger.info(f"   Errors: {stats['errors']}")
            logger.info(f"   Duration: {duration:.2f} seconds")
            
            return stats
    
    except Exception as e:
        logger.error(f"❌ Error in view-all leaderboard recalculation: {e}", exc_info=True)
        raise
    
    finally:
        _scheduler_running = False


async def start_periodic_view_all_recalculation(interval_hours: float = 2.0):
    """
    Start a background task that recalculates view-all leaderboard every N hours.
    Also runs immediately on startup.
    
    Args:
        interval_hours: Interval in hours between recalculations (default: 2.0)
    """
    logger.info(f"📅 Starting periodic view-all leaderboard recalculation (every {interval_hours} hours)")
    
    async def periodic_task():
        # Run immediately on startup
        try:
            await recalculate_view_all_leaderboard_job()
        except Exception as e:
            logger.error(f"Error in initial view-all leaderboard recalculation: {e}", exc_info=True)
        
        # Then run periodically
        while True:
            try:
                # Wait for the specified interval before next run
                await asyncio.sleep(interval_hours * 3600)  # Convert hours to seconds
                await recalculate_view_all_leaderboard_job()
            except Exception as e:
                logger.error(f"Error in periodic view-all leaderboard recalculation: {e}", exc_info=True)
    
    # Start the task in the background using the current event loop
    try:
        loop = asyncio.get_event_loop()
        loop.create_task(periodic_task())
    except RuntimeError:
        # If no event loop is running, create one
        asyncio.create_task(periodic_task())


def get_last_run_time() -> Optional[datetime]:
    """Get the last time the view-all leaderboard was recalculated."""
    return _last_run_time


def is_running() -> bool:
    """Check if the scheduler is currently running."""
    return _scheduler_running
