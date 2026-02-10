"""
Background scheduler: refresh biggest winners of the month (API + scoring) once daily at 12 AM (midnight).
Persists result to JSON so the dashboard can load it from file. No refresh on server startup.
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

# Default: persist under backend/data (create if missing)
def _cache_dir() -> str:
    base = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    d = os.path.join(base, "data")
    os.makedirs(d, exist_ok=True)
    return d

BIGGEST_WINNERS_FILENAME = "biggest_winners_with_scoring.json"
CACHE_FILE_MAX_AGE_SECONDS = 24 * 3600 + 600  # 24h + 10min grace (daily job)
# Run only at 12 AM (midnight) in server local time
_scheduler_task: asyncio.Task | None = None
_running = False


def _cache_path() -> str:
    return os.path.join(_cache_dir(), BIGGEST_WINNERS_FILENAME)


def _fallback_cache_path() -> str:
    """Path when file lives in backend/ root (e.g. script run from backend with data/ missing)."""
    base = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    return os.path.join(base, BIGGEST_WINNERS_FILENAME)


def load_biggest_winners_from_file() -> List[Dict[str, Any]]:
    """Load persisted biggest winners from file. Return [] if missing or invalid."""
    for path in (_cache_path(), _fallback_cache_path()):
        if not os.path.isfile(path):
            continue
        try:
            with open(path, "r") as f:
                data = json.load(f)
            if isinstance(data, list) and data:
                return data
            return []
        except Exception as e:
            logger.warning("Could not load biggest winners cache file %s: %s", path, e)
    return []


def get_stored_biggest_winners(limit: int = 20) -> List[Dict[str, Any]]:
    """
    Return biggest winners from storage only (file written by 12 AM job).
    No Polymarket API calls. Use this for dashboard so only stored data is shown.
    """
    return load_biggest_winners_from_file()[:limit]


def save_biggest_winners_to_file(records: List[Dict[str, Any]]) -> None:
    """Persist biggest winners list to file."""
    path = _cache_path()
    try:
        with open(path, "w") as f:
            json.dump(records, f, indent=2)
        logger.info("Saved %s biggest winners to %s", len(records), path)
    except Exception as e:
        logger.error("Failed to save biggest winners to %s: %s", path, e)


def is_cache_file_fresh() -> bool:
    """True if cache file exists and is newer than 24 hours (daily refresh)."""
    path = _cache_path()
    if not os.path.isfile(path):
        return False
    try:
        age = time.time() - os.path.getmtime(path)
        return age < CACHE_FILE_MAX_AGE_SECONDS
    except Exception:
        return False


def _seconds_until_midnight_local() -> float:
    """Seconds until next 12 AM (midnight) in server local time."""
    now = datetime.now()
    next_midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return (next_midnight - now).total_seconds()


async def refresh_biggest_winners_job() -> List[Dict[str, Any]]:
    """
    Run the biggest-winners-with-scoring refresh (same logic as dashboard).
    Updates in-memory cache and persists to file.
    """
    global _running
    if _running:
        logger.warning("Biggest winners refresh already in progress, skipping.")
        return load_biggest_winners_from_file()

    _running = True
    try:
        from app.services.dashboard_service import fetch_biggest_winners_month_with_scoring

        logger.info("Refreshing biggest winners of the month (API + scoring)...")
        # Bypass cache so we always fetch fresh; result is then stored in cache and file with 12h TTL
        records = await fetch_biggest_winners_month_with_scoring(limit=20, force_refresh=True)
        if records:
            save_biggest_winners_to_file(records)
            # Update in-memory cache with 12h TTL (done inside fetch_* when we set it)
            from app.services.dashboard_service import _BIGGEST_WINNERS_CACHE
            _BIGGEST_WINNERS_CACHE["ts"] = time.time()
            _BIGGEST_WINNERS_CACHE["data"] = records
            logger.info("Biggest winners refresh done: %s entries", len(records))
        return records
    except Exception as e:
        logger.error("Biggest winners refresh failed: %s", e, exc_info=True)
        return load_biggest_winners_from_file()
    finally:
        _running = False


async def _daily_at_midnight_loop():
    """Run refresh only at 12 AM (midnight) local time. No run on startup."""
    while True:
        try:
            secs = _seconds_until_midnight_local()
            logger.info("Biggest winners next refresh at 12 AM (in %.0f seconds)", secs)
            await asyncio.sleep(secs)
            await refresh_biggest_winners_job()
        except asyncio.CancelledError:
            logger.info("Biggest winners scheduler stopped.")
            break
        except Exception as e:
            logger.error("Error in biggest winners scheduler: %s", e, exc_info=True)


def start_biggest_winners_scheduler() -> None:
    """
    Start the daily scheduler: run only at 12 AM (midnight) local time.
    Does not run on startup; only loads from file so dashboard has data.
    """
    from app.services.dashboard_service import _BIGGEST_WINNERS_CACHE

    # Load from file into cache so dashboard has data (no fetch on startup)
    loaded = load_biggest_winners_from_file()
    if loaded:
        _BIGGEST_WINNERS_CACHE["ts"] = time.time()
        _BIGGEST_WINNERS_CACHE["data"] = loaded
        logger.info("Loaded %s biggest winners from cache file (next refresh at 12 AM)", len(loaded))
    else:
        logger.info("No biggest winners cache file; next refresh at 12 AM.")

    global _scheduler_task
    if _scheduler_task is not None:
        return
    loop = asyncio.get_event_loop()
    _scheduler_task = loop.create_task(_daily_at_midnight_loop())
    logger.info("Biggest winners scheduler started (runs daily at 12 AM only).")


def stop_biggest_winners_scheduler() -> None:
    """Cancel the periodic task."""
    global _scheduler_task
    if _scheduler_task is not None:
        _scheduler_task.cancel()
        _scheduler_task = None
