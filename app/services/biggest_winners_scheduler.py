"""
Background scheduler: refresh biggest winners of the month (API + scoring) every 12 hours.
Persists result to JSON so the dashboard can load it on startup and stay accurate/live.
"""

import asyncio
import json
import logging
import os
import time
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

# Default: persist under backend/data (create if missing)
def _cache_dir() -> str:
    base = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    d = os.path.join(base, "data")
    os.makedirs(d, exist_ok=True)
    return d

BIGGEST_WINNERS_FILENAME = "biggest_winners_with_scoring.json"
CACHE_FILE_MAX_AGE_SECONDS = 12 * 3600 + 600  # 12h + 10min grace
INTERVAL_SECONDS = 12 * 3600  # 12 hours
_scheduler_task: asyncio.Task | None = None
_running = False


def _cache_path() -> str:
    return os.path.join(_cache_dir(), BIGGEST_WINNERS_FILENAME)


def load_biggest_winners_from_file() -> List[Dict[str, Any]]:
    """Load persisted biggest winners from file. Return [] if missing or invalid."""
    path = _cache_path()
    if not os.path.isfile(path):
        return []
    try:
        with open(path, "r") as f:
            data = json.load(f)
        if isinstance(data, list) and data:
            return data
        return []
    except Exception as e:
        logger.warning("Could not load biggest winners cache file %s: %s", path, e)
        return []


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
    """True if cache file exists and is newer than 12 hours."""
    path = _cache_path()
    if not os.path.isfile(path):
        return False
    try:
        age = time.time() - os.path.getmtime(path)
        return age < CACHE_FILE_MAX_AGE_SECONDS
    except Exception:
        return False


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


async def _periodic_loop():
    """Run refresh every 12 hours."""
    while True:
        try:
            await asyncio.sleep(INTERVAL_SECONDS)
            await refresh_biggest_winners_job()
        except asyncio.CancelledError:
            logger.info("Biggest winners scheduler stopped.")
            break
        except Exception as e:
            logger.error("Error in biggest winners scheduler: %s", e, exc_info=True)


def start_biggest_winners_scheduler() -> None:
    """
    Start the 12-hour periodic refresh.
    Does not run immediately; first run is after 12 hours.
    Load from file into cache on startup so dashboard has data immediately.
    """
    from app.services.dashboard_service import _BIGGEST_WINNERS_CACHE

    # Load from file into cache so dashboard has data without waiting 12h
    loaded = load_biggest_winners_from_file()
    if loaded:
        _BIGGEST_WINNERS_CACHE["ts"] = time.time()
        _BIGGEST_WINNERS_CACHE["data"] = loaded
        logger.info("Loaded %s biggest winners from cache file (12h refresh scheduled)", len(loaded))
    else:
        logger.info("No biggest winners cache file; first refresh in 12 hours.")

    global _scheduler_task
    if _scheduler_task is not None:
        return
    loop = asyncio.get_event_loop()
    _scheduler_task = loop.create_task(_periodic_loop())
    logger.info("Biggest winners scheduler started (every 12 hours).")


def stop_biggest_winners_scheduler() -> None:
    """Cancel the periodic task."""
    global _scheduler_task
    if _scheduler_task is not None:
        _scheduler_task.cancel()
        _scheduler_task = None
