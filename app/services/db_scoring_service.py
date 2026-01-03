"""
Service for calculating advanced scoring metrics using database-stored data.
Uses the same formulas as the live leaderboards (shrunk values, percentiles).
"""

from typing import List, Dict, Optional, Any
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from app.services.leaderboard_service import (
    calculate_trader_metrics_with_time_filter,
    calculate_scores_and_rank_with_percentiles,
    get_unique_wallet_addresses
)
from app.services.pnl_median_service import calculate_pnl_median_from_traders

async def get_advanced_db_analytics(
    session: AsyncSession, 
    wallet_addresses: Optional[List[str]] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    max_traders: Optional[int] = None
) -> Dict[str, Any]:
    """
    Calculate advanced metrics for wallets in DB using the full scoring logic.
    If wallet_addresses is None, it processes all unique wallets in the DB.
    Uses async batching for concurrent processing to improve performance.
    
    Args:
        max_traders: Optional limit on number of traders to process for faster response.
                    If None, processes all traders.
    """
    # 1. Get wallet addresses
    if not wallet_addresses:
        wallet_addresses = await get_unique_wallet_addresses(session)
    
    if not wallet_addresses:
        return {
            "traders": [],
            "percentiles": {},
            "medians": {}
        }
    
    # Limit traders for processing if specified (for faster response)
    # If max_traders is None, process all traders (no limit)
    traders_to_process = wallet_addresses
    if max_traders is not None and len(wallet_addresses) > max_traders:
        traders_to_process = wallet_addresses[:max_traders]

    # 2. Calculate raw metrics for each wallet using async batching for better performance
    traders_metrics = []
    failed_wallets = []
    semaphore = asyncio.Semaphore(20)  # Process up to 20 traders concurrently for better performance
    batch_size = 50  # Process in batches to avoid memory issues
    
    async def process_wallet(wallet: str):
        async with semaphore:
            try:
                metrics = await calculate_trader_metrics_with_time_filter(
                    session, wallet, period='all'
                )
                if metrics is None:
                    failed_wallets.append(wallet)
                return metrics
            except Exception as e:
                failed_wallets.append(wallet)
                # Log first few errors to understand what's failing
                import logging
                logger = logging.getLogger(__name__)
                # Only log first 5 errors per batch to avoid spam
                if not hasattr(process_wallet, '_error_count'):
                    process_wallet._error_count = 0
                if process_wallet._error_count < 5:
                    logger.warning(f"Error calculating metrics for wallet {wallet}: {str(e)}")
                    process_wallet._error_count += 1
                return None
    
    # Process wallets in batches to avoid overwhelming the system
    for i in range(0, len(traders_to_process), batch_size):
        batch = traders_to_process[i:i + batch_size]
        tasks = [process_wallet(wallet) for wallet in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out None results and exceptions
        for result in results:
            if result and isinstance(result, dict):
                traders_metrics.append(result)
        
        # Small delay between batches to avoid overwhelming the database
        if i + batch_size < len(traders_to_process):
            await asyncio.sleep(0.1)  # 100ms delay between batches
    
    # Log summary of failures
    if failed_wallets:
        import logging
        logger = logging.getLogger(__name__)
        logger.warning(f"Failed to calculate metrics for {len(failed_wallets)} out of {len(traders_to_process)} wallets")
            
    if not traders_metrics:
        return {
            "traders": [],
            "percentiles": {},
            "medians": {}
        }

    # 3. Calculate PnL median from DB population (same as view-all does from API)
    # This is critical for correct PnL shrinkage calculations
    pnl_median_db = await calculate_pnl_median_from_traders(traders_metrics)

    # 4. Calculate scores, ranks, and percentiles (The "Advanced" part)
    # This uses calculate_scores_and_rank_with_percentiles from leaderboard_service
    # Pass the PnL median from DB population (same as view-all passes API median)
    result = calculate_scores_and_rank_with_percentiles(
        traders_metrics,
        pnl_median=pnl_median_db
    )
    return result

async def get_db_leaderboard(
    session: AsyncSession, 
    wallet_addresses: Optional[List[str]] = None,
    limit: int = 100,
    offset: int = 0,
    metric: str = "final_score"
) -> List[Dict[str, Any]]:
    """
    Get a specific leaderboard (default final_score) from DB using advanced logic.
    Returns a ranked list compatible with LeaderboardResponse.
    """
    analytics = await get_advanced_db_analytics(session, wallet_addresses)
    traders = analytics.get("traders", [])
    
    if not traders:
        return []

    # Sort based on metric
    # User requested descending order (highest is better)
    # Most metrics like final_score, score_win_rate, win_rate, roi, pnl are better when higher.
    # Shrunk values (W_shrunk, roi_shrunk, pnl_shrunk) are also better when higher in our scoring logic.
    
    # We'll default to descending unless specifically known otherwise
    is_reverse = True # Higher is better across the board now
    
    try:
        traders.sort(key=lambda x: x.get(metric, 0) if x.get(metric) is not None else (float('-inf')), reverse=is_reverse)
    except Exception as e:
        pass  # Silently skip sorting errors
        # Fallback to final_score
        traders.sort(key=lambda x: x.get("final_score", 0), reverse=True)

    # Apply limit and offset
    leaderboard = traders[offset : offset + limit]
    
    # Add rank
    for i, entry in enumerate(leaderboard, offset + 1):
        entry["rank"] = i
        
    return leaderboard
