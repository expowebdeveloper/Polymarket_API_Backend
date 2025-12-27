"""
Service for calculating advanced scoring metrics using database-stored data.
Uses the same formulas as the live leaderboards (shrunk values, percentiles).
"""

from typing import List, Dict, Optional, Any
from sqlalchemy.ext.asyncio import AsyncSession
from app.services.leaderboard_service import (
    calculate_trader_metrics_with_time_filter,
    calculate_scores_and_rank_with_percentiles,
    get_unique_wallet_addresses
)

async def get_advanced_db_analytics(
    session: AsyncSession, 
    wallet_addresses: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Calculate advanced metrics for wallets in DB using the full scoring logic.
    If wallet_addresses is None, it processes all unique wallets in the DB.
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

    # 2. Calculate raw metrics for each wallet
    traders_metrics = []
    for wallet in wallet_addresses:
        try:
            metrics = await calculate_trader_metrics_with_time_filter(
                session, wallet, period='all'
            )
            if metrics:
                traders_metrics.append(metrics)
        except Exception as e:
            print(f"Error calculating DB metrics for {wallet}: {e}")
            continue
            
    if not traders_metrics:
        return {
            "traders": [],
            "percentiles": {},
            "medians": {}
        }

    # 3. Calculate scores, ranks, and percentiles (The "Advanced" part)
    # This uses calculate_scores_and_rank_with_percentiles from leaderboard_service
    result = calculate_scores_and_rank_with_percentiles(traders_metrics)
    return result

async def get_db_leaderboard(
    session: AsyncSession, 
    wallet_addresses: Optional[List[str]] = None,
    limit: int = 100,
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
    # Note: Some metrics are ascending (lower is better for shrunk values)
    # but the user screenshot shows "ROI Score" etc which are descending (higher is better).
    # We'll follow the same sorting logic as in leaderboard_service or router.
    
    ascending_metrics = ["w_shrunk", "roi_shrunk", "pnl_shrunk", "W_shrunk"]
    is_reverse = metric not in ascending_metrics
    
    # Map metrics if needed (e.g. final_score is in the dict)
    try:
        traders.sort(key=lambda x: x.get(metric, 0) if x.get(metric) is not None else (float('inf') if not is_reverse else float('-inf')), reverse=is_reverse)
    except Exception as e:
        print(f"Sorting error for metric {metric}: {e}")
        # Fallback to final_score
        traders.sort(key=lambda x: x.get("final_score", 0), reverse=True)

    # Apply limit
    leaderboard = traders[:limit]
    
    # Add rank
    for i, entry in enumerate(leaderboard, 1):
        entry["rank"] = i
        
    return leaderboard
