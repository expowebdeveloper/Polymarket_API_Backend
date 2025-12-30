
from typing import List, Dict
import asyncio
from app.services.polymarket_service import PolymarketService
from app.services.leaderboard_service import calculate_scores_and_rank
from app.services.data_fetcher import async_client

async def fetch_live_leaderboard_from_file(file_path: str) -> List[Dict]:
    """
    Fetch live leaderboard data for wallets listed in a file.
    """
    try:
        with open(file_path, 'r') as f:
            wallets = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        return []
        
    return await fetch_live_leaderboard(wallets)

async def fetch_live_leaderboard(wallets: List[str]) -> List[Dict]:
    """
    Fetch live metrics for a list of wallets and calculate scores.
    Uses concurrency limit to avoid rate limiting.
    """
    semaphore = asyncio.Semaphore(5) # Limit concurrency
    
    async def fetch_wallet_safe(wallet: str):
        async with semaphore:
            try:
                # PolymarketService.calculate_portfolio_stats is now async
                stats = await PolymarketService.calculate_portfolio_stats(wallet)
                if stats is None:
                    return None
                return transform_stats_for_scoring(stats)
            except Exception as e:
                print(f"Error fetching stats for {wallet}: {e}")
                return None

    tasks = [fetch_wallet_safe(w) for w in wallets]
    results = await asyncio.gather(*tasks)
    
    # Filter None results
    valid_metrics = [r for r in results if r is not None]
    
    # Calculate scores
    ranked_leaderboard = calculate_scores_and_rank(valid_metrics)
    
    # Sort by PnL Score (default) or PnL
    ranked_leaderboard.sort(key=lambda x: x.get('score_pnl', 0), reverse=True)
    
    # Add rank
    for i, entry in enumerate(ranked_leaderboard, 1):
        entry['rank'] = i
        
    return ranked_leaderboard

def transform_stats_for_scoring(stats: Dict) -> Dict:
    """
    Transform nested PolymarketService output to flat structure expected by scoring.
    """
    # Handle None stats
    if stats is None:
        return None
    
    pnl_metrics = stats.get('pnl_metrics', {}) or {}
    perf_metrics = stats.get('performance_metrics', {}) or {}
    positions_summary = stats.get('positions_summary', {}) or {}
    
    # Get all_losses if available (for average of N worst losses feature)
    all_losses = perf_metrics.get('all_losses', []) or []
    
    # Flatten
    return {
        "wallet_address": stats.get('user_address'),
        "total_pnl": pnl_metrics.get('total_pnl', 0.0), # Or total_calculated_pnl? 
        # Request said: "total_pnl = leaderboard_stats.get('pnl')" in Formula 3 logic?
        # "PnL_total = total profit across trades". 
        # PolymarketService returns 'total_pnl' (from leaderboard) and 'total_calculated_pnl'.
        # Leaderboard usually matches 'total_pnl'.
        "roi": perf_metrics.get('roi', 0.0),
        "win_rate": perf_metrics.get('win_rate', 0.0),
        
        # Advanced Metrics for Scoring
        "total_stakes": perf_metrics.get('total_stakes', perf_metrics.get('total_stakes_calculated', 0.0)), # Use total_stakes or fallback to total_stakes_calculated
        "winning_stakes": perf_metrics.get('winning_stakes', 0.0),
        "sum_sq_stakes": perf_metrics.get('sum_sq_stakes', 0.0),
        "max_stake": perf_metrics.get('max_stake', 0.0),
        "worst_loss": perf_metrics.get('worst_loss', 0.0),
        "all_losses": all_losses,  # All losses for average calculation (future enhancement)
        "portfolio_value": perf_metrics.get('portfolio_value', 0.0),
        
        "total_trades": positions_summary.get('closed_positions_count', 0),
        
        # Extras for response
        "name": None,
        "pseudonym": None,
        "profile_image": None,
        "total_trades_with_pnl": positions_summary.get('closed_positions_count', 0), # Simplified
        "winning_trades": perf_metrics.get('wins', 0)
    }


async def fetch_raw_metrics_for_scoring(file_path: str) -> List[Dict]:
    """
    Fetch raw metrics for wallets without calculating scores.
    This is used by endpoints that need to calculate scores with percentiles.
    """
    try:
        with open(file_path, 'r') as f:
            wallets = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        return []
    
    semaphore = asyncio.Semaphore(5)  # Limit concurrency
    
    async def fetch_wallet_safe(wallet: str):
        async with semaphore:
            try:
                # PolymarketService.calculate_portfolio_stats is now async
                stats = await PolymarketService.calculate_portfolio_stats(wallet)
                if stats is None:
                    return None
                return transform_stats_for_scoring(stats)
            except Exception as e:
                print(f"Error fetching stats for {wallet}: {e}")
                return None

    tasks = [fetch_wallet_safe(w) for w in wallets]
    results = await asyncio.gather(*tasks)
    
    # Filter None results
    valid_metrics = [r for r in results if r is not None]
    
    return valid_metrics


async def fetch_polymarket_leaderboard_api(
    time_period: str = "day",
    order_by: str = "PNL",
    limit: int = 20,
    offset: int = 0,
    category: str = "overall"
) -> List[Dict]:
    """
    Fetch leaderboard data directly from Polymarket API.
    
    Args:
        time_period: Time period (day, week, month, all)
        order_by: Order by metric (PNL, VOL)
        limit: Maximum number of entries
        offset: Offset for pagination
        category: Category filter (overall)
    
    Returns:
        List of leaderboard entries
    """
    try:
        url = "https://data-api.polymarket.com/v1/leaderboard"
        params = {
            "timePeriod": time_period,
            "orderBy": order_by,
            "limit": limit,
            "offset": offset,
            "category": category
        }
        
        response = await async_client.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        return data if isinstance(data, list) else []
    except Exception as e:
        print(f"Error fetching Polymarket leaderboard API: {e}")
        return []


async def fetch_polymarket_biggest_winners(
    time_period: str = "day",
    limit: int = 20,
    offset: int = 0,
    category: str = "overall"
) -> List[Dict]:
    """
    Fetch biggest winners data directly from Polymarket API.
    
    Args:
        time_period: Time period (day, week, month, all)
        limit: Maximum number of entries
        offset: Offset for pagination
        category: Category filter (overall)
    
    Returns:
        List of biggest winners entries
    """
    try:
        url = "https://data-api.polymarket.com/v1/biggest-winners"
        params = {
            "timePeriod": time_period,
            "limit": limit,
            "offset": offset,
            "category": category
        }
        
        response = await async_client.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        return data if isinstance(data, list) else []
    except Exception as e:
        print(f"Error fetching Polymarket biggest winners API: {e}")
        return []


def transform_polymarket_api_entry(entry: Dict, entry_type: str = "leaderboard") -> Dict:
    """
    Transform Polymarket API entry to LeaderboardEntry format.
    
    Args:
        entry: Raw entry from Polymarket API
        entry_type: Type of entry (leaderboard or biggest_winners)
    
    Returns:
        Transformed entry matching LeaderboardEntry schema
    """
    if entry_type == "biggest_winners":
        # Biggest winners format
        return {
            "rank": int(entry.get("winRank", 0)),
            "wallet_address": entry.get("proxyWallet", ""),
            "name": entry.get("userName", ""),
            "pseudonym": entry.get("xUsername", ""),
            "profile_image": entry.get("profileImage", ""),
            "total_pnl": float(entry.get("pnl", 0.0)),
            "roi": 0.0,  # Not available in biggest winners
            "win_rate": 0.0,  # Not available in biggest winners
            "total_trades": 0,  # Not available in biggest winners
            "total_trades_with_pnl": 0,
            "winning_trades": 0,
            "total_stakes": 0.0,
            "score_win_rate": 0.0,
            "score_roi": 0.0,
            "score_pnl": 0.0,
            "score_risk": 0.0,
            "final_score": 0.0,
            "W_shrunk": None,
            "roi_shrunk": None,
            "pnl_shrunk": None,
        }
    else:
        # Regular leaderboard format
        return {
            "rank": int(entry.get("rank", 0)),
            "wallet_address": entry.get("proxyWallet", ""),
            "name": entry.get("userName", ""),
            "pseudonym": entry.get("xUsername", ""),
            "profile_image": entry.get("profileImage", ""),
            "total_pnl": float(entry.get("pnl", 0.0)),
            "roi": 0.0,  # Not available in API response
            "win_rate": 0.0,  # Not available in API response
            "total_trades": 0,  # Not available in API response
            "total_trades_with_pnl": 0,
            "winning_trades": 0,
            "total_stakes": float(entry.get("vol", 0.0)),  # Using vol as total_stakes
            "score_win_rate": 0.0,
            "score_roi": 0.0,
            "score_pnl": 0.0,
            "score_risk": 0.0,
            "final_score": 0.0,
            "W_shrunk": None,
            "roi_shrunk": None,
            "pnl_shrunk": None,
        }

