
from typing import List, Dict
import asyncio
from app.services.polymarket_service import PolymarketService
from app.services.leaderboard_service import calculate_scores_and_rank

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
                # PolymarketService.calculate_portfolio_stats is synchronous (requests)
                # Run in thread pool to not block event loop
                stats = await asyncio.to_thread(PolymarketService.calculate_portfolio_stats, wallet)
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
    pnl_metrics = stats.get('pnl_metrics', {})
    perf_metrics = stats.get('performance_metrics', {})
    positions_summary = stats.get('positions_summary', {})
    
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
<<<<<<< HEAD
        "total_stakes": perf_metrics.get('total_stakes_calculated', 0.0), # Use calculated stakes
=======
        "total_stakes": perf_metrics.get('total_stakes', perf_metrics.get('total_stakes_calculated', 0.0)), # Use total_stakes or fallback to total_stakes_calculated
>>>>>>> 999959a3e342a80b83a369a0da4c339fb0c5fe66
        "winning_stakes": perf_metrics.get('winning_stakes', 0.0),
        "sum_sq_stakes": perf_metrics.get('sum_sq_stakes', 0.0),
        "max_stake": perf_metrics.get('max_stake', 0.0),
        "worst_loss": perf_metrics.get('worst_loss', 0.0),
        "portfolio_value": perf_metrics.get('portfolio_value', 0.0),
        
        "total_trades": positions_summary.get('closed_positions_count', 0),
        
        # Extras for response
        "name": None,
        "pseudonym": None,
        "profile_image": None,
        "total_trades_with_pnl": positions_summary.get('closed_positions_count', 0), # Simplified
        "winning_trades": perf_metrics.get('wins', 0)
    }

