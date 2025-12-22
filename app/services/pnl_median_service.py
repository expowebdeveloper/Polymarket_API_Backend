"""Service to calculate PnL median from population for shrinkage formula."""

from typing import Optional, List, Dict
from sqlalchemy.ext.asyncio import AsyncSession
# Note: Import leaderboard_service functions inside functions to avoid circular import


def calculate_median(values: List[float]) -> float:
    """
    Calculate median using the exact traditional formula.
    
    Case A: When n is Odd
    Median = ((n + 1) / 2)^th term (1-indexed)
    In 0-indexed: index = (n - 1) // 2 = n // 2
    
    Case B: When n is Even
    Median = average of (n/2)^th and (n/2 + 1)^th terms (1-indexed)
    In 0-indexed: indices = (n//2 - 1) and (n//2)
    
    Args:
        values: List of numeric values (will be sorted)
    
    Returns:
        Median value (0.0 if empty list)
    """
    if not values:
        return 0.0
    
    # Sort the values
    sorted_values = sorted(values)
    n = len(sorted_values)
    
    if n == 0:
        return 0.0
    
    # Case A: When n is Odd
    # Median = ((n + 1) / 2)^th term (1-indexed)
    # In 0-indexed: index = (n - 1) // 2 = n // 2
    if n % 2 == 1:
        # Odd: (n+1)/2 th term (1-indexed) = (n-1)/2 index (0-indexed) = n//2
        median_index = n // 2
        return sorted_values[median_index]
    
    # Case B: When n is Even
    # Median = average of (n/2)^th and (n/2 + 1)^th terms (1-indexed)
    # In 0-indexed: indices = (n//2 - 1) and (n//2)
    else:
        # Even: average of (n/2)th and (n/2+1)th terms
        # (n/2)th term (1-indexed) = index (n//2 - 1) (0-indexed)
        # (n/2 + 1)th term (1-indexed) = index (n//2) (0-indexed)
        mid1_index = (n // 2) - 1
        mid2_index = n // 2
        median = (sorted_values[mid1_index] + sorted_values[mid2_index]) / 2.0
        return median


async def get_pnl_median_from_population(
    session: AsyncSession = None,
    file_path: str = "wallet_address.txt"
) -> float:
    """
    Calculate PnL median from population of traders using Polymarket API.
    
    This calculates the median of adjusted PnL (PnL_adj) from all traders
    with >= 5 trades, which is used in the PnL shrinkage formula:
    PnL_shrunk = (PnL_adj * N_eff + PnL_m * k_p) / (N_eff + k_p)
    
    Uses traditional median calculation: sort all values and take the middle value.
    Fetches data from Polymarket API, not from database.
    
    Args:
        session: Database session (not used, kept for compatibility)
        file_path: Path to wallet address file (default: "wallet_address.txt")
    
    Returns:
        Median adjusted PnL value (0.0 if no traders found)
    """
    try:
        # Import here to avoid circular dependency
        from app.services.live_leaderboard_service import fetch_raw_metrics_for_scoring
        
        # Fetch metrics from Polymarket API for all wallets in file
        traders_metrics = await fetch_raw_metrics_for_scoring(file_path)
        
        if not traders_metrics:
            return 0.0
        
        # Calculate median using traditional method
        return await calculate_pnl_median_from_traders(traders_metrics)
        
    except Exception as e:
        print(f"Error calculating PnL median from population (API): {e}")
        return 0.0


async def calculate_pnl_median_from_traders(
    traders_metrics: List[Dict]
) -> float:
    """
    Calculate PnL median from a list of trader metrics using traditional median method.
    
    Uses the exact median formula:
    Case A (Odd n): Median = ((n+1)/2)^th term (1-indexed)
    Case B (Even n): Median = average of (n/2)^th and (n/2+1)^th terms (1-indexed)
    
    Args:
        traders_metrics: List of trader metric dictionaries
    
    Returns:
        Median adjusted PnL value (0.0 if no valid traders)
    """
    if not traders_metrics:
        return 0.0
    
    # Filter valid traders for population stats (>= 5 trades)
    population_metrics = [t for t in traders_metrics if t.get('total_trades', 0) >= 5]
    
    # If no traders with >= 5 trades, use all traders
    if not population_metrics:
        population_metrics = traders_metrics
    
    # Calculate adjusted PnL for all traders in population
    pnl_adjs_pop = []
    
    for t in population_metrics:
        pnl_total = t.get('total_pnl', 0.0)
        S = t.get('total_stakes', 0.0)
        max_s = t.get('max_stake', 0.0)
        alpha = 4.0
        
        ratio = 0.0
        if S > 0:
            ratio = max_s / S
        
        # PnL_adj = PnL_total / (1 + alpha * ratio)
        # Avoid division by zero
        denominator = 1 + alpha * ratio
        if denominator > 0:
            pnl_adj = pnl_total / denominator
            pnl_adjs_pop.append(pnl_adj)
    
    if not pnl_adjs_pop:
        return 0.0
    
    # Calculate median using exact traditional formula
    # Sort the values first
    sorted_pnl_adjs = sorted(pnl_adjs_pop)
    n = len(sorted_pnl_adjs)
    
    # Case A: When n is Odd
    # Median = ((n + 1) / 2)^th term (1-indexed)
    # In 0-indexed: index = (n - 1) // 2 = n // 2
    if n % 2 == 1:
        # Odd: (n+1)/2 th term (1-indexed) = (n-1)/2 index (0-indexed) = n//2
        median_index = n // 2
        pnl_m = sorted_pnl_adjs[median_index]
    else:
        # Case B: When n is Even
        # Median = average of (n/2)^th and (n/2 + 1)^th terms (1-indexed)
        # In 0-indexed: indices = (n//2 - 1) and (n//2)
        # (n/2)th term (1-indexed) = index (n//2 - 1) (0-indexed)
        # (n/2 + 1)th term (1-indexed) = index (n//2) (0-indexed)
        mid1_index = (n // 2) - 1
        mid2_index = n // 2
        pnl_m = (sorted_pnl_adjs[mid1_index] + sorted_pnl_adjs[mid2_index]) / 2.0
    
    # Debug logging with detailed calculation steps
    print(f"\n=== PnL Median Calculation (Exact Formula) ===")
    print(f"Number of traders with >=5 trades: {len(population_metrics)}")
    print(f"Number of valid PnL_adj values: {n}")
    print(f"PnL_adj values (sorted): {[round(v, 2) for v in sorted_pnl_adjs]}")
    print(f"\nMedian Formula Application:")
    print(f"  n = {n} ({'Odd' if n % 2 == 1 else 'Even'})")
    
    if n % 2 == 1:
        # Case A: Odd
        # Median = ((n + 1) / 2)^th term (1-indexed)
        term_position = (n + 1) // 2
        print(f"  Case A (Odd): Median = (({n} + 1) / 2)^th term = {term_position}^th term (1-indexed)")
        print(f"  In 0-indexed: index = {n} // 2 = {median_index}")
        print(f"  Median = sorted_pnl_adjs[{median_index}] = {sorted_pnl_adjs[median_index]:.2f}")
    else:
        # Case B: Even
        # Median = average of (n/2)^th and (n/2 + 1)^th terms (1-indexed)
        term1_position = n // 2
        term2_position = (n // 2) + 1
        print(f"  Case B (Even): Median = average of ({n}/2)^th and ({n}/2 + 1)^th terms")
        print(f"  = average of {term1_position}^th and {term2_position}^th terms (1-indexed)")
        print(f"  In 0-indexed: indices = ({n}//2 - 1) = {mid1_index} and ({n}//2) = {mid2_index}")
        print(f"  Median = (sorted_pnl_adjs[{mid1_index}] + sorted_pnl_adjs[{mid2_index}]) / 2")
        print(f"  Median = ({sorted_pnl_adjs[mid1_index]:.2f} + {sorted_pnl_adjs[mid2_index]:.2f}) / 2")
        print(f"  Median = {pnl_m:.2f}")
    
    print(f"\nFinal PnL Median: ${pnl_m:.2f}")
    print(f"Min PnL_adj: ${min(sorted_pnl_adjs):.2f}, Max PnL_adj: ${max(sorted_pnl_adjs):.2f}")
    print(f"==============================================\n")
    
    return pnl_m

