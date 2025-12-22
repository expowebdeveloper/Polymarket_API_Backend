"""Leaderboard service for ranking traders by various metrics."""

from typing import List, Dict, Optional
from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, distinct
from decimal import Decimal
import math
from app.db.models import Trade, Position, Activity
from app.core.scoring_config import ScoringConfig, default_scoring_config


def get_time_filter(timestamp: int, period: str) -> bool:
    """
    Check if a timestamp falls within the specified time period.
    
    Args:
        timestamp: Unix timestamp
        period: Time period ('7d', '30d', 'all')
    
    Returns:
        True if timestamp is within period, False otherwise
    """
    if period == 'all':
        return True
    
    now = datetime.utcnow()
    if period == '7d':
        cutoff = now - timedelta(days=7)
    elif period == '30d':
        cutoff = now - timedelta(days=30)
    else:
        return True
    
    cutoff_timestamp = int(cutoff.timestamp())
    return timestamp >= cutoff_timestamp


async def get_unique_wallet_addresses(session: AsyncSession) -> List[str]:
    """
    Get all unique wallet addresses from trades, positions, and activities.
    
    Args:
        session: Database session
    
    Returns:
        List of unique wallet addresses
    """
    wallets = set()
    
    # Get from trades
    stmt = select(distinct(Trade.proxy_wallet))
    result = await session.execute(stmt)
    trade_wallets = [row[0] for row in result.all()]
    wallets.update(trade_wallets)
    
    # Get from positions
    stmt = select(distinct(Position.proxy_wallet))
    result = await session.execute(stmt)
    position_wallets = [row[0] for row in result.all()]
    wallets.update(position_wallets)
    
    # Get from activities
    stmt = select(distinct(Activity.proxy_wallet))
    result = await session.execute(stmt)
    activity_wallets = [row[0] for row in result.all()]
    wallets.update(activity_wallets)
    
    return list(wallets)


async def calculate_trader_metrics_with_time_filter(
    session: AsyncSession,
    wallet_address: str,
    period: str = 'all'
) -> Optional[Dict]:
    """
    Calculate PnL metrics for a trader with time filtering.
    
    Args:
        session: Database session
        wallet_address: Wallet address
        period: Time period ('7d', '30d', 'all')
    
    Returns:
        Dictionary with trader metrics or None if no data
    """
    # Get all data
    from app.services.trade_service import get_trades_from_db
    from app.services.position_service import get_positions_from_db
    from app.services.activity_service import get_activities_from_db
    
    trades = await get_trades_from_db(session, wallet_address)
    positions = await get_positions_from_db(session, wallet_address)
    activities = await get_activities_from_db(session, wallet_address)
    
    # Filter by time period
    if period != 'all':
        cutoff_timestamp = int((datetime.utcnow() - timedelta(
            days=7 if period == '7d' else 30
        )).timestamp())
        
        # Filter trades
        trades = [t for t in trades if t.timestamp >= cutoff_timestamp]
        
        # For positions, we need to check if they have recent activity
        # We'll filter positions based on their updated_at timestamp
        # If a position was updated recently, include it
        filtered_positions = []
        for position in positions:
            # Convert updated_at to timestamp if it's a datetime
            if hasattr(position, 'updated_at') and position.updated_at:
                if isinstance(position.updated_at, datetime):
                    pos_timestamp = int(position.updated_at.timestamp())
                else:
                    pos_timestamp = position.updated_at
                
                if pos_timestamp >= cutoff_timestamp:
                    filtered_positions.append(position)
            else:
                # If no updated_at, include it (better to include than exclude)
                filtered_positions.append(position)
        positions = filtered_positions
        
        # Filter activities
        activities = [a for a in activities if a.timestamp >= cutoff_timestamp]
    
    # If no data after filtering, return None
    if not trades and not positions and not activities:
        return None
    
    # Calculate metrics (similar to calculate_user_pnl but with filtered data)
    total_invested = Decimal('0')
    total_realized_pnl = Decimal('0')
    total_unrealized_pnl = Decimal('0')
    total_rewards = Decimal('0')
    total_redemptions = Decimal('0')
    total_current_value = Decimal('0')
    
    # Calculate from positions (only include positions with recent activity)
    for position in positions:
        # For time filtering, we'll include all positions but this is a simplification
        # In production, you might want to track when positions were created/updated
        total_invested += position.initial_value
        total_realized_pnl += position.realized_pnl
        unrealized = position.cash_pnl - position.realized_pnl
        total_unrealized_pnl += unrealized
        total_current_value += position.current_value
    
    # Calculate from activities
    for activity in activities:
        if activity.type == "REWARD":
            total_rewards += activity.usdc_size
        elif activity.type == "REDEEM":
            total_redemptions += activity.usdc_size
    
    # Calculate total PnL
    total_pnl = total_realized_pnl + total_unrealized_pnl + total_rewards - total_redemptions
    
    # Calculate trade metrics
    total_trade_pnl = Decimal('0')
    total_stakes = Decimal('0')
    winning_trades_count = 0
    total_trades_with_pnl = 0
    stakes_of_wins = Decimal('0')
    
    for trade in trades:
        stake = trade.size * trade.price
        total_stakes += stake
        
        if trade.pnl is not None:
            total_trade_pnl += trade.pnl
            total_trades_with_pnl += 1
            
            if trade.pnl > 0:
                winning_trades_count += 1
                stakes_of_wins += stake
    
    # Advanced Metrics for Scoring
    worst_loss = Decimal('0')
    stakes_list = []  # Collect all stakes for top 5 calculation
    sum_sq_stakes = Decimal('0')
    
    for trade in trades:
        stake = trade.size * trade.price
        stakes_list.append(stake)
        sum_sq_stakes += stake ** 2
        
        if trade.pnl is not None:
             if trade.pnl < worst_loss:
                 worst_loss = trade.pnl
    
    # Calculate max_stake: average of top 5 highest stakes (or all if < 5)
    max_stake = Decimal('0')
    if stakes_list:
        sorted_stakes = sorted(stakes_list, reverse=True)
        top_n = min(5, len(sorted_stakes))
        top_stakes = sorted_stakes[:top_n]
        max_stake = sum(top_stakes) / Decimal(str(top_n)) if top_n > 0 else Decimal('0')
    
    # Calculate ROI
    roi = Decimal('0')
    if total_stakes > 0:
        roi = (total_trade_pnl / total_stakes) * 100
    
    # Calculate Win Rate
    win_rate = Decimal('0')
    if total_trades_with_pnl > 0:
        win_rate = (winning_trades_count / total_trades_with_pnl) * 100
    
    # Get trader info (name, pseudonym, etc.)
    trader_name = None
    trader_pseudonym = None
    trader_profile_image = None
    
    if trades:
        trader_name = trades[0].name
        trader_pseudonym = trades[0].pseudonym
        trader_profile_image = trades[0].profile_image_optimized or trades[0].profile_image
    
    return {
        "wallet_address": wallet_address,
        "name": trader_name,
        "pseudonym": trader_pseudonym,
        "profile_image": trader_profile_image,
        "total_pnl": float(total_pnl),
        "roi": float(roi),
        "win_rate": float(win_rate),
        "total_trades": len(trades),
        "total_trades_with_pnl": total_trades_with_pnl,
        "winning_trades": winning_trades_count,
        "total_stakes": float(total_stakes),
        "winning_stakes": float(stakes_of_wins),
        "worst_loss": float(worst_loss),
        "max_stake": float(max_stake),
        "sum_sq_stakes": float(sum_sq_stakes),
        "portfolio_value": float(total_current_value) if total_current_value > 0 else 0.0 # Use current value as capital proxy
    }


async def get_leaderboard_by_pnl(
    session: AsyncSession,
    period: str = 'all',
    limit: int = 100
) -> List[Dict]:
    """
    Get leaderboard sorted by Total PnL.
    
    Args:
        session: Database session
        period: Time period ('7d', '30d', 'all')
        limit: Maximum number of traders to return
    
    Returns:
        List of trader metrics sorted by total PnL (descending)
    """
    wallets = await get_unique_wallet_addresses(session)
    
    # Calculate metrics for each wallet
    leaderboard = []
    for wallet in wallets:
        try:
            metrics = await calculate_trader_metrics_with_time_filter(
                session, wallet, period
            )
            if metrics and metrics['total_trades'] > 0:  # Only include traders with trades
                leaderboard.append(metrics)
        except Exception as e:
            # Skip traders with errors
            print(f"Error calculating metrics for {wallet}: {e}")
            continue
    
    # Calculate scores
    leaderboard = calculate_scores_and_rank(leaderboard)
    
    # Sort by total PnL (descending)
    leaderboard.sort(key=lambda x: x['total_pnl'], reverse=True)
    
    # Add rank
    for rank, trader in enumerate(leaderboard, 1):
        trader['rank'] = rank
    
    return leaderboard[:limit]


async def get_leaderboard_by_roi(
    session: AsyncSession,
    period: str = 'all',
    limit: int = 100
) -> List[Dict]:
    """
    Get leaderboard sorted by ROI.
    
    Args:
        session: Database session
        period: Time period ('7d', '30d', 'all')
        limit: Maximum number of traders to return
    
    Returns:
        List of trader metrics sorted by ROI (descending)
    """
    wallets = await get_unique_wallet_addresses(session)
    
    # Calculate metrics for each wallet
    leaderboard = []
    for wallet in wallets:
        try:
            metrics = await calculate_trader_metrics_with_time_filter(
                session, wallet, period
            )
            # Only include traders with trades and stakes > 0
            if metrics and metrics['total_stakes'] > 0:
                leaderboard.append(metrics)
        except Exception as e:
            # Skip traders with errors
            print(f"Error calculating metrics for {wallet}: {e}")
            continue
    
    # Calculate scores
    leaderboard = calculate_scores_and_rank(leaderboard)
    
    # Sort by ROI (descending)
    leaderboard.sort(key=lambda x: x['roi'], reverse=True)
    
    # Add rank
    for rank, trader in enumerate(leaderboard, 1):
        trader['rank'] = rank
    
    return leaderboard[:limit]


async def get_leaderboard_by_win_rate(
    session: AsyncSession,
    period: str = 'all',
    limit: int = 100
) -> List[Dict]:
    """
    Get leaderboard sorted by Win Rate.
    
    Args:
        session: Database session
        period: Time period ('7d', '30d', 'all')
        limit: Maximum number of traders to return
    
    Returns:
        List of trader metrics sorted by win rate (descending)
    """
    wallets = await get_unique_wallet_addresses(session)
    
    # Calculate metrics for each wallet
    leaderboard = []
    for wallet in wallets:
        try:
            metrics = await calculate_trader_metrics_with_time_filter(
                session, wallet, period
            )
            # Only include traders with trades that have PnL calculated
            if metrics and metrics['total_trades_with_pnl'] > 0:
                leaderboard.append(metrics)
        except Exception as e:
            # Skip traders with errors
            print(f"Error calculating metrics for {wallet}: {e}")
            continue
    
    # Calculate scores
    leaderboard = calculate_scores_and_rank(leaderboard)
    
    # Sort by win rate (descending)
    leaderboard.sort(key=lambda x: x['win_rate'], reverse=True)
    
    # Add rank
    for rank, trader in enumerate(leaderboard, 1):
        trader['rank'] = rank
    
    return leaderboard[:limit]
    
    
def get_percentile_value(values: List[float], percentile: float) -> float:
    """
    Get the value at a specific percentile (0-100).
    """
    if not values:
        return 0.0
    sorted_values = sorted(values)
    k = (len(sorted_values) - 1) * (percentile / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_values[int(k)]
    d0 = sorted_values[int(f)] * (c - k)
    d1 = sorted_values[int(c)] * (k - f)
    return d0 + d1


def clamp(n: float, minn: float, maxn: float) -> float:
    return max(min(n, maxn), minn)


def calculate_risk_score(
    worst_loss: float,
    total_stake: float,
    config: ScoringConfig,
    all_losses: Optional[List[float]] = None
) -> float:
    """
    Calculate Risk Score = |Worst Loss| / Total Stake (output range: 0 → 1).
    
    If config.risk_n_worst_losses > 1 and all_losses is provided, uses average of N worst losses.
    Otherwise uses single worst loss.
    
    Args:
        worst_loss: Single worst loss value
        total_stake: Total stake/capital
        config: Scoring configuration
        all_losses: Optional list of all losses for calculating average of N worst
    
    Returns:
        Risk score in range [0, 1]
    """
    if total_stake <= 0:
        return 0.0
    
    # Calculate worst loss value (single or average of N)
    if config.risk_n_worst_losses > 1 and all_losses:
        # Average of N worst losses
        losses = sorted([abs(loss) for loss in all_losses if loss < 0], reverse=True)
        n = min(config.risk_n_worst_losses, len(losses))
        if n > 0:
            avg_worst_loss = sum(losses[:n]) / n
        else:
            avg_worst_loss = abs(worst_loss)
    else:
        # Single worst loss (current behavior)
        avg_worst_loss = abs(worst_loss)
    
    # Risk Score = |Worst Loss| / Total Stake
    risk_score = avg_worst_loss / total_stake
    
    # Ensure output range: 0 → 1 (clamp if exceeds 1)
    return clamp(risk_score, 0.0, 1.0)


def calculate_scores_and_rank(
    traders_metrics: List[Dict],
    config: Optional[ScoringConfig] = None
) -> List[Dict]:
    """
    Calculate advanced scores for a list of traders.
    
    Args:
        traders_metrics: List of trader metrics dictionaries
        config: Scoring configuration (uses default if not provided)
    
    Returns:
        List of traders with calculated scores
    """
    if not traders_metrics:
        return []
    
    # Use default config if not provided
    if config is None:
        config = default_scoring_config
    config.validate()
        
    # Filter valid traders for population stats (meets minimum activity threshold)
    population_metrics = [
        t for t in traders_metrics 
        if t.get('total_trades', 0) >= config.min_trades_threshold
    ]
    
    # Fallback if no active traders
    if not population_metrics:
        population_metrics = traders_metrics 

    # --- PnL Population Median (for Formula 3) ---
    pnl_adjs_pop = []
    
    for t in population_metrics:
        pnl_total = t.get('total_pnl', 0.0)
        S = t.get('total_stakes', 0.0)
        max_s = t.get('max_stake', 0.0)
        
        ratio = 0.0
        if S > 0:
            ratio = max_s / S
            
        pnl_adj = pnl_total / (1 + config.shrink_alpha * ratio)
        pnl_adjs_pop.append(pnl_adj)
        
    pnl_m = sorted(pnl_adjs_pop)[len(pnl_adjs_pop) // 2] if pnl_adjs_pop else 0.0

    # --- ROI Population Median (for Formula 2) ---
    rois_pop = [t.get('roi', 0.0) for t in population_metrics]
    roi_m = sorted(rois_pop)[len(rois_pop) // 2] if rois_pop else 0.0

    # Calculate Shrunk Values for ALL traders
    for t in traders_metrics:
        S = t.get('total_stakes', 0.0)
        sum_sq_s = t.get('sum_sq_stakes', 0.0)
        N_eff = (S**2) / sum_sq_s if sum_sq_s > 0 else 0.0
        
        # --- Formula 1: Win Rate ---
        s_w = t.get('winning_stakes', 0.0)
        W = (s_w / S) if S > 0 else 0.0
        W_shrunk = (W * N_eff + config.shrink_baseline_win_rate * config.shrink_kw) / (N_eff + config.shrink_kw)
        t['W_shrunk'] = W_shrunk
        
        # --- Formula 2: ROI ---
        roi_raw = t.get('roi', 0.0)
        roi_shrunk = (roi_raw * N_eff + roi_m * config.shrink_kr) / (N_eff + config.shrink_kr)
        t['roi_shrunk'] = roi_shrunk
        
        # --- Formula 3: PnL ---
        pnl_total = t.get('total_pnl', 0.0)
        max_s = t.get('max_stake', 0.0)
        ratio = (max_s / S) if S > 0 else 0.0
        pnl_adj = pnl_total / (1 + config.shrink_alpha * ratio)
        
        pnl_shrunk = (pnl_adj * N_eff + pnl_m * config.shrink_kp) / (N_eff + config.shrink_kp)
        t['pnl_shrunk'] = pnl_shrunk
        
        # --- Formula 4: Risk ---
        # Risk Score = |Worst Loss| / Total Stake (output range: 0 → 1)
        worst_loss = t.get('worst_loss', 0.0)
        # For future: if we have all_losses list, pass it for average of N worst losses
        all_losses = t.get('all_losses', None)  # Optional: list of all losses
        t['score_risk'] = calculate_risk_score(worst_loss, S, config, all_losses)
    
    # Collect Shrunk values from POPULATION for Percentiles
    w_shrunk_pop = [t['W_shrunk'] for t in population_metrics]
    roi_shrunk_pop = [t['roi_shrunk'] for t in population_metrics]
    pnl_shrunk_pop = [t['pnl_shrunk'] for t in population_metrics]
    
    # Anchors (using configurable percentiles)
    w_1 = get_percentile_value(w_shrunk_pop, config.percentile_lower)
    w_99 = get_percentile_value(w_shrunk_pop, config.percentile_upper)
    
    r_1 = get_percentile_value(roi_shrunk_pop, config.percentile_lower)
    r_99 = get_percentile_value(roi_shrunk_pop, config.percentile_upper)
    
    p_1 = get_percentile_value(pnl_shrunk_pop, config.percentile_lower)
    p_99 = get_percentile_value(pnl_shrunk_pop, config.percentile_upper)
    
    # Final Normalization
    for t in traders_metrics:
        # W score
        if w_99 - w_1 != 0:
            w_score = (t['W_shrunk'] - w_1) / (w_99 - w_1)
        else:
            w_score = 0.5 
        t['score_win_rate'] = clamp(w_score, 0, 1)
        
        # R score
        if r_99 - r_1 != 0:
            r_score = (t['roi_shrunk'] - r_1) / (r_99 - r_1)
        else:
            r_score = 0.5
        t['score_roi'] = clamp(r_score, 0, 1)
        
        # P score
        if p_99 - p_1 != 0:
            p_score = (t['pnl_shrunk'] - p_1) / (p_99 - p_1)
        else:
            p_score = 0.5
        t['score_pnl'] = clamp(p_score, 0, 1)
        
        # Final Rating Formula (0-100 scale)
        # Rating = 100 × [ wW · Wscore + wR · Rscore + wP · Pscore + wrisk · (1 − Risk Score) ]
        w_score = t.get('score_win_rate', 0.0)
        r_score = t.get('score_roi', 0.0)
        p_score = t.get('score_pnl', 0.0)
        risk_score = t.get('score_risk', 0.0)
        
        final_score = 100.0 * (
            config.weight_win_rate * w_score + 
            config.weight_roi * r_score + 
            config.weight_pnl * p_score + 
            config.weight_risk * (1.0 - risk_score)
        )
        t['final_score'] = clamp(final_score, 0, 100)
        
    return traders_metrics


def calculate_scores_and_rank_with_percentiles(
    traders_metrics: List[Dict],
    config: Optional[ScoringConfig] = None
) -> Dict:
    """
    Calculate advanced scores for a list of traders and return with percentile information.
    
    Args:
        traders_metrics: List of trader metrics dictionaries
        config: Scoring configuration (uses default if not provided)
    
    Returns:
        Dict containing:
        - traders: List of traders with all scores
        - percentiles: Dict with percentile values (configurable lower/upper)
        - medians: Dict with median values used in calculations
        - population_size: Number of traders meeting minimum activity threshold
    """
    if not traders_metrics:
        return {
            "traders": [],
            "percentiles": {
                "w_shrunk_lower_percent": 0.0,
                "w_shrunk_upper_percent": 0.0,
                "roi_shrunk_lower_percent": 0.0,
                "roi_shrunk_upper_percent": 0.0,
                "pnl_shrunk_lower_percent": 0.0,
                "pnl_shrunk_upper_percent": 0.0,
            },
            "medians": {
                "roi_median": 0.0,
                "pnl_median": 0.0,
            },
            "population_size": 0,
            "total_traders": 0
        }
    
    # Use default config if not provided
    if config is None:
        config = default_scoring_config
    config.validate()
        
    # Filter valid traders for population stats (meets minimum activity threshold)
    population_metrics = [
        t for t in traders_metrics 
        if t.get('total_trades', 0) >= config.min_trades_threshold
    ]
    
    # Fallback if no active traders
    if not population_metrics:
        population_metrics = traders_metrics 

    # --- PnL Population Median (for Formula 3) ---
    pnl_adjs_pop = []
    
    for t in population_metrics:
        pnl_total = t.get('total_pnl', 0.0)
        S = t.get('total_stakes', 0.0)
        max_s = t.get('max_stake', 0.0)
        
        ratio = 0.0
        if S > 0:
            ratio = max_s / S
            
        pnl_adj = pnl_total / (1 + config.shrink_alpha * ratio)
        pnl_adjs_pop.append(pnl_adj)
        
    pnl_m = sorted(pnl_adjs_pop)[len(pnl_adjs_pop) // 2] if pnl_adjs_pop else 0.0

    # --- ROI Population Median (for Formula 2) ---
    rois_pop = [t.get('roi', 0.0) for t in population_metrics]
    roi_m = sorted(rois_pop)[len(rois_pop) // 2] if rois_pop else 0.0

    # Calculate Shrunk Values for ALL traders
    for t in traders_metrics:
        S = t.get('total_stakes', 0.0)
        sum_sq_s = t.get('sum_sq_stakes', 0.0)
        N_eff = (S**2) / sum_sq_s if sum_sq_s > 0 else 0.0
        
        # --- Formula 1: Win Rate ---
        s_w = t.get('winning_stakes', 0.0)
        W = (s_w / S) if S > 0 else 0.0
        W_shrunk = (W * N_eff + config.shrink_baseline_win_rate * config.shrink_kw) / (N_eff + config.shrink_kw)
        t['W_shrunk'] = W_shrunk
        
        # --- Formula 2: ROI ---
        roi_raw = t.get('roi', 0.0)
        roi_shrunk = (roi_raw * N_eff + roi_m * config.shrink_kr) / (N_eff + config.shrink_kr)
        t['roi_shrunk'] = roi_shrunk
        
        # --- Formula 3: PnL ---
        pnl_total = t.get('total_pnl', 0.0)
        max_s = t.get('max_stake', 0.0)
        ratio = (max_s / S) if S > 0 else 0.0
        pnl_adj = pnl_total / (1 + config.shrink_alpha * ratio)
        
        pnl_shrunk = (pnl_adj * N_eff + pnl_m * config.shrink_kp) / (N_eff + config.shrink_kp)
        t['pnl_shrunk'] = pnl_shrunk
        
        # --- Formula 4: Risk ---
        # Risk Score = |Worst Loss| / Total Stake (output range: 0 → 1)
        worst_loss = t.get('worst_loss', 0.0)
        # For future: if we have all_losses list, pass it for average of N worst losses
        all_losses = t.get('all_losses', None)  # Optional: list of all losses
        t['score_risk'] = calculate_risk_score(worst_loss, S, config, all_losses)
    
    # Collect Shrunk values from POPULATION for Percentiles
    w_shrunk_pop = [t['W_shrunk'] for t in population_metrics]
    roi_shrunk_pop = [t['roi_shrunk'] for t in population_metrics]
    pnl_shrunk_pop = [t['pnl_shrunk'] for t in population_metrics]
    
    # Anchors (using configurable percentiles)
    w_1 = get_percentile_value(w_shrunk_pop, config.percentile_lower)
    w_99 = get_percentile_value(w_shrunk_pop, config.percentile_upper)
    
    r_1 = get_percentile_value(roi_shrunk_pop, config.percentile_lower)
    r_99 = get_percentile_value(roi_shrunk_pop, config.percentile_upper)
    
    p_1 = get_percentile_value(pnl_shrunk_pop, config.percentile_lower)
    p_99 = get_percentile_value(pnl_shrunk_pop, config.percentile_upper)
    
    # Final Normalization
    for t in traders_metrics:
        # W score
        if w_99 - w_1 != 0:
            w_score = (t['W_shrunk'] - w_1) / (w_99 - w_1)
        else:
            w_score = 0.5 
        t['score_win_rate'] = clamp(w_score, 0, 1)
        
        # R score
        if r_99 - r_1 != 0:
            r_score = (t['roi_shrunk'] - r_1) / (r_99 - r_1)
        else:
            r_score = 0.5
        t['score_roi'] = clamp(r_score, 0, 1)
        
        # P score
        if p_99 - p_1 != 0:
            p_score = (t['pnl_shrunk'] - p_1) / (p_99 - p_1)
        else:
            p_score = 0.5
        t['score_pnl'] = clamp(p_score, 0, 1)
        
        # Final Rating Formula (0-100 scale)
        # Rating = 100 × [ wW · Wscore + wR · Rscore + wP · Pscore + wrisk · (1 − Risk Score) ]
        w_score = t.get('score_win_rate', 0.0)
        r_score = t.get('score_roi', 0.0)
        p_score = t.get('score_pnl', 0.0)
        risk_score = t.get('score_risk', 0.0)
        
        final_score = 100.0 * (
            config.weight_win_rate * w_score + 
            config.weight_roi * r_score + 
            config.weight_pnl * p_score + 
            config.weight_risk * (1.0 - risk_score)
        )
        t['final_score'] = clamp(final_score, 0, 100)
    
    return {
        "traders": traders_metrics,
        "percentiles": {
            f"w_shrunk_{config.percentile_lower}_percent": w_1,
            f"w_shrunk_{config.percentile_upper}_percent": w_99,
            f"roi_shrunk_{config.percentile_lower}_percent": r_1,
            f"roi_shrunk_{config.percentile_upper}_percent": r_99,
            f"pnl_shrunk_{config.percentile_lower}_percent": p_1,
            f"pnl_shrunk_{config.percentile_upper}_percent": p_99,
        },
        "medians": {
            "roi_median": roi_m,
            "pnl_median": pnl_m,
        },
        "population_size": len(population_metrics),
        "total_traders": len(traders_metrics)
    }

