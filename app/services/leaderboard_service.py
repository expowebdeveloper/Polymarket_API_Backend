"""Leaderboard service for ranking traders by various metrics."""

from typing import List, Dict, Optional
from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, distinct
from decimal import Decimal
import math
from app.db.models import Trade, Position, Activity
from app.core.scoring_config import scoring_config


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
    all_losses = []  # Collect all losses for average calculation (future enhancement)
    stakes_list = []  # Collect all stakes for top 5 calculation
    sum_sq_stakes = Decimal('0')
    
    for trade in trades:
        stake = trade.size * trade.price
        stakes_list.append(stake)
        sum_sq_stakes += stake ** 2
        
        if trade.pnl is not None:
            if trade.pnl < worst_loss:
                worst_loss = trade.pnl
            # Collect all losses (negative PnL values) for average calculation
            if trade.pnl < 0:
                all_losses.append(trade.pnl)
    
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
        "all_losses": [float(loss) for loss in all_losses],  # Store all losses for average calculation
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
    trader: Dict,
    config: Optional[Dict] = None
) -> Dict[str, any]:
    """
    Calculate Risk Score using the fixed formula: Risk Score = |Worst Loss| / Total Stake
    
    Base Rules:
    - Output range: 0 → 1
    - Higher value = higher risk
    - This formula is not percentile-based
    
    Future Enhancements (Configurable):
    1. Average of N Worst Losses: If enabled, use average of N worst losses
    2. Minimum Activity Condition: If enabled, check minimum trades threshold
    
    Args:
        trader: Trader dictionary with metrics
        config: Optional configuration override (uses scoring_config if not provided)
    
    Returns:
        Dictionary with:
        - risk_score: Risk score (0-1)
        - worst_loss: Worst loss value used
        - avg_worst_loss_n: Average of N worst losses (if enabled)
        - insufficient_data: True if minimum activity not met
    """
    if config is None:
        config = scoring_config.get_risk_config()
    
    total_stakes = trader.get('total_stakes', 0.0)
    total_trades = trader.get('total_trades', 0)
    worst_loss = trader.get('worst_loss', 0.0)
    all_losses = trader.get('all_losses', [])
    
    # Future Enhancement 2: Minimum Activity Condition
    insufficient_data = False
    if config.get('min_activity_enabled', False):
        min_trades = config.get('min_trades_threshold', 10)
        if total_trades < min_trades:
            insufficient_data = True
            action = config.get('insufficient_data_action', 'exclude')
            if action == 'exclude':
                return {
                    'risk_score': None,  # Mark for exclusion
                    'worst_loss': worst_loss,
                    'avg_worst_loss_n': None,
                    'insufficient_data': True
                }
            elif action == 'mark_insufficient':
                # Still calculate but mark as insufficient
                pass
            # 'calculate_anyway' - continue with calculation
    
    # Ensure total_stakes is positive to avoid division by zero
    if total_stakes <= 0:
        return {
            'risk_score': 0.0,
            'worst_loss': worst_loss,
            'avg_worst_loss_n': None,
            'insufficient_data': insufficient_data
        }
    
    # Future Enhancement 1: Average of N Worst Losses
    loss_value = abs(worst_loss)  # Default: use single worst loss
    avg_worst_loss_n = None
    
    if config.get('use_avg_n_worst', False):
        n = config.get('n_worst_losses', 5)
        if all_losses and len(all_losses) > 0:
            # Sort losses (most negative first)
            sorted_losses = sorted(all_losses)
            # Take N worst losses (or all if less than N)
            n_worst = sorted_losses[:min(n, len(sorted_losses))]
            if n_worst:
                avg_worst_loss_n = sum(n_worst) / len(n_worst)
                loss_value = abs(avg_worst_loss_n)
    
    # Base Formula: Risk Score = |Worst Loss| / Total Stake
    risk_score = loss_value / total_stakes
    
    # Clamp to 0-1 range (as per specification)
    risk_score = clamp(risk_score, 0.0, 1.0)
    
    return {
        'risk_score': risk_score,
        'worst_loss': worst_loss,
        'avg_worst_loss_n': avg_worst_loss_n,
        'insufficient_data': insufficient_data
    }


def calculate_final_rating(
    scores: Dict[str, float],
    config: Optional[Dict] = None
) -> float:
    """
    Calculate Final Rating using the formula:
    Rating = 100 × [ wW · Wscore + wR · Rscore + wP · Pscore + wrisk · (1 − Risk Score) ]
    
    All component scores (W, R, P) must be normalized to 0–1 before applying weights.
    Risk Score is also 0-1, and we use (1 - Risk Score) in the formula.
    
    Args:
        scores: Dictionary with normalized scores (0-1):
            - w_score: Win-related score
            - r_score: ROI score
            - p_score: PnL score
            - risk_score: Risk score
        config: Optional configuration override (uses scoring_config if not provided)
    
    Returns:
        Final rating (0-100)
    """
    if config is None:
        weights = scoring_config.get_all_weights()
    else:
        weights = config.get('weights', scoring_config.get_all_weights())
    
    w_score = scores.get('w_score', 0.0)
    r_score = scores.get('r_score', 0.0)
    p_score = scores.get('p_score', 0.0)
    risk_score = scores.get('risk_score', 0.0)
    
    # Ensure all scores are in 0-1 range
    w_score = clamp(w_score, 0.0, 1.0)
    r_score = clamp(r_score, 0.0, 1.0)
    p_score = clamp(p_score, 0.0, 1.0)
    risk_score = clamp(risk_score, 0.0, 1.0)
    
    # Final Rating Formula
    # Rating = 100 × [ wW · Wscore + wR · Rscore + wP · Pscore + wrisk · (1 − Risk Score) ]
    rating = 100.0 * (
        weights.get('w', 0.30) * w_score +
        weights.get('r', 0.30) * r_score +
        weights.get('p', 0.30) * p_score +
        weights.get('risk', 0.10) * (1.0 - risk_score)
    )
    
    # Clamp to 0-100 range
    return clamp(rating, 0.0, 100.0)


def calculate_scores_and_rank(traders_metrics: List[Dict]) -> List[Dict]:
    """
    Calculate advanced scores for a list of traders.
    """
    if not traders_metrics:
        return []
        
    # Filter valid traders for population stats (>= 5 trades)
    population_metrics = [t for t in traders_metrics if t.get('total_trades', 0) >= 5]
    
    # Fallback if no active traders
    if not population_metrics:
        population_metrics = traders_metrics 

    # --- PnL Population Median (for Formula 3) ---
    pnl_adjs_pop = []
    
    for t in population_metrics:
        pnl_total = t.get('total_pnl', 0.0)
        S = t.get('total_stakes', 0.0)
        max_s = t.get('max_stake', 0.0)
        alpha = 4.0
        
        ratio = 0.0
        if S > 0:
            ratio = max_s / S
            
        pnl_adj = pnl_total / (1 + alpha * ratio)
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
        b = 0.5
        kW = 50
        W_shrunk = (W * N_eff + b * kW) / (N_eff + kW)
        t['W_shrunk'] = W_shrunk
        
        # --- Formula 2: ROI ---
        roi_raw = t.get('roi', 0.0)
        kR = 50
        roi_shrunk = (roi_raw * N_eff + roi_m * kR) / (N_eff + kR)
        t['roi_shrunk'] = roi_shrunk
        
        # --- Formula 3: PnL ---
        pnl_total = t.get('total_pnl', 0.0)
        max_s = t.get('max_stake', 0.0)
        alpha = 4.0
        ratio = (max_s / S) if S > 0 else 0.0
        pnl_adj = pnl_total / (1 + alpha * ratio)
        
        kp = 50
        pnl_shrunk = (pnl_adj * N_eff + pnl_m * kp) / (N_eff + kp)
        t['pnl_shrunk'] = pnl_shrunk
        
        # --- Formula 4: Risk Score (Fixed Formula) ---
        # Risk Score = |Worst Loss| / Total Stake
        # Output range: 0 → 1, Higher value = higher risk
        risk_result = calculate_risk_score(t)
        
        # Check if trader should be excluded due to insufficient data
        if risk_result['risk_score'] is None:
            # Mark trader for exclusion (will be filtered out later)
            t['_exclude'] = True
            t['score_risk'] = 0.0  # Default value for excluded traders
        else:
            t['score_risk'] = risk_result['risk_score']
            t['_exclude'] = False
    
    # Collect Shrunk values from POPULATION for Percentiles
    w_shrunk_pop = [t['W_shrunk'] for t in population_metrics]
    roi_shrunk_pop = [t['roi_shrunk'] for t in population_metrics]
    pnl_shrunk_pop = [t['pnl_shrunk'] for t in population_metrics]
    
    # Anchors - Use configurable percentiles
    percentile_config = scoring_config.get_percentile_config()
    lower_percentile = percentile_config['lower']
    upper_percentile = percentile_config['upper']
    
    w_1 = get_percentile_value(w_shrunk_pop, lower_percentile)
    w_99 = get_percentile_value(w_shrunk_pop, upper_percentile)
    
    r_1 = get_percentile_value(roi_shrunk_pop, lower_percentile)
    r_99 = get_percentile_value(roi_shrunk_pop, upper_percentile)
    
    p_1 = get_percentile_value(pnl_shrunk_pop, lower_percentile)
    p_99 = get_percentile_value(pnl_shrunk_pop, upper_percentile)
    
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
        
        # Final Score: Weighted combination using configurable weights
        # Rating = 100 × [ wW · Wscore + wR · Rscore + wP · Pscore + wrisk · (1 − Risk Score) ]
        w_score = t.get('score_win_rate', 0.0)
        r_score = t.get('score_roi', 0.0)
        p_score = t.get('score_pnl', 0.0)
        risk_score = t.get('score_risk', 0.0)
        
        scores = {
            'w_score': w_score,
            'r_score': r_score,
            'p_score': p_score,
            'risk_score': risk_score
        }
        
        t['final_score'] = calculate_final_rating(scores)
    
    # Filter out excluded traders (insufficient data)
    traders_metrics = [t for t in traders_metrics if not t.get('_exclude', False)]
    
    return traders_metrics


def calculate_scores_and_rank_with_percentiles(traders_metrics: List[Dict]) -> Dict:
    """
    Calculate advanced scores for a list of traders and return with percentile information.
    
    Returns:
        Dict containing:
        - traders: List of traders with all scores
        - percentiles: Dict with 1% and 99% percentile values
        - medians: Dict with median values used in calculations
        - population_size: Number of traders with >= 5 trades
    """
    if not traders_metrics:
        return {
            "traders": [],
            "percentiles": {
                "w_shrunk_1_percent": 0.0,
                "w_shrunk_99_percent": 0.0,
                "roi_shrunk_1_percent": 0.0,
                "roi_shrunk_99_percent": 0.0,
                "pnl_shrunk_1_percent": 0.0,
                "pnl_shrunk_99_percent": 0.0,
            },
            "medians": {
                "roi_median": 0.0,
                "pnl_median": 0.0,
            },
            "population_size": 0,
            "total_traders": 0
        }
        
    # Filter valid traders for population stats (>= 5 trades)
    population_metrics = [t for t in traders_metrics if t.get('total_trades', 0) >= 5]
    
    # Fallback if no active traders
    if not population_metrics:
        population_metrics = traders_metrics 

    # --- PnL Population Median (for Formula 3) ---
    pnl_adjs_pop = []
    
    for t in population_metrics:
        pnl_total = t.get('total_pnl', 0.0)
        S = t.get('total_stakes', 0.0)
        max_s = t.get('max_stake', 0.0)
        alpha = 4.0
        
        ratio = 0.0
        if S > 0:
            ratio = max_s / S
            
        pnl_adj = pnl_total / (1 + alpha * ratio)
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
        b = 0.5
        kW = 50
        W_shrunk = (W * N_eff + b * kW) / (N_eff + kW)
        t['W_shrunk'] = W_shrunk
        
        # --- Formula 2: ROI ---
        roi_raw = t.get('roi', 0.0)
        kR = 50
        roi_shrunk = (roi_raw * N_eff + roi_m * kR) / (N_eff + kR)
        t['roi_shrunk'] = roi_shrunk
        
        # --- Formula 3: PnL ---
        pnl_total = t.get('total_pnl', 0.0)
        max_s = t.get('max_stake', 0.0)
        alpha = 4.0
        ratio = (max_s / S) if S > 0 else 0.0
        pnl_adj = pnl_total / (1 + alpha * ratio)
        
        kp = 50
        pnl_shrunk = (pnl_adj * N_eff + pnl_m * kp) / (N_eff + kp)
        t['pnl_shrunk'] = pnl_shrunk
        
        # --- Formula 4: Risk Score (Fixed Formula) ---
        # Risk Score = |Worst Loss| / Total Stake
        # Output range: 0 → 1, Higher value = higher risk
        risk_result = calculate_risk_score(t)
        
        # Check if trader should be excluded due to insufficient data
        if risk_result['risk_score'] is None:
            # Mark trader for exclusion (will be filtered out later)
            t['_exclude'] = True
            t['score_risk'] = 0.0  # Default value for excluded traders
        else:
            t['score_risk'] = risk_result['risk_score']
            t['_exclude'] = False
    
    # Collect Shrunk values from POPULATION for Percentiles
    w_shrunk_pop = [t['W_shrunk'] for t in population_metrics]
    roi_shrunk_pop = [t['roi_shrunk'] for t in population_metrics]
    pnl_shrunk_pop = [t['pnl_shrunk'] for t in population_metrics]
    
    # Anchors - Use configurable percentiles
    percentile_config = scoring_config.get_percentile_config()
    lower_percentile = percentile_config['lower']
    upper_percentile = percentile_config['upper']
    
    w_1 = get_percentile_value(w_shrunk_pop, lower_percentile)
    w_99 = get_percentile_value(w_shrunk_pop, upper_percentile)
    
    r_1 = get_percentile_value(roi_shrunk_pop, lower_percentile)
    r_99 = get_percentile_value(roi_shrunk_pop, upper_percentile)
    
    p_1 = get_percentile_value(pnl_shrunk_pop, lower_percentile)
    p_99 = get_percentile_value(pnl_shrunk_pop, upper_percentile)
    
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
        
        # Final Score: Weighted combination using configurable weights
        # Rating = 100 × [ wW · Wscore + wR · Rscore + wP · Pscore + wrisk · (1 − Risk Score) ]
        w_score = t.get('score_win_rate', 0.0)
        r_score = t.get('score_roi', 0.0)
        p_score = t.get('score_pnl', 0.0)
        risk_score = t.get('score_risk', 0.0)
        
        scores = {
            'w_score': w_score,
            'r_score': r_score,
            'p_score': p_score,
            'risk_score': risk_score
        }
        
        t['final_score'] = calculate_final_rating(scores)
    
    # Filter out excluded traders (insufficient data)
    traders_metrics = [t for t in traders_metrics if not t.get('_exclude', False)]
    
    return {
        "traders": traders_metrics,
        "percentiles": {
            "w_shrunk_1_percent": w_1,
            "w_shrunk_99_percent": w_99,
            "roi_shrunk_1_percent": r_1,
            "roi_shrunk_99_percent": r_99,
            "pnl_shrunk_1_percent": p_1,
            "pnl_shrunk_99_percent": p_99,
        },
        "medians": {
            "roi_median": roi_m,
            "pnl_median": pnl_m,
        },
        "population_size": len(population_metrics),
        "total_traders": len(traders_metrics)
    }

