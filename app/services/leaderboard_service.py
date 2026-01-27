"""Leaderboard service for ranking traders by various metrics."""

from typing import List, Dict, Optional
from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, distinct
from decimal import Decimal
import math
from app.db.models import Trade, Position, Activity, ClosedPosition
from app.core.scoring_config import ScoringConfig, default_scoring_config
from app.services.confidence_scoring import calculate_confidence_score
from app.services.scoring_engine import calculate_pnl_score, calculate_new_roi_score

def calculate_median(values: List[float]) -> float:
    """Calculate median of a list of numbers."""
    if not values:
        return 0.0
    sorted_v = sorted(values)
    n = len(sorted_v)
    if n % 2 == 1:
        return sorted_v[n // 2]
    else:
        return (sorted_v[n // 2 - 1] + sorted_v[n // 2]) / 2.0

def calculate_win_score(w_trade: float, w_stake: float) -> float:
    """
    Calculate blended win score.
    W_score = 0.5 * W_trade + 0.5 * W_stake
    Input ranges: 0-1
    Output range: 0-1
    """
    return 0.5 * w_trade + 0.5 * w_stake


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


def process_trader_data_points(
    wallet_address: str,
    trades: List[Dict],
    positions: List[Dict],
    activities: List[Dict],
    closed_positions: List[Dict],
    trader_info: Optional[Dict] = None
) -> Dict:
    """
    Core logic for calculating trader metrics from raw data points.
    Bypasses database sessions.
    """
    total_realized_pnl = Decimal('0')
    total_unrealized_pnl = Decimal('0')
    total_rewards = Decimal('0')
    total_current_value = Decimal('0')
    total_stakes = Decimal('0')
    winning_trades_count = 0
    total_trades_with_pnl = 0
    stakes_of_wins = Decimal('0')
    sum_sq_stakes = Decimal('0')
    worst_loss = Decimal('0')
    all_losses = []
    stakes_list = []
    
    # Buy/Sell Volume (New tracked metrics)
    buy_volume = Decimal('0')
    sell_volume = Decimal('0')
    
    # 1. Activities (Rewards, Buy/Sell Volume)
    for activity in activities:
        # Standardize keys (live responses use camelCase or snake_case)
        a_type = activity.get("type")
        size = Decimal(str(activity.get("usdcSize") or activity.get("usdc_size") or 0))
        side = str(activity.get("side") or "").upper()
        
        if a_type == "REWARD":
            total_rewards += size
        
        if side == "BUY":
            buy_volume += size
        elif side == "SELL":
            sell_volume += size

    # 2. Closed Positions (Stakes, Realized PnL, Win Rate)
    # Sort for MDD
    sorted_closed = sorted(closed_positions, key=lambda x: x.get("timestamp") or x.get("time") or 0)
    running_pnl_accumulator = 0.0
    equity_curve = [0.0]
    
    # Win Rate & Streaks
    wins = 0
    total_losses = 0
    largest_win = 0.0
    
    # Streaks
    longest_streak = 0
    current_streak = 0
    temp_streak = 0

    for cp in sorted_closed:
        # Standardize keys
        bought = Decimal(str(cp.get("totalBought") or cp.get("total_bought") or 0))
        price = Decimal(str(cp.get("avgPrice") or cp.get("avg_price") or 0))
        stake = bought * price
        total_stakes += stake
        sum_sq_stakes += stake ** 2
        stakes_list.append(stake)
        
        realized_pnl = Decimal(str(cp.get("realizedPnl") or cp.get("realized_pnl") or 0))
        total_realized_pnl += realized_pnl
        
        pnl_val = float(realized_pnl)
        running_pnl_accumulator += pnl_val
        equity_curve.append(running_pnl_accumulator)
        
        total_trades_with_pnl += 1
        if pnl_val > 0:
            winning_trades_count += 1
            stakes_of_wins += stake
            wins += 1
            temp_streak += 1
            current_streak = temp_streak
            longest_streak = max(longest_streak, temp_streak)
            if pnl_val > largest_win: largest_win = pnl_val
        elif pnl_val < 0:
            total_losses += 1
            temp_streak = 0
            current_streak = 0
            all_losses.append(pnl_val)
            if worst_loss == Decimal('0') or realized_pnl < worst_loss:
                worst_loss = realized_pnl

    # 3. Active Positions (Risk, Current Value)
    for pos in positions:
        stake = Decimal(str(pos.get("initialValue") or pos.get("initial_value") or 0))
        if stake > 0:
            total_stakes += stake
            sum_sq_stakes += stake ** 2
            stakes_list.append(stake)
        
        cash_pnl = Decimal(str(pos.get("cashPnl") or pos.get("cash_pnl") or 0))
        if cash_pnl < 0:
            all_losses.append(float(cash_pnl))
            if worst_loss == Decimal('0') or cash_pnl < worst_loss:
                worst_loss = cash_pnl
        
        total_current_value += Decimal(str(pos.get("currentValue") or pos.get("current_value") or 0))
        total_unrealized_pnl += cash_pnl # In Polymarket, positioning cashPnl is total pnl for that position

    # 4. Final Aggregations
    total_pnl = total_realized_pnl + total_unrealized_pnl + total_rewards
    
    # Max Stake (Average of top 5)
    max_stake = Decimal('0')
    if stakes_list:
        sorted_stakes = sorted(stakes_list, reverse=True)
        top_n = min(5, len(sorted_stakes))
        max_stake = sum(sorted_stakes[:top_n]) / Decimal(str(top_n)) if top_n > 0 else Decimal('0')

    from app.services.scoring_engine import calculate_max_drawdown
    max_drawdown = calculate_max_drawdown(equity_curve)
    
    roi = (total_pnl / total_stakes * 100) if total_stakes > 0 else Decimal('0')
    win_rate = (Decimal(str(winning_trades_count)) / Decimal(str(total_trades_with_pnl)) * 100) if total_trades_with_pnl > 0 else Decimal('0')
    
    # Stake Volatility
    stake_volatility = 0.0
    if stakes_list:
        n = len(stakes_list)
        mean_stake = float(total_stakes) / n
        if mean_stake > 0:
            variance = (float(sum_sq_stakes) / n) - (mean_stake ** 2)
            std_dev = max(0, variance) ** 0.5
            stake_volatility = std_dev / mean_stake

    # Unique markets
    closed_market_ids = {cp.get("conditionId") or cp.get("condition_id") for cp in closed_positions if cp.get("conditionId") or cp.get("condition_id")}
    active_market_ids = {p.get("conditionId") or p.get("condition_id") for p in positions if p.get("conditionId") or p.get("condition_id")}
    unique_markets = len(closed_market_ids | active_market_ids)
    
    # Total trades (Predictions)
    # Polymarket "Predictions" count usually matches total number of trades/activities
    total_trades_count = len(trades) if trades else unique_markets

    return {
        "wallet_address": wallet_address,
        "name": trader_info.get("name") if trader_info else None,
        "pseudonym": trader_info.get("pseudonym") if trader_info else None,
        "profile_image": trader_info.get("profile_image") if trader_info else None,
        "total_pnl": float(total_pnl),
        "roi": float(roi),
        "win_rate": float(win_rate),
        "total_trades": total_trades_count,
        "unique_markets": unique_markets,
        "total_trades_with_pnl": total_trades_with_pnl,
        "winning_trades": winning_trades_count,
        "winning_trades_count": winning_trades_count,
        "total_stakes": float(total_stakes),
        "winning_stakes": float(stakes_of_wins),
        "worst_loss": float(worst_loss),
        "max_drawdown": float(max_drawdown),
        "all_losses": all_losses,
        "max_stake": float(max_stake),
        "sum_sq_stakes": float(sum_sq_stakes),
        "stake_volatility": float(stake_volatility),
        "portfolio_value": float(total_current_value),
        "buy_volume": float(buy_volume),
        "sell_volume": float(sell_volume),
        "largest_win": float(largest_win),
        "streaks": {
            "longest_streak": longest_streak,
            "current_streak": current_streak,
            "total_wins": wins,
            "total_losses": total_losses
        }
    }


async def calculate_trader_metrics_with_time_filter(
    session: AsyncSession,
    wallet_address: str,
    period: str = 'all',
    use_scraped_data: bool = False
) -> Optional[Dict]:
    """
    Calculate PnL metrics for a trader with time filtering.
    """
    if use_scraped_data:
        from app.services.trader_detail_service import (
            get_scraped_trades_for_calc,
            get_scraped_positions_for_calc,
            get_scraped_activities_for_calc,
            get_scraped_closed_positions_for_calc
        )
        trades = await get_scraped_trades_for_calc(session, wallet_address)
        positions = await get_scraped_positions_for_calc(session, wallet_address)
        activities = await get_scraped_activities_for_calc(session, wallet_address)
        closed_positions = await get_scraped_closed_positions_for_calc(session, wallet_address)
        
        # Helper to convert object to dict if needed (scraped accessors return dicts already, but lets be safe)
        # They return dicts, so no need for row_to_dict
        
        # Scraped data already has timestamps, but lets ensure object attribute access vs dict access is handled
        # process_trader_data_points handles dicts.
        
        # Filter by time - scraped data is dicts, so use ['timestamp'] or .get('timestamp')
        if period != 'all':
            cutoff = int((datetime.utcnow() - timedelta(days=7 if period == '7d' else 30)).timestamp())
            trades = [t for t in trades if t.get('timestamp', 0) >= cutoff]
            closed_positions = [cp for cp in closed_positions if cp.get('timestamp', 0) >= cutoff]
            activities = [a for a in activities if a.get('timestamp', 0) >= cutoff]
            
        if not trades and not positions and not activities:
            return None
            
        # Prepare info from scraped profile/trades
        trader_info = {}
        if trades:
            trader_info = {
                "name": trades[0].get("name"),
                "pseudonym": trades[0].get("pseudonym"),
                "profile_image": trades[0].get("profile_image_optimized") or trades[0].get("profile_image")
            }
            
        return process_trader_data_points(
            wallet_address,
            trades,
            positions,
            activities,
            closed_positions,
            trader_info
        )

    else:
        # ORIGINAL LOGIC using Portfolio tables
        from app.services.trade_service import get_trades_from_db
        from app.services.position_service import get_positions_from_db
        from app.services.activity_service import get_activities_from_db
        from app.services.dashboard_service import row_to_dict
        
        trades = await get_trades_from_db(session, wallet_address)
        positions = await get_positions_from_db(session, wallet_address)
        activities = await get_activities_from_db(session, wallet_address)
        
        stmt = select(ClosedPosition).where(ClosedPosition.proxy_wallet == wallet_address)
        result = await session.execute(stmt)
        closed_positions = result.scalars().all()
        
        # Filter by time (trades are Objects here)
        if period != 'all':
            cutoff = int((datetime.utcnow() - timedelta(days=7 if period == '7d' else 30)).timestamp())
            trades = [t for t in trades if t.timestamp >= cutoff]
            closed_positions = [cp for cp in closed_positions if cp.timestamp >= cutoff]
            activities = [a for a in activities if a.timestamp >= cutoff]
        
        if not trades and not positions and not activities:
            return None
            
        # Prepare info
        trader_info = {}
        if trades:
            trader_info = {
                "name": trades[0].name,
                "pseudonym": trades[0].pseudonym,
                "profile_image": trades[0].profile_image_optimized or trades[0].profile_image
            }

        return process_trader_data_points(
            wallet_address,
            [row_to_dict(t) for t in trades],
            [row_to_dict(p) for p in positions],
            [row_to_dict(a) for a in activities],
            [row_to_dict(cp) for cp in closed_positions],
            trader_info
        )


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
        
    # Calculate PnL median using exact traditional formula
    pnl_m = calculate_median(pnl_adjs_pop)

    # --- ROI Population Median (for Formula 2) ---
    rois_pop = [t.get('roi', 0.0) for t in population_metrics]
    # Calculate ROI median using exact traditional formula
    roi_m = calculate_median(rois_pop)

    # Calculate Shrunk Values for ALL traders
    # --- NEW Scoring Implementation (Replaces Shrunk/Percentile Logic) ---
    
    for t in traders_metrics:
        # 1. Risk Score (Average Worst Loss)
        all_losses = t.get('all_losses', [])
        total_stakes = t.get('total_stakes', 0.0)
        total_trades = t.get('total_trades', 0)
        
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
            t['score_risk'] = float(risk_score_new)
            t['risk_score'] = float(risk_score_new)
            t['_exclude'] = False

        # 2. Win Score
        win_rate_percent = t.get('win_rate', 0.0)
        win_rate_trade = win_rate_percent / 100.0
        
        winning_stakes = t.get('winning_stakes', 0.0)
        win_rate_stake = (winning_stakes / total_stakes) if total_stakes > 0 else 0.0
        
        t['score_win_rate'] = calculate_win_score(win_rate_trade, win_rate_stake)
        
        # Use Deterministic Scoring for ROI and PnL
        # This overrides population percentiles to ensure consistency
        roi_val = t.get('roi', 0.0) / 100.0 # Input as decimal
        pnl_val = t.get('total_pnl', 0.0)
        
        t['score_roi'] = calculate_new_roi_score(roi_val) # returns 0-1
        t['score_pnl'] = calculate_pnl_score(pnl_val) # returns 0-1
        
        # Risk Score is already 0-1
        risk_score = t.get('score_risk', 0.0)
        risk_factor = 1.0 - risk_score
        
        # Final Rating Formula (Deterministic matching Specifications)
        # Formula: Rating = 100 * Conf(N) * [ 0.225 * W_score + 0.225 * ROI_score + 0.45 * PnL_score + 0.10 * Risk_factor ]
        
        # Calculate Confidence Score based on number of trades
        num_predictions = t.get('total_trades_with_pnl', 0)
        confidence_multiplier = calculate_confidence_score(num_predictions)
        
        # Win Score (Blended) is already 0-1
        w_score_final = t.get('score_win_rate', 0.0)
        
        # Calculate weighted sum (0-1 range)
        weighted_sum = (
            config.weight_win_rate * w_score_final + 
            config.weight_roi * t['score_roi'] + 
            config.weight_pnl * t['score_pnl'] + 
            config.weight_risk * risk_factor
        )
        
        # Apply Confidence Multiplier and scale to 100
        final_score = 100.0 * confidence_multiplier * weighted_sum
        
        t['final_score'] = clamp(final_score, 0, 100)
        t['confidence_score'] = confidence_multiplier
        
        # Also scale component scores to 0-100 for display compatibility if needed
        t['score_roi'] *= 100.0
        t['score_pnl'] *= 100.0
        
    return traders_metrics


def calculate_scores_and_rank_with_percentiles(
    traders_metrics: List[Dict],
    config: Optional[ScoringConfig] = None,
    pnl_median: Optional[float] = None,
    roi_median: Optional[float] = None
) -> Dict:
    """
    Calculate advanced scores for a list of traders and return with percentile information.
    
    Args:
        traders_metrics: List of trader metrics dictionaries
        config: Scoring configuration (uses default if not provided)
        pnl_median: Optional pre-calculated PnL median
        roi_median: Optional pre-calculated ROI median
    
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
    if pnl_median is not None:
        pnl_m = pnl_median
    else:
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
    # Use provided median from database, or calculate from current population
    if roi_median is not None:
        roi_m = roi_median
    else:
        # Calculate from current population (fallback for backward compatibility)
        rois_pop = [t.get('roi', 0.0) for t in population_metrics]
        # Calculate ROI median using exact traditional formula
        if not rois_pop:
            roi_m = 0.0
        else:
            rois_pop.sort()
            n = len(rois_pop)
            if n % 2 == 1:
                roi_m = rois_pop[n // 2]
            else:
                roi_m = (rois_pop[n // 2 - 1] + rois_pop[n // 2]) / 2.0

    # --- NEW Scoring Implementation (Deterministic) ---
    for t in traders_metrics:
        # 1. Risk Score (Average Worst Loss)
        all_losses = t.get('all_losses', [])
        total_stakes = t.get('total_stakes', 0.0)
        S = total_stakes
        total_trades = t.get('total_trades', 0)
        N_eff = float(total_trades)
        
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
            # Normalize w_shrunk to 0-1 range based on percentiles
            w_factor = (t['W_shrunk'] - w_1) / (w_99 - w_1)
        else:
            w_factor = 0.5 # Neutral if no variance
        
        # 2. Win Score (Blended)
        win_rate_percent = t.get('win_rate', 0.0)
        win_rate_trade = win_rate_percent / 100.0
        
        winning_stakes = t.get('winning_stakes', 0.0)
        total_stakes = t.get('total_stakes', 0.0) # Ensure using the loop variable
        win_rate_stake = (winning_stakes / total_stakes) if total_stakes > 0 else 0.0
        
        t['score_win_rate'] = calculate_win_score(win_rate_trade, win_rate_stake)
        
        # Normalize other scores if needed, for now we assume they are 0-100 or 0-1
        # In this implementation, we map shrunk values to scores implicitly or explicitly
        # For simplicity and matching dashboard:
        
        # ROI Score
        # ROI Score
        if r_99 - r_1 != 0:
             r_score = (t['roi_shrunk'] - r_1) / (r_99 - r_1)
        else:
             r_score = 0.5
        t['score_roi'] = clamp(r_score, 0.0, 1.0) * 100.0 # Scale to 0-100 for final weight
        
        # PnL Score
        if p_99 - p_1 != 0:
             p_score = (t['pnl_shrunk'] - p_1) / (p_99 - p_1)
        else:
             p_score = 0.5
        t['score_pnl'] = clamp(p_score, 0.0, 1.0) * 100.0
        
        # Risk Score is 0-1
        risk_score = t.get('score_risk', 0.0)
        
        # Use Deterministic Scoring for ROI and PnL
        # This overrides population percentiles to ensure consistency
        roi_val = t.get('roi', 0.0) / 100.0 # Input as decimal
        pnl_val = t.get('total_pnl', 0.0)
        
        t['score_roi'] = calculate_new_roi_score(roi_val) # returns 0-1
        t['score_pnl'] = calculate_pnl_score(pnl_val) # returns 0-1
        
        # Risk Score is already 0-1
        risk_score = t.get('score_risk', 0.0)
        risk_factor = 1.0 - risk_score
        
        # Final Rating Formula (Deterministic matching Specifications)
        # Formula: Rating = 100 * Conf(N) * [ 0.225 * W_score + 0.225 * ROI_score + 0.45 * PnL_score + 0.10 * Risk_factor ]
        
        # Calculate Confidence Score based on number of trades
        num_predictions = t.get('total_trades_with_pnl', 0)
        confidence_multiplier = calculate_confidence_score(num_predictions)
        
        # Win Score (Blended) is already 0-1
        w_score_final = t.get('score_win_rate', 0.0)
        
        # Calculate weighted sum (0-1 range)
        weighted_sum = (
            config.weight_win_rate * w_score_final + 
            config.weight_roi * t['score_roi'] + 
            config.weight_pnl * t['score_pnl'] + 
            config.weight_risk * risk_factor
        )
        
        # Apply Confidence Multiplier and scale to 100
        final_score = 100.0 * confidence_multiplier * weighted_sum
        
        t['final_score'] = clamp(final_score, 0, 100)
        t['confidence_score'] = confidence_multiplier
        
        # Also scale component scores to 0-100 for display compatibility
        t['score_roi'] *= 100.0
        t['score_pnl'] *= 100.0
    
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
            "roi_median": float(roi_m),
            "pnl_median": float(pnl_m),
        },
        "population_size": len(population_metrics),
        "total_traders": len(traders_metrics)
    }

