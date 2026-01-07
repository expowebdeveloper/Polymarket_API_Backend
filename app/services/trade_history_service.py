"""Trade history service for calculating comprehensive trade metrics."""

from typing import List, Dict, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text
from decimal import Decimal
from collections import defaultdict

from app.db.models import Trade, Position, ClosedPosition
from app.services.data_fetcher import (
    fetch_market_by_slug_from_dome, 
    get_market_category,
    fetch_resolved_markets,
    get_market_by_id,
    get_market_resolution,
    fetch_trades_for_wallet
)
from app.services.scoring_engine import (
    calculate_metrics,
    calculate_roi,
    calculate_consistency,
    calculate_recency,
    calculate_trade_pnl,
    calculate_new_risk_score,
    calculate_win_score,
    calculate_confidence_score,
    calculate_new_roi_score,
    calculate_max_drawdown
)
from app.core.constants import (
    DEFAULT_CATEGORY,
    ROI_WEIGHT,
    WIN_RATE_WEIGHT,
    CONSISTENCY_WEIGHT,
    RECENCY_WEIGHT
)
from app.services.pnl_median_service import get_pnl_median_from_population


async def get_trade_history(
    session: AsyncSession,
    wallet_address: str
) -> Dict:
    """
    Get comprehensive trade history including open/closed positions, PnL, ROI, Win Rate, and Score.
    Also includes category breakdowns.
    
    Args:
        session: Database session
        wallet_address: Wallet address
    
    Returns:
        Dictionary with trade history data including:
        - open_positions: List of open positions
        - closed_positions: List of closed positions
        - trades: List of all trades with PnL and ROI
        - overall_metrics: Overall ROI, PnL, Win Rate, Score
        - category_breakdown: Metrics broken down by category
    """
    # Fetch trades from database
    trades_query = select(Trade).filter(
        Trade.proxy_wallet == wallet_address
    ).order_by(Trade.timestamp.desc())
    
    trades_result = await session.execute(trades_query)
    trades = trades_result.scalars().all()
    
    # Fetch open positions from database
    positions_query = select(Position).filter(
        Position.proxy_wallet == wallet_address
    )
    positions_result = await session.execute(positions_query)
    open_positions = positions_result.scalars().all()
    
    # Fetch closed positions from database
    closed_positions_query = select(ClosedPosition).filter(
        ClosedPosition.proxy_wallet == wallet_address
    ).order_by(ClosedPosition.timestamp.desc())
    closed_positions_result = await session.execute(closed_positions_query)
    closed_positions = closed_positions_result.scalars().all()
    
    # Convert trades to dictionaries and enrich with category
    trades_list = []
    market_cache = {}  # Cache market data to avoid repeated API calls
    
    for trade in trades:
        trade_dict = {
            "id": trade.id,
            "proxy_wallet": trade.proxy_wallet,
            "side": trade.side,
            "asset": trade.asset,
            "condition_id": trade.condition_id,
            "size": float(trade.size) if trade.size else 0.0,
            "price": float(trade.price) if trade.price else 0.0,
            "entry_price": float(trade.entry_price) if trade.entry_price else None,
            "exit_price": float(trade.exit_price) if trade.exit_price else None,
            "pnl": float(trade.pnl) if trade.pnl else None,
            "timestamp": trade.timestamp,
            "title": trade.title,
            "slug": trade.slug,
            "icon": trade.icon,
            "event_slug": trade.event_slug,
            "outcome": trade.outcome,
            "outcome_index": trade.outcome_index,
            "transaction_hash": trade.transaction_hash,
        }
        
        # Get category for this trade
        category = DEFAULT_CATEGORY
        if trade.slug:
            # Try to get market data to extract category
            if trade.slug not in market_cache:
                market = fetch_market_by_slug_from_dome(trade.slug)
                if market:
                    market_cache[trade.slug] = market
                else:
                    market_cache[trade.slug] = None
            
            market = market_cache.get(trade.slug)
            if market:
                category = get_market_category(market)
        
        trade_dict["category"] = category
        
        # Calculate ROI for this trade if we have entry and exit prices
        if trade.entry_price and trade.exit_price and trade.entry_price > 0:
            roi = ((float(trade.exit_price) - float(trade.entry_price)) / float(trade.entry_price)) * 100
            trade_dict["roi"] = round(roi, 2)
        elif trade.pnl and trade.entry_price and trade.entry_price > 0:
            # Calculate ROI from PnL: ROI = (PnL / (entry_price * size)) * 100
            cost_basis = float(trade.entry_price) * float(trade.size)
            if cost_basis > 0:
                roi = (float(trade.pnl) / cost_basis) * 100
                trade_dict["roi"] = round(roi, 2)
            else:
                trade_dict["roi"] = None
        else:
            trade_dict["roi"] = None
        
        trades_list.append(trade_dict)
    
    # Convert open positions to dictionaries
    open_positions_list = []
    for pos in open_positions:
        pos_dict = {
            "id": pos.id,
            "proxy_wallet": pos.proxy_wallet,
            "asset": pos.asset,
            "condition_id": pos.condition_id,
            "size": float(pos.size) if pos.size else 0.0,
            "avg_price": float(pos.avg_price) if pos.avg_price else 0.0,
            "initial_value": float(pos.initial_value) if pos.initial_value else 0.0,
            "current_value": float(pos.current_value) if pos.current_value else 0.0,
            "cash_pnl": float(pos.cash_pnl) if pos.cash_pnl else 0.0,
            "percent_pnl": float(pos.percent_pnl) if pos.percent_pnl else 0.0,
            "cur_price": float(pos.cur_price) if pos.cur_price else 0.0,
            "title": pos.title,
            "slug": pos.slug,
            "icon": pos.icon,
            "outcome": pos.outcome,
            "category": DEFAULT_CATEGORY,
        }
        
        # Get category for position
        if pos.slug:
            if pos.slug not in market_cache:
                market = fetch_market_by_slug_from_dome(pos.slug)
                if market:
                    market_cache[pos.slug] = market
                else:
                    market_cache[pos.slug] = None
            
            market = market_cache.get(pos.slug)
            if market:
                pos_dict["category"] = get_market_category(market)
        
        # Calculate ROI for open position
        if pos.avg_price and pos.avg_price > 0:
            roi = ((float(pos.cur_price) - float(pos.avg_price)) / float(pos.avg_price)) * 100
            pos_dict["roi"] = round(roi, 2)
        else:
            pos_dict["roi"] = None
        
        open_positions_list.append(pos_dict)
    
    # Convert closed positions to dictionaries
    closed_positions_list = []
    for pos in closed_positions:
        pos_dict = {
            "id": pos.id,
            "proxy_wallet": pos.proxy_wallet,
            "asset": pos.asset,
            "condition_id": pos.condition_id,
            "avg_price": float(pos.avg_price) if pos.avg_price else 0.0,
            "cur_price": float(pos.cur_price) if pos.cur_price else 0.0,
            "realized_pnl": float(pos.realized_pnl) if pos.realized_pnl else 0.0,
            "title": pos.title,
            "slug": pos.slug,
            "icon": pos.icon,
            "outcome": pos.outcome,
            "timestamp": pos.timestamp,
            "category": DEFAULT_CATEGORY,
        }
        
        # Get category for closed position
        if pos.slug:
            if pos.slug not in market_cache:
                market = fetch_market_by_slug_from_dome(pos.slug)
                if market:
                    market_cache[pos.slug] = market
                else:
                    market_cache[pos.slug] = None
            
            market = market_cache.get(pos.slug)
            if market:
                pos_dict["category"] = get_market_category(market)
        
        # Calculate ROI for closed position
        if pos.avg_price and pos.avg_price > 0:
            roi = ((float(pos.cur_price) - float(pos.avg_price)) / float(pos.avg_price)) * 100
            pos_dict["roi"] = round(roi, 2)
        else:
            pos_dict["roi"] = None
        
        closed_positions_list.append(pos_dict)
    
    # Fetch trades from API (same as Trader Analytics) for accurate scoring
    # This ensures we use the same data source and calculation method
    try:
        api_trades = await fetch_trades_for_wallet(wallet_address)
    except Exception as e:
        print(f"Warning: Could not fetch trades from API: {e}")
        # Fallback: convert database trades to format expected by scoring engine
        api_trades = []
        for trade in trades_list:
            scoring_trade = {
                "market_id": trade.get("slug") or trade.get("condition_id"),
                "market_slug": trade.get("slug"),
                "slug": trade.get("slug"),
                "side": trade.get("side"),
                "size": trade.get("size"),
                "shares_normalized": trade.get("size"),
                "price": trade.get("price"),
                "timestamp": trade.get("timestamp"),
                "outcome": trade.get("outcome"),
                "outcomeIndex": trade.get("outcome_index"),
            }
            api_trades.append(scoring_trade)
    
    # Fetch resolved markets for scoring engine
    markets = await fetch_resolved_markets(limit=200)
    
    # This ensures consistency between Trade History and Trader Analytics
    scoring_metrics = await calculate_metrics(wallet_address, api_trades, markets)
    
    # Get PnL median from population for shrinkage calculation (from Polymarket API)
    pnl_median = await get_pnl_median_from_population()
    
    # Calculate overall metrics using both scoring engine results and position data
    overall_metrics = calculate_overall_metrics_enhanced(
        trades_list, 
        open_positions_list, 
        closed_positions_list,
        scoring_metrics,
        pnl_median
    )
    
    # Calculate category breakdown using scoring engine results
    category_breakdown = calculate_category_breakdown_enhanced(
        trades_list, 
        open_positions_list, 
        closed_positions_list,
        scoring_metrics
    )
    
    return {
        "wallet_address": wallet_address,
        "open_positions": open_positions_list,
        "closed_positions": closed_positions_list,
        "trades": trades_list,
        "overall_metrics": overall_metrics,
        "category_breakdown": category_breakdown,
    }


def calculate_overall_metrics_enhanced(
    trades: List[Dict],
    open_positions: List[Dict],
    closed_positions: List[Dict],
    scoring_metrics: Dict,
    pnl_median: float = 0.0
) -> Dict:
    """
    Calculate overall ROI, PnL, Win Rate, and Score using scoring engine results.
    This ensures consistency with Trader Analytics.
    """
    # Use scoring engine metrics as base (these are calculated from resolved markets)
    # Fix: Use the sum of wins and losses for total trades to ensure consistency with displayed metrics
    # instead of using the local DB count which might be out of sync or stale.
    winning_trades = scoring_metrics.get("win_count", 0)
    losing_trades = scoring_metrics.get("loss_count", 0)
    total_trades_with_pnl = winning_trades + losing_trades
    
    # Use the calculated total from scoring metrics as the primary Total Trades count
    total_trades_count = total_trades_with_pnl
    
    # PnL from scoring engine (based on resolved markets)
    scoring_pnl = scoring_metrics.get("pnl", 0.0)
    
    # Add unrealized PnL from open positions
    total_unrealized_pnl = 0.0
    for pos in open_positions:
        if pos.get("cash_pnl"):
            total_unrealized_pnl += pos["cash_pnl"]
    
    # Add realized PnL from closed positions (if not already in scoring_pnl)
    total_realized_pnl_from_positions = 0.0
    for pos in closed_positions:
        if pos.get("realized_pnl"):
            total_realized_pnl_from_positions += pos["realized_pnl"]
    
    # Total PnL = scoring PnL (from resolved trades) + unrealized from open positions
    # Note: Closed positions PnL might already be included in scoring_pnl if trades were processed
    total_pnl = scoring_pnl + total_unrealized_pnl
    
    # Calculate total volume from all trades
    total_volume = 0.0
    for trade in trades:
        if trade.get("entry_price") and trade.get("size"):
            total_volume += trade["entry_price"] * trade["size"]
        elif trade.get("price") and trade.get("size"):
            total_volume += trade["price"] * trade["size"]
    
    # Add volume from open positions
    for pos in open_positions:
        if pos.get("initial_value"):
            total_volume += pos["initial_value"]
    
    # Calculate ROI using scoring engine formula
    roi = scoring_metrics.get("roi", 0.0)
    if roi == 0.0 and total_volume > 0:
        # Fallback: calculate ROI from total PnL and volume
        roi = (total_pnl / total_volume * 100) if total_volume > 0 else 0.0
    
    # Win Rate from scoring engine
    win_rate = scoring_metrics.get("win_rate_percent", 0.0)
    
    # Score from scoring engine (uses ROI, Win Rate, Consistency, Recency)
    score = scoring_metrics.get("final_score", 0.0)
    
    # Calculate PnL shrunk using the same formula as leaderboard
    # Step 1: Calculate PnL_adj (whale-adjusted)
    # First, calculate max stake and total stakes from closed positions
    max_stake = 0.0
    total_stakes = 0.0
    
    for pos in closed_positions:
        # Calculate stake from closed position
        if pos.get("avg_price") and pos.get("size"):
            stake = float(pos.get("avg_price", 0)) * float(pos.get("size", 0))
        elif pos.get("initial_value"):
            stake = float(pos.get("initial_value", 0))
        else:
            continue
        
        total_stakes += stake
        if stake > max_stake:
            max_stake = stake
    
    # Also check trades for stakes
    for trade in trades:
        if trade.get("entry_price") and trade.get("size"):
            stake = float(trade.get("entry_price", 0)) * float(trade.get("size", 0))
        elif trade.get("price") and trade.get("size"):
            stake = float(trade.get("price", 0)) * float(trade.get("size", 0))
        else:
            continue
        
        total_stakes += stake
        if stake > max_stake:
            max_stake = stake
    
    # Calculate PnL_adj = PnL_total / (1 + alpha * (max_s / S))
    alpha = 4.0
    ratio = (max_stake / total_stakes) if total_stakes > 0 else 0.0
    pnl_adj = total_pnl / (1 + alpha * ratio) if (1 + alpha * ratio) > 0 else total_pnl
    
    # Step 2: Calculate N_eff (effective number of trades)
    # N_eff = (Sum s_i)^2 / Sum (s_i^2)
    sum_sq_stakes = 0.0
    stakes_list = []
    
    for pos in closed_positions:
        if pos.get("avg_price") and pos.get("size"):
            stake = float(pos.get("avg_price", 0)) * float(pos.get("size", 0))
        elif pos.get("initial_value"):
            stake = float(pos.get("initial_value", 0))
        else:
            continue
        stakes_list.append(stake)
        sum_sq_stakes += stake * stake
    
    for trade in trades:
        if trade.get("entry_price") and trade.get("size"):
            stake = float(trade.get("entry_price", 0)) * float(trade.get("size", 0))
        elif trade.get("price") and trade.get("size"):
            stake = float(trade.get("price", 0)) * float(trade.get("size", 0))
        else:
            continue
        stakes_list.append(stake)
        sum_sq_stakes += stake * stake
    
    n_eff = (total_stakes ** 2 / sum_sq_stakes) if sum_sq_stakes > 0 else len(stakes_list) if stakes_list else 1.0
    
    # Step 3: Calculate PnL_shrunk = (PnL_adj * N_eff + PnL_m * k_p) / (N_eff + k_p)
    k_p = 50.0
    pnl_shrunk = (pnl_adj * n_eff + pnl_median * k_p) / (n_eff + k_p) if (n_eff + k_p) > 0 else pnl_adj
    
    # --- New Metrics Calculation ---
    # Risk Score
    all_losses = []
    equity_curve = [0.0]
    running_pnl = 0.0
    
    # Sort closed positions by timestamp for equity curve
    sorted_closed = sorted(closed_positions, key=lambda x: x.get("timestamp") or 0)
    for pos in sorted_closed:
        pnl = pos.get("realized_pnl", 0.0)
        running_pnl += pnl
        equity_curve.append(running_pnl)
        if pnl < 0:
            all_losses.append(pnl)
            
    # Max Drawdown
    max_drawdown = calculate_max_drawdown(equity_curve)
    
    # Worst Loss
    worst_loss = min(all_losses) if all_losses else 0.0
    
    # Risk Score (Average Worst Loss / Total Stake)
    risk_score = calculate_new_risk_score(all_losses, total_stakes, total_trades_count) or 0.0
    
    # Win Score
    win_rate_trade = win_rate / 100.0
    # Stake-weighted win rate: Winning Stake / Total Stake
    winning_stakes = 0.0
    for pos in closed_positions:
        if pos.get("realized_pnl", 0.0) > 0:
            if pos.get("avg_price") and pos.get("size"):
                winning_stakes += float(pos.get("avg_price", 0)) * float(pos.get("size", 0))
            elif pos.get("initial_value"):
                winning_stakes += float(pos.get("initial_value", 0))
    
    win_rate_stake = (winning_stakes / total_stakes) if total_stakes > 0 else 0.0
    win_score = calculate_win_score(win_rate_trade, win_rate_stake)
    
    # ROI Score
    roi_decimal = roi / 100.0
    roi_score = calculate_new_roi_score(roi_decimal)
    
    # PnL Score
    from app.services.scoring_engine import calculate_pnl_score
    pnl_score_val = calculate_pnl_score(total_pnl)
    
    # Confidence Score
    confidence_score = calculate_confidence_score(total_trades_with_pnl)
    
    # Final Score (Blended)
    # Using the same weights as leaderboard_service
    # Rating = 100 × [ 0.30 * win_score + 0.30 * roi_score + 0.30 * pnl_score + 0.10 * (1 - risk) ] * confidence
    risk_val = max(0.0, min(1.0, risk_score))
    final_score = (
        0.30 * win_score +
        0.30 * roi_score +
        0.30 * pnl_score_val +
        0.10 * (1.0 - risk_val)
    ) * 100.0 * confidence_score

    # Stake Volatility
    stake_volatility = 0.0
    if stakes_list:
        n = len(stakes_list)
        mean_stake = sum(stakes_list) / n
        if mean_stake > 0:
            variance = sum((s - mean_stake) ** 2 for s in stakes_list) / n
            std_dev = variance ** 0.5
            stake_volatility = std_dev / mean_stake

    # Shrunk ROI (simple version for now, could be improved)
    # Formula: ROI_shrunk = (ROI_adj * N_eff + ROI_m * k_r) / (N_eff + k_r)
    # For now, we'll just return raw ROI or a simplified shrunk version
    roi_shrunk = roi # Placeholder for now, can be refined if median is available
    
    return {
        "total_pnl": round(total_pnl, 2),
        "realized_pnl": round(scoring_pnl, 2),  # Realized from resolved trades
        "unrealized_pnl": round(total_unrealized_pnl, 2),
        "roi": round(roi, 2),
        "win_rate": round(win_rate, 2),
        "winning_trades": winning_trades,
        "losing_trades": losing_trades,
        "total_trades": total_trades_count,  # All trades from database
        "total_trades_with_pnl": total_trades_with_pnl,  # Trades with calculated PnL
        "score": round(final_score, 2),
        "total_volume": round(total_volume, 2),
        "pnl_adj": round(pnl_adj, 2),  # Whale-adjusted PnL
        "pnl_shrunk": round(pnl_shrunk, 2),  # Shrunk PnL
        "n_eff": round(n_eff, 2),  # Effective number of trades
        "pnl_median_used": round(pnl_median, 2),  # Median used in calculation
        "risk_score": round(risk_score, 4),
        "confidence_score": round(confidence_score, 4),
        "stake_volatility": round(stake_volatility, 4),
        "max_drawdown": round(max_drawdown, 2),
        "worst_loss": round(worst_loss, 2),
        "roi_shrunk": round(roi_shrunk, 2),
    }


def calculate_category_breakdown_enhanced(
    trades: List[Dict],
    open_positions: List[Dict],
    closed_positions: List[Dict],
    scoring_metrics: Dict,
    pnl_median: float = 0.0
) -> Dict[str, Dict]:
    """
    Calculate ROI, PnL, Win Rate, and Score broken down by category.
    Uses scoring engine category data for consistency.
    """
    # Start with category data from scoring engine
    scoring_categories = scoring_metrics.get("categories", {})
    
    # Build breakdown from scoring engine categories
    breakdown = {}
    for category, cat_metrics in scoring_categories.items():
        total_wins = cat_metrics.get("total_wins", 0.0)
        total_losses = cat_metrics.get("total_losses", 0.0)
        win_count = cat_metrics.get("win_count", 0)
        loss_count = cat_metrics.get("loss_count", 0)
        cat_pnl = cat_metrics.get("pnl", 0.0)
        win_rate_percent = cat_metrics.get("win_rate_percent", 0.0)
        
        # Calculate total trades in this category
        total_trades = win_count + loss_count
        
        # Calculate volume for this category from trades
        total_volume = 0.0
        for trade in trades:
            if trade.get("category", DEFAULT_CATEGORY) == category:
                if trade.get("entry_price") and trade.get("size"):
                    total_volume += trade["entry_price"] * trade["size"]
                elif trade.get("price") and trade.get("size"):
                    total_volume += trade["price"] * trade["size"]
        
        # Add volume from positions in this category
        for pos in open_positions:
            if pos.get("category", DEFAULT_CATEGORY) == category:
                if pos.get("initial_value"):
                    total_volume += pos["initial_value"]
        
        # Calculate ROI
        roi = (cat_pnl / total_volume * 100) if total_volume > 0 else 0.0
        
        # Calculate Score using same formula as overall score
        roi_score = min(max(roi / 100.0, -1.0), 1.0)
        win_rate_score = win_rate_percent / 100.0
        pnl_score = min(max(cat_pnl / 10000.0, -1.0), 1.0) if cat_pnl != 0 else 0.0
        
        score = (ROI_WEIGHT * roi_score + WIN_RATE_WEIGHT * win_rate_score + 0.3 * pnl_score) * 100
        score = max(0, min(100, score))
        
        # Add unrealized PnL from open positions in this category
        unrealized_pnl = 0.0
        for pos in open_positions:
            if pos.get("category", DEFAULT_CATEGORY) == category:
                if pos.get("cash_pnl"):
                    unrealized_pnl += pos["cash_pnl"]
        
        breakdown[category] = {
            "roi": round(roi, 2),
            "pnl": round(cat_pnl + unrealized_pnl, 2),
            "realized_pnl": round(cat_pnl, 2),
            "unrealized_pnl": round(unrealized_pnl, 2),
            "win_rate": round(win_rate_percent, 2),
            "winning_trades": win_count,
            "losing_trades": loss_count,
            "total_trades": total_trades,
            "score": round(score, 2),
            "total_volume": round(total_volume, 2),
        }
    
    return breakdown

