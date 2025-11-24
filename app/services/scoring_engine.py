"""
Scoring engine service for calculating trader performance metrics.
"""

from typing import List, Dict, Tuple
from datetime import datetime, timedelta
from collections import defaultdict

from app.services.data_fetcher import (
    get_market_resolution,
    get_market_by_id,
    get_market_category
)
from app.core.constants import (
    DEFAULT_CURRENT_VALUE,
    ROI_WEIGHT,
    WIN_RATE_WEIGHT,
    CONSISTENCY_WEIGHT,
    RECENCY_WEIGHT,
    MAX_CONSISTENCY_TRADES,
    CONSISTENCY_WEIGHTS,
    RECENCY_DAYS,
    RESOLUTION_YES,
    RESOLUTION_NO,
)


def calculate_trade_pnl(trade: Dict, market_resolution: str) -> Tuple[float, bool]:
    """
    Calculate profit/loss for a single trade.
    Returns (pnl_amount, is_win) tuple.
    
    For winning trades: profit = size * (1 - price)
    For losing trades: loss = -size
    """
    outcome = trade.get("outcome") or trade.get("side") or trade.get("outcomeYes")
    if not outcome:
        return 0.0, False
    
    outcome = str(outcome).upper()
    size = float(trade.get("size") or trade.get("amount") or trade.get("quantity") or 0)
    price = float(trade.get("price") or trade.get("fillPrice") or trade.get("fill_price") or 0)
    
    if size == 0:
        return 0.0, False
    
    # Determine if this trade is a win
    is_win = False
    if market_resolution == RESOLUTION_YES and (outcome == RESOLUTION_YES or outcome == "1"):
        is_win = True
    elif market_resolution == RESOLUTION_NO and (outcome == RESOLUTION_NO or outcome == "0"):
        is_win = True
    
    # Calculate profit/loss
    if is_win:
        if price > 0:
            profit = size * (1 - price)
        else:
            profit = size  # If price is 0, full size is profit
        return profit, True
    else:
        loss = -size
        return loss, False


def calculate_consistency(trades: List[Dict], markets: List[Dict]) -> float:
    """
    Calculate consistency as weighted average of last 10 trades.
    Weights: 10, 9, 8, ..., 1 for most to least recent.
    """
    if not trades:
        return 0.0
    
    # Sort trades by timestamp (most recent first)
    def get_timestamp(trade):
        timestamp_str = trade.get("timestamp") or trade.get("createdAt") or trade.get("created_at") or trade.get("time")
        if not timestamp_str:
            return datetime.min
        try:
            # Handle various timestamp formats
            if isinstance(timestamp_str, (int, float)):
                return datetime.fromtimestamp(timestamp_str)
            timestamp_str = str(timestamp_str).replace("Z", "+00:00")
            return datetime.fromisoformat(timestamp_str)
        except:
            return datetime.min
    
    sorted_trades = sorted(trades, key=get_timestamp, reverse=True)
    
    # Take last N trades
    recent_trades = sorted_trades[:MAX_CONSISTENCY_TRADES]
    
    if not recent_trades:
        return 0.0
    
    total_weight = 0
    weighted_sum = 0
    
    for idx, trade in enumerate(recent_trades):
        weight = CONSISTENCY_WEIGHTS[idx]
        market_id = trade.get("market_id") or trade.get("market") or trade.get("marketId")
        if not market_id:
            continue
        
        market_resolution = get_market_resolution(market_id, markets)
        if not market_resolution:
            continue
        
        _, is_win = calculate_trade_pnl(trade, market_resolution)
        weighted_sum += weight * (1.0 if is_win else 0.0)
        total_weight += weight
    
    if total_weight == 0:
        return 0.0
    
    return weighted_sum / total_weight


def calculate_recency(trades: List[Dict]) -> float:
    """
    Calculate recency score as % of trades in last 7 days.
    """
    if not trades:
        return 0.0
    
    now = datetime.now()
    seven_days_ago = now - timedelta(days=RECENCY_DAYS)
    
    recent_count = 0
    
    for trade in trades:
        try:
            timestamp_str = trade.get("timestamp") or trade.get("createdAt") or trade.get("created_at") or trade.get("time")
            if not timestamp_str:
                continue
            
            # Parse timestamp
            if isinstance(timestamp_str, (int, float)):
                timestamp = datetime.fromtimestamp(timestamp_str)
            else:
                timestamp_str = str(timestamp_str).replace("Z", "+00:00")
                timestamp = datetime.fromisoformat(timestamp_str)
            
            if timestamp >= seven_days_ago:
                recent_count += 1
        except:
            continue
    
    return recent_count / len(trades) if trades else 0.0


def calculate_roi(total_profit: float, total_volume: float) -> float:
    """Calculate ROI as percentage."""
    if total_volume == 0:
        return 0.0
    return (total_profit / total_volume) * 100


def calculate_metrics(wallet_address: str, trades: List[Dict], markets: List[Dict]) -> Dict:
    """
    Calculate all performance metrics for a wallet.
    Returns a dictionary with all metrics including category breakdown.
    """
    if not trades:
        return {
            "wallet_id": wallet_address,
            "total_positions": 0,
            "active_positions": 0,
            "total_wins": 0.0,
            "total_losses": 0.0,
            "win_rate_percent": 0.0,
            "win_count": 0,
            "loss_count": 0,
            "pnl": 0.0,
            "current_value": DEFAULT_CURRENT_VALUE,
            "final_score": 0.0,
            "categories": {}
        }
    
    # Track positions (unique market_id)
    positions = set()
    active_positions = set()
    
    total_wins = 0.0
    total_losses = 0.0
    total_volume = 0.0
    winning_trades = 0
    losing_trades = 0
    
    # Category breakdown
    category_stats = defaultdict(lambda: {
        "total_wins": 0.0,
        "total_losses": 0.0,
        "win_count": 0,
        "loss_count": 0,
        "pnl": 0.0
    })
    
    # Process each trade
    for trade in trades:
        market_id = trade.get("market_id") or trade.get("market") or trade.get("marketId")
        if not market_id:
            continue
        
        market = get_market_by_id(market_id, markets)
        if not market:
            continue
        
        # Check if market is resolved
        market_resolution = get_market_resolution(market_id, markets)
        is_resolved = market_resolution is not None
        
        # Track positions
        positions.add(market_id)
        if not is_resolved:
            active_positions.add(market_id)
        
        # Only calculate PnL for resolved markets
        if not market_resolution:
            continue
        
        size = float(trade.get("size") or trade.get("amount") or trade.get("quantity") or 0)
        if size == 0:
            continue
        
        total_volume += size
        
        # Calculate PnL
        pnl, is_win = calculate_trade_pnl(trade, market_resolution)
        
        if is_win:
            total_wins += pnl
            winning_trades += 1
        else:
            total_losses += pnl  # Already negative
            losing_trades += 1
        
        # Category breakdown
        category = get_market_category(market)
        if is_win:
            category_stats[category]["total_wins"] += pnl
            category_stats[category]["win_count"] += 1
        else:
            category_stats[category]["total_losses"] += pnl
            category_stats[category]["loss_count"] += 1
        category_stats[category]["pnl"] += pnl
    
    # Calculate overall metrics
    total_positions = len(positions)
    active_positions_count = len(active_positions)
    total_trades = winning_trades + losing_trades
    win_rate_percent = (winning_trades / total_trades * 100) if total_trades > 0 else 0.0
    overall_pnl = total_wins + total_losses
    
    # Calculate ROI
    roi = calculate_roi(overall_pnl, total_volume)
    
    # Calculate consistency and recency
    consistency = calculate_consistency(trades, markets)
    recency = calculate_recency(trades)
    
    # Calculate final score
    win_rate_normalized = win_rate_percent / 100.0
    roi_normalized = roi / 100.0 if roi != 0 else 0.0
    
    final_score = (
        ROI_WEIGHT * roi_normalized +
        WIN_RATE_WEIGHT * win_rate_normalized +
        CONSISTENCY_WEIGHT * consistency +
        RECENCY_WEIGHT * recency
    ) * 100  # Scale to 0-100
    
    # Format category stats
    categories = {}
    for category, stats in category_stats.items():
        cat_total_trades = stats["win_count"] + stats["loss_count"]
        cat_win_rate = (stats["win_count"] / cat_total_trades * 100) if cat_total_trades > 0 else 0.0
        categories[category] = {
            "total_wins": round(stats["total_wins"], 2),
            "total_losses": round(stats["total_losses"], 2),
            "win_rate_percent": round(cat_win_rate, 1),
            "pnl": round(stats["pnl"], 2)
        }
    
    return {
        "wallet_id": wallet_address,
        "total_positions": total_positions,
        "active_positions": active_positions_count,
        "total_wins": round(total_wins, 2),
        "total_losses": round(total_losses, 2),
        "win_rate_percent": round(win_rate_percent, 1),
        "win_count": winning_trades,
        "loss_count": losing_trades,
        "pnl": round(overall_pnl, 2),
        "current_value": DEFAULT_CURRENT_VALUE,
        "final_score": round(final_score, 1),
        "categories": categories
    }

