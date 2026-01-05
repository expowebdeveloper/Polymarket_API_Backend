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
    RECENCY_DAYS,
    RESOLUTION_YES,
    RESOLUTION_NO,
)
import math

def log_interpolate(x: float, x_min: float, x_max: float, s_min: float, s_max: float) -> float:
    """
    Interpolate score using logarithmic scale keys.
    f(x) = s_min + (s_max - s_min) * (ln(x) - ln(x_min)) / (ln(x_max) - ln(x_min))
    """
    if x <= x_min: return s_min
    if x >= x_max: return s_max
    
    # Use math.log for natural logarithm
    try:
        log_x = math.log(x)
        log_x_min = math.log(x_min)
        log_x_max = math.log(x_max)
        
        return s_min + (s_max - s_min) * (log_x - log_x_min) / (log_x_max - log_x_min)
    except ValueError:
        return s_min

def calculate_pnl_score(pnl: float) -> float:
    """
    Calculate deterministic PnL score based on log-interpolated bands.
    Output range: [0, 1]
    """
    
    # 1. Handle Profits (PnL >= 0)
    if pnl >= 0:
        # 0 - 100: f(PnL; 1, 100, 0.15 -> 0.25)
        # Note: log(0) is undefined, so we treat 0 as 1 for log scale or handle 0 separately
        if pnl < 1:
            # Linear interpolation for tiny profit 0-1 to avoid log(0)
            return 0.15 + (0.25 - 0.15) * (pnl / 100.0) 
            
        if pnl < 100:
            return log_interpolate(pnl, 1, 100, 0.15, 0.25)
        elif pnl < 1000:
            return log_interpolate(pnl, 100, 1000, 0.25, 0.40)
        elif pnl < 5000:
            return log_interpolate(pnl, 1000, 5000, 0.40, 0.60)
        elif pnl < 10000:
            return log_interpolate(pnl, 5000, 10000, 0.60, 0.75)
        elif pnl < 50000:
            return log_interpolate(pnl, 10000, 50000, 0.75, 0.85)
        elif pnl < 100000:
            return log_interpolate(pnl, 50000, 100000, 0.85, 0.92)
        elif pnl < 500000:
            return log_interpolate(pnl, 100000, 500000, 0.92, 0.98)
        elif pnl < 1000000:
            return log_interpolate(pnl, 500000, 1000000, 0.98, 0.999)
        else:
            return 1.00

    # 2. Handle Losses (PnL < 0)
    else:
        abs_pnl = abs(pnl)
        
        # Missing ranges from prompt served as:
        # 0 to -100 (0.15 -> ?)
        # -100 to -1000 (? -> ?)
        # -1000 to -10000 (? -> 0.05)
        
        # We assume a smooth degradation from 0.15 down to 0.05 over the range -1 to -10000
        # This penalizes losses.
        
        if abs_pnl < 100:
             # -100 < PnL < 0: Score 0.15 -> 0.12
             return log_interpolate(abs_pnl, 1, 100, 0.15, 0.12)
        elif abs_pnl < 1000:
             # -1000 < PnL <= -100: Score 0.12 -> 0.08
             return log_interpolate(abs_pnl, 100, 1000, 0.12, 0.08)
        elif abs_pnl < 10000:
             # -10000 < PnL <= -1000: Score 0.08 -> 0.05
             return log_interpolate(abs_pnl, 1000, 10000, 0.08, 0.05)
        else:
             # PnL < -10,000 (abs_pnl > 10,000)
             # Formula: 0.05 * (1 - (ln(|PnL|) - ln(10,000)) / (ln(1,000,000) - ln(10,000)))
             try:
                 ln_pnl = math.log(abs_pnl)
                 ln_10k = math.log(10000)
                 ln_1m = math.log(1000000)
                 
                 term = (ln_pnl - ln_10k) / (ln_1m - ln_10k)
                 score = 0.05 * (1.0 - term)
                 return max(0.0, score) # Clamp to >= 0
             except:
                 return 0.0


def calculate_trade_pnl(trade: Dict, market_resolution: str) -> Tuple[float, bool]:
    """
    Calculate profit/loss for a single trade.
    Returns (pnl_amount, is_win) tuple.
    
    For winning trades: profit = size * (1 - price)
    For losing trades: loss = -size
    """
    # Extract outcome/side - Dome orders use "side" field (BUY/SELL)
    # For binary markets: BUY = YES outcome, SELL = NO outcome
    side = (
        trade.get("side") or
        trade.get("outcome") or 
        trade.get("outcomeYes") or
        trade.get("outcome_yes") or
        trade.get("outcomeIndex") or
        trade.get("outcome_index") or
        trade.get("position")
    )
    
    # Handle boolean outcome
    if isinstance(side, bool):
        side = "YES" if side else "NO"
    
    if not side:
        return 0.0, False
    
    side = str(side).upper()
    
    # Extract size - Dome orders use "shares" field (normalized or raw)
    # shares_normalized is in dollars, shares is in raw units (divide by 1e6 to normalize)
    raw_shares = trade.get("shares")
    if raw_shares:
        try:
            size = float(trade.get("shares_normalized") or (float(raw_shares) / 1e6))
        except:
            size = float(trade.get("shares_normalized") or 0)
    else:
        size = float(
            trade.get("shares_normalized") or 
            trade.get("size") or 
            trade.get("amount") or 
            trade.get("quantity") or
            trade.get("orderSize") or
            trade.get("order_size") or
            trade.get("filledSize") or
            trade.get("filled_size") or
            trade.get("volume") or
            0
        )
    
    # Extract price - Dome orders may use different field names
    price = float(
        trade.get("price") or 
        trade.get("fillPrice") or 
        trade.get("fill_price") or
        trade.get("avgPrice") or
        trade.get("avg_price") or
        trade.get("executionPrice") or
        trade.get("execution_price") or
        0
    )
    
    if size == 0:
        return 0.0, False
    
    # For Polymarket binary markets:
    # - BUY side = buying YES shares
    # - SELL side = selling YES shares (or buying NO shares)
    # If market resolves YES: BUY wins, SELL loses
    # If market resolves NO: BUY loses, SELL wins
    
    is_win = False
    if side == "BUY":
        # BUY means buying YES shares
        is_win = (market_resolution == RESOLUTION_YES)
    elif side == "SELL":
        # SELL means selling YES shares (or buying NO)
        is_win = (market_resolution == RESOLUTION_NO)
    else:
        # Fallback for other outcome formats (YES/NO, 1/0, etc.)
        if market_resolution == RESOLUTION_YES and (side == RESOLUTION_YES or side == "1" or side == "TRUE"):
            is_win = True
        elif market_resolution == RESOLUTION_NO and (side == RESOLUTION_NO or side == "0" or side == "FALSE"):
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


async def calculate_consistency(trades: List[Dict], markets: List[Dict]) -> float:
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
        market_id = (
            trade.get("market_id") or 
            trade.get("market") or 
            trade.get("marketId") or
            trade.get("market_slug") or
            trade.get("marketSlug") or
            trade.get("slug")
        )
        if not market_id:
            continue
        
        market_resolution = await get_market_resolution(market_id, markets)
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


async def calculate_metrics(wallet_address: str, trades: List[Dict], markets: List[Dict]) -> Dict:
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
    trades_processed = 0
    trades_skipped_no_market_id = 0
    trades_skipped_market_not_found = 0
    trades_skipped_unresolved = 0
    trades_skipped_no_size = 0
    
    for trade in trades:
        # Extract market identifier - Dome orders may use market_slug, marketSlug, etc.
        market_id = (
            trade.get("market_id") or 
            trade.get("market") or 
            trade.get("marketId") or
            trade.get("market_slug") or
            trade.get("marketSlug") or
            trade.get("slug")
        )
        
        if not market_id:
            trades_skipped_no_market_id += 1
            continue
        
        # Track positions even if market not found (so we know total unique markets traded)
        positions.add(market_id)
        
        market = get_market_by_id(market_id, markets)
        if not market:
            trades_skipped_market_not_found += 1
            # Market not in our resolved markets list - treat as active/unresolved
            active_positions.add(market_id)
            continue
        
        # Check if market is resolved
        market_resolution = await get_market_resolution(market_id, markets)
        is_resolved = market_resolution is not None
        
        if not is_resolved:
            active_positions.add(market_id)
        
        # Only calculate PnL for resolved markets
        if not market_resolution:
            trades_skipped_unresolved += 1
            continue
        
        # Extract size/amount - Dome orders use "shares" field (normalized or raw)
        # shares_normalized is in dollars, shares is in raw units
        size = float(
            trade.get("shares_normalized") or  # Preferred: already normalized
            (float(trade.get("shares") or 0) / 1e6) or  # Convert raw shares to normalized (divide by 1e6)
            trade.get("size") or 
            trade.get("amount") or 
            trade.get("quantity") or
            trade.get("orderSize") or
            trade.get("order_size") or
            trade.get("filledSize") or
            trade.get("filled_size") or
            trade.get("volume") or
            0
        )
        if size == 0:
            trades_skipped_no_size += 1
            continue
        
        trades_processed += 1
        
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
    
    # Debug logging
    if trades_processed == 0 and len(trades) > 0:
        print(f"⚠ Warning: Processed 0 trades out of {len(trades)} for wallet {wallet_address}")
        print(f"  - {trades_skipped_no_market_id} skipped: no market_id")
        print(f"  - {trades_skipped_market_not_found} skipped: market not found")
        print(f"  - {trades_skipped_unresolved} skipped: market unresolved")
        print(f"  - {trades_skipped_no_size} skipped: no size/amount")
        if len(trades) > 0:
            # Show sample trade structure for debugging
            sample_trade = trades[0]
            print(f"  Sample trade keys: {list(sample_trade.keys())[:15]}")
            market_id_sample = (
                sample_trade.get("market_id") or 
                sample_trade.get("market_slug") or 
                sample_trade.get("slug") or
                sample_trade.get("market") or
                sample_trade.get("marketId")
            )
            print(f"  Sample trade market identifier: {market_id_sample}")
            print(f"  Sample trade size fields: shares={sample_trade.get('shares')}, shares_normalized={sample_trade.get('shares_normalized')}, size={sample_trade.get('size')}")
            print(f"  Sample trade side: {sample_trade.get('side')}")
            # Check if market exists in our markets list
            if market_id_sample:
                found_market = get_market_by_id(market_id_sample, markets)
                if found_market:
                    resolution = await get_market_resolution(market_id_sample, markets)
                    print(f"  Market found: {found_market.get('slug') or found_market.get('id')}, resolution: {resolution}")
                else:
                    print(f"  Market NOT found in {len(markets)} markets")
    elif trades_processed > 0:
        print(f"✓ Processed {trades_processed} trades with PnL calculation for wallet {wallet_address}")
        print(f"  - {trades_skipped_unresolved} trades skipped (unresolved markets)")
        print(f"  - {trades_skipped_market_not_found} trades skipped (market not found)")
    else:
        # Even if we processed some, show summary
        if len(trades) > 0:
            print(f"ℹ Summary for wallet {wallet_address}: {trades_processed} processed, {trades_skipped_unresolved} unresolved, {trades_skipped_market_not_found} not found")
    
    # Calculate overall metrics
    total_positions = len(positions)
    active_positions_count = len(active_positions)
    total_trades = winning_trades + losing_trades
    win_rate_percent = (winning_trades / total_trades * 100) if total_trades > 0 else 0.0
    overall_pnl = total_wins + total_losses
    
    # Calculate ROI
    roi = calculate_roi(overall_pnl, total_volume)
    
    # Calculate consistency and recency
    consistency = await calculate_consistency(trades, markets)
    recency = calculate_recency(trades)
    
    
    # Calculate final score using NEW Deterministic PnL Scoring
    pnl_score = calculate_pnl_score(overall_pnl)
    
    # Multiply by 100 for 0-100 scale compliance with frontend
    final_score = min(100.0, max(0.0, pnl_score * 100.0))
    
    # Previous Scoring System (Deprecated)
    # win_rate_normalized = win_rate_percent / 100.0
    # roi_normalized = roi / 100.0 if roi != 0 else 0.0
    # 
    # final_score = (
    #     ROI_WEIGHT * roi_normalized +
    #     WIN_RATE_WEIGHT * win_rate_normalized +
    #     CONSISTENCY_WEIGHT * consistency +
    #     RECENCY_WEIGHT * recency
    # ) * 100  # Scale to 0-100
    
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

