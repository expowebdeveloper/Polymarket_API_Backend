"""Trade history service for calculating comprehensive trade metrics."""

from typing import List, Dict, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text
from decimal import Decimal
from collections import defaultdict

from app.db.models import Trade, Position, ClosedPosition
from app.services.data_fetcher import fetch_market_by_slug_from_dome, get_market_category
from app.services.scoring_engine import calculate_metrics
from app.core.constants import DEFAULT_CATEGORY


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
    
    # Calculate overall metrics
    overall_metrics = calculate_overall_metrics(trades_list, open_positions_list, closed_positions_list)
    
    # Calculate category breakdown
    category_breakdown = calculate_category_breakdown(trades_list, open_positions_list, closed_positions_list)
    
    return {
        "wallet_address": wallet_address,
        "open_positions": open_positions_list,
        "closed_positions": closed_positions_list,
        "trades": trades_list,
        "overall_metrics": overall_metrics,
        "category_breakdown": category_breakdown,
    }


def calculate_overall_metrics(
    trades: List[Dict],
    open_positions: List[Dict],
    closed_positions: List[Dict]
) -> Dict:
    """Calculate overall ROI, PnL, Win Rate, and Score."""
    # Calculate realized PnL from closed positions and trades with PnL
    total_realized_pnl = 0.0
    total_unrealized_pnl = 0.0
    total_volume = 0.0
    
    # From closed positions
    for pos in closed_positions:
        if pos.get("realized_pnl"):
            total_realized_pnl += pos["realized_pnl"]
    
    # From trades with calculated PnL
    winning_trades = 0
    losing_trades = 0
    for trade in trades:
        if trade.get("pnl") is not None:
            pnl = trade["pnl"]
            total_realized_pnl += pnl
            if pnl > 0:
                winning_trades += 1
            elif pnl < 0:
                losing_trades += 1
        
        # Calculate volume (cost basis)
        if trade.get("entry_price") and trade.get("size"):
            total_volume += trade["entry_price"] * trade["size"]
        elif trade.get("price") and trade.get("size"):
            total_volume += trade["price"] * trade["size"]
    
    # From open positions (unrealized)
    for pos in open_positions:
        if pos.get("cash_pnl"):
            total_unrealized_pnl += pos["cash_pnl"]
    
    total_pnl = total_realized_pnl + total_unrealized_pnl
    
    # Calculate ROI
    roi = (total_pnl / total_volume * 100) if total_volume > 0 else 0.0
    
    # Calculate Win Rate
    total_trades_with_pnl = winning_trades + losing_trades
    win_rate = (winning_trades / total_trades_with_pnl * 100) if total_trades_with_pnl > 0 else 0.0
    
    # Calculate Score (simplified - can be enhanced with scoring engine)
    # Score = weighted combination of ROI, Win Rate, and PnL
    roi_score = min(max(roi / 100.0, -1.0), 1.0)  # Normalize ROI to -1 to 1
    win_rate_score = win_rate / 100.0  # Normalize Win Rate to 0 to 1
    pnl_score = min(max(total_pnl / 10000.0, -1.0), 1.0) if total_pnl != 0 else 0.0  # Normalize PnL
    
    score = (0.4 * roi_score + 0.3 * win_rate_score + 0.3 * pnl_score) * 100
    score = max(0, min(100, score))  # Clamp to 0-100
    
    return {
        "total_pnl": round(total_pnl, 2),
        "realized_pnl": round(total_realized_pnl, 2),
        "unrealized_pnl": round(total_unrealized_pnl, 2),
        "roi": round(roi, 2),
        "win_rate": round(win_rate, 2),
        "winning_trades": winning_trades,
        "losing_trades": losing_trades,
        "total_trades": total_trades_with_pnl,
        "score": round(score, 2),
        "total_volume": round(total_volume, 2),
    }


def calculate_category_breakdown(
    trades: List[Dict],
    open_positions: List[Dict],
    closed_positions: List[Dict]
) -> Dict[str, Dict]:
    """Calculate ROI, PnL, Win Rate, and Score broken down by category."""
    category_stats = defaultdict(lambda: {
        "total_pnl": 0.0,
        "realized_pnl": 0.0,
        "unrealized_pnl": 0.0,
        "total_volume": 0.0,
        "winning_trades": 0,
        "losing_trades": 0,
        "total_trades": 0,
    })
    
    # Process trades
    for trade in trades:
        category = trade.get("category", DEFAULT_CATEGORY)
        
        if trade.get("pnl") is not None:
            pnl = trade["pnl"]
            category_stats[category]["realized_pnl"] += pnl
            category_stats[category]["total_pnl"] += pnl
            if pnl > 0:
                category_stats[category]["winning_trades"] += 1
            elif pnl < 0:
                category_stats[category]["losing_trades"] += 1
            category_stats[category]["total_trades"] += 1
        
        # Calculate volume
        if trade.get("entry_price") and trade.get("size"):
            category_stats[category]["total_volume"] += trade["entry_price"] * trade["size"]
        elif trade.get("price") and trade.get("size"):
            category_stats[category]["total_volume"] += trade["price"] * trade["size"]
    
    # Process closed positions
    for pos in closed_positions:
        category = pos.get("category", DEFAULT_CATEGORY)
        if pos.get("realized_pnl"):
            category_stats[category]["realized_pnl"] += pos["realized_pnl"]
            category_stats[category]["total_pnl"] += pos["realized_pnl"]
    
    # Process open positions
    for pos in open_positions:
        category = pos.get("category", DEFAULT_CATEGORY)
        if pos.get("cash_pnl"):
            category_stats[category]["unrealized_pnl"] += pos["cash_pnl"]
            category_stats[category]["total_pnl"] += pos["cash_pnl"]
    
    # Calculate metrics for each category
    breakdown = {}
    for category, stats in category_stats.items():
        total_pnl = stats["total_pnl"]
        total_volume = stats["total_volume"]
        winning_trades = stats["winning_trades"]
        losing_trades = stats["losing_trades"]
        total_trades = stats["total_trades"]
        
        # Calculate ROI
        roi = (total_pnl / total_volume * 100) if total_volume > 0 else 0.0
        
        # Calculate Win Rate
        win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0.0
        
        # Calculate Score
        roi_score = min(max(roi / 100.0, -1.0), 1.0)
        win_rate_score = win_rate / 100.0
        pnl_score = min(max(total_pnl / 10000.0, -1.0), 1.0) if total_pnl != 0 else 0.0
        
        score = (0.4 * roi_score + 0.3 * win_rate_score + 0.3 * pnl_score) * 100
        score = max(0, min(100, score))
        
        breakdown[category] = {
            "roi": round(roi, 2),
            "pnl": round(total_pnl, 2),
            "realized_pnl": round(stats["realized_pnl"], 2),
            "unrealized_pnl": round(stats["unrealized_pnl"], 2),
            "win_rate": round(win_rate, 2),
            "winning_trades": winning_trades,
            "losing_trades": losing_trades,
            "total_trades": total_trades,
            "score": round(score, 2),
            "total_volume": round(total_volume, 2),
        }
    
    return breakdown

