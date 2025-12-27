from typing import Dict, Any, List, Optional
from sqlalchemy.future import select
from sqlalchemy import desc, func
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime

from app.db.models import (
    Trader, Position, ClosedPosition, Activity, 
    UserPnL, AggregatedMetrics, ProfileStats
)

async def get_db_dashboard_data(session: AsyncSession, wallet_address: str) -> Dict[str, Any]:
    """
    Aggregate all necessary data for the wallet dashboard from the local database.
    """
    # 1. Fetch Trader & Metrics
    stmt = select(Trader).where(Trader.wallet_address == wallet_address).order_by(desc(Trader.updated_at))
    result = await session.execute(stmt)
    trader = result.scalars().first()
    
    # 2. Fetch Profile Stats
    stmt = select(ProfileStats).where(ProfileStats.proxy_address == wallet_address).order_by(desc(ProfileStats.updated_at))
    result = await session.execute(stmt)
    profile_stats = result.scalars().first()
    
    # 3. Fetch Aggregated Metrics
    agg_metrics = None
    if trader:
        stmt = select(AggregatedMetrics).where(AggregatedMetrics.trader_id == trader.id).order_by(desc(AggregatedMetrics.updated_at))
        result = await session.execute(stmt)
        agg_metrics = result.scalars().first()

    # 4. Fetch Active Positions
    stmt = select(Position).where(Position.proxy_wallet == wallet_address)
    result = await session.execute(stmt)
    active_positions = result.scalars().all()
    
    # 5. Fetch Closed Positions
    stmt = select(ClosedPosition).where(ClosedPosition.proxy_wallet == wallet_address).order_by(ClosedPosition.timestamp.desc())
    result = await session.execute(stmt)
    closed_positions = result.scalars().all()
    
    # 6. Fetch Recent Activity
    stmt = select(Activity).where(Activity.proxy_wallet == wallet_address).order_by(Activity.timestamp.desc()).limit(500)
    result = await session.execute(stmt)
    activities = result.scalars().all()
    
    # 7. Fetch PnL History
    stmt = select(UserPnL).where(
        UserPnL.user_address == wallet_address,
        UserPnL.interval == "1m",
        UserPnL.fidelity == "1d"
    ).order_by(UserPnL.timestamp.asc())
    result = await session.execute(stmt)
    pnl_history = result.scalars().all()
    
    # --- derived calculations ---
    
    # Username Fallback: Try Activity table if name/pseudonym missing from Trader/Profile
    username = trader.name if trader and trader.name else (profile_stats.username if profile_stats and profile_stats.username else "Unknown")
    if username == "Unknown" and activities:
        # Check first few activities for name or pseudonym
        for a in activities:
            if a.name:
                username = a.name
                break
            if a.pseudonym:
                username = a.pseudonym
                break

    # Portfolio Value calculation
    # 1. Use aggregated value if available (now includes cash via sync)
    # 2. Fallback to positions sum
    portfolio_value = float(agg_metrics.portfolio_value) if agg_metrics and agg_metrics.portfolio_value else sum(float(p.current_value or 0) for p in active_positions)
    
    # Largest Win / Worst Loss / Realized PnL from Closed Positions
    largest_win = 0.0
    worst_loss = 0.0
    realized_pnl_total = 0.0
    
    for cp in closed_positions:
        pnl = float(cp.realized_pnl or 0)
        realized_pnl_total += pnl
        if pnl > largest_win:
            largest_win = pnl
        if pnl < worst_loss:
            worst_loss = pnl
            
    # Also check profile stats for largest win if available
    if profile_stats and profile_stats.largest_win:
        if float(profile_stats.largest_win) > largest_win:
            largest_win = float(profile_stats.largest_win)

    # Total Investment (Volume) Fallback
    total_investment = float(agg_metrics.total_volume) if agg_metrics and agg_metrics.total_volume else 0.0
    if total_investment == 0:
        # Sum of cost basis for all trades/positions
        # This is a rough estimation of "total volume" if agg_metrics is empty
        total_investment = sum(float(cp.total_bought or 0) for cp in closed_positions) + sum(float(p.initial_value or 0) for p in active_positions)

    # ROI Calculation: ((realized_pnl + unrealized_pnl) / total_investment) * 100
    unrealized_pnl = sum(float(p.cash_pnl or 0) for p in active_positions)
    total_pnl = float(agg_metrics.total_pnl) if agg_metrics and agg_metrics.total_pnl else (realized_pnl_total + unrealized_pnl)
    roi = (total_pnl / total_investment * 100) if total_investment > 0 else 0.0

    # Win Rate from closed positions
    total_closed = len(closed_positions)
    wins = sum(1 for cp in closed_positions if (cp.realized_pnl or 0) > 0)
    win_rate = (wins / total_closed * 100) if total_closed > 0 else 0.0
    
    # Construct Response Objects matching Frontend Expectations
    
    # ProfileStatsResponse
    profile_data = {
        "username": username,
        "trades": profile_stats.trades if profile_stats else len(closed_positions),
        "largestWin": largest_win,
        "views": profile_stats.views if profile_stats else 0,
        "joinDate": profile_stats.join_date if profile_stats and profile_stats.join_date else None,
    }
    
    # UserLeaderboardData
    leaderboard_data = {
        "address": wallet_address,
        "userName": username,
        "profileImage": trader.profile_image if trader else None,
        "vol": total_investment,
        "pnl": total_pnl,
        "rank": 0,
        "verifiedBadge": False,
        "xUsername": None
    }
    
    # PortfolioStats
    portfolio_data = {
        "performance_metrics": {
            "portfolio_value": portfolio_value,
            "total_pnl": total_pnl,
            "realized_pnl": realized_pnl_total,
            "unrealized_pnl": unrealized_pnl,
            "roi": roi,
            "total_investment": total_investment,
            "win_rate": win_rate,
            "worst_loss": worst_loss
        },
        "positions_summary": {
            "open_positions_count": len(active_positions),
            "closed_positions_count": len(closed_positions)
        }
    }
    
    # TradeHistory (for graph)
    trade_history_data = {
        "trades": [
            {"timestamp": int(p.timestamp), "pnl": float(p.pnl)}
            for p in pnl_history
        ]
    }
    
    if not trade_history_data["trades"] and closed_positions:
         trade_history_data["trades"] = [
            {"timestamp": int(cp.timestamp), "pnl": float(cp.realized_pnl or 0)}
            for cp in closed_positions
         ][:20] # Limit fallback trades for graph performance

    return {
        "profile": profile_data,
        "leaderboard": leaderboard_data,
        "portfolio": portfolio_data,
        "positions": [row_to_dict(p) for p in active_positions],
        "closed_positions": [row_to_dict(cp) for cp in closed_positions],
        "activities": [row_to_dict(a) for a in activities],
        "trade_history": trade_history_data
    }

def row_to_dict(obj):
    """Helper to convert SQLAlchemy model to dict."""
    d = {}
    for column in obj.__table__.columns:
        val = getattr(obj, column.name)
        if isinstance(val, (datetime)):
             val = val.isoformat()
        d[column.name] = val
    return d
