"""Leaderboard service for ranking traders by various metrics."""

from typing import List, Dict, Optional
from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, distinct
from decimal import Decimal
from app.db.models import Trade, Position, Activity


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
    
    # Sort by win rate (descending)
    leaderboard.sort(key=lambda x: x['win_rate'], reverse=True)
    
    # Add rank
    for rank, trader in enumerate(leaderboard, 1):
        trader['rank'] = rank
    
    return leaderboard[:limit]

