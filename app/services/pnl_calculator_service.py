"""PnL calculation service that aggregates data from database."""

import asyncio
from typing import Dict, List
from sqlalchemy.ext.asyncio import AsyncSession
from decimal import Decimal
from app.services.trade_service import get_trades_from_db, fetch_and_save_trades
from app.services.position_service import get_positions_from_db, fetch_and_save_positions
from app.services.activity_service import get_activities_from_db, fetch_and_save_activities
from app.db.models import Trade, Position, Activity


async def calculate_user_pnl(
    session: AsyncSession,
    wallet_address: str
) -> Dict:
    """
    Calculate comprehensive PnL for a user by aggregating data from database.
    
    This function:
    1. Fetches all trades from database (or API if not in DB)
    2. Fetches all positions from database (or API if not in DB)
    3. Fetches all activities from database (or API if not in DB)
    4. Calculates realized and unrealized PnL
    5. Includes rewards and redemptions
    
    Args:
        session: Database session
        wallet_address: Wallet address
    
    Returns:
        Dictionary with comprehensive PnL metrics
    """
    
    def safe_decimal(val) -> Decimal:
        """Helper to safely convert value to Decimal, handling None."""
        if val is None:
            return Decimal('0')
        return Decimal(str(val))

    # Fetch all data from database, or from API if not available
    trades = await get_trades_from_db(session, wallet_address)
    if not trades:
        # If no trades in DB, fetch from API and save
        try:
            trades_data, _ = await fetch_and_save_trades(session, wallet_address)
            # Yield control to ensure we're back in proper async context after thread execution
            await asyncio.sleep(0)
            # Expire all to clear session cache and ensure fresh query
            session.expire_all()
            # Re-query to get the saved trades
            trades = await get_trades_from_db(session, wallet_address)
        except Exception as e:
            # If API fetch fails, continue with empty trades
            # Log error but don't fail the entire calculation
            print(f"Warning: Failed to fetch trades from API: {e}")
            trades = []
    
    positions = await get_positions_from_db(session, wallet_address)
    if not positions:
        # If no positions in DB, fetch from API and save
        try:
            positions_data, _ = await fetch_and_save_positions(session, wallet_address)
            # Yield control to ensure we're back in proper async context after thread execution
            await asyncio.sleep(0)
            # Expire all to clear session cache and ensure fresh query
            session.expire_all()
            # Re-query to get the saved positions
            positions = await get_positions_from_db(session, wallet_address)
        except Exception as e:
            # If API fetch fails, continue with empty positions
            print(f"Warning: Failed to fetch positions from API: {e}")
            positions = []
    
    activities = await get_activities_from_db(session, wallet_address)
    if not activities:
        # If no activities in DB, fetch from API and save
        try:
            activities_data, _ = await fetch_and_save_activities(session, wallet_address)
            # Yield control to ensure we're back in proper async context after thread execution
            await asyncio.sleep(0)
            # Expire all to clear session cache and ensure fresh query
            session.expire_all()
            # Re-query to get the saved activities
            activities = await get_activities_from_db(session, wallet_address)
        except Exception as e:
            # If API fetch fails, continue with empty activities
            print(f"Warning: Failed to fetch activities from API: {e}")
            activities = []
    
    # Initialize PnL metrics
    total_invested = Decimal('0')
    total_realized_pnl = Decimal('0')
    total_unrealized_pnl = Decimal('0')
    total_rewards = Decimal('0')
    total_redemptions = Decimal('0')
    total_current_value = Decimal('0')
    
    # Calculate from positions
    # Calculate from positions
    for position in positions:
        # Add initial investment
        total_invested += safe_decimal(position.initial_value)
        
        # Add realized PnL from positions
        realized = safe_decimal(position.realized_pnl)
        total_realized_pnl += realized
        
        # Calculate unrealized PnL (cash_pnl - realized_pnl)
        # For closed positions, unrealized will be 0 since cash_pnl = realized_pnl
        cash_pnl = safe_decimal(position.cash_pnl)
        unrealized = cash_pnl - realized
        total_unrealized_pnl += unrealized
        
        # Add current value
        total_current_value += safe_decimal(position.current_value)
    
    # Calculate from activities (rewards and redemptions)
    for activity in activities:
        if activity.type == "REWARD":
            total_rewards += safe_decimal(activity.usdc_size)
        elif activity.type == "REDEEM":
            total_redemptions += safe_decimal(activity.usdc_size)
    
    # Calculate total PnL
    total_pnl = total_realized_pnl + total_unrealized_pnl + total_rewards - total_redemptions
    
    # Calculate PnL percentage
    pnl_percentage = Decimal('0')
    if total_invested > 0:
        pnl_percentage = (total_pnl / total_invested) * 100
    
    # Count statistics
    total_trades = len(trades)
    buy_trades = len([t for t in trades if t.side == "BUY"])
    sell_trades = len([t for t in trades if t.side == "SELL"])
    active_positions = len([p for p in positions if safe_decimal(p.current_value) > 0])
    closed_positions = len([p for p in positions if safe_decimal(p.current_value) == 0])
    
    # Calculate average trade size
    avg_trade_size = Decimal('0')
    if total_trades > 0:
        total_trade_size = sum(safe_decimal(t.size) for t in trades)
        avg_trade_size = total_trade_size / total_trades
    
    # Calculate key metrics from trades
    # 1. Total PnL from trades = sum of all trade PnLs
    total_trade_pnl = Decimal('0')
    total_stakes = Decimal('0')  # Sum of all stakes (size * price)
    winning_trades_count = 0
    total_trades_with_pnl = 0
    stakes_of_wins = Decimal('0')  # Sum of stakes for winning trades
    
    for trade in trades:
        # Calculate stake for this trade (initial value invested)
        size = safe_decimal(trade.size)
        price = safe_decimal(trade.price)
        stake = size * price
        total_stakes += stake
        
        # Only count trades with calculated PnL
        if trade.pnl is not None:
            total_trade_pnl += trade.pnl
            total_trades_with_pnl += 1
            
            # Check if this is a winning trade (pnl > 0)
            if trade.pnl > 0:
                winning_trades_count += 1
                stakes_of_wins += stake
    
    # 2. ROI = (Total PnL from trades / Total stakes) * 100
    roi = Decimal('0')
    if total_stakes > 0:
        roi = (total_trade_pnl / total_stakes) * 100
    
    # 3. Win Rate = (Winning trades ÷ Total trades) × 100
    win_rate = Decimal('0')
    if total_trades_with_pnl > 0:
        win_rate = (winning_trades_count / total_trades_with_pnl) * 100
    
    # 4. Stake-Weighted Win Rate = Sum(stakes of wins) ÷ Sum(stakes of all trades)
    stake_weighted_win_rate = Decimal('0')
    if total_stakes > 0:
        stake_weighted_win_rate = (stakes_of_wins / total_stakes) * 100
    
    return {
        "wallet_address": wallet_address,
        "total_invested": float(total_invested),
        "total_current_value": float(total_current_value),
        "total_realized_pnl": float(total_realized_pnl),
        "total_unrealized_pnl": float(total_unrealized_pnl),
        "total_rewards": float(total_rewards),
        "total_redemptions": float(total_redemptions),
        "total_pnl": float(total_pnl),
        "pnl_percentage": float(pnl_percentage),
        "key_metrics": {
            "total_trade_pnl": float(total_trade_pnl),
            "roi": float(roi),
            "win_rate": float(win_rate),
            "stake_weighted_win_rate": float(stake_weighted_win_rate),
            "winning_trades": winning_trades_count,
            "total_trades_with_pnl": total_trades_with_pnl,
            "total_stakes": float(total_stakes),
        },
        "statistics": {
            "total_trades": total_trades,
            "buy_trades": buy_trades,
            "sell_trades": sell_trades,
            "active_positions": active_positions,
            "closed_positions": closed_positions,
            "total_positions": len(positions),
            "avg_trade_size": float(avg_trade_size),
        },
        "breakdown": {
            "from_positions": {
                "realized_pnl": float(total_realized_pnl),
                "unrealized_pnl": float(total_unrealized_pnl),
            },
            "from_activities": {
                "rewards": float(total_rewards),
                "redemptions": float(total_redemptions),
            }
        }
    }

