"""Profile stats service for saving and retrieving profile statistics."""

from typing import Optional, Dict, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from app.db.models import ProfileStats, Trade
from app.services.data_fetcher import fetch_profile_stats
from app.services.leaderboard_service import (
    calculate_trader_metrics_with_time_filter,
    calculate_scores_and_rank,
    get_unique_wallet_addresses
)
from decimal import Decimal


async def save_profile_stats_to_db(
    session: AsyncSession,
    proxy_address: str,
    stats_data: Dict,
    username: Optional[str] = None
) -> ProfileStats:
    """
    Save profile stats to database. Updates existing record or inserts new one.
    
    Args:
        session: Database session
        proxy_address: Wallet address
        stats_data: Dictionary with stats data from API
        username: Optional username
    
    Returns:
        ProfileStats object
    """
    # Convert stats data to database model
    stats_dict = {
        "proxy_address": proxy_address,
        "username": username,
        "trades": stats_data.get("trades", 0),
        "largest_win": Decimal(str(stats_data.get("largestWin", 0))),
        "views": stats_data.get("views", 0),
        "join_date": stats_data.get("joinDate"),
    }
    
    # Use PostgreSQL upsert (INSERT ... ON CONFLICT DO UPDATE)
    # Conflict on unique combination of proxy_address and username
    stmt = pg_insert(ProfileStats).values(**stats_dict)
    stmt = stmt.on_conflict_do_update(
        constraint="uq_profile_stats_unique",
        set_={
            "trades": stmt.excluded.trades,
            "largest_win": stmt.excluded.largest_win,
            "views": stmt.excluded.views,
            "join_date": stmt.excluded.join_date,
            "updated_at": stmt.excluded.updated_at,
        }
    )
    
    await session.execute(stmt)
    await session.commit()
    
    # Fetch and return the saved record
    # Handle potential duplicates by getting the first match (most recent)
    # Order by updated_at descending to get the most recent one in case of duplicates
    query = select(ProfileStats).where(
        ProfileStats.proxy_address == proxy_address
    )
    if username:
        query = query.where(ProfileStats.username == username)
    else:
        query = query.where(ProfileStats.username.is_(None))
    
    query = query.order_by(ProfileStats.updated_at.desc())
    
    result = await session.execute(query)
    # Use first() instead of scalar_one_or_none() to handle duplicates gracefully
    row = result.first()
    profile_stat = row[0] if row else None
    
    # If None (shouldn't happen after insert), try without username filter as fallback
    if profile_stat is None:
        query = select(ProfileStats).where(
            ProfileStats.proxy_address == proxy_address
        ).order_by(ProfileStats.updated_at.desc())
        result = await session.execute(query)
        row = result.first()
        if row:
            profile_stat = row[0]
    
    if profile_stat is None:
        raise ValueError(f"Failed to retrieve saved profile stats for {proxy_address}")
    
    return profile_stat


async def get_profile_stats_from_db(
    session: AsyncSession,
    proxy_address: str,
    username: Optional[str] = None
) -> Optional[ProfileStats]:
    """
    Get profile stats from database.
    
    Args:
        session: Database session
        proxy_address: Wallet address
        username: Optional username
    
    Returns:
        ProfileStats object or None if not found
    """
    stmt = select(ProfileStats).where(ProfileStats.proxy_address == proxy_address)
    if username:
        stmt = stmt.where(ProfileStats.username == username)
    else:
        stmt = stmt.where(ProfileStats.username.is_(None))
    
    # Order by updated_at descending to get the most recent one
    stmt = stmt.order_by(ProfileStats.updated_at.desc())
    
    result = await session.execute(stmt)
    row = result.first()
    return row[0] if row else None


async def fetch_and_save_profile_stats(
    session: AsyncSession,
    proxy_address: str,
    username: Optional[str] = None
) -> tuple[Optional[Dict], Optional[ProfileStats]]:
    """
    Fetch profile stats from API and save to database.
    
    Args:
        session: Database session
        proxy_address: Wallet address
        username: Optional username
    
    Returns:
        Tuple of (api response dict, saved ProfileStats object)
    """
    # Fetch profile stats from API
    stats_data = await fetch_profile_stats(proxy_address, username=username)
    
    if not stats_data:
        return None, None
    
    # Save to database
    saved_stats = await save_profile_stats_to_db(session, proxy_address, stats_data, username=username)
    
    return stats_data, saved_stats


def calculate_winning_streaks(trades: list) -> Tuple[int, int]:
    """
    Calculate longest and current winning streaks from trades.
    
    Args:
        trades: List of Trade objects sorted by timestamp (oldest first)
    
    Returns:
        Tuple of (longest_winning_streak, current_winning_streak)
    """
    if not trades:
        return 0, 0
    
    # Sort trades by timestamp (oldest first)
    sorted_trades = sorted(trades, key=lambda t: t.timestamp)
    
    longest_streak = 0
    current_streak = 0
    max_streak = 0
    
    for trade in sorted_trades:
        if trade.pnl is not None and trade.pnl > 0:
            # Winning trade
            current_streak += 1
            max_streak = max(max_streak, current_streak)
        else:
            # Losing trade or no PnL
            longest_streak = max(longest_streak, max_streak)
            current_streak = 0
            max_streak = 0
    
    # Final check for longest streak
    longest_streak = max(longest_streak, max_streak)
    
    return longest_streak, current_streak


def calculate_ranking_tag(final_score: float, total_traders: int, rank: Optional[int]) -> Tuple[str, float]:
    """
    Calculate ranking tag and top percent based on final score and rank.
    
    Args:
        final_score: Final score (0-100)
        total_traders: Total number of traders
        rank: Current rank (1-based)
    
    Returns:
        Tuple of (ranking_tag, top_percent)
    """
    if rank is None or total_traders == 0:
        # Fallback to score-based estimation
        if final_score >= 95:
            return "Top 1%", 1.0
        elif final_score >= 90:
            return "Top 5%", 5.0
        elif final_score >= 80:
            return "Top 10%", 10.0
        elif final_score >= 70:
            return "Top 25%", 25.0
        elif final_score >= 50:
            return "Top 50%", 50.0
        else:
            return "Below 50%", 75.0
    
    # Calculate top percent from rank
    top_percent = (rank / total_traders) * 100
    
    if top_percent <= 1:
        return "Top 1%", top_percent
    elif top_percent <= 5:
        return "Top 5%", top_percent
    elif top_percent <= 10:
        return "Top 10%", top_percent
    elif top_percent <= 25:
        return "Top 25%", top_percent
    elif top_percent <= 50:
        return "Top 50%", top_percent
    else:
        return "Below 50%", top_percent


async def get_enhanced_profile_stats(
    session: AsyncSession,
    wallet_address: str,
    username: Optional[str] = None
) -> Optional[Dict]:
    """
    Get enhanced profile stats with scoring, streaks, and all metrics.
    
    Args:
        session: Database session
        wallet_address: Wallet address
        username: Optional username
    
    Returns:
        Dictionary with enhanced profile stats or None if not found
    """
    # Get trader metrics
    metrics = await calculate_trader_metrics_with_time_filter(session, wallet_address, period='all')
    
    if not metrics:
        return None
    
    # Get trades for streak calculation
    from app.services.trade_service import get_trades_from_db
    trades = await get_trades_from_db(session, wallet_address)
    
    # Calculate winning streaks
    longest_streak, current_streak = calculate_winning_streaks(trades)
    
    # Get all traders for ranking
    all_wallets = await get_unique_wallet_addresses(session)
    all_metrics = []
    for wallet in all_wallets:
        try:
            trader_metrics = await calculate_trader_metrics_with_time_filter(session, wallet, period='all')
            if trader_metrics and trader_metrics.get('total_trades', 0) > 0:
                all_metrics.append(trader_metrics)
        except Exception:
            continue
    
    # Calculate scores for all traders
    if all_metrics:
        scored_traders = calculate_scores_and_rank(all_metrics)
        # Find current trader's rank
        scored_traders.sort(key=lambda x: x.get('final_score', 0), reverse=True)
        rank = None
        trader_found = False
        for idx, trader in enumerate(scored_traders, 1):
            if trader.get('wallet_address') == wallet_address:
                rank = idx
                # Update metrics with scored data
                metrics.update(trader)
                trader_found = True
                break
        
        # If trader not found in scored list, calculate score separately
        if not trader_found:
            scored = calculate_scores_and_rank([metrics])
            if scored:
                metrics.update(scored[0])
        
        total_traders = len(scored_traders)
    else:
        rank = None
        total_traders = 1
        # Calculate score for single trader
        scored = calculate_scores_and_rank([metrics])
        if scored:
            metrics.update(scored[0])
        else:
            # Set default values if scoring fails
            metrics['final_score'] = 0.0
    
    # Calculate ranking tag
    final_score = metrics.get('final_score', 0.0)
    ranking_tag, top_percent = calculate_ranking_tag(final_score, total_traders, rank)
    
    # Get profile stats from database (username is optional, so we don't filter by it)
    # Try with username first, then without if not found
    profile_stats = None
    if username:
        profile_stats = await get_profile_stats_from_db(session, wallet_address, username)
    if not profile_stats:
        profile_stats = await get_profile_stats_from_db(session, wallet_address, None)
    
    # Calculate average stake
    total_stakes = metrics.get('total_stakes', 0.0)
    total_trades_count = metrics.get('total_trades', 0)
    average_stake = (total_stakes / total_trades_count) if total_trades_count > 0 else 0.0
    
    # Find biggest win from trades
    biggest_win = 0.0
    for trade in trades:
        if trade.pnl is not None and trade.pnl > biggest_win:
            biggest_win = float(trade.pnl)
    
    # Build response
    return {
        "proxy_address": wallet_address,
        "username": username,
        "name": metrics.get('name'),
        "pseudonym": metrics.get('pseudonym'),
        "profile_image": metrics.get('profile_image'),
        "final_score": final_score,
        "top_percent": top_percent,
        "ranking_tag": ranking_tag,
        "longest_winning_streak": longest_streak,
        "current_winning_streak": current_streak,
        "biggest_win": biggest_win,
        "worst_loss": metrics.get('worst_loss', 0.0),
        "maximum_stake": metrics.get('max_stake', 0.0),
        "portfolio_value": metrics.get('portfolio_value', 0.0),
        "average_stake_value": average_stake,
        "rank": rank,
        "total_trades": total_trades_count,
        "total_pnl": metrics.get('total_pnl', 0.0),
        "roi": metrics.get('roi', 0.0),
        "win_rate": metrics.get('win_rate', 0.0),
    }


async def search_trader_by_username_or_wallet(
    session: AsyncSession,
    query: str
) -> Optional[str]:
    """
    Search for trader by username or wallet address.
    
    Args:
        session: Database session
        query: Search query (username or wallet address)
    
    Returns:
        Wallet address if found, None otherwise
    """
    # Check if it's a wallet address
    if query.startswith("0x") and len(query) == 42:
        # Validate wallet address
        try:
            int(query[2:], 16)
            return query
        except:
            pass
    
    # Search by pseudonym in trades
    stmt = select(Trade.proxy_wallet).where(
        Trade.pseudonym == query
    ).limit(1)
    
    result = await session.execute(stmt)
    row = result.first()
    if row:
        return row[0]
    
    # Search in profile stats
    stmt = select(ProfileStats.proxy_address).where(
        ProfileStats.username == query
    ).limit(1)
    
    result = await session.execute(stmt)
    row = result.first()
    if row:
        return row[0]
    
    return None

