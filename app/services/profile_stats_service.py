"""Profile stats service for saving and retrieving profile statistics."""

from typing import Optional, Dict
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from app.db.models import ProfileStats
from app.services.data_fetcher import fetch_profile_stats
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
    
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


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
    stats_data = fetch_profile_stats(proxy_address, username=username)
    
    if not stats_data:
        return None, None
    
    # Save to database
    saved_stats = await save_profile_stats_to_db(session, proxy_address, stats_data, username=username)
    
    return stats_data, saved_stats

