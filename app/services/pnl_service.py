"""User PnL service for saving and retrieving PnL data."""

from typing import List, Dict, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from app.db.models import UserPnL
from app.services.data_fetcher import fetch_user_pnl
from decimal import Decimal


async def save_pnl_to_db(
    session: AsyncSession,
    user_address: str,
    pnl_data: List[Dict],
    interval: str = "1m",
    fidelity: str = "1d"
) -> int:
    """
    Save PnL data to database. Updates existing records or inserts new ones.
    
    Args:
        session: Database session
        user_address: Wallet address
        pnl_data: List of PnL data dictionaries with 't' (timestamp) and 'p' (pnl) fields
        interval: Time interval
        fidelity: Data fidelity
    
    Returns:
        Number of PnL records saved
    """
    saved_count = 0
    
    for pnl_point in pnl_data:
        # Convert PnL data to database model
        pnl_dict = {
            "user_address": user_address,
            "timestamp": pnl_point.get("t", 0),
            "pnl": Decimal(str(pnl_point.get("p", 0))),
            "interval": interval,
            "fidelity": fidelity,
        }
        
        # Use PostgreSQL upsert (INSERT ... ON CONFLICT DO UPDATE)
        # Conflict on unique combination of user_address, timestamp, interval, and fidelity
        stmt = pg_insert(UserPnL).values(**pnl_dict)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_user_pnl_unique",
            set_={
                "pnl": stmt.excluded.pnl,
                "updated_at": stmt.excluded.updated_at,
            }
        )
        
        await session.execute(stmt)
        saved_count += 1
    
    return saved_count


async def get_pnl_from_db(
    session: AsyncSession,
    user_address: str,
    interval: Optional[str] = None,
    fidelity: Optional[str] = None,
    limit: Optional[int] = None
) -> List[UserPnL]:
    """
    Get PnL data from database for a user.
    
    Args:
        session: Database session
        user_address: Wallet address
        interval: Filter by interval (optional)
        fidelity: Filter by fidelity (optional)
        limit: Maximum number of records to return (optional)
    
    Returns:
        List of UserPnL objects, ordered by timestamp ascending
    """
    stmt = select(UserPnL).where(UserPnL.user_address == user_address)
    
    if interval:
        stmt = stmt.where(UserPnL.interval == interval)
    if fidelity:
        stmt = stmt.where(UserPnL.fidelity == fidelity)
    
    stmt = stmt.order_by(UserPnL.timestamp.asc())
    
    if limit:
        stmt = stmt.limit(limit)
    
    result = await session.execute(stmt)
    return result.scalars().all()


async def fetch_and_save_pnl(
    session: AsyncSession,
    user_address: str,
    interval: str = "1m",
    fidelity: str = "1d"
) -> tuple[List[Dict], int]:
    """
    Fetch PnL data from API and save to database.
    
    Args:
        session: Database session
        user_address: Wallet address
        interval: Time interval
        fidelity: Data fidelity
    
    Returns:
        Tuple of (pnl data list, saved count)
    """
    # Fetch PnL data from API
    pnl_data = await fetch_user_pnl(user_address, interval=interval, fidelity=fidelity)
    
    # Save to database
    saved_count = await save_pnl_to_db(session, user_address, pnl_data, interval=interval, fidelity=fidelity)
    
    return pnl_data, saved_count

