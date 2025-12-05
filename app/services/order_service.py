"""Order service for saving and retrieving orders."""

from typing import List, Dict, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from app.db.models import Order
from app.services.data_fetcher import fetch_orders_from_dome
from decimal import Decimal


async def save_orders_to_db(
    session: AsyncSession,
    orders: List[Dict]
) -> int:
    """
    Save orders to database. Updates existing orders or inserts new ones.
    
    Args:
        session: Database session
        orders: List of order dictionaries from API
    
    Returns:
        Number of orders saved
    """
    saved_count = 0
    
    for order_data in orders:
        # Convert order data to database model
        order_dict = {
            "token_id": str(order_data.get("token_id", "")),
            "token_label": order_data.get("token_label", ""),
            "side": order_data.get("side", ""),
            "market_slug": order_data.get("market_slug", ""),
            "condition_id": order_data.get("condition_id", ""),
            "shares": Decimal(str(order_data.get("shares", 0))),
            "price": Decimal(str(order_data.get("price", 0))),
            "tx_hash": order_data.get("tx_hash", ""),
            "title": order_data.get("title"),
            "timestamp": order_data.get("timestamp", 0),
            "order_hash": order_data.get("order_hash", ""),
            "user": order_data.get("user", ""),
            "taker": order_data.get("taker", ""),
            "shares_normalized": Decimal(str(order_data.get("shares_normalized", 0))),
        }
        
        # Use PostgreSQL upsert (INSERT ... ON CONFLICT DO UPDATE)
        # Conflict on unique order_hash
        stmt = pg_insert(Order).values(**order_dict)
        stmt = stmt.on_conflict_do_update(
            index_elements=["order_hash"],
            set_={
                "token_id": stmt.excluded.token_id,
                "token_label": stmt.excluded.token_label,
                "side": stmt.excluded.side,
                "market_slug": stmt.excluded.market_slug,
                "condition_id": stmt.excluded.condition_id,
                "shares": stmt.excluded.shares,
                "price": stmt.excluded.price,
                "tx_hash": stmt.excluded.tx_hash,
                "title": stmt.excluded.title,
                "timestamp": stmt.excluded.timestamp,
                "user": stmt.excluded.user,
                "taker": stmt.excluded.taker,
                "shares_normalized": stmt.excluded.shares_normalized,
                "updated_at": stmt.excluded.updated_at,
            }
        )
        
        await session.execute(stmt)
        saved_count += 1
    
    await session.commit()
    return saved_count


async def get_orders_from_db(
    session: AsyncSession,
    user: Optional[str] = None,
    market_slug: Optional[str] = None,
    side: Optional[str] = None,
    limit: int = 100
) -> List[Order]:
    """
    Get orders from database with optional filters.
    
    Args:
        session: Database session
        user: Filter by wallet address
        market_slug: Filter by market slug
        side: Filter by side (BUY/SELL)
        limit: Maximum number of orders to return
    
    Returns:
        List of Order objects
    """
    stmt = select(Order)
    
    if user:
        stmt = stmt.where(Order.user == user)
    if market_slug:
        stmt = stmt.where(Order.market_slug == market_slug)
    if side:
        stmt = stmt.where(Order.side == side)
    
    stmt = stmt.order_by(Order.timestamp.desc()).limit(limit)
    
    result = await session.execute(stmt)
    return result.scalars().all()


async def fetch_and_save_orders(
    session: AsyncSession,
    limit: int = 100,
    status: Optional[str] = None,
    market_slug: Optional[str] = None,
    user: Optional[str] = None
) -> tuple[List[Dict], int, Dict]:
    """
    Fetch orders from API and save to database.
    
    Args:
        session: Database session
        limit: Maximum number of orders to fetch
        status: Order status filter
        market_slug: Filter by market slug
        user: Filter by wallet address
    
    Returns:
        Tuple of (orders list, saved count, pagination info)
    """
    # Fetch orders from API
    data = fetch_orders_from_dome(limit=limit, status=status, market_slug=market_slug, user=user)
    
    orders = data.get("orders", [])
    pagination = data.get("pagination", {})
    
    # Save to database
    saved_count = await save_orders_to_db(session, orders)
    
    return orders, saved_count, pagination

