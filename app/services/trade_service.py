"""Trade service for saving and retrieving trades."""

from typing import List, Dict, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text
from sqlalchemy.orm import defer
from sqlalchemy.dialects.postgresql import insert as pg_insert
from app.db.models import Trade
from app.services.data_fetcher import fetch_user_trades
from decimal import Decimal


async def save_trades_to_db(
    session: AsyncSession,
    wallet_address: str,
    trades: List[Dict]
) -> int:
    """
    Save trades to database. Updates existing trades or inserts new ones.
    
    Args:
        session: Database session
        wallet_address: Wallet address
        trades: List of trade dictionaries from API
    
    Returns:
        Number of trades saved
    """
    saved_count = 0
    
    for trade_data in trades:
        # Convert trade data to database model
        trade_dict = {
            "proxy_wallet": trade_data.get("proxyWallet", wallet_address),
            "side": trade_data.get("side", ""),
            "asset": str(trade_data.get("asset", "")),
            "condition_id": trade_data.get("conditionId", ""),
            "size": Decimal(str(trade_data.get("size", 0))),
            "price": Decimal(str(trade_data.get("price", 0))),
            "timestamp": trade_data.get("timestamp", 0),
            "title": trade_data.get("title"),
            "slug": trade_data.get("slug"),
            "icon": trade_data.get("icon"),
            "event_slug": trade_data.get("eventSlug"),
            "outcome": trade_data.get("outcome"),
            "outcome_index": trade_data.get("outcomeIndex"),
            "name": trade_data.get("name"),
            "pseudonym": trade_data.get("pseudonym"),
            "bio": trade_data.get("bio"),
            "profile_image": trade_data.get("profileImage"),
            "profile_image_optimized": trade_data.get("profileImageOptimized"),
            "transaction_hash": trade_data.get("transactionHash", ""),
        }
        
        # Use PostgreSQL upsert (INSERT ... ON CONFLICT DO UPDATE)
        # Conflict on unique combination of proxy_wallet, transaction_hash, timestamp, and asset
        stmt = pg_insert(Trade).values(**trade_dict)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_trade_unique",
            set_={
                "side": stmt.excluded.side,
                "size": stmt.excluded.size,
                "price": stmt.excluded.price,
                "title": stmt.excluded.title,
                "slug": stmt.excluded.slug,
                "icon": stmt.excluded.icon,
                "event_slug": stmt.excluded.event_slug,
                "outcome": stmt.excluded.outcome,
                "outcome_index": stmt.excluded.outcome_index,
                "name": stmt.excluded.name,
                "pseudonym": stmt.excluded.pseudonym,
                "bio": stmt.excluded.bio,
                "profile_image": stmt.excluded.profile_image,
                "profile_image_optimized": stmt.excluded.profile_image_optimized,
                "updated_at": stmt.excluded.updated_at,
            }
        )
        
        await session.execute(stmt)
        saved_count += 1
    
    await session.commit()
    return saved_count


async def get_trades_from_db(
    session: AsyncSession,
    wallet_address: str,
    side: Optional[str] = None,
    limit: Optional[int] = None
) -> List[Trade]:
    """
    Get trades from database for a wallet address.
    
    Args:
        session: Database session
        wallet_address: Wallet address
        side: Filter by side (BUY/SELL) - optional
        limit: Maximum number of trades to return - optional
    
    Returns:
        List of Trade objects, ordered by timestamp descending
    """
    # Query using raw SQL to handle missing columns gracefully
    # First check which optional columns exist
    check_sql = text("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'trades' 
        AND column_name IN ('entry_price', 'exit_price', 'pnl', 'trader_id')
    """)
    check_result = await session.execute(check_sql)
    existing_columns = {row[0] for row in check_result.all()}
    
    # Build SQL query with only existing columns
    base_columns = [
        "id", "proxy_wallet", "side", "asset", "condition_id", "size", "price",
        "timestamp", "title", "slug", "icon", "event_slug", "outcome", "outcome_index",
        "name", "pseudonym", "bio", "profile_image", "profile_image_optimized",
        "transaction_hash", "created_at", "updated_at"
    ]
    
    # Add optional columns if they exist
    if 'entry_price' in existing_columns:
        base_columns.append("entry_price")
    if 'exit_price' in existing_columns:
        base_columns.append("exit_price")
    if 'pnl' in existing_columns:
        base_columns.append("pnl")
    
    sql = f"SELECT {', '.join(base_columns)} FROM trades WHERE proxy_wallet = :wallet_address"
    
    params = {"wallet_address": wallet_address}
    
    if side:
        sql += " AND side = :side"
        params["side"] = side.upper()
    
    sql += " ORDER BY timestamp DESC"
    
    if limit:
        sql += " LIMIT :limit"
        params["limit"] = limit
    
    result = await session.execute(text(sql), params)
    rows = result.all()
    
    # Convert rows to Trade objects
    trades = []
    for row in rows:
        trade = Trade()
        idx = 0
        # Base columns (always present)
        trade.id = row[idx]
        idx += 1
        trade.proxy_wallet = row[idx]
        idx += 1
        trade.side = row[idx]
        idx += 1
        trade.asset = row[idx]
        idx += 1
        trade.condition_id = row[idx]
        idx += 1
        trade.size = row[idx]
        idx += 1
        trade.price = row[idx]
        idx += 1
        trade.timestamp = row[idx]
        idx += 1
        trade.title = row[idx] if idx < len(row) else None
        idx += 1
        trade.slug = row[idx] if idx < len(row) else None
        idx += 1
        trade.icon = row[idx] if idx < len(row) else None
        idx += 1
        trade.event_slug = row[idx] if idx < len(row) else None
        idx += 1
        trade.outcome = row[idx] if idx < len(row) else None
        idx += 1
        trade.outcome_index = row[idx] if idx < len(row) else None
        idx += 1
        trade.name = row[idx] if idx < len(row) else None
        idx += 1
        trade.pseudonym = row[idx] if idx < len(row) else None
        idx += 1
        trade.bio = row[idx] if idx < len(row) else None
        idx += 1
        trade.profile_image = row[idx] if idx < len(row) else None
        idx += 1
        trade.profile_image_optimized = row[idx] if idx < len(row) else None
        idx += 1
        trade.transaction_hash = row[idx] if idx < len(row) else None
        idx += 1
        trade.created_at = row[idx] if idx < len(row) else None
        idx += 1
        trade.updated_at = row[idx] if idx < len(row) else None
        idx += 1
        
        # Optional columns (set to None if they don't exist)
        if 'entry_price' in existing_columns:
            trade.entry_price = row[idx] if idx < len(row) else None
            idx += 1
        else:
            trade.entry_price = None
            
        if 'exit_price' in existing_columns:
            trade.exit_price = row[idx] if idx < len(row) else None
            idx += 1
        else:
            trade.exit_price = None
            
        if 'pnl' in existing_columns:
            trade.pnl = row[idx] if idx < len(row) else None
        else:
            trade.pnl = None
        
        trades.append(trade)
    
    return trades


async def fetch_and_save_trades(
    session: AsyncSession,
    wallet_address: str
) -> tuple[List[Dict], int]:
    """
    Fetch trades from API and save to database.
    
    Args:
        session: Database session
        wallet_address: Wallet address
    
    Returns:
        Tuple of (trades list, saved count)
    """
    # Fetch trades from API (run in thread pool to avoid blocking async event loop)
    import asyncio
    trades = await asyncio.to_thread(fetch_user_trades, wallet_address)
    
    # Save to database
    saved_count = await save_trades_to_db(session, wallet_address, trades)
    
    return trades, saved_count


