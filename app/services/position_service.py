"""Position service for saving and retrieving positions."""

from typing import List, Dict, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from app.db.models import Position
from app.services.data_fetcher import fetch_positions_for_wallet
from decimal import Decimal


async def save_positions_to_db(
    session: AsyncSession,
    wallet_address: str,
    positions: List[Dict]
) -> int:
    """
    Save positions to database. Updates existing positions or inserts new ones.
    
    Args:
        session: Database session
        wallet_address: Wallet address
        positions: List of position dictionaries from API
    
    Returns:
        Number of positions saved
    """
    saved_count = 0
    synced_combination_ids = []
    
    for pos_data in positions:
        asset = str(pos_data.get("asset", ""))
        condition_id = pos_data.get("conditionId", "")
        synced_combination_ids.append((asset, condition_id))
        
        # Convert position data to database model
        position_dict = {
            "proxy_wallet": pos_data.get("proxyWallet", wallet_address),
            "asset": asset,
            "condition_id": condition_id,
            "size": Decimal(str(pos_data.get("size", 0))),
            "avg_price": Decimal(str(pos_data.get("avgPrice", 0))),
            "initial_value": Decimal(str(pos_data.get("initialValue", 0))),
            "current_value": Decimal(str(pos_data.get("currentValue", 0))),
            "cash_pnl": Decimal(str(pos_data.get("cashPnl", 0))),
            "percent_pnl": Decimal(str(pos_data.get("percentPnl", 0))),
            "total_bought": Decimal(str(pos_data.get("totalBought", 0))),
            "realized_pnl": Decimal(str(pos_data.get("realizedPnl", 0))),
            "percent_realized_pnl": Decimal(str(pos_data.get("percentRealizedPnl", 0))),
            "cur_price": Decimal(str(pos_data.get("curPrice", 0))),
            "redeemable": pos_data.get("redeemable", False),
            "mergeable": pos_data.get("mergeable", False),
            "title": pos_data.get("title"),
            "slug": pos_data.get("slug"),
            "icon": pos_data.get("icon"),
            "event_id": pos_data.get("eventId"),
            "event_slug": pos_data.get("eventSlug"),
            "outcome": pos_data.get("outcome"),
            "outcome_index": pos_data.get("outcomeIndex"),
            "opposite_outcome": pos_data.get("oppositeOutcome"),
            "opposite_asset": str(pos_data.get("oppositeAsset", "")) if pos_data.get("oppositeAsset") else None,
            "end_date": pos_data.get("endDate"),
            "negative_risk": pos_data.get("negativeRisk", False),
        }
        
        # Use PostgreSQL upsert (INSERT ... ON CONFLICT DO UPDATE)
        # Conflict on unique combination of proxy_wallet, asset, and condition_id
        stmt = pg_insert(Position).values(**position_dict)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_position_wallet_asset_condition",
            set_={
                "size": stmt.excluded.size,
                "avg_price": stmt.excluded.avg_price,
                "initial_value": stmt.excluded.initial_value,
                "current_value": stmt.excluded.current_value,
                "cash_pnl": stmt.excluded.cash_pnl,
                "percent_pnl": stmt.excluded.percent_pnl,
                "total_bought": stmt.excluded.total_bought,
                "realized_pnl": stmt.excluded.realized_pnl,
                "percent_realized_pnl": stmt.excluded.percent_realized_pnl,
                "cur_price": stmt.excluded.cur_price,
                "redeemable": stmt.excluded.redeemable,
                "mergeable": stmt.excluded.mergeable,
                "title": stmt.excluded.title,
                "slug": stmt.excluded.slug,
                "icon": stmt.excluded.icon,
                "event_id": stmt.excluded.event_id,
                "event_slug": stmt.excluded.event_slug,
                "outcome": stmt.excluded.outcome,
                "outcome_index": stmt.excluded.outcome_index,
                "opposite_outcome": stmt.excluded.opposite_outcome,
                "opposite_asset": stmt.excluded.opposite_asset,
                "end_date": stmt.excluded.end_date,
                "negative_risk": stmt.excluded.negative_risk,
                "updated_at": stmt.excluded.updated_at,
            }
        )
        
        await session.execute(stmt)
        saved_count += 1
    
    # --- Stale Position Cleanup ---
    # Delete positions for this wallet that were NOT in the current sync list
    # This prevents closed positions from hanging around as "active" in the DB
    from sqlalchemy import and_, not_
    
    # We want to delete positions for this wallet AND (asset, condition_id) NOT IN syncing list
    if synced_combination_ids:
        # Create a condition to keep the ones we just synced
        from sqlalchemy import tuple_
        keep_condition = tuple_(Position.asset, Position.condition_id).in_(synced_combination_ids)
        delete_stmt = (
            Position.__table__.delete()
            .where(Position.proxy_wallet == wallet_address)
            .where(not_(keep_condition))
        )
        await session.execute(delete_stmt)
    else:
        # If no active positions returned from API, clear all for this wallet
        delete_stmt = (
            Position.__table__.delete()
            .where(Position.proxy_wallet == wallet_address)
        )
        await session.execute(delete_stmt)
    
    return saved_count


async def get_positions_from_db(
    session: AsyncSession,
    wallet_address: str
) -> List[Position]:
    """
    Get positions from database for a wallet address.
    
    Args:
        session: Database session
        wallet_address: Wallet address
    
    Returns:
        List of Position objects
    """
    stmt = select(Position).where(Position.proxy_wallet == wallet_address)
    result = await session.execute(stmt)
    return result.scalars().all()


async def fetch_and_save_positions(
    session: AsyncSession,
    wallet_address: str,
    sort_by: Optional[str] = None,
    sort_direction: Optional[str] = None,
    size_threshold: Optional[float] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None
) -> tuple[List[Dict], int]:
    """
    Fetch positions from API and save to database.
    
    Args:
        session: Database session
        wallet_address: Wallet address
        sort_by: Sort field (e.g., "CURRENT", "INITIAL", "PNL")
        sort_direction: Sort direction ("ASC" or "DESC")
        size_threshold: Minimum size threshold (e.g., 0.1)
        limit: Maximum number of positions to return
        offset: Offset for pagination
    
    Returns:
        Tuple of (positions list, saved count)
    """
    positions = await fetch_positions_for_wallet(
        wallet_address,
        sort_by,
        sort_direction,
        size_threshold,
        limit,
        offset
    )
    
    # Save to database
    saved_count = await save_positions_to_db(session, wallet_address, positions)
    
    return positions, saved_count

