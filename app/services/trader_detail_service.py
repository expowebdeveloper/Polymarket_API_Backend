"""
Service to fetch and store detailed trader data from Polymarket APIs.

Fetches:
1. Profile stats (trades, largestWin, views, joinDate)
2. Value (user value)
3. Positions (open positions)
4. Activity (trading activity)
5. Closed positions
6. Trades
"""

import asyncio
import json
from typing import Dict, List, Optional, Tuple
from decimal import Decimal
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert as pg_insert
from app.services.data_fetcher import async_client
from app.db.models import (
    TraderLeaderboard,
    TraderProfile,
    TraderValue,
    TraderPosition,
    TraderActivity,
    TraderClosedPosition,
    TraderTrade
)


async def fetch_trader_profile_stats(wallet_address: str, username: Optional[str] = None) -> Optional[Dict]:
    """
    Fetch trader profile stats from Polymarket API.
    
    Args:
        wallet_address: Wallet address
        username: Optional username
    
    Returns:
        Profile stats dict or None
    """
    try:
        url = "https://polymarket.com/api/profile/stats"
        params = {"proxyAddress": wallet_address}
        if username:
            params["username"] = username
        
        response = await async_client.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching profile stats for {wallet_address}: {e}")
        return None


async def fetch_trader_value(wallet_address: str) -> Optional[Dict]:
    """
    Fetch trader value from Polymarket API.
    
    Args:
        wallet_address: Wallet address
    
    Returns:
        Value dict or None
    """
    try:
        url = "https://data-api.polymarket.com/value"
        params = {"user": wallet_address}
        
        response = await async_client.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        # API returns a list, get first item
        if isinstance(data, list) and len(data) > 0:
            return data[0]
        return None
    except Exception as e:
        print(f"Error fetching value for {wallet_address}: {e}")
        return None


async def fetch_trader_positions(wallet_address: str) -> List[Dict]:
    """
    Fetch trader positions from Polymarket API.
    Fetches all pages.
    
    Args:
        wallet_address: Wallet address
    
    Returns:
        List of position dicts
    """
    try:
        from app.services.data_fetcher import fetch_positions_for_wallet
        return await fetch_positions_for_wallet(wallet_address)
    except Exception as e:
        print(f"Error fetching positions for {wallet_address}: {e}")
        return []


async def fetch_trader_activity(wallet_address: str) -> List[Dict]:
    """
    Fetch trader activity from Polymarket API.
    Fetches all pages.
    
    Args:
        wallet_address: Wallet address
    
    Returns:
        List of activity dicts
    """
    try:
        from app.services.data_fetcher import fetch_user_activity
        return await fetch_user_activity(wallet_address)
    except Exception as e:
        print(f"Error fetching activity for {wallet_address}: {e}")
        return []


async def fetch_trader_closed_positions(wallet_address: str) -> List[Dict]:
    """
    Fetch trader closed positions from Polymarket API.
    Fetches all pages.
    
    Args:
        wallet_address: Wallet address
    
    Returns:
        List of closed position dicts
    """
    try:
        from app.services.data_fetcher import fetch_closed_positions
        return await fetch_closed_positions(wallet_address)
    except Exception as e:
        print(f"Error fetching closed positions for {wallet_address}: {e}")
        return []


async def fetch_trader_trades(wallet_address: str) -> List[Dict]:
    """
    Fetch trader trades from Polymarket API.
    Fetches all pages.
    
    Args:
        wallet_address: Wallet address
    
    Returns:
        List of trade dicts
    """
    try:
        from app.services.data_fetcher import fetch_user_trades
        return await fetch_user_trades(wallet_address)
    except Exception as e:
        print(f"Error fetching trades for {wallet_address}: {e}")
        return []


async def save_trader_profile(session: AsyncSession, trader_id: int, profile_data: Dict) -> bool:
    """Save trader profile stats to database."""
    try:
        profile_dict = {
            "trader_id": trader_id,
            "trades": profile_data.get("trades"),
            "largest_win": Decimal(str(profile_data.get("largestWin", 0))) if profile_data.get("largestWin") is not None else None,
            "views": profile_data.get("views"),
            "join_date": profile_data.get("joinDate"),
            "raw_data": json.dumps(profile_data)
        }
        
        stmt = pg_insert(TraderProfile).values(**profile_dict)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_trader_profile_trader",
            set_={
                "trades": stmt.excluded.trades,
                "largest_win": stmt.excluded.largest_win,
                "views": stmt.excluded.views,
                "join_date": stmt.excluded.join_date,
                "raw_data": stmt.excluded.raw_data,
                "updated_at": text("NOW()")
            }
        )
        async with session.begin_nested():
            await session.execute(stmt)
        return True
    except Exception as e:
        # Log specific error if possible to help debug
        print(f"Error saving trader profile: {str(e)[:200]}")
        return False


async def save_trader_value(session: AsyncSession, trader_id: int, value_data: Dict) -> bool:
    """Save trader value to database."""
    try:
        value_dict = {
            "trader_id": trader_id,
            "value": Decimal(str(value_data.get("value", 0))) if value_data.get("value") is not None else None,
            "raw_data": json.dumps(value_data)
        }
        
        stmt = pg_insert(TraderValue).values(**value_dict)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_trader_value_trader",
            set_={
                "value": stmt.excluded.value,
                "raw_data": stmt.excluded.raw_data,
                "updated_at": text("NOW()")
            }
        )
        async with session.begin_nested():
            await session.execute(stmt)
        return True
    except Exception as e:
        print(f"Error saving trader value: {str(e)[:200]}")
        return False


async def save_trader_positions(session: AsyncSession, trader_id: int, positions: List[Dict]) -> int:
    """Save trader positions to database using batch insert. Returns count of saved positions."""
    if not positions:
        return 0
    
    try:
        # Prepare all position dicts
        pos_dicts = []
        for pos_data in positions:
            pos_dicts.append({
                "trader_id": trader_id,
                "asset": pos_data.get("asset", ""),
                "condition_id": pos_data.get("conditionId", ""),
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
                "opposite_asset": pos_data.get("oppositeAsset"),
                "end_date": pos_data.get("endDate"),
                "negative_risk": pos_data.get("negativeRisk", False),
                "raw_data": json.dumps(pos_data)
            })
        
        # Deduplicate positions based on unique constraint (trader_id, asset, condition_id)
        # Keep the last one encountered (or first, doesn't matter if identical)
        unique_pos = {}
        for pos in pos_dicts:
            key = (pos["trader_id"], pos["asset"], pos["condition_id"])
            unique_pos[key] = pos
        
        pos_dicts = list(unique_pos.values())
        
        # Batch insert - process in chunks to avoid datetime issues
        # SQLAlchemy batch insert can have issues with large lists and datetime defaults
        chunk_size = 100
        total_saved = 0
        
        for i in range(0, len(pos_dicts), chunk_size):
            chunk = pos_dicts[i:i + chunk_size]
            stmt = pg_insert(TraderPosition).values(chunk)
            stmt = stmt.on_conflict_do_update(
                constraint="uq_trader_position_trader_asset_condition",
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
                "raw_data": stmt.excluded.raw_data,
                "updated_at": text("NOW()")
            }
            )
            async with session.begin_nested():
                await session.execute(stmt)
            total_saved += len(chunk)
        
        return total_saved
    except Exception as e:
        print(f"Error saving positions: {str(e)[:200]}")
        # Return what we've saved so far instead of 0
        return total_saved


async def save_trader_activities(session: AsyncSession, trader_id: int, activities: List[Dict]) -> int:
    """Save trader activities to database using batch insert. Returns count of saved activities."""
    if not activities:
        return 0
    
    try:
        # Prepare all activity dicts
        activity_dicts = []
        for activity_data in activities:
            activity_dicts.append({
                "trader_id": trader_id,
                "timestamp": activity_data.get("timestamp", 0),
                "condition_id": activity_data.get("conditionId") or None,
                "type": activity_data.get("type", ""),
                "size": Decimal(str(activity_data.get("size", 0))),
                "usdc_size": Decimal(str(activity_data.get("usdcSize", 0))),
                "transaction_hash": activity_data.get("transactionHash", ""),
                "price": Decimal(str(activity_data.get("price", 0))),
                "asset": activity_data.get("asset") or None,
                "side": activity_data.get("side") or None,
                "outcome_index": activity_data.get("outcomeIndex") if activity_data.get("outcomeIndex") != 999 else None,
                "title": activity_data.get("title"),
                "slug": activity_data.get("slug"),
                "icon": activity_data.get("icon"),
                "event_slug": activity_data.get("eventSlug"),
                "outcome": activity_data.get("outcome"),
                "name": activity_data.get("name"),
                "pseudonym": activity_data.get("pseudonym"),
                "bio": activity_data.get("bio"),
                "profile_image": activity_data.get("profileImage"),
                "profile_image_optimized": activity_data.get("profileImageOptimized"),
                "raw_data": json.dumps(activity_data)
            })
        
        # Deduplicate activities based on unique constraint (trader_id, transaction_hash, timestamp, condition_id)
        unique_activities = {}
        for act in activity_dicts:
            # Use tuple key for uniqueness
            key = (act["trader_id"], act["transaction_hash"], act["timestamp"], act.get("condition_id"))
            unique_activities[key] = act
            
        activity_dicts = list(unique_activities.values())
        
        # Batch insert - process in chunks to avoid datetime issues
        chunk_size = 100
        total_saved = 0
        
        for i in range(0, len(activity_dicts), chunk_size):
            chunk = activity_dicts[i:i + chunk_size]
            stmt = pg_insert(TraderActivity).values(chunk)
            stmt = stmt.on_conflict_do_update(
                constraint="uq_trader_activity_unique",
                set_={
                "type": stmt.excluded.type,
                "size": stmt.excluded.size,
                "usdc_size": stmt.excluded.usdc_size,
                "price": stmt.excluded.price,
                "asset": stmt.excluded.asset,
                "side": stmt.excluded.side,
                "outcome_index": stmt.excluded.outcome_index,
                "title": stmt.excluded.title,
                "slug": stmt.excluded.slug,
                "icon": stmt.excluded.icon,
                "event_slug": stmt.excluded.event_slug,
                "outcome": stmt.excluded.outcome,
                "name": stmt.excluded.name,
                "pseudonym": stmt.excluded.pseudonym,
                "bio": stmt.excluded.bio,
                "profile_image": stmt.excluded.profile_image,
                "profile_image_optimized": stmt.excluded.profile_image_optimized,
                "raw_data": stmt.excluded.raw_data,
                "updated_at": text("NOW()")
            }
            )
            async with session.begin_nested():
                await session.execute(stmt)
            total_saved += len(chunk)
        
        return total_saved
    except Exception as e:
        print(f"Error saving activities: {str(e)[:200]}")
        return total_saved


async def save_trader_closed_positions(session: AsyncSession, trader_id: int, closed_positions: List[Dict]) -> int:
    """Save trader closed positions to database using batch insert. Returns count of saved positions."""
    if not closed_positions:
        return 0
    
    try:
        # Prepare all closed position dicts
        cp_dicts = []
        for cp_data in closed_positions:
            cp_dicts.append({
                "trader_id": trader_id,
                "asset": cp_data.get("asset", ""),
                "condition_id": cp_data.get("conditionId", ""),
                "avg_price": Decimal(str(cp_data.get("avgPrice", 0))),
                "total_bought": Decimal(str(cp_data.get("totalBought", 0))),
                "realized_pnl": Decimal(str(cp_data.get("realizedPnl", 0))),
                "cur_price": Decimal(str(cp_data.get("curPrice", 0))),
                "title": cp_data.get("title"),
                "slug": cp_data.get("slug"),
                "icon": cp_data.get("icon"),
                "event_slug": cp_data.get("eventSlug"),
                "outcome": cp_data.get("outcome"),
                "outcome_index": cp_data.get("outcomeIndex"),
                "opposite_outcome": cp_data.get("oppositeOutcome"),
                "opposite_asset": cp_data.get("oppositeAsset"),
                "end_date": cp_data.get("endDate"),
                "timestamp": cp_data.get("timestamp", 0),
                "raw_data": json.dumps(cp_data)
            })
        
        # Deduplicate closed positions based on unique constraint (trader_id, asset, condition_id, timestamp)
        unique_cp = {}
        for cp in cp_dicts:
            key = (cp["trader_id"], cp["asset"], cp["condition_id"], cp["timestamp"])
            unique_cp[key] = cp
            
        cp_dicts = list(unique_cp.values())
        
        # Batch insert - process in chunks to avoid datetime issues
        chunk_size = 100
        total_saved = 0
        
        for i in range(0, len(cp_dicts), chunk_size):
            chunk = cp_dicts[i:i + chunk_size]
            stmt = pg_insert(TraderClosedPosition).values(chunk)
            stmt = stmt.on_conflict_do_update(
                constraint="uq_trader_closed_position_unique",
                set_={
                "avg_price": stmt.excluded.avg_price,
                "total_bought": stmt.excluded.total_bought,
                "realized_pnl": stmt.excluded.realized_pnl,
                "cur_price": stmt.excluded.cur_price,
                "title": stmt.excluded.title,
                "slug": stmt.excluded.slug,
                "icon": stmt.excluded.icon,
                "event_slug": stmt.excluded.event_slug,
                "outcome": stmt.excluded.outcome,
                "outcome_index": stmt.excluded.outcome_index,
                "opposite_outcome": stmt.excluded.opposite_outcome,
                "opposite_asset": stmt.excluded.opposite_asset,
                "end_date": stmt.excluded.end_date,
                "raw_data": stmt.excluded.raw_data,
                "updated_at": text("NOW()")
            }
            )
            async with session.begin_nested():
                await session.execute(stmt)
            total_saved += len(chunk)
        
        return total_saved
    except Exception as e:
        print(f"Error saving closed positions: {str(e)[:200]}")
        return total_saved


async def save_trader_trades(session: AsyncSession, trader_id: int, trades: List[Dict]) -> int:
    """Save trader trades to database using batch insert. Returns count of saved trades."""
    if not trades:
        return 0
    
    try:
        # Prepare all trade dicts
        trade_dicts = []
        for trade_data in trades:
            trade_dicts.append({
                "trader_id": trader_id,
                "side": trade_data.get("side", ""),
                "asset": trade_data.get("asset", ""),
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
                "raw_data": json.dumps(trade_data)
            })
        
        # Deduplicate trades based on unique constraint (trader_id, transaction_hash, timestamp, asset)
        unique_trades = {}
        for trade in trade_dicts:
            key = (trade["trader_id"], trade["transaction_hash"], trade["timestamp"], trade["asset"])
            unique_trades[key] = trade
            
        trade_dicts = list(unique_trades.values())
        
        # Batch insert - process in chunks to avoid datetime issues
        chunk_size = 100
        total_saved = 0
        
        for i in range(0, len(trade_dicts), chunk_size):
            chunk = trade_dicts[i:i + chunk_size]
            stmt = pg_insert(TraderTrade).values(chunk)
            stmt = stmt.on_conflict_do_update(
                constraint="uq_trader_trade_unique",
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
                "raw_data": stmt.excluded.raw_data,
                "updated_at": text("NOW()")
            }
            )
            async with session.begin_nested():
                await session.execute(stmt)
            total_saved += len(chunk)
        
        return total_saved
    except Exception as e:
        print(f"Error saving trades: {str(e)[:200]}")
        return total_saved


async def get_trader_data_status(session: AsyncSession, trader_id: int, max_age_hours: int = 24) -> Dict:
    """
    Get all trader data status in a single query for better performance.
    Returns dict with:
    - profile_needs_update: bool
    - value_needs_update: bool
    - latest_activity_ts: Optional[int]
    - latest_closed_pos_ts: Optional[int]
    - latest_trade_ts: Optional[int]
    """
    from datetime import datetime, timedelta
    cutoff_time = datetime.utcnow() - timedelta(hours=max_age_hours)
    
    result = await session.execute(
        text("""
            SELECT 
                (SELECT updated_at FROM trader_profile WHERE trader_id = :trader_id) as profile_updated,
                (SELECT updated_at FROM trader_value WHERE trader_id = :trader_id) as value_updated,
                (SELECT MAX(timestamp) FROM trader_activity WHERE trader_id = :trader_id) as latest_activity,
                (SELECT MAX(timestamp) FROM trader_closed_positions WHERE trader_id = :trader_id) as latest_closed_pos,
                (SELECT MAX(timestamp) FROM trader_trades WHERE trader_id = :trader_id) as latest_trade
        """),
        {"trader_id": trader_id}
    )
    row = result.fetchone()
    
    profile_updated = row[0] if row else None
    value_updated = row[1] if row else None
    latest_activity = row[2] if row else None
    latest_closed_pos = row[3] if row else None
    latest_trade = row[4] if row else None
    
    return {
        "profile_needs_update": profile_updated is None or (isinstance(profile_updated, datetime) and profile_updated < cutoff_time),
        "value_needs_update": value_updated is None or (isinstance(value_updated, datetime) and value_updated < cutoff_time),
        "latest_activity_ts": int(latest_activity) if latest_activity is not None else None,
        "latest_closed_pos_ts": int(latest_closed_pos) if latest_closed_pos is not None else None,
        "latest_trade_ts": int(latest_trade) if latest_trade is not None else None
    }


async def filter_new_activities(activities: List[Dict], latest_timestamp: Optional[int]) -> List[Dict]:
    """Filter activities to only include new ones (after latest_timestamp)."""
    if latest_timestamp is None:
        return activities  # No existing data, return all
    
    return [a for a in activities if a.get("timestamp", 0) > latest_timestamp]


async def filter_new_closed_positions(closed_positions: List[Dict], latest_timestamp: Optional[int]) -> List[Dict]:
    """Filter closed positions to only include new ones (after latest_timestamp)."""
    if latest_timestamp is None:
        return closed_positions  # No existing data, return all
    
    return [cp for cp in closed_positions if cp.get("timestamp", 0) > latest_timestamp]


async def filter_new_trades(trades: List[Dict], latest_timestamp: Optional[int]) -> List[Dict]:
    """Filter trades to only include new ones (after latest_timestamp)."""
    if latest_timestamp is None:
        return trades  # No existing data, return all
    
    return [t for t in trades if t.get("timestamp", 0) > latest_timestamp]


async def fetch_and_save_trader_details(
    session: AsyncSession,
    trader_id: Optional[int] = None,
    wallet_address: Optional[str] = None,
    force_refresh: bool = False,
    skip_activity: bool = False
) -> Dict[str, any]:
    """
    Fetch and save all detailed trader data from Polymarket APIs.
    Only fetches new/updated data to avoid unnecessary API calls.
    
    Args:
        session: Database session
        trader_id: Trader ID from trader_leaderboard table
        wallet_address: Wallet address (used if trader_id not provided)
        force_refresh: If True, fetch all data regardless of what exists
    
    Returns:
        Dict with counts of saved records for each data type
    """
    # Get trader_id from wallet_address if needed
    if trader_id is None and wallet_address:
        result = await session.execute(
            text("SELECT id FROM trader_leaderboard WHERE wallet_address = :wallet"),
            {"wallet": wallet_address.lower()}
        )
        row = result.fetchone()
        if not row:
            return {"error": f"Trader not found in trader_leaderboard: {wallet_address}"}
        trader_id = row[0]
    elif trader_id is None:
        return {"error": "Either trader_id or wallet_address must be provided"}
    
    # Get wallet address from trader_id if needed
    if wallet_address is None:
        result = await session.execute(
            text("SELECT wallet_address FROM trader_leaderboard WHERE id = :trader_id"),
            {"trader_id": trader_id}
        )
        row = result.fetchone()
        if not row:
            return {"error": f"Trader not found: trader_id={trader_id}"}
        wallet_address = row[0]
    
    results = {
        "trader_id": trader_id,
        "wallet_address": wallet_address,
        "profile_saved": False,
        "value_saved": False,
        "positions_saved": 0,
        "activities_saved": 0,
        "closed_positions_saved": 0,
        "trades_saved": 0,
        "skipped": {
            "profile": False,
            "value": False,
            "activities": False,
            "closed_positions": False,
            "trades": False
        },
        "errors": []
    }
    
    # Get all data status in a single query (much faster than 5 separate queries)
    if force_refresh:
        data_status = {
            "profile_needs_update": True,
            "value_needs_update": True,
            "latest_activity_ts": None,
            "latest_closed_pos_ts": None,
            "latest_trade_ts": None
        }
    else:
        data_status = await get_trader_data_status(session, trader_id)
    
    fetch_profile = data_status["profile_needs_update"]
    fetch_value = data_status["value_needs_update"]
    latest_activity_ts = data_status["latest_activity_ts"]
    latest_closed_pos_ts = data_status["latest_closed_pos_ts"]
    latest_trade_ts = data_status["latest_trade_ts"]
    
    # Initialize data variables
    profile_data = None
    value_data = None
    positions = []
    activities = []
    closed_positions = []
    trades = []
    
    # Build fetch tasks for data that needs to be fetched
    fetch_tasks = []
    
    if fetch_profile:
        fetch_tasks.append(("profile", fetch_trader_profile_stats(wallet_address)))
    else:
        results["skipped"]["profile"] = True
    
    if fetch_value:
        fetch_tasks.append(("value", fetch_trader_value(wallet_address)))
    else:
        results["skipped"]["value"] = True
    
    # Always fetch positions (they can change)
    fetch_tasks.append(("positions", fetch_trader_positions(wallet_address)))
    
    # For time-based data, always fetch but we'll filter later
    if skip_activity:
        results["skipped"]["activities"] = True
    elif latest_activity_ts is None:
        # No existing data, fetch all
        fetch_tasks.append(("activities", fetch_trader_activity(wallet_address)))
    else:
        # Have existing data, fetch all but filter to new ones only
        fetch_tasks.append(("activities", fetch_trader_activity(wallet_address)))
        results["skipped"]["activities"] = True  # Will filter, not skip entirely
    
    if latest_closed_pos_ts is None:
        fetch_tasks.append(("closed_positions", fetch_trader_closed_positions(wallet_address)))
    else:
        fetch_tasks.append(("closed_positions", fetch_trader_closed_positions(wallet_address)))
        results["skipped"]["closed_positions"] = True
    
    if latest_trade_ts is None:
        fetch_tasks.append(("trades", fetch_trader_trades(wallet_address)))
    else:
        fetch_tasks.append(("trades", fetch_trader_trades(wallet_address)))
        results["skipped"]["trades"] = True
    
    # Execute all fetch tasks in parallel
    if fetch_tasks:
        task_names, tasks = zip(*fetch_tasks)
        fetched_data = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        for name, data in zip(task_names, fetched_data):
            if isinstance(data, Exception):
                results["errors"].append(f"{name} fetch error: {data}")
                continue
            
            if name == "profile":
                profile_data = data
            elif name == "value":
                value_data = data
            elif name == "positions":
                positions = data if data else []
            elif name == "activities":
                if latest_activity_ts is not None:
                    # Filter to only new activities
                    activities = await filter_new_activities(data if data else [], latest_activity_ts)
                else:
                    activities = data if data else []
            elif name == "closed_positions":
                if latest_closed_pos_ts is not None:
                    # Filter to only new closed positions
                    closed_positions = await filter_new_closed_positions(data if data else [], latest_closed_pos_ts)
                else:
                    closed_positions = data if data else []
            elif name == "trades":
                if latest_trade_ts is not None:
                    # Filter to only new trades
                    trades = await filter_new_trades(data if data else [], latest_trade_ts)
                else:
                    trades = data if data else []
    
    # Save profile
    if profile_data:
        results["profile_saved"] = await save_trader_profile(session, trader_id, profile_data)
    
    # Save value
    if value_data:
        results["value_saved"] = await save_trader_value(session, trader_id, value_data)
    
    # Save positions
    if positions:
        results["positions_saved"] = await save_trader_positions(session, trader_id, positions)
    
    # Save activities (only new ones)
    if activities:
        results["activities_saved"] = await save_trader_activities(session, trader_id, activities)
    
    # Save closed positions (only new ones)
    if closed_positions:
        results["closed_positions_saved"] = await save_trader_closed_positions(session, trader_id, closed_positions)
    
    # Save trades (only new ones)
    if trades:
        results["trades_saved"] = await save_trader_trades(session, trader_id, trades)
    
    return results


async def fetch_and_save_all_traders_details(
    session: AsyncSession,
    limit: Optional[int] = None,
    offset: int = 0,
    force_refresh: bool = False,
    session_factory = None,
    skip_activity: bool = False
) -> Dict[str, any]:
    """
    Fetch and save detailed data for all traders in trader_leaderboard.
    
    Args:
        session: Database session (used only for reading trader list)
        limit: Maximum number of traders to process (None = all)
        offset: Offset for pagination
        session_factory: Optional session factory to create individual sessions per trader
    
    Returns:
        Dict with summary statistics
    """
    # Get traders from database
    query = "SELECT id, wallet_address FROM trader_leaderboard ORDER BY id LIMIT :limit OFFSET :offset"
    if limit is None:
        query = "SELECT id, wallet_address FROM trader_leaderboard ORDER BY id OFFSET :offset"
        params = {"offset": offset}
    else:
        params = {"limit": limit, "offset": offset}
    
    result = await session.execute(text(query), params)
    traders = result.fetchall()
    
    if not traders:
        return {
            "total_traders": 0,
            "processed": 0,
            "summary": {}
        }
    
    total_processed = 0
    summary = {
        "profile_saved": 0,
        "value_saved": 0,
        "total_positions_saved": 0,
        "total_activities_saved": 0,
        "total_closed_positions_saved": 0,
        "total_trades_saved": 0,
        "errors": []
    }
    
    # Process traders with concurrency limit (reduced to avoid rate limits)
    semaphore = asyncio.Semaphore(5)  # Reduced from 15 to 5
    
    skipped_counts = {
        "profile": 0,
        "value": 0,
        "activities": 0,
        "closed_positions": 0,
        "trades": 0
    }
    
    async def process_trader(trader_id: int, wallet: str):
        async with semaphore:
            # Create a separate session for each trader to avoid concurrent operation errors
            if session_factory:
                async with session_factory() as trader_session:
                    try:
                        result = await fetch_and_save_trader_details(
                            trader_session, trader_id=trader_id, wallet_address=wallet, force_refresh=force_refresh, skip_activity=skip_activity
                        )
                        
                        # Check for errors in result
                        if "error" in result:
                            await trader_session.rollback()
                            summary["errors"].append(f"Error processing trader {trader_id}: {result['error']}")
                            return False
                        
                        # Commit this trader's transaction
                        await trader_session.commit()
                    except Exception as e:
                        await trader_session.rollback()
                        summary["errors"].append(f"Error processing trader {trader_id}: {e}")
                        return False
            else:
                # Fallback: use shared session but process sequentially
                try:
                    result = await fetch_and_save_trader_details(
                        session, trader_id=trader_id, wallet_address=wallet, force_refresh=force_refresh, skip_activity=skip_activity
                    )
                    
                    if "error" in result:
                        summary["errors"].append(f"Error processing trader {trader_id}: {result['error']}")
                        return False
                except Exception as e:
                    summary["errors"].append(f"Error processing trader {trader_id}: {e}")
                    return False
            
            # If we get here, trader processed successfully
            if result.get("profile_saved"):
                summary["profile_saved"] += 1
            elif result.get("skipped", {}).get("profile"):
                skipped_counts["profile"] += 1
                
            if result.get("value_saved"):
                summary["value_saved"] += 1
            elif result.get("skipped", {}).get("value"):
                skipped_counts["value"] += 1
                
            summary["total_positions_saved"] += result.get("positions_saved", 0)
            
            activities_saved = result.get("activities_saved", 0)
            summary["total_activities_saved"] += activities_saved
            if activities_saved == 0 and result.get("skipped", {}).get("activities"):
                skipped_counts["activities"] += 1
            
            closed_pos_saved = result.get("closed_positions_saved", 0)
            summary["total_closed_positions_saved"] += closed_pos_saved
            if closed_pos_saved == 0 and result.get("skipped", {}).get("closed_positions"):
                skipped_counts["closed_positions"] += 1
            
            trades_saved = result.get("trades_saved", 0)
            summary["total_trades_saved"] += trades_saved
            if trades_saved == 0 and result.get("skipped", {}).get("trades"):
                skipped_counts["trades"] += 1
            
            if result.get("errors"):
                summary["errors"].extend(result["errors"])
            
            return True
    
    # Process in batches (reduced for better stability)
    batch_size=10  # Reduced from 50 to 20
    for i in range(0, len(traders), batch_size):
        batch = traders[i:i + batch_size]
        tasks = [process_trader(trader_id, wallet) for trader_id, wallet in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        total_processed += sum(1 for r in results if r is True)
        
        # Delay between batches (increased from 0.1s to 1.0s)
        if i + batch_size < len(traders):
            await asyncio.sleep(1.0)
    
    summary["skipped"] = skipped_counts
    return {
        "total_traders": len(traders),
        "processed": total_processed,
        "summary": summary
    }

async def get_scraped_trades_for_calc(session: AsyncSession, wallet_address: str) -> List[Dict]:
    """
    Get scraped trades for calculation logic.
    Returns list of dicts consistent with what trade_service.get_trades_from_db would return as dicts.
    """
    stmt = text("SELECT * FROM trader_trades WHERE trader_id = (SELECT id FROM trader_leaderboard WHERE wallet_address = :wallet)")
    result = await session.execute(stmt, {"wallet": wallet_address})
    cols = result.keys()
    rows = result.fetchall()
    
    trades = []
    for row in rows:
        t = dict(zip(cols, row))
        # Map fields to match what calculation expects vs what is stored
        # Stored: size, price, side, etc.
        # Calculation expects: size, price, side, timestamp, etc.
        # Ensure types are correct (Decimal to float if needed, or keep Decimal)
        # transform keys if needed
        t["proxy_wallet"] = wallet_address
        trades.append(t)
    return trades

async def get_scraped_positions_for_calc(session: AsyncSession, wallet_address: str) -> List[Dict]:
    """Get scraped positions for calculation."""
    stmt = text("SELECT * FROM trader_positions WHERE trader_id = (SELECT id FROM trader_leaderboard WHERE wallet_address = :wallet)")
    result = await session.execute(stmt, {"wallet": wallet_address})
    cols = result.keys()
    
    positions = []
    for row in result.fetchall():
        p = dict(zip(cols, row))
        # Ensure mapping
        # Stored: initial_value, current_value, etc.
        p["proxy_wallet"] = wallet_address
        positions.append(p)
    return positions

async def get_scraped_activities_for_calc(session: AsyncSession, wallet_address: str) -> List[Dict]:
    """Get scraped activities for calculation."""
    stmt = text("SELECT * FROM trader_activity WHERE trader_id = (SELECT id FROM trader_leaderboard WHERE wallet_address = :wallet)")
    result = await session.execute(stmt, {"wallet": wallet_address})
    cols = result.keys()
    
    activities = []
    for row in result.fetchall():
        a = dict(zip(cols, row))
        a["proxy_wallet"] = wallet_address
        # Map keys if needed (usdc_size -> usdcSize handled in calc logic usually? calc logic expects snake_case from DB or camel from API)
        # process_trader_data_points expects: usdcSize or usdc_size, side (BUY/SELL)
        activities.append(a)
    return activities

async def get_scraped_closed_positions_for_calc(session: AsyncSession, wallet_address: str) -> List[Dict]:
    """Get scraped closed positions for calculation."""
    stmt = text("SELECT * FROM trader_closed_positions WHERE trader_id = (SELECT id FROM trader_leaderboard WHERE wallet_address = :wallet)")
    result = await session.execute(stmt, {"wallet": wallet_address})
    cols = result.keys()
    
    closed_pos = []
    for row in result.fetchall():
        cp = dict(zip(cols, row))
        cp["proxy_wallet"] = wallet_address
        closed_pos.append(cp)
    return closed_pos
