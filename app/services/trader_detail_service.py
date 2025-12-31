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
    
    Args:
        wallet_address: Wallet address
    
    Returns:
        List of position dicts
    """
    try:
        url = "https://data-api.polymarket.com/positions"
        params = {"user": wallet_address}
        
        response = await async_client.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        return data if isinstance(data, list) else []
    except Exception as e:
        print(f"Error fetching positions for {wallet_address}: {e}")
        return []


async def fetch_trader_activity(wallet_address: str) -> List[Dict]:
    """
    Fetch trader activity from Polymarket API.
    
    Args:
        wallet_address: Wallet address
    
    Returns:
        List of activity dicts
    """
    try:
        url = "https://data-api.polymarket.com/activity"
        params = {"user": wallet_address}
        
        response = await async_client.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        return data if isinstance(data, list) else []
    except Exception as e:
        print(f"Error fetching activity for {wallet_address}: {e}")
        return []


async def fetch_trader_closed_positions(wallet_address: str) -> List[Dict]:
    """
    Fetch trader closed positions from Polymarket API.
    
    Args:
        wallet_address: Wallet address
    
    Returns:
        List of closed position dicts
    """
    try:
        url = "https://data-api.polymarket.com/closed-positions"
        params = {"user": wallet_address}
        
        response = await async_client.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        return data if isinstance(data, list) else []
    except Exception as e:
        print(f"Error fetching closed positions for {wallet_address}: {e}")
        return []


async def fetch_trader_trades(wallet_address: str) -> List[Dict]:
    """
    Fetch trader trades from Polymarket API.
    
    Args:
        wallet_address: Wallet address
    
    Returns:
        List of trade dicts
    """
    try:
        url = "https://data-api.polymarket.com/trades"
        params = {"user": wallet_address}
        
        response = await async_client.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        return data if isinstance(data, list) else []
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
        await session.execute(stmt)
        return True
    except Exception as e:
        print(f"Error saving trader profile: {e}")
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
        await session.execute(stmt)
        return True
    except Exception as e:
        print(f"Error saving trader value: {e}")
        return False


async def save_trader_positions(session: AsyncSession, trader_id: int, positions: List[Dict]) -> int:
    """Save trader positions to database. Returns count of saved positions."""
    saved_count = 0
    
    for pos_data in positions:
        try:
            pos_dict = {
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
            }
            
            stmt = pg_insert(TraderPosition).values(**pos_dict)
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
            await session.execute(stmt)
            saved_count += 1
        except Exception as e:
            print(f"Error saving position: {e}")
            continue
    
    return saved_count


async def save_trader_activities(session: AsyncSession, trader_id: int, activities: List[Dict]) -> int:
    """Save trader activities to database. Returns count of saved activities."""
    saved_count = 0
    
    for activity_data in activities:
        try:
            activity_dict = {
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
            }
            
            stmt = pg_insert(TraderActivity).values(**activity_dict)
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
            await session.execute(stmt)
            saved_count += 1
        except Exception as e:
            print(f"Error saving activity: {e}")
            continue
    
    return saved_count


async def save_trader_closed_positions(session: AsyncSession, trader_id: int, closed_positions: List[Dict]) -> int:
    """Save trader closed positions to database. Returns count of saved positions."""
    saved_count = 0
    
    for cp_data in closed_positions:
        try:
            cp_dict = {
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
            }
            
            stmt = pg_insert(TraderClosedPosition).values(**cp_dict)
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
            await session.execute(stmt)
            saved_count += 1
        except Exception as e:
            print(f"Error saving closed position: {e}")
            continue
    
    return saved_count


async def save_trader_trades(session: AsyncSession, trader_id: int, trades: List[Dict]) -> int:
    """Save trader trades to database. Returns count of saved trades."""
    saved_count = 0
    
    for trade_data in trades:
        try:
            trade_dict = {
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
            }
            
            stmt = pg_insert(TraderTrade).values(**trade_dict)
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
            await session.execute(stmt)
            saved_count += 1
        except Exception as e:
            print(f"Error saving trade: {e}")
            continue
    
    return saved_count


async def fetch_and_save_trader_details(
    session: AsyncSession,
    trader_id: Optional[int] = None,
    wallet_address: Optional[str] = None
) -> Dict[str, any]:
    """
    Fetch and save all detailed trader data from Polymarket APIs.
    
    Args:
        session: Database session
        trader_id: Trader ID from trader_leaderboard table
        wallet_address: Wallet address (used if trader_id not provided)
    
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
        "errors": []
    }
    
    # Fetch all data in parallel
    try:
        profile_data, value_data, positions, activities, closed_positions, trades = await asyncio.gather(
            fetch_trader_profile_stats(wallet_address),
            fetch_trader_value(wallet_address),
            fetch_trader_positions(wallet_address),
            fetch_trader_activity(wallet_address),
            fetch_trader_closed_positions(wallet_address),
            fetch_trader_trades(wallet_address),
            return_exceptions=True
        )
        
        # Handle exceptions
        if isinstance(profile_data, Exception):
            results["errors"].append(f"Profile fetch error: {profile_data}")
            profile_data = None
        if isinstance(value_data, Exception):
            results["errors"].append(f"Value fetch error: {value_data}")
            value_data = None
        if isinstance(positions, Exception):
            results["errors"].append(f"Positions fetch error: {positions}")
            positions = []
        if isinstance(activities, Exception):
            results["errors"].append(f"Activities fetch error: {activities}")
            activities = []
        if isinstance(closed_positions, Exception):
            results["errors"].append(f"Closed positions fetch error: {closed_positions}")
            closed_positions = []
        if isinstance(trades, Exception):
            results["errors"].append(f"Trades fetch error: {trades}")
            trades = []
        
        # Save profile
        if profile_data:
            results["profile_saved"] = await save_trader_profile(session, trader_id, profile_data)
        
        # Save value
        if value_data:
            results["value_saved"] = await save_trader_value(session, trader_id, value_data)
        
        # Save positions
        if positions:
            results["positions_saved"] = await save_trader_positions(session, trader_id, positions)
        
        # Save activities
        if activities:
            results["activities_saved"] = await save_trader_activities(session, trader_id, activities)
        
        # Save closed positions
        if closed_positions:
            results["closed_positions_saved"] = await save_trader_closed_positions(session, trader_id, closed_positions)
        
        # Save trades
        if trades:
            results["trades_saved"] = await save_trader_trades(session, trader_id, trades)
        
    except Exception as e:
        results["errors"].append(f"Fatal error: {e}")
    
    return results


async def fetch_and_save_all_traders_details(
    session: AsyncSession,
    limit: Optional[int] = None,
    offset: int = 0
) -> Dict[str, any]:
    """
    Fetch and save detailed data for all traders in trader_leaderboard.
    
    Args:
        session: Database session
        limit: Maximum number of traders to process (None = all)
        offset: Offset for pagination
    
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
    
    # Process traders with concurrency limit
    semaphore = asyncio.Semaphore(5)  # Limit concurrent API calls
    
    async def process_trader(trader_id: int, wallet: str):
        async with semaphore:
            try:
                result = await fetch_and_save_trader_details(
                    session, trader_id=trader_id, wallet_address=wallet
                )
                
                if result.get("profile_saved"):
                    summary["profile_saved"] += 1
                if result.get("value_saved"):
                    summary["value_saved"] += 1
                summary["total_positions_saved"] += result.get("positions_saved", 0)
                summary["total_activities_saved"] += result.get("activities_saved", 0)
                summary["total_closed_positions_saved"] += result.get("closed_positions_saved", 0)
                summary["total_trades_saved"] += result.get("trades_saved", 0)
                
                if result.get("errors"):
                    summary["errors"].extend(result["errors"])
                
                return True
            except Exception as e:
                summary["errors"].append(f"Error processing trader {trader_id}: {e}")
                return False
    
    # Process in batches
    batch_size = 20
    for i in range(0, len(traders), batch_size):
        batch = traders[i:i + batch_size]
        tasks = [process_trader(trader_id, wallet) for trader_id, wallet in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        total_processed += sum(1 for r in results if r is True)
        
        # Commit after each batch
        try:
            await session.commit()
        except Exception as e:
            await session.rollback()
            summary["errors"].append(f"Batch commit error: {e}")
        
        # Small delay between batches
        if i + batch_size < len(traders):
            await asyncio.sleep(0.5)
    
    return {
        "total_traders": len(traders),
        "processed": total_processed,
        "summary": summary
    }
