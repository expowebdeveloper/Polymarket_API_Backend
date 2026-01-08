"""
Service for comprehensive trader data synchronization.
Fetches data from multiple Polymarket API endpoints and stores it in the database.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
import asyncio
import logging
from sqlalchemy.future import select
from sqlalchemy import delete
from sqlalchemy.exc import IntegrityError
from decimal import Decimal

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from app.db.session import AsyncSessionLocal
from app.db.models import (
    Trader, AggregatedMetrics, Position, Order, UserPnL, 
    ProfileStats, Activity, Trade, ClosedPosition
)
from app.services.data_fetcher import (
    fetch_positions_for_wallet,
    fetch_user_pnl,
    fetch_profile_stats,
    fetch_user_activity,
    fetch_user_trades,
    fetch_closed_positions,
    fetch_portfolio_value,
    fetch_leaderboard_stats
)


from app.services.position_service import save_positions_to_db
from app.services.activity_service import save_activities_to_db
from app.services.pnl_service import save_pnl_to_db

async def sync_trader_full_data(
    wallet_address: str, 
    session: Optional[AsyncSessionLocal] = None, 
    trader_metadata: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Sync all available data for a specific trader from 8 different endpoints.
    Uses asyncio.gather for concurrent fetching to maximize speed.
    
    Args:
        wallet_address: Wallet address to sync
        session: Database session (optional). If None, a new session is created.
        trader_metadata: Optional dict with 'name' and 'profile_image' to update trader info
    """
    if session is None:
        async with AsyncSessionLocal() as new_session:
            try:
                stats = await _sync_trader_with_session(wallet_address, new_session, trader_metadata)
                await new_session.commit()
                return stats
            except Exception as e:
                await new_session.rollback()
                logger.error(f"❌ Transaction failed for {wallet_address}, rolling back: {str(e)}")
                raise e
    else:
        return await _sync_trader_with_session(wallet_address, session, trader_metadata)

async def _sync_trader_with_session(
    wallet_address: str,
    session: AsyncSessionLocal,
    trader_metadata: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Internal helper to perform sync with a guaranteed session."""
    stats = {
        "positions": 0,
        "pnl_points": 0,
        "activities": 0,
        "closed_positions": 0,
        "trades": 0,
        "errors": []
    }
    
    logger.info(f"🚀 Starting full sync for trader: {wallet_address}")
    
    # 0. Ensure Trader exists and update metadata if provided
    logger.debug(f"[_sync_trader_with_session] Ensuring trader exists: {wallet_address}")
    trader_id = await _ensure_trader_exists(session, wallet_address, trader_metadata)
    if not trader_id:
        logger.error(f"❌ Could not create/find trader: {wallet_address}")
        stats["errors"].append("Could not create/find trader")
        return stats
    
    # Define tasks for concurrent fetching
    # We impose limits on historical data during bulk sync to reach the speed goal
    tasks = {
        "profile": fetch_profile_stats(wallet_address),
        "leaderboard": fetch_leaderboard_stats(wallet_address),
        "trades": fetch_user_trades(wallet_address), # Usually already limited to 100/1000 by API
        "positions": fetch_positions_for_wallet(wallet_address, limit=100), # Limit to 100 active positions
        "activities": fetch_user_activity(wallet_address, limit=200), # Reduced from 500 to 200
        "closed_positions": fetch_closed_positions(wallet_address, limit=200), # Limit to 200 closed positions
        "pnl_history": fetch_user_pnl(wallet_address, interval="1m", fidelity="1d"),
        "portfolio_value": fetch_portfolio_value(wallet_address)
    }
    
    # Run all fetches concurrently
    logger.info(f"📡 Fetching data from {len(tasks)} endpoints concurrently for {wallet_address}...")
    results = await asyncio.gather(*tasks.values(), return_exceptions=True)
    fetch_results = dict(zip(tasks.keys(), results))
    logger.info(f"✅ Concurrent fetching completed for {wallet_address}")
    
    # Process results
    
    # Process results sequentially to handle DB state correctly
    # If a critical DB error occurs, we should ROLLBACK the whole operation
    
    try:
        # 1. Profile Stats
        res = fetch_results["profile"]
        if isinstance(res, Exception):
            logger.warning(f"⚠️ Profile Fetch Error for {wallet_address}: {str(res)}")
            stats["errors"].append(f"Profile Fetch: {str(res)}")
        elif res:
            logger.debug(f"Syncing profile stats for {wallet_address}")
            await _sync_profile_stats(session, wallet_address, res)

        # 2. Leaderboard Stats
        res = fetch_results["leaderboard"]
        if isinstance(res, Exception):
            logger.warning(f"⚠️ Leaderboard Fetch Error for {wallet_address}: {str(res)}")
            stats["errors"].append(f"Leaderboard Fetch: {str(res)}")
        elif res:
            port_val = fetch_results.get("portfolio_value", 0.0)
            if isinstance(port_val, Exception):
                port_val = 0.0
            logger.debug(f"Syncing aggregated metrics for {wallet_address}")
            await _sync_aggregated_metrics(session, trader_id, res, portfolio_value=port_val)
            
        # 3. Trades
        res = fetch_results["trades"]
        if isinstance(res, Exception):
            logger.warning(f"⚠️ Trades Fetch Error for {wallet_address}: {str(res)}")
            stats["errors"].append(f"Trades Fetch: {str(res)}")
        elif res:
            logger.info(f"📥 Syncing {len(res)} trades for {wallet_address}")
            count = await _sync_trades(session, wallet_address, trader_id, res)
            stats["trades"] = count
            logger.info(f"✅ Synced {count} trades for {wallet_address}")

        # 4. Positions
        res = fetch_results["positions"]
        if isinstance(res, Exception):
            logger.warning(f"⚠️ Positions Fetch Error for {wallet_address}: {str(res)}")
            stats["errors"].append(f"Positions Fetch: {str(res)}")
        elif res:
            logger.info(f"📥 Syncing {len(res)} positions for {wallet_address}")
            count = await save_positions_to_db(session, wallet_address, res)
            stats["positions"] = count
            logger.info(f"✅ Synced {count} positions for {wallet_address}")

        # 5. Activity
        res = fetch_results["activities"]
        if isinstance(res, Exception):
            logger.warning(f"⚠️ Activity Fetch Error for {wallet_address}: {str(res)}")
            stats["errors"].append(f"Activity Fetch: {str(res)}")
        elif res:
            logger.info(f"📥 Syncing {len(res)} activities for {wallet_address}")
            count = await save_activities_to_db(session, wallet_address, res)
            stats["activities"] = count
            logger.info(f"✅ Synced {count} activities for {wallet_address}")

        # 6. Closed Positions
        res = fetch_results["closed_positions"]
        if isinstance(res, Exception):
            logger.warning(f"⚠️ ClosedPos Fetch Error for {wallet_address}: {str(res)}")
            stats["errors"].append(f"ClosedPos Fetch: {str(res)}")
        elif res:
            logger.info(f"📥 Syncing {len(res)} closed positions for {wallet_address}")
            count = await _sync_closed_positions(session, wallet_address, res)
            stats["closed_positions"] = count
            logger.info(f"✅ Synced {count} closed positions for {wallet_address}")

        # 7. User PnL History
        res = fetch_results["pnl_history"]
        if isinstance(res, Exception):
            logger.warning(f"⚠️ PnL Fetch Error for {wallet_address}: {str(res)}")
            stats["errors"].append(f"PnL Fetch: {str(res)}")
        elif res:
            logger.info(f"📥 Syncing {len(res)} PnL points for {wallet_address}")
            count = await save_pnl_to_db(session, wallet_address, res, interval="1m", fidelity="1d")
            stats["pnl_points"] = count
            logger.info(f"✅ Synced {count} PnL points for {wallet_address}")

    except Exception as db_err:
        logger.error(f"❌ DB Sync Error for {wallet_address}: {str(db_err)}")
        # Re-raise so the caller's rollback logic catches it
        raise db_err

    logger.info(f"🏁 Sync completed for trader: {wallet_address}")

    return stats


async def _ensure_trader_exists(
    session: AsyncSessionLocal, 
    wallet_address: str, 
    metadata: Optional[Dict[str, Any]] = None
) -> Optional[int]:
    """Get trader ID, creating if doesn't exist. Updates metadata if provided."""
    result = await session.execute(select(Trader).where(Trader.wallet_address == wallet_address))
    trader = result.scalar_one_or_none()
    
    if trader:
        if metadata:
            if metadata.get("name"):
                trader.name = metadata.get("name")
            if metadata.get("profile_image"):
                trader.profile_image = metadata.get("profile_image")
            # session.add(trader) # Not needed as it's attached, but safe
        return trader.id
    else:
        name = metadata.get("name") if metadata else None
        image = metadata.get("profile_image") if metadata else None
        
        trader = Trader(
            wallet_address=wallet_address, 
            name=name,
            profile_image=image,
            created_at=datetime.utcnow()
        )
        session.add(trader)
        await session.flush()
        return trader.id


async def _sync_profile_stats(session: AsyncSessionLocal, wallet_address: str, data: Dict[str, Any]):
    """Upsert profile stats."""
    result = await session.execute(select(ProfileStats).where(ProfileStats.proxy_address == wallet_address))
    stats = result.scalars().first()
    
    if stats:
        stats.username = data.get("username")
        stats.trades = data.get("trades", 0)
        stats.largest_win = data.get("largestWin", 0)
        stats.views = data.get("views", 0)
        stats.join_date = data.get("joinDate")
        # stats.updated_at = datetime.utcnow()
    else:
        stats = ProfileStats(
            proxy_address=wallet_address,
            username=data.get("username"),
            trades=data.get("trades", 0),
            largest_win=data.get("largestWin", 0),
            views=data.get("views", 0),
            join_date=data.get("joinDate"),
            created_at=datetime.utcnow()
        )
        session.add(stats)


async def _sync_aggregated_metrics(session: AsyncSessionLocal, trader_id: int, data: Dict[str, Any], portfolio_value: float = 0.0):
    """Update aggregated metrics from leaderboard data and portfolio value."""
    result = await session.execute(select(AggregatedMetrics).where(AggregatedMetrics.trader_id == trader_id))
    metrics = result.scalar_one_or_none()
    
    if metrics:
        metrics.total_volume = data.get("volume", 0)
        metrics.total_pnl = data.get("pnl", 0)
        metrics.portfolio_value = portfolio_value
        # Note: Leaderboard API returns limited fields. Win rate etc handled by other syncs or derived.
    else:
        metrics = AggregatedMetrics(
            trader_id=trader_id,
            total_volume=data.get("volume", 0),
            total_pnl=data.get("pnl", 0),
            portfolio_value=portfolio_value,
            created_at=datetime.utcnow()
        )
        session.add(metrics)


async def _sync_trades(session: AsyncSessionLocal, wallet_address: str, trader_id: int, trades: List[Dict]):
    """Upsert trades. Optimized: Fetch existing hashes first to avoid N queries."""
    count = 0
    
    # Batch fetch existing trade hashes to check against
    # We use a combination of proxy_wallet, transaction_hash, timestamp, and asset as unique identifier
    # To keep it simple and safe, we can fetch all trades for this proxy_wallet
    stmt = select(Trade.transaction_hash, Trade.timestamp, Trade.asset, Trade.id).where(Trade.proxy_wallet == wallet_address)
    result = await session.execute(stmt)
    # Use a dict with (hash, timestamp, asset) as key and id as value
    existing_map = { (r.transaction_hash, r.timestamp, r.asset): r.id for r in result.all() }
    
    for t in trades:
        tx_hash = t.get("transactionHash") or t.get("hash")
        if not tx_hash:
            continue
            
        timestamp = t.get("timestamp", 0)
        asset = t.get("asset", "")
        key = (tx_hash, timestamp, asset)
        
        if key in existing_map:
            # Update fields if needed
            # For brevity and since trades are historical data, we usually don't need to update much
            # but we can get the object if we wirklich need to
            pass
        else:
            new_trade = Trade(
                trader_id=trader_id,
                proxy_wallet=wallet_address,
                side=t.get("side", "BUY"),
                asset=asset,
                condition_id=t.get("conditionId", ""),
                size=t.get("size", 0),
                price=t.get("price", 0),
                timestamp=timestamp,
                transaction_hash=tx_hash,
                title=t.get("title"),
                outcome=t.get("outcome"),
                slug=t.get("slug")
            )
            session.add(new_trade)
            count += 1
            
    return count


async def _sync_closed_positions(session: AsyncSessionLocal, wallet_address: str, positions: List[Dict]):
    """Upsert closed positions. Optimized: Fetch existing records first."""
    count = 0
    
    # Batch fetch existing closed positions for this wallet
    stmt = select(ClosedPosition.asset, ClosedPosition.condition_id, ClosedPosition.timestamp, ClosedPosition.id).where(ClosedPosition.proxy_wallet == wallet_address)
    result = await session.execute(stmt)
    existing_map = { (r.asset, r.condition_id, r.timestamp): r.id for r in result.all() }
    
    for p in positions:
        asset = p.get("asset")
        condition_id = p.get("conditionId")
        timestamp = p.get("timestamp")
        
        if not asset or not condition_id or not timestamp:
            continue
            
        key = (asset, condition_id, timestamp)
        
        if key in existing_map:
            # Skip or update if necessary
            pass
        else:
            new_cp = ClosedPosition(
                proxy_wallet=wallet_address,
                asset=asset,
                condition_id=condition_id,
                avg_price=p.get("avgPrice", 0),
                total_bought=p.get("totalBought", 0),
                realized_pnl=p.get("realizedPnl", 0),
                cur_price=p.get("curPrice", 0),
                title=p.get("title"),
                slug=p.get("slug"),
                outcome=p.get("outcome"),
                timestamp=timestamp
            )
            session.add(new_cp)
            count += 1
            
    return count
