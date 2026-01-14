"""Trade service for saving and retrieving trades."""

from typing import List, Dict, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text, func
from sqlalchemy.orm import defer
from sqlalchemy.dialects.postgresql import insert as pg_insert
from app.db.models import Trade, ClosedPosition
from app.services.data_fetcher import fetch_user_trades, fetch_market_by_slug, get_market_resolution
from decimal import Decimal
import asyncio
from sqlalchemy import select, text, func, and_

async def process_resolved_markets_for_trades(
    session: AsyncSession,
    wallet_address: str,
    trades: List[Dict]
):
    """
    Check if any of the trades belong to resolved markets.
    If so, ensure they are present in the ClosedPosition table.
    If not, calculate PnL and add them.
    """
    if not trades:
        return

    # 1. Identify unique markets (by condition_id AND asset to be handling specific outcomes correctly)
    # Group trades by (asset, condition_id)
    market_groups = {}
    for trade in trades:
        # Key: condition_id (market identifier), asset (specific outcome token)
        condition_id = trade.get("conditionId") or trade.get("condition_id")
        asset = trade.get("asset")
        
        if not condition_id or not asset:
            continue
            
        key = (condition_id, asset)
        if key not in market_groups:
            market_groups[key] = []
        market_groups[key].append(trade)
    
    unique_condition_ids = set(k[0] for k in market_groups.keys())
    
    # 2. For each market, check if it is resolved
    # We can check DB first if we store markets, but likely we need to index or check API
    # Optimization: Check if we ALREADY have a closed position for this (condition_id, asset)
    # If we do, we don't need to do anything (unless we want to update it, but usually closed is closed)
    
    existing_closed_stm = select(ClosedPosition.condition_id, ClosedPosition.asset).where(
        ClosedPosition.proxy_wallet == wallet_address,
        ClosedPosition.condition_id.in_(unique_condition_ids)
    )
    existing_result = await session.execute(existing_closed_stm)
    existing_set = set(existing_result.all()) # Set of (condition_id, asset)
    
    # Filter out groups that already exist
    groups_to_process = {k: v for k, v in market_groups.items() if k not in existing_set}
    
    if not groups_to_process:
        return

    print(f"Checking {len(groups_to_process)} potential resolved markets for {wallet_address}...")
    
    # 3. Fetch market status for remaining
    # We might have many, so maybe limit concurrency
    processed_count = 0
    
    for (condition_id, asset), market_trades in groups_to_process.items():
        # Get market details to check resolution
        # Use simple caching or just fetch
        # The trade contains 'slug' usually, which is faster for lookup than condition_id sometimes
        slug = market_trades[0].get("slug")
        outcome_token = market_trades[0].get("outcome") # e.g. "Yes"
        
        # Identifier for fetching
        market_id = slug if slug else condition_id
        
        market_data = await fetch_market_by_slug(market_id)
        if not market_data:
            continue
            
        # Check if resolved
        is_resolved = False
        resolved_outcome = None
        
        # Check 'closed' or 'resolved' status
        if market_data.get("closed") or market_data.get("resolved") or market_data.get("status") in ["closed", "resolved"]:
             is_resolved = True
        
        # Try to determine winning outcome
        # Usually 'winningOutcome' in API data if resolved
        # Or 'question' might imply it.
        # But for binary yes/no, typically we check if 1 or 0 is winner.
        
        # If not resolved, skip
        if not is_resolved:
            continue
            
        # 4. Calculate PnL "Same Formula"
        # We need to determine if User WON or LOST
        
        # Polymarket API usually provides 'winningOutcome' if resolved.
        # Format can be "Yes", "No", or an ID.
        winning_outcome = market_data.get("winningOutcome")
        
        # Determine settlement price (1 or 0)
        settlement_price = 0.0
        if winning_outcome:
            # If user holds "Yes" and winner is "Yes" -> 1.0
            # If user holds "No" and winner is "No" -> 1.0
            # Else -> 0.0
            
            # Normalize strings
            user_outcome = str(outcome_token).lower() if outcome_token else ""
            winner = str(winning_outcome).lower()
            
            if user_outcome == winner:
                settlement_price = 1.0
            else:
                # Handle cases where winningOutcome is an ID ?? (Rare for simple binary)
                # Assume 0.0 if not match
                settlement_price = 0.0
        else:
             # Fallback: check prices. If One is 1 and other 0.
             # Or TokensRedeemable...
             # If we can't be sure, SKIP to avoid bad data.
             # Actually, if 'resolution' field exists?
             pass
        
        # Calculate stats for this position
        total_bought = Decimal(0)
        total_cost = Decimal(0)
        
        buy_trades = [t for t in market_trades if t.get("side") == "BUY"]
        
        for t in buy_trades:
            size = Decimal(str(t.get("size", 0)))
            price = Decimal(str(t.get("price", 0)))
            total_bought += size
            total_cost += (size * price)
            
        if total_bought == 0:
            continue
            
        avg_price = total_cost / total_bought
        
        # Realized PnL = (SettlementPrice - AvgPrice) * Size
        # Note: If they sold some, we should handle that. But simplified "Closed Position"
        # usually implies the final result of held tokens.
        # If they successfully exited before resolution, it's already "closed" via trade matching logic (maybe).
        # But here valid for "Held until resolution".
        
        # net bought = bought - sold
        total_sold = Decimal(0)
        sell_trades = [t for t in market_trades if t.get("side") == "SELL"]
        for t in sell_trades:
            size = Decimal(str(t.get("size", 0)))
            total_sold += size
        
        net_position = total_bought - total_sold
        
        if net_position <= 0:
            # They already closed it manually or have no position
            continue
            
        # Valid "Held to resolution" position
        realized_pnl = (Decimal(settlement_price) - avg_price) * net_position
        
        # Create ClosedPosition entry
        # MAPPING
        cp = ClosedPosition(
            proxy_wallet=wallet_address,
            asset=asset,
            condition_id=condition_id,
            avg_price=avg_price,
            total_bought=net_position, # The amount settled
            realized_pnl=realized_pnl,
            cur_price=Decimal(settlement_price), # Final price
            title=market_data.get("title") or market_trades[0].get("title"),
            slug=market_data.get("slug") or slug,
            icon=market_data.get("icon") or market_trades[0].get("icon"),
            event_slug=market_data.get("eventSlug") or market_trades[0].get("eventSlug"),
            outcome=outcome_token,
            outcome_index=market_trades[0].get("outcomeIndex"),
            end_date=market_data.get("endDate"),
            timestamp=market_data.get("updatedAt") or market_data.get("endDate") or market_trades[0].get("timestamp"), # Use resolution time ideally
        )
        
        # Handle timestamp format
        if isinstance(cp.timestamp, str):
             # Try parse or default to current
             from datetime import datetime
             try:
                 dt = datetime.fromisoformat(cp.timestamp.replace('Z', '+00:00'))
                 cp.timestamp = int(dt.timestamp())
             except:
                 cp.timestamp = int(market_trades[0].get("timestamp", 0))

        session.add(cp)
        processed_count += 1
        
    if processed_count > 0:
        await session.commit()
        print(f"✓ Added {processed_count} auto-resolved closed positions for {wallet_address}")


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
    
    if not trades:
        return 0

    try:
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
    except Exception as e:
        print(f"Error checking trade batch save: {e}")
        # If batch commit fails, try individually (slow fallback)
        await session.rollback()
        for trade_data in trades:
            try:
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
                async with session.begin_nested():
                    await session.execute(stmt)
                saved_count += 1
            except Exception as inner_e:
                print(f"Failed to save individual trade: {inner_e}")
        await session.commit()
    
    # After successful save, check for resolved markets
    try:
         await process_resolved_markets_for_trades(session, wallet_address, trades)
    except Exception as e:
         print(f"Error processing resolved markets: {e}")
         # Don't fail the whole trade save if this fails
         pass
         
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
    
    # Check dialect to see if we are using SQLite or PostgreSQL
    dialect_name = session.bind.dialect.name if session.bind else "postgresql"
    existing_columns = set()
    
    if dialect_name == "sqlite":
        # SQLite uses PRAGMA table_info
        check_sql = text("PRAGMA table_info(trades)")
        check_result = await session.execute(check_sql)
        # Row format: (cid, name, type, notnull, dflt_value, pk)
        # We verify by checking if the column name exists in the results
        all_cols = {row[1] for row in check_result.all()}
        # Intersect with our optional columns
        existing_columns = all_cols.intersection({'entry_price', 'exit_price', 'pnl', 'trader_id'})
    else:
        # PostgreSQL uses information_schema
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
    # Fetch trades from API (async function)
    trades = await fetch_user_trades(wallet_address)
    
    # Save to database
    saved_count = await save_trades_to_db(session, wallet_address, trades)
    
    return trades, saved_count


async def get_latest_trade_timestamp(
    session: AsyncSession,
    wallet_address: str
) -> Optional[int]:
    """Get the timestamp of the latest trade for a wallet."""
    stmt = select(func.max(Trade.timestamp)).where(Trade.proxy_wallet == wallet_address)
    result = await session.execute(stmt)
    return result.scalar()


async def sync_trades_since_timestamp(
    session: AsyncSession,
    wallet_address: str,
    min_timestamp: Optional[int] = None
) -> int:
    """
    Sync trades newer than min_timestamp.
    Fetches in batches to avoid getting full history.
    STOPS fetching if it encounters trades older than min_timestamp.
    """
    total_saved = 0
    limit = 500  # Increased batch size for faster sync
    offset = 0
    batch_count = 0
    max_batches = 2000  # limit: allow up to 1,000,000 trades (approx full history)
    
    while batch_count < max_batches:
        # Fetch batch
        trades = await fetch_user_trades(wallet_address, limit=limit, offset=offset)
        
        if not trades:
            break
            
        # Check if we have any new trades in this batch
        if min_timestamp:
            # Filter trades newer than min_timestamp
            new_trades = [t for t in trades if t.get("timestamp", 0) > min_timestamp]
            
            # If we found older trades in this batch (and filtered them out),
            # it means we've reached the history boundary.
            has_old_trades = any(t.get("timestamp", 0) <= min_timestamp for t in trades)
            
            if new_trades:
                saved = await save_trades_to_db(session, wallet_address, new_trades)
                total_saved += saved
            
            if has_old_trades:
                # We reached overlap with DB history, so we can stop
                break
                
            if len(new_trades) == 0:
                # All trades in this batch were old
                break
        else:
            # Initial sync or force refresh - save all
            saved = await save_trades_to_db(session, wallet_address, trades)
            total_saved += saved
            
        # Prepare next batch
        offset += limit
        batch_count += 1
        
        # Sleep briefly to be nice to API
        await asyncio.sleep(0.1)
        
    return total_saved
