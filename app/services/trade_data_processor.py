"""
Service for reading, cleaning, and inserting trade data into database.
Handles data cleaning, PnL calculation, and database insertion.
"""

from typing import List, Dict, Optional, Set, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from decimal import Decimal
from collections import defaultdict
from app.db.models import Trader, Trade, AggregatedMetrics
from app.services.data_fetcher import fetch_user_trades
import logging

logger = logging.getLogger(__name__)


def clean_trade_data(trades: List[Dict]) -> List[Dict]:
    """
    Clean trade data by:
    1. Removing duplicates
    2. Fixing missing values
    3. Validating required fields
    
    Args:
        trades: List of raw trade dictionaries
    
    Returns:
        List of cleaned trade dictionaries
    """
    if not trades:
        return []
    
    # Track seen trades to remove duplicates
    seen_trades: Set[Tuple[str, str, int, str]] = set()
    cleaned_trades = []
    
    for trade in trades:
        # Extract unique identifier fields
        wallet = trade.get("proxyWallet") or trade.get("proxy_wallet") or trade.get("user") or ""
        tx_hash = trade.get("transactionHash") or trade.get("transaction_hash") or trade.get("tx_hash") or ""
        timestamp = trade.get("timestamp") or 0
        asset = str(trade.get("asset") or trade.get("token_id") or "")
        
        # Create unique key
        trade_key = (wallet.lower(), tx_hash.lower(), timestamp, asset.lower())
        
        # Skip duplicates
        if trade_key in seen_trades:
            logger.debug(f"Skipping duplicate trade: {trade_key}")
            continue
        
        seen_trades.add(trade_key)
        
        # Fix missing values and normalize fields
        cleaned_trade = {
            "proxyWallet": wallet,
            "side": (trade.get("side") or "BUY").upper(),
            "asset": asset,
            "conditionId": trade.get("conditionId") or trade.get("condition_id") or "",
            "size": Decimal(str(trade.get("size") or trade.get("shares_normalized") or 0)),
            "price": Decimal(str(trade.get("price") or 0)),
            "timestamp": int(timestamp) if timestamp else 0,
            "title": trade.get("title") or None,
            "slug": trade.get("slug") or trade.get("market_slug") or None,
            "icon": trade.get("icon") or None,
            "eventSlug": trade.get("eventSlug") or trade.get("event_slug") or None,
            "outcome": trade.get("outcome") or None,
            "outcomeIndex": trade.get("outcomeIndex") or trade.get("outcome_index") or None,
            "name": trade.get("name") or None,
            "pseudonym": trade.get("pseudonym") or None,
            "bio": trade.get("bio") or None,
            "profileImage": trade.get("profileImage") or trade.get("profile_image") or None,
            "profileImageOptimized": trade.get("profileImageOptimized") or trade.get("profile_image_optimized") or None,
            "transactionHash": tx_hash,
        }
        
        # Validate required fields
        if not cleaned_trade["proxyWallet"] or not cleaned_trade["transactionHash"]:
            logger.warning(f"Skipping trade with missing required fields: {cleaned_trade}")
            continue
        
        # Ensure wallet address is valid format
        if not cleaned_trade["proxyWallet"].startswith("0x") or len(cleaned_trade["proxyWallet"]) != 42:
            logger.warning(f"Skipping trade with invalid wallet address: {cleaned_trade['proxyWallet']}")
            continue
        
        cleaned_trades.append(cleaned_trade)
    
    logger.info(f"Cleaned {len(cleaned_trades)} trades from {len(trades)} raw trades (removed {len(trades) - len(cleaned_trades)} duplicates/invalid)")
    return cleaned_trades


def calculate_trade_pnl(trades: List[Dict]) -> List[Dict]:
    """
    Calculate entry/exit prices and PnL for trades.
    Uses FIFO (First In First Out) method to match BUY and SELL trades.
    
    Args:
        trades: List of cleaned trade dictionaries
    
    Returns:
        List of trades with calculated entry_price, exit_price, and pnl
    """
    # Sort trades by timestamp
    sorted_trades = sorted(trades, key=lambda x: x["timestamp"])
    
    # Track positions per asset (FIFO queue)
    positions: Dict[str, List[Dict]] = defaultdict(list)
    
    for trade in sorted_trades:
        asset = trade["asset"]
        side = trade["side"]
        size = trade["size"]
        price = trade["price"]
        
        # Initialize entry/exit/pnl
        trade["entry_price"] = None
        trade["exit_price"] = None
        trade["pnl"] = None
        
        if side == "BUY":
            # Add to position queue
            positions[asset].append({
                "size": size,
                "price": price,
                "timestamp": trade["timestamp"]
            })
            # Entry price is the buy price
            trade["entry_price"] = price
        
        elif side == "SELL":
            # Match with existing BUY positions (FIFO)
            remaining_sell = size
            total_cost = Decimal('0')
            total_size = Decimal('0')
            
            while remaining_sell > 0 and positions[asset]:
                position = positions[asset][0]
                position_size = position["size"]
                position_price = position["price"]
                
                if position_size <= remaining_sell:
                    # Use entire position
                    total_cost += position_size * position_price
                    total_size += position_size
                    remaining_sell -= position_size
                    positions[asset].pop(0)
                else:
                    # Use part of position
                    used_size = remaining_sell
                    total_cost += used_size * position_price
                    total_size += used_size
                    position["size"] -= used_size
                    remaining_sell = 0
            
            if total_size > 0:
                # Calculate average entry price
                avg_entry_price = total_cost / total_size
                trade["entry_price"] = avg_entry_price
                trade["exit_price"] = price
                # PnL = (exit_price - entry_price) * size
                trade["pnl"] = (price - avg_entry_price) * size
            else:
                # No matching BUY position - mark as standalone SELL
                trade["exit_price"] = price
                # Can't calculate PnL without entry
                trade["pnl"] = None
    
    # For remaining BUY positions, calculate unrealized PnL using current price
    # (This would require market data, so we'll leave it as None for now)
    
    return sorted_trades


async def get_or_create_trader(
    session: AsyncSession,
    wallet_address: str,
    name: Optional[str] = None,
    pseudonym: Optional[str] = None,
    bio: Optional[str] = None,
    profile_image: Optional[str] = None,
    profile_image_optimized: Optional[str] = None
) -> Trader:
    """
    Get existing trader or create new one.
    
    Args:
        session: Database session
        wallet_address: Wallet address
        name: Optional name
        pseudonym: Optional pseudonym
        bio: Optional bio
        profile_image: Optional profile image URL
        profile_image_optimized: Optional optimized profile image URL
    
    Returns:
        Trader object
    """
    # Try to get existing trader
    stmt = select(Trader).where(Trader.wallet_address == wallet_address)
    result = await session.execute(stmt)
    trader = result.scalar_one_or_none()
    
    if trader:
        # Update trader info if provided
        if name:
            trader.name = name
        if pseudonym:
            trader.pseudonym = pseudonym
        if bio:
            trader.bio = bio
        if profile_image:
            trader.profile_image = profile_image
        if profile_image_optimized:
            trader.profile_image_optimized = profile_image_optimized
        return trader
    
    # Create new trader
    trader = Trader(
        wallet_address=wallet_address,
        name=name,
        pseudonym=pseudonym,
        bio=bio,
        profile_image=profile_image,
        profile_image_optimized=profile_image_optimized
    )
    session.add(trader)
    await session.flush()  # Flush to get the ID
    return trader


async def insert_trades_to_db(
    session: AsyncSession,
    trader: Trader,
    trades: List[Dict]
) -> int:
    """
    Insert trades into database.
    
    Args:
        session: Database session
        trader: Trader object
        trades: List of cleaned trade dictionaries with PnL calculated
    
    Returns:
        Number of trades inserted/updated
    """
    if not trades:
        return 0
    
    saved_count = 0
    
    for trade_data in trades:
        # Convert trade data to database model
        trade_dict = {
            "trader_id": trader.id,
            "proxy_wallet": trade_data.get("proxyWallet", trader.wallet_address),
            "side": trade_data.get("side", ""),
            "asset": str(trade_data.get("asset", "")),
            "condition_id": trade_data.get("conditionId", ""),
            "size": trade_data.get("size", Decimal('0')),
            "price": trade_data.get("price", Decimal('0')),
            "entry_price": trade_data.get("entry_price"),
            "exit_price": trade_data.get("exit_price"),
            "pnl": trade_data.get("pnl"),
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
        stmt = pg_insert(Trade).values(**trade_dict)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_trade_unique",
            set_={
                "trader_id": stmt.excluded.trader_id,
                "side": stmt.excluded.side,
                "size": stmt.excluded.size,
                "price": stmt.excluded.price,
                "entry_price": stmt.excluded.entry_price,
                "exit_price": stmt.excluded.exit_price,
                "pnl": stmt.excluded.pnl,
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


async def calculate_and_insert_aggregated_metrics(
    session: AsyncSession,
    trader: Trader
) -> AggregatedMetrics:
    """
    Calculate and insert/update aggregated metrics for a trader.
    
    Args:
        session: Database session
        trader: Trader object
    
    Returns:
        AggregatedMetrics object
    """
    # Get all trades for this trader
    stmt = select(Trade).where(Trade.trader_id == trader.id)
    result = await session.execute(stmt)
    trades = result.scalars().all()
    
    # Calculate metrics
    total_trades = len(trades)
    total_stake = Decimal('0')
    total_pnl = Decimal('0')
    realized_pnl = Decimal('0')
    unrealized_pnl = Decimal('0')
    win_count = 0
    loss_count = 0
    total_volume = Decimal('0')
    largest_win = Decimal('0')
    largest_loss = Decimal('0')
    
    for trade in trades:
        # Total stake (sum of all trade sizes)
        total_stake += trade.size
        
        # Total volume (size * price)
        total_volume += trade.size * trade.price
        
        # PnL calculations
        if trade.pnl is not None:
            total_pnl += trade.pnl
            if trade.exit_price is not None:
                # Realized PnL (has exit price)
                realized_pnl += trade.pnl
                if trade.pnl > 0:
                    win_count += 1
                    if trade.pnl > largest_win:
                        largest_win = trade.pnl
                elif trade.pnl < 0:
                    loss_count += 1
                    if trade.pnl < largest_loss:
                        largest_loss = trade.pnl
            else:
                # Unrealized PnL (no exit price yet)
                unrealized_pnl += trade.pnl
    
    # Calculate win rate
    total_closed_trades = win_count + loss_count
    win_rate = Decimal('0')
    if total_closed_trades > 0:
        win_rate = (Decimal(str(win_count)) / Decimal(str(total_closed_trades))) * 100
    
    # Calculate average trade size
    avg_trade_size = Decimal('0')
    if total_trades > 0:
        avg_trade_size = total_stake / total_trades
    
    # Get or create aggregated metrics
    stmt = select(AggregatedMetrics).where(AggregatedMetrics.trader_id == trader.id)
    result = await session.execute(stmt)
    metrics = result.scalar_one_or_none()
    
    if metrics:
        # Update existing metrics
        metrics.total_trades = total_trades
        metrics.total_stake = total_stake
        metrics.total_pnl = total_pnl
        metrics.realized_pnl = realized_pnl
        metrics.unrealized_pnl = unrealized_pnl
        metrics.win_count = win_count
        metrics.loss_count = loss_count
        metrics.win_rate = win_rate
        metrics.avg_trade_size = avg_trade_size
        metrics.largest_win = largest_win
        metrics.largest_loss = largest_loss
        metrics.total_volume = total_volume
    else:
        # Create new metrics
        metrics = AggregatedMetrics(
            trader_id=trader.id,
            total_trades=total_trades,
            total_stake=total_stake,
            total_pnl=total_pnl,
            realized_pnl=realized_pnl,
            unrealized_pnl=unrealized_pnl,
            win_count=win_count,
            loss_count=loss_count,
            win_rate=win_rate,
            avg_trade_size=avg_trade_size,
            largest_win=largest_win,
            largest_loss=largest_loss,
            total_volume=total_volume
        )
        session.add(metrics)
    
    await session.commit()
    return metrics


async def process_and_insert_trade_data(
    session: AsyncSession,
    wallet_address: str
) -> Dict:
    """
    Main function to read, clean, and insert trade data for a wallet address.
    
    This function:
    1. Fetches trade data from API
    2. Cleans data (removes duplicates, fixes missing values)
    3. Calculates entry/exit prices and PnL
    4. Creates/updates Trader record
    5. Inserts trades into database
    6. Calculates and inserts aggregated metrics
    
    Args:
        session: Database session
        wallet_address: Wallet address to process
    
    Returns:
        Dictionary with processing results
    """
    try:
        # Step 1: Fetch trade data from API
        logger.info(f"Fetching trade data for wallet: {wallet_address}")
        raw_trades = await fetch_user_trades(wallet_address)
        
        if not raw_trades:
            logger.warning(f"No trades found for wallet: {wallet_address}")
            return {
                "wallet_address": wallet_address,
                "raw_trades_count": 0,
                "cleaned_trades_count": 0,
                "saved_trades_count": 0,
                "trader_id": None,
                "error": "No trades found"
            }
        
        # Step 2: Clean data
        logger.info(f"Cleaning {len(raw_trades)} raw trades")
        cleaned_trades = clean_trade_data(raw_trades)
        
        if not cleaned_trades:
            logger.warning(f"No valid trades after cleaning for wallet: {wallet_address}")
            return {
                "wallet_address": wallet_address,
                "raw_trades_count": len(raw_trades),
                "cleaned_trades_count": 0,
                "saved_trades_count": 0,
                "trader_id": None,
                "error": "No valid trades after cleaning"
            }
        
        # Step 3: Calculate entry/exit prices and PnL
        logger.info(f"Calculating PnL for {len(cleaned_trades)} trades")
        trades_with_pnl = calculate_trade_pnl(cleaned_trades)
        
        # Step 4: Get or create trader
        # Extract trader info from first trade (if available)
        first_trade = trades_with_pnl[0] if trades_with_pnl else {}
        trader = await get_or_create_trader(
            session=session,
            wallet_address=wallet_address,
            name=first_trade.get("name"),
            pseudonym=first_trade.get("pseudonym"),
            bio=first_trade.get("bio"),
            profile_image=first_trade.get("profileImage"),
            profile_image_optimized=first_trade.get("profileImageOptimized")
        )
        
        # Step 5: Insert trades
        logger.info(f"Inserting {len(trades_with_pnl)} trades into database")
        saved_count = await insert_trades_to_db(session, trader, trades_with_pnl)
        
        # Step 6: Calculate and insert aggregated metrics
        logger.info(f"Calculating aggregated metrics for trader {trader.id}")
        metrics = await calculate_and_insert_aggregated_metrics(session, trader)
        
        logger.info(f"Successfully processed {saved_count} trades for wallet: {wallet_address}")
        
        return {
            "wallet_address": wallet_address,
            "raw_trades_count": len(raw_trades),
            "cleaned_trades_count": len(cleaned_trades),
            "saved_trades_count": saved_count,
            "trader_id": trader.id,
            "metrics": {
                "total_trades": metrics.total_trades,
                "total_stake": float(metrics.total_stake),
                "total_pnl": float(metrics.total_pnl),
                "realized_pnl": float(metrics.realized_pnl),
                "unrealized_pnl": float(metrics.unrealized_pnl),
                "win_rate": float(metrics.win_rate),
            }
        }
    
    except Exception as e:
        logger.error(f"Error processing trade data for wallet {wallet_address}: {e}", exc_info=True)
        raise


