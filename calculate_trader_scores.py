"""
Script to calculate and store trader scores.

This script:
1. Gets wallet addresses from trader_leaderboard table
2. Fetches/calculates metrics from APIs or database
3. Calculates scores using the same logic as view-all leaderboard
4. Stores results in trader_calculated_scores table
"""

import asyncio
import sys
from typing import Dict, List, Optional
from decimal import Decimal
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from app.core.config import settings
from app.db.models import Base, TraderCalculatedScore, TraderLeaderboard
from app.services.leaderboard_service import (
    calculate_trader_metrics_with_time_filter,
    calculate_scores_and_rank,
    get_percentile_value
)
from app.db.models import TraderClosedPosition, TraderTrade, TraderPosition
from app.services.trader_detail_service import (
    fetch_trader_closed_positions,
    fetch_trader_trades
)
from app.services.data_fetcher import async_client


async def check_table_exists(engine):
    """Check if trader_calculated_scores table exists, create if not."""
    async with engine.begin() as conn:
        if "sqlite" in settings.DATABASE_URL:
             result = await conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name='trader_calculated_scores'")
            )
             exists = bool(result.scalar())
        else:
            result = await conn.execute(
                text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = 'trader_calculated_scores'
                    )
                """)
            )
            exists = result.scalar()
        
        if not exists:
            print("Creating trader_calculated_scores table...")
            await conn.run_sync(
                lambda sync_conn: Base.metadata.create_all(
                    sync_conn,
                    tables=[TraderCalculatedScore.__table__]
                )
            )
            print("✅ Table created successfully!")
        else:
            print("✅ Table trader_calculated_scores already exists")


async def get_trader_metrics_from_db(
    session: AsyncSession,
    wallet_address: str
) -> Optional[Dict]:
    """
    Get trader metrics from database tables (trader_closed_positions, trader_trades, etc.).
    Prioritizes trader_* tables, falls back to main tables, then APIs if needed.
    """
    try:
        # Get trader_id from trader_leaderboard
        result = await session.execute(
            text("SELECT id FROM trader_leaderboard WHERE wallet_address = :wallet"),
            {"wallet": wallet_address.lower()}
        )
        row = result.fetchone()
        if not row:
            return None
        
        trader_id = row[0]
        
        # First check if trader_closed_positions has any data for this trader
        result = await session.execute(
            text("SELECT COUNT(*) FROM trader_closed_positions WHERE trader_id = :trader_id"),
            {"trader_id": trader_id}
        )
        closed_pos_count = result.scalar() or 0
        
        # Try to get metrics from trader_closed_positions first
        result = await session.execute(
            text("""
                SELECT 
                    COALESCE(SUM(total_bought * avg_price), 0) as total_stakes,
                    COALESCE(SUM(total_bought * avg_price * CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END), 0) as winning_stakes,
                    COALESCE(SUM(POWER(total_bought * avg_price, 2)), 0) as sum_sq_stakes,
                    COALESCE(SUM(realized_pnl), 0) as total_pnl,
                    COUNT(*) as total_trades_with_pnl,
                    COALESCE(SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END), 0) as winning_trades,
                    COALESCE(MIN(CASE WHEN realized_pnl < 0 THEN realized_pnl ELSE NULL END), 0) as worst_loss
                FROM trader_closed_positions
                WHERE trader_id = :trader_id
            """),
            {"trader_id": trader_id}
        )
        row = result.fetchone()
        
        # Use trader_closed_positions if we have data and total_stakes > 0
        if closed_pos_count > 0 and row and row[0] is not None and row[0] > 0:  # total_stakes > 0
            total_stakes = float(row[0] or 0)
            winning_stakes = float(row[1] or 0)
            sum_sq_stakes = float(row[2] or 0)
            total_pnl = float(row[3] or 0)
            total_trades_with_pnl = int(row[4] or 0)
            winning_trades = int(row[5] or 0)
            worst_loss = float(row[6] or 0)
            
            # Calculate ROI
            roi = (total_pnl / total_stakes * 100) if total_stakes > 0 else 0.0
            
            # Calculate win rate
            win_rate = (winning_trades / total_trades_with_pnl * 100) if total_trades_with_pnl > 0 else 0.0
            
            # Get max_stake (average of top 5)
            result = await session.execute(
                text("""
                    SELECT COALESCE(AVG(stake), 0) as max_stake
                    FROM (
                        SELECT total_bought * avg_price as stake
                        FROM trader_closed_positions
                        WHERE trader_id = :trader_id
                        ORDER BY stake DESC
                        LIMIT 5
                    ) top_stakes
                """),
                {"trader_id": trader_id}
            )
            max_stake_row = result.fetchone()
            max_stake = float(max_stake_row[0] or 0) if max_stake_row and max_stake_row[0] is not None else 0.0
            
            # Get all losses for risk calculation
            result = await session.execute(
                text("""
                    SELECT realized_pnl
                    FROM trader_closed_positions
                    WHERE trader_id = :trader_id AND realized_pnl < 0
                    ORDER BY realized_pnl ASC
                """),
                {"trader_id": trader_id}
            )
            all_losses = [float(row[0]) for row in result.fetchall()]
            
            # Get total trades count
            result = await session.execute(
                text("SELECT COUNT(*) FROM trader_trades WHERE trader_id = :trader_id"),
                {"trader_id": trader_id}
            )
            total_trades = result.scalar() or 0
            
            # Get trader name/pseudonym from trader_leaderboard
            result = await session.execute(
                text("SELECT name, pseudonym, profile_image FROM trader_leaderboard WHERE id = :trader_id"),
                {"trader_id": trader_id}
            )
            trader_info = result.fetchone()
            
            return {
                "wallet_address": wallet_address,
                "name": trader_info[0] if trader_info else None,
                "pseudonym": trader_info[1] if trader_info else None,
                "profile_image": trader_info[2] if trader_info else None,
                "total_pnl": total_pnl,
                "roi": roi,
                "win_rate": win_rate,
                "total_trades": total_trades,
                "total_trades_with_pnl": total_trades_with_pnl,
                "winning_trades": winning_trades,
                "total_stakes": total_stakes,
                "winning_stakes": winning_stakes,
                "worst_loss": worst_loss,
                "all_losses": all_losses,
                "max_stake": max_stake,
                "sum_sq_stakes": sum_sq_stakes,
                "portfolio_value": 0.0
            }
        
        # Fallback 1: Try main tables (ClosedPosition, Trade, etc.)
        # Only try this if trader_closed_positions has no data
        if closed_pos_count == 0:
            metrics = await calculate_trader_metrics_with_time_filter(
                session=session,
                wallet_address=wallet_address,
                period='all'
            )
            
            if metrics:
                return metrics
        
        # Fallback 2: Fetch from APIs and calculate metrics
        # Only fetch from APIs if we have no data in either trader_* tables or main tables
        if closed_pos_count == 0:
            print(f"   📡 Fetching data from APIs for {wallet_address[:10]}... (no DB data found)")
        try:
            closed_positions = await fetch_trader_closed_positions(wallet_address)
            trades = await fetch_trader_trades(wallet_address)
            
            if not closed_positions:
                return None
            
            # Calculate metrics from API data
            total_stakes = Decimal('0')
            winning_stakes = Decimal('0')
            sum_sq_stakes = Decimal('0')
            total_pnl = Decimal('0')
            total_trades_with_pnl = 0
            winning_trades = 0
            worst_loss = Decimal('0')
            all_losses = []
            stakes_list = []
            
            for cp in closed_positions:
                # API returns camelCase keys
                total_bought = Decimal(str(cp.get('totalBought', 0) or 0))
                avg_price = Decimal(str(cp.get('avgPrice', 0) or 0))
                stake = total_bought * avg_price
                
                total_stakes += stake
                sum_sq_stakes += stake ** 2
                stakes_list.append(stake)
                
                realized_pnl = Decimal(str(cp.get('realizedPnl', 0) or 0))
                total_pnl += realized_pnl
                total_trades_with_pnl += 1
                
                if realized_pnl > 0:
                    winning_trades += 1
                    winning_stakes += stake
                
                if realized_pnl < 0:
                    all_losses.append(float(realized_pnl))
                    if worst_loss == Decimal('0') or realized_pnl < worst_loss:
                        worst_loss = realized_pnl
            
            # Calculate max_stake (average of top 5)
            max_stake = Decimal('0')
            if stakes_list:
                sorted_stakes = sorted(stakes_list, reverse=True)
                top_n = min(5, len(sorted_stakes))
                top_stakes = sorted_stakes[:top_n]
                max_stake = sum(top_stakes) / Decimal(str(top_n)) if top_n > 0 else Decimal('0')
            
            # Calculate ROI and win rate
            roi = (total_pnl / total_stakes * 100) if total_stakes > 0 else Decimal('0')
            win_rate = (winning_trades / total_trades_with_pnl * 100) if total_trades_with_pnl > 0 else Decimal('0')
            
            # Get trader info
            trader_name = None
            trader_pseudonym = None
            if trades:
                trader_name = trades[0].get('name')
                trader_pseudonym = trades[0].get('pseudonym')
            
            return {
                "wallet_address": wallet_address,
                "name": trader_name,
                "pseudonym": trader_pseudonym,
                "profile_image": None,
                "total_pnl": float(total_pnl),
                "roi": float(roi),
                "win_rate": float(win_rate),
                "total_trades": len(trades),
                "total_trades_with_pnl": total_trades_with_pnl,
                "winning_trades": winning_trades,
                "total_stakes": float(total_stakes),
                "winning_stakes": float(winning_stakes),
                "worst_loss": float(worst_loss),
                "all_losses": all_losses,
                "max_stake": float(max_stake),
                "sum_sq_stakes": float(sum_sq_stakes),
                "portfolio_value": 0.0
            }
        except Exception as api_error:
            print(f"   ⚠️  API fetch error for {wallet_address}: {api_error}")
            return None
        
    except Exception as e:
        print(f"   ⚠️  Error getting metrics for {wallet_address}: {e}")
        return None


async def calculate_and_store_scores(
    session: AsyncSession,
    limit: Optional[int] = None,
    offset: int = 0,
    force_api_fetch: bool = False
) -> Dict[str, any]:
    """
    Calculate and store scores for all traders in trader_leaderboard.
    
    Args:
        session: Database session
        limit: Maximum number of traders to process (None = all)
        offset: Offset for pagination
    
    Returns:
        Dict with summary statistics
    """
    # Get traders from trader_leaderboard
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
            "calculated": 0,
            "errors": []
        }
    
    print(f"\n📊 Processing {len(traders)} traders...")
    
    # Step 1: Collect metrics for all traders
    all_metrics = []
    processed = 0
    errors = []
    
    # Process in batches with progress updates
    batch_size = 50
    for i in range(0, len(traders), batch_size):
        batch = traders[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (len(traders) + batch_size - 1) // batch_size
        
        print(f"   Processing batch {batch_num}/{total_batches} ({len(batch)} traders)...")
        
        for trader_id, wallet_address in batch:
            try:
                if force_api_fetch:
                    # Force fetch from APIs
                    closed_positions = await fetch_trader_closed_positions(wallet_address)
                    trades = await fetch_trader_trades(wallet_address)
                    
                    if closed_positions:
                        # Calculate from API data (same logic as in get_trader_metrics_from_db)
                        total_stakes = Decimal('0')
                        winning_stakes = Decimal('0')
                        sum_sq_stakes = Decimal('0')
                        total_pnl = Decimal('0')
                        total_trades_with_pnl = 0
                        winning_trades = 0
                        worst_loss = Decimal('0')
                        all_losses = []
                        stakes_list = []
                        
                        for cp in closed_positions:
                            total_bought = Decimal(str(cp.get('totalBought', 0) or 0))
                            avg_price = Decimal(str(cp.get('avgPrice', 0) or 0))
                            stake = total_bought * avg_price
                            
                            total_stakes += stake
                            sum_sq_stakes += stake ** 2
                            stakes_list.append(stake)
                            
                            realized_pnl = Decimal(str(cp.get('realizedPnl', 0) or 0))
                            total_pnl += realized_pnl
                            total_trades_with_pnl += 1
                            
                            if realized_pnl > 0:
                                winning_trades += 1
                                winning_stakes += stake
                            
                            if realized_pnl < 0:
                                all_losses.append(float(realized_pnl))
                                if worst_loss == Decimal('0') or realized_pnl < worst_loss:
                                    worst_loss = realized_pnl
                        
                        max_stake = Decimal('0')
                        if stakes_list:
                            sorted_stakes = sorted(stakes_list, reverse=True)
                            top_n = min(5, len(sorted_stakes))
                            top_stakes = sorted_stakes[:top_n]
                            max_stake = sum(top_stakes) / Decimal(str(top_n)) if top_n > 0 else Decimal('0')
                        
                        roi = (total_pnl / total_stakes * 100) if total_stakes > 0 else Decimal('0')
                        win_rate = (winning_trades / total_trades_with_pnl * 100) if total_trades_with_pnl > 0 else Decimal('0')
                        
                        trader_name = None
                        trader_pseudonym = None
                        if trades:
                            trader_name = trades[0].get('name')
                            trader_pseudonym = trades[0].get('pseudonym')
                        
                        metrics = {
                            "wallet_address": wallet_address,
                            "name": trader_name,
                            "pseudonym": trader_pseudonym,
                            "profile_image": None,
                            "total_pnl": float(total_pnl),
                            "roi": float(roi),
                            "win_rate": float(win_rate),
                            "total_trades": len(trades),
                            "total_trades_with_pnl": total_trades_with_pnl,
                            "winning_trades": winning_trades,
                            "total_stakes": float(total_stakes),
                            "winning_stakes": float(winning_stakes),
                            "worst_loss": float(worst_loss),
                            "all_losses": all_losses,
                            "max_stake": float(max_stake),
                            "sum_sq_stakes": float(sum_sq_stakes),
                            "portfolio_value": 0.0
                        }
                    else:
                        metrics = None
                else:
                    # Use DB first, fallback to API
                    metrics = await get_trader_metrics_from_db(session, wallet_address)
                
                if metrics:
                    metrics['trader_id'] = trader_id
                    all_metrics.append(metrics)
                    processed += 1
                else:
                    errors.append(f"No metrics data for {wallet_address}")
            except Exception as e:
                errors.append(f"Error processing {wallet_address}: {e}")
                continue
        
        # Small delay between batches to avoid rate limiting
        if i + batch_size < len(traders):
            await asyncio.sleep(0.5)
    
    if not all_metrics:
        return {
            "total_traders": len(traders),
            "processed": processed,
            "calculated": 0,
            "errors": errors
        }
    
    print(f"✅ Collected metrics for {len(all_metrics)} traders")
    
    # Step 2: Calculate scores using the same logic as view-all leaderboard
    print("📊 Calculating scores...")
    scored_traders = calculate_scores_and_rank(all_metrics)
    
    print(f"✅ Calculated scores for {len(scored_traders)} traders")
    
    # Step 3: Get percentile anchors for storage
    population_metrics = [t for t in scored_traders if t.get('total_trades', 0) >= 5]
    if not population_metrics:
        population_metrics = scored_traders
    
    w_shrunk_pop = [t.get('W_shrunk', 0.0) for t in population_metrics]
    roi_shrunk_pop = [t.get('roi_shrunk', 0.0) for t in population_metrics]
    pnl_shrunk_pop = [t.get('pnl_shrunk', 0.0) for t in population_metrics]
    
    w_1 = get_percentile_value(w_shrunk_pop, 1.0) if w_shrunk_pop else 0.0
    w_99 = get_percentile_value(w_shrunk_pop, 99.0) if w_shrunk_pop else 0.0
    r_1 = get_percentile_value(roi_shrunk_pop, 1.0) if roi_shrunk_pop else 0.0
    r_99 = get_percentile_value(roi_shrunk_pop, 99.0) if roi_shrunk_pop else 0.0
    p_1 = get_percentile_value(pnl_shrunk_pop, 1.0) if pnl_shrunk_pop else 0.0
    p_99 = get_percentile_value(pnl_shrunk_pop, 99.0) if pnl_shrunk_pop else 0.0
    
    # Step 4: Sort by final_score and assign ranks
    scored_traders.sort(key=lambda x: x.get('final_score', 0.0), reverse=True)
    for rank, trader in enumerate(scored_traders, start=1):
        trader['rank'] = rank
    
    # Step 5: Store in database
    print("💾 Storing scores in database...")
    stored_count = 0
    
    for trader in scored_traders:
        try:
            trader_id = trader.get('trader_id')
            if not trader_id:
                continue
            
            score_data = {
                "trader_id": trader_id,
                "wallet_address": trader.get('wallet_address', '').lower(),
                "rank": trader.get('rank'),
                "total_pnl": Decimal(str(trader.get('total_pnl', 0.0))),
                "roi": Decimal(str(trader.get('roi', 0.0))),
                "win_rate": Decimal(str(trader.get('win_rate', 0.0))),
                "trades": trader.get('total_trades', 0),
                "w_shrunk": Decimal(str(trader.get('W_shrunk', 0.0))),
                "roi_shrunk": Decimal(str(trader.get('roi_shrunk', 0.0))),
                "pnl_shrunk": Decimal(str(trader.get('pnl_shrunk', 0.0))),
                "w_score": Decimal(str(trader.get('score_win_rate', 0.0))),
                "roi_score": Decimal(str(trader.get('score_roi', 0.0))),
                "pnl_score": Decimal(str(trader.get('score_pnl', 0.0))),
                "risk_score": Decimal(str(trader.get('score_risk', 0.0))),
                "final_score": Decimal(str(trader.get('final_score', 0.0))),
                "total_stakes": Decimal(str(trader.get('total_stakes', 0.0))),
                "winning_stakes": Decimal(str(trader.get('winning_stakes', 0.0))),
                "sum_sq_stakes": Decimal(str(trader.get('sum_sq_stakes', 0.0))),
                "max_stake": Decimal(str(trader.get('max_stake', 0.0))),
                "worst_loss": Decimal(str(trader.get('worst_loss', 0.0))),
                "total_trades_with_pnl": trader.get('total_trades_with_pnl', 0),
                "winning_trades": trader.get('winning_trades', 0),
                "w_shrunk_1_percent": Decimal(str(w_1)),
                "w_shrunk_99_percent": Decimal(str(w_99)),
                "roi_shrunk_1_percent": Decimal(str(r_1)),
                "roi_shrunk_99_percent": Decimal(str(r_99)),
                "pnl_shrunk_1_percent": Decimal(str(p_1)),
                "pnl_shrunk_99_percent": Decimal(str(p_99))
            }
            
            if "sqlite" in settings.DATABASE_URL:
                stmt = sqlite_insert(TraderCalculatedScore).values(**score_data)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['trader_id'],
                    set_={
                        "rank": stmt.excluded.rank,
                        "total_pnl": stmt.excluded.total_pnl,
                        "roi": stmt.excluded.roi,
                        "win_rate": stmt.excluded.win_rate,
                        "trades": stmt.excluded.trades,
                        "w_shrunk": stmt.excluded.w_shrunk,
                        "roi_shrunk": stmt.excluded.roi_shrunk,
                        "pnl_shrunk": stmt.excluded.pnl_shrunk,
                        "w_score": stmt.excluded.w_score,
                        "roi_score": stmt.excluded.roi_score,
                        "pnl_score": stmt.excluded.pnl_score,
                        "risk_score": stmt.excluded.risk_score,
                        "final_score": stmt.excluded.final_score,
                        "total_stakes": stmt.excluded.total_stakes,
                        "winning_stakes": stmt.excluded.winning_stakes,
                        "sum_sq_stakes": stmt.excluded.sum_sq_stakes,
                        "max_stake": stmt.excluded.max_stake,
                        "worst_loss": stmt.excluded.worst_loss,
                        "total_trades_with_pnl": stmt.excluded.total_trades_with_pnl,
                        "winning_trades": stmt.excluded.winning_trades,
                        "w_shrunk_1_percent": stmt.excluded.w_shrunk_1_percent,
                        "w_shrunk_99_percent": stmt.excluded.w_shrunk_99_percent,
                        "roi_shrunk_1_percent": stmt.excluded.roi_shrunk_1_percent,
                        "roi_shrunk_99_percent": stmt.excluded.roi_shrunk_99_percent,
                        "pnl_shrunk_1_percent": stmt.excluded.pnl_shrunk_1_percent,
                        "pnl_shrunk_99_percent": stmt.excluded.pnl_shrunk_99_percent,
                        "updated_at": text("CURRENT_TIMESTAMP")
                    }
                )
            else:
                stmt = pg_insert(TraderCalculatedScore).values(**score_data)
                stmt = stmt.on_conflict_do_update(
                    constraint="uq_trader_calculated_score_trader",
                    set_={
                        "rank": stmt.excluded.rank,
                        "total_pnl": stmt.excluded.total_pnl,
                        "roi": stmt.excluded.roi,
                        "win_rate": stmt.excluded.win_rate,
                        "trades": stmt.excluded.trades,
                        "w_shrunk": stmt.excluded.w_shrunk,
                        "roi_shrunk": stmt.excluded.roi_shrunk,
                        "pnl_shrunk": stmt.excluded.pnl_shrunk,
                        "w_score": stmt.excluded.w_score,
                        "roi_score": stmt.excluded.roi_score,
                        "pnl_score": stmt.excluded.pnl_score,
                        "risk_score": stmt.excluded.risk_score,
                        "final_score": stmt.excluded.final_score,
                        "total_stakes": stmt.excluded.total_stakes,
                        "winning_stakes": stmt.excluded.winning_stakes,
                        "sum_sq_stakes": stmt.excluded.sum_sq_stakes,
                        "max_stake": stmt.excluded.max_stake,
                        "worst_loss": stmt.excluded.worst_loss,
                        "total_trades_with_pnl": stmt.excluded.total_trades_with_pnl,
                        "winning_trades": stmt.excluded.winning_trades,
                        "w_shrunk_1_percent": stmt.excluded.w_shrunk_1_percent,
                        "w_shrunk_99_percent": stmt.excluded.w_shrunk_99_percent,
                        "roi_shrunk_1_percent": stmt.excluded.roi_shrunk_1_percent,
                        "roi_shrunk_99_percent": stmt.excluded.roi_shrunk_99_percent,
                        "pnl_shrunk_1_percent": stmt.excluded.pnl_shrunk_1_percent,
                        "pnl_shrunk_99_percent": stmt.excluded.pnl_shrunk_99_percent,
                        "updated_at": text("NOW()")
                    }
                )
            await session.execute(stmt)
            stored_count += 1
            
        except Exception as e:
            errors.append(f"Error storing scores for trader_id {trader.get('trader_id')}: {e}")
            continue
    
    await session.commit()
    
    return {
        "total_traders": len(traders),
        "processed": processed,
        "calculated": len(scored_traders),
        "stored": stored_count,
        "errors": errors[:10]  # Limit errors shown
    }


async def main():
    """Main function."""
    print("="*60)
    print("Trader Score Calculation Script")
    print("="*60)
    
    # Create database engine
    engine = create_async_engine(
        settings.DATABASE_URL,
        echo=False
    )
    
    try:
        # Check and create table
        await check_table_exists(engine)
        
        AsyncSessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=engine,
            class_=AsyncSession
        )
        
        async with AsyncSessionLocal() as session:
            # Check how many traders we have
            result = await session.execute(
                text("SELECT COUNT(*) FROM trader_leaderboard")
            )
            total_traders = result.scalar()
            
            print(f"\n📊 Found {total_traders} traders in trader_leaderboard table")
            
            if total_traders == 0:
                print("⚠️  No traders found. Run fetch_leaderboard_data.py first.")
                return
            
            # Check if trader_closed_positions has data
            result = await session.execute(
                text("SELECT COUNT(*) FROM trader_closed_positions")
            )
            closed_pos_count = result.scalar() or 0
            
            if closed_pos_count == 0:
                print(f"\n⚠️  WARNING: trader_closed_positions table is empty!")
                print(f"   The script will fetch data from APIs for each trader.")
                print(f"   To use database data instead, run:")
                print(f"   python fetch_trader_details.py")
                print(f"   This will populate trader_closed_positions, trader_trades, etc.\n")
            else:
                print(f"✅ Found {closed_pos_count} closed positions in trader_closed_positions table")
                print(f"   Will use database data when available.\n")
            
            # Check command line arguments
            force_api = '--force-api' in sys.argv or '-f' in sys.argv
            
            print(f"\n🚀 Starting score calculation...")
            if force_api:
                print(f"   ⚠️  Force API fetch mode: Will fetch from APIs even if DB data exists")
            print(f"   This will:")
            print(f"   1. Get metrics for each trader")
            print(f"   2. Calculate scores (W_shrunk, ROI_shrunk, PNL_shrunk, Risk, Final)")
            print(f"   3. Store results in trader_calculated_scores table\n")
            
            # Calculate scores
            result = await calculate_and_store_scores(
                session=session,
                limit=None,  # Process all
                offset=0,
                force_api_fetch=force_api
            )
            
            # Print results
            print(f"\n{'='*60}")
            print(f"📊 FINAL STATISTICS")
            print(f"{'='*60}")
            print(f"Total traders:              {result['total_traders']}")
            print(f"Processed (with metrics):   {result['processed']}")
            print(f"Scores calculated:          {result['calculated']}")
            print(f"Stored in database:         {result['stored']}")
            
            if result['errors']:
                print(f"\n⚠️  Errors encountered: {len(result['errors'])}")
                print(f"   First 5 errors:")
                for error in result['errors'][:5]:
                    print(f"   - {error}")
            
            print(f"{'='*60}\n")
        
        print("✅ Script completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
