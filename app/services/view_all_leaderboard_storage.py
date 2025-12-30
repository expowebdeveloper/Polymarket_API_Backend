"""
Service for storing view-all leaderboard data in the database.
This stores leaderboard entries calculated from live Polymarket API wallets.
"""

from typing import List, Dict, Optional, Any
from datetime import datetime
from decimal import Decimal
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import update, delete
from app.db.models import LeaderboardEntry, LeaderboardMetadata
from app.services.live_leaderboard_service import (
    fetch_polymarket_leaderboard_api,
    transform_stats_for_scoring
)
from app.services.leaderboard_service import calculate_scores_and_rank_with_percentiles
from app.services.pnl_median_service import get_pnl_median_from_population
from app.services.polymarket_service import PolymarketService
import asyncio
import logging

logger = logging.getLogger(__name__)


async def calculate_and_store_view_all_leaderboard(
    session: AsyncSession,
    time_period: str = "all",
    order_by: str = "PNL",
    limit: int = 500
) -> Dict[str, Any]:
    """
    Calculate view-all leaderboard metrics from live API wallets and store them in the database.
    
    Args:
        session: Database session
        time_period: Time period for fetching wallets from live API
        order_by: Order by metric for fetching wallets
        limit: Maximum number of wallets to fetch from live API
    
    Returns:
        Dict with statistics about the operation
    """
    stats = {
        "processed": 0,
        "updated": 0,
        "created": 0,
        "errors": 0,
        "wallets_fetched": 0
    }
    
    try:
        logger.info(f"🔄 Starting view-all leaderboard calculation and storage...")
        logger.info(f"   Fetching wallets from live API (time_period={time_period}, order_by={order_by}, limit={limit})")
        
        # Step 1: Fetch wallet addresses from live leaderboard API
        live_api_data = await fetch_polymarket_leaderboard_api(
            time_period=time_period,
            order_by=order_by,
            limit=limit,
            offset=0,
            category="overall"
        )
        
        if not live_api_data:
            logger.warning("No data fetched from live API")
            return stats
        
        stats["wallets_fetched"] = len(live_api_data)
        logger.info(f"   Fetched {len(live_api_data)} wallets from live API")
        
        # Extract wallet addresses and preserve name/pseudonym/profile_image
        wallet_info_map = {}
        wallet_addresses = []
        for entry in live_api_data:
            wallet = entry.get("proxyWallet") or entry.get("wallet_address") or entry.get("wallet")
            if wallet and wallet.startswith("0x") and len(wallet) == 42:
                wallet_addresses.append(wallet)
                wallet_info_map[wallet] = {
                    "name": entry.get("userName") or entry.get("name") or None,
                    "pseudonym": entry.get("xUsername") or entry.get("pseudonym") or None,
                    "profile_image": entry.get("profileImage") or entry.get("profile_image") or None
                }
        
        if not wallet_addresses:
            logger.warning("No valid wallet addresses extracted from live API")
            return stats
        
        logger.info(f"   Extracted {len(wallet_addresses)} valid wallet addresses")
        
        # Step 2: Fetch metrics for all wallets
        entries_data = []
        semaphore = asyncio.Semaphore(5)  # Limit concurrency
        
        async def fetch_wallet_metrics(wallet: str):
            async with semaphore:
                try:
                    stats_result = await PolymarketService.calculate_portfolio_stats(wallet)
                    if stats_result is None:
                        return None
                    transformed = transform_stats_for_scoring(stats_result)
                    if transformed:
                        # Merge name/pseudonym/profile_image from live API
                        wallet_info = wallet_info_map.get(wallet, {})
                        transformed["name"] = wallet_info.get("name")
                        transformed["pseudonym"] = wallet_info.get("pseudonym")
                        transformed["profile_image"] = wallet_info.get("profile_image")
                    return transformed
                except Exception as e:
                    logger.warning(f"Error fetching stats for {wallet}: {e}")
                    return None
        
        # Process wallets in batches
        batch_size = 50
        for i in range(0, len(wallet_addresses), batch_size):
            batch = wallet_addresses[i:i + batch_size]
            tasks = [fetch_wallet_metrics(wallet) for wallet in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if result and isinstance(result, dict):
                    entries_data.append(result)
            
            # Small delay between batches
            if i + batch_size < len(wallet_addresses):
                await asyncio.sleep(0.1)
        
        if not entries_data:
            logger.warning("No metrics calculated for any wallets")
            stats["errors"] = len(wallet_addresses)
            return stats
        
        logger.info(f"   Calculated metrics for {len(entries_data)} wallets")
        
        # Step 3: Calculate scores with percentiles
        pnl_median_api = await get_pnl_median_from_population()
        result = calculate_scores_and_rank_with_percentiles(
            entries_data,
            pnl_median=pnl_median_api
        )
        
        traders = result["traders"]
        percentiles_data = result["percentiles"]
        medians_data = result["medians"]
        medians_data["pnl_median"] = pnl_median_api
        
        logger.info(f"   Calculated scores for {len(traders)} traders")
        
        # Step 4: Store/update leaderboard entries
        for trader_data in traders:
            try:
                wallet_address = trader_data.get("wallet_address")
                if not wallet_address:
                    continue
                
                # Check if entry exists
                stmt = select(LeaderboardEntry).where(LeaderboardEntry.wallet_address == wallet_address)
                result = await session.execute(stmt)
                existing_entry = result.scalar_one_or_none()
                
                # Prepare data for insertion/update
                entry_data = {
                    "name": trader_data.get("name"),
                    "pseudonym": trader_data.get("pseudonym"),
                    "profile_image": trader_data.get("profile_image"),
                    "total_pnl": Decimal(str(trader_data.get("total_pnl", 0.0))),
                    "roi": Decimal(str(trader_data.get("roi", 0.0))),
                    "win_rate": Decimal(str(trader_data.get("win_rate", 0.0))),
                    "total_trades": trader_data.get("total_trades", 0),
                    "total_trades_with_pnl": trader_data.get("total_trades_with_pnl", 0),
                    "winning_trades": trader_data.get("winning_trades", 0),
                    "w_shrunk": Decimal(str(trader_data.get("W_shrunk", 0.0))),
                    "roi_shrunk": Decimal(str(trader_data.get("roi_shrunk", 0.0))),
                    "pnl_shrunk": Decimal(str(trader_data.get("pnl_shrunk", 0.0))),
                    "score_win_rate": Decimal(str(trader_data.get("score_win_rate", 0.0))),
                    "score_roi": Decimal(str(trader_data.get("score_roi", 0.0))),
                    "score_pnl": Decimal(str(trader_data.get("score_pnl", 0.0))),
                    "score_risk": Decimal(str(trader_data.get("score_risk", 0.0))),
                    "final_score": Decimal(str(trader_data.get("final_score", 0.0))),
                    "total_stakes": Decimal(str(trader_data.get("total_stakes", 0.0))),
                    "winning_stakes": Decimal(str(trader_data.get("winning_stakes", 0.0))),
                    "sum_sq_stakes": Decimal(str(trader_data.get("sum_sq_stakes", 0.0))),
                    "max_stake": Decimal(str(trader_data.get("max_stake", 0.0))),
                    "worst_loss": Decimal(str(trader_data.get("worst_loss", 0.0))),
                    "population_size": len(traders),  # Use actual traders count
                    "calculated_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow()
                }
                
                if existing_entry:
                    # Update existing entry
                    stmt = update(LeaderboardEntry).where(
                        LeaderboardEntry.wallet_address == wallet_address
                    ).values(**entry_data)
                    await session.execute(stmt)
                    stats["updated"] += 1
                else:
                    # Create new entry
                    new_entry = LeaderboardEntry(
                        wallet_address=wallet_address,
                        **entry_data,
                        created_at=datetime.utcnow()
                    )
                    session.add(new_entry)
                    stats["created"] += 1
                
                stats["processed"] += 1
                
            except Exception as e:
                logger.error(f"Error storing entry for {wallet_address}: {e}", exc_info=True)
                stats["errors"] += 1
        
        # Step 5: Store/update metadata
        try:
            # Delete old metadata
            await session.execute(delete(LeaderboardMetadata))
            
            # Create new metadata
            metadata = LeaderboardMetadata(
                w_shrunk_1_percent=Decimal(str(percentiles_data["w_shrunk_1_percent"])),
                w_shrunk_99_percent=Decimal(str(percentiles_data["w_shrunk_99_percent"])),
                roi_shrunk_1_percent=Decimal(str(percentiles_data["roi_shrunk_1_percent"])),
                roi_shrunk_99_percent=Decimal(str(percentiles_data["roi_shrunk_99_percent"])),
                pnl_shrunk_1_percent=Decimal(str(percentiles_data["pnl_shrunk_1_percent"])),
                pnl_shrunk_99_percent=Decimal(str(percentiles_data["pnl_shrunk_99_percent"])),
                roi_median=Decimal(str(medians_data["roi_median"])),
                pnl_median=Decimal(str(medians_data["pnl_median"])),
                population_size=percentiles_data.get("population_size", len(traders)),
                total_traders=len(traders),
                calculated_at=datetime.utcnow(),
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            session.add(metadata)
            
            logger.info("   Stored leaderboard metadata")
        except Exception as e:
            logger.error(f"Error storing metadata: {e}", exc_info=True)
            stats["errors"] += 1
        
        # Commit all changes
        await session.commit()
        
        logger.info(f"✅ View-all leaderboard storage complete!")
        logger.info(f"   Processed: {stats['processed']}")
        logger.info(f"   Created: {stats['created']}")
        logger.info(f"   Updated: {stats['updated']}")
        logger.info(f"   Errors: {stats['errors']}")
        
        return stats
        
    except Exception as e:
        logger.error(f"❌ Error in view-all leaderboard calculation: {e}", exc_info=True)
        await session.rollback()
        raise


async def get_view_all_leaderboard_from_db(
    session: AsyncSession,
    limit: int = 100,
    offset: int = 0
) -> Optional[Dict[str, Any]]:
    """
    Retrieve view-all leaderboard data from database.
    
    Args:
        session: Database session
        limit: Maximum number of entries to return
        offset: Offset for pagination
    
    Returns:
        Dict with leaderboard data or None if no data exists
    """
    try:
        # Get metadata
        stmt = select(LeaderboardMetadata).order_by(LeaderboardMetadata.calculated_at.desc()).limit(1)
        result = await session.execute(stmt)
        metadata = result.scalar_one_or_none()
        
        if not metadata:
            return None
        
        # Get all entries
        stmt = select(LeaderboardEntry).order_by(LeaderboardEntry.final_score.desc())
        result = await session.execute(stmt)
        all_entries = result.scalars().all()
        
        if not all_entries:
            return None
        
        # Convert to dict format
        entries_dict = []
        for entry in all_entries:
            entries_dict.append({
                "wallet_address": entry.wallet_address,
                "name": entry.name,
                "pseudonym": entry.pseudonym,
                "profile_image": entry.profile_image,
                "total_pnl": float(entry.total_pnl),
                "roi": float(entry.roi),
                "win_rate": float(entry.win_rate),
                "total_trades": entry.total_trades,
                "total_trades_with_pnl": entry.total_trades_with_pnl,
                "winning_trades": entry.winning_trades,
                "total_stakes": float(entry.total_stakes),
                "W_shrunk": float(entry.w_shrunk),
                "roi_shrunk": float(entry.roi_shrunk),
                "pnl_shrunk": float(entry.pnl_shrunk),
                "score_win_rate": float(entry.score_win_rate),
                "score_roi": float(entry.score_roi),
                "score_pnl": float(entry.score_pnl),
                "score_risk": float(entry.score_risk),
                "final_score": float(entry.final_score)
            })
        
        # Create leaderboards sorted by different metrics
        leaderboards = {}
        
        # W_shrunk (ascending)
        w_shrunk_sorted = sorted(entries_dict, key=lambda x: x.get("W_shrunk", float('inf')))
        for i, trader in enumerate(w_shrunk_sorted, 1):
            trader["rank"] = i
        leaderboards["w_shrunk"] = w_shrunk_sorted[offset:offset + limit]
        
        # ROI raw (descending)
        roi_raw_sorted = sorted(entries_dict, key=lambda x: x.get("roi", float('-inf')), reverse=True)
        for i, trader in enumerate(roi_raw_sorted, 1):
            trader["rank"] = i
        leaderboards["roi_raw"] = roi_raw_sorted[offset:offset + limit]
        
        # ROI shrunk (ascending)
        roi_shrunk_sorted = sorted(entries_dict, key=lambda x: x.get("roi_shrunk", float('inf')))
        for i, trader in enumerate(roi_shrunk_sorted, 1):
            trader["rank"] = i
        leaderboards["roi_shrunk"] = roi_shrunk_sorted[offset:offset + limit]
        
        # PNL shrunk (ascending)
        pnl_shrunk_sorted = sorted(entries_dict, key=lambda x: x.get("pnl_shrunk", float('inf')))
        for i, trader in enumerate(pnl_shrunk_sorted, 1):
            trader["rank"] = i
        leaderboards["pnl_shrunk"] = pnl_shrunk_sorted[offset:offset + limit]
        
        # Scores (descending)
        win_rate_sorted = sorted(entries_dict, key=lambda x: x.get("score_win_rate", 0), reverse=True)
        for i, trader in enumerate(win_rate_sorted, 1):
            trader["rank"] = i
        leaderboards["score_win_rate"] = win_rate_sorted[offset:offset + limit]
        
        roi_score_sorted = sorted(entries_dict, key=lambda x: x.get("score_roi", 0), reverse=True)
        for i, trader in enumerate(roi_score_sorted, 1):
            trader["rank"] = i
        leaderboards["score_roi"] = roi_score_sorted[offset:offset + limit]
        
        pnl_score_sorted = sorted(entries_dict, key=lambda x: x.get("score_pnl", 0), reverse=True)
        for i, trader in enumerate(pnl_score_sorted, 1):
            trader["rank"] = i
        leaderboards["score_pnl"] = pnl_score_sorted[offset:offset + limit]
        
        risk_sorted = sorted(entries_dict, key=lambda x: x.get("score_risk", 0), reverse=True)
        for i, trader in enumerate(risk_sorted, 1):
            trader["rank"] = i
        leaderboards["score_risk"] = risk_sorted[offset:offset + limit]
        
        final_score_sorted = sorted(entries_dict, key=lambda x: x.get("final_score", 0), reverse=True)
        for i, trader in enumerate(final_score_sorted, 1):
            trader["rank"] = i
        leaderboards["final_score"] = final_score_sorted[offset:offset + limit]
        
        return {
            "percentiles": {
                "w_shrunk_1_percent": float(metadata.w_shrunk_1_percent),
                "w_shrunk_99_percent": float(metadata.w_shrunk_99_percent),
                "roi_shrunk_1_percent": float(metadata.roi_shrunk_1_percent),
                "roi_shrunk_99_percent": float(metadata.roi_shrunk_99_percent),
                "pnl_shrunk_1_percent": float(metadata.pnl_shrunk_1_percent),
                "pnl_shrunk_99_percent": float(metadata.pnl_shrunk_99_percent),
                "population_size": metadata.population_size
            },
            "medians": {
                "roi_median": float(metadata.roi_median),
                "pnl_median": float(metadata.pnl_median)
            },
            "leaderboards": leaderboards,
            "total_traders": metadata.total_traders,
            "population_traders": metadata.population_size
        }
        
    except Exception as e:
        logger.error(f"Error retrieving view-all leaderboard from DB: {e}", exc_info=True)
        return None
