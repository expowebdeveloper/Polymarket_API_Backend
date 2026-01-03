"""
Service for calculating and storing leaderboard entries in the database.
This allows fast retrieval without recalculating on every request.
"""

from typing import List, Dict, Optional, Any
from datetime import datetime
from decimal import Decimal
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import update, delete
from app.db.models import LeaderboardEntry
from app.services.leaderboard_service import (
    get_unique_wallet_addresses,
    calculate_trader_metrics_with_time_filter
)
from app.services.db_scoring_service import get_advanced_db_analytics
from app.services.pnl_median_service import calculate_pnl_median_from_traders
import asyncio

async def calculate_and_store_leaderboard_entries(
    session: AsyncSession,
    wallet_addresses: Optional[List[str]] = None,
    max_traders: Optional[int] = None
) -> Dict[str, Any]:
    """
    Calculate leaderboard metrics for all traders and store them in the database.
    
    Args:
        session: Database session
        wallet_addresses: Optional list of wallet addresses to process. If None, processes all.
        max_traders: Optional limit on number of traders to process
    
    Returns:
        Dict with statistics about the operation
    """
    # 1. Get wallet addresses
    if not wallet_addresses:
        wallet_addresses = await get_unique_wallet_addresses(session)
    
    if not wallet_addresses:
        return {
            "processed": 0,
            "updated": 0,
            "created": 0,
            "errors": 0
        }
    
    # Limit traders if specified
    traders_to_process = wallet_addresses
    if max_traders and len(wallet_addresses) > max_traders:
        traders_to_process = wallet_addresses[:max_traders]
    
    # 2. Calculate metrics for all traders using the existing service
    import logging
    logger = logging.getLogger(__name__)
    logger.info(f"Processing {len(traders_to_process)} traders for leaderboard calculation...")
    
    result = await get_advanced_db_analytics(
        session,
        wallet_addresses=traders_to_process,
        max_traders=None  # Already limited above
    )
    
    traders = result.get("traders", [])
    total_wallets = len(traders_to_process)
    successful_count = len(traders)
    failed_count = total_wallets - successful_count
    
    logger.info(f"Successfully calculated metrics for {successful_count} traders out of {total_wallets}")
    
    if not traders:
        # If no traders were successfully calculated, count all as errors
        logger.warning(f"No traders were successfully calculated. All {failed_count} traders failed.")
        return {
            "processed": 0,
            "updated": 0,
            "created": 0,
            "errors": failed_count
        }
    
    # Track initial error count from failed calculations
    initial_errors = failed_count
    
    # 3. Store/update each trader's leaderboard entry
    stats = {
        "processed": 0,
        "updated": 0,
        "created": 0,
        "errors": initial_errors  # Start with errors from failed calculations
    }
    
    for trader_data in traders:
        try:
            wallet_address = trader_data.get("wallet_address")
            if not wallet_address:
                continue
            
            # Check if entry exists
            stmt = select(LeaderboardEntry).where(LeaderboardEntry.wallet_address == wallet_address)
            db_result = await session.execute(stmt)
            existing_entry = db_result.scalar_one_or_none()
            
            # Prepare data for storage
            entry_data = {
                "name": trader_data.get("name"),
                "pseudonym": trader_data.get("pseudonym"),
                "profile_image": trader_data.get("profile_image"),
                "total_pnl": Decimal(str(trader_data.get("total_pnl", 0))),
                "roi": Decimal(str(trader_data.get("roi", 0))),
                "win_rate": Decimal(str(trader_data.get("win_rate", 0))),
                "total_trades": trader_data.get("total_trades", 0),
                "total_trades_with_pnl": trader_data.get("total_trades_with_pnl", trader_data.get("total_trades", 0)),
                "winning_trades": trader_data.get("winning_trades", 0),
                "w_shrunk": Decimal(str(trader_data.get("W_shrunk", 0))),
                "roi_shrunk": Decimal(str(trader_data.get("roi_shrunk", 0))),
                "pnl_shrunk": Decimal(str(trader_data.get("pnl_shrunk", 0))),
                "score_win_rate": Decimal(str(trader_data.get("score_win_rate", 0))),
                "score_roi": Decimal(str(trader_data.get("score_roi", 0))),
                "score_pnl": Decimal(str(trader_data.get("score_pnl", 0))),
                "score_risk": Decimal(str(trader_data.get("score_risk", 0))),
                "final_score": Decimal(str(trader_data.get("final_score", 0))),
                "total_stakes": Decimal(str(trader_data.get("total_stakes", 0))),
                "winning_stakes": Decimal(str(trader_data.get("winning_stakes", 0))),
                "sum_sq_stakes": Decimal(str(trader_data.get("sum_sq_stakes", 0))),
                "max_stake": Decimal(str(trader_data.get("max_stake", 0))),
                "worst_loss": Decimal(str(trader_data.get("worst_loss", 0))),
                "population_size": result.get("population_size", 0),  # From analytics result
                "calculated_at": datetime.utcnow(),
                "updated_at": datetime.utcnow()
            }
            
            if existing_entry:
                # Update existing entry
                for key, value in entry_data.items():
                    setattr(existing_entry, key, value)
                stats["updated"] += 1
            else:
                # Create new entry
                new_entry = LeaderboardEntry(
                    wallet_address=wallet_address,
                    **entry_data
                )
                session.add(new_entry)
                stats["created"] += 1
            
            stats["processed"] += 1
            
            # Commit in batches to avoid long transactions
            if stats["processed"] % 50 == 0:
                try:
                    await session.commit()
                except Exception as commit_error:
                    # If commit fails, rollback and continue
                    try:
                        await session.rollback()
                    except Exception:
                        pass
                    stats["errors"] += 1
                    continue
        
        except Exception as e:
            stats["errors"] += 1
            # Log error details for debugging (only first 10 to avoid spam)
            if stats["errors"] <= 10:
                import logging
                logger = logging.getLogger(__name__)
                logger.warning(f"Error processing trader {wallet_address}: {str(e)}")
            # Rollback on error to prevent transaction failure
            try:
                await session.rollback()
            except Exception:
                pass
            continue
    
    # Final commit for any remaining changes
    try:
        await session.commit()
    except Exception as commit_error:
        try:
            await session.rollback()
        except Exception:
            pass
        # Continue to metadata even if commit failed
    
    # 4. Store metadata (percentiles and medians)
    from app.db.models import LeaderboardMetadata
    percentiles_data = result.get("percentiles", {})
    medians_data = result.get("medians", {})
    
    # Check if metadata exists
    try:
        stmt = select(LeaderboardMetadata).order_by(LeaderboardMetadata.id.desc()).limit(1)
        db_result = await session.execute(stmt)
        existing_metadata = db_result.scalar_one_or_none()
    except Exception as metadata_error:
        # If transaction is in failed state, rollback and retry
        try:
            await session.rollback()
            stmt = select(LeaderboardMetadata).order_by(LeaderboardMetadata.id.desc()).limit(1)
            db_result = await session.execute(stmt)
            existing_metadata = db_result.scalar_one_or_none()
        except Exception:
            existing_metadata = None
    
    metadata_data = {
        "w_shrunk_1_percent": Decimal(str(percentiles_data.get("w_shrunk_1_percent", 0))),
        "w_shrunk_99_percent": Decimal(str(percentiles_data.get("w_shrunk_99_percent", 0))),
        "roi_shrunk_1_percent": Decimal(str(percentiles_data.get("roi_shrunk_1_percent", 0))),
        "roi_shrunk_99_percent": Decimal(str(percentiles_data.get("roi_shrunk_99_percent", 0))),
        "pnl_shrunk_1_percent": Decimal(str(percentiles_data.get("pnl_shrunk_1_percent", 0))),
        "pnl_shrunk_99_percent": Decimal(str(percentiles_data.get("pnl_shrunk_99_percent", 0))),
        "roi_median": Decimal(str(medians_data.get("roi_median", 0))),
        "pnl_median": Decimal(str(medians_data.get("pnl_median", 0))),
        "population_size": result.get("population_size", 0),
        "total_traders": result.get("total_traders", len(traders)),
        "calculated_at": datetime.utcnow(),
        "updated_at": datetime.utcnow()
    }
    
    try:
        if existing_metadata:
            for key, value in metadata_data.items():
                setattr(existing_metadata, key, value)
        else:
            new_metadata = LeaderboardMetadata(**metadata_data)
            session.add(new_metadata)
        
        # Final commit for metadata
        await session.commit()
    except Exception as metadata_commit_error:
        # If commit fails, rollback
        try:
            await session.rollback()
        except Exception:
            pass
        # Don't fail the whole operation if metadata commit fails
    
    return stats


async def get_leaderboard_from_db(
    session: AsyncSession,
    limit: int = 100,
    offset: int = 0,
    sort_by: str = "final_score",
    sort_desc: bool = True
) -> List[Dict[str, Any]]:
    """
    Get leaderboard entries from database.
    
    Args:
        session: Database session
        limit: Maximum number of entries to return
        offset: Offset for pagination
        sort_by: Field to sort by (default: "final_score")
        sort_desc: Whether to sort descending (default: True)
    
    Returns:
        List of leaderboard entry dictionaries
    """
    try:
        # Build query
        stmt = select(LeaderboardEntry)
        
        # Add sorting
        sort_column = getattr(LeaderboardEntry, sort_by, LeaderboardEntry.final_score)
        if sort_desc:
            stmt = stmt.order_by(sort_column.desc())
        else:
            stmt = stmt.order_by(sort_column.asc())
        
        # Add pagination
        stmt = stmt.offset(offset).limit(limit)
        
        result = await session.execute(stmt)
        entries = result.scalars().all()
        
        # Convert to dictionaries with proper rank
        leaderboard = []
        for idx, entry in enumerate(entries):
            leaderboard.append({
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
                "final_score": float(entry.final_score),
                "rank": offset + idx + 1  # Calculate rank based on offset and index
            })
        
        return leaderboard
    except Exception as e:
        # If transaction is in failed state, rollback and return empty list
        try:
            await session.rollback()
        except Exception:
            pass
        # Re-raise to let caller handle it
        raise


async def get_total_leaderboard_count(session: AsyncSession) -> int:
    """Get total number of leaderboard entries in database."""
    from sqlalchemy import func
    try:
        stmt = select(func.count(LeaderboardEntry.id))
        result = await session.execute(stmt)
        return result.scalar() or 0
    except Exception as e:
        # If transaction is in failed state, rollback and return 0
        try:
            await session.rollback()
        except Exception:
            pass
        return 0


async def get_leaderboard_metadata(session: AsyncSession) -> Optional[Dict[str, Any]]:
    """Get stored leaderboard metadata (percentiles and medians)."""
    from app.db.models import LeaderboardMetadata
    try:
        stmt = select(LeaderboardMetadata).order_by(LeaderboardMetadata.id.desc()).limit(1)
        result = await session.execute(stmt)
        metadata = result.scalar_one_or_none()
        
        if not metadata:
            return None
        
        return {
            "w_shrunk_1_percent": float(metadata.w_shrunk_1_percent),
            "w_shrunk_99_percent": float(metadata.w_shrunk_99_percent),
            "roi_shrunk_1_percent": float(metadata.roi_shrunk_1_percent),
            "roi_shrunk_99_percent": float(metadata.roi_shrunk_99_percent),
            "pnl_shrunk_1_percent": float(metadata.pnl_shrunk_1_percent),
            "pnl_shrunk_99_percent": float(metadata.pnl_shrunk_99_percent),
            "roi_median": float(metadata.roi_median),
            "pnl_median": float(metadata.pnl_median),
            "population_size": metadata.population_size,
            "total_traders": metadata.total_traders
        }
    except Exception as e:
        # If transaction is in failed state, rollback and return None
        try:
            await session.rollback()
        except Exception:
            pass
        return None

