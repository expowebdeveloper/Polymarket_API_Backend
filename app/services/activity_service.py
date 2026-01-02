"""Activity service for saving and retrieving user activity."""

from typing import List, Dict, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from app.db.models import Activity
from app.services.data_fetcher import fetch_user_activity
from decimal import Decimal


async def save_activities_to_db(
    session: AsyncSession,
    wallet_address: str,
    activities: List[Dict]
) -> int:
    """
    Save activities to database. Updates existing activities or inserts new ones.
    
    Args:
        session: Database session
        wallet_address: Wallet address
        activities: List of activity dictionaries from API
    
    Returns:
        Number of activities saved
    """
    saved_count = 0
    
    for activity_data in activities:
        # Convert activity data to database model
        activity_dict = {
            "proxy_wallet": activity_data.get("proxyWallet", wallet_address),
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
        }
        
        # Use PostgreSQL upsert (INSERT ... ON CONFLICT DO UPDATE)
        # Conflict on unique combination of proxy_wallet, transaction_hash, timestamp, and condition_id
        stmt = pg_insert(Activity).values(**activity_dict)
        
        # Build update dict with careful handling of values
        update_dict = {
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
            "updated_at": stmt.excluded.updated_at,
        }

        stmt = stmt.on_conflict_do_update(
            constraint="uq_activity_unique",
            set_=update_dict
        )
        
        try:
            await session.execute(stmt)
            saved_count += 1
        except Exception as e:
            # If explicit unique violation or other DB error for just this row, skip it
            # Log specific details to help debug uniqueness issues
            print(f"⚠️ Activity save error (skipping): {str(e)[:200]}")
            continue
    
    return saved_count


async def get_activities_from_db(
    session: AsyncSession,
    wallet_address: str,
    activity_type: Optional[str] = None,
    limit: Optional[int] = None
) -> List[Activity]:
    """
    Get activities from database for a wallet address.
    
    Args:
        session: Database session
        wallet_address: Wallet address
        activity_type: Filter by activity type (TRADE, REDEEM, REWARD, etc.) - optional
        limit: Maximum number of activities to return - optional
    
    Returns:
        List of Activity objects, ordered by timestamp descending
    """
    stmt = select(Activity).where(Activity.proxy_wallet == wallet_address)
    
    if activity_type:
        stmt = stmt.where(Activity.type == activity_type)
    
    stmt = stmt.order_by(Activity.timestamp.desc())
    
    if limit:
        stmt = stmt.limit(limit)
    
    result = await session.execute(stmt)
    return result.scalars().all()


async def fetch_and_save_activities(
    session: AsyncSession,
    wallet_address: str,
    activity_type: Optional[str] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None
) -> tuple[List[Dict], int]:
    """
    Fetch activities from API and save to database.
    
    Args:
        session: Database session
        wallet_address: Wallet address
        activity_type: Optional activity type filter
        limit: Optional limit
        offset: Optional offset
    
    Returns:
        Tuple of (activities list, saved count)
    """
    activities = await fetch_user_activity(
        wallet_address, 
        activity_type=activity_type, 
        limit=limit, 
        offset=offset
    )
    
    # Save to database
    saved_count = await save_activities_to_db(session, wallet_address, activities)
    
    return activities, saved_count


