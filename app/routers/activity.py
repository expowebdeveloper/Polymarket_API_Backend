"""Activity API routes."""

from fastapi import APIRouter, HTTPException, Query, status, Depends
from typing import Optional
from app.schemas.activity import ActivitiesListResponse, ActivityResponse
from app.schemas.general import ErrorResponse
from app.services.activity_service import fetch_and_save_activities, get_activities_from_db
from app.db.session import get_db
from sqlalchemy.ext.asyncio import AsyncSession
from decimal import Decimal

router = APIRouter(prefix="/activity", tags=["Activity"])


def validate_wallet(wallet_address: str) -> bool:
    """Validate wallet address format."""
    if not wallet_address:
        return False
    if not wallet_address.startswith("0x"):
        return False
    if len(wallet_address) != 42:
        return False
    try:
        int(wallet_address[2:], 16)
        return True
    except:
        return False


@router.get(
    "",
    response_model=ActivitiesListResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid wallet address"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Fetch and save user activity",
    description="Fetch user activity from Polymarket API and save it to the database"
)
async def fetch_and_save_activity_endpoint(
    user: str = Query(
        ...,
        description="Wallet address to fetch activity for (must be 42 characters starting with 0x)",
        example="0x17db3fcd93ba12d38382a0cade24b200185c5f6d",
        min_length=42,
        max_length=42
    ),
    db: AsyncSession = Depends(get_db)
):
    """
    Fetch user activity from Polymarket Data API and save it to the database.
    
    This endpoint:
    1. Validates the wallet address format
    2. Fetches activity from https://data-api.polymarket.com/activity?user={wallet}
    3. Saves each activity to the database (updates if already exists)
    4. Returns the fetched activities
    
    Args:
        user: Wallet address (query parameter)
        db: Database session (injected)
    
    Returns:
        ActivitiesListResponse with wallet address, count, and list of activities
    """
    if not validate_wallet(user):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid wallet address format: {user}. Must be 42 characters starting with 0x"
        )
    
    try:
        # Fetch activities from API and save to database
        activities_data, saved_count = await fetch_and_save_activities(db, user)
        
        # Convert to response format
        activities_response = []
        for activity in activities_data:
            activities_response.append(ActivityResponse(
                proxy_wallet=activity.get("proxyWallet", user),
                timestamp=activity.get("timestamp", 0),
                condition_id=activity.get("conditionId"),
                type=activity.get("type", ""),
                size=Decimal(str(activity.get("size", 0))),
                usdc_size=Decimal(str(activity.get("usdcSize", 0))),
                transaction_hash=activity.get("transactionHash", ""),
                price=Decimal(str(activity.get("price", 0))),
                asset=activity.get("asset"),
                side=activity.get("side"),
                outcome_index=activity.get("outcomeIndex") if activity.get("outcomeIndex") != 999 else None,
                title=activity.get("title"),
                slug=activity.get("slug"),
                icon=activity.get("icon"),
                event_slug=activity.get("eventSlug"),
                outcome=activity.get("outcome"),
                name=activity.get("name"),
                pseudonym=activity.get("pseudonym"),
                bio=activity.get("bio"),
                profile_image=activity.get("profileImage"),
                profile_image_optimized=activity.get("profileImageOptimized"),
            ))
        
        return ActivitiesListResponse(
            wallet_address=user,
            count=len(activities_response),
            activities=activities_response
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching and saving activity: {str(e)}"
        )


@router.get(
    "/from-db",
    response_model=ActivitiesListResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid wallet address"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Get user activity from database",
    description="Retrieve user activity from the database (without fetching from API)"
)
async def get_activity_from_db_endpoint(
    user: str = Query(
        ...,
        description="Wallet address to get activity for (must be 42 characters starting with 0x)",
        example="0x17db3fcd93ba12d38382a0cade24b200185c5f6d",
        min_length=42,
        max_length=42
    ),
    type: Optional[str] = Query(
        None,
        description="Filter by activity type (TRADE, REDEEM, REWARD, etc.)"
    ),
    limit: Optional[int] = Query(
        None,
        ge=1,
        description="Maximum number of activities to return"
    ),
    db: AsyncSession = Depends(get_db)
):
    """
    Get user activity from the database with optional filters.
    
    This endpoint retrieves activities that were previously saved to the database.
    Use the main /activity endpoint to fetch fresh data from the API.
    
    Args:
        user: Wallet address (query parameter)
        type: Filter by activity type (optional)
        limit: Maximum number of activities to return (optional)
        db: Database session (injected)
    
    Returns:
        ActivitiesListResponse with wallet address, count, and list of activities
    """
    if not validate_wallet(user):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid wallet address format: {user}. Must be 42 characters starting with 0x"
        )
    
    try:
        # Get activities from database
        activities = await get_activities_from_db(db, user, activity_type=type, limit=limit)
        
        # Convert to response format
        activities_response = []
        for activity in activities:
            activities_response.append(ActivityResponse(
                proxy_wallet=activity.proxy_wallet,
                timestamp=activity.timestamp,
                condition_id=activity.condition_id,
                type=activity.type,
                size=activity.size,
                usdc_size=activity.usdc_size,
                transaction_hash=activity.transaction_hash,
                price=activity.price,
                asset=activity.asset,
                side=activity.side,
                outcome_index=activity.outcome_index,
                title=activity.title,
                slug=activity.slug,
                icon=activity.icon,
                event_slug=activity.event_slug,
                outcome=activity.outcome,
                name=activity.name,
                pseudonym=activity.pseudonym,
                bio=activity.bio,
                profile_image=activity.profile_image,
                profile_image_optimized=activity.profile_image_optimized,
            ))
        
        return ActivitiesListResponse(
            wallet_address=user,
            count=len(activities_response),
            activities=activities_response
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving activity from database: {str(e)}"
        )


