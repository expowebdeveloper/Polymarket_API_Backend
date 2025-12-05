"""User PnL API routes."""

from fastapi import APIRouter, HTTPException, Query, status, Depends
from typing import Optional
from app.schemas.pnl import UserPnLResponse, PnLDataPoint
from app.schemas.pnl_calculation import PnLCalculationResponse
from app.schemas.general import ErrorResponse
from app.services.pnl_service import fetch_and_save_pnl, get_pnl_from_db
from app.services.pnl_calculator_service import calculate_user_pnl
from app.db.session import get_db
from sqlalchemy.ext.asyncio import AsyncSession
from decimal import Decimal

router = APIRouter(prefix="/pnl", tags=["User PnL"])


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
    response_model=UserPnLResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid wallet address"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Fetch and save user PnL",
    description="Fetch user PnL data from Polymarket API and save it to the database"
)
async def fetch_and_save_pnl_endpoint(
    user_address: str = Query(
        ...,
        description="Wallet address to fetch PnL for (must be 42 characters starting with 0x)",
        example="0x554ad2bc8a8f372d7e3376918fcb6e284387859a",
        min_length=42,
        max_length=42
    ),
    interval: str = Query(
        "1m",
        description="Time interval (e.g., '1m', '5m', '1h', '1d')",
        example="1m"
    ),
    fidelity: str = Query(
        "1d",
        description="Data fidelity (e.g., '1d', '1w', '1m')",
        example="1d"
    ),
    db: AsyncSession = Depends(get_db)
):
   
    if not validate_wallet(user_address):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid wallet address format: {user_address}. Must be 42 characters starting with 0x"
        )
    
    try:
        # Fetch PnL data from API and save to database
        pnl_data, saved_count = await fetch_and_save_pnl(
            db, user_address, interval=interval, fidelity=fidelity
        )
        
        # Convert to response format
        data_points = []
        for point in pnl_data:
            data_points.append(PnLDataPoint(
                t=point.get("t", 0),
                p=Decimal(str(point.get("p", 0)))
            ))
        
        return UserPnLResponse(
            user_address=user_address,
            interval=interval,
            fidelity=fidelity,
            count=len(data_points),
            data=data_points
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching and saving PnL data: {str(e)}"
        )


@router.get(
    "/from-db",
    response_model=UserPnLResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid wallet address"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Get user PnL from database",
    description="Retrieve user PnL data from the database (without fetching from API)"
)
async def get_pnl_from_db_endpoint(
    user_address: str = Query(
        ...,
        description="Wallet address to get PnL for (must be 42 characters starting with 0x)",
        example="0x554ad2bc8a8f372d7e3376918fcb6e284387859a",
        min_length=42,
        max_length=42
    ),
    interval: Optional[str] = Query(
        None,
        description="Filter by time interval (optional)"
    ),
    fidelity: Optional[str] = Query(
        None,
        description="Filter by data fidelity (optional)"
    ),
    limit: Optional[int] = Query(
        None,
        ge=1,
        description="Maximum number of data points to return (optional)"
    ),
    db: AsyncSession = Depends(get_db)
):
   
    if not validate_wallet(user_address):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid wallet address format: {user_address}. Must be 42 characters starting with 0x"
        )
    
    try:
        # Get PnL data from database
        pnl_records = await get_pnl_from_db(
            db, user_address, interval=interval, fidelity=fidelity, limit=limit
        )
        
        if not pnl_records:
            # Return empty response if no data found
            return UserPnLResponse(
                user_address=user_address,
                interval=interval or "unknown",
                fidelity=fidelity or "unknown",
                count=0,
                data=[]
            )
        
        # Get interval and fidelity from first record (they should all be the same for a query)
        first_record = pnl_records[0]
        
        # Convert to response format
        data_points = []
        for record in pnl_records:
            data_points.append(PnLDataPoint(
                t=record.timestamp,
                p=record.pnl
            ))
        
        return UserPnLResponse(
            user_address=user_address,
            interval=first_record.interval,
            fidelity=first_record.fidelity,
            count=len(data_points),
            data=data_points
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving PnL data from database: {str(e)}"
        )


@router.get(
    "/calculate",
    response_model=PnLCalculationResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid wallet address"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Calculate comprehensive PnL from database",
    description="Calculate comprehensive PnL by aggregating trades, positions, and activities from the database"
)
async def calculate_pnl_from_db_endpoint(
    user: str = Query(
        ...,
        description="Wallet address to calculate PnL for (must be 42 characters starting with 0x)",
        example="0x17db3fcd93ba12d38382a0cade24b200185c5f6d",
        min_length=42,
        max_length=42
    ),
    db: AsyncSession = Depends(get_db)
):
    """
    Calculate comprehensive PnL for a user by aggregating data from the database.
    
    This endpoint:
    1. Fetches all trades from the database
    2. Fetches all positions from the database
    3. Fetches all activities from the database
    4. Calculates:
       - Total invested (from positions initial values)
       - Total current value (from positions current values)
       - Realized PnL (from positions realized PnL)
       - Unrealized PnL (from positions cash PnL)
       - Rewards (from REWARD activities)
       - Redemptions (from REDEEM activities)
       - Total PnL (realized + unrealized + rewards - redemptions)
       - PnL percentage
    5. Returns comprehensive PnL metrics with statistics
    
    Args:
        user: Wallet address (query parameter)
        db: Database session (injected)
    
    Returns:
        PnLCalculationResponse with comprehensive PnL metrics
    """
    if not validate_wallet(user):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid wallet address format: {user}. Must be 42 characters starting with 0x"
        )
    
    try:
        # Calculate PnL from database
        pnl_data = await calculate_user_pnl(db, user)
        
        return PnLCalculationResponse(**pnl_data)
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error calculating PnL from database: {str(e)}"
        )

