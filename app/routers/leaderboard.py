"""Leaderboard API routes."""

from fastapi import APIRouter, Query, HTTPException, status, Depends, Body
from typing import Literal, List
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel, Field
from app.schemas.leaderboard import LeaderboardResponse, LeaderboardEntry
from app.schemas.general import ErrorResponse
from app.services.leaderboard_service import (
    get_leaderboard_by_pnl,
    get_leaderboard_by_roi,
    get_leaderboard_by_win_rate
)
from app.services.live_leaderboard_service import fetch_live_leaderboard_from_file
from app.services.trade_service import fetch_and_save_trades
from app.services.position_service import fetch_and_save_positions
from app.services.activity_service import fetch_and_save_activities
from app.db.session import get_db

router = APIRouter(prefix="/leaderboard", tags=["Leaderboards"])


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


class AddWalletRequest(BaseModel):
    """Request model for adding wallet to leaderboard."""
    wallet_address: str = Field(..., description="Wallet address to add", example="0x17db3fcd93ba12d38382a0cade24b200185c5f6d")


class AddWalletsRequest(BaseModel):
    """Request model for adding multiple wallets to leaderboard."""
    wallet_addresses: List[str] = Field(..., description="List of wallet addresses to add", min_items=1, max_items=100)


class AddWalletResponse(BaseModel):
    """Response model for adding wallet."""
    wallet_address: str
    success: bool
    trades_saved: int = 0
    positions_saved: int = 0
    activities_saved: int = 0
    message: str


@router.get(
    "/pnl",
    response_model=LeaderboardResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid parameters"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Get leaderboard by Total PnL",
    description="Get leaderboard of traders ranked by Total PnL with optional time period filtering"
)
async def get_pnl_leaderboard(
    period: Literal["7d", "30d", "all"] = Query(
        "all",
        description="Time period filter: 7d (7 days), 30d (30 days), or all (all time)"
    ),
    limit: int = Query(
        100,
        ge=1,
        le=1000,
        description="Maximum number of traders to return"
    ),
    db: AsyncSession = Depends(get_db)
):
    """
    Get leaderboard sorted by Total PnL.
    
    Returns traders ranked by their total profit and loss (PnL).
    """
    try:
        leaderboard = await get_leaderboard_by_pnl(db, period=period, limit=limit)
        
        entries = [
            LeaderboardEntry(**trader) for trader in leaderboard
        ]
        
        return LeaderboardResponse(
            period=period,
            metric="pnl",
            count=len(entries),
            entries=entries
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generating PnL leaderboard: {str(e)}"
        )


@router.get(
    "/roi",
    response_model=LeaderboardResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid parameters"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Get leaderboard by ROI",
    description="Get leaderboard of traders ranked by Return on Investment (ROI) with optional time period filtering"
)
async def get_roi_leaderboard(
    period: Literal["7d", "30d", "all"] = Query(
        "all",
        description="Time period filter: 7d (7 days), 30d (30 days), or all (all time)"
    ),
    limit: int = Query(
        100,
        ge=1,
        le=1000,
        description="Maximum number of traders to return"
    ),
    db: AsyncSession = Depends(get_db)
):
    """
    Get leaderboard sorted by ROI.
    
    Returns traders ranked by their Return on Investment (ROI) percentage.
    """
    try:
        leaderboard = await get_leaderboard_by_roi(db, period=period, limit=limit)
        
        entries = [
            LeaderboardEntry(**trader) for trader in leaderboard
        ]
        
        return LeaderboardResponse(
            period=period,
            metric="roi",
            count=len(entries),
            entries=entries
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generating ROI leaderboard: {str(e)}"
        )


@router.get(
    "/win-rate",
    response_model=LeaderboardResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid parameters"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Get leaderboard by Win Rate",
    description="Get leaderboard of traders ranked by Win Rate with optional time period filtering"
)
async def get_win_rate_leaderboard(
    period: Literal["7d", "30d", "all"] = Query(
        "all",
        description="Time period filter: 7d (7 days), 30d (30 days), or all (all time)"
    ),
    limit: int = Query(
        100,
        ge=1,
        le=1000,
        description="Maximum number of traders to return"
    ),
    db: AsyncSession = Depends(get_db)
):
    """
    Get leaderboard sorted by Win Rate.
    
    Returns traders ranked by their win rate percentage.
    """
    try:
        leaderboard = await get_leaderboard_by_win_rate(db, period=period, limit=limit)
        
        entries = [
            LeaderboardEntry(**trader) for trader in leaderboard
        ]
        
        return LeaderboardResponse(
            period=period,
            metric="win_rate",
            count=len(entries),
            entries=entries
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generating Win Rate leaderboard: {str(e)}"
        )


@router.post(
    "/add-wallet",
    response_model=AddWalletResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid wallet address"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Add wallet to leaderboard",
    description="Fetch and save data for a wallet address to include it in leaderboards"
)
async def add_wallet_to_leaderboard(
    request: AddWalletRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Add a wallet address to the leaderboard by fetching and saving its data.
    
    This endpoint:
    1. Validates the wallet address
    2. Fetches trades, positions, and activities from Polymarket API
    3. Saves all data to the database
    4. The wallet will now appear in leaderboards
    
    Args:
        request: AddWalletRequest with wallet_address
        db: Database session
    
    Returns:
        AddWalletResponse with success status and counts
    """
    wallet_address = request.wallet_address
    
    if not validate_wallet(wallet_address):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid wallet address format: {wallet_address}. Must be 42 characters starting with 0x"
        )
    
    try:
        trades_saved = 0
        positions_saved = 0
        activities_saved = 0
        errors = []
        
        # Fetch and save trades
        try:
            _, trades_saved = await fetch_and_save_trades(db, wallet_address)
        except Exception as e:
            errors.append(f"Trades: {str(e)}")
        
        # Fetch and save positions
        try:
            _, positions_saved = await fetch_and_save_positions(db, wallet_address)
        except Exception as e:
            errors.append(f"Positions: {str(e)}")
        
        # Fetch and save activities
        try:
            _, activities_saved = await fetch_and_save_activities(db, wallet_address)
        except Exception as e:
            errors.append(f"Activities: {str(e)}")
        
        if trades_saved == 0 and positions_saved == 0 and activities_saved == 0:
            return AddWalletResponse(
                wallet_address=wallet_address,
                success=False,
                trades_saved=0,
                positions_saved=0,
                activities_saved=0,
                message=f"No data found for wallet. Errors: {', '.join(errors) if errors else 'No trades, positions, or activities found'}"
            )
        
        message = f"Successfully added wallet. Saved: {trades_saved} trades, {positions_saved} positions, {activities_saved} activities"
        if errors:
            message += f". Warnings: {', '.join(errors)}"
        
        return AddWalletResponse(
            wallet_address=wallet_address,
            success=True,
            trades_saved=trades_saved,
            positions_saved=positions_saved,
            activities_saved=activities_saved,
            message=message
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error adding wallet to leaderboard: {str(e)}"
        )


@router.post(
    "/add-wallets",
    response_model=List[AddWalletResponse],
    responses={
        400: {"model": ErrorResponse, "description": "Invalid wallet addresses"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Add multiple wallets to leaderboard",
    description="Fetch and save data for multiple wallet addresses to include them in leaderboards"
)
async def add_wallets_to_leaderboard(
    request: AddWalletsRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Add multiple wallet addresses to the leaderboard.
    
    This endpoint processes multiple wallets in batch.
    Each wallet is processed independently, so failures for one wallet don't affect others.
    
    Args:
        request: AddWalletsRequest with list of wallet_addresses
        db: Database session
    
    Returns:
        List of AddWalletResponse for each wallet
    """
    results = []
    
    for wallet_address in request.wallet_addresses:
        if not validate_wallet(wallet_address):
            results.append(AddWalletResponse(
                wallet_address=wallet_address,
                success=False,
                trades_saved=0,
                positions_saved=0,
                activities_saved=0,
                message="Invalid wallet address format"
            ))
            continue
        
        try:
            trades_saved = 0
            positions_saved = 0
            activities_saved = 0
            errors = []
            
            # Fetch and save trades
            try:
                _, trades_saved = await fetch_and_save_trades(db, wallet_address)
            except Exception as e:
                errors.append(f"Trades: {str(e)}")
            
            # Fetch and save positions
            try:
                _, positions_saved = await fetch_and_save_positions(db, wallet_address)
            except Exception as e:
                errors.append(f"Positions: {str(e)}")
            
            # Fetch and save activities
            try:
                _, activities_saved = await fetch_and_save_activities(db, wallet_address)
            except Exception as e:
                errors.append(f"Activities: {str(e)}")
            
            if trades_saved == 0 and positions_saved == 0 and activities_saved == 0:
                results.append(AddWalletResponse(
                    wallet_address=wallet_address,
                    success=False,
                    trades_saved=0,
                    positions_saved=0,
                    activities_saved=0,
                    message=f"No data found. Errors: {', '.join(errors) if errors else 'No data available'}"
                ))
            else:
                message = f"Successfully added. Saved: {trades_saved} trades, {positions_saved} positions, {activities_saved} activities"
                if errors:
                    message += f". Warnings: {', '.join(errors)}"
                
                results.append(AddWalletResponse(
                    wallet_address=wallet_address,
                    success=True,
                    trades_saved=trades_saved,
                    positions_saved=positions_saved,
                    activities_saved=activities_saved,
                    message=message
                ))
        except Exception as e:
            results.append(AddWalletResponse(
                wallet_address=wallet_address,
                success=False,
                trades_saved=0,
                positions_saved=0,
                activities_saved=0,
                message=f"Error: {str(e)}"
            ))
    
    return results

@router.post(
    "/live",
    response_model=LeaderboardResponse,
    summary="Get Live Leaderboard from wallet_address.txt",
    description="Calculate live leaderboard scores for wallets listed in wallet_address.txt"
)
async def get_live_leaderboard_from_file():
    """
    Generate a live leaderboard using the wallet_address.txt file.
    
    This endpoint:
    1. Reads wallet_address.txt from the server.
    2. Fetches LIVE data from Polymarket API (bypassing DB).
    3. Calculates advanced scores (Win Rate, ROI, PnL, Risk).
    4. Returns ranked results.
    """
    try:
        file_path = "wallet_address.txt" # Relative to root where app runs
        entries_data = await fetch_live_leaderboard_from_file(file_path)
        
        entries = [LeaderboardEntry(**e) for e in entries_data]
        
        return LeaderboardResponse(
            period="all",
            metric="score_pnl",
            count=len(entries),
            entries=entries
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generating live leaderboard: {str(e)}"
        )

