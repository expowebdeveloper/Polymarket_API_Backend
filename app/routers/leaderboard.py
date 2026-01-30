"""Leaderboard API routes."""

import asyncio
from fastapi import APIRouter, Query, HTTPException, status, Depends, Body
from fastapi.responses import JSONResponse
from typing import Literal, List, Optional
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel, Field
from app.schemas.leaderboard import LeaderboardResponse, LeaderboardEntry, AllLeaderboardsResponse, PercentileInfo, MedianInfo
from app.schemas.general import ErrorResponse
from app.services.leaderboard_service import (
    calculate_scores_and_rank_with_percentiles
)
from app.core.config import settings
from app.core.scoring_config import default_scoring_config
from app.services.live_leaderboard_service import (
    fetch_live_leaderboard_from_file,
    fetch_polymarket_leaderboard_api,
    fetch_polymarket_biggest_winners,
    transform_polymarket_api_entry,
    load_wallet_addresses_from_json
)
from app.services.trade_service import fetch_and_save_trades
from app.services.position_service import fetch_and_save_positions
from app.services.activity_service import fetch_and_save_activities
# Removed db_scoring_service - now using Polymarket API directly
from app.db.session import get_db
from app.services.goldsky_service import GoldskyService
from app.services.leaderboard_service import calculate_scores_and_rank

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
    description="Get leaderboard of traders ranked by Total PnL using live Polymarket API data"
)
async def get_pnl_leaderboard(
    period: Literal["7d", "30d", "all"] = Query(
        "all",
        description="Time period filter: 7d (7 days), 30d (30 days), or all (all time) - Note: Currently uses all time data from API"
    ),
    limit: int = Query(
        100,
        ge=1,
        le=1000,
        description="Maximum number of traders to return"
    )
):
    """
    Get leaderboard sorted by Total PnL using live Polymarket API data.
    
    Returns traders ranked by their total profit and loss (PnL).
    Uses wallet_address.txt file and fetches fresh data from Polymarket API.
    """
    try:
        file_path = "wallet_address.txt"
        entries_data = await fetch_live_leaderboard_from_file(file_path)
        
        # Sort by total_pnl (descending - highest PnL = rank 1)
        entries_data.sort(key=lambda x: x.get('total_pnl', float('-inf')), reverse=True)
        
        # Apply limit
        entries_data = entries_data[:limit]
        
        # Add rank
        for i, trader in enumerate(entries_data, 1):
            trader['rank'] = i
        
        entries = [LeaderboardEntry(**trader) for trader in entries_data]
        
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
    description="Get leaderboard of traders ranked by Return on Investment (ROI) using live Polymarket API data"
)
async def get_roi_leaderboard(
    period: Literal["7d", "30d", "all"] = Query(
        "all",
        description="Time period filter: 7d (7 days), 30d (30 days), or all (all time) - Note: Currently uses all time data from API"
    ),
    limit: int = Query(
        100,
        ge=1,
        le=1000,
        description="Maximum number of traders to return"
    )
):
    """
    Get leaderboard sorted by ROI using live Polymarket API data.
    
    Returns traders ranked by their return on investment percentage.
    Uses wallet_address.txt file and fetches fresh data from Polymarket API.
    """
    try:
        file_path = "wallet_address.txt"
        entries_data = await fetch_live_leaderboard_from_file(file_path)
        
        # Sort by roi (descending - highest ROI = rank 1)
        entries_data.sort(key=lambda x: x.get('roi', float('-inf')), reverse=True)
        
        # Apply limit
        entries_data = entries_data[:limit]
        
        # Add rank
        for i, trader in enumerate(entries_data, 1):
            trader['rank'] = i
        
        entries = [LeaderboardEntry(**trader) for trader in entries_data]
        
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
    description="Get leaderboard of traders ranked by Win Rate using live Polymarket API data"
)
async def get_win_rate_leaderboard(
    period: Literal["7d", "30d", "all"] = Query(
        "all",
        description="Time period filter: 7d (7 days), 30d (30 days), or all (all time) - Note: Currently uses all time data from API"
    ),
    limit: int = Query(
        100,
        ge=1,
        le=1000,
        description="Maximum number of traders to return"
    )
):
    """
    Get leaderboard sorted by Win Rate using live Polymarket API data.
    
    Returns traders ranked by their win rate percentage.
    Uses wallet_address.txt file and fetches fresh data from Polymarket API.
    """
    try:
        file_path = "wallet_address.txt"
        entries_data = await fetch_live_leaderboard_from_file(file_path)
        
        # Sort by win_rate (descending - highest win rate = rank 1)
        entries_data.sort(key=lambda x: x.get('win_rate', float('-inf')), reverse=True)
        
        # Apply limit
        entries_data = entries_data[:limit]
        
        # Add rank
        for i, trader in enumerate(entries_data, 1):
            trader['rank'] = i
        
        entries = [LeaderboardEntry(**trader) for trader in entries_data]
        
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
    summary="Get Live Leaderboard from Polymarket API",
    description="Get live leaderboard data directly from Polymarket API (day leaderboard by PNL by default)"
)
async def get_live_leaderboard_from_file(
    time_period: Literal["day", "week", "month", "all"] = Query(
        "day",
        description="Time period: day, week, month, or all"
    ),
    order_by: Literal["PNL", "VOL"] = Query(
        "PNL",
        description="Order by metric: PNL or VOL"
    ),
    limit: int = Query(
        20,
        ge=1,
        le=100,
        description="Maximum number of traders to return"
    ),
    offset: int = Query(
        0,
        ge=0,
        description="Offset for pagination"
    )
):
    """
    Get live leaderboard data directly from Polymarket API.
    
    This endpoint fetches data from Polymarket's live API:
    - Default: Day leaderboard by PNL
    - Can be configured for week by VOL, day by VOL, etc.
    
    Returns ranked results from Polymarket's live API.
    """
    try:
        # 1. Fetch from Polymarket API to get the current ranked list of wallets
        api_data = await fetch_polymarket_leaderboard_api(
            time_period=time_period,
            order_by=order_by,
            limit=limit,
            offset=offset,
            category="overall"
        )
        
        if not api_data:
            return LeaderboardResponse(period=time_period, metric=order_by.lower(), count=0, entries=[])

        # 2. Extract wallet addresses
        wallets = []
        for entry in api_data:
            wallet = entry.get("proxyWallet") or entry.get("wallet_address") or entry.get("wallet")
            if wallet:
                wallets.append(wallet)

        # 3. Fetch detailed stats and calculate scores for these specific wallets
        # This provides the "Full Functionality" with real scores in the Live view
        from app.services.live_leaderboard_service import fetch_live_leaderboard
        entries_data = await fetch_live_leaderboard(wallets)
        
        # 4. Map back any missing metadata (names, images) from the original API call if needed
        # fetch_live_leaderboard already attempts to get some info, but api_data has the latest from Leaderboard
        wallet_meta = {
            (e.get("proxyWallet") or e.get("wallet_address")): {
                "name": e.get("userName") or e.get("name"),
                "pseudonym": e.get("xUsername") or e.get("pseudonym"),
                "profile_image": e.get("profileImage") or e.get("profile_image")
            } for e in api_data
        }
        
        for entry in entries_data:
            addr = entry.get("wallet_address")
            if addr in wallet_meta:
                meta = wallet_meta[addr]
                if not entry.get("name") and meta["name"]: entry["name"] = meta["name"]
                if not entry.get("pseudonym") and meta["pseudonym"]: entry["pseudonym"] = meta["pseudonym"]
                if not entry.get("profile_image") and meta["profile_image"]: entry["profile_image"] = meta["profile_image"]

        entries = [LeaderboardEntry(**e) for e in entries_data]
        
        return LeaderboardResponse(
            period=time_period,
            metric=order_by.lower(),
            count=len(entries),
            entries=entries
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generating live leaderboard: {str(e)}"
        )


@router.get(
    "/live/biggest-winners",
    response_model=LeaderboardResponse,
    summary="Get Biggest Winners from Polymarket API",
    description="Get biggest winners data directly from Polymarket API (day by default)"
)
async def get_live_biggest_winners(
    time_period: Literal["day", "week", "month", "all"] = Query(
        "day",
        description="Time period: day, week, month, or all"
    ),
    limit: int = Query(
        20,
        ge=1,
        le=100,
        description="Maximum number of traders to return"
    ),
    offset: int = Query(
        0,
        ge=0,
        description="Offset for pagination"
    )
):
    """
    Get biggest winners data directly from Polymarket API.
    
    Returns the biggest individual wins from Polymarket's live API.
    """
    try:
        # Fetch from Polymarket API
        api_data = await fetch_polymarket_biggest_winners(
            time_period=time_period,
            limit=limit,
            offset=offset,
            category="overall"
        )
        
        # Transform API entries to LeaderboardEntry format
        entries_data = [transform_polymarket_api_entry(entry, "biggest_winners") for entry in api_data]
        
        entries = [LeaderboardEntry(**e) for e in entries_data]
        
        return LeaderboardResponse(
            period=time_period,
            metric="biggest_winners",
            count=len(entries),
            entries=entries
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generating biggest winners leaderboard: {str(e)}"
        )


@router.post(
    "/live-roi",
    response_model=LeaderboardResponse,
    summary="Get Live Leaderboard by ROI Score",
    description="Calculate live leaderboard scores for wallets listed in wallet_address.txt, ranked by ROI Score"
)
async def get_live_roi_leaderboard_from_file():
    """
    Generate a live leaderboard using the wallet_address.txt file, sorted by ROI Score.
    """
    try:
        file_path = "wallet_address.txt"
        entries_data = await fetch_live_leaderboard_from_file(file_path)
        
        # Sort by ROI Score
        # Note: keys in dictionary from fetch_live_leaderboard might need checking
        # app/services/live_leaderboard_service.py calls calculate_scores_and_rank
        # app/services/leaderboard_service.py produces 'score_roi'.
        
        # Sort by ROI_shrunk in ascending order (best = lowest shrunk value = rank 1)
        entries_data.sort(key=lambda x: x.get('roi_shrunk', float('inf')))
        
        entries = [LeaderboardEntry(**e) for e in entries_data]
        
        return LeaderboardResponse(
            period="all",
            metric="roi_shrunk",
            count=len(entries),
            entries=entries
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generating live ROI leaderboard: {str(e)}"
        )


@router.post(
    "/live-pnl",
    response_model=LeaderboardResponse,
    summary="Get Live Leaderboard by PnL Score",
    description="Calculate live leaderboard scores for wallets listed in wallet_address.txt, ranked by PnL Score"
)
async def get_live_pnl_leaderboard_from_file():
    """
    Generate a live leaderboard using the wallet_address.txt file, sorted by PnL Score.
    """
    try:
        file_path = "wallet_address.txt"
        entries_data = await fetch_live_leaderboard_from_file(file_path)
        
        # Sort by PNL_shrunk in ascending order (best = lowest shrunk value = rank 1)
        entries_data.sort(key=lambda x: x.get('pnl_shrunk', float('inf')))
        
        entries = [LeaderboardEntry(**e) for e in entries_data]
        
        return LeaderboardResponse(
            period="all",
            metric="pnl_shrunk",
            count=len(entries),
            entries=entries
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generating live PnL leaderboard: {str(e)}"
        )

@router.post(
    "/live-risk",
    response_model=LeaderboardResponse,
    summary="Get Live Leaderboard by Risk Score",
    description="Calculate live leaderboard scores for wallets listed in wallet_address.txt, ranked by Risk Score"
)
async def get_live_risk_leaderboard_from_file():
    """
    Generate a live leaderboard using the wallet_address.txt file, sorted by Risk Score.
    """
    try:
        file_path = "wallet_address.txt"
        entries_data = await fetch_live_leaderboard_from_file(file_path)
        
        # Sort by Risk Score
        entries_data.sort(key=lambda x: x.get('score_risk', 0), reverse=True)
        
        entries = [LeaderboardEntry(**e) for e in entries_data]
        
        return LeaderboardResponse(
            period="all",
            metric="score_risk",
            count=len(entries),
            entries=entries
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generating live Risk leaderboard: {str(e)}"
        )


@router.post(
    "/w-shrunk",
    response_model=LeaderboardResponse,
    summary="Get Live Leaderboard by W Shrunk",
    description="Calculate live leaderboard scores for wallets listed in wallet_address.txt, ranked by W_shrunk (ascending - best = rank 1)"
)
async def get_live_w_shrunk_leaderboard_from_file():
    """
    Generate a live leaderboard using the wallet_address.txt file, sorted by W_shrunk in ascending order.
    Lower W_shrunk = better performance = rank 1.
    """
    try:
        file_path = "wallet_address.txt"
        entries_data = await fetch_live_leaderboard_from_file(file_path)
        
        # Sort by W_shrunk in ascending order (best = lowest shrunk value = rank 1)
        entries_data.sort(key=lambda x: x.get('W_shrunk', float('inf')))
        
        entries = [LeaderboardEntry(**e) for e in entries_data]
        
        return LeaderboardResponse(
            period="all",
            metric="W_shrunk",
            count=len(entries),
            entries=entries
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generating live W shrunk leaderboard: {str(e)}"
        )


@router.post(
    "/roi-raw",
    response_model=LeaderboardResponse,
    summary="Get Live Leaderboard by Raw ROI",
    description="Calculate live leaderboard for wallets listed in wallet_address.txt, ranked by raw ROI (before shrinkage)"
)
async def get_live_roi_raw_leaderboard_from_file():
    """
    Generate a live leaderboard using the wallet_address.txt file, sorted by raw ROI (before shrinkage).
    """
    try:
        file_path = "wallet_address.txt"
        entries_data = await fetch_live_leaderboard_from_file(file_path)
        
        # Sort by raw ROI in descending order (highest ROI = rank 1)
        entries_data.sort(key=lambda x: x.get('roi', float('-inf')), reverse=True)
        
        entries = [LeaderboardEntry(**e) for e in entries_data]
        
        return LeaderboardResponse(
            period="all",
            metric="roi",
            count=len(entries),
            entries=entries
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generating live ROI raw leaderboard: {str(e)}"
        )


@router.post(
    "/roi-shrunk",
    response_model=LeaderboardResponse,
    summary="Get Live Leaderboard by ROI Shrunk",
    description="Calculate live leaderboard scores for wallets listed in wallet_address.txt, ranked by ROI_shrunk (ascending - best = rank 1)"
)
async def get_live_roi_shrunk_leaderboard_from_file():
    """
    Generate a live leaderboard using the wallet_address.txt file, sorted by ROI_shrunk in ascending order.
    Lower ROI_shrunk = better performance = rank 1.
    """
    try:
        file_path = "wallet_address.txt"
        entries_data = await fetch_live_leaderboard_from_file(file_path)
        
        # Sort by ROI_shrunk in ascending order (best = lowest shrunk value = rank 1)
        entries_data.sort(key=lambda x: x.get('roi_shrunk', float('inf')))
        
        entries = [LeaderboardEntry(**e) for e in entries_data]
        
        return LeaderboardResponse(
            period="all",
            metric="roi_shrunk",
            count=len(entries),
            entries=entries
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generating live ROI shrunk leaderboard: {str(e)}"
        )


@router.post(
    "/pnl-shrunk",
    response_model=LeaderboardResponse,
    summary="Get Live Leaderboard by PnL Shrunk",
    description="Calculate live leaderboard scores for wallets listed in wallet_address.txt, ranked by PNL_shrunk (ascending - best = rank 1)"
)
async def get_live_pnl_shrunk_leaderboard_from_file():
    """
    Generate a live leaderboard using the wallet_address.txt file, sorted by PNL_shrunk in ascending order.
    Lower PNL_shrunk = better performance = rank 1.
    """
    try:
        file_path = "wallet_address.txt"
        entries_data = await fetch_live_leaderboard_from_file(file_path)
        
        # Sort by PNL_shrunk in ascending order (best = lowest shrunk value = rank 1)
        entries_data.sort(key=lambda x: x.get('pnl_shrunk', float('inf')))
        
        entries = [LeaderboardEntry(**e) for e in entries_data]
        
        return LeaderboardResponse(
            period="all",
            metric="pnl_shrunk",
            count=len(entries),
            entries=entries
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generating live PnL shrunk leaderboard: {str(e)}"
        )


@router.post(
    "/all",
    response_model=AllLeaderboardsResponse,
    summary="Get All Leaderboards with Percentile Information",
    description="Get all leaderboards (sorted by different metrics) along with percentile anchors and median values used in calculations"
)
async def get_all_leaderboards_with_percentiles():
    """
    Generate all leaderboards with percentile information.
    
    Returns:
    - All leaderboards sorted by different metrics (W_shrunk, ROI_raw, ROI_shrunk, PNL_shrunk, final scores)
    - Percentile information (1% and 99% anchors for W, ROI, and PNL shrunk values)
    - Median values (ROI median and PNL median used in shrinkage)
    - Population statistics
    """
    try:
        file_path = "wallet_address.txt"
        entries_data = await fetch_live_leaderboard_from_file(file_path)
        
        if not entries_data:
            return AllLeaderboardsResponse(
                percentiles=PercentileInfo(
                    w_shrunk_1_percent=0.0,
                    w_shrunk_99_percent=0.0,
                    roi_shrunk_1_percent=0.0,
                    roi_shrunk_99_percent=0.0,
                    pnl_shrunk_1_percent=0.0,
                    pnl_shrunk_99_percent=0.0,
                    population_size=0
                ),
                medians=MedianInfo(
                    roi_median=0.0,
                    pnl_median=0.0
                ),
                leaderboards={},
                total_traders=0,
                population_traders=0
            )
        
        # Calculate scores with percentile information using configurable scoring
        result = calculate_scores_and_rank_with_percentiles(entries_data, default_scoring_config)
        traders = result["traders"]
        percentiles_data = result["percentiles"]
        medians_data = result["medians"]
        
        # Extract percentile values using configurable keys (default uses 1% and 99%)
        config = default_scoring_config
        w_lower_key = f"w_shrunk_{config.percentile_lower}_percent"
        w_upper_key = f"w_shrunk_{config.percentile_upper}_percent"
        roi_lower_key = f"roi_shrunk_{config.percentile_lower}_percent"
        roi_upper_key = f"roi_shrunk_{config.percentile_upper}_percent"
        pnl_lower_key = f"pnl_shrunk_{config.percentile_lower}_percent"
        pnl_upper_key = f"pnl_shrunk_{config.percentile_upper}_percent"
        
        # Create all different leaderboards
        leaderboards = {}
        
        # 1. W_shrunk leaderboard (ascending - best = lowest)
        w_shrunk_sorted = sorted(traders, key=lambda x: x.get('W_shrunk', float('inf')))
        for i, trader in enumerate(w_shrunk_sorted, 1):
            trader['rank'] = i
        leaderboards["w_shrunk"] = [LeaderboardEntry(**t) for t in w_shrunk_sorted]
        
        # 2. ROI raw leaderboard (descending - best = highest)
        roi_raw_sorted = sorted(traders, key=lambda x: x.get('roi', float('-inf')), reverse=True)
        for i, trader in enumerate(roi_raw_sorted, 1):
            trader['rank'] = i
        leaderboards["roi_raw"] = [LeaderboardEntry(**t) for t in roi_raw_sorted]
        
        # 3. ROI shrunk leaderboard (ascending - best = lowest)
        roi_shrunk_sorted = sorted(traders, key=lambda x: x.get('roi_shrunk', float('inf')))
        for i, trader in enumerate(roi_shrunk_sorted, 1):
            trader['rank'] = i
        leaderboards["roi_shrunk"] = [LeaderboardEntry(**t) for t in roi_shrunk_sorted]
        
        # 4. PNL shrunk leaderboard (ascending - best = lowest)
        pnl_shrunk_sorted = sorted(traders, key=lambda x: x.get('pnl_shrunk', float('inf')))
        for i, trader in enumerate(pnl_shrunk_sorted, 1):
            trader['rank'] = i
        leaderboards["pnl_shrunk"] = [LeaderboardEntry(**t) for t in pnl_shrunk_sorted]
        
        # 5. Final Score leaderboards (descending - best = highest)
        # Win Rate Score
        win_rate_sorted = sorted(traders, key=lambda x: x.get('score_win_rate', 0), reverse=True)
        for i, trader in enumerate(win_rate_sorted, 1):
            trader['rank'] = i
        leaderboards["score_win_rate"] = [LeaderboardEntry(**t) for t in win_rate_sorted]
        
        # ROI Score
        roi_score_sorted = sorted(traders, key=lambda x: x.get('score_roi', 0), reverse=True)
        for i, trader in enumerate(roi_score_sorted, 1):
            trader['rank'] = i
        leaderboards["score_roi"] = [LeaderboardEntry(**t) for t in roi_score_sorted]
        
        # PNL Score
        pnl_score_sorted = sorted(traders, key=lambda x: x.get('score_pnl', 0), reverse=True)
        for i, trader in enumerate(pnl_score_sorted, 1):
            trader['rank'] = i
        leaderboards["score_pnl"] = [LeaderboardEntry(**t) for t in pnl_score_sorted]
        
        # Risk Score (using fixed formula: |Worst Loss| / Total Stake, range 0-1)
        risk_sorted = sorted(traders, key=lambda x: x.get('score_risk', 0), reverse=True)
        for i, trader in enumerate(risk_sorted, 1):
            trader['rank'] = i
        leaderboards["score_risk"] = [LeaderboardEntry(**t) for t in risk_sorted]
        
        # Final Score (descending - best = highest)
        # Uses formula: Rating = 100 × [ wW · Wscore + wR · Rscore + wP · Pscore + wrisk · (1 − Risk Score) ]
        final_score_sorted = sorted(traders, key=lambda x: x.get('final_score', 0), reverse=True)
        for i, trader in enumerate(final_score_sorted, 1):
            trader['rank'] = i
        leaderboards["final_score"] = [LeaderboardEntry(**t) for t in final_score_sorted]
        
        return AllLeaderboardsResponse(
            percentiles=PercentileInfo(
                w_shrunk_1_percent=percentiles_data.get(w_lower_key, 0.0),
                w_shrunk_99_percent=percentiles_data.get(w_upper_key, 0.0),
                roi_shrunk_1_percent=percentiles_data.get(roi_lower_key, 0.0),
                roi_shrunk_99_percent=percentiles_data.get(roi_upper_key, 0.0),
                pnl_shrunk_1_percent=percentiles_data.get(pnl_lower_key, 0.0),
                pnl_shrunk_99_percent=percentiles_data.get(pnl_upper_key, 0.0),
                population_size=result["population_size"]
            ),
            medians=MedianInfo(
                roi_median=medians_data["roi_median"],
                pnl_median=medians_data["pnl_median"]
            ),
            leaderboards=leaderboards,
            total_traders=len(traders),
            population_traders=percentiles_data.get("population_size", len(traders))
        )
    except Exception as e:
        import traceback
        print(f"Error generating all leaderboards: {e}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generating all leaderboards: {str(e)}"
        )


@router.get(
    "/db/final-score",
    response_model=LeaderboardResponse,
    responses={
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Get Leaderboard by Final Score from Polymarket API",
    description="Calculate leaderboard scores using Polymarket API with advanced formula. Same as /view-all but sorted by final score only."
)
async def get_db_final_score_leaderboard(
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of traders to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination")
):
    """
    Get leaderboard sorted by final score from Polymarket API.
    Uses the advanced scoring formula with shrinkage and percentiles.
    Now uses Polymarket API directly instead of database.
    """
    try:
        from app.services.live_leaderboard_service import fetch_raw_metrics_for_scoring
        from app.services.leaderboard_service import calculate_scores_and_rank_with_percentiles
        from app.services.pnl_median_service import get_pnl_median_from_population
        
        file_path = "wallet_address.txt"
        # Fetch raw metrics from Polymarket API
        entries_data = await fetch_raw_metrics_for_scoring(file_path)
        
        if not entries_data:
            return LeaderboardResponse(
                period="all",
                entries=[]
            )
        
        # Get medians from Polymarket API
        pnl_median_api = await get_pnl_median_from_population()
        
        # Calculate scores with percentile information
        result = calculate_scores_and_rank_with_percentiles(
            entries_data,
            pnl_median=pnl_median_api
        )
        traders = result["traders"]
        
        # Sort by final score (descending - best = highest)
        final_score_sorted = sorted(traders, key=lambda x: x.get('final_score', 0), reverse=True)
        for i, trader in enumerate(final_score_sorted, 1):
            trader['rank'] = i
        
        # Apply limit and offset
        paginated_traders = final_score_sorted[offset : offset + limit]
        
        entries = [LeaderboardEntry(**t) for t in paginated_traders]
        
        return LeaderboardResponse(
            period="all",
            entries=entries
        )
    except Exception as e:
        import traceback
        print(f"Error generating DB final score leaderboard: {e}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generating DB final score leaderboard: {str(e)}"
        )


@router.get(
    "/db/all",
    response_model=AllLeaderboardsResponse,
    summary="Get All Leaderboards from Polymarket API",
    description="Get all leaderboards (W_shrunk, ROI_shrunk, etc.) using Polymarket API with advanced formulas. Same as /view-all endpoint. Uses wallet_input.json for wallet addresses (saved by /leaderboard/live endpoint)."
)
async def get_all_db_leaderboards(
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of traders to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    time_period: Literal["day", "week", "month", "all"] = Query(
        "all",
        description="Time period for both wallet selection and metrics calculation (day, week, month, or all)"
    ),
    order_by: Literal["PNL", "VOL"] = Query(
        "PNL",
        description="Order by metric for fetching wallet addresses from live leaderboard"
    )
):
    """
    Generate all leaderboards with percentile information using Polymarket API.
    Uses the advanced formulas (shrinkage, whale penalty, percentiles).
    Loads wallet addresses from wallet_input.json (saved by /leaderboard/live endpoint).
    Same as /view-all endpoint.
    """
    try:
        from app.services.leaderboard_service import calculate_scores_and_rank_with_percentiles
        from app.services.pnl_median_service import get_pnl_median_from_population
        
        # Step 1: Load wallet addresses from wallet_input.json (saved by /leaderboard/live endpoint)
        # If file doesn't exist or is empty, return empty response
        wallet_addresses, wallet_info_map = load_wallet_addresses_from_json()
        
        if not wallet_addresses:
            return AllLeaderboardsResponse(
                percentiles=PercentileInfo(
                    w_shrunk_1_percent=0.0,
                    w_shrunk_99_percent=0.0,
                    roi_shrunk_1_percent=0.0,
                    roi_shrunk_99_percent=0.0,
                    pnl_shrunk_1_percent=0.0,
                    pnl_shrunk_99_percent=0.0,
                    population_size=0
                ),
                medians=MedianInfo(
                    roi_median=0.0,
                    pnl_median=0.0
                ),
                leaderboards={},
                total_traders=0,
                population_traders=0
            )
        
        # Step 2: Fetch raw metrics for these wallets
        entries_data = []
        semaphore = asyncio.Semaphore(5)  # Limit concurrency
        
        async def fetch_wallet_metrics(wallet: str):
            async with semaphore:
                try:
                    from app.services.polymarket_service import PolymarketService
                    # Pass time_period to calculate metrics for the specified time period
                    stats = await PolymarketService.calculate_portfolio_stats(wallet, time_period=time_period)
                    if stats is None:
                        return None
                    from app.services.live_leaderboard_service import transform_stats_for_scoring
                    transformed = transform_stats_for_scoring(stats)
                    if transformed:
                        # Merge name/pseudonym/profile_image from live API
                        wallet_info = wallet_info_map.get(wallet, {})
                        transformed["name"] = wallet_info.get("name")
                        transformed["pseudonym"] = wallet_info.get("pseudonym")
                        transformed["profile_image"] = wallet_info.get("profile_image")
                    return transformed
                except Exception as e:
                    print(f"Error fetching stats for {wallet}: {e}")
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
            return AllLeaderboardsResponse(
                percentiles=PercentileInfo(
                    w_shrunk_1_percent=0.0,
                    w_shrunk_99_percent=0.0,
                    roi_shrunk_1_percent=0.0,
                    roi_shrunk_99_percent=0.0,
                    pnl_shrunk_1_percent=0.0,
                    pnl_shrunk_99_percent=0.0,
                    population_size=0
                ),
                medians=MedianInfo(
                    roi_median=0.0,
                    pnl_median=0.0
                ),
                leaderboards={},
                total_traders=0,
                population_traders=0
            )
        
        # Calculate scores with percentile information using configurable scoring
        result = calculate_scores_and_rank_with_percentiles(entries_data, default_scoring_config)
        traders = result["traders"]
        percentiles_data = result["percentiles"]
        medians_data = result["medians"]
        
        # Extract percentile values using configurable keys (default uses 1% and 99%)
        config = default_scoring_config
        w_lower_key = f"w_shrunk_{config.percentile_lower}_percent"
        w_upper_key = f"w_shrunk_{config.percentile_upper}_percent"
        roi_lower_key = f"roi_shrunk_{config.percentile_lower}_percent"
        roi_upper_key = f"roi_shrunk_{config.percentile_upper}_percent"
        pnl_lower_key = f"pnl_shrunk_{config.percentile_lower}_percent"
        pnl_upper_key = f"pnl_shrunk_{config.percentile_upper}_percent"
        
        # Create all different leaderboards
        leaderboards = {}
        
        # 1. W_shrunk leaderboard (ascending - best = lowest)
        w_shrunk_sorted = sorted(traders, key=lambda x: x.get('W_shrunk', float('inf')))
        for i, trader in enumerate(w_shrunk_sorted, 1):
            trader['rank'] = i
        leaderboards["w_shrunk"] = [LeaderboardEntry(**t) for t in w_shrunk_sorted]
        
        # 2. ROI raw leaderboard (descending - best = highest)
        roi_raw_sorted = sorted(traders, key=lambda x: x.get('roi', float('-inf')), reverse=True)
        for i, trader in enumerate(roi_raw_sorted, 1):
            trader['rank'] = i
        leaderboards["roi_raw"] = [LeaderboardEntry(**t) for t in roi_raw_sorted]
        
        # 3. ROI shrunk leaderboard (ascending - best = lowest)
        roi_shrunk_sorted = sorted(traders, key=lambda x: x.get('roi_shrunk', float('inf')))
        for i, trader in enumerate(roi_shrunk_sorted, 1):
            trader['rank'] = i
        leaderboards["roi_shrunk"] = [LeaderboardEntry(**t) for t in roi_shrunk_sorted]
        
        # 4. PNL shrunk leaderboard (ascending - best = lowest)
        pnl_shrunk_sorted = sorted(traders, key=lambda x: x.get('pnl_shrunk', float('inf')))
        for i, trader in enumerate(pnl_shrunk_sorted, 1):
            trader['rank'] = i
        leaderboards["pnl_shrunk"] = [LeaderboardEntry(**t) for t in pnl_shrunk_sorted]
        
        # 5. Final Score leaderboards (descending - best = highest)
        # Win Rate Score
        win_rate_sorted = sorted(traders, key=lambda x: x.get('score_win_rate', 0), reverse=True)
        for i, trader in enumerate(win_rate_sorted, 1):
            trader['rank'] = i
        leaderboards["score_win_rate"] = [LeaderboardEntry(**t) for t in win_rate_sorted]
        
        # ROI Score
        roi_score_sorted = sorted(traders, key=lambda x: x.get('score_roi', 0), reverse=True)
        for i, trader in enumerate(roi_score_sorted, 1):
            trader['rank'] = i
        leaderboards["score_roi"] = [LeaderboardEntry(**t) for t in roi_score_sorted]
        
        # PNL Score
        pnl_score_sorted = sorted(traders, key=lambda x: x.get('score_pnl', 0), reverse=True)
        for i, trader in enumerate(pnl_score_sorted, 1):
            trader['rank'] = i
        leaderboards["score_pnl"] = [LeaderboardEntry(**t) for t in pnl_score_sorted]
        
        # Risk Score (using fixed formula: |Worst Loss| / Total Stake, range 0-1)
        risk_sorted = sorted(traders, key=lambda x: x.get('score_risk', 0), reverse=True)
        for i, trader in enumerate(risk_sorted, 1):
            trader['rank'] = i
        leaderboards["score_risk"] = [LeaderboardEntry(**t) for t in risk_sorted]
        
        # Final Score (descending - best = highest)
        # Uses formula: Rating = 100 × [ wW · Wscore + wR · Rscore + wP · Pscore + wrisk · (1 − Risk Score) ]
        final_score_sorted = sorted(traders, key=lambda x: x.get('final_score', 0), reverse=True)
        for i, trader in enumerate(final_score_sorted, 1):
            trader['rank'] = i
        leaderboards["final_score"] = [LeaderboardEntry(**t) for t in final_score_sorted]
        
        # Apply limit and offset to each list
        for key in list(leaderboards.keys()):
            leaderboards[key] = leaderboards[key][offset : offset + limit]
        
        return AllLeaderboardsResponse(
            percentiles=PercentileInfo(
                w_shrunk_1_percent=percentiles_data.get(w_lower_key, 0.0),
                w_shrunk_99_percent=percentiles_data.get(w_upper_key, 0.0),
                roi_shrunk_1_percent=percentiles_data.get(roi_lower_key, 0.0),
                roi_shrunk_99_percent=percentiles_data.get(roi_upper_key, 0.0),
                pnl_shrunk_1_percent=percentiles_data.get(pnl_lower_key, 0.0),
                pnl_shrunk_99_percent=percentiles_data.get(pnl_upper_key, 0.0),
                population_size=result["population_size"]
            ),
            medians=MedianInfo(
                roi_median=medians_data["roi_median"],
                pnl_median=medians_data["pnl_median"]
            ),
            leaderboards=leaderboards,
            total_traders=len(traders),
            population_traders=percentiles_data.get("population_size", len(traders))
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generating all leaderboards: {str(e)}"
        )


@router.post(
    "/fetch-trader-details",
    summary="Fetch and Store Detailed Trader Data",
    description="Fetch detailed data (profile, value, positions, activity, closed positions, trades) for traders from trader_leaderboard and store in respective tables"
)
async def fetch_trader_details(
    trader_id: Optional[int] = Query(None, description="Trader ID from trader_leaderboard table"),
    wallet_address: Optional[str] = Query(None, description="Wallet address (used if trader_id not provided)"),
    fetch_all: bool = Query(False, description="Fetch details for all traders in trader_leaderboard"),
    limit: Optional[int] = Query(None, ge=1, description="Limit number of traders when fetch_all=true"),
    offset: int = Query(0, ge=0, description="Offset for pagination when fetch_all=true"),
    db: AsyncSession = Depends(get_db)
):
    """
    Fetch and store detailed trader data from Polymarket APIs.
    
    Fetches:
    1. Profile stats (trades, largestWin, views, joinDate)
    2. Value (user value)
    3. Positions (open positions)
    4. Activity (trading activity)
    5. Closed positions
    6. Trades
    
    Stores data in:
    - trader_profile
    - trader_value
    - trader_positions
    - trader_activity
    - trader_closed_positions
    - trader_trades
    
    Args:
        trader_id: Specific trader ID to fetch (optional)
        wallet_address: Specific wallet address to fetch (optional)
        fetch_all: If True, fetch for all traders in trader_leaderboard
        limit: Maximum number of traders when fetch_all=true
        offset: Offset for pagination when fetch_all=true
        db: Database session
    """
    try:
        from app.services.trader_detail_service import (
            fetch_and_save_trader_details,
            fetch_and_save_all_traders_details
        )
        
        if fetch_all:
            # Fetch for all traders
            result = await fetch_and_save_all_traders_details(
                session=db,
                limit=limit,
                offset=offset
            )
            
            return {
                "success": True,
                "message": f"Fetched details for {result['processed']} traders",
                "total_traders": result["total_traders"],
                "processed": result["processed"],
                "summary": result["summary"]
            }
        else:
            # Fetch for specific trader
            if not trader_id and not wallet_address:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Either trader_id or wallet_address must be provided when fetch_all=false"
                )
            
            result = await fetch_and_save_trader_details(
                session=db,
                trader_id=trader_id,
                wallet_address=wallet_address
            )
            
            if "error" in result:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=result["error"]
                )
            
            await db.commit()
            
            return {
                "success": True,
                "trader_id": result["trader_id"],
                "wallet_address": result["wallet_address"],
                "profile_saved": result["profile_saved"],
                "value_saved": result["value_saved"],
                "positions_saved": result["positions_saved"],
                "activities_saved": result["activities_saved"],
                "closed_positions_saved": result["closed_positions_saved"],
                "trades_saved": result["trades_saved"],
                "errors": result.get("errors", [])
            }
            
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching trader details: {str(e)}"
        )


@router.get(
    "/daily-volume",
    response_model=LeaderboardResponse,
    responses={
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Get Daily Volume Leaderboard from Database",
    description="Get daily volume leaderboard data from database with pagination, sorted by volume descending"
)
async def get_daily_volume_leaderboard(
    limit: int = Query(50, ge=1, le=1000, description="Maximum number of traders to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    order_by: str = Query("VOL", regex="^(VOL|PNL|ROI|WIN_RATE|SCORE)$", description="Order by 'VOL', 'PNL', 'ROI', 'WIN_RATE', or 'SCORE'"),
    db: AsyncSession = Depends(get_db)
):
    """
    Get daily volume leaderboard from database.
    
    Returns traders ranked by metric (descending) with pagination.
    Data is fetched from daily_volume_leaderboard table.
    """
    try:
        # Check if we should use Goldsky (only for Volume sorting)
        if order_by == "VOL" and settings.GOLDSKY_SUBGRAPH_URL:
            try:
                goldsky_data = await GoldskyService.fetch_volume_leaderboard("day", limit=limit)
                if goldsky_data:
                    entries = []
                    for idx, row in enumerate(goldsky_data):
                        entry = LeaderboardEntry(
                            rank=offset + idx + 1,
                            wallet_address=row["wallet_address"],
                            name="", # Goldsky doesn't have profile info
                            pseudonym="",
                            profile_image="",
                            total_pnl=0.0,
                            roi=0.0,
                            win_rate=0.0,
                            total_trades=row["total_trades"],
                            total_trades_with_pnl=0,
                            winning_trades=0,
                            total_stakes=row["volume"], # Using volume
                            score_win_rate=0.0,
                            score_roi=0.0,
                            score_pnl=0.0,
                            score_risk=0.0,
                            final_score=0.0,
                            W_shrunk=None,
                            roi_shrunk=None,
                            pnl_shrunk=None
                        )
                        entries.append(entry)
                    
                    return LeaderboardResponse(
                        period="day",
                        metric="vol",
                        count=len(entries), # Approximation
                        entries=entries
                    )
            except Exception as e:
                print(f"Goldsky fetch failed, falling back to DB: {e}")
                # Fallback to DB

        sort_column = "volume"
        if order_by == "PNL":
            sort_column = "pnl"
        elif order_by == "ROI":
            sort_column = "roi"
        elif order_by == "WIN_RATE":
            sort_column = "win_rate"
        elif order_by == "SCORE":
            sort_column = "final_score"

        # Query database for daily volume leaderboard, sorted by chosen metric descending
        result = await db.execute(
            text(f"""
                SELECT 
                    rank,
                    wallet_address,
                    name,
                    pseudonym,
                    profile_image,
                    pnl,
                    volume,
                    roi,
                    win_rate,
                    total_trades,
                    total_trades_with_pnl,
                    winning_trades,
                    total_stakes,
                    score_win_rate,
                    score_roi,
                    score_pnl,
                    score_risk,
                    final_score,
                    w_shrunk,
                    roi_shrunk,
                    pnl_shrunk,
                    verified_badge,
                    raw_data
                FROM daily_volume_leaderboard
                WHERE {sort_column} IS NOT NULL
                ORDER BY {sort_column} DESC
                LIMIT :limit OFFSET :offset
            """),
            {"limit": limit, "offset": offset}
        )
        
        rows = result.fetchall()
        
        # Get total count
        count_result = await db.execute(
            text(f"SELECT COUNT(*) FROM daily_volume_leaderboard WHERE {sort_column} IS NOT NULL")
        )
        total_count = count_result.scalar()
        
        # Transform to LeaderboardEntry format
        entries = []
        for idx, row in enumerate(rows):
            # Calculate rank based on offset
            actual_rank = offset + idx + 1
            
            entry = LeaderboardEntry(
                rank=actual_rank,
                wallet_address=row.wallet_address,
                name=row.name,
                pseudonym=row.pseudonym,
                profile_image=row.profile_image,
                total_pnl=float(row.pnl) if row.pnl is not None else 0.0,
                roi=float(row.roi) if row.roi is not None else 0.0,
                win_rate=float(row.win_rate) if row.win_rate is not None else 0.0,
                total_trades=int(row.total_trades) if row.total_trades is not None else 0,
                total_trades_with_pnl=int(row.total_trades_with_pnl) if row.total_trades_with_pnl is not None else 0,
                winning_trades=int(row.winning_trades) if row.winning_trades is not None else 0,
                total_stakes=float(row.total_stakes) if row.total_stakes is not None else float(row.volume) if row.volume else 0.0,
                score_win_rate=float(row.score_win_rate) if row.score_win_rate is not None else 0.0,
                score_roi=float(row.score_roi) if row.score_roi is not None else 0.0,
                score_pnl=float(row.score_pnl) if row.score_pnl is not None else 0.0,
                score_risk=float(row.score_risk) if row.score_risk is not None else 0.0,
                final_score=float(row.final_score) if row.final_score is not None else 0.0,
                W_shrunk=float(row.w_shrunk) if row.w_shrunk is not None else None,
                roi_shrunk=float(row.roi_shrunk) if row.roi_shrunk is not None else None,
                pnl_shrunk=float(row.pnl_shrunk) if row.pnl_shrunk is not None else None
            )
            entries.append(entry)
        
        return LeaderboardResponse(
            period="day",
            metric=order_by.lower(),
            count=total_count,
            entries=entries
        )
    except Exception as e:
        import traceback
        print(f"Error fetching daily volume leaderboard: {e}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching daily volume leaderboard: {str(e)}"
        )


@router.get(
    "/weekly-volume",
    response_model=LeaderboardResponse,
    responses={
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Get Weekly Volume Leaderboard from Database",
    description="Get weekly volume leaderboard data from database with pagination, sorted by volume descending"
)
async def get_weekly_volume_leaderboard(
    limit: int = Query(50, ge=1, le=1000, description="Maximum number of traders to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    order_by: str = Query("VOL", regex="^(VOL|PNL|ROI|WIN_RATE|SCORE)$", description="Order by 'VOL', 'PNL', 'ROI', 'WIN_RATE', or 'SCORE'"),
    db: AsyncSession = Depends(get_db)
):
    """
    Get weekly volume leaderboard from database.
    
    Returns traders ranked by metric (descending) with pagination.
    Data is fetched from weekly_volume_leaderboard table.
    """
    try:
        # Check if we should use Goldsky (only for Volume sorting)
        if order_by == "VOL" and settings.GOLDSKY_SUBGRAPH_URL:
            try:
                goldsky_data = await GoldskyService.fetch_volume_leaderboard("week", limit=limit)
                if goldsky_data:
                    entries = []
                    for idx, row in enumerate(goldsky_data):
                        entry = LeaderboardEntry(
                            rank=offset + idx + 1,
                            wallet_address=row["wallet_address"],
                            name="",
                            pseudonym="",
                            profile_image="",
                            total_pnl=0.0,
                            roi=0.0,
                            win_rate=0.0,
                            total_trades=row["total_trades"],
                            total_trades_with_pnl=0,
                            winning_trades=0,
                            total_stakes=row["volume"],
                            score_win_rate=0.0,
                            score_roi=0.0,
                            score_pnl=0.0,
                            score_risk=0.0,
                            final_score=0.0,
                            W_shrunk=None,
                            roi_shrunk=None,
                            pnl_shrunk=None
                        )
                        entries.append(entry)
                    
                    return LeaderboardResponse(
                        period="week",
                        metric="vol",
                        count=len(entries),
                        entries=entries
                    )
            except Exception as e:
                print(f"Goldsky fetch failed, falling back to DB: {e}")

        sort_column = "volume"
        if order_by == "PNL":
            sort_column = "pnl"
        elif order_by == "ROI":
            sort_column = "roi"
        elif order_by == "WIN_RATE":
            sort_column = "win_rate"
        elif order_by == "SCORE":
            sort_column = "final_score"

        # Query database for weekly volume leaderboard, sorted by chosen metric descending
        result = await db.execute(
            text(f"""
                SELECT 
                    rank,
                    wallet_address,
                    name,
                    pseudonym,
                    profile_image,
                    pnl,
                    volume,
                    roi,
                    win_rate,
                    total_trades,
                    total_trades_with_pnl,
                    winning_trades,
                    total_stakes,
                    score_win_rate,
                    score_roi,
                    score_pnl,
                    score_risk,
                    final_score,
                    w_shrunk,
                    roi_shrunk,
                    pnl_shrunk,
                    verified_badge,
                    raw_data
                FROM weekly_volume_leaderboard
                WHERE {sort_column} IS NOT NULL
                ORDER BY {sort_column} DESC
                LIMIT :limit OFFSET :offset
            """),
            {"limit": limit, "offset": offset}
        )
        
        rows = result.fetchall()
        
        # Get total count
        count_result = await db.execute(
            text(f"SELECT COUNT(*) FROM weekly_volume_leaderboard WHERE {sort_column} IS NOT NULL")
        )
        total_count = count_result.scalar()
        
        # Transform to LeaderboardEntry format
        entries = []
        for idx, row in enumerate(rows):
            # Calculate rank based on offset
            actual_rank = offset + idx + 1
            
            entry = LeaderboardEntry(
                rank=actual_rank,
                wallet_address=row.wallet_address,
                name=row.name,
                pseudonym=row.pseudonym,
                profile_image=row.profile_image,
                total_pnl=float(row.pnl) if row.pnl is not None else 0.0,
                roi=float(row.roi) if row.roi is not None else 0.0,
                win_rate=float(row.win_rate) if row.win_rate is not None else 0.0,
                total_trades=int(row.total_trades) if row.total_trades is not None else 0,
                total_trades_with_pnl=int(row.total_trades_with_pnl) if row.total_trades_with_pnl is not None else 0,
                winning_trades=int(row.winning_trades) if row.winning_trades is not None else 0,
                total_stakes=float(row.total_stakes) if row.total_stakes is not None else float(row.volume) if row.volume else 0.0,
                score_win_rate=float(row.score_win_rate) if row.score_win_rate is not None else 0.0,
                score_roi=float(row.score_roi) if row.score_roi is not None else 0.0,
                score_pnl=float(row.score_pnl) if row.score_pnl is not None else 0.0,
                score_risk=float(row.score_risk) if row.score_risk is not None else 0.0,
                final_score=float(row.final_score) if row.final_score is not None else 0.0,
                W_shrunk=float(row.w_shrunk) if row.w_shrunk is not None else None,
                roi_shrunk=float(row.roi_shrunk) if row.roi_shrunk is not None else None,
                pnl_shrunk=float(row.pnl_shrunk) if row.pnl_shrunk is not None else None
            )
            entries.append(entry)
        
        return LeaderboardResponse(
            period="week",
            metric=order_by.lower(),
            count=total_count,
            entries=entries
        )
    except Exception as e:
        import traceback
        print(f"Error fetching weekly volume leaderboard: {e}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching weekly volume leaderboard: {str(e)}"
        )


@router.get(
    "/monthly-volume",
    response_model=LeaderboardResponse,
    responses={
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Get Monthly Volume Leaderboard from Database",
    description="Get monthly volume leaderboard data from database with pagination, sorted by volume descending"
)
async def get_monthly_volume_leaderboard(
    limit: int = Query(50, ge=1, le=1000, description="Maximum number of traders to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    order_by: str = Query("VOL", regex="^(VOL|PNL|ROI|WIN_RATE|SCORE)$", description="Order by 'VOL', 'PNL', 'ROI', 'WIN_RATE', or 'SCORE'"),
    db: AsyncSession = Depends(get_db)
):
    """
    Get monthly volume leaderboard from database.
    
    Returns traders ranked by volume (descending) with pagination.
    Data is fetched from monthly_volume_leaderboard table.
    """
    try:
        # Check if we should use Goldsky (only for Volume sorting)
        if order_by == "VOL" and settings.GOLDSKY_SUBGRAPH_URL:
            try:
                goldsky_data = await GoldskyService.fetch_volume_leaderboard("month", limit=limit)
                if goldsky_data:
                    entries = []
                    for idx, row in enumerate(goldsky_data):
                        entry = LeaderboardEntry(
                            rank=offset + idx + 1,
                            wallet_address=row["wallet_address"],
                            name="",
                            pseudonym="",
                            profile_image="",
                            total_pnl=0.0,
                            roi=0.0,
                            win_rate=0.0,
                            total_trades=row["total_trades"],
                            total_trades_with_pnl=0,
                            winning_trades=0,
                            total_stakes=row["volume"],
                            score_win_rate=0.0,
                            score_roi=0.0,
                            score_pnl=0.0,
                            score_risk=0.0,
                            final_score=0.0,
                            W_shrunk=None,
                            roi_shrunk=None,
                            pnl_shrunk=None
                        )
                        entries.append(entry)
                    
                    return LeaderboardResponse(
                        period="month",
                        metric="vol",
                        count=len(entries),
                        entries=entries
                    )
            except Exception as e:
                print(f"Goldsky fetch failed, falling back to DB: {e}")

        sort_column = "volume"
        if order_by == "PNL":
            sort_column = "pnl"
        elif order_by == "ROI":
            sort_column = "roi"
        elif order_by == "WIN_RATE":
            sort_column = "win_rate"
        elif order_by == "SCORE":
            sort_column = "final_score"
        
        # Query database for monthly volume leaderboard, sorted by chosen metric descending
        result = await db.execute(
            text(f"""
                SELECT 
                    rank,
                    wallet_address,
                    name,
                    pseudonym,
                    profile_image,
                    pnl,
                    volume,
                    roi,
                    win_rate,
                    total_trades,
                    total_trades_with_pnl,
                    winning_trades,
                    total_stakes,
                    score_win_rate,
                    score_roi,
                    score_pnl,
                    score_risk,
                    final_score,
                    w_shrunk,
                    roi_shrunk,
                    pnl_shrunk,
                    verified_badge,
                    raw_data
                FROM monthly_volume_leaderboard
                WHERE {sort_column} IS NOT NULL
                ORDER BY {sort_column} DESC
                LIMIT :limit OFFSET :offset
            """),
            {"limit": limit, "offset": offset}
        )
        
        rows = result.fetchall()
        
        # Get total count
        count_result = await db.execute(
            text(f"SELECT COUNT(*) FROM monthly_volume_leaderboard WHERE {sort_column} IS NOT NULL")
        )
        total_count = count_result.scalar()
        
        # Transform to LeaderboardEntry format
        entries = []
        for idx, row in enumerate(rows):
            # Calculate rank based on offset
            actual_rank = offset + idx + 1
            
            entry = LeaderboardEntry(
                rank=actual_rank,
                wallet_address=row.wallet_address,
                name=row.name,
                pseudonym=row.pseudonym,
                profile_image=row.profile_image,
                total_pnl=float(row.pnl) if row.pnl is not None else 0.0,
                roi=float(row.roi) if row.roi is not None else 0.0,
                win_rate=float(row.win_rate) if row.win_rate is not None else 0.0,
                total_trades=int(row.total_trades) if row.total_trades is not None else 0,
                total_trades_with_pnl=int(row.total_trades_with_pnl) if row.total_trades_with_pnl is not None else 0,
                winning_trades=int(row.winning_trades) if row.winning_trades is not None else 0,
                total_stakes=float(row.total_stakes) if row.total_stakes is not None else float(row.volume) if row.volume else 0.0,
                score_win_rate=float(row.score_win_rate) if row.score_win_rate is not None else 0.0,
                score_roi=float(row.score_roi) if row.score_roi is not None else 0.0,
                score_pnl=float(row.score_pnl) if row.score_pnl is not None else 0.0,
                score_risk=float(row.score_risk) if row.score_risk is not None else 0.0,
                final_score=float(row.final_score) if row.final_score is not None else 0.0,
                W_shrunk=float(row.w_shrunk) if row.w_shrunk is not None else None,
                roi_shrunk=float(row.roi_shrunk) if row.roi_shrunk is not None else None,
                pnl_shrunk=float(row.pnl_shrunk) if row.pnl_shrunk is not None else None
            )
            entries.append(entry)
        
        return LeaderboardResponse(
            period="month",
            metric=order_by.lower(),
            count=total_count,
            entries=entries
        )
    except Exception as e:
        import traceback
        print(f"Error fetching monthly volume leaderboard: {e}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching monthly volume leaderboard: {str(e)}"
        )


@router.get(
    "/entries",
    response_model=LeaderboardResponse,
    responses={
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Get Leaderboard Entries from Database",
    description="Get calculated leaderboard entries directly from leaderboard_entries table"
)
async def get_leaderboard_entries(
    limit: int = Query(50, ge=1, le=1000, description="Maximum number of traders to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    order_by: str = Query("SCORE", regex="^(VOL|PNL|ROI|WIN_RATE|SCORE)$", description="Order by 'VOL', 'PNL', 'ROI', 'WIN_RATE', or 'SCORE'"),
    db: AsyncSession = Depends(get_db)
):
    """
    Get leaderboard entries from database.
    
    Returns traders ranked by calculated score/metric from leaderboard_entries table.
    """
    try:
        from app.services.leaderboard_storage_service import get_leaderboard_from_db, get_total_leaderboard_count
        
        # Map API sort param to database column
        sort_field = "final_score"
        if order_by == "PNL":
            sort_field = "total_pnl" # Note: total_pnl in entries vs pnl in volume tables
        elif order_by == "ROI":
            sort_field = "roi"
        elif order_by == "WIN_RATE":
            sort_field = "win_rate"
        elif order_by == "VOL":
            sort_field = "total_stakes"
        
        # Fetch entries
        entries_data = await get_leaderboard_from_db(
            session=db,
            limit=limit,
            offset=offset,
            sort_by=sort_field,
            sort_desc=True
        )
        
        # Get total count
        total_count = await get_total_leaderboard_count(db)
        
        # strict typing map
        entries = []
        for entry in entries_data:
            entries.append(LeaderboardEntry(
                rank=entry["rank"],
                wallet_address=entry["wallet_address"],
                name=entry["name"],
                pseudonym=entry["pseudonym"],
                profile_image=entry["profile_image"],
                total_pnl=entry["total_pnl"],
                roi=entry["roi"],
                win_rate=entry["win_rate"],
                total_trades=entry["total_trades"],
                total_trades_with_pnl=entry["total_trades_with_pnl"],
                winning_trades=entry["winning_trades"],
                total_stakes=entry["total_stakes"],
                score_win_rate=entry["score_win_rate"],
                score_roi=entry["score_roi"],
                score_pnl=entry["score_pnl"],
                score_risk=entry["score_risk"],
                final_score=entry["final_score"],
                W_shrunk=entry["W_shrunk"],
                roi_shrunk=entry["roi_shrunk"],
                pnl_shrunk=entry["pnl_shrunk"]
            ))
            
        return LeaderboardResponse(
            period="all_time",
            metric=order_by.lower(),
            count=total_count,
            entries=entries
        )
    except Exception as e:
        import traceback
        print(f"Error fetching leaderboard entries: {e}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching leaderboard entries: {str(e)}"
        )



# Force reload trigger
