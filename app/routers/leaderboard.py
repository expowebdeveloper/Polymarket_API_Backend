"""Leaderboard API routes."""

from fastapi import APIRouter, Query, HTTPException, status, Depends, Body
from fastapi.responses import JSONResponse
from typing import Literal, List
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel, Field
from app.schemas.leaderboard import LeaderboardResponse, LeaderboardEntry, AllLeaderboardsResponse, PercentileInfo, MedianInfo
from app.schemas.general import ErrorResponse
from app.services.leaderboard_service import (
    calculate_scores_and_rank_with_percentiles
)
from app.services.pnl_median_service import get_pnl_median_from_population
from app.services.live_leaderboard_service import (
    fetch_live_leaderboard_from_file,
    fetch_raw_metrics_for_scoring
)
from app.services.trade_service import fetch_and_save_trades
from app.services.position_service import fetch_and_save_positions
from app.services.activity_service import fetch_and_save_activities
from app.services.db_scoring_service import get_db_leaderboard
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
        # scoring_script.py produces 'score_roi'.
        
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
        
        # Get medians from Polymarket API (all traders in file, fetched from API)
        pnl_median_api = await get_pnl_median_from_population()
        
        # Calculate scores with percentile information
        # Pass API PnL median to use in calculations
        result = calculate_scores_and_rank_with_percentiles(
            entries_data,
            pnl_median=pnl_median_api
        )
        traders = result["traders"]
        percentiles_data = result["percentiles"]
        medians_data = result["medians"]
        
        # Override PnL median with API value
        medians_data["pnl_median"] = pnl_median_api
        
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
        
        # Risk Score
        risk_sorted = sorted(traders, key=lambda x: x.get('score_risk', 0), reverse=True)
        for i, trader in enumerate(risk_sorted, 1):
            trader['rank'] = i
        leaderboards["score_risk"] = [LeaderboardEntry(**t) for t in risk_sorted]
        
        # Final Score (descending - best = highest)
        final_score_sorted = sorted(traders, key=lambda x: x.get('final_score', 0), reverse=True)
        for i, trader in enumerate(final_score_sorted, 1):
            trader['rank'] = i
        leaderboards["final_score"] = [LeaderboardEntry(**t) for t in final_score_sorted]
        
        return AllLeaderboardsResponse(
            percentiles=PercentileInfo(
                w_shrunk_1_percent=percentiles_data["w_shrunk_1_percent"],
                w_shrunk_99_percent=percentiles_data["w_shrunk_99_percent"],
                roi_shrunk_1_percent=percentiles_data["roi_shrunk_1_percent"],
                roi_shrunk_99_percent=percentiles_data["roi_shrunk_99_percent"],
                pnl_shrunk_1_percent=percentiles_data["pnl_shrunk_1_percent"],
                pnl_shrunk_99_percent=percentiles_data["pnl_shrunk_99_percent"],
                population_size=result["population_size"]
            ),
            medians=MedianInfo(
                roi_median=medians_data["roi_median"],
                pnl_median=medians_data["pnl_median"]
            ),
            leaderboards=leaderboards,
            total_traders=len(traders),
            population_traders=result["population_size"]
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
    summary="Get Database Leaderboard by Final Score",
    description="Calculate leaderboard scores for ALL wallets in database using advanced formula"
)
async def get_db_final_score_leaderboard(
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of traders to return"),
    use_file: bool = Query(False, description="Whether to limit to wallets in wallet_address.txt"),
    db: AsyncSession = Depends(get_db)
):
    """
    Generate a database-backed leaderboard using the advanced scoring logic.
    By default, calculates for all wallets in DB.
    """
    try:
        wallets = None
        if use_file:
            file_path = "wallet_address.txt"
            try:
                with open(file_path, 'r') as f:
                    wallets = [line.strip() for line in f if line.strip()]
            except FileNotFoundError:
                pass
            
        leaderboard_data = await get_db_leaderboard(db, wallet_addresses=wallets, limit=limit, metric="final_score")
        
        entries = [LeaderboardEntry(**e) for e in leaderboard_data]
        
        return LeaderboardResponse(
            period="all",
            metric="final_score",
            count=len(entries),
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
    summary="Get All Database Leaderboards with Advanced Percentile Logic",
    description="Get all database-backed leaderboards (W_shrunk, ROI_shrunk, etc.) using advanced formulas"
)
async def get_all_db_leaderboards(
    use_file: bool = Query(False, description="Whether to limit to wallets in wallet_address.txt"),
    db: AsyncSession = Depends(get_db)
):
    """
    Generate all database-backed leaderboards with percentile information.
    Uses the advanced formulas (shrinkage, whale penalty, percentiles).
    """
    try:
        from app.services.db_scoring_service import get_advanced_db_analytics
        
        wallets = None
        if use_file:
            file_path = "wallet_address.txt"
            try:
                with open(file_path, 'r') as f:
                    wallets = [line.strip() for line in f if line.strip()]
            except FileNotFoundError:
                pass

        result = await get_advanced_db_analytics(db, wallet_addresses=wallets)
        traders = result.get("traders", [])
        percentiles_data = result.get("percentiles", {})
        medians_data = result.get("medians", {})
        
        # Create all different leaderboards (same as in view_all_leaderboards)
        leaderboards = {}
        
        # 1. W_shrunk leaderboard (ascending)
        w_shrunk_sorted = sorted(traders, key=lambda x: x.get('W_shrunk', float('inf')))
        for i, t in enumerate(w_shrunk_sorted, 1):
            t['rank'] = i
        leaderboards["w_shrunk"] = [LeaderboardEntry(**t) for t in w_shrunk_sorted]
        
        # 2. ROI raw leaderboard (descending)
        roi_raw_sorted = sorted(traders, key=lambda x: x.get('roi', float('-inf')), reverse=True)
        for i, t in enumerate(roi_raw_sorted, 1):
            t['rank'] = i
        leaderboards["roi_raw"] = [LeaderboardEntry(**t) for t in roi_raw_sorted]
        
        # 3. ROI shrunk leaderboard (ascending)
        roi_shrunk_sorted = sorted(traders, key=lambda x: x.get('roi_shrunk', float('inf')))
        for i, t in enumerate(roi_shrunk_sorted, 1):
            t['rank'] = i
        leaderboards["roi_shrunk"] = [LeaderboardEntry(**t) for t in roi_shrunk_sorted]
        
        # 4. PNL shrunk leaderboard (ascending)
        pnl_shrunk_sorted = sorted(traders, key=lambda x: x.get('pnl_shrunk', float('inf')))
        for i, t in enumerate(pnl_shrunk_sorted, 1):
            t['rank'] = i
        leaderboards["pnl_shrunk"] = [LeaderboardEntry(**t) for t in pnl_shrunk_sorted]
        
        # 5. Score leaderboards (descending)
        for score_metric in ["score_win_rate", "score_roi", "score_pnl", "score_risk", "final_score"]:
            sorted_traders = sorted(traders, key=lambda x: x.get(score_metric, 0), reverse=True)
            for i, t in enumerate(sorted_traders, 1):
                t['rank'] = i
            leaderboards[score_metric] = [LeaderboardEntry(**t) for t in sorted_traders]
            
        return AllLeaderboardsResponse(
            percentiles=PercentileInfo(
                w_shrunk_1_percent=percentiles_data.get("w_shrunk_1_percent", 0.0),
                w_shrunk_99_percent=percentiles_data.get("w_shrunk_99_percent", 0.0),
                roi_shrunk_1_percent=percentiles_data.get("roi_shrunk_1_percent", 0.0),
                roi_shrunk_99_percent=percentiles_data.get("roi_shrunk_99_percent", 0.0),
                pnl_shrunk_1_percent=percentiles_data.get("pnl_shrunk_1_percent", 0.0),
                pnl_shrunk_99_percent=percentiles_data.get("pnl_shrunk_99_percent", 0.0),
                population_size=result.get("population_size", 0)
            ),
            medians=MedianInfo(
                roi_median=medians_data.get("roi_median", 0.0),
                pnl_median=medians_data.get("pnl_median", 0.0)
            ),
            leaderboards=leaderboards,
            total_traders=len(traders),
            population_traders=result.get("population_size", 0)
        )
    except Exception as e:
        import traceback
        print(f"Error generating all DB leaderboards: {e}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generating all DB leaderboards: {str(e)}"
        )



@router.get(
    "/view-all",
    response_model=AllLeaderboardsResponse,
    responses={
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="View All Leaderboards (JSON)",
    description="Get all leaderboards and percentile information in JSON format with proper data structure"
)
async def view_all_leaderboards():
    """
    Get all leaderboards and percentile information in JSON format.
    
    This endpoint:
    1. Fetches raw metrics for all wallets from wallet_address.txt
    2. Calculates scores with percentile information using the fixed formulas
    3. Returns all leaderboards sorted by different metrics
    4. Includes percentile anchors, medians, and population statistics
    
    Returns:
    - All leaderboards sorted by different metrics (W_shrunk, ROI_raw, ROI_shrunk, PNL_shrunk, final scores)
    - Percentile information (configurable percentiles for W, ROI, and PNL shrunk values)
    - Median values (ROI median and PNL median used in shrinkage)
    - Population statistics
    """
    try:
        file_path = "wallet_address.txt"
        # Fetch raw metrics without calculating scores (to avoid double calculation)
        entries_data = await fetch_raw_metrics_for_scoring(file_path)
        
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
        
        # Get medians from Polymarket API (all traders in file, fetched from API)
        pnl_median_api = await get_pnl_median_from_population()
        
        # Calculate scores with percentile information (single calculation)
        # Pass API PnL median to use in calculations
        result = calculate_scores_and_rank_with_percentiles(
            entries_data,
            pnl_median=pnl_median_api
        )
        traders = result["traders"]
        percentiles_data = result["percentiles"]
        medians_data = result["medians"]
        
        # Override PnL median with API value
        medians_data["pnl_median"] = pnl_median_api
        
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
        
        # Risk Score
        risk_sorted = sorted(traders, key=lambda x: x.get('score_risk', 0), reverse=True)
        for i, trader in enumerate(risk_sorted, 1):
            trader['rank'] = i
        leaderboards["score_risk"] = [LeaderboardEntry(**t) for t in risk_sorted]
        
        # Final Score (descending - best = highest)
        final_score_sorted = sorted(traders, key=lambda x: x.get('final_score', 0), reverse=True)
        for i, trader in enumerate(final_score_sorted, 1):
            trader['rank'] = i
        leaderboards["final_score"] = [LeaderboardEntry(**t) for t in final_score_sorted]
        
        return AllLeaderboardsResponse(
            percentiles=PercentileInfo(
                w_shrunk_1_percent=percentiles_data["w_shrunk_1_percent"],
                w_shrunk_99_percent=percentiles_data["w_shrunk_99_percent"],
                roi_shrunk_1_percent=percentiles_data["roi_shrunk_1_percent"],
                roi_shrunk_99_percent=percentiles_data["roi_shrunk_99_percent"],
                pnl_shrunk_1_percent=percentiles_data["pnl_shrunk_1_percent"],
                pnl_shrunk_99_percent=percentiles_data["pnl_shrunk_99_percent"],
                population_size=result["population_size"]
            ),
            medians=MedianInfo(
                roi_median=medians_data["roi_median"],
                pnl_median=medians_data["pnl_median"]
            ),
            leaderboards=leaderboards,
            total_traders=result["total_traders"],
            )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generating all leaderboards: {str(e)}"
        )


# Force reload trigger
