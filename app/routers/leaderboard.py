"""Leaderboard API routes."""

from fastapi import APIRouter, Query, HTTPException, status, Depends, Body
from fastapi.responses import HTMLResponse
from typing import Literal, List
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel, Field
from app.schemas.leaderboard import LeaderboardResponse, LeaderboardEntry, AllLeaderboardsResponse, PercentileInfo, MedianInfo
from app.schemas.general import ErrorResponse
from app.services.leaderboard_service import (
    get_leaderboard_by_pnl,
    get_leaderboard_by_roi,
    get_leaderboard_by_win_rate,
    calculate_scores_and_rank_with_percentiles
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
        
        # Calculate scores with percentile information
        result = calculate_scores_and_rank_with_percentiles(entries_data)
        traders = result["traders"]
        percentiles_data = result["percentiles"]
        medians_data = result["medians"]
        
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
            population_traders=result["population_size"]
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generating all leaderboards: {str(e)}"
        )


@router.get(
    "/view-all",
    response_class=HTMLResponse,
    summary="View All Leaderboards (HTML)",
    description="Display all leaderboards and percentile information in a readable HTML format"
)
async def view_all_leaderboards_html():
    """
    Display all leaderboards and percentile information in HTML format.
    """
    try:
        file_path = "wallet_address.txt"
        entries_data = await fetch_live_leaderboard_from_file(file_path)
        
        if not entries_data:
            return HTMLResponse(content="""
            <!DOCTYPE html>
            <html>
            <head>
                <title>All Leaderboards</title>
                <style>
                    body { font-family: Arial, sans-serif; margin: 20px; background: #1a1a1a; color: #fff; }
                    h1 { color: #f59e0b; }
                    .error { color: #ef4444; }
                </style>
            </head>
            <body>
                <h1>All Leaderboards</h1>
                <p class="error">No trader data available. Please ensure wallet_address.txt exists and contains wallet addresses.</p>
            </body>
            </html>
            """)
        
        # Calculate scores with percentile information
        result = calculate_scores_and_rank_with_percentiles(entries_data)
        traders = result["traders"]
        percentiles_data = result["percentiles"]
        medians_data = result["medians"]
        
        # Create HTML content
        html_content = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>All Leaderboards & Percentile Information</title>
            <style>
                body { 
                    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
                    margin: 20px; 
                    background: #0f172a; 
                    color: #e2e8f0; 
                }
                h1 { color: #f59e0b; border-bottom: 2px solid #f59e0b; padding-bottom: 10px; }
                h2 { color: #8b5cf6; margin-top: 30px; }
                h3 { color: #10b981; margin-top: 20px; }
                .section { 
                    background: #1e293b; 
                    padding: 20px; 
                    margin: 20px 0; 
                    border-radius: 8px; 
                    border: 1px solid #334155; 
                }
                .percentile-info, .median-info {
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                    gap: 15px;
                    margin: 15px 0;
                }
                .info-card {
                    background: #0f172a;
                    padding: 15px;
                    border-radius: 6px;
                    border-left: 4px solid #8b5cf6;
                }
                .info-label { color: #94a3b8; font-size: 0.9em; }
                .info-value { color: #f59e0b; font-size: 1.2em; font-weight: bold; }
                table { 
                    width: 100%; 
                    border-collapse: collapse; 
                    margin: 15px 0;
                    background: #1e293b;
                }
                th { 
                    background: #334155; 
                    color: #f59e0b; 
                    padding: 12px; 
                    text-align: left; 
                    border: 1px solid #475569;
                }
                td { 
                    padding: 10px; 
                    border: 1px solid #334155; 
                }
                tr:nth-child(even) { background: #0f172a; }
                .rank { color: #10b981; font-weight: bold; }
                .positive { color: #10b981; }
                .negative { color: #ef4444; }
                .leaderboard-section { margin: 30px 0; }
                .stats { 
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                    gap: 15px;
                    margin: 20px 0;
                }
                .stat-card {
                    background: #0f172a;
                    padding: 15px;
                    border-radius: 6px;
                    border-left: 4px solid #10b981;
                }
            </style>
        </head>
        <body>
            <h1>📊 All Leaderboards & Percentile Information</h1>
        """
        
        # Add statistics
        html_content += f"""
            <div class="section">
                <h2>📈 Statistics</h2>
                <div class="stats">
                    <div class="stat-card">
                        <div class="info-label">Total Traders</div>
                        <div class="info-value">{result['total_traders']}</div>
                    </div>
                    <div class="stat-card">
                        <div class="info-label">Population (≥5 trades)</div>
                        <div class="info-value">{result['population_size']}</div>
                    </div>
                </div>
            </div>
        """
        
        # Add percentile information
        html_content += f"""
            <div class="section">
                <h2>📊 Percentile Anchors (for Normalization)</h2>
                <p style="color: #94a3b8; margin-bottom: 15px;">
                    These values are calculated from traders with ≥5 trades and used to normalize scores to 0-1 range.
                </p>
                <div class="percentile-info">
                    <div class="info-card">
                        <div class="info-label">W_shrunk 1st Percentile</div>
                        <div class="info-value">{percentiles_data['w_shrunk_1_percent']:.6f}</div>
                    </div>
                    <div class="info-card">
                        <div class="info-label">W_shrunk 99th Percentile</div>
                        <div class="info-value">{percentiles_data['w_shrunk_99_percent']:.6f}</div>
                    </div>
                    <div class="info-card">
                        <div class="info-label">ROI_shrunk 1st Percentile</div>
                        <div class="info-value">{percentiles_data['roi_shrunk_1_percent']:.6f}</div>
                    </div>
                    <div class="info-card">
                        <div class="info-label">ROI_shrunk 99th Percentile</div>
                        <div class="info-value">{percentiles_data['roi_shrunk_99_percent']:.6f}</div>
                    </div>
                    <div class="info-card">
                        <div class="info-label">PNL_shrunk 1st Percentile</div>
                        <div class="info-value">{percentiles_data['pnl_shrunk_1_percent']:.6f}</div>
                    </div>
                    <div class="info-card">
                        <div class="info-label">PNL_shrunk 99th Percentile</div>
                        <div class="info-value">{percentiles_data['pnl_shrunk_99_percent']:.6f}</div>
                    </div>
                </div>
            </div>
        """
        
        # Add median information
        html_content += f"""
            <div class="section">
                <h2>📊 Median Values (used in Shrinkage)</h2>
                <p style="color: #94a3b8; margin-bottom: 15px;">
                    These medians are calculated from traders with ≥5 trades and used in the shrinkage formulas.
                </p>
                <div class="median-info">
                    <div class="info-card">
                        <div class="info-label">ROI Median</div>
                        <div class="info-value">{medians_data['roi_median']:.6f}%</div>
                    </div>
                    <div class="info-card">
                        <div class="info-label">PnL Median (Adjusted)</div>
                        <div class="info-value">${medians_data['pnl_median']:.2f}</div>
                    </div>
                </div>
            </div>
        """
        
        # Helper function to format leaderboard table
        def format_leaderboard_table(traders_list, title, limit=20):
            table_html = f"""
            <div class="leaderboard-section">
                <h3>{title} (Top {min(limit, len(traders_list))})</h3>
                <table>
                    <thead>
                        <tr>
                            <th>Rank</th>
                            <th>Wallet</th>
                            <th>Total PnL</th>
                            <th>ROI</th>
                            <th>Win Rate</th>
                            <th>Trades</th>
                            <th>W_shrunk</th>
                            <th>ROI_shrunk</th>
                            <th>PNL_shrunk</th>
                            <th>W_Score</th>
                            <th>ROI_Score</th>
                            <th>PNL_Score</th>
                            <th>Risk_Score</th>
                        </tr>
                    </thead>
                    <tbody>
            """
            for trader in traders_list[:limit]:
                wallet_short = trader.get('wallet_address', 'N/A')[:8] + '...' + trader.get('wallet_address', '')[-6:] if trader.get('wallet_address') else 'N/A'
                table_html += f"""
                        <tr>
                            <td class="rank">#{trader.get('rank', 0)}</td>
                            <td>{wallet_short}</td>
                            <td class="{'positive' if trader.get('total_pnl', 0) >= 0 else 'negative'}">${trader.get('total_pnl', 0):.2f}</td>
                            <td class="{'positive' if trader.get('roi', 0) >= 0 else 'negative'}">{trader.get('roi', 0):.2f}%</td>
                            <td>{trader.get('win_rate', 0):.2f}%</td>
                            <td>{trader.get('total_trades', 0)}</td>
                            <td>{trader.get('W_shrunk', 0):.6f}</td>
                            <td>{trader.get('roi_shrunk', 0):.6f}</td>
                            <td>{trader.get('pnl_shrunk', 0):.6f}</td>
                            <td>{trader.get('score_win_rate', 0):.4f}</td>
                            <td>{trader.get('score_roi', 0):.4f}</td>
                            <td>{trader.get('score_pnl', 0):.4f}</td>
                            <td>{trader.get('score_risk', 0):.4f}</td>
                        </tr>
                """
            table_html += """
                    </tbody>
                </table>
            </div>
            """
            return table_html
        
        # Add all leaderboards
        html_content += """
            <div class="section">
                <h2>🏆 All Leaderboards</h2>
        """
        
        # 1. W_shrunk leaderboard
        w_shrunk_sorted = sorted(traders, key=lambda x: x.get('W_shrunk', float('inf')))
        for i, trader in enumerate(w_shrunk_sorted, 1):
            trader['rank'] = i
        html_content += format_leaderboard_table(w_shrunk_sorted, "1. W_shrunk Leaderboard (Ascending - Best = Rank 1)")
        
        # 2. ROI raw leaderboard
        roi_raw_sorted = sorted(traders, key=lambda x: x.get('roi', float('-inf')), reverse=True)
        for i, trader in enumerate(roi_raw_sorted, 1):
            trader['rank'] = i
        html_content += format_leaderboard_table(roi_raw_sorted, "2. ROI Raw Leaderboard (Descending - Best = Rank 1)")
        
        # 3. ROI shrunk leaderboard
        roi_shrunk_sorted = sorted(traders, key=lambda x: x.get('roi_shrunk', float('inf')))
        for i, trader in enumerate(roi_shrunk_sorted, 1):
            trader['rank'] = i
        html_content += format_leaderboard_table(roi_shrunk_sorted, "3. ROI_shrunk Leaderboard (Ascending - Best = Rank 1)")
        
        # 4. PNL shrunk leaderboard
        pnl_shrunk_sorted = sorted(traders, key=lambda x: x.get('pnl_shrunk', float('inf')))
        for i, trader in enumerate(pnl_shrunk_sorted, 1):
            trader['rank'] = i
        html_content += format_leaderboard_table(pnl_shrunk_sorted, "4. PNL_shrunk Leaderboard (Ascending - Best = Rank 1)")
        
        # 5. Final Score leaderboards
        win_rate_sorted = sorted(traders, key=lambda x: x.get('score_win_rate', 0), reverse=True)
        for i, trader in enumerate(win_rate_sorted, 1):
            trader['rank'] = i
        html_content += format_leaderboard_table(win_rate_sorted, "5. Win Rate Score Leaderboard (Descending - Best = Rank 1)")
        
        roi_score_sorted = sorted(traders, key=lambda x: x.get('score_roi', 0), reverse=True)
        for i, trader in enumerate(roi_score_sorted, 1):
            trader['rank'] = i
        html_content += format_leaderboard_table(roi_score_sorted, "6. ROI Score Leaderboard (Descending - Best = Rank 1)")
        
        pnl_score_sorted = sorted(traders, key=lambda x: x.get('score_pnl', 0), reverse=True)
        for i, trader in enumerate(pnl_score_sorted, 1):
            trader['rank'] = i
        html_content += format_leaderboard_table(pnl_score_sorted, "7. PNL Score Leaderboard (Descending - Best = Rank 1)")
        
        risk_sorted = sorted(traders, key=lambda x: x.get('score_risk', 0), reverse=True)
        for i, trader in enumerate(risk_sorted, 1):
            trader['rank'] = i
        html_content += format_leaderboard_table(risk_sorted, "8. Risk Score Leaderboard (Descending - Best = Rank 1)")
        
        html_content += """
            </div>
            </body>
        </html>
        """
        
        return HTMLResponse(content=html_content)
    except Exception as e:
        return HTMLResponse(content=f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Error</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; background: #1a1a1a; color: #fff; }}
                .error {{ color: #ef4444; }}
            </style>
        </head>
        <body>
            <h1>Error Loading Leaderboards</h1>
            <p class="error">{str(e)}</p>
        </body>
        </html>
        """)
