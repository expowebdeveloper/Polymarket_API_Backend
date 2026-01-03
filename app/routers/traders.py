"""Traders API routes."""

from fastapi import APIRouter, HTTPException, Query, Path, status, Depends
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from app.schemas.traders import (
    TraderBasicInfo,
    TraderDetail,
    TradersListResponse,
    TraderTradesResponse,
    LeaderboardTrader,
    LeaderboardTradersResponse
)
from app.schemas.leaderboard import (
    AllLeaderboardsResponse,
    LeaderboardEntry,
    PercentileInfo,
    MedianInfo
)
from app.schemas.polymarket_profile import (
    PolymarketTraderProfile,
    Badge,
    TradeHistoryEntry,
    RecentTradeSentiment,
    SentimentDataPoint
)
from app.schemas.markets import PaginationInfo
from app.schemas.general import ErrorResponse
from app.db.session import get_db
from app.services.trader_service import (
    get_trader_basic_info,
    get_trader_detail,
    get_traders_list as fetch_traders_list
)
from app.services.data_fetcher import fetch_resolved_markets, fetch_trades_for_wallet, fetch_traders_from_leaderboard

router = APIRouter(prefix="/traders", tags=["Traders"])


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


from fastapi import APIRouter, HTTPException, Query, Path, status, BackgroundTasks

@router.post(
    "/sync",
    summary="Sync traders to database",
    description="Fetch traders from Leaderboard API and save/update them in the database. Done in background. Set limit=0 to sync all available traders."
)
async def sync_traders(
    background_tasks: BackgroundTasks,
    limit: int = Query(50, ge=0, description="Number of traders to sync (use 0 for all)")
):
    """Trigger synchronization of traders from API to Database."""
    from app.services.trader_service import sync_traders_to_db
    
    # Trigger sync in background
    background_tasks.add_task(sync_traders_to_db, limit)
    
    return {"message": f"Sync started in background for {'all' if limit == 0 else limit} traders", "limit": limit}


@router.get(
    "/analytics",
    response_model=AllLeaderboardsResponse,
    summary="Get leaderboards analytics from Polymarket API",
    description="Get aggregated analytics and leaderboards directly from Polymarket API with pagination support. Uses same logic as /leaderboard/view-all endpoint."
)
async def get_db_analytics(
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of traders per leaderboard to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination")
):
    """
    Get full leaderboard analytics directly from Polymarket API.
    Fetches data from Polymarket API, calculates scores, and returns all leaderboards.
    Uses the exact same logic as /leaderboard/view-all endpoint.
    """
    try:
        from app.services.live_leaderboard_service import fetch_raw_metrics_for_scoring
        from app.services.leaderboard_service import calculate_scores_and_rank_with_percentiles
        from app.services.pnl_median_service import get_pnl_median_from_population
        from app.schemas.leaderboard import AllLeaderboardsResponse, PercentileInfo, MedianInfo
        
        file_path = "wallet_address.txt"
        # Fetch raw metrics from Polymarket API (same as view-all endpoint)
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
        
        # Create all different leaderboards (same as view-all endpoint)
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
        
        # Apply limit and offset to each list
        for key in list(leaderboards.keys()):
            leaderboards[key] = leaderboards[key][offset : offset + limit]
        
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
            detail=f"Error generating analytics from Polymarket API: {str(e)}"
        )


@router.post(
    "/analytics/clear-cache",
    summary="Clear analytics cache",
    description="Clear the cached analytics data to force recalculation on next request"
)
async def clear_analytics_cache():
    """Clear the analytics cache."""
    try:
        from app.services.analytics_cache import analytics_cache
        await analytics_cache.clear()
        return {"message": "Analytics cache cleared successfully"}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error clearing cache: {str(e)}"
        )


@router.post(
    "/analytics/recalculate",
    summary="Recalculate and store leaderboard entries from DB",
    description="Calculate leaderboard metrics from traders already in the database and store them. This runs in the background."
)
async def recalculate_leaderboard(
    background_tasks: BackgroundTasks,
    max_traders: Optional[int] = Query(None, ge=50, le=10000, description="Maximum number of traders to process (None = all)")
):
    """
    Manually trigger recalculation of leaderboard entries from database traders.
    This will calculate metrics for all traders in DB and store them in the leaderboard_entries table.
    """
    from app.services.leaderboard_storage_service import calculate_and_store_leaderboard_entries
    from app.db.session import AsyncSessionLocal
    
    async def recalculate_task():
        async with AsyncSessionLocal() as session:
            return await calculate_and_store_leaderboard_entries(
                session,
                wallet_addresses=None,
                max_traders=max_traders
            )
    
    background_tasks.add_task(recalculate_task)
    
    return {
        "message": "Leaderboard recalculation from DB started in background",
        "max_traders": max_traders if max_traders else "all"
    }


@router.post(
    "/analytics/sync-from-polymarket",
    summary="Sync traders from Polymarket and calculate leaderboard",
    description="Fetch all traders from Polymarket Leaderboard API, sync their data to DB, then calculate and store leaderboard metrics. This runs in the background."
)
async def sync_from_polymarket(
    background_tasks: BackgroundTasks,
    limit: Optional[int] = Query(None, ge=0, description="Number of traders to fetch from Polymarket (None or 0 = all available)")
):
    """
    Sync traders from Polymarket Leaderboard API, then calculate and store leaderboard entries.
    
    This endpoint:
    1. Fetches traders from Polymarket Leaderboard API
    2. Syncs their full data to the database
    3. Calculates leaderboard metrics and stores them
    
    All steps run in the background.
    """
    from app.services.trader_service import sync_traders_to_db
    from app.services.leaderboard_storage_service import calculate_and_store_leaderboard_entries
    from app.db.session import AsyncSessionLocal
    
    async def sync_and_calculate_task():
        # Step 1: Sync traders from Polymarket
        sync_stats = await sync_traders_to_db(limit=limit if limit else 0)
        
        # Step 2: Calculate and store leaderboard entries
        async with AsyncSessionLocal() as session:
            calc_stats = await calculate_and_store_leaderboard_entries(
                session,
                wallet_addresses=None,
                max_traders=None
            )
        
        return {
            "sync_stats": sync_stats,
            "calculation_stats": calc_stats
        }
    
    background_tasks.add_task(sync_and_calculate_task)
    
    return {
        "message": "Sync from Polymarket and leaderboard calculation started in background",
        "limit": limit if limit else "all"
    }


@router.get(
    "/analytics/db-status",
    summary="Get database status",
    description="Check if leaderboard data exists in the database"
)
async def get_db_status(db: AsyncSession = Depends(get_db)):
    """Check the status of leaderboard data in the database."""
    from app.services.leaderboard_storage_service import (
        get_total_leaderboard_count,
        get_leaderboard_metadata
    )
    from sqlalchemy.future import select
    from sqlalchemy import func
    from app.db.models import Trader
    
    try:
        # Check leaderboard entries
        total_entries = await get_total_leaderboard_count(db)
        metadata = await get_leaderboard_metadata(db)
        
        # Check total traders in DB
        stmt = select(func.count(Trader.id))
        result = await db.execute(stmt)
        total_traders = result.scalar() or 0
        
        return {
            "leaderboard_entries_count": total_entries,
            "has_metadata": metadata is not None,
            "total_traders_in_db": total_traders,
            "is_populated": total_entries > 0 and metadata is not None,
            "message": "Database is populated" if (total_entries > 0 and metadata) else "Database is empty - use /traders/analytics/sync-from-polymarket to populate"
        }
    except Exception as e:
        try:
            await db.rollback()
        except Exception:
            pass
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error checking database status: {str(e)}"
        )


@router.get(
    "/analytics/status",
    summary="Get leaderboard recalculation status",
    description="Get information about the last leaderboard recalculation"
)
async def get_leaderboard_status():
    """Get status of leaderboard recalculation."""
    try:
        from app.services.leaderboard_scheduler import get_last_run_time, is_scheduler_running
        from app.db.session import AsyncSessionLocal
        from app.services.leaderboard_storage_service import get_total_leaderboard_count
        
        async with AsyncSessionLocal() as session:
            total_entries = await get_total_leaderboard_count(session)
        
        return {
            "scheduler_running": is_scheduler_running(),
            "last_run_time": get_last_run_time().isoformat() if get_last_run_time() else None,
            "total_entries_in_db": total_entries
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting status: {str(e)}"
        )


@router.get(
    "",
    response_model=TradersListResponse,
    summary="Get list of traders",
    description="Get a list of traders from the database with basic information"
)
async def get_traders(
    limit: int = Query(50, ge=1, le=100, description="Maximum number of traders to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination")
):
    """
    Get a list of traders from the database.
    """
    from app.services.trader_service import get_traders_from_db
    try:
        traders = await get_traders_from_db(limit=limit, offset=offset)
        return TradersListResponse(
            count=len(traders),
            traders=[TraderBasicInfo(**trader) for trader in traders]
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching traders list: {str(e)}"
        )


@router.get(
    "/leaderboard",
    response_model=LeaderboardTradersResponse,
    summary="Get traders from Polymarket Leaderboard",
    description="Fetch traders from Polymarket Leaderboard API with ranking and stats"
)
async def get_leaderboard_traders(
    category: str = Query("overall", description="Category filter: 'overall', 'politics', 'sports', etc."),
    time_period: str = Query("all", description="Time period: 'all', '1m', '3m', '6m', '1y'"),
    order_by: str = Query("VOL", description="Sort by: 'VOL', 'PNL', 'ROI'"),
    limit: Optional[int] = Query(None, ge=0, description="Maximum number of traders to return. If not specified or 0, fetches ALL available traders"),
    offset: int = Query(0, ge=0, description="Offset for pagination (only used when limit is specified)")
):
    """
    Get traders from Polymarket Leaderboard API.
    
    This endpoint fetches traders directly from Polymarket's leaderboard API,
    which includes ranking, volume, PnL, ROI, and other performance metrics.
    
    If limit is not specified or is 0, it will fetch ALL available traders by automatically paginating.
    """
    try:
        all_traders = []
        current_offset = offset
        fetch_batch_size = 100  # Fetch in batches of 100
        fetch_all = limit is None or limit == 0
        
        if fetch_all:
            # Fetch all traders by paginating through all available pages
            print(f"📡 Fetching ALL traders from Polymarket Leaderboard API...")
            while True:
                traders_batch, pagination_dict = await fetch_traders_from_leaderboard(
                    category=category,
                    time_period=time_period,
                    order_by=order_by,
                    limit=fetch_batch_size,
                    offset=current_offset
                )
                
                if not traders_batch:
                    break
                
                all_traders.extend(traders_batch)
                
                # Check if there are more traders to fetch
                if not pagination_dict.get("has_more", False):
                    break
                
                current_offset += len(traders_batch)
                print(f"   Fetched {len(all_traders)} traders so far...")
            
            print(f"✅ Successfully fetched {len(all_traders)} total traders from Polymarket Leaderboard API")
            
            return LeaderboardTradersResponse(
                count=len(all_traders),
                traders=[LeaderboardTrader(**trader) for trader in all_traders],
                pagination=PaginationInfo(
                    limit=len(all_traders),
                    offset=0,
                    total=len(all_traders),
                    has_more=False
                )
            )
        else:
            # Fetch only the requested limit
            traders, pagination_dict = await fetch_traders_from_leaderboard(
                category=category,
                time_period=time_period,
                order_by=order_by,
                limit=limit,
                offset=offset
            )
            
            pagination_info = None
            if pagination_dict:
                pagination_info = PaginationInfo(
                    limit=pagination_dict["limit"],
                    offset=pagination_dict["offset"],
                    total=pagination_dict["total"],
                    has_more=pagination_dict["has_more"]
                )
            
            return LeaderboardTradersResponse(
                count=len(traders),
                traders=[LeaderboardTrader(**trader) for trader in traders],
                pagination=pagination_info
            )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching traders from leaderboard: {str(e)}"
        )


@router.get(
    "/{wallet}",
    response_model=TraderDetail,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid wallet address"},
        404: {"model": ErrorResponse, "description": "Trader not found"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Get trader details",
    description="Get detailed information and analytics for a specific trader"
)
async def get_trader(
    wallet: str = Path(..., description="Wallet address of the trader")
):
    """
    Get detailed trader information including full analytics.
    
    Returns:
    - Basic trader info (wallet, trades, positions)
    - Performance metrics (wins, losses, PnL, win rate)
    - Final score
    - Category breakdown
    - Trade dates
    """
    if not validate_wallet(wallet):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid wallet address format: {wallet}. Must be 42 characters starting with 0x"
        )
    
    try:
        trader_data = await get_trader_detail(wallet)
        
        # Check if trader has any data
        if trader_data.get("total_trades", 0) == 0:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No trades found for trader {wallet}"
            )
        
        # Validate and return the data
        try:
            return TraderDetail(**trader_data)
        except Exception as validation_error:
            # If validation fails, log the error and return a more helpful message
            print(f"⚠ Validation error for trader {wallet}: {validation_error}")
            print(f"  Trader data keys: {list(trader_data.keys())}")
            print(f"  Trader data: {trader_data}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Data validation error: {str(validation_error)}. Trader data may be incomplete."
            )
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        print(f"✗ Error fetching trader data for {wallet}: {e}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching trader data: {str(e)}"
        )


@router.get(
    "/{wallet}/basic",
    response_model=TraderBasicInfo,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid wallet address"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Get trader basic info",
    description="Get basic information about a trader (faster than full details)"
)
async def get_trader_basic(
    wallet: str = Path(..., description="Wallet address of the trader")
):
    """
    Get basic trader information without full analytics calculation.
    Faster than the full details endpoint.
    """
    if not validate_wallet(wallet):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid wallet address format: {wallet}. Must be 42 characters starting with 0x"
        )
    
    try:
        markets = fetch_resolved_markets()
        trader_data = await get_trader_basic_info(wallet, markets)
        return TraderBasicInfo(**trader_data)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching trader basic info: {str(e)}"
        )


@router.get(
    "/{wallet}/trades",
    response_model=TraderTradesResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid wallet address"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Get trader trades",
    description="Get all trades for a specific trader"
)
async def get_trader_trades(
    wallet: str = Path(..., description="Wallet address of the trader"),
    limit: Optional[int] = Query(None, ge=1, le=1000, description="Maximum number of trades to return")
):
    """
    Get all trades for a specific trader.
    
    Returns raw trade data for the specified wallet address.
    """
    if not validate_wallet(wallet):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid wallet address format: {wallet}. Must be 42 characters starting with 0x"
        )
    
    try:
        trades = fetch_trades_for_wallet(wallet)
        
        if limit:
            trades = trades[:limit]
        
        return TraderTradesResponse(
            wallet_address=wallet,
            count=len(trades),
            trades=trades
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching trader trades: {str(e)}"
        )


@router.get(
    "/{wallet}/polymarket-profile",
    response_model=PolymarketTraderProfile,
    summary="Get Polymarket-style trader profile",
    description="Get comprehensive trader profile matching Polymarket's exact UI format with badges, streaks, rewards, and trade history"
)
async def get_polymarket_trader_profile(
    wallet: str = Path(..., description="Wallet address of the trader"),
    db: AsyncSession = Depends(get_db)
):
    """
    Get comprehensive trader profile in Polymarket's exact format.
    
    Returns:
    - Final Score with Top % and badges (Top 10, Whale, Hot Streak)
    - KPIs: ROI %, Win Rate, Total Volume, Total Trades
    - Streaks: Longest and Current
    - Total Wins/Losses
    - Reward Earned
    - Trade History with MARKET, OUTCOME, PRICE, PNL, DATE format
    - Recent Trade Sentiment (last 7 days graph data)
    - Trade Confidence
    """
    if not validate_wallet(wallet):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid wallet address format: {wallet}. Must be 42 characters starting with 0x"
        )
    
    try:
        from app.services.trade_history_service import get_trade_history
        from app.services.profile_stats_service import get_enhanced_profile_stats, calculate_winning_streaks
        from app.services.activity_service import get_activities_from_db
        from app.services.pnl_calculator_service import calculate_user_pnl
        from app.services.data_fetcher import get_market_by_id
        from datetime import datetime, timedelta
        from decimal import Decimal
        
        # 1. Get enhanced profile stats (includes final score, streaks, etc.)
        enhanced_stats = await get_enhanced_profile_stats(db, wallet)
        if not enhanced_stats:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Trader profile not found for wallet: {wallet}"
            )
        
        # 2. Get trade history
        trade_history_data = await get_trade_history(db, wallet)
        
        # 3. Get activities for reward calculation
        activities = await get_activities_from_db(db, wallet, activity_type="REWARD")
        
        # 4. Calculate reward earned
        reward_earned = sum(float(activity.usdc_size) for activity in activities if activity.type == "REWARD")
        
        # 5. Get PnL data
        pnl_data = await calculate_user_pnl(db, wallet)
        
        # 6. Calculate badges
        badges = []
        final_score = enhanced_stats.get("final_score", 0.0)
        total_volume = enhanced_stats.get("total_volume", 0.0) or pnl_data.get("total_volume", 0.0)
        
        if final_score >= 90:
            badges.append(Badge(label="Top 10", color="yellow"))
        if total_volume >= 100000:  # $100K+ volume = Whale
            badges.append(Badge(label="Whale", color="blue"))
        
        # Check for hot streak (recent wins)
        recent_trades = trade_history_data.get("trades", [])
        recent_wins = sum(1 for t in recent_trades[-10:] if t.get("pnl", 0) > 0)
        recent_losses = sum(1 for t in recent_trades[-10:] if t.get("pnl", 0) < 0)
        if recent_wins >= 5 and recent_losses <= 2:
            badges.append(Badge(label="Hot Streak", color="purple"))
        
        # 7. Format trade history
        closed_positions = trade_history_data.get("closed_positions", [])
        trade_history_entries = []
        
        for position in closed_positions[:50]:  # Limit to 50 most recent
            market = position.get("title") or "Unknown Market"
            outcome = "YES" if position.get("outcome") == "YES" else "NO"
            price = f"${position.get('cur_price', 0):,.2f}" if position.get('cur_price') else None
            pnl_value = position.get("realized_pnl", 0.0)
            pnl_str = f"+${abs(pnl_value):,.0f}" if pnl_value >= 0 else f"-${abs(pnl_value):,.0f}"
            
            # Format date
            timestamp = position.get("timestamp", 0)
            if timestamp:
                trade_date = datetime.fromtimestamp(timestamp)
                now = datetime.now()
                diff = now - trade_date
                
                if diff.total_seconds() < 3600:  # Less than 1 hour
                    hours = int(diff.total_seconds() / 3600)
                    date_str = f"{hours} hour{'s' if hours != 1 else ''} ago"
                elif diff.days == 0:  # Today
                    hours = int(diff.total_seconds() / 3600)
                    date_str = f"{hours} hour{'s' if hours != 1 else ''} ago"
                elif diff.days == 1:
                    date_str = "1 day ago"
                elif diff.days < 7:
                    date_str = f"{diff.days} days ago"
                else:
                    date_str = trade_date.strftime("%b %d, %Y")
            else:
                date_str = "Unknown"
            
            trade_history_entries.append(TradeHistoryEntry(
                market=market,
                outcome=outcome,
                price=price,
                pnl=pnl_str,
                date=date_str,
                timestamp=timestamp
            ))
        
        # 8. Calculate recent trade sentiment (last 7 days)
        seven_days_ago = datetime.now() - timedelta(days=7)
        recent_trades_7d = [
            t for t in recent_trades
            if t.get("timestamp") and datetime.fromtimestamp(t["timestamp"]) >= seven_days_ago
        ]
        
        recent_wins_7d = sum(1 for t in recent_trades_7d if t.get("pnl", 0) > 0)
        recent_losses_7d = sum(1 for t in recent_trades_7d if t.get("pnl", 0) < 0)
        total_recent = len(recent_trades_7d)
        
        sentiment_change = ((recent_wins_7d - recent_losses_7d) / total_recent * 100) if total_recent > 0 else 0.0
        sentiment_change_str = f"+{sentiment_change:.1f}%" if sentiment_change >= 0 else f"{sentiment_change:.1f}%"
        
        trade_confidence = (recent_wins_7d / total_recent * 100) if total_recent > 0 else 0.0
        
        # Create sentiment data points (daily for last 7 days)
        sentiment_data_points = []
        for i in range(7):
            day = datetime.now() - timedelta(days=6-i)
            day_start = day.replace(hour=0, minute=0, second=0, microsecond=0)
            day_end = day.replace(hour=23, minute=59, second=59, microsecond=999999)
            
            day_trades = [
                t for t in recent_trades_7d
                if day_start.timestamp() <= t.get("timestamp", 0) <= day_end.timestamp()
            ]
            
            day_wins = sum(1 for t in day_trades if t.get("pnl", 0) > 0)
            day_losses = sum(1 for t in day_trades if t.get("pnl", 0) < 0)
            day_total = len(day_trades)
            
            day_sentiment = ((day_wins - day_losses) / day_total * 100) if day_total > 0 else 0.0
            
            sentiment_data_points.append(SentimentDataPoint(
                date=day.strftime("%b %d"),
                timestamp=int(day.timestamp()),
                value=day_sentiment
            ))
        
        recent_sentiment = RecentTradeSentiment(
            period="Last 7 days",
            change=sentiment_change_str,
            data_points=sentiment_data_points,
            trade_confidence=trade_confidence,
            wins=recent_wins_7d,
            losses=recent_losses_7d
        )
        
        # 9. Get overall metrics
        overall_metrics = trade_history_data.get("overall_metrics", {})
        roi = overall_metrics.get("roi", 0.0)
        win_rate = overall_metrics.get("win_rate", 0.0)
        total_trades_count = overall_metrics.get("total_trades", 0)
        winning_trades = overall_metrics.get("winning_trades", 0)
        losing_trades = overall_metrics.get("losing_trades", 0)
        total_volume_value = overall_metrics.get("total_volume", 0.0)
        
        # Count unique markets
        unique_markets = len(set(
            p.get("title") or p.get("slug") or "Unknown"
            for p in closed_positions
        ))
        
        # Format win rate detail
        win_rate_detail = f"{winning_trades} of {total_trades_count} trades"
        
        # Format volume detail
        volume_detail = f"Across {unique_markets} markets" if unique_markets > 0 else "No markets"
        
        # Format trades detail
        trades_detail = "Since joining"
        
        # 10. Build response
        return PolymarketTraderProfile(
            wallet_address=wallet,
            name=enhanced_stats.get("name"),
            pseudonym=enhanced_stats.get("pseudonym"),
            profile_image=enhanced_stats.get("profile_image"),
            final_score=final_score,
            top_percent=enhanced_stats.get("top_percent", 50.0),
            ranking_tag=enhanced_stats.get("ranking_tag", "Below 50%"),
            badges=badges,
            roi_percent=roi,
            win_rate=win_rate,
            win_rate_detail=win_rate_detail,
            total_volume=total_volume_value,
            total_volume_detail=volume_detail,
            total_trades=total_trades_count,
            total_trades_detail=trades_detail,
            longest_streak=enhanced_stats.get("longest_winning_streak", 0),
            current_streak=enhanced_stats.get("current_winning_streak", 0),
            total_wins=winning_trades,
            total_losses=losing_trades,
            reward_earned=reward_earned,
            trade_history=trade_history_entries,
            trade_history_total=len(closed_positions),
            recent_sentiment=recent_sentiment
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching Polymarket trader profile: {str(e)}"
        )

