from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Dict, Any

from app.db.session import get_db
from app.services.dashboard_service import get_db_dashboard_data, get_profile_stat_data, search_user_by_name, dashboard_service
from app.services.dashboard_service_trades import get_filtered_trades
from app.services.sync_service import sync_trader_full_data

router = APIRouter(
    prefix="/dashboard",
    tags=["Dashboard"]
)

@router.get("/stats")
async def get_dashboard_stats():
    """
    Returns real-time scraped stats from Polydata.
    """
    return await dashboard_service.get_stats()

@router.post("/sync/{wallet_address}", response_model=Dict[str, Any])
async def sync_dashboard_data(
    wallet_address: str,
    background_tasks: BackgroundTasks,
    background: bool = True
):
    """
    Fetch everything from Polymarket for a specific wallet and store it in the DB.
    """
    if not wallet_address.startswith("0x") or len(wallet_address) != 42:
        raise HTTPException(status_code=400, detail="Invalid wallet address")
        
    if background:
        # Trigger sync in background
        background_tasks.add_task(sync_trader_full_data, wallet_address)
        return {"message": "Sync initiated in background", "wallet_address": wallet_address}
    else:
        # Run synchronously and return stats
        try:
            stats = await sync_trader_full_data(wallet_address)
            return {"message": "Sync completed", "wallet_address": wallet_address, "stats": stats}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Sync failed: {str(e)}")

@router.get("/db/{wallet_address}", response_model=Dict[str, Any])
async def get_dashboard_db(
    wallet_address: str,
    session: AsyncSession = Depends(get_db)
):
    """
    Get comprehensive dashboard data for a wallet directly from the local database.
    Does NOT call external APIs (Poly/Gamma).
    """
    if not wallet_address.startswith("0x") or len(wallet_address) != 42:
        raise HTTPException(status_code=400, detail="Invalid wallet address")
        
    try:
        data = await get_db_dashboard_data(session, wallet_address)
        return data
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/profile-stat/{wallet_address}", response_model=Dict[str, Any])
async def get_profile_stat(
    wallet_address: str,
    skip_trades: bool = False
):
    """
    Get comprehensive profile statistics for a wallet by fetching directly from Polymarket APIs.
    Bypasses the local database.
    
    Args:
        wallet_address: Wallet address
        skip_trades: Skip fetching trade history for faster initial load
    """
    if not wallet_address.startswith("0x") or len(wallet_address) != 42:
        raise HTTPException(status_code=400, detail="Invalid wallet address")
        
    try:
        data = await get_profile_stat_data(wallet_address, skip_trades=skip_trades)
        return data
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/profile-stat/{wallet_address}/trades", response_model=Dict[str, Any])
async def get_profile_stat_trades(
    wallet_address: str,
    filter: str = "all"
):
    """
    Get filtered trade history for a wallet.
    
    Args:
        wallet_address: Wallet address
        filter: Filter type - "recent10", "7days", "30days", "1year", "all"
    """
    if not wallet_address.startswith("0x") or len(wallet_address) != 42:
        raise HTTPException(status_code=400, detail="Invalid wallet address")
    
    valid_filters = ["recent10", "7days", "30days", "1year", "all"]
    if filter not in valid_filters:
        raise HTTPException(status_code=400, detail=f"Invalid filter. Must be one of: {', '.join(valid_filters)}")
        
    try:
        data = await get_filtered_trades(wallet_address, filter)
        return data
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/search/{query}", response_model=Dict[str, Any])
async def search_wallet_or_user(
    query: str,
    session: AsyncSession = Depends(get_db)
):
    """
    Search for a wallet address or username.
    - If query is 42-char hex, assumes wallet address.
    - Else, assumes username/pseudonym and looks up local DB.
    """
    # 1. Check if valid wallet address
    if query.startswith("0x") and len(query) == 42:
        return {
            "wallet_address": query,
            "type": "address",
            "name": None,
            "pseudonym": None
        }
    
    # 2. Lookup username in DB
    try:
        user_info = await search_user_by_name(session, query)
        if user_info:
            return {
                "wallet_address": user_info["wallet_address"],
                "type": "username",
                "name": user_info["name"],
                "pseudonym": user_info["pseudonym"],
                "profile_image": user_info["profile_image"]
            }
        
        # Not found
        raise HTTPException(status_code=404, detail=f"User '{query}' not found")
        
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
