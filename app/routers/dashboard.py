from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Dict, Any

from app.db.session import get_db
from app.services.dashboard_service import get_db_dashboard_data
from app.services.sync_service import sync_trader_full_data

router = APIRouter(
    prefix="/dashboard",
    tags=["Dashboard"]
)

@router.post("/sync/{wallet_address}", response_model=Dict[str, Any])
async def sync_dashboard_data(
    wallet_address: str,
    background_tasks: BackgroundTasks
):
    """
    Fetch everything from Polymarket for a specific wallet and store it in the DB.
    Done in background to avoid blocking.
    """
    if not wallet_address.startswith("0x") or len(wallet_address) != 42:
        raise HTTPException(status_code=400, detail="Invalid wallet address")
        
    # Trigger sync in background
    # Note: sync_trader_full_data handles its own session if none passed
    background_tasks.add_task(sync_trader_full_data, wallet_address)
    
    return {"message": "Sync initiated in background", "wallet_address": wallet_address}

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
