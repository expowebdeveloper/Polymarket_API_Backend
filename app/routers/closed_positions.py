from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.db.session import get_db
from app.services.closed_position_service import fetch_and_store_closed_positions
from app.schemas.closed_position import ClosedPosition as ClosedPositionSchema
from typing import List

router = APIRouter(
    prefix="/closed-positions",
    tags=["Closed Positions"],
    responses={404: {"description": "Not found"}},
)

@router.post("/{user_address}", response_model=List[ClosedPositionSchema])
async def trigger_fetch_and_store_closed_positions(user_address: str, db: AsyncSession = Depends(get_db)):
    """
    Fetch closed positions from Polymarket API and store them in the database.
    Returns the list of closed positions for the user.
    """
    try:
        positions = await fetch_and_store_closed_positions(user_address, db)
        return positions
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{user_address}", response_model=List[ClosedPositionSchema])
async def get_stored_closed_positions(user_address: str, db: AsyncSession = Depends(get_db)):
    """
    Retrieve stored closed positions from the database.
    """
    from app.db.models import ClosedPosition
    query = select(ClosedPosition).filter(ClosedPosition.proxy_wallet == user_address)
    result = await db.execute(query)
    positions = result.scalars().all()
    return positions
