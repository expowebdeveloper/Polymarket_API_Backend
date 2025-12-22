"""Trade History API routes."""

from fastapi import APIRouter, HTTPException, Query, status, Depends
from app.schemas.trade_history import TradeHistoryResponse
from app.schemas.general import ErrorResponse
from app.services.trade_history_service import get_trade_history
from app.db.session import get_db
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter(prefix="/trade-history", tags=["Trade History"])


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
    response_model=TradeHistoryResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid wallet address"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Get comprehensive trade history",
    description="Get trade history including open/closed positions, PnL, ROI, Win Rate, Score, and category breakdowns"
)
async def get_trade_history_endpoint(
    user: str = Query(
        ...,
        description="Wallet address to get trade history for (must be 42 characters starting with 0x)",
        example="0xdbade4c82fb72780a0db9a38f821d8671aba9c95",
        min_length=42,
        max_length=42
    ),
    db: AsyncSession = Depends(get_db)
):
    """
    Get comprehensive trade history for a wallet address.
    
    This endpoint returns:
    - Open positions with current PnL and ROI
    - Closed positions with realized PnL and ROI
    - All trades with PnL and ROI per trade
    - Overall metrics: ROI, PnL, Win Rate, and Score
    - Category breakdown: ROI, PnL, Win Rate, and Score by category
    
    Args:
        user: Wallet address (query parameter)
        db: Database session (injected)
    
    Returns:
        TradeHistoryResponse with complete trade history data
    """
    if not validate_wallet(user):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid wallet address format: {user}. Must be 42 characters starting with 0x"
        )
    
    try:
        trade_history = await get_trade_history(db, user)
        return TradeHistoryResponse(**trade_history)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving trade history: {str(e)}"
        )

