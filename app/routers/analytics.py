"""Analytics API routes."""

from fastapi import APIRouter, HTTPException, Query, status
from app.schemas.analytics import AnalyticsResponse
from app.schemas.general import ErrorResponse
from app.schemas.requests import WalletRequest
from app.services.data_fetcher import fetch_resolved_markets, fetch_trades_for_wallet
from app.services.scoring_engine import calculate_metrics

router = APIRouter(prefix="/analytics", tags=["Analytics"])


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
    response_model=AnalyticsResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid wallet address"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Get wallet analytics",
    description="Calculate and return comprehensive analytics for a specific wallet address"
)
async def get_analytics(
    wallet: str = Query(
        ...,
        description="Wallet address to analyze (must be 42 characters starting with 0x)",
        example="0x56687bf447db6ffa42ffe2204a05edaa20f55839",
        min_length=42,
        max_length=42
    )
):
    """
    Get analytics for a specific wallet address.
    
    This endpoint:
    1. Fetches all resolved markets from Polymarket
    2. Fetches all trades for the specified wallet
    3. Calculates comprehensive performance metrics
    4. Returns structured analytics including:
       - Total and active positions
       - Win/loss statistics
       - PnL calculations
       - Final performance score
       - Category breakdown
    """
    if not validate_wallet(wallet):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid wallet address format: {wallet}. Must be 42 characters starting with 0x"
        )
    
    try:
        markets = fetch_resolved_markets()
        trades = fetch_trades_for_wallet(wallet)
        metrics = calculate_metrics(wallet, trades, markets)
        return AnalyticsResponse(**metrics)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error calculating analytics: {str(e)}"
        )


@router.post(
    "",
    response_model=AnalyticsResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid wallet address"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Get wallet analytics (POST)",
    description="Calculate and return comprehensive analytics for a specific wallet address using POST method"
)
async def get_analytics_post(request: WalletRequest):
    """
    Get analytics for a specific wallet address using POST method.
    
    Same functionality as GET /analytics but accepts wallet in request body.
    """
    wallet = request.wallet
    
    try:
        markets = fetch_resolved_markets()
        trades = fetch_trades_for_wallet(wallet)
        metrics = calculate_metrics(wallet, trades, markets)
        return AnalyticsResponse(**metrics)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error calculating analytics: {str(e)}"
        )

