"""PnL calculation-related schemas."""

from pydantic import BaseModel, Field
from typing import Dict


class KeyMetrics(BaseModel):
    """Key trading metrics calculated from trades."""
    total_trade_pnl: float = Field(..., description="Total PnL from all trades")
    roi: float = Field(..., description="Return on Investment as percentage")
    win_rate: float = Field(..., description="Win rate as percentage")
    stake_weighted_win_rate: float = Field(..., description="Stake-weighted win rate as percentage")
    winning_trades: int = Field(..., description="Number of winning trades")
    total_trades_with_pnl: int = Field(..., description="Total trades with calculated PnL")
    total_stakes: float = Field(..., description="Total stakes (sum of size * price for all trades)")


class PnLStatistics(BaseModel):
    """Statistics for PnL calculation."""
    total_trades: int = Field(..., description="Total number of trades")
    buy_trades: int = Field(..., description="Number of BUY trades")
    sell_trades: int = Field(..., description="Number of SELL trades")
    active_positions: int = Field(..., description="Number of active positions")
    closed_positions: int = Field(..., description="Number of closed positions")
    total_positions: int = Field(..., description="Total number of positions")
    avg_trade_size: float = Field(..., description="Average trade size")


class PnLBreakdown(BaseModel):
    """Breakdown of PnL sources."""
    from_positions: Dict[str, float] = Field(..., description="PnL from positions")
    from_activities: Dict[str, float] = Field(..., description="PnL from activities (rewards/redemptions)")


class PnLCalculationResponse(BaseModel):
    """Response model for PnL calculation."""
    wallet_address: str = Field(..., description="Wallet address")
    total_invested: float = Field(..., description="Total amount invested")
    total_current_value: float = Field(..., description="Total current value of positions")
    total_realized_pnl: float = Field(..., description="Total realized PnL")
    total_unrealized_pnl: float = Field(..., description="Total unrealized PnL")
    total_rewards: float = Field(..., description="Total rewards received")
    total_redemptions: float = Field(..., description="Total redemptions")
    total_pnl: float = Field(..., description="Total PnL (realized + unrealized + rewards - redemptions)")
    pnl_percentage: float = Field(..., description="PnL as percentage of total invested")
    key_metrics: KeyMetrics = Field(..., description="Key trading metrics calculated from trades")
    statistics: PnLStatistics = Field(..., description="Trading statistics")
    breakdown: PnLBreakdown = Field(..., description="PnL breakdown by source")

    class Config:
        json_schema_extra = {
            "example": {
                "wallet_address": "0x17db3fcd93ba12d38382a0cade24b200185c5f6d",
                "total_invested": 100000.0,
                "total_current_value": 105000.0,
                "total_realized_pnl": 2000.0,
                "total_unrealized_pnl": 3000.0,
                "total_rewards": 100.0,
                "total_redemptions": 0.0,
                "total_pnl": 5100.0,
                "pnl_percentage": 5.1,
                "statistics": {
                    "total_trades": 50,
                    "buy_trades": 30,
                    "sell_trades": 20,
                    "active_positions": 10,
                    "closed_positions": 5,
                    "total_positions": 15,
                    "avg_trade_size": 2000.0
                },
                "breakdown": {
                    "from_positions": {
                        "realized_pnl": 2000.0,
                        "unrealized_pnl": 3000.0
                    },
                    "from_activities": {
                        "rewards": 100.0,
                        "redemptions": 0.0
                    }
                }
            }
        }

