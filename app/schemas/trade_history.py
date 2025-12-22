"""Trade history schemas."""

from pydantic import BaseModel, Field
from typing import Optional, List, Dict
from decimal import Decimal


class TradeHistoryTrade(BaseModel):
    """Trade with PnL and ROI information."""
    id: int
    proxy_wallet: str
    side: str
    asset: str
    condition_id: str
    size: float
    price: float
    entry_price: Optional[float] = None
    exit_price: Optional[float] = None
    pnl: Optional[float] = None
    roi: Optional[float] = None
    timestamp: int
    title: Optional[str] = None
    slug: Optional[str] = None
    icon: Optional[str] = None
    event_slug: Optional[str] = None
    outcome: Optional[str] = None
    outcome_index: Optional[int] = None
    transaction_hash: str
    category: str = "Uncategorized"

    class Config:
        json_encoders = {
            Decimal: str
        }


class TradeHistoryOpenPosition(BaseModel):
    """Open position with current PnL and ROI."""
    id: int
    proxy_wallet: str
    asset: str
    condition_id: str
    size: float
    avg_price: float
    initial_value: float
    current_value: float
    cash_pnl: float
    percent_pnl: float
    cur_price: float
    roi: Optional[float] = None
    title: Optional[str] = None
    slug: Optional[str] = None
    icon: Optional[str] = None
    outcome: Optional[str] = None
    category: str = "Uncategorized"

    class Config:
        json_encoders = {
            Decimal: str
        }


class TradeHistoryClosedPosition(BaseModel):
    """Closed position with realized PnL and ROI."""
    id: int
    proxy_wallet: str
    asset: str
    condition_id: str
    avg_price: float
    cur_price: float
    realized_pnl: float
    roi: Optional[float] = None
    title: Optional[str] = None
    slug: Optional[str] = None
    icon: Optional[str] = None
    outcome: Optional[str] = None
    timestamp: int
    category: str = "Uncategorized"

    class Config:
        json_encoders = {
            Decimal: str
        }


class OverallMetrics(BaseModel):
    """Overall performance metrics."""
    total_pnl: float = Field(..., description="Total PnL (realized + unrealized)")
    realized_pnl: float = Field(..., description="Realized PnL from closed positions")
    unrealized_pnl: float = Field(..., description="Unrealized PnL from open positions")
    roi: float = Field(..., description="Return on Investment percentage")
    win_rate: float = Field(..., description="Win rate percentage")
    winning_trades: int = Field(..., description="Number of winning trades")
    losing_trades: int = Field(..., description="Number of losing trades")
    total_trades: int = Field(..., description="Total number of trades")
    total_trades_with_pnl: Optional[int] = Field(None, description="Number of trades with calculated PnL")
    score: float = Field(..., description="Overall performance score (0-100)")
    total_volume: float = Field(..., description="Total trading volume")
    pnl_adj: Optional[float] = Field(None, description="Whale-adjusted PnL")
    pnl_shrunk: Optional[float] = Field(None, description="Shrunk PnL using population median")
    n_eff: Optional[float] = Field(None, description="Effective number of trades")
    pnl_median_used: Optional[float] = Field(None, description="PnL median used in shrinkage calculation")


class CategoryMetrics(BaseModel):
    """Category-specific metrics."""
    roi: float = Field(..., description="ROI for this category")
    pnl: float = Field(..., description="Total PnL for this category")
    realized_pnl: float = Field(..., description="Realized PnL for this category")
    unrealized_pnl: float = Field(..., description="Unrealized PnL for this category")
    win_rate: float = Field(..., description="Win rate for this category")
    winning_trades: int = Field(..., description="Winning trades in this category")
    losing_trades: int = Field(..., description="Losing trades in this category")
    total_trades: int = Field(..., description="Total trades in this category")
    score: float = Field(..., description="Score for this category")
    total_volume: float = Field(..., description="Total volume for this category")


class TradeHistoryResponse(BaseModel):
    """Complete trade history response."""
    wallet_address: str = Field(..., description="Wallet address")
    open_positions: List[TradeHistoryOpenPosition] = Field(default_factory=list, description="List of open positions")
    closed_positions: List[TradeHistoryClosedPosition] = Field(default_factory=list, description="List of closed positions")
    trades: List[TradeHistoryTrade] = Field(default_factory=list, description="List of all trades")
    overall_metrics: OverallMetrics = Field(..., description="Overall performance metrics")
    category_breakdown: Dict[str, CategoryMetrics] = Field(default_factory=dict, description="Metrics broken down by category")

    class Config:
        json_schema_extra = {
            "example": {
                "wallet_address": "0xdbade4c82fb72780a0db9a38f821d8671aba9c95",
                "open_positions": [],
                "closed_positions": [],
                "trades": [],
                "overall_metrics": {
                    "total_pnl": 1000.0,
                    "realized_pnl": 800.0,
                    "unrealized_pnl": 200.0,
                    "roi": 15.5,
                    "win_rate": 65.0,
                    "winning_trades": 13,
                    "losing_trades": 7,
                    "total_trades": 20,
                    "score": 72.5,
                    "total_volume": 10000.0
                },
                "category_breakdown": {
                    "Sports": {
                        "roi": 18.2,
                        "pnl": 600.0,
                        "realized_pnl": 500.0,
                        "unrealized_pnl": 100.0,
                        "win_rate": 70.0,
                        "winning_trades": 7,
                        "losing_trades": 3,
                        "total_trades": 10,
                        "score": 75.0,
                        "total_volume": 5000.0
                    }
                }
            }
        }

