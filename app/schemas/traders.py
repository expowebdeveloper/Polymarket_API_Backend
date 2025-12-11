"""Trader-related schemas."""

from pydantic import BaseModel, Field
from typing import Optional, List, Dict
from datetime import datetime


class TraderBasicInfo(BaseModel):
    """Basic trader information."""
    wallet_address: str = Field(..., description="Wallet address", example="0x56687bf447db6ffa42ffe2204a05edaa20f55839")
    total_trades: int = Field(..., description="Total number of trades", example=12)
    total_positions: int = Field(..., description="Total number of positions", example=5)
    first_trade_date: Optional[str] = Field(None, description="Date of first trade")
    last_trade_date: Optional[str] = Field(None, description="Date of last trade")

    class Config:
        json_schema_extra = {
            "example": {
                "wallet_address": "0x56687bf447db6ffa42ffe2204a05edaa20f55839",
                "total_trades": 12,
                "total_positions": 5,
                "first_trade_date": "2024-01-15T10:30:00Z",
                "last_trade_date": "2024-03-20T14:45:00Z"
            }
        }


class TraderDetail(BaseModel):
    """Detailed trader information with analytics."""
    wallet_address: str = Field(..., description="Wallet address")
    total_trades: int = Field(..., description="Total number of trades")
    total_positions: int = Field(..., description="Total number of positions")
    active_positions: int = Field(..., description="Number of active positions")
    total_wins: float = Field(..., description="Total wins")
    total_losses: float = Field(..., description="Total losses")
    win_rate_percent: float = Field(..., description="Win rate percentage")
    pnl: float = Field(..., description="Profit and loss")
    final_score: float = Field(..., description="Final performance score")
    first_trade_date: Optional[str] = Field(None, description="Date of first trade")
    last_trade_date: Optional[str] = Field(None, description="Date of last trade")
    categories: Dict[str, Dict] = Field(default_factory=dict, description="Category breakdown")

    class Config:
        json_schema_extra = {
            "example": {
                "wallet_address": "0x56687bf447db6ffa42ffe2204a05edaa20f55839",
                "total_trades": 12,
                "total_positions": 5,
                "active_positions": 0,
                "total_wins": 12685747.18,
                "total_losses": -32326485.30,
                "win_rate_percent": 58.3,
                "pnl": -19640738.12,
                "final_score": 42.6,
                "first_trade_date": "2024-01-15T10:30:00Z",
                "last_trade_date": "2024-03-20T14:45:00Z",
                "categories": {}
            }
        }


class TradersListResponse(BaseModel):
    """Response for list of traders."""
    count: int = Field(..., description="Number of traders returned", example=50)
    traders: List[TraderBasicInfo] = Field(..., description="List of traders")

    class Config:
        json_schema_extra = {
            "example": {
                "count": 50,
                "traders": []
            }
        }


class TraderTradesResponse(BaseModel):
    """Response for trader trades."""
    wallet_address: str = Field(..., description="Wallet address")
    count: int = Field(..., description="Number of trades returned")
    trades: List[Dict] = Field(..., description="List of trades")

    class Config:
        json_schema_extra = {
            "example": {
                "wallet_address": "0x56687bf447db6ffa42ffe2204a05edaa20f55839",
                "count": 12,
                "trades": []
            }
        }

