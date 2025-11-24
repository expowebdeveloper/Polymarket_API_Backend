"""Analytics-related schemas."""

from pydantic import BaseModel, Field
from typing import Dict


class CategoryMetrics(BaseModel):
    """Schema for category-specific metrics."""
    total_wins: float = Field(..., description="Total wins for this category", example=12685747.18)
    total_losses: float = Field(..., description="Total losses for this category", example=-32326485.30)
    win_rate_percent: float = Field(..., description="Win rate percentage for this category", example=58.3)
    pnl: float = Field(..., description="Profit and loss for this category", example=-19640738.12)

    class Config:
        json_schema_extra = {
            "example": {
                "total_wins": 12685747.18,
                "total_losses": -32326485.30,
                "win_rate_percent": 58.3,
                "pnl": -19640738.12
            }
        }


class AnalyticsResponse(BaseModel):
    """Schema for wallet analytics response."""
    wallet_id: str = Field(..., description="Wallet address", example="0x56687bf447db6ffa42ffe2204a05edaa20f55839")
    total_positions: int = Field(..., description="Total number of unique markets traded", example=14)
    active_positions: int = Field(..., description="Number of unresolved markets", example=0)
    total_wins: float = Field(..., description="Sum of all winning trade profits", example=12685747.18)
    total_losses: float = Field(..., description="Sum of all losing trade losses (negative)", example=-32326485.30)
    win_rate_percent: float = Field(..., description="Win rate as percentage", example=58.3)
    win_count: int = Field(..., description="Number of winning trades", example=7)
    loss_count: int = Field(..., description="Number of losing trades", example=5)
    pnl: float = Field(..., description="Overall profit and loss", example=-19640738.12)
    current_value: float = Field(..., description="Current portfolio value", example=0.01)
    final_score: float = Field(..., description="Final performance score (0-100)", example=42.6)
    categories: Dict[str, CategoryMetrics] = Field(
        default_factory=dict,
        description="Category breakdown of metrics"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "wallet_id": "0x56687bf447db6ffa42ffe2204a05edaa20f55839",
                "total_positions": 14,
                "active_positions": 0,
                "total_wins": 12685747.18,
                "total_losses": -32326485.30,
                "win_rate_percent": 58.3,
                "win_count": 7,
                "loss_count": 5,
                "pnl": -19640738.12,
                "current_value": 0.01,
                "final_score": 42.6,
                "categories": {
                    "Sports": {
                        "total_wins": 12685747.18,
                        "total_losses": -32326485.30,
                        "win_rate_percent": 58.3,
                        "pnl": -19640738.12
                    }
                }
            }
        }

