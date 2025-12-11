"""Position-related schemas."""

from pydantic import BaseModel, Field
from typing import Optional, List
from decimal import Decimal


class PositionResponse(BaseModel):
    """Position data from API response."""
    proxy_wallet: str = Field(..., description="Proxy wallet address")
    asset: str = Field(..., description="Asset ID")
    condition_id: str = Field(..., description="Condition ID")
    size: Decimal = Field(..., description="Position size")
    avg_price: Decimal = Field(..., description="Average price")
    initial_value: Decimal = Field(..., description="Initial value")
    current_value: Decimal = Field(..., description="Current value")
    cash_pnl: Decimal = Field(..., description="Cash PnL")
    percent_pnl: Decimal = Field(..., description="Percentage PnL")
    total_bought: Decimal = Field(..., description="Total bought")
    realized_pnl: Decimal = Field(..., description="Realized PnL")
    percent_realized_pnl: Decimal = Field(..., description="Percentage realized PnL")
    cur_price: Decimal = Field(..., description="Current price")
    redeemable: bool = Field(..., description="Whether position is redeemable")
    mergeable: bool = Field(..., description="Whether position is mergeable")
    title: Optional[str] = Field(None, description="Market title")
    slug: Optional[str] = Field(None, description="Market slug")
    icon: Optional[str] = Field(None, description="Market icon URL")
    event_id: Optional[str] = Field(None, description="Event ID")
    event_slug: Optional[str] = Field(None, description="Event slug")
    outcome: Optional[str] = Field(None, description="Outcome")
    outcome_index: Optional[int] = Field(None, description="Outcome index")
    opposite_outcome: Optional[str] = Field(None, description="Opposite outcome")
    opposite_asset: Optional[str] = Field(None, description="Opposite asset ID")
    end_date: Optional[str] = Field(None, description="End date")
    negative_risk: bool = Field(False, description="Negative risk flag")

    class Config:
        json_encoders = {
            Decimal: str
        }


class PositionsListResponse(BaseModel):
    """Response model for list of positions."""
    wallet_address: str = Field(..., description="Wallet address")
    count: int = Field(..., description="Number of positions")
    positions: List[PositionResponse] = Field(..., description="List of positions")

    class Config:
        json_schema_extra = {
            "example": {
                "wallet_address": "0x554ad2bc8a8f372d7e3376918fcb6e284387859a",
                "count": 2,
                "positions": []
            }
        }

