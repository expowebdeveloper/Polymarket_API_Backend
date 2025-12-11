"""User PnL-related schemas."""

from pydantic import BaseModel, Field
from typing import Optional, List
from decimal import Decimal


class PnLDataPoint(BaseModel):
    """Single PnL data point."""
    t: int = Field(..., description="Unix timestamp")
    p: Decimal = Field(..., description="Profit and Loss value")

    class Config:
        json_encoders = {
            Decimal: str
        }


class UserPnLResponse(BaseModel):
    """Response model for user PnL data."""
    user_address: str = Field(..., description="Wallet address")
    interval: str = Field(..., description="Time interval")
    fidelity: str = Field(..., description="Data fidelity")
    count: int = Field(..., description="Number of data points")
    data: List[PnLDataPoint] = Field(..., description="List of PnL data points")

    class Config:
        json_schema_extra = {
            "example": {
                "user_address": "0x554ad2bc8a8f372d7e3376918fcb6e284387859a",
                "interval": "1m",
                "fidelity": "1d",
                "count": 11,
                "data": [
                    {"t": 1763769600, "p": "-2.307538"},
                    {"t": 1763856000, "p": "49.7984"}
                ]
            }
        }

