"""Profile stats-related schemas."""

from pydantic import BaseModel, Field
from typing import Optional
from decimal import Decimal


class ProfileStatsResponse(BaseModel):
    """Response model for profile statistics."""
    proxy_address: str = Field(..., description="Wallet address", alias="proxyAddress")
    username: Optional[str] = Field(None, description="Username")
    trades: int = Field(..., description="Number of trades")
    largest_win: Decimal = Field(..., description="Largest win amount", alias="largestWin")
    views: int = Field(..., description="Profile views")
    join_date: Optional[str] = Field(None, description="Join date", alias="joinDate")

    class Config:
        json_encoders = {
            Decimal: str
        }
        populate_by_name = True
        json_schema_extra = {
            "example": {
                "proxyAddress": "0x17db3fcd93ba12d38382a0cade24b200185c5f6d",
                "username": "fengdubiying",
                "trades": 114,
                "largestWin": "818736.309837",
                "views": 110385,
                "joinDate": "Oct 2025"
            }
        }

