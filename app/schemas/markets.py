"""Market-related schemas."""

from pydantic import BaseModel, Field
from typing import Optional, List, Any


class MarketInfo(BaseModel):
    """Schema for market information."""
    market_id: str = Field(..., description="Market identifier")
    question: Optional[str] = Field(None, description="Market question")
    status: Optional[str] = Field(None, description="Market status")
    resolution: Optional[str] = Field(None, description="Market resolution (YES/NO)")
    category: Optional[str] = Field(None, description="Market category")

    class Config:
        json_schema_extra = {
            "example": {
                "market_id": "0x123...",
                "question": "Will X happen?",
                "status": "resolved",
                "resolution": "YES",
                "category": "Sports"
            }
        }


class MarketsResponse(BaseModel):
    """Schema for markets list response."""
    count: int = Field(..., description="Number of markets returned", example=100)
    markets: List[Any] = Field(..., description="List of market objects")

    class Config:
        json_schema_extra = {
            "example": {
                "count": 100,
                "markets": []
            }
        }

