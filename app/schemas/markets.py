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


class PaginationInfo(BaseModel):
    """Pagination information."""
    limit: int = Field(..., description="Limit per page")
    offset: int = Field(..., description="Current offset")
    total: int = Field(..., description="Total number of markets available")
    has_more: bool = Field(..., description="Whether there are more markets")


class MarketsResponse(BaseModel):
    """Schema for markets list response."""
    count: int = Field(..., description="Number of markets returned", example=100)
    markets: List[Any] = Field(..., description="List of market objects")
    pagination: Optional[PaginationInfo] = Field(None, description="Pagination information")

    class Config:
        json_schema_extra = {
            "example": {
                "count": 100,
                "markets": [],
                "pagination": {
                    "limit": 20,
                    "offset": 0,
                    "total": 500,
                    "has_more": True
                }
            }
        }

