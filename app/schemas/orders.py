"""Order-related schemas."""

from pydantic import BaseModel, Field
from typing import Optional, List
from decimal import Decimal


class OrderResponse(BaseModel):
    """Order data from API response."""
    token_id: str = Field(..., description="Token ID")
    token_label: str = Field(..., description="Token label (Yes/No)")
    side: str = Field(..., description="Order side (BUY/SELL)")
    market_slug: str = Field(..., description="Market slug")
    condition_id: str = Field(..., description="Condition ID")
    shares: Decimal = Field(..., description="Number of shares")
    price: Decimal = Field(..., description="Price per share")
    tx_hash: str = Field(..., description="Transaction hash")
    title: Optional[str] = Field(None, description="Market title")
    timestamp: int = Field(..., description="Unix timestamp")
    order_hash: str = Field(..., description="Order hash")
    user: str = Field(..., description="User wallet address")
    taker: str = Field(..., description="Taker wallet address")
    shares_normalized: Decimal = Field(..., description="Normalized shares")

    class Config:
        json_encoders = {
            Decimal: str
        }


class PaginationInfo(BaseModel):
    """Pagination information."""
    limit: int = Field(..., description="Limit per page")
    offset: int = Field(..., description="Current offset")
    total: int = Field(..., description="Total number of orders")
    has_more: bool = Field(..., description="Whether there areFF more orders")


class OrdersListResponse(BaseModel):
    """Response model for list of orders."""
    count: int = Field(..., description="Number of orders in response")
    orders: List[OrderResponse] = Field(..., description="List of orders")
    pagination: Optional[PaginationInfo] = Field(None, description="Pagination information")

    class Config:
        json_schema_extra = {
            "example": {
                "count": 10,
                "orders": [],
                "pagination": {
                    "limit": 10,
                    "offset": 0,
                    "total": 100,
                    "has_more": True
                }
            }
        }

