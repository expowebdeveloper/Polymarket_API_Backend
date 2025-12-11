"""Trade-related schemas."""

from pydantic import BaseModel, Field
from typing import Optional, List
from decimal import Decimal


class TradeResponse(BaseModel):
    """Trade data from API response."""
    proxy_wallet: str = Field(..., description="Proxy wallet address", alias="proxyWallet")
    side: str = Field(..., description="Trade side (BUY/SELL)")
    asset: str = Field(..., description="Asset ID")
    condition_id: str = Field(..., description="Condition ID", alias="conditionId")
    size: Decimal = Field(..., description="Trade size")
    price: Decimal = Field(..., description="Trade price")
    timestamp: int = Field(..., description="Unix timestamp")
    title: Optional[str] = Field(None, description="Market title")
    slug: Optional[str] = Field(None, description="Market slug")
    icon: Optional[str] = Field(None, description="Icon URL")
    event_slug: Optional[str] = Field(None, description="Event slug", alias="eventSlug")
    outcome: Optional[str] = Field(None, description="Outcome")
    outcome_index: Optional[int] = Field(None, description="Outcome index", alias="outcomeIndex")
    name: Optional[str] = Field(None, description="User name")
    pseudonym: Optional[str] = Field(None, description="User pseudonym")
    bio: Optional[str] = Field(None, description="User bio")
    profile_image: Optional[str] = Field(None, description="Profile image URL", alias="profileImage")
    profile_image_optimized: Optional[str] = Field(None, description="Optimized profile image URL", alias="profileImageOptimized")
    transaction_hash: str = Field(..., description="Transaction hash", alias="transactionHash")

    class Config:
        json_encoders = {
            Decimal: str
        }
        populate_by_name = True


class TradesListResponse(BaseModel):
    """Response model for list of trades."""
    wallet_address: str = Field(..., description="Wallet address")
    count: int = Field(..., description="Number of trades")
    trades: List[TradeResponse] = Field(..., description="List of trades")

    class Config:
        json_schema_extra = {
            "example": {
                "wallet_address": "0xdbade4c82fb72780a0db9a38f821d8671aba9c95",
                "count": 10,
                "trades": []
            }
        }


