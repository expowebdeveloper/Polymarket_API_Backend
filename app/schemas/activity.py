"""Activity-related schemas."""

from pydantic import BaseModel, Field
from typing import Optional, List
from decimal import Decimal


class ActivityResponse(BaseModel):
    """Activity data from API response."""
    proxy_wallet: str = Field(..., description="Proxy wallet address", alias="proxyWallet")
    timestamp: int = Field(..., description="Unix timestamp")
    condition_id: Optional[str] = Field(None, description="Condition ID", alias="conditionId")
    type: str = Field(..., description="Activity type (TRADE, REDEEM, REWARD, etc.)")
    size: Decimal = Field(..., description="Size")
    usdc_size: Decimal = Field(..., description="USDC size", alias="usdcSize")
    transaction_hash: str = Field(..., description="Transaction hash", alias="transactionHash")
    price: Decimal = Field(..., description="Price")
    asset: Optional[str] = Field(None, description="Asset ID")
    side: Optional[str] = Field(None, description="Side (BUY/SELL)")
    outcome_index: Optional[int] = Field(None, description="Outcome index", alias="outcomeIndex")
    title: Optional[str] = Field(None, description="Market title")
    slug: Optional[str] = Field(None, description="Market slug")
    icon: Optional[str] = Field(None, description="Icon URL")
    event_slug: Optional[str] = Field(None, description="Event slug", alias="eventSlug")
    outcome: Optional[str] = Field(None, description="Outcome")
    name: Optional[str] = Field(None, description="User name")
    pseudonym: Optional[str] = Field(None, description="User pseudonym")
    bio: Optional[str] = Field(None, description="User bio")
    profile_image: Optional[str] = Field(None, description="Profile image URL", alias="profileImage")
    profile_image_optimized: Optional[str] = Field(None, description="Optimized profile image URL", alias="profileImageOptimized")

    class Config:
        json_encoders = {
            Decimal: str
        }
        populate_by_name = True


class ActivitiesListResponse(BaseModel):
    """Response model for list of activities."""
    wallet_address: str = Field(..., description="Wallet address")
    count: int = Field(..., description="Number of activities")
    activities: List[ActivityResponse] = Field(..., description="List of activities")

    class Config:
        json_schema_extra = {
            "example": {
                "wallet_address": "0x17db3fcd93ba12d38382a0cade24b200185c5f6d",
                "count": 10,
                "activities": []
            }
        }


