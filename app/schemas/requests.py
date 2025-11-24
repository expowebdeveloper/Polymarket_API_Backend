"""Request schemas."""

from pydantic import BaseModel, Field, validator


class WalletRequest(BaseModel):
    """Schema for wallet analysis request."""
    wallet: str = Field(..., description="Wallet address to analyze", example="0x56687bf447db6ffa42ffe2204a05edaa20f55839")

    @validator('wallet')
    def validate_wallet(cls, v):
        """Validate wallet address format."""
        if not v:
            raise ValueError('Wallet address is required')
        if not v.startswith('0x'):
            raise ValueError('Wallet address must start with 0x')
        if len(v) != 42:
            raise ValueError('Wallet address must be 42 characters long')
        try:
            int(v[2:], 16)
        except ValueError:
            raise ValueError('Wallet address contains invalid hexadecimal characters')
        return v

    class Config:
        json_schema_extra = {
            "example": {
                "wallet": "0x56687bf447db6ffa42ffe2204a05edaa20f55839"
            }
        }

