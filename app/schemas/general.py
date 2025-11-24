"""General schemas for common responses."""

from pydantic import BaseModel, Field
from typing import Optional


class HealthResponse(BaseModel):
    """Schema for health check response."""
    status: str = Field(..., description="Service status", example="healthy")
    version: str = Field(..., description="API version", example="1.0.0")
    service: str = Field(..., description="Service name", example="Polymarket Analytics Platform")

    class Config:
        json_schema_extra = {
            "example": {
                "status": "healthy",
                "version": "1.0.0",
                "service": "Polymarket Analytics Platform"
            }
        }


class ErrorResponse(BaseModel):
    """Schema for error responses."""
    error: str = Field(..., description="Error message", example="Invalid wallet address format")
    detail: Optional[str] = Field(None, description="Detailed error information")
    status_code: int = Field(..., description="HTTP status code", example=400)

    class Config:
        json_schema_extra = {
            "example": {
                "error": "Invalid wallet address format",
                "detail": "Wallet address must be 42 characters starting with 0x",
                "status_code": 400
            }
        }

