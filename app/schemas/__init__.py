"""Pydantic schemas for API validation."""

from app.schemas.analytics import AnalyticsResponse, CategoryMetrics
from app.schemas.general import HealthResponse, ErrorResponse
from app.schemas.markets import MarketsResponse, MarketInfo
from app.schemas.requests import WalletRequest
from app.schemas.traders import (
    TraderBasicInfo,
    TraderDetail,
    TradersListResponse,
    TraderTradesResponse
)

__all__ = [
    "AnalyticsResponse",
    "CategoryMetrics",
    "HealthResponse",
    "ErrorResponse",
    "MarketsResponse",
    "MarketInfo",
    "WalletRequest",
    "TraderBasicInfo",
    "TraderDetail",
    "TradersListResponse",
    "TraderTradesResponse",
]

