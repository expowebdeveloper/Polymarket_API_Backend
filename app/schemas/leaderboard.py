"""Leaderboard-related schemas."""

from pydantic import BaseModel, Field
from typing import Optional, List, Dict


class LeaderboardEntry(BaseModel):
    """Single entry in a leaderboard."""
    rank: int = Field(..., description="Rank in the leaderboard (1-based)")
    wallet_address: str = Field(..., description="Wallet address")
    name: Optional[str] = Field(None, description="Trader name")
    pseudonym: Optional[str] = Field(None, description="Trader pseudonym")
    profile_image: Optional[str] = Field(None, description="Profile image URL")
    total_pnl: float = Field(..., description="Total PnL")
    roi: float = Field(..., description="Return on Investment (%)")
    win_rate: float = Field(..., description="Win rate (%)")
    total_trades: int = Field(..., description="Total number of trades")
    total_trades_with_pnl: int = Field(..., description="Total trades with calculated PnL")
    winning_trades: int = Field(..., description="Number of winning trades")
    total_stakes: float = Field(..., description="Total stakes invested")
    score_win_rate: float = Field(0.0, description="Win Rate Score (0-1)")
    score_roi: float = Field(0.0, description="ROI Score (0-1)")
    score_pnl: float = Field(0.0, description="PnL Score (0-1)")
    score_risk: float = Field(0.0, description="Risk Score (0-1)")
    # Intermediate values for leaderboard sorting
    W_shrunk: Optional[float] = Field(None, description="W shrunk value (before final score)")
    roi_shrunk: Optional[float] = Field(None, description="ROI shrunk value (before final score)")
    pnl_shrunk: Optional[float] = Field(None, description="PnL shrunk value (before final score)")


class PercentileInfo(BaseModel):
    """Percentile information for normalization."""
    w_shrunk_1_percent: float = Field(..., description="1st percentile of W_shrunk")
    w_shrunk_99_percent: float = Field(..., description="99th percentile of W_shrunk")
    roi_shrunk_1_percent: float = Field(..., description="1st percentile of ROI_shrunk")
    roi_shrunk_99_percent: float = Field(..., description="99th percentile of ROI_shrunk")
    pnl_shrunk_1_percent: float = Field(..., description="1st percentile of PNL_shrunk")
    pnl_shrunk_99_percent: float = Field(..., description="99th percentile of PNL_shrunk")
    population_size: int = Field(..., description="Number of traders with >= 5 trades used for percentiles")


class MedianInfo(BaseModel):
    """Median values used in shrinkage calculations."""
    roi_median: float = Field(..., description="Median ROI across population")
    pnl_median: float = Field(..., description="Median adjusted PnL across population")


class AllLeaderboardsResponse(BaseModel):
    """Response model containing all leaderboards and percentile information."""
    percentiles: PercentileInfo = Field(..., description="Percentile anchors for normalization")
    medians: MedianInfo = Field(..., description="Median values used in calculations")
    leaderboards: Dict[str, List[LeaderboardEntry]] = Field(..., description="All leaderboards keyed by metric type")
    total_traders: int = Field(..., description="Total number of traders")
    population_traders: int = Field(..., description="Number of traders with >= 5 trades")


class LeaderboardResponse(BaseModel):
    """Response model for leaderboard endpoints."""
    period: str = Field(..., description="Time period filter (7d, 30d, all)")
    metric: str = Field(..., description="Metric used for ranking (pnl, roi, win_rate)")
    count: int = Field(..., description="Number of traders in leaderboard")
    entries: List[LeaderboardEntry] = Field(..., description="List of leaderboard entries")

    class Config:
        json_schema_extra = {
            "example": {
                "period": "30d",
                "metric": "pnl",
                "count": 50,
                "entries": [
                    {
                        "rank": 1,
                        "wallet_address": "0x17db3fcd93ba12d38382a0cade24b200185c5f6d",
                        "name": "Trader Name",
                        "pseudonym": "trader_pseudonym",
                        "profile_image": "https://example.com/image.png",
                        "total_pnl": 10000.50,
                        "roi": 15.5,
                        "win_rate": 65.0,
                        "total_trades": 100,
                        "total_trades_with_pnl": 95,
                        "winning_trades": 62,
                        "total_stakes": 50000.0
                    }
                ]
            }
        }

