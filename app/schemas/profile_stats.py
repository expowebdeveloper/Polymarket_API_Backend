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


class EnhancedProfileStatsResponse(BaseModel):
    """Enhanced response model for profile statistics with scoring and streaks."""
    proxy_address: str = Field(..., description="Wallet address", alias="proxyAddress")
    username: Optional[str] = Field(None, description="Username")
    name: Optional[str] = Field(None, description="Trader name")
    pseudonym: Optional[str] = Field(None, description="Trader pseudonym")
    profile_image: Optional[str] = Field(None, description="Profile image URL", alias="profileImage")
    
    # Highlighted Metrics
    final_score: float = Field(..., description="Final score (0-100)", alias="finalScore")
    top_percent: float = Field(..., description="Top % based on score", alias="topPercent")
    ranking_tag: str = Field(..., description="Ranking tag (e.g., Top 10%, Top 1%)", alias="rankingTag")
    longest_winning_streak: int = Field(..., description="Longest winning streak", alias="longestWinningStreak")
    current_winning_streak: int = Field(..., description="Current winning streak", alias="currentWinningStreak")
    
    # View Details Metrics
    biggest_win: float = Field(..., description="Biggest win amount", alias="biggestWin")
    worst_loss: float = Field(..., description="Worst loss amount", alias="worstLoss")
    maximum_stake: float = Field(..., description="Maximum stake (avg of top 5)", alias="maximumStake")
    portfolio_value: float = Field(..., description="Portfolio value", alias="portfolioValue")
    average_stake_value: float = Field(..., description="Average stake value", alias="averageStakeValue")
    
    # Additional Info
    rank: Optional[int] = Field(None, description="Current rank in leaderboard")
    total_trades: int = Field(..., description="Total number of trades", alias="totalTrades")
    total_pnl: float = Field(..., description="Total PnL", alias="totalPnl")
    roi: float = Field(..., description="ROI percentage", alias="roi")
    win_rate: float = Field(..., description="Win rate percentage", alias="winRate")
    is_badge_holder: bool = Field(False, description="Polymarket badge holder status", alias="isBadgeHolder")
    
    class Config:
        populate_by_name = True
        json_schema_extra = {
            "example": {
                "proxyAddress": "0x17db3fcd93ba12d38382a0cade24b200185c5f6d",
                "username": "fengdubiying",
                "name": "Trader Name",
                "finalScore": 85.5,
                "topPercent": 5.2,
                "rankingTag": "Top 5%",
                "longestWinningStreak": 12,
                "currentWinningStreak": 3,
                "biggestWin": 50000.0,
                "worstLoss": -10000.0,
                "maximumStake": 5000.0,
                "portfolioValue": 100000.0,
                "averageStakeValue": 1000.0,
                "rank": 1,
                "totalTrades": 150,
                "totalPnl": 25000.0,
                "roi": 25.0,
                "winRate": 65.5,
                "isBadgeHolder": False
            }
        }

