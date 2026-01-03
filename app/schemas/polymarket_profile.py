"""Polymarket-style trader profile schemas matching exact UI format."""

from pydantic import BaseModel, Field
from typing import Optional, List, Dict
from datetime import datetime


class Badge(BaseModel):
    """Trader badge information."""
    label: str = Field(..., description="Badge label (e.g., 'Top 10', 'Whale', 'Hot Streak')")
    color: str = Field(..., description="Badge color (e.g., 'yellow', 'blue', 'purple')")


class TradeHistoryEntry(BaseModel):
    """Trade history entry matching Polymarket format."""
    market: str = Field(..., description="Market name")
    outcome: str = Field(..., description="Outcome (YES/NO)")
    price: Optional[str] = Field(None, description="Trade price")
    pnl: str = Field(..., description="Profit/Loss (e.g., '+$3,200' or '-$1,450')")
    date: str = Field(..., description="Date (e.g., '2 hours ago', '1 day ago')")
    timestamp: int = Field(..., description="Unix timestamp")


class SentimentDataPoint(BaseModel):
    """Data point for sentiment graph."""
    date: str = Field(..., description="Date label")
    timestamp: int = Field(..., description="Unix timestamp")
    value: float = Field(..., description="Sentiment value (percentage)")


class RecentTradeSentiment(BaseModel):
    """Recent trade sentiment data."""
    period: str = Field(..., description="Period label (e.g., 'Last 7 days')")
    change: str = Field(..., description="Change percentage (e.g., '+18.2%')")
    data_points: List[SentimentDataPoint] = Field(..., description="Graph data points", alias="dataPoints")
    trade_confidence: float = Field(..., description="Trade confidence percentage", alias="tradeConfidence")
    wins: int = Field(..., description="Number of wins")
    losses: int = Field(..., description="Number of losses")


class PolymarketTraderProfile(BaseModel):
    """Comprehensive trader profile matching Polymarket's exact format."""
    # Header
    wallet_address: str = Field(..., description="Wallet address", alias="walletAddress")
    name: Optional[str] = Field(None, description="Trader name")
    pseudonym: Optional[str] = Field(None, description="Trader pseudonym")
    profile_image: Optional[str] = Field(None, description="Profile image URL", alias="profileImage")
    
    # Final Score Section
    final_score: float = Field(..., description="Final score (0-100)", alias="finalScore")
    top_percent: float = Field(..., description="Top % (e.g., 1 for Top 1%)", alias="topPercent")
    ranking_tag: str = Field(..., description="Ranking tag (e.g., 'Top 1% Trader')", alias="rankingTag")
    badges: List[Badge] = Field(default_factory=list, description="List of badges")
    
    # KPIs
    roi_percent: float = Field(..., description="ROI percentage", alias="roiPercent")
    win_rate: float = Field(..., description="Win rate percentage", alias="winRate")
    win_rate_detail: str = Field(..., description="Win rate detail (e.g., '156 of 211 trades')", alias="winRateDetail")
    total_volume: float = Field(..., description="Total volume", alias="totalVolume")
    total_volume_detail: str = Field(..., description="Volume detail (e.g., 'Across 12 markets')", alias="totalVolumeDetail")
    total_trades: int = Field(..., description="Total number of trades", alias="totalTrades")
    total_trades_detail: str = Field(..., description="Trades detail (e.g., 'Since joining')", alias="totalTradesDetail")
    
    # Streaks
    longest_streak: int = Field(..., description="Longest winning streak", alias="longestStreak")
    current_streak: int = Field(..., description="Current winning streak", alias="currentStreak")
    
    # Wins/Losses
    total_wins: int = Field(..., description="Total wins", alias="totalWins")
    total_losses: int = Field(..., description="Total losses", alias="totalLosses")
    
    # Reward
    reward_earned: float = Field(..., description="Total reward earned", alias="rewardEarned")
    
    # Trade History
    trade_history: List[TradeHistoryEntry] = Field(..., description="Trade history entries", alias="tradeHistory")
    trade_history_total: int = Field(..., description="Total number of trades in history", alias="tradeHistoryTotal")
    
    # Recent Trade Sentiment
    recent_sentiment: RecentTradeSentiment = Field(..., description="Recent trade sentiment data", alias="recentSentiment")
    
    class Config:
        populate_by_name = True
        json_schema_extra = {
            "example": {
                "walletAddress": "0x9d84ce0306f8551e02efef1680475fc0f1dc1344",
                "finalScore": 94.2,
                "topPercent": 1,
                "rankingTag": "Top 1% Trader",
                "badges": [
                    {"label": "Top 10", "color": "yellow"},
                    {"label": "Whale", "color": "blue"},
                    {"label": "Hot Streak", "color": "purple"}
                ],
                "roiPercent": 127.3,
                "winRate": 73.8,
                "winRateDetail": "156 of 211 trades",
                "totalVolume": 245000,
                "totalVolumeDetail": "Across 12 markets",
                "totalTrades": 211,
                "totalTradesDetail": "Since joining",
                "longestStreak": 16,
                "currentStreak": 3,
                "totalWins": 20,
                "totalLosses": 7,
                "rewardEarned": 60.0,
                "tradeHistory": [],
                "tradeHistoryTotal": 0,
                "recentSentiment": {
                    "period": "Last 7 days",
                    "change": "+18.2%",
                    "dataPoints": [],
                    "tradeConfidence": 78.0,
                    "wins": 12,
                    "losses": 3
                }
            }
        }
