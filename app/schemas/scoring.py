from pydantic import BaseModel, Field
from typing import Optional

class ScoringV2Response(BaseModel):
    """Latest scoring metrics for a trader."""
    wallet_address: str = Field(..., description="Wallet address")
    w_shrunk: float = Field(..., description="W Shrunk (Win Rate with reliability correction)")
    pnl_score: float = Field(..., description="PnL Score (0-1 normalized)")
    win_rate: float = Field(..., description="Win Rate (%)")
    win_rate_percent: float = Field(..., description="Win Rate (%) - same as win_rate for compatibility")
    final_rating: float = Field(..., description="Final Rating (0-100 weighted combination)")
    confidence_score: float = Field(..., description="Confidence Score (0-1 based on trade count)")
    roi_score: float = Field(..., description="ROI Score (0-1 normalized)")
    risk_score: float = Field(..., description="Risk Score (0-1, lower is better)")
    
    # Extra metrics that might be useful
    total_pnl: float = Field(..., description="Total PnL")
    roi: float = Field(..., description="Return on Investment (%)")
    total_trades: int = Field(..., description="Total trades count")
    winning_trades: int = Field(..., description="Number of winning trades")
