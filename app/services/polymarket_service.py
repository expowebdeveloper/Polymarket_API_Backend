"""
Service for Polymarket data aggregation and calculation.
"""
from typing import Dict, List, Optional, Any
from app.services.data_fetcher import (
    fetch_positions_for_wallet,
    fetch_closed_positions,
    fetch_portfolio_value,
    fetch_leaderboard_stats
)

class PolymarketService:
    @staticmethod
    def calculate_portfolio_stats(user_address: str) -> Dict[str, Any]:
        """
        Calculate comprehensive portfolio statistics including PnL, Win Rates, and ROI.
        
        Args:
            user_address: Wallet address
            
        Returns:
            Dictionary containing PnL, Win Rate, ROI, and other metrics
        """
        # Fetch data
        positions = fetch_positions_for_wallet(user_address)
        closed_positions = fetch_closed_positions(user_address)
        portfolio_value = fetch_portfolio_value(user_address)
        leaderboard_stats = fetch_leaderboard_stats(user_address)
        
        # Core Metrics from Leaderboard (Source of Truth for Profile Stats)
        total_pnl = leaderboard_stats.get("pnl", 0.0)
        total_investment = leaderboard_stats.get("volume", 0.0) # User defined "total stakes" as volume
        
        # Breakdown Metrics
        unrealized_pnl = sum(float(p.get("cashPnl", 0.0)) for p in positions)
        reailzed_pnl_sum = sum(float(c.get("realizedPnl", 0.0)) for c in closed_positions)
        
        # Win Rate Calculations
        total_closed_count = len(closed_positions)
        wins = 0
        winning_stakes = 0.0
        total_stakes = 0.0
        
        for c in closed_positions:
            # Calculating Stake for Closed Position
            # totalBought = size, avgPrice = entry price
            size = float(c.get("totalBought", 0.0))
            avg_price = float(c.get("avgPrice", 0.0))
            stake = size * avg_price
            
            total_stakes += stake
            
            # Check for Win
            if float(c.get("realizedPnl", 0.0)) > 0:
                wins += 1
                winning_stakes += stake
        
        # Standard Win Rate
        win_rate = (wins / total_closed_count * 100) if total_closed_count > 0 else 0.0
        
        # Stake-Weighted Win Rate
        # Formula: Sum(stakes of wins) / Sum(stakes of all trades)
        stake_weighted_win_rate = (winning_stakes / total_stakes * 100) if total_stakes > 0 else 0.0
        
        # ROI Calculation
        # ROI = Total PnL (Leaderboard) / Total Volume
        roi = (total_pnl / total_investment * 100) if total_investment > 0 else 0.0
        
        return {
            "user_address": user_address,
            "pnl_metrics": {
                "realized_pnl": round(reailzed_pnl_sum, 2),
                "unrealized_pnl": round(unrealized_pnl, 2),
                "total_pnl": round(total_pnl, 2) # Sourced from Leaderboard
            },
            "performance_metrics": {
                "win_rate": round(win_rate, 2),
                "stake_weighted_win_rate": round(stake_weighted_win_rate, 2),
                "roi": round(roi, 2),
                "total_investment": round(total_investment, 2),
                "portfolio_value": round(portfolio_value, 2)
            },
            "positions_summary": {
                "open_positions_count": len(positions),
                "closed_positions_count": total_closed_count
            }
        }
