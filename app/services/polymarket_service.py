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
        total_volume = leaderboard_stats.get("volume", 0.0) # Previously "total_investment"
        
        # Breakdown Metrics
        unrealized_pnl = sum(float(p.get("cashPnl", 0.0)) for p in positions)
        reailzed_pnl_sum = sum(float(c.get("realizedPnl", 0.0)) for c in closed_positions)
        total_calculated_pnl = unrealized_pnl + reailzed_pnl_sum

        # Win Rate Calculations
        total_closed_count = len(closed_positions)
        wins = 0
        winning_stakes = 0.0
        total_stakes = 0.0 # This is closed trades investment
        sum_sq_stakes = 0.0
        max_stake = 0.0
        worst_loss = 0.0
        
        for c in closed_positions:
            # Calculating Stake for Closed Position
            # totalBought = size, avgPrice = entry price
            size = float(c.get("totalBought", 0.0))
            avg_price = float(c.get("avgPrice", 0.0))
            stake = size * avg_price
            
            total_stakes += stake
            sum_sq_stakes += stake ** 2
            if stake > max_stake:
                max_stake = stake
            
            # Check for Win/Loss
            realized_pnl = float(c.get("realizedPnl", 0.0))
            if realized_pnl > 0:
                wins += 1
                winning_stakes += stake
            
            # Worst loss (min PnL)
            if realized_pnl < worst_loss:
                worst_loss = realized_pnl 

        win_rate = (wins / total_closed_count * 100) if total_closed_count > 0 else 0.0
        
        # Stake-Weighted Win Rate
        # Formula: Sum(stakes of wins) / Sum(stakes of all trades)
        stake_weighted_win_rate = (winning_stakes / total_stakes * 100) if total_stakes > 0 else 0.0
        
        # ROI Calculation
        # ROI = Total PnL (Leaderboard) / Total Investment (Actual)
        
        # Calculate Investment for Open Positions
        total_investment_open = 0.0
        for p in positions:
             # For open positions: size * avgPrice (buyPrice)
             size = float(p.get("size", 0.0))
             avg_price = float(p.get("avgPrice", 0.0)) 
             # Note: API usually returns 'avgPrice' as the buy price for the position
             total_investment_open += abs(size * avg_price)
             
        # Actual Total Investment = Closed Investment + Open Investment
        total_investment_closed = total_stakes
        total_investment = total_investment_closed + total_investment_open
        
        # Recalculate ROI using Realized PnL / Closed Investment
        # User Request: "calculate ROI and ROI % using teh realized_PnL / total_investment_of closed_markket"
        if total_investment_closed > 0:
            roi = (reailzed_pnl_sum / total_investment_closed * 100)
        else:
            roi = 0.0
        
        return {
            "user_address": user_address,
            "pnl_metrics": {
                "realized_pnl": round(reailzed_pnl_sum, 2),
                "unrealized_pnl": round(unrealized_pnl, 2),
                "total_pnl": round(total_pnl, 2), # Sourced from Leaderboard
                "total_calculated_pnl": round(total_calculated_pnl, 2)
            },
            "performance_metrics": {
                "win_rate": round(win_rate, 2),
                "stake_weighted_win_rate": round(stake_weighted_win_rate, 2),
                "roi": round(roi, 2),
                "total_volume": round(total_volume, 2), # Ex-total_investment (from leaderboard)
                "total_investment": round(total_investment, 2),
                "investment_value_closed_trades": round(total_investment_closed, 2),
                "total_investment_open_markets": round(total_investment_open, 2),
                "portfolio_value": round(portfolio_value, 2),
                "winning_stakes": winning_stakes,
                "sum_sq_stakes": sum_sq_stakes,
                "max_stake": max_stake,
                "worst_loss": worst_loss,
                "wins": wins
            },
            "positions_summary": {
                "open_positions_count": len(positions),
                "closed_positions_count": total_closed_count
            }
        }
