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
    async def calculate_portfolio_stats(user_address: str, time_period: str = "all") -> Dict[str, Any]:
        """
        Calculate comprehensive portfolio statistics including PnL, Win Rates, and ROI.
        
        Args:
            user_address: Wallet address
            time_period: Time period for filtering data ("day", "week", "month", "all")
            
        Returns:
            Dictionary containing PnL, Win Rate, ROI, and other metrics
        """
<<<<<<< HEAD
        # Fetch data (AWAITING all async calls)
        positions = await fetch_positions_for_wallet(user_address) or []
        closed_positions = await fetch_closed_positions(user_address) or []
        portfolio_value = await fetch_portfolio_value(user_address) or 0.0
        leaderboard_stats = await fetch_leaderboard_stats(user_address) or {}
=======
        # Fetch data (AWAITING all async calls) with error handling
        try:
            positions = await fetch_positions_for_wallet(user_address)
        except Exception:
            positions = []
        
        try:
            # Fetch ALL closed positions (no limit) - user requested all data
            closed_positions = await fetch_closed_positions(
                user_address, 
                time_period=time_period,
                limit=None  # Fetch all positions
            )
        except Exception:
            closed_positions = []
        
<<<<<<< HEAD
        try:
            portfolio_value = await fetch_portfolio_value(user_address)
        except Exception:
            portfolio_value = 0.0
        
        try:
            leaderboard_stats = await fetch_leaderboard_stats(user_address, time_period=time_period)
        except Exception:
            leaderboard_stats = {}
        
        # Ensure we have valid data structures (handle None returns)
        if positions is None:
            positions = []
        if closed_positions is None:
            closed_positions = []
        if leaderboard_stats is None:
            leaderboard_stats = {}
        if portfolio_value is None:
            portfolio_value = 0.0
>>>>>>> 1e267d7cee08180e9c110108b558c48504150e5b
=======
        # Breakdown Metrics
        unrealized_pnl = sum(float(p.get("cashPnl", 0.0)) for p in positions)
        reailzed_pnl_sum = sum(float(c.get("realizedPnl", 0.0)) for c in closed_positions)
        total_calculated_pnl = unrealized_pnl + reailzed_pnl_sum

        # Win Rate Calculations
        total_closed_count = len(closed_positions)
        wins = 0
        winning_stakes = 0.0
        total_stakes = 0.0
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
        # ROI = Total PnL (Leaderboard) / Total Volume
        roi = (total_pnl / total_investment * 100) if total_investment > 0 else 0.0
        
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
                "roi": round(roi, 2),
                "total_investment": round(total_investment, 2),
                "portfolio_value": round(portfolio_value, 2),
                "winning_stakes": winning_stakes,
                "sum_sq_stakes": sum_sq_stakes,
                "max_stake": max_stake,
                "worst_loss": worst_loss,
                "total_stakes_calculated": total_stakes,
                "wins": wins
            },
            "positions_summary": {
                "open_positions_count": len(positions),
                "closed_positions_count": total_closed_count
            }
        }

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
>>>>>>> 7ffa6dd982d968bfe597ebd0f22d2268454ce1bc
        
        # Core Metrics from Leaderboard (Source of Truth for Profile Stats)
        total_pnl = leaderboard_stats.get("pnl", 0.0)
        total_volume = leaderboard_stats.get("volume", 0.0) # Previously "total_investment"
        
        # Breakdown Metrics
        unrealized_pnl = sum(float(p.get("cashPnl", 0.0)) for p in positions) if positions else 0.0
        reailzed_pnl_sum = sum(float(c.get("realizedPnl", 0.0)) for c in closed_positions) if closed_positions else 0.0
        total_calculated_pnl = unrealized_pnl + reailzed_pnl_sum

        # Win Rate Calculations
        total_closed_count = len(closed_positions) if closed_positions else 0
        wins = 0
        winning_stakes = 0.0
        total_stakes = 0.0 # This is closed trades investment
        sum_sq_stakes = 0.0
        max_stake = 0.0
        worst_loss = 0.0
        all_losses = []  # Collect all losses for average calculation (future enhancement)
        
        # Ensure closed_positions is iterable
        if not closed_positions:
            closed_positions = []
        
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
            
            # Worst loss (min PnL) and collect all losses
            if realized_pnl < worst_loss:
                worst_loss = realized_pnl
            # Collect all losses (negative PnL values) for average calculation
            if realized_pnl < 0:
                all_losses.append(realized_pnl) 

        win_rate = (wins / total_closed_count * 100) if total_closed_count > 0 else 0.0
        
        # Stake-Weighted Win Rate
        # Formula: Sum(stakes of wins) / Sum(stakes of all trades)
        stake_weighted_win_rate = (winning_stakes / total_stakes * 100) if total_stakes > 0 else 0.0
        
        # ROI Calculation
        # ROI = Total PnL / Total Investment
        
        # Calculate Investment for Open Positions
        total_investment_open = 0.0
        if positions:
            for p in positions:
                size = float(p.get("size", 0.0))
                avg_price = float(p.get("avgPrice", 0.0)) 
                total_investment_open += abs(size * avg_price)
             
        total_investment_closed = total_stakes
        total_investment = total_investment_closed + total_investment_open
        
        # Use a more robust total_pnl (fallback to calculated if leaderboard is suspect)
        # If total_pnl is significantly different from calculated, we might prefer calculated
        # but for consistency with leaderboard, we stick to leaderboard if available.
        final_total_pnl = total_pnl if total_pnl != 0 else total_calculated_pnl

        if total_investment > 0:
            roi = (final_total_pnl / total_investment * 100)
        elif total_investment_closed > 0:
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
                "total_stakes": round(total_stakes, 2),  # Total stakes from closed positions
                "total_stakes_calculated": round(total_stakes, 2),  # Alias for compatibility
                "winning_stakes": round(winning_stakes, 2),
                "sum_sq_stakes": round(sum_sq_stakes, 2),
                "max_stake": round(max_stake, 2),
                "worst_loss": round(worst_loss, 2),
                "all_losses": [round(loss, 2) for loss in all_losses],  # All losses for average calculation
                "wins": wins
            },
            "positions_summary": {
                "open_positions_count": len(positions),
                "closed_positions_count": total_closed_count
            }
        }
