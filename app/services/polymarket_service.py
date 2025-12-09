"""
Service for Polymarket data aggregation and calculation.
"""
from typing import Dict, List, Optional
from decimal import Decimal

from app.services.data_fetcher import (
    fetch_positions_for_wallet,
    fetch_closed_positions,
    fetch_portfolio_value
)

class PolymarketService:
    @staticmethod
    def calculate_portfolio_stats(user_address: str) -> Dict:
        """
        Calculate comprehensive portfolio statistics for a user.
        
        Args:
            user_address: Wallet address
            
        Returns:
            Dictionary containing PnL, Win Rate, ROI, and other metrics
        """
        # Fetch data
        open_positions = fetch_positions_for_wallet(user_address)
        closed_positions = fetch_closed_positions(user_address)
        portfolio_value = fetch_portfolio_value(user_address)
        
        # Initialize metrics
        realized_pnl = 0.0
        unrealized_pnl = 0.0
        invested_open = 0.0
        invested_closed = 0.0
        wins = 0
        total_closed = len(closed_positions)
        
        # Process Closed Positions
        for pos in closed_positions:
            r_pnl = float(pos.get("realizedPnl", 0.0))
            realized_pnl += r_pnl
            
            # Count wins (profitable positions)
            if r_pnl > 0:
                wins += 1
                
            # Calculate investment for closed positions
            # totalBought is typically the size (share count)
            # avgPrice is the average buy price
            # Investment = Size * AvgPrice
            size = float(pos.get("totalBought", 0.0))
            avg_price = float(pos.get("avgPrice", 0.0))
            investment = size * avg_price
            invested_closed += investment
            
        # Process Open Positions
        for pos in open_positions:
            # cashPnl is typically (CurrentValue - Cost)
            cash_pnl = float(pos.get("cashPnl", 0.0))
            unrealized_pnl += cash_pnl
            
            # initialValue is the cost basis
            initial_value = float(pos.get("initialValue", 0.0))
            invested_open += initial_value
            
        # Aggregate metrics
        total_pnl = realized_pnl + unrealized_pnl
        total_investment = invested_open + invested_closed
        
        # Calculate derived metrics
        win_rate = (wins / total_closed * 100) if total_closed > 0 else 0.0
        roi = (total_pnl / total_investment * 100) if total_investment > 0 else 0.0
        
        return {
            "user_address": user_address,
            "pnl_metrics": {
                "realized_pnl": round(realized_pnl, 2),
                "unrealized_pnl": round(unrealized_pnl, 2),
                "total_pnl": round(total_pnl, 2)
            },
            "performance_metrics": {
                "win_rate": round(win_rate, 2),
                "roi": round(roi, 2),
                "total_investment": round(total_investment, 2),
                "portfolio_value": round(portfolio_value, 2)
            },
            "positions_summary": {
                "open_positions_count": len(open_positions),
                "closed_positions_count": total_closed
            }
        }
