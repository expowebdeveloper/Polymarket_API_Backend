from typing import Dict, Any, List, Optional
from sqlalchemy.future import select
from sqlalchemy import desc, func
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime
from collections import Counter

from app.db.models import (
    Trader, Position, ClosedPosition, Activity, 
    UserPnL, AggregatedMetrics, ProfileStats, Trade
)
from app.services.leaderboard_service import (
    calculate_trader_metrics_with_time_filter,
    calculate_scores_and_rank_with_percentiles,
    process_trader_data_points,
    calculate_scores_and_rank
)
from app.services.pnl_median_service import get_pnl_median_from_population
from app.services.confidence_scoring import calculate_confidence_score, calculate_confidence_with_details

async def get_db_dashboard_data(session: AsyncSession, wallet_address: str) -> Dict[str, Any]:
    """
    Aggregate all necessary data for the wallet dashboard from the local database.
    """
    # 1. Fetch Trader & Metrics
    stmt = select(Trader).where(Trader.wallet_address == wallet_address).order_by(desc(Trader.updated_at))
    result = await session.execute(stmt)
    trader = result.scalars().first()
    
    # 2. Fetch Profile Stats
    stmt = select(ProfileStats).where(ProfileStats.proxy_address == wallet_address).order_by(desc(ProfileStats.updated_at))
    result = await session.execute(stmt)
    profile_stats = result.scalars().first()
    
    # 3. Fetch Aggregated Metrics
    agg_metrics = None
    if trader:
        stmt = select(AggregatedMetrics).where(AggregatedMetrics.trader_id == trader.id).order_by(desc(AggregatedMetrics.updated_at))
        result = await session.execute(stmt)
        agg_metrics = result.scalars().first()

    # 4. Fetch Active Positions
    stmt = select(Position).where(Position.proxy_wallet == wallet_address)
    result = await session.execute(stmt)
    active_positions = result.scalars().all()
    
    # 5. Fetch Closed Positions
    stmt = select(ClosedPosition).where(ClosedPosition.proxy_wallet == wallet_address).order_by(ClosedPosition.timestamp.desc())
    result = await session.execute(stmt)
    closed_positions = result.scalars().all()
    
    # 6. Fetch All Activities (no limit for complete activity list)
    stmt = select(Activity).where(Activity.proxy_wallet == wallet_address).order_by(Activity.timestamp.desc())
    result = await session.execute(stmt)
    activities = result.scalars().all()
    
    # 7. Fetch All Trades for market distribution and trade count
    stmt = select(Trade).where(Trade.proxy_wallet == wallet_address).order_by(Trade.timestamp.desc())
    result = await session.execute(stmt)
    all_trades = result.scalars().all()
    
    # 8. Fetch PnL History
    stmt = select(UserPnL).where(
        UserPnL.user_address == wallet_address,
        UserPnL.interval == "1m",
        UserPnL.fidelity == "1d"
    ).order_by(UserPnL.timestamp.asc())
    result = await session.execute(stmt)
    pnl_history = result.scalars().all()
    
    # --- derived calculations ---
    
    # Username Fallback: Try Activity table if name/pseudonym missing from Trader/Profile
    username = trader.name if trader and trader.name else (profile_stats.username if profile_stats and profile_stats.username else "Unknown")
    if username == "Unknown" and activities:
        # Check first few activities for name or pseudonym
        for a in activities:
            if a.name:
                username = a.name
                break
            if a.pseudonym:
                username = a.pseudonym
                break

    # Portfolio Value calculation
    # 1. Use aggregated value if available (now includes cash via sync)
    # 2. Fallback to positions sum
    portfolio_value = float(agg_metrics.portfolio_value) if agg_metrics and agg_metrics.portfolio_value else sum(float(p.current_value or 0) for p in active_positions)
    
    # Largest Win / Worst Loss / Realized PnL from Closed Positions
    largest_win = 0.0
    worst_loss = 0.0
    realized_pnl_total = 0.0
    
    for cp in closed_positions:
        pnl = float(cp.realized_pnl or 0)
        realized_pnl_total += pnl
        if pnl > largest_win:
            largest_win = pnl
        if pnl < worst_loss:
            worst_loss = pnl
            
    # Also check profile stats for largest win if available
    if profile_stats and profile_stats.largest_win:
        if float(profile_stats.largest_win) > largest_win:
            largest_win = float(profile_stats.largest_win)

    # Calculate realized PnL from closed positions
    realized_pnl_total = sum(float(cp.realized_pnl or 0) for cp in closed_positions)
    
    # Calculate investment
    total_investment = sum(float(cp.total_bought or 0) * float(cp.avg_price or 0) for cp in closed_positions)
    if active_positions:
        total_investment += sum(float(p.initial_value or 0) for p in active_positions)

    # ROI Calculation: ((realized_pnl + unrealized_pnl) / total_investment) * 100
    unrealized_pnl = sum(float(p.cash_pnl or 0) for p in active_positions)
    
    # Priority for total_pnl: 
    # 1. Calculated (Realized + Unrealized) - most reliable based on our DB
    # 2. Agg metrics (fallback)
    total_pnl_calculated = realized_pnl_total + unrealized_pnl
    total_pnl = total_pnl_calculated
    
    roi = (total_pnl / total_investment * 100) if total_investment > 0 else 0.0

    # Win Rate from closed positions
    total_closed = len(closed_positions)
    wins = sum(1 for cp in closed_positions if (cp.realized_pnl or 0) > 0)
    win_rate = (wins / total_closed * 100) if total_closed > 0 else 0.0
    
    # Construct Response Objects matching Frontend Expectations
    
    # ProfileStatsResponse
    profile_data = {
        "username": username,
        "trades": profile_stats.trades if profile_stats else len(closed_positions),
        "largestWin": largest_win,
        "views": profile_stats.views if profile_stats else 0,
        "joinDate": profile_stats.join_date if profile_stats and profile_stats.join_date else None,
    }
    
    # UserLeaderboardData
    leaderboard_data = {
        "address": wallet_address,
        "userName": username,
        "profileImage": trader.profile_image if trader else None,
        "vol": total_investment,
        "pnl": total_pnl,
        "rank": 0,
        "verifiedBadge": False,
        "xUsername": None
    }
    
    # PortfolioStats
    portfolio_data = {
        "performance_metrics": {
            "portfolio_value": portfolio_value,
            "total_pnl": total_pnl,
            "realized_pnl": realized_pnl_total,
            "unrealized_pnl": unrealized_pnl,
            "roi": roi,
            "total_investment": total_investment,
            "win_rate": win_rate,
            "worst_loss": worst_loss,
            "max_drawdown": 0.0 # Default value, will be updated if trader_metrics is available
        },
        "positions_summary": {
            "open_positions_count": len(active_positions),
            "closed_positions_count": len(closed_positions)
        }
    }
    
    # TradeHistory (for graph)
    trade_history_data = {
        "trades": [
            {"timestamp": int(p.timestamp), "pnl": float(p.pnl)}
            for p in pnl_history
        ]
    }
    
    if not trade_history_data["trades"] and closed_positions:
         trade_history_data["trades"] = [
            {"timestamp": int(cp.timestamp), "pnl": float(cp.realized_pnl or 0)}
            for cp in closed_positions
         ][:20] # Limit fallback trades for graph performance

    # --- Calculate Advanced Scoring Metrics ---
    scoring_metrics = {}
    try:
        # Calculate raw trader metrics using the same function as leaderboard
        trader_metrics = await calculate_trader_metrics_with_time_filter(
            session, wallet_address, period='all'
        )
        
        if trader_metrics:
            # Get PnL median from population (needed for scoring)
            pnl_median = await get_pnl_median_from_population()
            
            # Calculate scores using the same function as leaderboard
            # We need to pass a list with just this trader, but we need population medians
            traders_list = [trader_metrics]
            scoring_result = calculate_scores_and_rank_with_percentiles(
                traders_list,
                pnl_median=pnl_median
            )
            
            if scoring_result.get("traders") and len(scoring_result["traders"]) > 0:
                scored_trader = scoring_result["traders"][0]
                
                # Calculate Custom Win Score (User Request)
                # W_score = 0.5 * W_trade + 0.5 * W_stake
                
                # W_trade = win_rate_percent / 100
                w_trade = scored_trader.get("win_rate", 0.0) / 100.0
                
                # W_stake = winning_stakes / total_stakes
                total_stakes_val = scored_trader.get("total_stakes", 0.0)
                winning_stakes_val = scored_trader.get("winning_stakes", 0.0)
                w_stake = (winning_stakes_val / total_stakes_val) if total_stakes_val > 0 else 0.0
                
                # Combined Score
                win_score_custom = 0.5 * w_trade + 0.5 * w_stake
                
                # Extract all scoring metrics
                scoring_metrics = {
                    "total_pnl": scored_trader.get("total_pnl", 0.0),
                    "roi": scored_trader.get("roi", 0.0),
                    "win_rate": scored_trader.get("win_rate", 0.0),
                    "win_rate_percent": scored_trader.get("win_rate", 0.0),  # Same as win_rate but as percentage
                    "W_shrunk": scored_trader.get("W_shrunk", 0.0),
                    "roi_shrunk": scored_trader.get("roi_shrunk", 0.0),
                    "pnl_shrunk": scored_trader.get("pnl_shrunk", 0.0),
                    "score_win_rate": scored_trader.get("score_win_rate", 0.0),
                    "score_roi": scored_trader.get("score_roi", 0.0),
                    "score_pnl": scored_trader.get("score_pnl", 0.0),
                    "score_risk": scored_trader.get("score_risk", 0.0),
                    "final_score": scored_trader.get("final_score", 0.0),
                    "total_trades": scored_trader.get("total_trades", 0),
                    "total_trades_with_pnl": scored_trader.get("total_trades_with_pnl", 0),
                    "winning_trades": scored_trader.get("winning_trades", 0),
                    "total_stakes": scored_trader.get("total_stakes", 0.0),
                    "winning_stakes": scored_trader.get("winning_stakes", 0.0),
                    "losing_stakes": scored_trader.get("total_stakes", 0.0) - scored_trader.get("winning_stakes", 0.0),
                    "max_stake": scored_trader.get("max_stake", 0.0),
                    "worst_loss": scored_trader.get("worst_loss", 0.0),
                    "max_drawdown": scored_trader.get("max_drawdown", 0.0),
                    "losing_trades": scored_trader.get("total_trades_with_pnl", 0) - scored_trader.get("winning_trades", 0),
                    # New Custom Metrics
                    "w_trade": round(w_trade, 4),
                    "w_stake": round(w_stake, 4),
                    "win_score_blended": round(win_score_custom, 4),
                    "stake_volatility": scored_trader.get("stake_volatility", 0.0),
                }
                
                # Calculate Confidence Score based on number of trades
                num_predictions = scored_trader.get("total_trades_with_pnl", 0)
                confidence_details = calculate_confidence_with_details(num_predictions)
                
                # Add confidence metrics to scoring_metrics
                scoring_metrics.update({
                    "confidence_score": confidence_details["confidence_score"],
                    "confidence_percent": confidence_details["confidence_percent"],
                    "confidence_level": confidence_details["confidence_level"],
                })
    except Exception as e:
        # If scoring calculation fails, use basic metrics
        import traceback
        print(f"Error calculating scoring metrics: {e}")
        print(traceback.format_exc())
        
        # Fallback calculation if possible
        try:
             # Basic estimates from available local variables if fallback
             w_trade_fallback = win_rate / 100.0 if 'win_rate' in locals() else 0.0
             # We might not have total_stakes available easily here without iterating again, 
             # but we can try from closed_positions if we want a better fallback.
             # For now, keep it simple.
             win_score_fallback = 0.5 * w_trade_fallback # Assuming w_stake is 0 if unknown
        except:
             win_score_fallback = 0.0

        scoring_metrics = {
            "total_pnl": total_pnl,
            "roi": roi,
            "win_rate": win_rate,
            "win_rate_percent": win_rate,
            "total_trades": len(set(cp.condition_id for cp in closed_positions)) + len(set(p.condition_id for p in active_positions)),
            "win_score_blended": 0.0 # Default failure
        }

    # --- Calculate Winning Streaks ---
    longest_streak = 0
    current_streak = 0
    total_wins = 0
    total_losses = 0
    
    try:
        # Sort closed positions by timestamp (oldest first)
        sorted_closed = sorted(closed_positions, key=lambda cp: cp.timestamp)
        
        # We calculate streaks and counts from closed positions
        # This will be consistent with the streaks themselves
        longest_streak_temp = 0
        current_streak_temp = 0
        
        for cp in sorted_closed:
            pnl = float(cp.realized_pnl or 0)
            if pnl > 0:
                # Winning trade
                total_wins += 1
                current_streak_temp += 1
                longest_streak_temp = max(longest_streak_temp, current_streak_temp)
            elif pnl < 0:
                # Losing trade
                total_losses += 1
                longest_streak = max(longest_streak, longest_streak_temp)
                current_streak_temp = 0
                longest_streak_temp = 0
        
        # Final check for longest streak
        longest_streak = max(longest_streak, longest_streak_temp)
        current_streak = current_streak_temp

        # Override with scoring_metrics if available and different (should be rare)
        if scoring_metrics and "winning_trades" in scoring_metrics:
            total_wins = scoring_metrics["winning_trades"]
            total_losses = scoring_metrics.get("losing_trades", total_losses)
    except Exception as e:
        print(f"Error calculating streaks: {e}")
    
    # --- Calculate Rewards Earned ---
    rewards_earned = 0.0
    try:
        for activity in activities:
            if activity.type == "REWARD":
                rewards_earned += float(activity.usdc_size or 0)
    except Exception as e:
        print(f"Error calculating rewards: {e}")
    
    # --- Calculate Total Volume ---
    total_volume = 0.0
    try:
        # From closed positions
        for cp in closed_positions:
            stake = float(cp.total_bought or 0) * float(cp.avg_price or 0)
            total_volume += stake
        
        # From active positions
        for pos in active_positions:
            stake = float(pos.initial_value or 0)
            total_volume += stake
        
        # From trades
        for trade in all_trades:
            stake = float(trade.size or 0) * float(trade.price or 0)
            total_volume += stake
    except Exception as e:
        print(f"Error calculating total volume: {e}")
        # Fallback to aggregated metrics
        total_volume = float(agg_metrics.total_volume) if agg_metrics and agg_metrics.total_volume else total_investment

    # --- Helper function to categorize market ---
    def categorize_market(title: str, slug: str) -> str:
        """Categorize market into Politics, Crypto, Sports, Macro/Rates, or Other."""
        title_lower = (title or "").lower()
        slug_lower = (slug or "").lower()
        combined = f"{title_lower} {slug_lower}"
        
        # Politics keywords
        if any(keyword in combined for keyword in ['president', 'election', 'politics', 'trump', 'biden', 'senate', 'congress', 'vote', 'poll', 'democrat', 'republican', 'political']):
            return "Politics"
        
        # Crypto keywords
        if any(keyword in combined for keyword in ['bitcoin', 'btc', 'ethereum', 'eth', 'crypto', 'cryptocurrency', 'blockchain', 'defi', 'nft', 'token', 'coin']):
            return "Crypto"
        
        # Sports keywords
        if any(keyword in combined for keyword in ['nfl', 'nba', 'mlb', 'soccer', 'football', 'basketball', 'baseball', 'hockey', 'sports', 'game', 'match', 'championship', 'super bowl', 'world cup']):
            return "Sports"
        
        # Macro/Rates keywords
        if any(keyword in combined for keyword in ['fed', 'federal reserve', 'interest rate', 'inflation', 'gdp', 'unemployment', 'macro', 'rates', 'treasury', 'bond', 'economic']):
            return "Macro / Rates"
        
        return "Other"
    
    # --- Calculate Detailed Market Distribution with ROI and Win Rate ---
    market_distribution = []
    primary_edge = "No trading data available."
    market_category_stats = {}  # category -> {capital, roi, win_rate, trades, wins, losses}
    
    try:
        # Process closed positions for market distribution
        for cp in closed_positions:
            market_title = cp.title or "Unknown"
            market_slug = cp.slug or "Unknown"
            category = categorize_market(market_title, market_slug)
            
            # Calculate stake (capital allocation)
            stake = float(cp.total_bought or 0) * float(cp.avg_price or 0)
            pnl = float(cp.realized_pnl or 0)
            
            if category not in market_category_stats:
                market_category_stats[category] = {
                    "capital": 0.0,
                    "total_pnl": 0.0,
                    "wins": 0,
                    "losses": 0,
                    "trades": 0,
                    "markets": set()  # Track unique markets
                }
            
            market_category_stats[category]["capital"] += stake
            market_category_stats[category]["total_pnl"] += pnl
            market_category_stats[category]["trades"] += 1
            market_category_stats[category]["markets"].add(market_slug)
            
            if pnl > 0:
                market_category_stats[category]["wins"] += 1
            elif pnl < 0:
                market_category_stats[category]["losses"] += 1
        
        # Process active positions for capital allocation
        for pos in active_positions:
            market_title = pos.title or "Unknown"
            market_slug = pos.slug or "Unknown"
            category = categorize_market(market_title, market_slug)
            
            capital = float(pos.initial_value or 0)
            
            if category not in market_category_stats:
                market_category_stats[category] = {
                    "capital": 0.0,
                    "total_pnl": 0.0,
                    "wins": 0,
                    "losses": 0,
                    "trades": 0,
                    "markets": set()
                }
            
            market_category_stats[category]["capital"] += capital
            market_category_stats[category]["markets"].add(market_slug)
        
        # Calculate ROI and Win Rate for each category
        total_capital = sum(stats["capital"] for stats in market_category_stats.values())
        
        for category, stats in market_category_stats.items():
            capital = stats["capital"]
            total_pnl = stats["total_pnl"]
            wins = stats["wins"]
            losses = stats["losses"]
            total_trades = stats["trades"]
            
            # Calculate ROI %
            roi_percent = (total_pnl / capital * 100) if capital > 0 else 0.0
            
            # Calculate Win Rate %
            win_rate_percent = (wins / total_trades * 100) if total_trades > 0 else 0.0
            
            # Calculate capital percentage
            capital_percent = (capital / total_capital * 100) if total_capital > 0 else 0.0
            
            # Calculate risk (worst loss / capital)
            # We'll use a simple risk metric based on losses
            risk_score = abs(stats.get("worst_loss", 0)) / capital if capital > 0 else 0.0
            
            market_distribution.append({
                "category": category,
                "market": category,  # For display
                "capital": round(capital, 2),
                "capital_percent": round(capital_percent, 2),
                "roi_percent": round(roi_percent, 2),
                "win_rate_percent": round(win_rate_percent, 2),
                "trades_count": total_trades,
                "wins": wins,
                "losses": losses,
                "total_pnl": round(total_pnl, 2),
                "risk_score": round(risk_score, 4),
                "unique_markets": len(stats["markets"])
            })
        
        # Sort by capital (descending)
        market_distribution.sort(key=lambda x: x["capital"], reverse=True)
        
        # Determine primary edge
        if market_distribution:
            primary_category = market_distribution[0]
            primary_edge = f"Primary edge in {primary_category['category']} markets with "
            if primary_category['roi_percent'] > 0:
                primary_edge += f"{'high' if primary_category['roi_percent'] > 50 else 'consistent'} ROI "
            else:
                primary_edge += "moderate ROI "
            
            if primary_category['risk_score'] < 0.1:
                primary_edge += "and low risk."
            elif primary_category['risk_score'] < 0.3:
                primary_edge += "and moderate risk."
            else:
                primary_edge += "and high risk."
        else:
            primary_edge = "No trading data available."
        
    except Exception as e:
        import traceback
        print(f"Error calculating market distribution: {e}")
        print(traceback.format_exc())
        market_distribution = []
        primary_edge = "Unable to calculate market distribution."

    # --- Calculate Profit Trend (Last 7 Days) ---
    profit_trend = []
    try:
        from datetime import datetime, timedelta
        
        # Get last 7 days
        today = datetime.utcnow()
        days_data = {}
        
        # Initialize all 7 days with 0 profit
        for i in range(7):
            day = today - timedelta(days=6-i)
            day_key = day.strftime("%Y-%m-%d")
            days_data[day_key] = {
                "date": day.strftime("%a"),  # Mon, Tue, etc.
                "full_date": day_key,
                "profit": 0.0
            }
        
        # Aggregate PnL by day from closed positions
        for cp in closed_positions:
            if cp.timestamp:
                # Convert timestamp to date
                trade_date = datetime.fromtimestamp(cp.timestamp)
                day_key = trade_date.strftime("%Y-%m-%d")
                
                # Check if within last 7 days
                if day_key in days_data:
                    days_data[day_key]["profit"] += float(cp.realized_pnl or 0)
        
        # Convert to list and calculate cumulative profit
        cumulative_profit = 0.0
        for day_key in sorted(days_data.keys()):
            day_data = days_data[day_key]
            cumulative_profit += day_data["profit"]
            profit_trend.append({
                "day": day_data["date"],
                "date": day_data["full_date"],
                "profit": round(day_data["profit"], 2),
                "cumulative_profit": round(cumulative_profit, 2)
            })
    except Exception as e:
        import traceback
        print(f"Error calculating profit trend: {e}")
        print(traceback.format_exc())
        profit_trend = []

    # --- Total Number of Trades ---
    total_trades_count = len(all_trades) if all_trades else 0

    return {
        "profile": profile_data,
        "leaderboard": leaderboard_data,
        "portfolio": portfolio_data,
        "positions": [row_to_dict(p) for p in active_positions],
        "closed_positions": [row_to_dict(cp) for cp in closed_positions],
        "activities": [row_to_dict(a) for a in activities],  # All activities included
        "trade_history": trade_history_data,
        "scoring_metrics": scoring_metrics,  # All calculated scoring metrics
        "market_distribution": market_distribution,  # Market distribution with ROI, Win Rate
        "primary_edge": primary_edge,
        "total_trades": total_trades_count,  # Total number of trades
        "streaks": {
            "longest_streak": trader_metrics.get("streaks", {}).get("longest_streak", 0),
            "current_streak": trader_metrics.get("streaks", {}).get("current_streak", 0),
            "total_wins": trader_metrics.get("streaks", {}).get("total_wins", 0),
            "total_losses": trader_metrics.get("streaks", {}).get("total_losses", 0),
        },
        "rewards_earned": trader_metrics.get("rewards_earned", 0.0),
        "total_volume": total_volume,
        "profit_trend": profit_trend,  # Last 7 days profit trend
        "largest_win": trader_metrics.get("largest_win", 0.0),
    }



def _normalize_closed_position(pos: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize closed position data from API for frontend display.
    Ensures fields like size, avgPrice, realizedPnl are correctly populated.
    """
    # Create a copy to avoid mutating original
    normalized = pos.copy()
    
    # 1. Size / Total Bought
    # API v1 uses 'totalBought' for the position size usually
    size = pos.get("size")
    if size is None:
        size = pos.get("totalBought") or pos.get("total_bought") or 0.0
    normalized["size"] = float(size)
    
    # 2. Avg Price
    avg_price = pos.get("avgPrice") or pos.get("avg_price")
    if avg_price is None:
        avg_price = 0.0
    normalized["avgPrice"] = float(avg_price)
    normalized["avg_price"] = float(avg_price)

    # 3. Realized PnL
    pnl = pos.get("realizedPnl") or pos.get("realized_pnl") or pos.get("pnl")
    if pnl is None:
        pnl = 0.0
    normalized["realizedPnl"] = float(pnl)
    normalized["realized_pnl"] = float(pnl)
    
    # 4. Exit Price
    # API might not return exitPrice. We can sometimes derive it?
    # realizedPnl = (exitPrice - avgPrice) * size
    # exitPrice = (realizedPnl / size) + avgPrice  (if size != 0)
    exit_price = pos.get("exitPrice") or pos.get("exit_price")
    if exit_price is None:
        if normalized["size"] > 0:
            exit_price = (normalized["realizedPnl"] / normalized["size"]) + normalized["avgPrice"]
        else:
            exit_price = 0.0
            
    normalized["exitPrice"] = float(exit_price)
    normalized["exit_price"] = float(exit_price)
    
    return normalized


async def get_live_dashboard_data(wallet_address: str) -> Dict[str, Any]:
    """
    Aggregate ALL necessary data for the wallet dashboard by fetching directly from Polymarket APIs.
    Bypasses the local database entirely.
    """
    import asyncio
    from app.services.data_fetcher import (
        fetch_positions_for_wallet,
        fetch_closed_positions,
        fetch_user_activity,
        fetch_user_trades,
        fetch_user_pnl,
        fetch_profile_stats,
        fetch_portfolio_value,
        fetch_leaderboard_stats,
        fetch_user_traded_count,
        fetch_user_profile_data_v2,
        fetch_resolved_markets,
        fetch_market_by_slug,
        get_market_resolution,
        DNSAwareAsyncClient
    )

    # 1. Fetch everything concurrently
    tasks = {
        "positions": fetch_positions_for_wallet(wallet_address), # limit=None by default
        "closed_positions": fetch_closed_positions(wallet_address, limit=None),
        "activities": fetch_user_activity(wallet_address),
        "trades": fetch_user_trades(wallet_address),
        "user_pnl": fetch_user_pnl(wallet_address),
        "profile": fetch_profile_stats(wallet_address),
        "portfolio_value": fetch_portfolio_value(wallet_address),
        "leaderboard": fetch_leaderboard_stats(wallet_address),
        "traded_count": fetch_user_traded_count(wallet_address),
        "profile_v2": fetch_user_profile_data_v2(wallet_address),
        "resolved_markets": fetch_resolved_markets(limit=2000)
    }

    results = await asyncio.gather(*tasks.values(), return_exceptions=True)
    f = dict(zip(tasks.keys(), results))

    # Helper to handle exceptions
    def safe_get(key, default=[]):
        val = f.get(key)
        return val if not isinstance(val, Exception) and val is not None else default

    active_positions = safe_get("positions", [])
    closed_positions = safe_get("closed_positions", [])
    activities = safe_get("activities", [])
    trades_list = safe_get("trades", [])
    user_pnl = safe_get("user_pnl", [])
    profile_stats = safe_get("profile", {})
    portfolio_value = safe_get("portfolio_value", 0.0)
    leaderboard_stats = safe_get("leaderboard", {})
    traded_count = safe_get("traded_count", 0)
    profile_v2 = safe_get("profile_v2", {})
    resolved_markets = safe_get("resolved_markets", [])

    # 1.5. Check for resolved positions in active positions and move them to closed
    actual_active_positions = []
    newly_closed_positions = []
    
    # Execute checks using heuristic (No external API calls)
    # User heuristic: if curPrice is 0 or redeemable is True, the market is resolved.
    for pos in active_positions:
        cur_price = float(pos.get("curPrice") or pos.get("cur_price") or 0)
        is_redeemable = pos.get("redeemable") is True
        
        # Check if market is resolved based on heuristic
        if cur_price == 0 or is_redeemable:
            # Position is resolved! Move to closed
            
            # Determine final price for PnL calculation
            # If curPrice is 0, likely a loss (0.0).
            # If redeemable is True, it could be a win (1.0) or loss (0.0).
            # But usually if it's in "active" list with redeemable=True, it requires action.
            # The user's specific case was curPrice=0 -> Loss.
            
            # We trust the current metric or derive it.
            # If curPrice is 0, we assume final_price is 0.
            # If curPrice is > 0 and redeemable, maybe it's 1.0? 
            # Or we just rely on PnL calc.
            
            final_price = 0.0 if cur_price == 0 else 1.0 
            # Note: This is a simplification. A redeemable YES token might be worth 1.0.
            # If curPrice is 0.99 and redeemable, it's essentially 1.0.
            # However, for the specific "Loss Discrepancy" bug, curPrice was 0.
            
            avg_price = float(pos.get("avgPrice") or pos.get("avg_price") or 0)
            size = float(pos.get("size") or pos.get("totalBought") or pos.get("total_bought") or 0)
            
            realized_pnl = (final_price - avg_price) * size
            
            # Create a closed position object
            cp = pos.copy()
            cp["realizedPnl"] = realized_pnl
            cp["realized_pnl"] = realized_pnl
            cp["curPrice"] = final_price
            cp["cur_price"] = final_price
            cp["resolved"] = True
            
            newly_closed_positions.append(cp)
        else:
            actual_active_positions.append(pos)

    active_positions = actual_active_positions
    closed_positions = newly_closed_positions + closed_positions

    # Username Fallback
    username = profile_stats.get("username") if profile_stats else "Unknown"
    if (not username or username == "Unknown") and activities:
        for a in activities:
            if a.get("name"):
                username = a.get("name")
                break
            if a.get("pseudonym"):
                username = a.get("pseudonym")
                break

    # 2. Advanced Performance Calculations (Shared Logic)
    # This ensures live results match the backend/leaderboard logic exactly.
    trader_metrics = process_trader_data_points(
        wallet_address,
        trades_list,
        active_positions,
        activities,
        closed_positions,
        {"name": username}
    )

    # Calculate Scores using Population Medians for consistency
    pnl_median = await get_pnl_median_from_population()
    scoring_result = calculate_scores_and_rank_with_percentiles(
        [trader_metrics],
        pnl_median=pnl_median
    )
    
    scored_trader = scoring_result["traders"][0] if scoring_result.get("traders") else trader_metrics
    
    # Custom Blended Win Score
    w_trade = scored_trader.get("win_rate", 0.0) / 100.0
    total_stakes_val = scored_trader.get("total_stakes", 0.0)
    winning_stakes_val = scored_trader.get("winning_stakes", 0.0)
    w_stake = (winning_stakes_val / total_stakes_val) if total_stakes_val > 0 else 0.0
    win_score_blended = 0.5 * w_trade + 0.5 * w_stake

    try:
        # Explicitly calculate PnL breakdowns for Live Dashboard
        # Support both camelCase (API) and snake_case (Internal)
        
        if active_positions:
            pass
        if closed_positions:
            pass

        unrealized_pnl = sum(float(p.get("cashPnl") or p.get("cash_pnl") or 0) for p in (active_positions or []))
        realized_pnl = sum(float(cp.get("realizedPnl") or cp.get("realized_pnl") or 0) for cp in (closed_positions or []))
        
        # Calculate Max Stake from active and closed
        max_stake = 0.0
        for p in (active_positions or []):
            # Check initialValue or initial_value
            stake = float(p.get("initialValue") or p.get("initial_value") or 0)
            if stake > max_stake:
                max_stake = stake
        
        for cp in (closed_positions or []):
             # Check for size/avgPrice variants
             size = float(cp.get("totalBought") or cp.get("total_bought") or cp.get("size") or 0)
             price = float(cp.get("avgPrice") or cp.get("avg_price") or 0)
             stake = size * price
             
             if stake > max_stake:
                 max_stake = stake

        # Winning vs Losing Stakes
        winning_stakes = scored_trader.get("winning_stakes", 0.0)
        total_stakes = scored_trader.get("total_stakes", 0.0)
        losing_stakes = total_stakes - winning_stakes
    except Exception as e:
        print(f"Error calculating detailed metrics: {e}")
        import traceback
        print(traceback.format_exc())
        unrealized_pnl = 0.0
        realized_pnl = 0.0
        max_stake = 0.0
        winning_stakes = 0.0
        losing_stakes = 0.0


    # Confidence Score
    num_predictions = scored_trader.get("total_trades_with_pnl", 0)
    confidence_details = calculate_confidence_with_details(num_predictions)

    scoring_metrics = {
        "total_pnl": scored_trader.get("total_pnl", 0.0),
        "roi": scored_trader.get("roi", 0.0),
        "win_rate": scored_trader.get("win_rate", 0.0),
        "win_rate_percent": scored_trader.get("win_rate", 0.0),
        "score_win_rate": scored_trader.get("score_win_rate", 0.0),
        "score_roi": scored_trader.get("score_roi", 0.0),
        "score_pnl": scored_trader.get("score_pnl", 0.0),
        "score_risk": scored_trader.get("score_risk", 0.0),
        "final_score": scored_trader.get("final_score", 0.0),
        "total_trades": scored_trader.get("total_trades", 0),
        "total_trades_with_pnl": scored_trader.get("total_trades_with_pnl", 0),
        "winning_trades": scored_trader.get("winning_trades", 0),
        "losing_trades": scored_trader.get("losing_trades", 0),
        "total_stakes": scored_trader.get("total_stakes", 0.0),
        "winning_stakes": scored_trader.get("winning_stakes", 0.0),
        "worst_loss": scored_trader.get("worst_loss", 0.0),
        "largest_win": trader_metrics.get("largest_win", 0.0),
        "max_drawdown": scored_trader.get("max_drawdown", 0.0),
        "stake_volatility": scored_trader.get("stake_volatility", 0.0),
        "buy_volume": trader_metrics.get("buy_volume", 0.0),
        "sell_volume": trader_metrics.get("sell_volume", 0.0),
        "total_volume": trader_metrics.get("buy_volume", 0.0) + trader_metrics.get("sell_volume", 0.0),
        "confidence_score": confidence_details["confidence_score"],
        "win_score_blended": win_score_blended,
        "streaks": trader_metrics.get("streaks", {
            "longest_streak": 0,
            "current_streak": 0,
            "total_wins": 0,
            "total_losses": 0
        }),
        # Detailed Metrics for Dashboard
        "unrealized_pnl": unrealized_pnl,
        "realized_pnl": realized_pnl,
        "max_stake": max_stake,
        "winning_stakes": winning_stakes,
        "losing_stakes": losing_stakes,
        "w_trade": w_trade,
        "w_stake": w_stake,
        "open_positions": len(active_positions),
        "closed_positions": len(closed_positions),
    }

    # CRITICAL: Override metrics with official values from Polymarket Data API
    # These values are the Source of Truth for the dashboard display.
    official_pnl = leaderboard_stats.get("pnl", scoring_metrics["total_pnl"])
    official_vol = leaderboard_stats.get("volume", scoring_metrics["total_volume"])
    official_rank = leaderboard_stats.get("rank", 0)
    
    scoring_metrics["total_pnl"] = official_pnl
    scoring_metrics["total_volume"] = official_vol
    scoring_metrics["buy_volume"] = official_vol # In v1/leaderboard, 'vol' is the primary volume metric
    scoring_metrics["total_trades"] = traded_count or scoring_metrics["total_trades"]
    
    # Update username and profile image from official userData API
    username = profile_v2.get("name") or profile_v2.get("pseudonym") or username
    profile_image = profile_v2.get("profileImage") or (profile_stats.get("profileImage") if profile_stats else None)

    return {
        "profile": {
            "username": username,
            "trades": scoring_metrics["total_trades"],
            "largestWin": scoring_metrics["largest_win"],
            "views": profile_stats.get("views", 0),
            "joinDate": profile_stats.get("joinDate"),
            "profileImage": profile_image
        },
        "leaderboard": {
            "address": wallet_address,
            "userName": username,
            "vol": scoring_metrics["total_volume"],
            "pnl": scoring_metrics["total_pnl"],
            "rank": official_rank
        },
        "portfolio": {
            "performance_metrics": scoring_metrics,
            "positions_summary": {
                "open_positions_count": len(active_positions),
                "closed_positions_count": len(closed_positions),
                "current_value": portfolio_value
            }
        },
        "scoring_metrics": scoring_metrics,
        "positions": active_positions,
        "closed_positions": [_normalize_closed_position(cp) for cp in closed_positions],
        "activities": activities,
        "trade_history": {
            "trades": user_pnl
        },
        "streaks": scoring_metrics["streaks"],
        "rewards_earned": trader_metrics.get("rewards_earned", 0.0),
        "total_volume": scoring_metrics["total_volume"],
        "portfolio_value": portfolio_value
    }

def row_to_dict(obj):
    """Helper to convert SQLAlchemy model to dict."""
    d = {}
    for column in obj.__table__.columns:
        val = getattr(obj, column.name)
        if isinstance(val, (datetime)):
             val = val.isoformat()
        d[column.name] = val
    return d
