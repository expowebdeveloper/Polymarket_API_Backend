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
    calculate_scores_and_rank_with_percentiles
)
from app.services.pnl_median_service import get_pnl_median_from_population

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

    # Total Investment (Volume) Fallback
    total_investment = float(agg_metrics.total_volume) if agg_metrics and agg_metrics.total_volume else 0.0
    if total_investment == 0:
        # Sum of cost basis for all trades/positions
        # This is a rough estimation of "total volume" if agg_metrics is empty
        total_investment = sum(float(cp.total_bought or 0) for cp in closed_positions) + sum(float(p.initial_value or 0) for p in active_positions)

    # ROI Calculation: ((realized_pnl + unrealized_pnl) / total_investment) * 100
    unrealized_pnl = sum(float(p.cash_pnl or 0) for p in active_positions)
    total_pnl = float(agg_metrics.total_pnl) if agg_metrics and agg_metrics.total_pnl else (realized_pnl_total + unrealized_pnl)
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
            "worst_loss": worst_loss
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
                    "max_stake": scored_trader.get("max_stake", 0.0),
                    "worst_loss": scored_trader.get("worst_loss", 0.0),
                }
    except Exception as e:
        # If scoring calculation fails, use basic metrics
        import traceback
        print(f"Error calculating scoring metrics: {e}")
        print(traceback.format_exc())
        scoring_metrics = {
            "total_pnl": total_pnl,
            "roi": roi,
            "win_rate": win_rate,
            "win_rate_percent": win_rate,
            "total_trades": len(all_trades),
        }

    # --- Calculate Winning Streaks ---
    longest_streak = 0
    current_streak = 0
    total_wins = 0
    total_losses = 0
    
    try:
        # Sort closed positions by timestamp (oldest first)
        sorted_closed = sorted(closed_positions, key=lambda cp: cp.timestamp)
        
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
            "longest_streak": longest_streak,
            "current_streak": current_streak,
            "total_wins": total_wins,
            "total_losses": total_losses,
        },
        "rewards_earned": rewards_earned,
        "total_volume": total_volume,
        "profit_trend": profit_trend,  # Last 7 days profit trend
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
