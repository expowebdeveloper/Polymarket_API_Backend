from typing import Dict, Any, List, Optional
from sqlalchemy.future import select
from sqlalchemy import desc, func
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime
from decimal import Decimal
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
from app.services.data_fetcher import fetch_category_stats

# --- Helper function to categorize market (shared across functions) ---
def categorize_market(title: str, slug: str) -> str:
    """Categorize market into the standard Polymarket categories."""
    title_lower = (title or "").lower()
    slug_lower = (slug or "").lower()
    combined = f"{title_lower} {slug_lower}"
    
    # Elections (check first as it's more specific)
    if any(keyword in combined for keyword in ['election', 'electoral', 'vote', 'voting', 'ballot']):
        return "Elections"
    
    # Politics (check before geopolitics)
    if any(keyword in combined for keyword in ['politics', 'political', 'president', 'trump', 'biden', 'senate', 
                                                'congress', 'democrat', 'republican', 'party', 'campaign']):
        return "Politics"
    
    # Geopolitics
    if any(keyword in combined for keyword in ['geopolitics', 'geopolitical', 'war', 'conflict', 'military', 
                                                'nato', 'alliance', 'diplomacy', 'sanctions']):
        return "Geopolitics"
    
    # Sports
    if any(keyword in combined for keyword in ['sports', 'sport', 'nfl', 'nba', 'mlb', 'soccer', 'football', 
                                                'basketball', 'baseball', 'hockey', 'tennis', 'golf', 'game', 
                                                'match', 'championship', 'super bowl', 'world cup', 'olympics', 
                                                'tournament', 'league']):
        return "Sports"
    
    # Crypto
    if any(keyword in combined for keyword in ['crypto', 'cryptocurrency', 'bitcoin', 'btc', 'ethereum', 'eth', 
                                                'blockchain', 'defi', 'nft', 'token', 'coin', 'altcoin', 
                                                'dogecoin', 'solana', 'cardano']):
        return "Crypto"
    
    # Tech
    if any(keyword in combined for keyword in ['tech', 'technology', 'ai', 'artificial intelligence', 'software', 
                                                'hardware', 'startup', 'silicon valley', 'apple', 'google', 
                                                'microsoft', 'meta', 'amazon', 'tesla', 'nvidia', 'chip', 
                                                'semiconductor']):
        return "Tech"
    
    # Finance
    if any(keyword in combined for keyword in ['finance', 'financial', 'bank', 'banking', 'investment', 'trading', 
                                                'stock', 'market', 'hedge fund', 'private equity', 'venture capital']):
        return "Finance"
    
    # Economy
    if any(keyword in combined for keyword in ['economy', 'economic', 'gdp', 'unemployment', 'inflation', 'recession', 
                                                'growth', 'productivity', 'trade', 'commerce', 'business cycle']):
        return "Economy"
    
    # Earnings
    if any(keyword in combined for keyword in ['earnings', 'revenue', 'profit', 'quarterly', 'q1', 'q2', 'q3', 'q4', 
                                                'eps', 'guidance', 'beat', 'miss']):
        return "Earnings"
    
    # Climate & Science
    if any(keyword in combined for keyword in ['climate', 'environment', 'environmental', 'science', 'scientific', 
                                                'research', 'global warming', 'carbon', 'emissions', 'renewable', 
                                                'solar', 'wind', 'energy', 'green', 'sustainability']):
        return "Climate & Science"
    
    # Culture
    if any(keyword in combined for keyword in ['culture', 'cultural', 'entertainment', 'movie', 'film', 'music', 
                                                'celebrity', 'tv', 'television', 'award', 'oscar', 'grammy', 
                                                'fashion', 'art', 'media']):
        return "Culture"
    
    # World
    if any(keyword in combined for keyword in ['world', 'global', 'international', 'country', 'nation', 
                                                'united nations', 'un', 'eu', 'european union']):
        return "World"
    
    return "Other"

def calculate_market_distribution(active_positions: List[Any], closed_positions: List[Any]) -> tuple[List[Dict[str, Any]], str]:
    """
    Calculate market distribution statistics from active and closed positions.
    Handles both SQLAlchemy objects and Dictionary objects.
    """
    market_distribution = []
    primary_edge = "No trading data available."
    market_category_stats = {}
    
    try:
        # Helper to safely get values from Dict or Object
        def get_val(item, keys, default=None):
            if isinstance(item, dict):
                for k in keys:
                    if k in item: return item[k]
            else:
                for k in keys:
                    if hasattr(item, k): return getattr(item, k)
            return default

        # Process closed positions
        for cp in closed_positions:
            market_title = get_val(cp, ["title"], "Unknown") or "Unknown"
            market_slug = get_val(cp, ["slug", "market_slug"], "Unknown") or "Unknown"
            category = categorize_market(market_title, market_slug)

            # Stake
            total_bought = float(get_val(cp, ["total_bought", "totalBought", "size"], 0) or 0)
            avg_price = float(get_val(cp, ["avg_price", "avgPrice"], 0) or 0)
            stake = total_bought * avg_price
            
            pnl = float(get_val(cp, ["realized_pnl", "realizedPnl"], 0) or 0)

            if category not in market_category_stats:
                market_category_stats[category] = {
                    "capital": 0.0,
                    "total_pnl": 0.0,
                    "wins": 0,
                    "losses": 0,
                    "trades": 0,
                    "markets": set(),
                    "worst_loss": 0.0
                }

            market_category_stats[category]["capital"] += stake
            market_category_stats[category]["total_pnl"] += pnl
            market_category_stats[category]["trades"] += 1
            market_category_stats[category]["markets"].add(market_slug)

            if pnl > 0:
                market_category_stats[category]["wins"] += 1
            elif pnl < 0:
                market_category_stats[category]["losses"] += 1
                if pnl < market_category_stats[category]["worst_loss"]:
                    market_category_stats[category]["worst_loss"] = pnl

        # Process active positions
        for pos in active_positions:
            market_title = get_val(pos, ["title"], "Unknown") or "Unknown"
            market_slug = get_val(pos, ["slug", "market_slug"], "Unknown") or "Unknown"
            category = categorize_market(market_title, market_slug)

            # Calculate capital from Size * Price to be safe (avoid global market volume in initial_value)
            size = float(get_val(pos, ["size", "totalBought", "total_bought"], 0) or 0)
            avg_price = float(get_val(pos, ["avgBuyPrice", "avg_buy_price", "avgPrice", "avg_price"], 0) or 0)
            capital = size * avg_price
            
            # Fallback to initial_value ONLY if calculation yields 0 and initial_value is reasonable (< 1M)
            if capital == 0:
                iv = float(get_val(pos, ["initial_value", "initialValue"], 0) or 0)
                if iv < 1_000_000: # Sanity check: User likely didn't bet $1M+ on a single position
                    capital = iv

            if category not in market_category_stats:
                market_category_stats[category] = {
                    "capital": 0.0,
                    "total_pnl": 0.0,
                    "wins": 0,
                    "losses": 0,
                    "trades": 0,
                    "markets": set(),
                    "worst_loss": 0.0
                }
            
            market_category_stats[category]["capital"] += capital
            market_category_stats[category]["trades"] += 1  # Count active positions as trades too
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
        return [], "Unable to calculate market distribution."

    return market_distribution, primary_edge

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
        # Handle cases where timestamp might be missing or None
        sorted_closed = sorted(closed_positions, key=lambda cp: cp.timestamp or 0)
        
        # Reset counters
        longest_streak = 0
        current_streak = 0
        total_wins = 0
        total_losses = 0
        
        for cp in sorted_closed:
            pnl = float(cp.realized_pnl or 0)
            if pnl > 0:
                # Winning trade
                total_wins += 1
                current_streak += 1
                longest_streak = max(longest_streak, current_streak)
            else:
                # Losing trade (pnl <= 0 counts as loss/streak breaker)
                total_losses += 1
                current_streak = 0

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
    # optimization: Prefer official Leaderboard API volume (fast & accurate).
    # If missing, fallback to summing Closed + Active positions (which are fetched fully).
    # Do NOT rely on 'all_trades' for volume sum anymore, as we now Limit trades to 1000 for speed.
    total_volume = 0.0
    
    if leaderboard_data and leaderboard_data.get("volume", 0) > 0:
        total_volume = float(leaderboard_data.get("volume"))
    else:
        try:
            # Fallback: Sum Closed + Active Positions (Full History)
            for cp in closed_positions:
                stake = float(cp.total_bought or 0) * float(cp.avg_price or 0)
                total_volume += stake
            
            for pos in active_positions:
                stake = float(pos.initial_value or 0)
                total_volume += stake
                
            # Note: We skip 'all_trades' here because it's now truncated (1000 items).
            # Using partial trades list would under-report volume. 
            # Positions data is full history (limit=None), so it's the better fallback.
        except Exception as e:
            print(f"Error calculating total volume fallback: {e}")
            total_volume = float(agg_metrics.total_volume) if agg_metrics and agg_metrics.total_volume else total_investment

    
        # --- Calculate Detailed Market Distribution ---
    # Moved to separate endpoint /dashboard/market-distribution
    market_distribution = []
    primary_edge = "See detailed distribution tab"
    
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

    result_data = {
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

    # Save to Cache
    _DASHBOARD_CACHE[wallet_address] = (time.time(), result_data)
    
    return result_data



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
    
    # 5. Timestamp / Date
    timestamp = pos.get("timestamp") or pos.get("closedAt") or pos.get("updatedAt") or pos.get("time")
    if timestamp:
        # If it's a string (ISO format), try to convert to unix timestamp
        if isinstance(timestamp, str):
             try:
                 # minimalistic approach, assuming simple ISO or similar
                 from dateutil import parser
                 dt = parser.parse(timestamp)
                 timestamp = dt.timestamp()
             except:
                 # If parsing fails, leave as is or set to 0?
                 # Frontend handles string dates in new Date(), so maybe leave as string if not parseable?
                 # But our frontend logic prefers number for * 1000.
                 # Let's try to keep it as number if possible.
                 pass
    
    normalized["timestamp"] = timestamp
    # Also ensure created_at is present as fallback
    if "created_at" not in normalized and timestamp:
        normalized["created_at"] = timestamp

    return normalized


# Simple In-Memory Cache for Dashboard Data
# Format: { wallet_address: (timestamp, data_dict) }
_DASHBOARD_CACHE = {}
CACHE_TTL = 120  # Seconds - Increased from 30 to 120 for better performance

async def get_global_dashboard_stats(session: AsyncSession) -> Dict[str, Any]:
    """
    Get global dashboard statistics.
    Attempts to fetch real-time market data from API for Volume/TVL/OI.
    Falls back to DB for counts (Traders/Trades) as those are expensive to compute via API.
    """
    try:
        from app.services.data_fetcher import fetch_markets
        from app.db.models import TraderLeaderboard, TraderTrade
        from sqlalchemy import func, select, case

        # 1. Fetch Active Markets from API (Limit 1000 to get a good chunk of liquidity)
        # We use a large limit to approximate "Global" stats
        api_markets, pagination = await fetch_markets(status="active", limit=1000)
        
        total_volume = 0.0
        tvl = 0.0
        open_interest = 0.0
        total_markets = pagination.get("total", 0)
        
        for m in api_markets:
            total_volume += float(m.get("volume", 0) or 0)
            tvl += float(m.get("liquidity", 0) or 0)
            open_interest += float(m.get("openInterest", 0) or 0)
            
        # 2. DB Stats for Counts (Traders, Trades)
        # These are historical/scraped and safer to get from DB than trying to count all users via API
        
        # Total Traders
        stmt = select(func.count(TraderLeaderboard.id))
        result = await session.execute(stmt)
        total_traders = result.scalar() or 0
        
        # Total Trades
        stmt = select(
            func.count(TraderTrade.id),
            func.sum(case((TraderTrade.side == 'BUY', 1), else_=0)),
            func.sum(case((TraderTrade.side == 'SELL', 1), else_=0))
        )
        result = await session.execute(stmt)
        trade_stats = result.first()
        
        total_trades = trade_stats[0] or 0
        total_buys = trade_stats[1] or 0
        total_sells = trade_stats[2] or 0
        
        # LP Rewards - Mock or 0
        lp_rewards = 0
        
        return {
            "total_volume": f"${total_volume:,.2f}",
            "tvl": f"${tvl:,.2f}",
            "open_interest": f"${open_interest:,.2f}",
            "markets_volume": f"${total_volume:,.2f}", 
            "total_markets": str(total_markets),
            "total_traders": str(total_traders),
            "lp_rewards": f"${lp_rewards:,.2f}",
            "total_trades": str(total_trades),
            "total_buys": str(total_buys),
            "total_sells": str(total_sells)
        }
        
    except Exception as e:
        import traceback
        print(f"Error fetching global dashboard stats: {e}")
        traceback.print_exc()
        return {
            "total_volume": "Error",
            "tvl": "Error",
            "open_interest": "Error",
            "markets_volume": "Error",
            "total_markets": "0",
            "total_traders": "0",
            "lp_rewards": "0",
            "total_trades": "0",
            "total_buys": "0",
            "total_sells": "0"
        }


async def get_profile_stat_data(wallet_address: str, force_refresh: bool = False, skip_trades: bool = False) -> Dict[str, Any]:
    """
    Aggregate ALL necessary data for the wallet profile stats by fetching directly from Polymarket APIs.
    Bypasses the local database entirely.
    
    Args:
        wallet_address: Wallet address to fetch data for
        force_refresh: Force refresh cache
        skip_trades: Skip fetching trade history (for initial load performance)
    
    Includes a 120-second in-memory cache to prevent API rate limiting and speed up reloads.
    """
    import time
    
    # Check Cache
    if not force_refresh:
        now = time.time()
        if wallet_address in _DASHBOARD_CACHE:
            ts, cached_data = _DASHBOARD_CACHE[wallet_address]
            if now - ts < CACHE_TTL:
                print(f"⚡ [CACHE HIT] Serving dashboard data for {wallet_address} from memory ({round(now - ts, 1)}s old)")
                return cached_data
            else:
                print(f"⌛ [CACHE EXPIRED] Refetching dashboard data for {wallet_address}")
    
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
        fetch_traders_from_leaderboard, # Added this based on the provided snippet context
        fetch_wallet_address_from_profile_page # Added import
    )

    # 1. Fetch everything concurrently with timeout
    tasks = {
        "positions": fetch_positions_for_wallet(wallet_address), # limit=None (Fetch ALL)
        "closed_positions": fetch_closed_positions(wallet_address, limit=None), # limit=None (Fetch ALL)
        "user_pnl": fetch_user_pnl(wallet_address),
        "profile": fetch_profile_stats(wallet_address),
        "portfolio_value": fetch_portfolio_value(wallet_address),
        "leaderboard": fetch_leaderboard_stats(wallet_address, order_by="VOL"),
        "leaderboard_pnl": fetch_leaderboard_stats(wallet_address, order_by="PNL"),
        "traded_count": fetch_user_traded_count(wallet_address),
        "profile_v2": fetch_user_profile_data_v2(wallet_address)
    }
    



    import time
    t0 = time.time()
    # Add overall timeout of 45 seconds for all API calls
    try:
        results = await asyncio.wait_for(
            asyncio.gather(*tasks.values(), return_exceptions=True),
            timeout=45.0
        )
    except asyncio.TimeoutError:
        print(f"⚠️ [TIMEOUT] Dashboard fetch exceeded 45s timeout for {wallet_address}")
        # Return partial results with defaults
        results = [Exception("Timeout")] * len(tasks)
    t1 = time.time()
    print(f"⏱️ [TIMING] Dashboard parallel fetch took {round(t1 - t0, 3)}s")
    
    # Log individual task times if possible (future improvement) or just inspect total
    
    f = dict(zip(tasks.keys(), results))
    
    # Check for exceptions and handle timeouts
    for key, res in f.items():
        if isinstance(res, Exception):
            print(f"⚠️ Error fetching {key}: {res}")
            # Set default empty values for failed fetches
            if key == "positions":
                f[key] = []
            elif key == "closed_positions":
                f[key] = []
            elif key == "trades":
                f[key] = []
            elif key == "activities":
                f[key] = []
            elif key == "user_pnl":
                f[key] = []
            elif key in ["profile", "leaderboard", "leaderboard_pnl", "profile_v2"]:
                f[key] = {}
            elif key in ["portfolio_value", "traded_count"]:
                f[key] = 0
            
    # Optimization: Reconstruct "activities" for Trade History using "trades" data
    # This avoids the heavy fetch_user_activity call while keeping the Trade History tab functional.
    # The "Activity" tab will only show Trades (no rewards/redeems) which is acceptable for speed.
    activities_from_trades = []
    trades_data = f.get("trades", []) or []
    
    for t in trades_data:
        # Map trade object to activity schema expected by frontend
        # Schema: type, title, side, size, price, timestamp, transactionHash
        act = {
            "type": "TRADE",
            "title": t.get("title") or t.get("market_slug") or "Market",
            "slug": t.get("market_slug"),
            "side": t.get("side"),
            "size": t.get("size"),
            "usdcSize": float(t.get("size") or 0) * float(t.get("price") or 0), # Approx value
            "usdc_size": float(t.get("size") or 0) * float(t.get("price") or 0),
            "price": t.get("price"),
            "timestamp": t.get("timestamp"),
            "transactionHash": t.get("match_id") or t.get("transactionHash") or "", 
            "transaction_hash": t.get("match_id") or t.get("transactionHash") or "",
            "asset": t.get("asset"),
            "outcome": t.get("outcome")
        }
        activities_from_trades.append(act)
        
    f["activities"] = activities_from_trades

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
    leaderboard_pnl_stats = safe_get("leaderboard_pnl", {})
    traded_count = safe_get("traded_count", 0)
    profile_v2 = safe_get("profile_v2", {})

    # 1.5. Check for resolved positions in active positions and move them to closed
    actual_active_positions = []
    newly_closed_positions = []
    
    # Execute checks using heuristic (No external API calls)
    # User heuristic: if curPrice is 0 or currentValue is 0, the market is resolved/closed.
    for pos in active_positions:
        # Normalize keys (fill snake_case from camelCase)
        pos["avg_price"] = float(pos.get("avgPrice") or pos.get("avg_price") or 0)
        pos["cur_price"] = float(pos.get("curPrice") or pos.get("cur_price") or 0)
        pos["initial_value"] = float(pos.get("initialValue") or pos.get("initial_value") or 0)
        pos["current_value"] = float(pos.get("currentValue") or pos.get("current_value") or 0)
        pos["cash_pnl"] = float(pos.get("cashPnl") or pos.get("cash_pnl") or 0)
        pos["percent_pnl"] = float(pos.get("percentPnl") or pos.get("percent_pnl") or 0)
        pos["realized_pnl"] = float(pos.get("realizedPnl") or pos.get("realized_pnl") or 0)
        pos["total_bought"] = float(pos.get("totalBought") or pos.get("total_bought") or 0)
        
        cur_price = pos["cur_price"]
        current_value = pos["current_value"]
        
        # Check if market is resolved based on heuristic
        # If curPrice is 0 (or currentValue is 0), then market is closed
        if cur_price == 0 or current_value == 0:
            # Position is resolved! Move to closed
            
            # Use current metrics for final PnL
            avg_price = pos["avg_price"]
            size = float(pos.get("size") or pos["total_bought"] or 0)
            
            # If curPrice is 0, we assume the final price/payout is 0 (Loss)
            final_price = 0.0 
            
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

    # CRITICAL: Override trade counts with official Source of Truth (API)
    # This prevents artificially low Confidence Scores when skip_trades=True or when fetching is limited.
    # The Confidence Score formula depends on 'total_trades_with_pnl', so we assume the full count 
    # consists of valid trades (stats approximation).
    official_trade_count = traded_count or (profile_stats.get("trades", 0) if profile_stats else 0)
    if official_trade_count > 0:
        # Update metrics to reflect full history size
        trader_metrics["total_trades_with_pnl"] = official_trade_count
        trader_metrics["total_trades"] = official_trade_count

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
        "volume_rank": leaderboard_stats.get("rank"),
        "pnl_rank": leaderboard_pnl_stats.get("rank"),
        "winning_stakes": winning_stakes,
        "losing_stakes": losing_stakes,
        "w_trade": w_trade,
        "w_stake": w_stake,
        "stake_weighted_win_rate": w_stake * 100.0,  # Convert to percentage
        "open_positions": len(active_positions),
        "closed_positions": len(closed_positions),
    }

    # CRITICAL: Override metrics with official values from Polymarket Data API
    # These values are the Source of Truth for the dashboard display.
    # ONLY override if we have a valid official rank (meaning the user exists in leaderboard)
    # or if we have non-zero official values (rare case where rank might be missing but data exists)
    official_rank = leaderboard_stats.get("rank", 0)
    official_pnl = leaderboard_stats.get("pnl", 0.0)
    official_vol = leaderboard_stats.get("volume", 0.0)
    
    # Check if we should use official stats
    # Cases:
    # 1. User has a rank > 0
    # 2. User has significant volume/pnl recorded in API even without rank
    use_official_stats = (official_rank and official_rank > 0) or (abs(official_vol) > 0)
    
    if use_official_stats:
        scoring_metrics["total_pnl"] = official_pnl
        scoring_metrics["total_volume"] = official_vol
        scoring_metrics["buy_volume"] = official_vol # In v1/leaderboard, 'vol' is the primary volume metric
    else:
        # Keep local calculations if official stats are missing/empty
        pass

    scoring_metrics["total_trades"] = traded_count or scoring_metrics["total_trades"]
    scoring_metrics["total_trades"] = traded_count or scoring_metrics["total_trades"]
    
    # Update username and profile image from official userData API
    username = profile_v2.get("name") or profile_v2.get("pseudonym") or username
    profile_image = profile_v2.get("profileImage") or (profile_stats.get("profileImage") if profile_stats else None)

    # Categorize positions using our fast categorization function (no API calls needed)
    # This is much faster than fetching market data for each position
    for pos in active_positions:
        title = pos.get("title") or ""
        slug = pos.get("slug") or pos.get("market_slug") or ""
        pos["category"] = categorize_market(title, slug)
    
    for pos in closed_positions:
        title = pos.get("title") or ""
        slug = pos.get("slug") or pos.get("market_slug") or ""
        pos["category"] = categorize_market(title, slug)

    # Calculate market distribution for Live Dashboard logic
    market_distribution, primary_edge = calculate_market_distribution(active_positions, closed_positions)

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
        "portfolio_value": portfolio_value,
        "market_distribution": market_distribution,
        "primary_edge": primary_edge
    }

async def search_user_by_name(session: AsyncSession, query: str) -> Optional[Dict[str, Any]]:
    """
    Search for a user by name or pseudonym in the database.
    Query is matched against TraderLeaderboard and Trader tables.
    Returns a dict with wallet_address and name/pseudonym, or None if not found.
    """
    from app.db.models import TraderLeaderboard, Trader
    from sqlalchemy import select, or_
    
    # Clean query
    search_term = query.strip()
    if not search_term:
        return None
        
    # Remove @ if present
    if search_term.startswith("@"):
        search_term = search_term[1:]
        
    pattern = f"%{search_term}%"
    
    # 1. Search in TraderLeaderboard (Highest Priority)
    # Check userName and xUsername
    stmt = select(TraderLeaderboard).where(
        or_(
            TraderLeaderboard.name.ilike(pattern),
            TraderLeaderboard.pseudonym.ilike(pattern),
            TraderLeaderboard.wallet_address.ilike(pattern)
        )
    ).limit(1)
    
    result = await session.execute(stmt)
    leaderboard_entry = result.scalars().first()
    
    if leaderboard_entry:
        return {
            "wallet_address": leaderboard_entry.wallet_address,
            "name": leaderboard_entry.name,
            "pseudonym": leaderboard_entry.pseudonym,
            "profile_image": leaderboard_entry.profile_image
        }
        
    # 2. Search in Trader table (Fallback)
    stmt = select(Trader).where(
        or_(
            Trader.name.ilike(pattern),
            Trader.pseudonym.ilike(pattern)
        )
    ).limit(1)
    
    result = await session.execute(stmt)
    trader_entry = result.scalars().first()
    
    if trader_entry:
        return {
            "wallet_address": trader_entry.wallet_address,
            "name": trader_entry.name,
            "pseudonym": trader_entry.pseudonym,
            "profile_image": trader_entry.profile_image
        }
    
    
    # 3. Fallback: Search in Remote Leaderboard via API
    # If local DB is empty or user not found, try the live API (top 100)
    try:
        from app.services.data_fetcher import (
            fetch_traders_from_leaderboard,
            fetch_wallet_address_from_profile_page
        )
        
        # Check top 100 traders
        traders, _ = await fetch_traders_from_leaderboard(limit=100, time_period="all")
        
        query_lower = search_term.lower()
        for t in traders:
            # Check username
            u_name = t.get("userName") or ""
            if u_name.lower() == query_lower:
                return {
                    "wallet_address": t.get("wallet_address"),
                    "name": t.get("userName"),
                    "pseudonym": t.get("xUsername"),
                    "profile_image": t.get("profileImage"),
                    "user_id": None
                }
            
            # Check pseudonym (xUsername)
            x_name = t.get("xUsername") or ""
            if x_name.lower() == query_lower:
                return {
                    "wallet_address": t.get("wallet_address"),
                    "name": t.get("userName"), 
                    "pseudonym": t.get("xUsername"),
                    "profile_image": t.get("profileImage"),
                    "user_id": None
                }
                
    except Exception as e:
        print(f"Error in fallback search API: {e}")

    # 4. Final Fallback: Scraping Profile Page
    # If not in top 100, try direct profile lookup (e.g. for users like BetOnHope)
    try:
        print(f"Checking profile page fallback for: {search_term}")
        scraped_address = await fetch_wallet_address_from_profile_page(search_term)
        if scraped_address:
            return {
                "wallet_address": scraped_address,
                "name": search_term, # Best effort name
                "pseudonym": None,
                "profile_image": None,
                "user_id": None
            }
    except Exception as e:
        print(f"Error in final fallback scraping: {e}")

    return None

async def enrich_positions_with_categories(positions: List[Dict], closed_positions: List[Dict]) -> None:
    """
    Enrich positions and closed positions with actual Polymarket categories from market tags.
    Modifies the position dictionaries in-place by adding a 'category' field.
    
    Args:
        positions: List of active position dictionaries
        closed_positions: List of closed position dictionaries
    """
    from app.services.data_fetcher import fetch_market_by_slug, get_market_category
    from app.core.constants import DEFAULT_CATEGORY
    import asyncio
    
    # Collect all unique slugs from both active and closed positions
    slugs_to_fetch = set()
    
    for pos in positions:
        slug = pos.get("slug") or pos.get("market_slug")
        if slug:
            slugs_to_fetch.add(slug)
    
    for pos in closed_positions:
        slug = pos.get("slug") or pos.get("market_slug")
        if slug:
            slugs_to_fetch.add(slug)
    
    if not slugs_to_fetch:
        # No slugs to fetch, set all to default category
        for pos in positions:
            pos["category"] = DEFAULT_CATEGORY
        for pos in closed_positions:
            pos["category"] = DEFAULT_CATEGORY
        return
    
    # Fetch all market data in parallel
    print(f"🔍 Fetching market data for {len(slugs_to_fetch)} unique markets to extract categories...")
    
    async def fetch_with_slug(slug: str):
        try:
            market = await fetch_market_by_slug(slug)
            if market:
                category = get_market_category(market)
                return (slug, category)
        except Exception as e:
            print(f"Error fetching market {slug}: {e}")
        return (slug, DEFAULT_CATEGORY)
    
    # Fetch all markets concurrently with semaphore to limit concurrent requests
    # Limit to 10 concurrent requests to avoid rate limiting
    semaphore = asyncio.Semaphore(10)
    
    async def fetch_with_slug_limited(slug: str):
        async with semaphore:
            return await fetch_with_slug(slug)
    
    tasks = [fetch_with_slug_limited(slug) for slug in slugs_to_fetch]
    
    # Add timeout to prevent hanging if there are many markets
    try:
        results = await asyncio.wait_for(
            asyncio.gather(*tasks, return_exceptions=True),
            timeout=15.0  # 15 second timeout for category enrichment
        )
    except asyncio.TimeoutError:
        print(f"⚠️ [TIMEOUT] Category enrichment exceeded 15s timeout for {len(slugs_to_fetch)} markets")
        # Return default category for all
        results = [(slug, DEFAULT_CATEGORY) for slug in slugs_to_fetch]
    
    # Build slug -> category mapping
    slug_to_category = {}
    for result in results:
        if isinstance(result, tuple) and len(result) == 2:
            slug, category = result
            slug_to_category[slug] = category
        elif isinstance(result, Exception):
            print(f"Exception during market fetch: {result}")
    
    print(f"✓ Fetched categories for {len(slug_to_category)} markets")
    
    # Enrich active positions
    for pos in positions:
        slug = pos.get("slug") or pos.get("market_slug")
        pos["category"] = slug_to_category.get(slug, DEFAULT_CATEGORY) if slug else DEFAULT_CATEGORY
    
    # Enrich closed positions
    for pos in closed_positions:
        slug = pos.get("slug") or pos.get("market_slug")
        pos["category"] = slug_to_category.get(slug, DEFAULT_CATEGORY) if slug else DEFAULT_CATEGORY


def row_to_dict(row) -> Dict[str, Any]:
    """Convert SQLAlchemy row to dictionary."""
    if hasattr(row, '__dict__'):
        d = {k: v for k, v in row.__dict__.items() if not k.startswith('_')}
        # Convert Decimal to float for JSON serialization
        for key, value in d.items():
            if isinstance(value, Decimal):
                d[key] = float(value)
        return d
    return {}


async def get_market_distribution_api(wallet_address: str) -> Dict[str, Any]:
    """
    Fetch market distribution stats directly from Polymarket Leaderboard API (Parallel).
    """
    target_categories = ["politics", "sports", "crypto", "finance", "culture", "mentions", "weather", "economics", "tech"]
    api_stats_map = await fetch_category_stats(wallet_address, target_categories)
    
    market_distribution = []
    total_capital_api = sum(s["volume"] for s in api_stats_map.values())
    
    for cat, stats in api_stats_map.items():
        vol = stats["volume"]
        pnl = stats["pnl"]
        
        # Approximate metrics
        roi = (pnl / vol * 100) if vol > 0 else 0.0
        capital_percent = (vol / total_capital_api * 100) if total_capital_api > 0 else 0.0
        
        market_distribution.append({
            "category": cat.title(),
            "market": cat.title(),
            "capital": round(vol, 2),
            "capital_percent": round(capital_percent, 2),
            "roi_percent": round(roi, 2),
            "win_rate_percent": 0.0,
            "trades_count": 0,
            "wins": 0,
            "losses": 0,
            "total_pnl": round(pnl, 2),
            "risk_score": 0.0,
            "unique_markets": 0
        })
        
    market_distribution.sort(key=lambda x: x["capital"], reverse=True)
    
    return {"market_distribution": market_distribution}
