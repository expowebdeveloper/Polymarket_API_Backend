"""
Trader service for extracting and managing trader data.
"""

from typing import List, Dict, Set, Optional
from collections import defaultdict
from datetime import datetime

from app.services.data_fetcher import (
    fetch_resolved_markets,
    fetch_trades_for_wallet,
    fetch_traders_from_leaderboard,
    get_market_by_id
)
from app.services.scoring_engine import calculate_metrics
from app.db.session import AsyncSessionLocal
from app.db.models import Trader, AggregatedMetrics, Trade
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from sqlalchemy.exc import IntegrityError

# Simple in-memory cache to store orders by wallet address
# This avoids re-fetching when we already have the data from market extraction
_trader_orders_cache: Dict[str, List[Dict]] = {}


def extract_traders_from_markets(markets: List[Dict], limit: Optional[int] = None) -> List[str]:
    """
    Extract unique wallet addresses from markets by fetching trades.
    Returns a list of unique trader wallet addresses.
    
    Args:
        markets: List of market dictionaries
        limit: Maximum number of traders to extract (for testing)
    
    Returns:
        List of unique wallet addresses
    """
    traders: Set[str] = set()
    
    # Use a seed list of known traders for faster response
    # In production, you might want to maintain a database of traders
    from app.core.config import settings
    
    # Seed list - you can add more known traders here
    # These are example addresses - in production, maintain a database
    seed_traders = [
        settings.TARGET_WALLET,
        # Add more known active traders here if you have them
        # "0x...",  # Example trader 1
        # "0x...",  # Example trader 2
    ]
    
    # Filter out invalid addresses
    seed_traders = [t for t in seed_traders if t and t.startswith("0x") and len(t) == 42]
    
    # Add seed traders
    for trader in seed_traders:
        if trader and trader.startswith("0x") and len(trader) == 42:
            traders.add(trader)
            if limit and len(traders) >= limit:
                break
    
    # First, try to extract traders from market data itself (if available)
    # Some markets might have creator/owner fields
    for market in markets:
        if limit and len(traders) >= limit:
            break
        
        # Check for creator/owner fields in market data
        for field in ["creator", "owner", "author", "user", "trader"]:
            address = market.get(field) or market.get(f"{field}_address") or market.get(f"{field}Address")
            if address and isinstance(address, str) and address.startswith("0x") and len(address) == 42:
                traders.add(address)
                if limit and len(traders) >= limit:
                    break
    
    # If we need more traders, we previously tried to extract from Dome.
    # Now limiting to only Polymarket data, so we stop here.
    if (not limit) or len(traders) < limit:
        print(f"Extraction stopped. Found {len(traders)} traders from available markets.")
    
    trader_list = list(traders)
    
    # If we don't have enough traders and we have a limit, generate some sample traders for testing
    if limit and len(trader_list) < limit:
        needed = limit - len(trader_list)
        print(f"Only found {len(trader_list)} traders. Generating {needed} sample traders for testing...")
        
        # Generate sample trader addresses (for testing purposes)
        # In production, you'd want to maintain a database of known traders
        import random
        import string
        
        for i in range(needed):
            # Generate a valid-looking Ethereum address (for testing)
            # Format: 0x + 40 hex characters
            hex_chars = '0123456789abcdef'
            address = '0x' + ''.join(random.choice(hex_chars) for _ in range(40))
            trader_list.append(address)
        
        print(f"Generated {needed} sample traders. Total: {len(trader_list)}")
    
    print(f"Extracted {len(trader_list)} unique traders")
    return trader_list


async def get_trader_basic_info(wallet_address: str, markets: List[Dict]) -> Dict:
    """
    Get basic information about a trader without full analytics (async version).
    Faster than full analytics calculation.
    """
    trades = await fetch_trades_for_wallet(wallet_address)
    
    if not trades:
        return {
            "wallet_address": wallet_address,
            "total_trades": 0,
            "total_positions": 0,
            "first_trade_date": None,
            "last_trade_date": None
        }
    
    # Extract unique positions
    positions = set()
    timestamps = []
    
    for trade in trades:
        # Extract market identifier - handle Dome order field variations
        market_id = (
            trade.get("market_id") or 
            trade.get("market") or 
            trade.get("marketId") or
            trade.get("market_slug") or
            trade.get("marketSlug") or
            trade.get("slug")
        )
        if market_id:
            positions.add(market_id)
        
        # Extract timestamp
        timestamp_str = (
            trade.get("timestamp") or 
            trade.get("createdAt") or 
            trade.get("created_at") or 
            trade.get("time")
        )
        if timestamp_str:
            try:
                if isinstance(timestamp_str, (int, float)):
                    timestamps.append(datetime.fromtimestamp(timestamp_str))
                else:
                    ts_str = str(timestamp_str).replace("Z", "+00:00")
                    timestamps.append(datetime.fromisoformat(ts_str))
            except:
                pass
    
    first_trade = min(timestamps) if timestamps else None
    last_trade = max(timestamps) if timestamps else None
    
    return {
        "wallet_address": wallet_address,
        "total_trades": len(trades),
        "total_positions": len(positions),
        "first_trade_date": first_trade.isoformat() if first_trade else None,
        "last_trade_date": last_trade.isoformat() if last_trade else None
    }


async def get_trader_detail(wallet_address: str) -> Dict:
    """
    Get detailed trader information including full analytics.
    """
    # Fetch trades first to know which markets we need
    trades = await fetch_trades_for_wallet(wallet_address)
    
    # Extract unique market slugs from trades
    market_slugs_from_trades = set()
    for trade in trades:
        market_id = (
            trade.get("market_id") or 
            trade.get("market") or 
            trade.get("marketId") or
            trade.get("market_slug") or
            trade.get("marketSlug") or
            trade.get("slug")
        )
        if market_id:
            market_slugs_from_trades.add(market_id)
    
    # Fetch resolved markets (these have resolution data)
    markets = await fetch_resolved_markets(limit=200)  # Fetch more markets for better matching
    
    # For markets in trades that aren't in resolved markets, try to fetch from Dome
    # This helps get resolution data for markets that might be resolved but not in our list
    from app.services.data_fetcher import fetch_market_by_slug_from_dome
    markets_found = {m.get("slug") or m.get("market_id") or m.get("id"): m for m in markets}
    
    # Try to fetch missing markets from Dome (limit to avoid too many API calls)
    for slug in list(market_slugs_from_trades)[:10]:  # Limit to 10 to avoid rate limits
        if slug not in markets_found:
            dome_market = fetch_market_by_slug_from_dome(slug)
            if dome_market:
                markets.append(dome_market)
                markets_found[slug] = dome_market
    
    metrics = await calculate_metrics(wallet_address, trades, markets)

    # Derive total trades from raw trades list so it reflects what Dome returned
    total_trades = len(trades) if trades else 0
    
    # Add trade date information
    timestamps = []
    for trade in trades or []:
        timestamp_str = (
            trade.get("timestamp")
            or trade.get("createdAt")
            or trade.get("created_at")
            or trade.get("time")
        )
        if timestamp_str:
            try:
                if isinstance(timestamp_str, (int, float)):
                    timestamps.append(datetime.fromtimestamp(timestamp_str))
                else:
                    ts_str = str(timestamp_str).replace("Z", "+00:00")
                    timestamps.append(datetime.fromisoformat(ts_str))
            except Exception:
                # Ignore malformed timestamps, they just won't affect first/last trade dates
                pass
    
    first_trade = min(timestamps) if timestamps else None
    last_trade = max(timestamps) if timestamps else None
    
    # Shape the response to match TraderDetail schema exactly
    return {
        "wallet_address": wallet_address,
        "total_trades": total_trades,
        "total_positions": metrics.get("total_positions", 0),
        "active_positions": metrics.get("active_positions", 0),
        "total_wins": metrics.get("total_wins", 0.0),
        "total_losses": metrics.get("total_losses", 0.0),
        "win_rate_percent": metrics.get("win_rate_percent", 0.0),
        "pnl": metrics.get("pnl", 0.0),
        "final_score": metrics.get("final_score", 0.0),
        "first_trade_date": first_trade.isoformat() if first_trade else None,
        "last_trade_date": last_trade.isoformat() if last_trade else None,
        "categories": metrics.get("categories", {}),
    }


async def get_traders_list(limit: int = 50) -> List[Dict]:
    """
    Get a list of traders with basic information (async version with concurrent fetching).
    Extracts traders from markets.
    
    Args:
        limit: Maximum number of traders to return
    
    Returns:
        List of trader dictionaries with basic info
    """
    import asyncio
    
    markets = await fetch_resolved_markets()
    
    # Extract traders - try to get more than the limit to account for traders with no data
    # We'll request 1.5x the limit to ensure we have enough after filtering
    extraction_limit = int(limit * 1.5) if limit else None
    trader_addresses = extract_traders_from_markets(markets, limit=extraction_limit)
    
    if not trader_addresses:
        print("⚠ No traders found. Returning empty list.")
        return []
    
    print(f"Found {len(trader_addresses)} unique trader addresses. Getting basic info...")
    
    # Helper function to fetch a single trader with timeout
    async def fetch_single_trader(wallet: str) -> Dict:
        """Fetch trader info with timeout and error handling."""
        try:
            # Set timeout for individual trader fetch
            return await asyncio.wait_for(
                get_trader_basic_info(wallet, markets),
                timeout=10.0  # 10 second timeout per trader
            )
        except asyncio.TimeoutError:
            print(f"  Timeout fetching trader {wallet[:20]}...")
            return {
                "wallet_address": wallet,
                "total_trades": 0,
                "total_positions": 0,
                "first_trade_date": None,
                "last_trade_date": None
            }
        except Exception as e:
            print(f"  Error fetching trader {wallet[:20]}...: {str(e)[:50]}")
            return {
                "wallet_address": wallet,
                "total_trades": 0,
                "total_positions": 0,
                "first_trade_date": None,
                "last_trade_date": None
            }
    
    # Fetch traders in batches to avoid overwhelming the API
    batch_size = 10  # Fetch 10 traders concurrently at a time
    traders_info = []
    traders_with_trades = 0
    traders_without_trades = 0
    
    for i in range(0, len(trader_addresses), batch_size):
        if limit and len(traders_info) >= limit:
            break
        
        batch = trader_addresses[i:i + batch_size]
        # Only fetch up to the limit
        if limit:
            remaining = limit - len(traders_info)
            batch = batch[:remaining]
        
        print(f"Fetching batch {i//batch_size + 1} ({len(batch)} traders)...")
        
        # Fetch batch concurrently
        batch_results = await asyncio.gather(*[fetch_single_trader(wallet) for wallet in batch])
        
        # Add results to traders_info
        for info in batch_results:
            traders_info.append(info)
            if info.get("total_trades", 0) > 0:
                traders_with_trades += 1
            else:
                traders_without_trades += 1
    
    print(f"✓ Successfully retrieved info for {len(traders_info)} traders")
    print(f"  - {traders_with_trades} with trades, {traders_without_trades} without trades (API may have failed)")
    return traders_info

async def sync_traders_to_db(limit: int = 50) -> Dict[str, int]:
    """
    Fetch traders from Leaderboard API and comprehensively sync ALL data to the database.
    
    Args:
        limit: Number of traders to fetch and sync. Use 0 or -1 for 'all available'.
        
    Returns:
        Dict with stats (added, updated, failed)
    """
    from app.services.sync_service import sync_trader_full_data
    import asyncio
    
    stats = {
        "processed": 0, 
        "failed": 0, 
        "details": defaultdict(int) 
    }
    
    semaphore = asyncio.Semaphore(10) # Limit concurrent syncs
    
    async def sync_single_trader(i: int, trader_info: Dict, total: int):
        wallet = trader_info.get("wallet_address")
        if not wallet:
            return None
        
        async with semaphore:
            if (i + 1) % 10 == 0 or i == 0 or i == total - 1:
                print(f"[{i+1}/{total}] Syncing full data for {wallet}...")
            try:
                metadata = {
                    "name": trader_info.get("userName") or trader_info.get("name"),
                    "profile_image": trader_info.get("profileImage") or trader_info.get("image")
                }
                return await sync_trader_full_data(wallet, trader_metadata=metadata)
            except Exception as e:
                print(f"Error syncing trader {wallet}: {e}")
                return None

    try:
        # Phase 1: Discovery
        all_traders_data = []
        current_offset = 0
        fetch_batch_size = 50 
        sync_all = limit <= 0 or limit > 5000 # Treat 0 or very high as sync all
        max_limit = limit if not sync_all else 10000 
        
        print(f"📡 Discovery Phase: Fetching top traders (Target: {'All' if sync_all else limit})...")
        
        while len(all_traders_data) < max_limit:
            batch_limit = min(fetch_batch_size, max_limit - len(all_traders_data))
            traders_batch, pagination = await fetch_traders_from_leaderboard(
                limit=batch_limit,
                offset=current_offset,
                time_period="all",
                order_by="VOL"
            )
            
            if not traders_batch:
                break
                
            all_traders_data.extend(traders_batch)
            if not pagination.get("has_more"):
                break
                
            current_offset += len(traders_batch)
            print(f"   Fetched {len(all_traders_data)} traders so far...")

        if not all_traders_data:
            print("⚠ No traders fetched from Leaderboard API to sync.")
            return stats
            
        print(f"✅ Discovery complete. Found {len(all_traders_data)} traders. Starting Sync Phase...")
        
        # Phase 2: Concurrent Sync
        tasks = [sync_single_trader(i, t, len(all_traders_data)) for i, t in enumerate(all_traders_data)]
        results = await asyncio.gather(*tasks)
        
        # Aggregate results
        for res in results:
            if res:
                stats["processed"] += 1
                for k, v in res.items():
                    if isinstance(v, int):
                        stats["details"][k] += v
            else:
                stats["failed"] += 1
            
    except Exception as e:
        print(f"Error in sync_traders_to_db: {e}")
        import traceback
        traceback.print_exc()
        
    print(f"🏁 Full Sync complete: {stats}")
    stats["details"] = dict(stats["details"])
    return stats


async def get_traders_from_db(limit: int = 50, offset: int = 0) -> List[Dict]:
    """
    Fetch traders from the database with pagination.
    Returns a list of dictionaries matching the api response format.
    """
    async with AsyncSessionLocal() as session:
        # Query traders with their metrics
        stmt = (
            select(Trader)
            .options(selectinload(Trader.aggregated_metrics))
            .limit(limit)
            .offset(offset)
            .order_by(Trader.updated_at.desc()) # Or order by volume if joined
        )
        
        result = await session.execute(stmt)
        traders = result.scalars().all()
        
        trader_list = []
        for trader in traders:
            # Get metrics if available
            metrics = trader.aggregated_metrics[0] if trader.aggregated_metrics else None
            
            # Map DB model to dict
            trader_dict = {
                "wallet_address": trader.wallet_address,
                "userName": trader.name,
                "profileImage": trader.profile_image,
                # Metrics
                "vol": float(metrics.total_volume) if metrics else 0.0,
                "pnl": float(metrics.total_pnl) if metrics else 0.0,
                "winRate": float(metrics.win_rate) if metrics else 0.0,
                "totalTrades": metrics.total_trades if metrics else 0,
                
                # Required fields for TraderBasicInfo
                "total_trades": metrics.total_trades if metrics else 0,
                "total_positions": 0, # Not currently synced from leaderboard
                "first_trade_date": None,
                "last_trade_date": None,
                
                # Fields not strictly in DB but expected by some views (defaults)
                "rank": 0, # Rank would need to be calculated or stored
                "verifiedBadge": False
            }
            trader_list.append(trader_dict)
            
        return trader_list


async def get_traders_analytics_from_db() -> Dict:
    """
    Get all leaderboards and analytics from the database.
    Returns data in the format expected by AllLeaderboardsResponse.
    """
    from app.services.leaderboard_service import (
        calculate_trader_metrics_with_time_filter,
        calculate_scores_and_rank_with_percentiles,
        get_unique_wallet_addresses
    )
    
    async with AsyncSessionLocal() as session:
        # 1. Get all unique wallet addresses from DB
        wallets = await get_unique_wallet_addresses(session)
        
        # 2. Calculate metrics for each wallet from raw data
        traders_metrics = []
        for wallet in wallets:
            try:
                metrics = await calculate_trader_metrics_with_time_filter(
                    session, wallet, period='all'
                )
                if metrics:
                    traders_metrics.append(metrics)
            except Exception as e:
                print(f"Error calculating DB metrics for {wallet}: {e}")
                continue
        
        # 3. Calculate scores, ranks, and percentiles
        # This adds W_shrunk, roi_shrunk, pnl_shrunk, final_score, etc.
        result = calculate_scores_and_rank_with_percentiles(traders_metrics)
        traders = result["traders"]
        
        # 4. Construct the specific sorted leaderboards
        leaderboards = {}
        
        # 1. W_shrunk (ascending)
        w_shrunk_sorted = sorted(traders, key=lambda x: x.get('W_shrunk', float('inf')))
        for i, t in enumerate(w_shrunk_sorted, 1):
            t_copy = t.copy()
            t_copy['rank'] = i
            w_shrunk_sorted[i-1] = t_copy
        leaderboards["w_shrunk"] = w_shrunk_sorted
        
        # 2. ROI Raw (descending)
        roi_raw_sorted = sorted(traders, key=lambda x: x.get('roi', float('-inf')), reverse=True)
        for i, t in enumerate(roi_raw_sorted, 1):
            t_copy = t.copy()
            t_copy['rank'] = i
            roi_raw_sorted[i-1] = t_copy
        leaderboards["roi_raw"] = roi_raw_sorted
        
        # 3. ROI Shrunk (ascending)
        roi_shrunk_sorted = sorted(traders, key=lambda x: x.get('roi_shrunk', float('inf')))
        for i, t in enumerate(roi_shrunk_sorted, 1):
            t_copy = t.copy()
            t_copy['rank'] = i
            roi_shrunk_sorted[i-1] = t_copy
        leaderboards["roi_shrunk"] = roi_shrunk_sorted

        # 4. PNL Shrunk (ascending)
        pnl_shrunk_sorted = sorted(traders, key=lambda x: x.get('pnl_shrunk', float('inf')))
        for i, t in enumerate(pnl_shrunk_sorted, 1):
            t_copy = t.copy()
            t_copy['rank'] = i
            pnl_shrunk_sorted[i-1] = t_copy
        leaderboards["pnl_shrunk"] = pnl_shrunk_sorted
        
        # 5. Score Leaderboards (descending)
        # Win Rate Score
        score_win_sorted = sorted(traders, key=lambda x: x.get('score_win_rate', 0), reverse=True)
        for i, t in enumerate(score_win_sorted, 1):
            t_copy = t.copy()
            t_copy['rank'] = i
            score_win_sorted[i-1] = t_copy
        leaderboards["score_win_rate"] = score_win_sorted
        
        # ROI Score
        score_roi_sorted = sorted(traders, key=lambda x: x.get('score_roi', 0), reverse=True)
        for i, t in enumerate(score_roi_sorted, 1):
            t_copy = t.copy()
            t_copy['rank'] = i
            score_roi_sorted[i-1] = t_copy
        leaderboards["score_roi"] = score_roi_sorted
        
        # PnL Score
        score_pnl_sorted = sorted(traders, key=lambda x: x.get('score_pnl', 0), reverse=True)
        for i, t in enumerate(score_pnl_sorted, 1):
            t_copy = t.copy()
            t_copy['rank'] = i
            score_pnl_sorted[i-1] = t_copy
        leaderboards["score_pnl"] = score_pnl_sorted
        
        # Risk Score (descending - usually risk score is higher = riskier, but for leaderboard we might want low risk?)
        # Wait, View-All sorts Risk Score descending? Let's check leaderboard.py
        # leaderboard.py: sorted(..., key=lambda x: x.get('score_risk', 0), reverse=True)
        # So yes, descending.
        score_risk_sorted = sorted(traders, key=lambda x: x.get('score_risk', 0), reverse=True)
        for i, t in enumerate(score_risk_sorted, 1):
            t_copy = t.copy()
            t_copy['rank'] = i
            score_risk_sorted[i-1] = t_copy
        leaderboards["score_risk"] = score_risk_sorted
        
        # Final Score (descending)
        final_score_sorted = sorted(traders, key=lambda x: x.get('final_score', 0), reverse=True)
        for i, t in enumerate(final_score_sorted, 1):
            t_copy = t.copy()
            t_copy['rank'] = i
            final_score_sorted[i-1] = t_copy
        leaderboards["final_score"] = final_score_sorted
        
        return {
            "percentiles": result["percentiles"],
            "medians": result["medians"],
            "leaderboards": leaderboards,
            "total_traders": result["total_traders"],
            "population_traders": result["population_size"]
        }
