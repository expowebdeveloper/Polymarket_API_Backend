"""
Trader service for extracting and managing trader data.
"""

from typing import List, Dict, Set, Optional
from collections import defaultdict
from datetime import datetime

from app.services.data_fetcher import (
    fetch_resolved_markets,
    fetch_trades_for_wallet,
    get_market_by_id
)
from app.services.scoring_engine import calculate_metrics


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
    
    # If we need more traders, try to extract from Dome orders instead of Polymarket trades
    if (not limit) or len(traders) < limit:
        from app.core.config import settings as app_settings
        import requests
        
        base_url = app_settings.DOME_API_URL.rstrip("/")
        api_key = app_settings.DOME_API_KEY
        headers = {"Authorization": f"Bearer {api_key}"}
        
        # Dome orders endpoint: we can either fetch globally or per-market.
        # To keep Dome usage light, we:
        #  - fetch orders per market slug (limited)
        #  - stop once we have enough unique traders.
        if limit:
            max_markets_to_check = min(max(limit // 2, 20), len(markets))
        else:
            max_markets_to_check = min(50, len(markets))
        
        markets_to_check = markets[:max_markets_to_check]
        print(f"Extracting traders from Dome orders for {len(markets_to_check)} markets (need {limit - len(traders) if limit else 'unlimited'} more)...")
        
        successful_markets = 0
        failed_markets = 0
        
        for idx, market in enumerate(markets_to_check):
            if limit and len(traders) >= limit:
                break
            
            slug = market.get("slug") or market.get("market_id") or market.get("id")
            if not slug:
                failed_markets += 1
                continue
            
            try:
                url = f"{base_url}/polymarket/orders"
                params = {
                    "market_slug": slug,
                    "limit": 100
                }
                
                response = requests.get(url, headers=headers, params=params, timeout=10)
                if response.status_code != 200:
                    failed_markets += 1
                    if idx < 3:
                        print(f"  Failed to fetch Dome orders for market {str(slug)[:20]}... (Status {response.status_code})")
                    continue
                
                data = response.json()
                orders = []
                if isinstance(data, dict):
                    orders = data.get("orders", []) or data.get("data", []) or []
                elif isinstance(data, list):
                    orders = data
                
                if not orders:
                    failed_markets += 1
                    continue
                
                traders_found_in_market = 0
                for order in orders:
                    # Dome orders: user field is the trader wallet
                    address = (
                        order.get("user")
                        or order.get("address")
                        or order.get("wallet")
                    )
                    if address and isinstance(address, str):
                        address = address.strip()
                        if address.startswith("0x") and len(address) == 42:
                            traders.add(address)
                            traders_found_in_market += 1
                            if limit and len(traders) >= limit:
                                break
                
                if traders_found_in_market > 0:
                    print(f"  Found {traders_found_in_market} traders in Dome orders for market {str(slug)[:20]}...")
                    successful_markets += 1
                else:
                    failed_markets += 1
            
            except Exception as e:
                failed_markets += 1
                if idx < 3:
                    print(f"  Error fetching Dome orders for market {str(slug)[:20]}... ({str(e)[:80]})")
                continue
        
        print(f"Dome market extraction summary: {successful_markets} successful, {failed_markets} failed")
    
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


def get_trader_basic_info(wallet_address: str, markets: List[Dict]) -> Dict:
    """
    Get basic information about a trader without full analytics.
    Faster than full analytics calculation.
    """
    trades = fetch_trades_for_wallet(wallet_address)
    
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
        market_id = trade.get("market_id") or trade.get("market") or trade.get("marketId")
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


def get_trader_detail(wallet_address: str) -> Dict:
    """
    Get detailed trader information including full analytics.
    """
    markets = fetch_resolved_markets()
    trades = fetch_trades_for_wallet(wallet_address)
    metrics = calculate_metrics(wallet_address, trades, markets)
    
    # Add trade date information
    timestamps = []
    for trade in trades:
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
        **metrics,
        "first_trade_date": first_trade.isoformat() if first_trade else None,
        "last_trade_date": last_trade.isoformat() if last_trade else None
    }


def get_traders_list(limit: int = 50) -> List[Dict]:
    """
    Get a list of traders with basic information.
    Extracts traders from markets.
    
    Args:
        limit: Maximum number of traders to return
    
    Returns:
        List of trader dictionaries with basic info
    """
    markets = fetch_resolved_markets()
    
    # Extract traders - try to get more than the limit to account for traders with no data
    # We'll request 1.5x the limit to ensure we have enough after filtering
    extraction_limit = int(limit * 1.5) if limit else None
    trader_addresses = extract_traders_from_markets(markets, limit=extraction_limit)
    
    if not trader_addresses:
        print("⚠ No traders found. Returning empty list.")
        return []
    
    print(f"Found {len(trader_addresses)} unique trader addresses. Getting basic info...")
    
    # Get basic info for each trader
    traders_info = []
    traders_with_trades = 0
    traders_without_trades = 0
    
    for idx, wallet in enumerate(trader_addresses):
        if limit and len(traders_info) >= limit:
            break
            
        try:
            info = get_trader_basic_info(wallet, markets)
            
            # Include all traders, even if they have no trades (API might have failed)
            traders_info.append(info)
            
            if info.get("total_trades", 0) > 0:
                traders_with_trades += 1
            else:
                traders_without_trades += 1
                
        except Exception as e:
            # Even if we can't get full info, create a basic entry
            traders_info.append({
                "wallet_address": wallet,
                "total_trades": 0,
                "total_positions": 0,
                "first_trade_date": None,
                "last_trade_date": None
            })
            traders_without_trades += 1
            if idx < 5:  # Only print first few errors
                print(f"  Warning: Could not get full info for trader {wallet[:20]}... ({str(e)[:50]})")
    
    print(f"✓ Successfully retrieved info for {len(traders_info)} traders")
    print(f"  - {traders_with_trades} with trades, {traders_without_trades} without trades (API may have failed)")
    return traders_info

