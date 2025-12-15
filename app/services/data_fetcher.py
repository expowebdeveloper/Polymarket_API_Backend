"""
Data fetcher service for Polymarket API with authentication support.
"""

import requests
import httpx
from typing import List, Dict, Optional, Any

from app.core.config import settings


def get_polymarket_headers() -> Dict[str, str]:
    """Get authenticated headers for Polymarket API."""
    return {
        "X-API-KEY": settings.POLYMARKET_API_KEY,
        "X-SECRET": settings.POLYMARKET_SECRET,
        "X-PASSPHRASE": settings.POLYMARKET_PASSPHRASE
    }


def fetch_markets(
    status: str = "active", 
    limit: Optional[int] = None,
    offset: Optional[int] = None
) -> tuple[List[Dict], Dict[str, Any]]:
    """
    Fetch markets from Polymarket API with pagination.
    Returns a tuple of (markets list, pagination info).
    
    Args:
        status: Market status to fetch ("active", "resolved", "closed", etc.)
        limit: Maximum number of markets to fetch (default: 50 from config for testing)
        offset: Offset for pagination (default: 0)
    
    Returns:
        Tuple of (markets list, pagination dict with keys: limit, offset, total, has_more)
    
    Note: If DNS resolution fails for api.polymarket.com, check:
    1. Network connectivity
    2. DNS server configuration
    3. API endpoint URL (may have changed)
    """
    if limit is None:
        limit = settings.MARKETS_FETCH_LIMIT
    if offset is None:
        offset = 0
    
    all_markets = []
    page = 1
    per_page = 50  # Fetch 50 per page from API
    
    # Calculate which pages we need to fetch
    start_page = (offset // per_page) + 1
    end_offset = offset + limit
    end_page = (end_offset // per_page) + 2  # Fetch one extra page to check has_more
    
    headers = get_polymarket_headers()
    
    # List of possible API endpoints to try
    api_endpoints = [
        settings.POLYMARKET_API_URL,
        settings.POLYMARKET_BASE_URL,
        "https://clob.polymarket.com",
    ]
    
    for base_url in api_endpoints:
        all_markets = []
        page = start_page
        
        while page <= end_page:
            try:
                url = f"{base_url}/markets"
                params = {
                    "status": status,
                    "page": page,
                    "limit": per_page,  # Use 'limit' for clob.polymarket.com API
                    "per_page": per_page  # Also include per_page for compatibility
                }
                
                # Try with auth headers first
                try:
                    response = requests.get(url, headers=headers, params=params, timeout=10)
                    response.raise_for_status()
                except requests.exceptions.RequestException:
                    # If auth fails, try without headers (public API)
                    try:
                        response = requests.get(url, params=params, timeout=10)
                        response.raise_for_status()
                    except requests.exceptions.RequestException as e:
                        # If this endpoint fails, try next one
                        error_msg = str(e)
                        if "Failed to resolve" in error_msg or "No address associated" in error_msg:
                            print(f"DNS resolution failed for {base_url}. Trying next endpoint...")
                        else:
                            print(f"Connection failed for {base_url}: {e}")
                        break
                
                data = response.json()
                
                # Debug: Print response structure
                if page == 1:
                    print(f"✓ Successfully connected to {base_url}")
                    if isinstance(data, dict):
                        print(f"  Response keys: {list(data.keys())}")
                    elif isinstance(data, list):
                        print(f"  Response is a list with {len(data)} items")
                
                # Handle different possible response formats
                if isinstance(data, list):
                    page_markets = data
                elif isinstance(data, dict):
                    # Try various possible keys
                    page_markets = (
                        data.get("data") or 
                        data.get("markets") or 
                        data.get("results") or 
                        data.get("items") or
                        []
                    )
                    # If still empty, check if data itself is the list
                    if not page_markets and isinstance(data.get("data"), list):
                        page_markets = data.get("data")
                else:
                    page_markets = []
                
                if not page_markets:
                    if page == start_page:
                        print(f"⚠ Warning: No markets found in response from {base_url}")
                        print(f"  Response preview: {str(data)[:300]}")
                    break
                
                # Add all markets from this page
                all_markets.extend(page_markets)
                
                # Check if we have enough markets or if there are no more
                if len(page_markets) < per_page:
                    # No more pages available
                    break
                    
                page += 1
                
            except requests.exceptions.RequestException as e:
                error_msg = str(e)
                if "Failed to resolve" in error_msg or "No address associated" in error_msg:
                    print(f"✗ DNS resolution failed for {base_url}")
                    print(f"  Error: {error_msg}")
                    if page == start_page:
                        break  # Try next endpoint
                else:
                    print(f"✗ Error fetching markets page {page} from {base_url}: {e}")
                    if page == start_page:
                        break
                    else:
                        # Return what we have so far
                        break
            except Exception as e:
                print(f"✗ Unexpected error fetching markets page {page} from {base_url}: {e}")
                if page == start_page:
                    break
                else:
                    # Return what we have so far
                    break
        
        # If we got markets from this endpoint, process them
        if all_markets:
            # Apply offset and limit
            total_available = len(all_markets)
            paginated_markets = all_markets[offset:offset + limit]
            
            # Determine if there are more markets
            has_more = (offset + limit) < total_available or len(paginated_markets) == limit
            
            pagination_info = {
                "limit": limit,
                "offset": offset,
                "total": total_available,  # This is approximate - actual total may be higher
                "has_more": has_more
            }
            
            print(f"✓ Successfully fetched {len(paginated_markets)} markets (offset: {offset}, limit: {limit})")
            return paginated_markets, pagination_info
    
    # If all endpoints failed
    print("\n" + "="*60)
    print("⚠ WARNING: All API endpoints failed!")
    print("="*60)
    print("Possible issues:")
    print("1. Network connectivity problem")
    print("2. DNS resolution failure (api.polymarket.com cannot be resolved)")
    print("3. API endpoint URL may have changed")
    print("4. Firewall or proxy blocking the connection")
    print("\nTroubleshooting:")
    print("- Check internet connection")
    print("- Try: nslookup api.polymarket.com")
    print("- Verify API endpoint URL is correct")
    print("- Check if you need to use a VPN or proxy")
    print("="*60 + "\n")
    
    # Return empty result with pagination info
    pagination_info = {
        "limit": limit,
        "offset": offset,
        "total": 0,
        "has_more": False
    }
    return [], pagination_info


def fetch_resolved_markets(limit: Optional[int] = None) -> List[Dict]:
    """
    Fetch resolved markets from Polymarket API (wrapper for backward compatibility).
    
    Args:
        limit: Maximum number of markets to fetch (default: 50 from config for testing)
    """
    markets, _ = fetch_markets(status="resolved", limit=limit)
    return markets


async def fetch_trades_for_wallet(wallet_address: str) -> List[Dict]:
    """
    Fetch trades (orders) for a given wallet address from Polymarket API (async version).
    
    This uses the Polymarket Data API to fetch user trades.
    
    First checks an in-memory cache from trader extraction to avoid API calls.
    """
    # Check cache first (orders collected during trader extraction)
    # Use a function to avoid circular import
    try:
        import app.services.trader_service as trader_service_module
        if hasattr(trader_service_module, '_trader_orders_cache'):
            cache = getattr(trader_service_module, '_trader_orders_cache')
            if wallet_address in cache:
                cached_orders = cache[wallet_address]
                if cached_orders:
                    print(f"✓ Using {len(cached_orders)} cached orders for wallet {wallet_address}")
                    return cached_orders
    except:
        pass  # If cache not available, continue with API call
    
    # Use Polymarket Data API
    return await fetch_user_trades(wallet_address)


def fetch_wallet_performance_dome(wallet_address: str) -> Optional[Dict]:
    """
    Fallback: Fetch wallet performance from Dome API.
    DEPRECATED: Returning None to enforce using Polymarket values only.
    """
    return None


def get_market_by_id(market_id: str, markets: List[Dict]) -> Optional[Dict]:
    """Get market data by market ID or slug."""
    if not market_id:
        return None
    
    market_id_lower = market_id.lower()
    
    for market in markets:
        # Try multiple field variations for market identifier
        m_id = (
            market.get("id") or 
            market.get("market_id") or 
            market.get("slug") or
            market.get("market_slug") or
            market.get("marketSlug")
        )
        
        if not m_id:
            continue
            
        # Match by exact string or case-insensitive
        if m_id == market_id or str(m_id).lower() == market_id_lower:
            return market
    
    return None


def get_market_resolution(market_id: str, markets: List[Dict]) -> Optional[str]:
    """Get the resolution (YES/NO) for a given market ID."""
    market = get_market_by_id(market_id, markets)
    
    # If market not found in our list, try fetching from Dome API
    if not market:
        market = fetch_market_by_slug_from_dome(market_id)
        if market:
            # Add to markets list for future lookups
            markets.append(market)
    
    if not market:
        return None
    
    # Check various resolution field formats
    resolution = (
        market.get("resolution") or 
        market.get("outcome") or
        market.get("winningOutcome") or
        market.get("winning_outcome")
    )
    if resolution:
        resolution_str = str(resolution).upper()
        # Normalize to YES/NO
        if "YES" in resolution_str or resolution_str == "1" or "TRUE" in resolution_str:
            return "YES"
        elif "NO" in resolution_str or resolution_str == "0" or "FALSE" in resolution_str:
            return "NO"
        return resolution_str
    
    # Check resolution source
    resolution_source = market.get("resolutionSource") or market.get("resolution_source")
    if resolution_source:
        return "YES" if "yes" in str(resolution_source).lower() else "NO"
    
    # Check if resolved to Yes/No
    if market.get("resolved") or market.get("isResolved") or market.get("is_resolved"):
        # Try to infer from other fields
        outcome = market.get("outcome") or market.get("winningOutcome") or market.get("winning_outcome")
        if outcome:
            outcome_str = str(outcome).lower()
            if "yes" in outcome_str or outcome_str == "1" or "true" in outcome_str:
                return "YES"
            elif "no" in outcome_str or outcome_str == "0" or "false" in outcome_str:
                return "NO"
    
    # Check status field
    status = market.get("status") or market.get("marketStatus")
    if status:
        status_str = str(status).lower()
        if "resolved" in status_str or "closed" in status_str:
            # If resolved but no outcome, we can't determine YES/NO
            pass
    
    return None


def fetch_market_by_slug_from_dome(market_slug: str) -> Optional[Dict]:
    """
    Fetch market data from Dome API for a specific market slug.
    DEPRECATED: Returning None.
    """
    return None


def get_market_category(market: Dict) -> str:
    """Extract category from market data. Defaults to 'Uncategorized'."""
    from app.core.constants import DEFAULT_CATEGORY
    
    category = market.get("category") or market.get("group") or market.get("tags")
    if isinstance(category, list) and category:
        return category[0]
    if isinstance(category, str):
        return category
    return DEFAULT_CATEGORY


def fetch_positions_for_wallet(
    wallet_address: str,
    sort_by: Optional[str] = None,
    sort_direction: Optional[str] = None,
    size_threshold: Optional[float] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None
) -> List[Dict]:
    """
    Fetch positions for a wallet address from Polymarket Data API.
    
    Args:
        wallet_address: Ethereum wallet address (0x...)
        sort_by: Sort field (e.g., "CURRENT", "INITIAL", "PNL")
        sort_direction: Sort direction ("ASC" or "DESC")
        size_threshold: Minimum size threshold (e.g., 0.1)
        limit: Maximum number of positions to return
        offset: Offset for pagination
    
    Returns:
        List of position dictionaries
    """
    try:
        url = f"{settings.POLYMARKET_DATA_API_URL}/positions"
        params = {"user": wallet_address}
        
        if sort_by:
            params["sortBy"] = sort_by
        if sort_direction:
            params["sortDirection"] = sort_direction
        if size_threshold is not None:
            params["sizeThreshold"] = size_threshold
            
        # If limit is specified, just fetch that single page
        if limit is not None:
            params["limit"] = limit
            if offset is not None:
                params["offset"] = offset
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            positions = response.json()
            return positions if isinstance(positions, list) else []
            
        # If limit is None, fetch ALL data using pagination
        all_positions = []
        fetch_limit = 10  # Fetch in chunks of 100
        current_offset = offset or 0
        
        while True:
            params["limit"] = fetch_limit
            params["offset"] = current_offset
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            if not isinstance(data, list) or not data:
                break
                
            all_positions.extend(data)
            
            if len(data) < fetch_limit:
                break
                
            current_offset += fetch_limit
            
        return all_positions

    except requests.exceptions.RequestException as e:
        raise Exception(f"Error fetching positions from Polymarket API: {str(e)}")
    except Exception as e:
        raise Exception(f"Unexpected error fetching positions: {str(e)}")


def fetch_orders_from_dome(
    limit: Optional[int] = 100,
    status: Optional[str] = None,
    market_slug: Optional[str] = None,
    user: Optional[str] = None
) -> Dict:
    """
    Fetch orders from Dome API.
    DEPRECATED: Returning empty dict.
    """
    return {"orders": [], "pagination": {}}


def fetch_user_pnl(
    user_address: str,
    interval: str = "1m",
    fidelity: str = "1d"
) -> List[Dict]:
    """
    Fetch user PnL (Profit and Loss) data from Polymarket User PnL API.
    
    Args:
        user_address: Ethereum wallet address (0x...)
        interval: Time interval (e.g., "1m", "5m", "1h", "1d")
        fidelity: Data fidelity (e.g., "1d", "1w", "1m")
    
    Returns:
        List of PnL data points with 't' (timestamp) and 'p' (pnl) fields
    """
    try:
        url = "https://user-pnl-api.polymarket.com/user-pnl"
        params = {
            "user_address": user_address,
            "interval": interval,
            "fidelity": fidelity
        }
        
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        pnl_data = response.json()
        if isinstance(pnl_data, list):
            return pnl_data
        return []
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error fetching user PnL from Polymarket API: {str(e)}")
    except Exception as e:
        raise Exception(f"Unexpected error fetching user PnL: {str(e)}")


def fetch_profile_stats(proxy_address: str, username: Optional[str] = None) -> Optional[Dict]:
    """
    Fetch profile stats from Polymarket API.
    
    Args:
        proxy_address: Ethereum wallet address (0x...)
        username: Optional username
    
    Returns:
        Dictionary with profile stats (trades, largestWin, views, joinDate) or None if not found
    """
    try:
        url = "https://polymarket.com/api/profile/stats"
        params = {"proxyAddress": proxy_address}
        if username:
            params["username"] = username
        
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        if isinstance(data, dict):
            return data
        return None
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error fetching profile stats from Polymarket API: {str(e)}")
    except Exception as e:
        raise Exception(f"Unexpected error fetching profile stats: {str(e)}")



def fetch_user_activity(
    wallet_address: str, 
    activity_type: Optional[str] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None
) -> List[Dict]:
    """
    Fetch user activity from Polymarket Data API.
    
    Args:
        wallet_address: Ethereum wallet address (0x...)
        activity_type: Optional activity type filter (e.g., "REDEEM", "TRADE")
        limit: Optional limit
        offset: Optional offset
    
    Returns:
        List of activity dictionaries
    """
    try:
        url = f"{settings.POLYMARKET_DATA_API_URL}/activity"
        params = {"user": wallet_address}
        
        if activity_type:
            params["type"] = activity_type
        if limit:
            params["limit"] = limit
        if offset:
            params["offset"] = offset
        
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        activity = response.json()
        if isinstance(activity, list):
            return activity
        return []
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error fetching user activity from Polymarket API: {str(e)}")
    except Exception as e:
        raise Exception(f"Unexpected error fetching user activity: {str(e)}")


async def fetch_user_trades(wallet_address: str) -> List[Dict]:
    """
    Fetch user trades from Polymarket Data API (async version).
    
    Args:
        wallet_address: Ethereum wallet address (0x...)
    
    Returns:
        List of trade dictionaries
    """
    try:
        url = f"{settings.POLYMARKET_DATA_API_URL}/trades"
        params = {"user": wallet_address}
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
            
            trades = response.json()
            if isinstance(trades, list):
                return trades
            return []
    except httpx.HTTPStatusError as e:
        raise Exception(f"Error fetching user trades from Polymarket API: {str(e)}")
    except Exception as e:
        raise Exception(f"Unexpected error fetching user trades: {str(e)}")


def fetch_closed_positions(
    wallet_address: str,
    limit: Optional[int] = None,
    offset: Optional[int] = None
) -> List[Dict]:
    """
    Fetch closed positions for a wallet address from Polymarket Data API.
    
    Args:
        wallet_address: Ethereum wallet address (0x...)
        limit: Maximum number of positions to return
        offset: Offset for pagination
    
    Returns:
        List of closed position dictionaries
    """
    try:
        url = f"{settings.POLYMARKET_DATA_API_URL}/closed-positions"
        params = {"user": wallet_address}
        
        # If limit is specified, just fetch that single page
        if limit is not None:
            params["limit"] = limit
            if offset is not None:
                params["offset"] = offset
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            positions = response.json()
            return positions if isinstance(positions, list) else []

        # If limit is None, fetch ALL data using pagination
        all_positions = []
        fetch_limit = 1000  # Fetch in chunks of 100
        current_offset = offset or 0
        
        while True:
            params["limit"] = fetch_limit
            params["offset"] = current_offset
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            if not isinstance(data, list) or not data:
                break
                
            all_positions.extend(data)
            
            # Increment offset by the number of received items
            # Polymarket API might cap limit at 50 even if we ask for 100
            # So we only stop if we get 0 items (handled above)
            current_offset += len(data)
            
            # Safety break if we receive fewer items than a very small threshold
            # e.g., if we get less than 1 item, which is covered by 'not data' check
            # We remove the < fetch_limit check to support server-side limit capping
            
        return all_positions

    except requests.exceptions.RequestException as e:
        raise Exception(f"Error fetching closed positions from Polymarket API: {str(e)}")
    except Exception as e:
        raise Exception(f"Unexpected error fetching closed positions: {str(e)}")


def fetch_portfolio_value(wallet_address: str) -> float:
    """
    Fetch current portfolio value for a wallet address from Polymarket Data API.
    
    Args:
        wallet_address: Ethereum wallet address (0x...)
    
    Returns:
        Portfolio value as float, or 0.0 if not found
    """
    try:
        url = f"{settings.POLYMARKET_DATA_API_URL}/value"
        params = {"user": wallet_address}
        
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        # API returns list of objects: [{"user": "...", "value": 0.041534}]
        if isinstance(data, list) and len(data) > 0:
            return float(data[0].get("value", 0.0))
        return 0.0
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error fetching portfolio value from Polymarket API: {str(e)}")
    except Exception as e:
        raise Exception(f"Unexpected error fetching portfolio value: {str(e)}")


def fetch_leaderboard_stats(wallet_address: str) -> Dict[str, float]:
    """
    Fetch all-time stats (volume, pnl) for a user from the Leaderboard API.
    
    Args:
        wallet_address: Ethereum wallet address (0x...)
    
    Returns:
        Dictionary with "volume" and "pnl" keys
    """
    try:
        url = "https://data-api.polymarket.com/v1/leaderboard"
        params = {
            "timePeriod": "all",
            "orderBy": "VOL",
            "limit": 1,
            "offset": 0,
            "category": "overall",
            "user": wallet_address
        }
        
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        # Example: [{"user": "...", "vol": 12345.67, "pnl": -353.01...}]
        stats = {"volume": 0.0, "pnl": 0.0}
        if isinstance(data, list) and len(data) > 0:
            item = data[0]
            stats["volume"] = float(item.get("vol", 0.0))
            stats["pnl"] = float(item.get("pnl", 0.0))
            return stats
        return stats
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error fetching leaderboard stats from Polymarket API: {str(e)}")
    except Exception as e:
        raise Exception(f"Unexpected error fetching leaderboard stats: {str(e)}")


def fetch_market_orders(market_slug: str, limit: int = 100, offset: int = 0) -> Dict[str, Any]:
    """
    Fetch market orders from DomeAPI.
    
    Args:
        market_slug: Market slug identifier
        limit: Maximum number of orders to return (default: 100)
        offset: Offset for pagination (default: 0)
    
    Returns:
        Dictionary with orders list and pagination info
    """
    try:
        url = "https://api.domeapi.io/v1/polymarket/orders"
        params = {
            "market_slug": market_slug,
            "limit": limit,
            "offset": offset
        }
        
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        return {
            "orders": data.get("orders", []),
            "pagination": data.get("pagination", {})
        }
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error fetching market orders from DomeAPI: {str(e)}")
    except Exception as e:
        raise Exception(f"Unexpected error fetching market orders: {str(e)}")


def fetch_user_leaderboard_data(wallet_address: str, category: str = "politics") -> Optional[Dict[str, Any]]:
    """
    Fetch full user leaderboard data including username, xUsername, profileImage, volume, pnl, etc.
    Tries multiple categories (overall, politics) to find the user.
    
    Args:
        wallet_address: Ethereum wallet address (0x...)
        category: Preferred category filter (default: "politics", can be "overall", "politics", etc.)
    
    Returns:
        Dictionary with user data or None if not found
    """
    url = "https://data-api.polymarket.com/v1/leaderboard"
    categories_to_try = ["overall", category] if category != "overall" else ["overall"]
    
    for cat in categories_to_try:
        try:
            params = {
                "timePeriod": "all",
                "orderBy": "VOL",
                "limit": 1,
                "offset": 0,
                "category": cat,
                "user": wallet_address
            }
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            if isinstance(data, list) and len(data) > 0:
                item = data[0]
                return {
                    "rank": item.get("rank"),
                    "proxyWallet": item.get("proxyWallet"),
                    "userName": item.get("userName"),
                    "xUsername": item.get("xUsername"),
                    "verifiedBadge": item.get("verifiedBadge", False),
                    "vol": float(item.get("vol", 0.0)),
                    "pnl": float(item.get("pnl", 0.0)),
                    "profileImage": item.get("profileImage")
                }
        except requests.exceptions.RequestException:
            # Continue to next category if this one fails
            continue
        except Exception:
            # Continue to next category if this one fails
            continue
    
    # If all categories failed, return None
    return None

