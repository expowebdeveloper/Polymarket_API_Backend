"""
Data fetcher service for Polymarket API with authentication support.
"""

import requests
from typing import List, Dict, Optional

from app.core.config import settings


def get_polymarket_headers() -> Dict[str, str]:
    """Get authenticated headers for Polymarket API."""
    return {
        "X-API-KEY": settings.POLYMARKET_API_KEY,
        "X-SECRET": settings.POLYMARKET_SECRET,
        "X-PASSPHRASE": settings.POLYMARKET_PASSPHRASE
    }


def fetch_resolved_markets(limit: Optional[int] = None) -> List[Dict]:
    """
    Fetch resolved markets from Polymarket API with pagination.
    Returns a list of market dictionaries (limited to 50 for testing by default).
    
    Args:
        limit: Maximum number of markets to fetch (default: 50 from config for testing)
    
    Note: If DNS resolution fails for api.polymarket.com, check:
    1. Network connectivity
    2. DNS server configuration
    3. API endpoint URL (may have changed)
    """
    if limit is None:
        limit = settings.MARKETS_FETCH_LIMIT
    
    markets = []
    page = 1
    per_page = min(50, limit)  # Fetch max 50 per page to match testing limit
    
    headers = get_polymarket_headers()
    
    # List of possible API endpoints to try
    api_endpoints = [
        settings.POLYMARKET_API_URL,
        settings.POLYMARKET_BASE_URL,
        "https://clob.polymarket.com",
    ]
    
    for base_url in api_endpoints:
        markets = []
        page = 1
        
        while True:
            try:
                url = f"{base_url}/markets"
                params = {
                    "status": "resolved",
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
                    if page == 1:
                        print(f"⚠ Warning: No markets found in response from {base_url}")
                        print(f"  Response preview: {str(data)[:300]}")
                    break
                
                # Limit the number of markets to fetch (only take what we need)
                remaining = limit - len(markets)
                if remaining > 0:
                    markets_to_add = page_markets[:remaining]
                    markets.extend(markets_to_add)
                    if page == 1:
                        print(f"✓ Fetched {len(markets_to_add)} markets from {base_url} (total: {len(markets)}/{limit})")
                else:
                    if page == 1:
                        print(f"✓ Already have {len(markets)} markets (limit: {limit})")
                
                # Check if we've reached the limit
                if len(markets) >= limit:
                    print(f"✓ Reached limit of {limit} markets. Stopping fetch.")
                    break
                    
                # Check if there are more pages (for cursor-based pagination, check next_cursor)
                has_more = False
                if isinstance(data, dict):
                    # Check for cursor-based pagination
                    if data.get("next_cursor"):
                        has_more = True
                    # Check for traditional pagination
                    elif len(page_markets) >= per_page:
                        has_more = True
                elif isinstance(data, list) and len(page_markets) >= per_page:
                    has_more = True
                
                if not has_more:
                    break
                    
                page += 1
                
            except requests.exceptions.RequestException as e:
                error_msg = str(e)
                if "Failed to resolve" in error_msg or "No address associated" in error_msg:
                    print(f"✗ DNS resolution failed for {base_url}")
                    print(f"  Error: {error_msg}")
                    if page == 1:
                        break  # Try next endpoint
                else:
                    print(f"✗ Error fetching markets page {page} from {base_url}: {e}")
                    if page == 1:
                        break
                    else:
                        return markets  # Return what we have
            except Exception as e:
                print(f"✗ Unexpected error fetching markets page {page} from {base_url}: {e}")
                if page == 1:
                    break
                else:
                    return markets
        
        # If we got markets from this endpoint, return them
        if markets:
            print(f"✓ Successfully fetched {len(markets)} total markets")
            return markets
    
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
    return markets


def fetch_trades_for_wallet(wallet_address: str) -> List[Dict]:
    """
    Fetch trades (orders) for a given wallet address from Dome API.
    
    This uses https://api.domeapi.io/v1/polymarket/orders with the `user`
    or `address` filter (depending on Dome's API) instead of calling
    Polymarket APIs directly.
    
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
    
    trades: List[Dict] = []
    base_url = settings.DOME_API_URL.rstrip("/")
    api_key = settings.DOME_API_KEY
    
    # Dome free tier: 1 QPS / 10 requests per 10 seconds – keep it to a single call
    try:
        url = f"{base_url}/polymarket/orders"
        
        # Try multiple parameter name variations - Dome API might use different field names
        # Also try without limit first to see if that's the issue
        params_candidates = [
            {"user": wallet_address, "limit": 100},
            {"address": wallet_address, "limit": 100},
            {"wallet": wallet_address, "limit": 100},
            {"wallet_address": wallet_address, "limit": 100},
            {"trader": wallet_address, "limit": 100},
            {"account": wallet_address, "limit": 100},
            {"user": wallet_address},  # Try without limit
            {"address": wallet_address},  # Try without limit
        ]
        
        headers = {
            "Authorization": f"Bearer {api_key}"
        }
        
        last_error = None
        response = None
        successful_params = None
        
        for params in params_candidates:
            try:
                response = requests.get(url, headers=headers, params=params, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    # Check if we got actual orders
                    if isinstance(data, dict):
                        orders = data.get("orders", []) or data.get("data", []) or []
                    elif isinstance(data, list):
                        orders = data
                    else:
                        orders = []
                    
                    if orders:
                        successful_params = params
                        break
                    # If no orders but 200 status, continue trying other params
                else:
                    last_error = f"Status {response.status_code}"
            except requests.exceptions.RequestException as e:
                last_error = str(e)
                continue
        
        if not response or response.status_code != 200:
            print(f"✗ Error fetching trades from Dome for wallet {wallet_address}: {last_error}")
            return []
        
        data = response.json()
        
        # Dome orders endpoint returns { orders: [...], pagination: {...} }
        if isinstance(data, dict):
            trades = data.get("orders", []) or data.get("data", []) or []
        elif isinstance(data, list):
            trades = data
        else:
            trades = []
        
        if trades:
            if successful_params:
                print(f"✓ Fetched {len(trades)} trades (orders) for wallet {wallet_address} from Dome using params: {successful_params}")
            else:
                print(f"✓ Fetched {len(trades)} trades (orders) for wallet {wallet_address} from Dome")
        else:
            # Debug: show what we got back
            print(f"⚠ Warning: No trades found for wallet {wallet_address} from Dome")
            print(f"  Response preview: {str(data)[:300]}")
            # Try to understand the response structure
            if isinstance(data, dict):
                print(f"  Response keys: {list(data.keys())}")
                if "pagination" in data:
                    print(f"  Pagination: {data['pagination']}")
    
    except requests.exceptions.RequestException as e:
        print(f"✗ Error fetching trades for wallet {wallet_address} from Dome: {e}")
    except Exception as e:
        print(f"✗ Unexpected error fetching trades for wallet {wallet_address} from Dome: {e}")
    
    if not trades:
        print(f"\n⚠ WARNING: Could not fetch trades for wallet {wallet_address} from Dome\n")
    else:
        print(f"✓ Successfully fetched {len(trades)} total trades for wallet {wallet_address} from Dome")
    return trades


def fetch_wallet_performance_dome(wallet_address: str) -> Optional[Dict]:
    """
    Fallback: Fetch wallet performance from Dome API.
    Free tier: 1 QPS
    """
    try:
        url = f"{settings.DOME_API_URL}/wallets/{wallet_address}/performance"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching from Dome API: {e}")
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
    This helps get market resolution data for markets not in the resolved markets list.
    """
    try:
        base_url = settings.DOME_API_URL.rstrip("/")
        api_key = settings.DOME_API_KEY
        
        # Try different Dome API endpoints for market data
        endpoints = [
            f"{base_url}/polymarket/markets/{market_slug}",
            f"{base_url}/polymarket/markets?slug={market_slug}",
        ]
        
        headers = {
            "Authorization": f"Bearer {api_key}"
        }
        
        for url in endpoints:
            try:
                response = requests.get(url, headers=headers, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    # Handle different response formats
                    if isinstance(data, dict):
                        return data.get("market") or data.get("data") or data
                    elif isinstance(data, list) and len(data) > 0:
                        return data[0]
            except:
                continue
    except Exception as e:
        pass  # Silently fail - we'll just use the markets we have
    
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
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        positions = response.json()
        if isinstance(positions, list):
            return positions
        return []
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
    
    Args:
        limit: Maximum number of orders to fetch (default: 100)
        status: Order status filter (e.g., "closed", "open")
        market_slug: Filter by market slug
        user: Filter by wallet address
    
    Returns:
        Dictionary with 'orders' list and 'pagination' info
    """
    try:
        base_url = settings.DOME_API_URL.rstrip("/")
        url = f"{base_url}/polymarket/orders"
        
        params = {"limit": limit}
        if status:
            params["status"] = status
        if market_slug:
            params["market_slug"] = market_slug
        if user:
            params["user"] = user
        
        headers = {
            "Authorization": f"Bearer {settings.DOME_API_KEY}"
        }
        
        response = requests.get(url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error fetching orders from Dome API: {str(e)}")
    except Exception as e:
        raise Exception(f"Unexpected error fetching orders: {str(e)}")


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


def fetch_user_activity(wallet_address: str) -> List[Dict]:
    """
    Fetch user activity from Polymarket Data API.
    
    Args:
        wallet_address: Ethereum wallet address (0x...)
    
    Returns:
        List of activity dictionaries
    """
    try:
        url = f"{settings.POLYMARKET_DATA_API_URL}/activity"
        params = {"user": wallet_address}
        
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


def fetch_user_trades(wallet_address: str) -> List[Dict]:
    """
    Fetch user trades from Polymarket Data API.
    
    Args:
        wallet_address: Ethereum wallet address (0x...)
    
    Returns:
        List of trade dictionaries
    """
    try:
        url = f"{settings.POLYMARKET_DATA_API_URL}/trades"
        params = {"user": wallet_address}
        
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        trades = response.json()
        if isinstance(trades, list):
            return trades
        return []
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error fetching user trades from Polymarket API: {str(e)}")
    except Exception as e:
        raise Exception(f"Unexpected error fetching user trades: {str(e)}")

