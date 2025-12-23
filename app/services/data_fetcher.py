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


async def fetch_markets(
    status: str = "active", 
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    tag_slug: Optional[str] = None
) -> tuple[List[Dict], Dict[str, Any]]:
    """
    Fetch markets from Polymarket Gamma API with pagination (async).
    Uses https://gamma-api.polymarket.com/events/pagination endpoint.
    Returns a tuple of (markets list, pagination info).
    
    Args:
        status: Market status to fetch ("active", "resolved", "closed", etc.)
        limit: Maximum number of markets to fetch (default: 50 from config)
        offset: Offset for pagination (default: 0)
        tag_slug: Optional tag filter (e.g., "sports", "politics", "crypto")
    
    Returns:
        Tuple of (markets list, pagination dict with keys: limit, offset, total, has_more)
    """
    if limit is None:
        limit = settings.MARKETS_FETCH_LIMIT
    if offset is None:
        offset = 0
    
    try:
        # Use Gamma API endpoint
        url = "https://gamma-api.polymarket.com/events/pagination"
        
        # Map status to Gamma API parameters
        active = None
        closed = None
        archived = None
        
        if status == "active":
            active = True
            closed = False
            archived = False
        elif status == "closed":
            closed = True
            active = None  # Can be either
            archived = False
        elif status == "resolved":
            closed = True
            archived = False
        elif status == "archived":
            archived = True
        
        # Build query parameters
        # Optimize: Only fetch what we need (limit + offset) but cap at reasonable size
        # For better performance, we fetch only the required amount
        fetch_limit = min(offset + limit, 200)  # Cap at 200 to avoid huge requests
        
        params = {
            "limit": fetch_limit,
        }
        
        if active is not None:
            params["active"] = str(active).lower()
        if closed is not None:
            params["closed"] = str(closed).lower()
        if archived is not None:
            params["archived"] = str(archived).lower()
        if tag_slug:
            params["tag_slug"] = tag_slug
        
        # Add ordering by volume (descending) for better results
        params["order"] = "volume"
        params["ascending"] = "false"
        
        # Use async HTTP client for better performance
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
                
        # Gamma API returns: {"data": [...events...], "pagination": {"hasMore": bool, "totalResults": int}}
        events = data.get("data", [])
        pagination = data.get("pagination", {})
        
        # Convert events to market format efficiently
        # IMPORTANT: Don't include nested markets array to keep response size small
        all_markets = []
        for event in events:
            # Create a market object from the event
            tags = event.get("tags", [])
            market = {
                "id": event.get("id"),
                "slug": event.get("slug"),
                "ticker": event.get("ticker"),
                "question": event.get("title"),
                "title": event.get("title"),
                "description": event.get("description"),
                "status": "active" if event.get("active") else ("closed" if event.get("closed") else "archived"),
                "volume": float(event.get("volume", 0)),
                "liquidity": float(event.get("liquidity", 0)),
                "openInterest": float(event.get("openInterest", 0)),
                "image": event.get("image"),
                "icon": event.get("icon"),
                "startDate": event.get("startDate"),
                "endDate": event.get("endDate"),
                "end_date": event.get("endDate"),  # Also include snake_case for compatibility
                "creationDate": event.get("creationDate"),
                "createdAt": event.get("createdAt"),
                "updatedAt": event.get("updatedAt"),
                "volume24hr": float(event.get("volume24hr", 0)),
                "volume1wk": float(event.get("volume1wk", 0)),
                "volume1mo": float(event.get("volume1mo", 0)),
                "volume1yr": float(event.get("volume1yr", 0)),
                "competitive": event.get("competitive"),
                "tags": [tag.get("slug") for tag in tags] if tags else [],
                "category": tags[0].get("label", "Uncategorized") if tags else "Uncategorized",
                # Don't include nested markets array - it makes response too large
                # "markets": event.get("markets", []),  # Removed to reduce response size
                "markets_count": len(event.get("markets", [])),  # Just include count instead
                "featured": event.get("featured", False),
                "restricted": event.get("restricted", False),
            }
            
            # Extract outcome prices from the first nested market if available
            # This fixes the issue where all probabilities show as 50%
            event_markets = event.get("markets", [])
            if event_markets and isinstance(event_markets, list) and len(event_markets) > 0:
                primary_market = event_markets[0]
                
                # Extract outcomes and prices
                outcomes = primary_market.get("outcomes", [])
                outcome_prices = primary_market.get("outcomePrices", [])
                
                # Create outcomePrices map if both lists exist and have same length
                if outcomes and outcome_prices and len(outcomes) == len(outcome_prices):
                    try:
                        prices_map = {}
                        for i, outcome in enumerate(outcomes):
                            # Clean outcome name (sometimes valid JSON string) and price
                            name = str(outcome).strip()
                            price = float(outcome_prices[i])
                            prices_map[name] = price
                        
                        market["outcomePrices"] = prices_map
                        
                        # Set main price (usually the first outcome's price, e.g., Yes)
                        if len(outcome_prices) > 0:
                            market["price"] = float(outcome_prices[0])
                            
                    except (ValueError, TypeError):
                        pass
            
            all_markets.append(market)
        
        # Apply offset and limit to the fetched results
        # IMPORTANT: Only return the requested number of markets (limit)
        total_results = pagination.get("totalResults", len(all_markets))
        
        # Slice the array to get only the requested page
        if offset < len(all_markets):
            paginated_markets = all_markets[offset:offset + limit]
        else:
            paginated_markets = []
        
        # Ensure we don't return more than requested
        if len(paginated_markets) > limit:
            paginated_markets = paginated_markets[:limit]
            
        # Determine if there are more results
        has_more = pagination.get("hasMore", False) or (offset + limit < total_results)
            
        pagination_info = {
            "limit": limit,
            "offset": offset,
            "total": total_results,
            "has_more": has_more
        }
            
        print(f"✓ Successfully fetched {len(paginated_markets)} markets from Gamma API (offset: {offset}, limit: {limit}, total: {total_results}, fetched: {len(all_markets)})")
        return paginated_markets, pagination_info
    
    except httpx.HTTPStatusError as e:
        print(f"✗ HTTP error fetching markets from Gamma API: {e}")
        pagination_info = {
            "limit": limit,
            "offset": offset,
            "total": 0,
            "has_more": False
        }
        return [], pagination_info
    except httpx.RequestError as e:
        print(f"✗ Request error fetching markets from Gamma API: {e}")
        pagination_info = {
            "limit": limit,
            "offset": offset,
            "total": 0,
            "has_more": False
        }
        return [], pagination_info
    except Exception as e:
        print(f"✗ Unexpected error fetching markets from Gamma API: {e}")
    pagination_info = {
        "limit": limit,
        "offset": offset,
        "total": 0,
        "has_more": False
    }
    return [], pagination_info


async def fetch_resolved_markets(limit: Optional[int] = None) -> List[Dict]:
    """
    Fetch resolved markets from Polymarket API (wrapper for backward compatibility).
    
    Args:
        limit: Maximum number of markets to fetch (default: 50 from config for testing)
    """
    markets, _ = await fetch_markets(status="resolved", limit=limit)
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
    
    # If market not found in our list, try fetching from Polymarket API
    if not market:
        market = fetch_market_by_slug(market_id)
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
    DEPRECATED: Returning None. Use fetch_market_by_slug instead.
    """
    return None


async def fetch_market_by_slug(market_slug: str) -> Optional[Dict]:
    """
    Fetch market details by slug from Polymarket API only (async).
    
    Args:
        market_slug: Market slug identifier
    
    Returns:
        Market dictionary or None if not found
    """
    try:
        # Try api.polymarket.com first
        api_endpoints = [
            "https://api.polymarket.com",
            "https://data-api.polymarket.com",
        ]
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            for base_url in api_endpoints:
                try:
                    # Try /markets/{slug} endpoint
                    url = f"{base_url}/markets/{market_slug}"
                    response = await client.get(url)
                    response.raise_for_status()
                    data = response.json()
                    
                    if isinstance(data, dict):
                        return data
                    elif isinstance(data, list) and len(data) > 0:
                        return data[0]
                        
                except httpx.HTTPStatusError as e:
                    if e.response and e.response.status_code == 404:
                        # Market not found, try next endpoint
                        continue
                    # For other HTTP errors, try next endpoint
                    continue
                except httpx.RequestError:
                    # Connection error, try next endpoint
                    continue
        
        # If direct slug endpoint doesn't work, try fetching from markets list and filtering
        # This is a fallback approach
        try:
            markets, _ = await fetch_markets(status="active", limit=100, offset=0)
            for market in markets:
                market_slug_field = (
                    market.get("slug") or 
                    market.get("market_slug") or 
                    market.get("id") or
                    market.get("market_id")
                )
                if market_slug_field and str(market_slug_field).lower() == market_slug.lower():
                    return market
        except Exception:
            pass
        
        return None
        
    except Exception as e:
        print(f"Error fetching market by slug '{market_slug}': {e}")
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
    Fetch market orders from Polymarket Data API only (no CLOB, Dome, Gamma, etc.).
    Uses the trades endpoint filtered by market slug.
    
    Note: The trades endpoint filtered by market may not include user addresses.
    This is a limitation of the Polymarket Data API when filtering by market.
    
    Args:
        market_slug: Market slug identifier
        limit: Maximum number of orders to return (default: 100)
        offset: Offset for pagination (default: 0)
    
    Returns:
        Dictionary with orders list and pagination info
    """
    try:
        # Use trades endpoint - activity endpoint requires user parameter, not market
        trades_url = "https://data-api.polymarket.com/trades"
        
        # Fetch more than limit to account for pagination
        fetch_limit = min(limit + offset, 1000)  # API might have limits
        trades_params = {
            "market": market_slug,
            "limit": fetch_limit
        }
        
        response = requests.get(trades_url, params=trades_params, timeout=30)
        response.raise_for_status()
        
        trades_data = response.json()
        
        if not isinstance(trades_data, list):
            # If response is not a list, return empty
            return {
                "orders": [],
                "pagination": {
                    "limit": limit,
                    "offset": offset,
                    "total": 0,
                    "has_more": False
                }
            }
        
        # Get market title if available (from first trade)
        market_title = ""
        condition_id = ""
        if trades_data:
            first_trade = trades_data[0]
            market_title = first_trade.get("marketTitle", first_trade.get("title", ""))
            condition_id = first_trade.get("conditionId", first_trade.get("condition_id", ""))
            
            # Debug: Log available fields in first trade to help diagnose user field issues
            available_fields = list(first_trade.keys())
            print(f"📋 Available fields in trade data: {available_fields}")
            
            # Check if user info is available
            has_user_info = any([
                first_trade.get("proxyWallet"), 
                first_trade.get("user"), 
                first_trade.get("maker"), 
                first_trade.get("from"), 
                first_trade.get("trader"),
                first_trade.get("proxy_wallet")
            ])
            
            if not has_user_info:
                print(f"⚠ Warning: Market-filtered trades endpoint doesn't include user addresses.")
                print(f"   This is a limitation of the Polymarket Data API when filtering by market.")
                print(f"   Available fields: {available_fields[:15]}...")
        
        # Convert trades to order format
        all_orders = []
        for trade in trades_data:
            try:
                # Get user address from multiple possible fields
                # Activity endpoint uses "proxyWallet" or "user" field
                # Trades endpoint may not have user info when filtered by market
                user_address = (
                    trade.get("proxyWallet") or  # Primary field from Activity API
                    trade.get("proxy_wallet") or  # Snake case variant
                    trade.get("user") or 
                    trade.get("userProfileAddress") or  # Alternative field name
                    trade.get("maker") or 
                    trade.get("makerAddress") or
                    trade.get("userAddress") or
                    trade.get("trader") or
                    trade.get("wallet") or
                    trade.get("walletAddress") or
                    trade.get("from") or  # Transaction from field
                    trade.get("account") or  # Account field
                    trade.get("owner") or  # Owner field
                    ""
                )
                
                # Get taker address similarly
                taker_address = (
                    trade.get("taker") or
                    trade.get("takerAddress") or
                    trade.get("takerWallet") or
                    trade.get("to") or  # Transaction to field
                    ""
                )
                
                # Map fields from activity or trades endpoint
                # Activity endpoint uses different field names than trades
                order = {
                    "token_id": trade.get("asset", trade.get("token_id", "")),
                    "token_label": trade.get("outcome", ""),
                    "side": trade.get("side", "BUY"),
                    "market_slug": market_slug,
                    "condition_id": trade.get("conditionId", trade.get("condition_id", condition_id)),
                    "shares": float(trade.get("size", trade.get("shares", 0))),
                    "price": float(trade.get("price", 0)),
                    "tx_hash": trade.get("transactionHash", trade.get("transaction_hash", trade.get("tx_hash", ""))),
                    "title": market_title or trade.get("marketTitle", trade.get("title", "")),
                    "timestamp": trade.get("timestamp", trade.get("time", 0)),
                    "order_hash": trade.get("id", trade.get("order_hash", "")),
                    "user": user_address,  # Use the extracted user address (wallet address)
                    "taker": taker_address,
                    "shares_normalized": float(trade.get("size", trade.get("shares", 0)))
                }
                all_orders.append(order)
            except (ValueError, TypeError) as e:
                # Skip trades with invalid data
                continue
        
        # Apply pagination
        total_orders = len(all_orders)
        paginated_orders = all_orders[offset:offset + limit]
        
        return {
            "orders": paginated_orders,
            "pagination": {
                "limit": limit,
                "offset": offset,
                "total": total_orders,
                "has_more": (offset + limit) < total_orders
            }
        }
        
    except requests.exceptions.HTTPError as e:
        # Handle specific HTTP errors
        if e.response and e.response.status_code == 404:
            # Market not found - return empty result
            return {
                "orders": [],
                "pagination": {
                    "limit": limit,
                    "offset": offset,
                    "total": 0,
                    "has_more": False
                }
            }
        raise Exception(f"Error fetching market orders from Polymarket API: {str(e)}")
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error fetching market orders from Polymarket API: {str(e)}")
    except Exception as e:
        raise Exception(f"Unexpected error fetching market orders: {str(e)}")


async def fetch_traders_from_leaderboard(
    category: str = "overall",
    time_period: str = "all",
    order_by: str = "VOL",
    limit: int = 50,
    offset: int = 0
) -> tuple[List[Dict], Dict[str, Any]]:
    """
    Fetch traders from Polymarket Leaderboard API.
    
    Args:
        category: Category filter ("overall", "politics", "sports", etc.)
        time_period: Time period ("all", "1m", "3m", "6m", "1y")
        order_by: Sort by ("VOL", "PNL", "ROI")
        limit: Maximum number of traders to return
        offset: Offset for pagination
    
    Returns:
        Tuple of (traders list, pagination info)
    """
    try:
        url = "https://data-api.polymarket.com/v1/leaderboard"
        params = {
            "timePeriod": time_period,
            "orderBy": order_by,
            "limit": limit,
            "offset": offset,
            "category": category
        }
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
        
        # API returns a list of trader objects
        if not isinstance(data, list):
            return [], {
                "limit": limit,
                "offset": offset,
                "total": 0,
                "has_more": False
            }
        
        # Convert to trader format
        traders = []
        for item in data:
            trader = {
                "wallet_address": item.get("user") or item.get("proxyWallet") or "",
                "rank": item.get("rank"),
                "userName": item.get("userName"),
                "xUsername": item.get("xUsername"),
                "verifiedBadge": item.get("verifiedBadge", False),
                "profileImage": item.get("profileImage"),
                "vol": float(item.get("vol", 0.0)),
                "pnl": float(item.get("pnl", 0.0)),
                "roi": float(item.get("roi", 0.0)) if item.get("roi") is not None else None,
                "winRate": float(item.get("winRate", 0.0)) if item.get("winRate") is not None else None,
                "totalTrades": int(item.get("totalTrades", 0)) if item.get("totalTrades") is not None else 0,
            }
            traders.append(trader)
        
        # Determine pagination
        has_more = len(data) == limit  # If we got full limit, there might be more
        
        pagination_info = {
            "limit": limit,
            "offset": offset,
            "total": len(traders) + (offset if has_more else 0),  # Approximate total
            "has_more": has_more
        }
        
        print(f"✓ Successfully fetched {len(traders)} traders from Polymarket Leaderboard API")
        return traders, pagination_info
        
    except httpx.HTTPStatusError as e:
        print(f"✗ HTTP error fetching traders from leaderboard: {e}")
        return [], {
            "limit": limit,
            "offset": offset,
            "total": 0,
            "has_more": False
        }
    except httpx.RequestError as e:
        print(f"✗ Request error fetching traders from leaderboard: {e}")
        return [], {
            "limit": limit,
            "offset": offset,
            "total": 0,
            "has_more": False
        }
    except Exception as e:
        print(f"✗ Unexpected error fetching traders from leaderboard: {e}")
        return [], {
            "limit": limit,
            "offset": offset,
            "total": 0,
            "has_more": False
        }


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

