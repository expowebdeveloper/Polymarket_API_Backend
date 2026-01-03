"""
Data fetcher service for Polymarket API with authentication support.
"""


# import requests  # We will use httpx instead
import httpx
import dns.resolver
from typing import List, Dict, Optional, Any, Set
import asyncio
from datetime import datetime, timedelta
from app.core.config import settings

# List of domains known to be hijacked/blocked by some ISPs (e.g., Jio)
HIJACKED_DOMAINS = {
    "data-api.polymarket.com",
    "api.polymarket.com",
    "gamma-api.polymarket.com",
    "user-pnl-api.polymarket.com",
    "clob.polymarket.com",
    "polymarket.com"
}

# Cache for resolved IPs to avoid repeated DNS queries
DNS_CACHE: Dict[str, str] = {}

def resolve_domain_securely(domain: str) -> str:
    """Resolve a domain using Google DNS (8.8.8.8) to bypass local hijacking."""
    if domain in DNS_CACHE:
        return DNS_CACHE[domain]
    
    try:
        resolver = dns.resolver.Resolver()
        resolver.nameservers = ['8.8.8.8', '1.1.1.1']
        answers = resolver.resolve(domain, 'A')
        if answers:
            ip = str(answers[0])
            DNS_CACHE[domain] = ip
            print(f"📡 DNS Bypass: Resolved {domain} to {ip} via 8.8.8.8")
            return ip
    except Exception as e:
        print(f"⚠️ DNS Bypass Error for {domain}: {e}")
    
    return domain

class DNSAwareAsyncClient(httpx.AsyncClient):
    """
    HTTPX AsyncClient that automatically resolves problematic domains 
    via secure DNS to bypass local hijacking.
    """
    async def request(self, method: str, url: httpx.URL | str, **kwargs) -> httpx.Response:
        url_obj = httpx.URL(url)
        if url_obj.host in HIJACKED_DOMAINS:
            resolved_ip = resolve_domain_securely(url_obj.host)
            if resolved_ip != url_obj.host:
                # Store original host for the 'Host' header
                original_host = url_obj.host
                # Update URL to use the IP address
                new_url = url_obj.copy_with(host=resolved_ip)
                
                # Add Host header if not present
                headers = kwargs.get("headers")
                if headers is None:
                    headers = {}
                else:
                    headers = dict(headers) # Create a copy to avoid mutating original
                
                if "Host" not in headers:
                    headers["Host"] = original_host
                kwargs["headers"] = headers
                
                # Disable SSL verification for IP-based requests if needed, 
                # but better to use 'verify=False' with caution or properly 
                # use the 'extensions' or 'mounts' for better SNI support.
                # Actually, providing the 'Host' header and 'verify=True' 
                # usually works if the library supports it, but httpx 
                # might need more help with SNI when using IP.
                
                # To handle SNI correctly, we should use a custom transport 
                # or just set the 'verify' to the original host's cert.
                # A simpler way is to use extensions:
                extensions = kwargs.get("extensions")
                if extensions is None:
                    extensions = {}
                else:
                    extensions = dict(extensions)
                
                extensions["sni_hostname"] = original_host
                kwargs["extensions"] = extensions
                
                return await super().request(method, new_url, **kwargs)
        
        return await super().request(method, url, **kwargs)

class DNSAwareClient(httpx.Client):
    """
    HTTPX Sync Client that automatically resolves problematic domains 
    via secure DNS to bypass local hijacking.
    """
    def request(self, method: str, url: httpx.URL | str, **kwargs) -> httpx.Response:
        url_obj = httpx.URL(url)
        if url_obj.host in HIJACKED_DOMAINS:
            resolved_ip = resolve_domain_securely(url_obj.host)
            if resolved_ip != url_obj.host:
                original_host = url_obj.host
                new_url = url_obj.copy_with(host=resolved_ip)
                
                # Ensure headers is a dictionary and add Host header
                headers = kwargs.get("headers")
                if headers is None:
                    headers = {}
                else:
                    headers = dict(headers)
                
                if "Host" not in headers:
                    headers["Host"] = original_host
                kwargs["headers"] = headers
                
                # Set SNI hostname via extensions
                extensions = kwargs.get("extensions")
                if extensions is None:
                    extensions = {}
                else:
                    extensions = dict(extensions)
                extensions["sni_hostname"] = original_host
                kwargs["extensions"] = extensions
                
                return super().request(method, new_url, **kwargs)
        
        return super().request(method, url, **kwargs)

# Shared sync client instance
sync_client = DNSAwareClient(timeout=30.0)

# Shared async client instance for extreme performance
# We use a single instance to reuse connection pools and SSL handshakes
async_client = DNSAwareAsyncClient(
    timeout=httpx.Timeout(30.0, connect=10.0),
    limits=httpx.Limits(max_connections=100, max_keepalive_connections=20)
)

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
        
<<<<<<< HEAD
        # Initialize results container
=======
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
        
        # Use shared async HTTP client for better performance
        response = await async_client.get(url, params=params)
        response.raise_for_status()
        data = response.json()
                
        # Gamma API returns: {"data": [...events...], "pagination": {"hasMore": bool, "totalResults": int}}
        events = data.get("data", [])
        pagination = data.get("pagination", {})
        
        # Convert events to market format efficiently
        # IMPORTANT: Don't include nested markets array to keep response size small
>>>>>>> 1e267d7cee08180e9c110108b558c48504150e5b
        all_markets = []
        
        # Pagination loop
        current_offset = offset
        items_to_fetch = limit if limit is not None else float('inf')
        fetched_so_far = 0
        
        # Fetch in chunks (default 100 for safety and speed)
        chunk_size = 100 
        
        while fetched_so_far < items_to_fetch:
            # Determine current limit
            current_limit = min(chunk_size, items_to_fetch - fetched_so_far)
            # The API might be finicky with very large limits, so stick to reasonable chunks
            if current_limit > 100: 
                current_limit = 100

            params = {
                "limit": current_limit,
                "offset": current_offset
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
            async with DNSAwareAsyncClient(timeout=30.0) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                    
            # Gamma API returns: {"data": [...events...], "pagination": {"hasMore": bool, "totalResults": int}}
            events = data.get("data", [])
            pagination = data.get("pagination", {})
            
            if not events:
                break
                
            # Convert events to market format efficiently
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
            
            # Update counters
            fetched_so_far += len(events)
            current_offset += len(events)
            
            # Check if we're done
            if not pagination.get("hasMore", False) or len(events) < current_limit:
                break
                
            # If explicit limit was provided and we reached it
            if limit is not None and fetched_so_far >= limit:
                break

        total_results = pagination.get("totalResults", len(all_markets))
        has_more = pagination.get("hasMore", False) or (current_offset < total_results)
            
        pagination_info = {
            "limit": limit if limit else len(all_markets),
            "offset": offset,
            "total": total_results,
            "has_more": has_more
        }
            
        print(f"✓ Successfully fetched {len(all_markets)} markets from Gamma API (total requested: {limit if limit else 'ALL'}, available: {total_results})")
        return all_markets, pagination_info
    
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


async def get_market_resolution(market_id: str, markets: List[Dict]) -> Optional[str]:
    """Get the resolution (YES/NO) for a given market ID."""
    market = get_market_by_id(market_id, markets)
    
    # If market not found in our list, try fetching from Polymarket API
    if not market:
        market = await fetch_market_by_slug(market_id)
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
        
        async with DNSAwareAsyncClient(timeout=10.0) as client:
            # Try Gamma API /events endpoint with slug filter
            # Gamma API: https://gamma-api.polymarket.com/events?slug={slug}
            gamma_url = "https://gamma-api.polymarket.com/events"
            params = {"slug": market_slug}
            
            try:
                response = await client.get(gamma_url, params=params)
                if response.status_code == 200:
                    data = response.json()
                    # Response is list of events. Each event has 'markets' list.
                    if isinstance(data, list) and len(data) > 0:
                        event = data[0]
                        # Look for specific market if multiple, or just return the first/main one
                        # If the slug matches the event slug, usually we want the main market or all markets.
                        # For this function which returns 1 market, we'll take the first one or try to match slug if market-level slug exists.
                        markets = event.get("markets", [])
                        if markets:
                            # Enhance market data with event info
                            market = markets[0]
                            market["input_slug"] = market_slug # Keep track of what we looked up
                            if not market.get("title") and event.get("title"):
                                market["title"] = event.get("title")
                            if not market.get("icon") and event.get("icon"):
                                market["icon"] = event.get("icon")
                            if not market.get("image") and event.get("image"):
                                market["image"] = event.get("image")
                            if not market.get("description") and event.get("description"):
                                market["description"] = event.get("description")
                            return market
            except Exception as e:
                print(f"Error fetching from Gamma API /events: {e}")

            # Try Data API as fallback (if it supports this endpoint)
            # data-api usually mirrors or aggregates
        
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


async def fetch_positions_for_wallet(
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
                
                response = await async_client.get(url, params=params)
                response.raise_for_status()
                positions = response.json()
                # Ensure we always return a list, even if API returns None or empty
                if positions is None:
                    return []
                return positions if isinstance(positions, list) else []
                
            # If limit is None, fetch ALL data using pagination
            all_positions = []
            fetch_limit = 1000  # Fetch in chunks
            current_offset = offset or 0
            
            while True:
                params["limit"] = fetch_limit
                params["offset"] = current_offset
                
                response = await async_client.get(url, params=params)
                response.raise_for_status()
                
                data = response.json()
                if not isinstance(data, list) or not data:
                    break
                    
                all_positions.extend(data)
                
                if len(data) < fetch_limit:
                    break
                    
                current_offset += len(data)
                
            return all_positions

    except httpx.HTTPStatusError as e:
        print(f"Error fetching positions from Polymarket API: {str(e)}")
        return []
    except Exception as e:
        print(f"Unexpected error fetching positions: {str(e)}")
        return []


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


async def fetch_user_pnl(
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
        
<<<<<<< HEAD
        async with DNSAwareAsyncClient(timeout=30.0) as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
            pnl_data = response.json()
            
=======
        response = await async_client.get(url, params=params)
        response.raise_for_status()
        
        pnl_data = response.json()
>>>>>>> 1e267d7cee08180e9c110108b558c48504150e5b
        if isinstance(pnl_data, list):
            return pnl_data
        return []
    except httpx.HTTPStatusError as e:
        print(f"Error fetching user PnL from Polymarket API: {str(e)}")
        return []
    except Exception as e:
        print(f"Unexpected error fetching user PnL: {str(e)}")
        return []


async def fetch_profile_stats(proxy_address: str, username: Optional[str] = None) -> Optional[Dict]:
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
        
<<<<<<< HEAD
        async with DNSAwareAsyncClient(timeout=30.0) as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
=======
        response = await async_client.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()
>>>>>>> 1e267d7cee08180e9c110108b558c48504150e5b
        if isinstance(data, dict):
            return data
        return None
    except httpx.HTTPStatusError as e:
        print(f"Error fetching profile stats from Polymarket API: {str(e)}")
        return None
    except Exception as e:
        print(f"Unexpected error fetching profile stats: {str(e)}")
        return None



async def fetch_user_activity(
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
        
<<<<<<< HEAD
        async with DNSAwareAsyncClient(timeout=30.0) as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
            activity = response.json()
            
=======
        response = await async_client.get(url, params=params)
        response.raise_for_status()
        
        activity = response.json()
>>>>>>> 1e267d7cee08180e9c110108b558c48504150e5b
        if isinstance(activity, list):
            return activity
        return []
    except httpx.HTTPStatusError as e:
        print(f"Error fetching user activity from Polymarket API: {str(e)}")
        return []
    except Exception as e:
        print(f"Unexpected error fetching user activity: {str(e)}")
        return []


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
        
        response = await async_client.get(url, params=params)
        response.raise_for_status()
        
        trades = response.json()
        if isinstance(trades, list):
            return trades
        return []
    except httpx.HTTPStatusError as e:
        print(f"Error fetching user trades from Polymarket API: {str(e)}")
        return []
    except Exception as e:
        print(f"Unexpected error fetching user trades: {str(e)}")
        return []


async def fetch_closed_positions(
    wallet_address: str,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    time_period: Optional[str] = None
) -> List[Dict]:
    """
    Fetch closed positions for a wallet address from Polymarket Data API.
    
    Args:
        wallet_address: Ethereum wallet address (0x...)
        limit: Maximum number of positions to return
        offset: Offset for pagination
        time_period: Optional time period filter ("day", "week", "month", "all")
                     If provided, filters positions by timestamp
    
    Returns:
        List of closed position dictionaries
    """
    try:
        url = f"{settings.POLYMARKET_DATA_API_URL}/closed-positions"
        params = {"user": wallet_address}
        
<<<<<<< HEAD
        async with DNSAwareAsyncClient(timeout=30.0) as client:
            # If limit is specified, just fetch that single page
            if limit is not None:
                params["limit"] = limit
                if offset is not None:
                    params["offset"] = offset
                
                response = await client.get(url, params=params)
                response.raise_for_status()
                positions = response.json()
                return positions if isinstance(positions, list) else []
=======
        # If limit is specified, just fetch that single page
        if limit is not None:
            params["limit"] = limit
            if offset is not None:
                params["offset"] = offset
            
            response = await async_client.get(url, params=params)
            response.raise_for_status()
            positions = response.json()
            positions = positions if isinstance(positions, list) else []
            
            # Apply time period filter if specified
            if time_period and time_period != "all":
                cutoff_timestamp = _get_cutoff_timestamp(time_period)
                filtered_positions = []
                for pos in positions:
                    timestamp = pos.get("timestamp") or pos.get("time") or pos.get("closedAt") or pos.get("updatedAt")
                    if timestamp:
                        if isinstance(timestamp, str):
                            try:
                                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                                pos_timestamp = int(dt.timestamp())
                            except:
                                continue
                        else:
                            pos_timestamp = int(timestamp)
                        
                        if pos_timestamp >= cutoff_timestamp:
                            filtered_positions.append(pos)
                positions = filtered_positions
            
            return positions

        # If limit is None, fetch ALL data using pagination (no maximum limit)
        all_positions = []
        fetch_limit = 1000  # Fetch in chunks
        current_offset = offset or 0
        
        while True:
            params["limit"] = fetch_limit
            params["offset"] = current_offset
            
            response = await async_client.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if not isinstance(data, list) or not data:
                break
                
            all_positions.extend(data)
            
            # If we got fewer results than requested, we've reached the end
            if len(data) < fetch_limit:
                break
            
            current_offset += len(data)
        
        # Apply time period filter if specified
        if time_period and time_period != "all":
            cutoff_timestamp = _get_cutoff_timestamp(time_period)
            filtered_positions = []
            for pos in all_positions:
                # Check various timestamp fields that might exist
                timestamp = pos.get("timestamp") or pos.get("time") or pos.get("closedAt") or pos.get("updatedAt")
                if timestamp:
                    # Handle both Unix timestamp (int) and ISO string
                    if isinstance(timestamp, str):
                        try:
                            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                            pos_timestamp = int(dt.timestamp())
                        except:
                            continue
                    else:
                        pos_timestamp = int(timestamp)
                    
                    if pos_timestamp >= cutoff_timestamp:
                        filtered_positions.append(pos)
            all_positions = filtered_positions
        
        return all_positions
>>>>>>> 1e267d7cee08180e9c110108b558c48504150e5b

    except httpx.HTTPStatusError as e:
        print(f"Error fetching closed positions from Polymarket API: {str(e)}")
        return []
    except Exception as e:
        print(f"Unexpected error fetching closed positions: {str(e)}")
        return []


def _get_cutoff_timestamp(time_period: str) -> int:
    """
    Get cutoff timestamp for a time period.
    
    Args:
        time_period: Time period ("day", "week", "month")
    
    Returns:
        Unix timestamp cutoff
    """
    now = datetime.utcnow()
    if time_period == "day":
        cutoff = now - timedelta(days=1)
    elif time_period == "week":
        cutoff = now - timedelta(days=7)
    elif time_period == "month":
        # Current calendar month (from 1st day of current month)
        cutoff = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        return 0  # No cutoff for "all"
    
    return int(cutoff.timestamp())


async def fetch_portfolio_value(wallet_address: str) -> float:
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
        
<<<<<<< HEAD
        async with DNSAwareAsyncClient(timeout=30.0) as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
=======
        response = await async_client.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()
>>>>>>> 1e267d7cee08180e9c110108b558c48504150e5b
        # API returns list of objects: [{"user": "...", "value": 0.041534}]
        if isinstance(data, list) and len(data) > 0:
            return float(data[0].get("value", 0.0))
        return 0.0
    except httpx.HTTPStatusError as e:
        print(f"Error fetching portfolio value from Polymarket API: {str(e)}")
        return 0.0
    except Exception as e:
        print(f"Unexpected error fetching portfolio value: {str(e)}")
        return 0.0


async def fetch_leaderboard_stats(wallet_address: str, time_period: str = "all") -> Dict[str, float]:
    """
    Fetch stats (volume, pnl) for a user from the Leaderboard API for a specific time period.
    
    Args:
        wallet_address: Ethereum wallet address (0x...)
        time_period: Time period ("day", "week", "month", "all")
    
    Returns:
        Dictionary with "volume" and "pnl" keys
    """
    try:
        url = "https://data-api.polymarket.com/v1/leaderboard"
        params = {
            "timePeriod": time_period,
            "orderBy": "VOL",
            "limit": 1,
            "offset": 0,
            "category": "overall",
            "user": wallet_address
        }
        
        response = await async_client.get(url, params=params)
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
    except httpx.HTTPStatusError as e:
        print(f"Error fetching leaderboard stats from Polymarket API: {str(e)}")
        return {"volume": 0.0, "pnl": 0.0}
    except Exception as e:
        print(f"Unexpected error fetching leaderboard stats: {str(e)}")
        return {"volume": 0.0, "pnl": 0.0}


def fetch_market_orders(market_slug: str, limit: int = 5000, offset: int = 0) -> Dict[str, Any]:
    """
    Fetch market orders from Polymarket Data API only (no CLOB, Dome, Gamma, etc.).
    Uses the trades endpoint filtered by market slug.
    
    Args:
        market_slug: Market slug identifier
        limit: Maximum number of orders to return (default: 5000)
        offset: Offset for pagination (default: 0)
    
    Returns:
        Dictionary with orders list and pagination info
    """
    try:
        # Use trades endpoint
        trades_url = "https://data-api.polymarket.com/trades"
        
        all_trades_data = []
        current_offset = 0 # API offset
        # We need to fetch enough to cover our (offset + limit) requirements
        target_count = limit + offset
        fetched_so_far = 0
        
        # Helper to get sync client (reusing global if available or creating new)
        # Note: In the viewed code, 'sync_client' usage suggested a global, but we should be safe.
        # Use a new client to be sure, or 'httpx.get'.
        
        while fetched_so_far < target_count:
            # Batch size for API (max usually 1000)
            batch_limit = 1000
            
            trades_params = {
                "market": market_slug,
                "limit": batch_limit,
                "offset": current_offset
            }
            
            # Using httpx.get directly for sync call as seen in previous code ('sync_client' might be custom)
            # Assuming 'sync_client' is available in scope or falling back to httpx
            try:
                # Try using the global sync_client if it was defined in the file
                response = sync_client.get(trades_url, params=trades_params)
            except NameError:
                 # Fallback if sync_client isn't defined locally
                 with DNSAwareClient() as client:
                    response = client.get(trades_url, params=trades_params)

            response.raise_for_status()
            
            batch_data = response.json()
            
            if not isinstance(batch_data, list) or not batch_data:
                break
                
            all_trades_data.extend(batch_data)
            fetched_count = len(batch_data)
            fetched_so_far += fetched_count
            current_offset += fetched_count
            
            if fetched_count < batch_limit:
                # End of results
                break
        
        trades_data = all_trades_data
        
        if not trades_data:
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
        
    except httpx.HTTPStatusError as e:
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
    except httpx.RequestError as e:
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
        
        response = await async_client.get(url, params=params)
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
            
            response = sync_client.get(url, params=params)
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
        except httpx.RequestError:
            # Continue to next category if this one fails
            continue
        except Exception:
            # Continue to next category if this one fails
            continue
    
    # If all categories failed, return None
    return None

