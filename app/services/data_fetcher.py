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
    "polymarket.com",
    "www.polymarket.com"
}

# Cache for resolved IPs to avoid repeated DNS queries
DNS_CACHE: Dict[str, str] = {
    "data-api.polymarket.com": "104.18.34.205", 
    "gamma-api.polymarket.com": "104.18.34.205",
    "polymarket.com": "104.18.34.205",
    "www.polymarket.com": "104.18.34.205"
}

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"

def get_standard_headers(host: Optional[str] = None) -> Dict[str, str]:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json, */*",
        "Connection": "keep-alive",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache"
    }
    if host:
        headers["Host"] = host
        headers["Origin"] = f"https://{host}"
        headers["Referer"] = f"https://{host}/"
    return headers

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

def _skip_dns_bypass() -> bool:
    """When True, do not replace hostnames with IP (use for server to avoid 403 from Polymarket)."""
    import os
    return os.getenv("DISABLE_DNS_BYPASS", "").lower() in ("1", "true", "yes")


class DNSAwareAsyncClient(httpx.AsyncClient):
    """
    HTTPX AsyncClient that automatically resolves problematic domains 
    via secure DNS to bypass local hijacking.
    Set DISABLE_DNS_BYPASS=true on server to use hostnames (avoids 403 from Polymarket/Cloudflare).
    """
    async def request(self, method: str, url: httpx.URL | str, **kwargs) -> httpx.Response:
        url_obj = httpx.URL(url)
        if not _skip_dns_bypass() and url_obj.host in HIJACKED_DOMAINS:
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
    Set DISABLE_DNS_BYPASS=true on server to use hostnames (avoids 403).
    """
    def request(self, method: str, url: httpx.URL | str, **kwargs) -> httpx.Response:
        url_obj = httpx.URL(url)
        if not _skip_dns_bypass() and url_obj.host in HIJACKED_DOMAINS:
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
sync_client = DNSAwareClient(
    timeout=30.0,
    headers=get_standard_headers("polymarket.com")
)

# Shared async client instance for extreme performance
# We use a single instance to reuse connection pools and SSL handshakes
async_client = DNSAwareAsyncClient(
    timeout=httpx.Timeout(30.0, connect=10.0),
    limits=httpx.Limits(max_connections=100, max_keepalive_connections=50),
    headers=get_standard_headers("polymarket.com")
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
    Uses parallel fetching for max speed.
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
        
        # Base params for all requests
        base_params = {
            "order": "volume",
            "ascending": "false"
        }
        
        if active is not None:
            base_params["active"] = str(active).lower()
        if closed is not None:
            base_params["closed"] = str(closed).lower()
        if archived is not None:
            base_params["archived"] = str(archived).lower()
        if tag_slug:
            base_params["tag_slug"] = tag_slug
            
        # Helper to process events into markets
        def process_events(events_list):
            processed_markets = []
            for event in events_list:
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
                    "openInterest": float(event.get("openInterest", 0) or event.get("open_interest", 0)),
                    "image": event.get("image"),
                    "icon": event.get("icon"),
                    "startDate": event.get("startDate"),
                    "endDate": event.get("endDate"),
                    "end_date": event.get("endDate"),
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
                    "markets_count": len(event.get("markets", [])),
                    "featured": event.get("featured", False),
                    "restricted": event.get("restricted", False),
                }
                
                # Extract outcome prices per previous logic
                event_markets = event.get("markets", [])
                if event_markets and isinstance(event_markets, list) and len(event_markets) > 0:
                    primary_market = event_markets[0]
                    outcomes = primary_market.get("outcomes", [])
                    outcome_prices = primary_market.get("outcomePrices", [])
                    
                    if outcomes and outcome_prices and len(outcomes) == len(outcome_prices):
                        try:
                            prices_map = {}
                            for i, outcome in enumerate(outcomes):
                                name = str(outcome).strip()
                                price = float(outcome_prices[i])
                                prices_map[name] = price
                            
                            market["outcomePrices"] = prices_map
                            if len(outcome_prices) > 0:
                                market["price"] = float(outcome_prices[0])
                        except (ValueError, TypeError):
                            pass
                
                processed_markets.append(market)
            return processed_markets

        # Initial request to get total results and first page
        fetch_limit = min(limit, 100) # Max 100 per request
        initial_params = base_params.copy()
        initial_params["limit"] = fetch_limit
        initial_params["offset"] = offset
        
        response = await async_client.get(url, params=initial_params)
        response.raise_for_status()
        data = response.json()
        
        events = data.get("data", [])
        pagination = data.get("pagination", {})
        total_results = pagination.get("totalResults", 0)
        
        all_markets = process_events(events)
        
        # If we need more and there are more
        needed_more = limit - len(all_markets)
        has_more = pagination.get("hasMore", False)
        
        if needed_more > 0 and has_more:
            # Parallel fetch logic
            PARALLEL_BATCH_SIZE = 10
            batch_offset = offset + len(all_markets)
            
            # Don't fetch more than total available
            # Note: Gamma API totalResults is sometimes approximate or capped, relying on hasMore is safer
            # But we can assume we can fetch up to 'limit' total
            
            # We will loop until we have enough
            while needed_more > 0:
                tasks = []
                current_batch_count = 0
                
                for i in range(PARALLEL_BATCH_SIZE):
                     if current_batch_count >= needed_more:
                         break
                         
                     req_offset = batch_offset + (i * 100)
                     # Don't go beyond total results if we know it
                     if req_offset >= total_results:
                         break

                     req_limit = min(100, needed_more - current_batch_count)
                     if req_limit <= 0:
                         break
                         
                     task_params = base_params.copy()
                     task_params["limit"] = req_limit
                     task_params["offset"] = req_offset
                     tasks.append(async_client.get(url, params=task_params))
                     current_batch_count += req_limit
                
                if not tasks:
                    break
                    
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                batch_has_end = False
                fetched_in_batch = 0
                
                for res in results:
                    if isinstance(res, Exception) or res.status_code != 200:
                        print(f"✗ Error in parallel fetch markets")
                        continue
                        
                    p_data = res.json()
                    p_events = p_data.get("data", [])
                    p_mkts = process_events(p_events)
                    all_markets.extend(p_mkts)
                    fetched_in_batch += len(p_mkts)
                    
                    if not p_data.get("pagination", {}).get("hasMore", False) or not p_events:
                        batch_has_end = True
                
                needed_more = limit - len(all_markets)
                batch_offset += (len(tasks) * 100)
                
                if batch_has_end or fetched_in_batch == 0:
                    break

        pagination_info = {
            "limit": limit,
            "offset": offset,
            "total": total_results,
            "has_more": len(all_markets) < total_results and (offset + len(all_markets) < total_results)
        }
            
        print(f"✓ Successfully fetched {len(all_markets)} markets from Gamma API (Parallel)")
        return all_markets, pagination_info
    
    except httpx.HTTPStatusError as e:
        print(f"✗ HTTP error fetching markets from Gamma API: {e}")
        return [], {"limit": limit, "offset": offset, "total": 0, "has_more": False}
    except httpx.RequestError as e:
        print(f"✗ Request error fetching markets from Gamma API: {e}")
        return [], {"limit": limit, "offset": offset, "total": 0, "has_more": False}
    except Exception as e:
        print(f"✗ Unexpected error fetching markets from Gamma API: {e}")
        return [], {"limit": limit, "offset": offset, "total": 0, "has_more": False}
    return [], pagination_info


async def fetch_resolved_markets(limit: Optional[int] = None) -> List[Dict]:
    """
    Fetch resolved markets from Polymarket API (wrapper for backward compatibility).
    
    Args:
        limit: Maximum number of markets to fetch (default: 50 from config for testing)
    """
    markets, _ = await fetch_markets(status="resolved", limit=limit)
    return markets


async def fetch_total_markets_count(include_resolved: bool = False) -> int:
    """
    Fetch total markets count from Gamma API (lightweight request).
    Uses limit=1 to minimize payload; returns totalResults from pagination.
    If include_resolved=True, returns active + closed (resolved) count.
    """
    try:
        url = "https://gamma-api.polymarket.com/events/pagination"
        base_params = {"limit": 1, "offset": 0, "order": "volume", "ascending": "false"}

        # Active count
        active_params = {**base_params, "active": "true", "closed": "false"}
        resp = await async_client.get(url, params=active_params)
        resp.raise_for_status()
        active_total = int(resp.json().get("pagination", {}).get("totalResults", 0) or 0)

        if not include_resolved:
            return active_total

        # Closed/resolved count
        closed_params = {**base_params, "closed": "true"}
        resp2 = await async_client.get(url, params=closed_params)
        resp2.raise_for_status()
        closed_total = int(resp2.json().get("pagination", {}).get("totalResults", 0) or 0)

        return active_total + closed_total
    except Exception as e:
        print(f"Error fetching total markets count: {e}")
        return 0


async def fetch_volume_and_tvl_from_gamma_events(limit: int = 500) -> tuple[float, float, int]:
    """
    Fallback: Fetch volume and TVL from Gamma API /events endpoint (non-pagination).
    Used when events/pagination returns empty (e.g. network/DNS issues).
    Returns (total_volume, total_tvl, event_count).
    """
    try:
        url = "https://gamma-api.polymarket.com/events"
        params = {"limit": limit, "active": "true", "closed": "false"}
        response = await async_client.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, list):
            return 0.0, 0.0, 0
        total_volume = sum(float(e.get("volume", 0) or 0) for e in data)
        total_tvl = sum(float(e.get("liquidity", 0) or 0) for e in data)
        return total_volume, total_tvl, len(data)
    except Exception as e:
        print(f"Error fetching volume/TVL from Gamma /events: {e}")
        return 0.0, 0.0, 0


async def fetch_open_interest() -> float:
    """
    Fetch total open interest from Polymarket Data API.
    Uses https://data-api.polymarket.com/oi endpoint.
    Returns the sum of all market open interest values in USDC.
    """
    try:
        url = "https://data-api.polymarket.com/oi"
        response = await async_client.get(url)
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, list):
            return 0.0
        total = sum(float(item.get("value", 0) or 0) for item in data)
        return total
    except Exception as e:
        print(f"Error fetching open interest from Data API: {e}")
        return 0.0


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


async def get_market_resolution(market_id: str, markets: List[Dict], client: Optional[httpx.AsyncClient] = None) -> Optional[str]:
    """Get the resolution (YES/NO) for a given market ID."""
    market = get_market_by_id(market_id, markets)
    
    # If market not found in our list, try fetching from Polymarket API
    if not market:
        market = await fetch_market_by_slug(market_id, client=client)
        if market:
            # Add to markets list for future lookups
            markets.append(market)
    
    if not market:
        return None
    
    # Check tokens array (CLOB API format)
    tokens = market.get("tokens")
    if tokens and isinstance(tokens, list):
        for token in tokens:
            if token.get("winner") is True:
                return str(token.get("outcome"))
    
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
    if market.get("resolved") or market.get("isResolved") or market.get("is_resolved") or market.get("closed"):
        # Try to infer from other fields
        outcome = market.get("outcome") or market.get("winningOutcome") or market.get("winning_outcome")
        if outcome:
            outcome_str = str(outcome).lower()
            if "yes" in outcome_str or outcome_str == "1" or "true" in outcome_str:
                return "YES"
            elif "no" in outcome_str or outcome_str == "0" or "false" in outcome_str:
                return "NO"
        
        # If it's closed but no outcome found in standard fields, check if we missed the tokens check above
        # (Already done at valid CLOB response start)
    
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


async def fetch_market_by_slug(market_slug: str, client: Optional[httpx.AsyncClient] = None) -> Optional[Dict]:
    """
    Fetch market details by slug or conditionId from Polymarket API (async).
    
    Args:
        market_slug: Market slug identifier OR conditionId (0x...)
        client: Optional shared HTTP client
    
    Returns:
        Market dictionary or None if not found
    """
    try:
        # Use provided client or create a new one
        if client:
            return await _fetch_market_internal(client, market_slug)
        else:
            async with DNSAwareAsyncClient(timeout=10.0) as new_client:
                return await _fetch_market_internal(new_client, market_slug)
    except Exception as e:
        print(f"Error fetching market details for '{market_slug}': {e}")
    return None

async def _fetch_market_internal(client: httpx.AsyncClient, market_slug: str) -> Optional[Dict]:
    """Internal helper to fetch market with a guaranteed client"""
    # Case 1: conditionId Lookup (starts with 0x)
    if market_slug.startswith("0x") and len(market_slug) >= 42:
        # Use CLOB API for reliable conditionId lookup
        # https://clob.polymarket.com/markets/{condition_id}
        clob_url = f"https://clob.polymarket.com/markets/{market_slug}"
        try:
            response = await client.get(clob_url)
            if response.status_code == 200:
                data = response.json()
                if data and data.get("condition_id") == market_slug:
                    return data
        except Exception as e:
            print(f"Error fetching from CLOB API /markets: {e}")

    # Case 2: Slug Lookup
    gamma_url = "https://gamma-api.polymarket.com/events"
    params = {"slug": market_slug}
    
    try:
        response = await client.get(gamma_url, params=params)
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list) and len(data) > 0:
                event = data[0]
                markets = event.get("markets", [])
                if markets:
                    market = markets[0]
                    market["input_slug"] = market_slug 
                    
                    # Copy event-level data to market
                    if not market.get("title") and event.get("title"):
                        market["title"] = event.get("title")
                    if not market.get("icon") and event.get("icon"):
                        market["icon"] = event.get("icon")
                    if not market.get("description") and event.get("description"):
                        market["description"] = event.get("description")
                    
                    # CRITICAL: Copy tags from event to market for category extraction
                    if event.get("tags"):
                        market["tags"] = event.get("tags")
                    
                    return market
    except Exception as e:
        print(f"Error fetching from Gamma API /events: {e}")
    
    return None


def get_market_category(market: Dict) -> str:
    """Extract category from market data. Defaults to 'Uncategorized'."""
    from app.core.constants import DEFAULT_CATEGORY
    
    category = market.get("category") or market.get("group") or market.get("tags")
    
    # If tags is a list of dictionaries (Polymarket format)
    if isinstance(category, list) and category:
        first_tag = category[0]
        # Extract label from tag dictionary
        if isinstance(first_tag, dict):
            return first_tag.get("label", DEFAULT_CATEGORY)
        # If it's already a string
        return str(first_tag)
    
    # If category is already a string
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
            
        # If limit is None, fetch ALL data using parallel pagination
        all_positions = []
        seen_ids = set()
        fetch_limit = 50  # API max is 50
        initial_offset = offset or 0
        
        # Step 1: Fetch first page
        params["limit"] = fetch_limit
        params["offset"] = initial_offset
        
        response = await async_client.get(url, params=params)
        response.raise_for_status()
        first_page_data = response.json()
        
        if not isinstance(first_page_data, list) or not first_page_data:
            return []
            
        # Process first page
        for item in first_page_data:
            item_id = item.get("conditionId") or item.get("condition_id") or item.get("asset")
            if item_id:
                seen_ids.add(item_id)
            all_positions.append(item)
            
        if len(first_page_data) < fetch_limit:
            return all_positions
            
        # Step 2: Parallel fetch (rate-limited to avoid 403)
        PARALLEL_BATCH_SIZE = 6
        BATCH_DELAY = 0.4
        batch_offset = initial_offset + fetch_limit
        more_data_available = True
        MAX_POSITIONS = 1000000
        
        print(f"DEBUG: Starting parallel fetch for active positions. Initial count: {len(all_positions)}")
        import time
        t_start = time.time()
        
        while more_data_available and len(all_positions) < MAX_POSITIONS:
            tasks = []
            for i in range(PARALLEL_BATCH_SIZE):
                current_req_offset = batch_offset + (i * fetch_limit)
                task_params = params.copy()
                task_params["offset"] = current_req_offset
                tasks.append(async_client.get(url, params=task_params))
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            batch_has_end = False
            valid_batch_items = []
            
            for res in results:
                if isinstance(res, Exception):
                    continue
                if res.status_code != 200:
                    continue
                    
                page_data = res.json()
                if isinstance(page_data, list) and page_data:
                    valid_batch_items.extend(page_data)
                    if len(page_data) < fetch_limit:
                        batch_has_end = True
                else:
                    batch_has_end = True
            
            if valid_batch_items:
                for item in valid_batch_items:
                    item_id = item.get("conditionId") or item.get("condition_id") or item.get("asset")
                    if item_id:
                        if item_id not in seen_ids:
                            seen_ids.add(item_id)
                            all_positions.append(item)
                    else:
                        all_positions.append(item)
            
            if batch_has_end or len(valid_batch_items) == 0:
                more_data_available = False
            else:
                batch_offset += (PARALLEL_BATCH_SIZE * fetch_limit)
                await asyncio.sleep(BATCH_DELAY)
                
        t_end = time.time()
        print(f"✓ fetched {len(all_positions)} active positions in {round(t_end - t_start, 2)}s")
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
        
        response = await async_client.get(url, params=params)
        response.raise_for_status()
        
        pnl_data = response.json()
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
        
        response = await async_client.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()
        if isinstance(data, dict):
            return data
        return None
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 403 and not _skip_dns_bypass():
            print("Profile stats 403 (on server run with: DISABLE_DNS_BYPASS=true)")
        else:
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
    Uses parallel fetching for max speed.
    
    Args:
        wallet_address: Ethereum wallet address (0x...)
        activity_type: Optional activity type filter (e.g., "REDEEM", "TRADE")
        limit: Optional limit. If None, fetches all pages.
        offset: Optional offset
    
    Returns:
        List of activity dictionaries
    """
    try:
        url = f"{settings.POLYMARKET_DATA_API_URL}/activity"
        params = {"user": wallet_address}
        
        if activity_type:
            params["type"] = activity_type
        
        # Determine total target limit
        # If limit is specified, use it (e.g. 1000)
        # If limit is None, use a safety cap (e.g. 10000) to prevent infinite loops
        target_limit = limit if limit is not None else 1000000
        
        all_activities = []
        seen_ids = set()
        fetch_limit = 50  # API max is 50
        initial_offset = offset or 0
        
        # Step 1: Fetch first page
        params["limit"] = fetch_limit
        params["offset"] = initial_offset
        
        # Retry logic for first page
        max_retries = 3
        response = None
        for attempt in range(max_retries):
            try:
                response = await async_client.get(url, params=params)
                response.raise_for_status()
                break
            except (httpx.HTTPStatusError, httpx.TimeoutException, httpx.RequestError) as e:
                if attempt == max_retries - 1:
                    print(f"✗ Failed to fetch first page of activity: {e}")
                    return []
                await asyncio.sleep(1)

        first_page_data = response.json()
        
        if not isinstance(first_page_data, list) or not first_page_data:
            return []
            
        # Process first page
        for item in first_page_data:
            item_id = item.get("id") or item.get("transactionHash") or item.get("tx_hash")
            if item_id:
                seen_ids.add(item_id)
            all_activities.append(item)
            
        # If we already have enough or that was everything, return
        if len(all_activities) >= target_limit or len(first_page_data) < fetch_limit:
            return all_activities[:target_limit]
            
        # Step 2: Parallel fetch for remaining
        MAX_BATCH_SIZE = 15
        current_batch_size = 5
        
        batch_offset = initial_offset + fetch_limit
        more_data_available = True
        
        while more_data_available and len(all_activities) < target_limit:
            tasks = []
            # Calculate how many more we need
            remaining_needed = target_limit - len(all_activities)
            
            # Prepare batch of requests
            for i in range(current_batch_size):
                current_req_offset = batch_offset + (i * fetch_limit)
                
                # Stop if we are initiating requests beyond reasonable bounds for this batch
                if len(tasks) * fetch_limit >= remaining_needed:
                    break
                    
                task_params = params.copy()
                task_params["offset"] = current_req_offset
                tasks.append(async_client.get(url, params=task_params))
            
            if not tasks:
                break
                
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            batch_has_end = False
            valid_batch_items = []
            fetched_in_batch = 0
            
            for res in results:
                if isinstance(res, Exception):
                    # print(f"✗ Error in parallel fetch activity: {res}")
                    continue
                    
                if res.status_code != 200:
                    # print(f"✗ API Error in parallel fetch activity: {res.status_code}")
                    continue
                
                page_data = res.json()
                if isinstance(page_data, list) and page_data:
                    valid_batch_items.extend(page_data)
                    fetched_in_batch += len(page_data)
                    if len(page_data) < fetch_limit:
                        batch_has_end = True
                else:
                    batch_has_end = True
            
            # Order matters less here as we sort later, but ideally we append in order
            if valid_batch_items:
                for item in valid_batch_items:
                    item_id = item.get("id") or item.get("transactionHash") or item.get("tx_hash")
                    if item_id:
                        if item_id not in seen_ids:
                            seen_ids.add(item_id)
                            all_activities.append(item)
                    else:
                        all_activities.append(item)
            
            if batch_has_end or fetched_in_batch == 0:
                more_data_available = False
            else:
                batch_offset += (len(tasks) * fetch_limit)
                current_batch_size = MAX_BATCH_SIZE
        
        print(f"✓ Successfully fetched {len(all_activities)} activities for {wallet_address} (Target: {target_limit})")
        return all_activities[:target_limit]

    except httpx.HTTPStatusError as e:
        print(f"Error fetching user activity from Polymarket API: {str(e)}")
        return []
    except Exception as e:
        print(f"Unexpected error fetching user activity: {str(e)}")
        return []


async def fetch_user_trades(
    wallet_address: str, 
    limit: Optional[int] = None, 
    offset: Optional[int] = None
) -> List[Dict]:
    """
    Fetch user trades from Polymarket Data API (async version).
    Uses parallel fetching for max speed.
    
    Args:
        wallet_address: Ethereum wallet address (0x...)
        limit: Maximum number of trades. If None, fetches all pages.
        offset: Pagination offset
    
    Returns:
        List of trade dictionaries
    """
    try:
        url = f"{settings.POLYMARKET_DATA_API_URL}/trades"
        
        # Determine total target limit: use explicit limit or safety cap (10000)
        # Determine total target limit: use explicit limit or default to a very high number if None
        # User requested ALL data, so we set a very high safety cap (e.g. 1M) or handle None
        target_limit = limit if limit is not None else 1000000

        all_trades = []
        fetch_limit = 100  # API max is usually 100
        initial_offset = offset or 0
        
        # Step 1: Fetch first page to see if we have data and how much
        params = {"user": wallet_address, "limit": fetch_limit, "offset": initial_offset}
        
        # Retry logic for first page
        max_retries = 3
        response = None
        for attempt in range(max_retries):
            try:
                response = await async_client.get(url, params=params)
                response.raise_for_status()
                break
            except (httpx.HTTPStatusError, httpx.TimeoutException, httpx.RequestError) as e:
                if attempt == max_retries - 1:
                    print(f"✗ Failed to fetch first page of trades: {e}")
                    return []
                await asyncio.sleep(1)

        first_page_data = response.json()
        
        if not isinstance(first_page_data, list) or not first_page_data:
            return []
            
        all_trades.extend(first_page_data)
        
        # If we already have enough or that was everything, return
        if len(all_trades) >= target_limit or len(first_page_data) < fetch_limit:
            return all_trades[:target_limit]

        # Step 2: Parallel fetch for remaining
        PARALLEL_BATCH_SIZE = 20 # Increased from 10 to 20 for faster full history fetch
        batch_offset = initial_offset + fetch_limit
        more_data_available = True
        
        while more_data_available and len(all_trades) < target_limit:
            tasks = []
            # Calculate how many more we need
            remaining_needed = target_limit - len(all_trades)
            
            # Prepare batch of requests
            for i in range(PARALLEL_BATCH_SIZE):
                current_req_offset = batch_offset + (i * fetch_limit)
                
                 # Stop if we are initiating requests beyond reasonable bounds for this batch
                if len(tasks) * fetch_limit >= remaining_needed:
                    break
                
                task_params = params.copy()
                task_params["offset"] = current_req_offset
                tasks.append(async_client.get(url, params=task_params))
            
            if not tasks:
                break
                
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            batch_has_end = False
            valid_batch_items = []
            fetched_in_batch = 0
            
            for res in results:
                if isinstance(res, Exception):
                    continue
                if res.status_code != 200:
                    continue
                
                page_data = res.json()
                if isinstance(page_data, list) and page_data:
                    valid_batch_items.extend(page_data)
                    fetched_in_batch += len(page_data)
                    if len(page_data) < fetch_limit:
                        batch_has_end = True
                else:
                    batch_has_end = True
            
            if valid_batch_items:
                all_trades.extend(valid_batch_items)
            
            if batch_has_end or fetched_in_batch == 0:
                more_data_available = False
            else:
                batch_offset += (len(tasks) * fetch_limit)
        
        print(f"✓ Successfully fetched {len(all_trades)} trades for {wallet_address} (Target: {target_limit})")
        return all_trades[:target_limit]
        


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
    Uses parallel fetching for max speed.
    
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
        url = f"{settings.POLYMARKET_DATA_API_URL}/v1/closed-positions"
        params = {
            "user": wallet_address,
            "sortBy": "timestamp",
            "sortDirection": "DESC"
        }
        
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

        # If limit is None, fetch ALL data using PARALLEL pagination
        all_positions = []
        seen_ids = set()
        fetch_limit = 50  # API max is 50
        initial_offset = offset or 0
        
        # Step 1: Fetch first page
        params["limit"] = fetch_limit
        params["offset"] = initial_offset
        
        response = await async_client.get(url, params=params)
        response.raise_for_status()
        first_page_data = response.json()
        
        if not isinstance(first_page_data, list) or not first_page_data:
            return []
            
        # Process first page
        for item in first_page_data:
            item_id = item.get("asset") or item.get("id") or item.get("conditionId")
            if item_id:
                seen_ids.add(item_id)
            all_positions.append(item)
            
        if len(first_page_data) < fetch_limit:
            # Done in one page
            return _filter_by_time_period(all_positions, time_period)
            
        # Step 2: Parallel fetch for remaining (rate-limited to avoid 403 / block)
        MAX_BATCH_SIZE = 6  # Keep low to avoid Polymarket rate limit
        BATCH_DELAY_SECONDS = 0.6  # Delay between batches to stay under rate limit
        current_batch_size = 4

        batch_offset = initial_offset + fetch_limit
        more_data_available = True
        
        while more_data_available:
            tasks = []
            for i in range(current_batch_size):
                current_req_offset = batch_offset + (i * fetch_limit)
                task_params = params.copy()
                task_params["offset"] = current_req_offset
                tasks.append(async_client.get(url, params=task_params))
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            batch_has_end = False
            valid_batch_items = []
            
            for res in results:
                if isinstance(res, Exception):
                    print(f"✗ Error in parallel fetch closed positions: {res}")
                    continue
                
                if res.status_code != 200:
                    print(f"✗ API Error in parallel fetch closed ps: {res.status_code}")
                    continue
                    
                page_data = res.json()
                if isinstance(page_data, list) and page_data:
                    valid_batch_items.extend(page_data)
                    if len(page_data) < fetch_limit:
                        batch_has_end = True
                else:
                    batch_has_end = True
            
            if valid_batch_items:
                for item in valid_batch_items:
                    item_id = item.get("asset") or item.get("id") or item.get("conditionId")
                    if item_id:
                        if item_id not in seen_ids:
                            seen_ids.add(item_id)
                            all_positions.append(item)
                    else:
                        all_positions.append(item)
            
            if batch_has_end or len(valid_batch_items) == 0:
                more_data_available = False
            else:
                batch_offset += (current_batch_size * fetch_limit)
                current_batch_size = min(current_batch_size + 1, MAX_BATCH_SIZE)
                await asyncio.sleep(BATCH_DELAY_SECONDS)
        
        print(f"✓ Successfully fetched {len(all_positions)} closed positions (Parallel)")
        return _filter_by_time_period(all_positions, time_period)

    except httpx.HTTPStatusError as e:
        print(f"Error fetching closed positions from Polymarket API: {str(e)}")
        return []
    except Exception as e:
        print(f"Unexpected error fetching closed positions: {str(e)}")
        return []

def _filter_by_time_period(positions: List[Dict], time_period: Optional[str]) -> List[Dict]:
    """Helper to filter positions by time period"""
    if not time_period or time_period == "all":
        return positions
        
    cutoff_timestamp = _get_cutoff_timestamp(time_period)
    filtered = []
    
    for pos in positions:
        timestamp = pos.get("timestamp") or pos.get("time") or pos.get("closedAt") or pos.get("updatedAt")
        if timestamp:
            pos_timestamp = 0
            if isinstance(timestamp, str):
                try:
                    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    pos_timestamp = int(dt.timestamp())
                except:
                    pass
            else:
                pos_timestamp = int(timestamp)
            
            if pos_timestamp >= cutoff_timestamp:
                filtered.append(pos)
                
    return filtered


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


def _get_cutoff_timestamp_for_period(period: str) -> int:
    """
    Get cutoff timestamp for dashboard period filter.
    period: "24h" -> last 1 day, "7d" -> last 7 days, "30d" -> last 30 days, "all" -> no cutoff.
    """
    if not period or period == "all":
        return 0
    now = datetime.utcnow()
    if period == "24h":
        return int((now - timedelta(days=1)).timestamp())
    if period == "7d":
        return int((now - timedelta(days=7)).timestamp())
    if period == "30d":
        return int((now - timedelta(days=30)).timestamp())
    return 0


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
        
        response = await async_client.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()
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


async def fetch_leaderboard_stats(wallet_address: str, time_period: str = "all", order_by: str = "VOL") -> Dict[str, float]:
    """
    Fetch stats (volume, pnl) for a user from the Leaderboard API for a specific time period.
    
    Args:
        wallet_address: Ethereum wallet address (0x...)
        time_period: Time period ("day", "week", "month", "all")
        order_by: Order by metric ("VOL" or "PNL")
    
    Returns:
        Dictionary with "volume" and "pnl" keys
    """
    try:
        url = "https://data-api.polymarket.com/v1/leaderboard"
        params = {
            "timePeriod": time_period,
            "orderBy": order_by,
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

            try:
                stats["rank"] = int(item.get("rank", 0))
            except (ValueError, TypeError):
                stats["rank"] = 0
            return stats
        return stats
    except httpx.HTTPStatusError as e:
        print(f"Error fetching leaderboard stats from Polymarket API: {str(e)}")
        return {"volume": 0.0, "pnl": 0.0}
    except Exception as e:
        print(f"Unexpected error fetching leaderboard stats: {str(e)}")
        return {"volume": 0.0, "pnl": 0.0}


def _leaderboard_item_to_winner(item: Dict) -> Dict:
    """Convert a leaderboard API item to biggest-winner dict."""
    return {
        "user": item.get("user") or item.get("proxyWallet") or "",
        "userName": item.get("userName"),
        "xUsername": item.get("xUsername"),
        "profileImage": item.get("profileImage"),
        "pnl": float(item.get("pnl", 0.0)),
        "vol": float(item.get("vol", 0.0)),
        "rank": item.get("rank"),
        "roi": float(item.get("roi", 0.0)) if item.get("roi") is not None else None,
        "winRate": float(item.get("winRate", 0.0)) if item.get("winRate") is not None else None,
        "totalTrades": int(item.get("totalTrades", 0)) if item.get("totalTrades") is not None else 0,
    }


async def fetch_biggest_winner_of_month() -> Optional[Dict]:
    """
    Fetch the biggest winner of the month from Polymarket Data API leaderboard.
    Returns the top trader by PnL for timePeriod=month, or None if unavailable.
    """
    winners = await fetch_biggest_winners_of_month(limit=1)
    return winners[0] if winners else None


async def fetch_biggest_winners_of_month(limit: int = 10) -> List[Dict]:
    """
    Fetch the list of biggest winners of the month from Polymarket Data API leaderboard.
    Returns top N traders by PnL for timePeriod=month (default 10).
    """
    try:
        url = "https://data-api.polymarket.com/v1/leaderboard"
        params = {
            "timePeriod": "month",
            "orderBy": "PNL",
            "limit": min(limit, 100),
            "offset": 0,
            "category": "overall",
        }
        response = await async_client.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        if isinstance(data, list):
            return [_leaderboard_item_to_winner(item) for item in data]
        return []
    except Exception as e:
        print(f"Error fetching biggest winners of month from Polymarket API: {str(e)}")
        return []


def _period_to_leaderboard_time(time_period: str) -> str:
    """Map dashboard period to leaderboard API timePeriod. API: day, week, month, all."""
    if time_period == "24h":
        return "day"
    if time_period == "7d":
        return "week"
    if time_period == "30d":
        return "month"
    return "all"


async def fetch_leaderboard_total_count(time_period: str = "all") -> Optional[int]:
    """
    Fetch total number of traders from Polymarket leaderboard API.
    Paginates through the leaderboard until fewer than limit results; total = last_offset + len(last_batch).
    Uses parallel batch fetches for speed (checks offsets 0, 2k, 4k, ... to find boundary, then narrows).
    time_period: "all", "24h", "7d", "30d" (maps to API day/week/month/all).
    """
    try:
        url = "https://data-api.polymarket.com/v1/leaderboard"
        limit = 50
        api_time = _period_to_leaderboard_time(time_period)

        async def fetch_page(offset: int) -> tuple[int, int]:
            params = {
                "timePeriod": api_time,
                "orderBy": "VOL",
                "limit": limit,
                "offset": offset,
                "category": "overall",
            }
            resp = await async_client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            count = len(data) if isinstance(data, list) else 0
            return offset, count

        # Probe offsets in parallel to find boundary (first offset with < limit results)
        probe_offsets = [0, 5000, 10000, 15000, 20000, 30000, 50000, 100000, 150000, 200000]
        tasks = [fetch_page(off) for off in probe_offsets]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Sort by offset and find first where count < limit
        valid = []
        for r in results:
            if isinstance(r, tuple) and len(r) == 2:
                off, cnt = r
                valid.append((off, cnt))
        valid.sort(key=lambda x: x[0])
        for off, cnt in valid:
            if cnt < limit:
                return off + cnt

        # All probes returned full page; use last probe offset + limit as floor
        return probe_offsets[-1] + limit
    except Exception as e:
        print(f"Error fetching leaderboard total count: {e}")
        return None


def _trade_timestamp(t: Dict) -> Optional[int]:
    """Extract Unix timestamp from a trade object. API may use 'timestamp', 't', or 'createdAt' (ISO)."""
    ts = t.get("timestamp") or t.get("t")
    if ts is not None:
        try:
            return int(ts)
        except (TypeError, ValueError):
            pass
    created = t.get("createdAt") or t.get("created_at")
    if created and isinstance(created, str):
        try:
            dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
            return int(dt.timestamp())
        except (ValueError, TypeError):
            pass
    return None


# Safety cap for total trades pagination (avoids runaway requests if API always returns full pages)
TRADES_COUNT_MAX_OFFSET = 500_000  # e.g. 500k trades = 500 requests of 1000


async def fetch_total_trades_count(period: str = "all") -> Optional[tuple[int, int, int]]:
    """
    Fetch total trades count from Polymarket Data API.
    Paginates until the API returns fewer than limit (no more data) or we hit TRADES_COUNT_MAX_OFFSET.
    period: "all" -> count all trades the API returns (paginated); "24h"/"7d"/"30d" -> count only in that window.
    Returns (total, buys, sells) or None on error.
    """
    try:
        url = "https://data-api.polymarket.com/trades"
        cutoff = _get_cutoff_timestamp_for_period(period) if period else 0

        if not cutoff:
            # All-time: paginate until API returns less than batch_size or we hit safety cap
            total = 0
            buys = 0
            sells = 0
            offset = 0
            batch_size = 1000
            while offset < TRADES_COUNT_MAX_OFFSET:
                response = await async_client.get(url, params={"limit": batch_size, "offset": offset})
                response.raise_for_status()
                data = response.json()
                if not isinstance(data, list) or len(data) == 0:
                    break
                total += len(data)
                buys += sum(1 for t in data if (t.get("side") or "").upper() == "BUY")
                sells += sum(1 for t in data if (t.get("side") or "").upper() == "SELL")
                if len(data) < batch_size:
                    break
                offset += batch_size
            return (total, buys, sells) if total > 0 else None

        # Period filter: fetch batches, count only trades with timestamp >= cutoff (newest first)
        total = 0
        buys = 0
        sells = 0
        offset = 0
        batch_size = 1000
        while offset < TRADES_COUNT_MAX_OFFSET:
            response = await async_client.get(url, params={"limit": batch_size, "offset": offset})
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, list) or len(data) == 0:
                break
            for t in data:
                ts = _trade_timestamp(t)
                if ts is not None and ts < cutoff:
                    # Trades are newest-first; rest of batch and all further batches are older
                    return (total, buys, sells) if total > 0 else None
                if ts is None or ts >= cutoff:
                    total += 1
                    if (t.get("side") or "").upper() == "BUY":
                        buys += 1
                    else:
                        sells += 1
            if len(data) < batch_size:
                break
            offset += batch_size
        return (total, buys, sells) if total > 0 else None
    except Exception as e:
        print(f"Error fetching total trades count: {e}")
        return None


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
            
            if not batch_data or not isinstance(batch_data, list):
                break
                
            all_trades_data.extend(batch_data)
            fetched_so_far += len(batch_data)
            current_offset += len(batch_data)
            
            if len(batch_data) < batch_limit:
                break
                
        # Slice to requested limit
        result_data = all_trades_data[:limit]
        
        return {
            "limit": limit,
            "offset": offset,
            "count": len(result_data),
            "orders": result_data
        }

    except Exception as e:
        print(f"Error fetching market orders: {e}")
        return {"limit": limit, "offset": offset, "count": 0, "orders": []}


def fetch_total_market_trades(market_slug: str) -> int:
    """
    Fetch the total count of trades for a market.
    Iterates through all pages to get an exact count.
    """
    try:
        trades_url = "https://data-api.polymarket.com/trades"
        total_count = 0
        current_offset = 0
        batch_limit = 1000
        
        while True:
            params = {
                "market": market_slug,
                "limit": batch_limit,
                "offset": current_offset
            }
            
            try:
                response = sync_client.get(trades_url, params=params)
            except NameError:
                with DNSAwareClient() as client:
                    response = client.get(trades_url, params=params)
            
            if response.status_code != 200:
                print(f"Error checking trade count (status {response.status_code})")
                break
                
            data = response.json()
            if not data or not isinstance(data, list):
                break
                
            count = len(data)
            total_count += count
            current_offset += count
            
            if count < batch_limit:
                break
                
        return total_count
        
    except Exception as e:
        print(f"Error fetching total market trades: {e}")
        return 0


def fetch_market_traders_aggregated(
    market_slug: str, 
    limit: int = 100, 
    offset: int = 0
) -> Dict[str, Any]:
    """
    Fetch all trades for a market and aggregate by trader address.
    Returns traders sorted by volume (descending).
    """
    try:
        # Fetch ALL trades for the market to ensure accurate aggregation
        # Warning: This can be slow for very liquid markets
        trades_url = "https://data-api.polymarket.com/trades"
        all_trades = []
        current_offset = 0
        batch_limit = 1000
        
        # Safety cap to prevent timeout on massive markets
        MAX_TRADES_TO_PROCESS = 10000 
        
        while len(all_trades) < MAX_TRADES_TO_PROCESS:
            params = {
                "market": market_slug,
                "limit": batch_limit,
                "offset": current_offset
            }
            
            try:
                response = sync_client.get(trades_url, params=params)
            except NameError:
                with DNSAwareClient() as client:
                    response = client.get(trades_url, params=params)
                    
            if response.status_code != 200:
                break
                
            data = response.json()
            if not data or not isinstance(data, list):
                break
                
            all_trades.extend(data)
            current_offset += len(data)
            
            if len(data) < batch_limit:
                break
        
        # Aggregate by trader
        traders_map = {}
        
        for trade in all_trades:
            # Polymarket data API trades usually have 'taker' and 'maker'
            # But the detailed public trade endpoint usually returns a list of individual fills which include 'match_id', 'price', 'size', 'side', 'timestamp'
            # AND importantly 'maker_address' and 'taker_address' OR just an address if it's from the perspective of a user.
            # However, /trades?market=slug returns a global trade list. 
            # It normally contains 'maker_address' and 'taker_address'.
            
            maker = trade.get("maker_address")
            taker = trade.get("taker_address")
            size = float(trade.get("size", 0) or 0)
            price = float(trade.get("price", 0) or 0)
            volume = size * price
            
            # Update Maker
            if maker:
                if maker not in traders_map:
                    traders_map[maker] = {"address": maker, "volume": 0.0, "count": 0, "role": "maker"}
                traders_map[maker]["volume"] += volume
                traders_map[maker]["count"] += 1
                
            # Update Taker
            if taker:
                if taker not in traders_map:
                    traders_map[taker] = {"address": taker, "volume": 0.0, "count": 0, "role": "taker"}
                traders_map[taker]["volume"] += volume
                traders_map[taker]["count"] += 1

        # Convert to list
        traders_list = list(traders_map.values())
        
        # Sort by volume desc
        traders_list.sort(key=lambda x: x["volume"], reverse=True)
        
        # Pagination on aggregated result
        total_traders = len(traders_list)
        paginated_traders = traders_list[offset : offset + limit]
        
        return {
            "limit": limit,
            "offset": offset,
            "total": total_traders,
            "traders": paginated_traders
        }
        
    except Exception as e:
        print(f"Error aggregating market traders: {e}")
        return {
            "limit": limit, 
            "offset": offset, 
            "total": 0, 
            "traders": []
        }




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


async def fetch_user_traded_count(wallet_address: str) -> int:
    """Fetch the number of trades counted by Polymarket (/traded endpoint)."""
    try:
        url = f"{settings.POLYMARKET_DATA_API_URL}/traded"
        params = {"user": wallet_address}
        response = await async_client.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        # {"user":"0x...","traded":1312}
        if isinstance(data, dict):
            return int(data.get("traded", 0))
        return 0
    except Exception:
        return 0


async def fetch_user_profile_data_v2(wallet_address: str) -> Dict[str, Any]:
    """Fetch user profile data from the official Polymarket API."""
    try:
        url = "https://polymarket.com/api/profile/userData"
        params = {"address": wallet_address}
        response = await async_client.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except Exception:
        return {}


async def get_polymarket_build_id() -> str:
    """
    Dynamically retrieve the Polymarket Next.js build ID from the homepage.
    """
    try:
        url = "https://polymarket.com/"
        # Use sync client or shared async client
        response = await async_client.get(url)
        if response.status_code != 200:
            return "E318riu5ztoJC_Kf28jRT"
            
        html = response.text
        
        # Look for buildId in script tag: "buildId":"..."
        import re
        match = re.search(r'"buildId":"([^"]+)"', html)
        if match:
            build_id = match.group(1)
            print(f"📡 Found Polymarket Build ID: {build_id}")
            return build_id
        
        return "E318riu5ztoJC_Kf28jRT"
    except Exception as e:
        print(f"⚠️ Error retrieving build ID: {e}")
        return "E318riu5ztoJC_Kf28jRT"


async def fetch_live_trending_markets() -> List[Dict[str, Any]]:
    """
    Fetch trending markets from Polymarket Next.js JSON API.
    Standardizes output to the format requested by the user.
    """
    build_id = await get_polymarket_build_id()
    url = f"https://polymarket.com/_next/data/{build_id}/index.json"
    
    try:
        response = await async_client.get(url)
        response.raise_for_status()
        data = response.json()
        
        markets_list = []
        
        if "pageProps" in data:
            pp = data["pageProps"]
            if "dehydratedState" in pp:
                ds = pp["dehydratedState"]
                queries = ds.get("queries", [])
                if queries:
                    # Query 0 is typically the trending/homepage events
                    q = queries[0]
                    state = q.get("state", {})
                    data_obj = state.get("data", {})
                    
                    if isinstance(data_obj, dict) and "pages" in data_obj:
                        for page in data_obj["pages"]:
                            events = page.get("events", [])
                            for event in events:
                                markets = event.get("markets", [])
                                for m in markets:
                                    # Standardize to requested format
                                    markets_list.append({
                                        "id": m.get("id"),
                                        "conditionId": m.get("conditionId"),
                                        "question": m.get("question"),
                                        "slug": m.get("slug"),
                                        "outcomes": m.get("outcomes", []),
                                        "outcomePrices": m.get("outcomePrices", []),
                                        "bestAsk": m.get("bestAsk"),
                                        "bestBid": m.get("bestBid"),
                                        "spread": m.get("spread"),
                                        "active": m.get("active", True),
                                        "closed": m.get("closed", False),
                                        "archived": m.get("archived", False),
                                        "volume": m.get("volume"),
                                        "volume_num": m.get("volume_num"),
                                        "liquidity": m.get("liquidity"),
                                        "liquidity_num": m.get("liquidity_num"),
                                        "groupItemTitle": m.get("groupItemTitle"),
                                        "groupItemThreshold": m.get("groupItemThreshold"),
                                        "clobTokenIds": m.get("clobTokenIds", []),
                                        "rewardsMinSize": m.get("rewardsMinSize"),
                                        "rewardsMaxSpread": m.get("rewardsMaxSpread"),
                                        "holdingRewardsEnabled": m.get("holdingRewardsEnabled", False),
                                        "sportsMarketType": m.get("sportsMarketType"),
                                        "negRisk": m.get("negRisk", False),
                                        "orderPriceMinTickSize": m.get("orderPriceMinTickSize"),
                                        "image": m.get("image") or event.get("image"),
                                        "icon": m.get("icon") or event.get("icon"),
                                        "gameStartTime": m.get("gameStartTime"),
                                        "eventStartTime": event.get("startTime"),
                                        "startDate": event.get("startDate"),
                                        "endDate": event.get("endDate")
                                    })
        
        print(f"✓ Successfully fetched {len(markets_list)} live trending markets from Next.js API")
        return markets_list
    except Exception as e:
        print(f"⚠️ Error fetching live markets: {e}")
        return []


def _extract_wallet_from_html(html: str) -> Optional[str]:
    """Extract first valid wallet address (0x + 40 hex) from HTML/JSON."""
    import re
    patterns = [
        r'"userAddress":"(0x[a-fA-F0-9]{40})"',
        r'"proxyWallet":"(0x[a-fA-F0-9]{40})"',
        r'"address":"(0x[a-fA-F0-9]{40})"',
        r'"user":"(0x[a-fA-F0-9]{40})"',
        r'"wallet":"(0x[a-fA-F0-9]{40})"',
    ]
    for p in patterns:
        match = re.search(p, html)
        if match:
            return match.group(1)
    return None


def _extract_wallet_from_next_data(html: str) -> Optional[str]:
    """Extract wallet from Next.js __NEXT_DATA__ JSON."""
    import re
    import json
    match = re.search(
        r'<script id="__NEXT_DATA__" type="application/json">(.+?)</script>',
        html,
        re.DOTALL,
    )
    if not match:
        return None
    try:
        data = json.loads(match.group(1))
        page_props = data.get("pageProps", {})
        if "dehydratedState" in page_props:
            queries = page_props["dehydratedState"].get("queries", [])
            for q in queries:
                if "profile" in str(q.get("queryKey", "")):
                    user_data = q.get("state", {}).get("data", {})
                    addr = user_data.get("proxyWallet") or user_data.get("userAddress")
                    if addr and re.match(r"^0x[a-fA-F0-9]{40}$", addr):
                        return addr
        # Fallback: any proxyWallet/userAddress in pageProps
        for key in ("proxyWallet", "userAddress", "address"):
            addr = page_props.get(key)
            if addr and re.match(r"^0x[a-fA-F0-9]{40}$", addr):
                return addr
    except Exception:
        pass
    return None


async def fetch_wallet_address_from_profile_page(username: str) -> Optional[str]:
    """
    Fallback method: Fetch user profile page HTML and regex search for wallet address.
    Used when username is not found in DB or Leaderboard API.
    Tries both @username and username for URLs (handles 0x-prefix usernames).
    """
    import re
    # Normalize: no leading @ for URL path
    slug = username.lstrip("@").strip() if username else ""
    if not slug:
        return None
    urls_to_try = [f"https://polymarket.com/@{slug}"]
    # If username looks like a handle (not a wallet), also try without @ in path (some routers use both)
    if not (slug.startswith("0x") and len(slug) == 42 and re.match(r"^0x[a-fA-F0-9]{40}$", slug)):
        urls_to_try.append(f"https://polymarket.com/{slug}")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    for url in urls_to_try:
        try:
            print(f"Fetching profile HTML: {url}")
            resp = await async_client.get(url, headers=headers, follow_redirects=True)
            if resp.status_code != 200:
                continue
            html = resp.text
            addr = _extract_wallet_from_html(html)
            if addr:
                print(f"✓ Found address via profile scrape: {addr}")
                return addr
            addr = _extract_wallet_from_next_data(html)
            if addr:
                print(f"✓ Found address via __NEXT_DATA__: {addr}")
                return addr
        except Exception as e:
            print(f"Error scraping profile page {url}: {e}")
            continue
    return None


async def fetch_users_metrics_batch(user_addresses: List[str]) -> Dict[str, Dict]:
    """
    Fetch 'Safety Score' and 'Win Score' metrics for a list of users using Polymarket Data API (REST).
    Uses asyncio.gather to fetch data for all users in parallel for maximum speed.

    Metrics fetched:
    - Worst Loss: Lowest (most negative) realizedPnL from closed positions.
    - Win Stats: Count and Volume of trades with realizedPnL > 0.
    - Safety Score: 1 - (|worst_loss| / total_volume)
    - Win Score: 0.5 * (WinCount/TotalTrades) + 0.5 * (WinVolume/TotalVolume)

    Returns:
        Dict mapped by user address containing computed metrics.
    """
    if not user_addresses:
        return {}

    # Normalize addresses
    user_addresses = [addr.lower() for addr in user_addresses]

    async def fetch_single_user(addr: str) -> Optional[Dict]:
        try:
            # Parallel fetch of Leaderboard Stats (Volume) and Closed Positions (PnL history)
            # We fetch ALL closed positions (limit=None) to ensure we find the absolute worst loss
            # and accurate win rates. fetch_closed_positions handles parallel pagination internally.
            stats_task = fetch_leaderboard_stats(addr, time_period="all")
            positions_task = fetch_closed_positions(addr, limit=None) 
            
            stats, positions = await asyncio.gather(stats_task, positions_task)
            
            # --- Process Data ---
            
            # 1. Total Volume
            # Use leaderboard volume if available, else sum up positions (which is less accurate for open positions)
            total_volume = stats.get("volume", 0.0)
            
            # 2. Worst Loss and Win Stats
            worst_loss = 0.0
            win_count = 0
            win_volume = 0.0
            closed_positions_volume = 0.0
            total_trades = len(positions)
            
            for pos in positions:
                # PnL
                pnl = float(pos.get("realizedPnl") or pos.get("realizedPnL") or 0.0)
                
                # Volume (Invested Amount)
                # 'totalBought' is typically the amount spent.
                # 'size' * 'avgPrice' is another way.
                # Let's use 'totalBought' if available, else size * avgPrice.
                bought = float(pos.get("totalBought", 0.0))
                if bought == 0:
                     bought = float(pos.get("size", 0.0)) * float(pos.get("avgPrice", 0.0))
                
                closed_positions_volume += bought
                
                # Check Worst Loss
                if pnl < worst_loss:
                    worst_loss = pnl
                
                # Check Wins
                if pnl > 0:
                    win_count += 1
                    win_volume += bought

            # Fallback for Total Volume if leaderboard returned 0 (e.g. user not in top list)
            # We use the sum of closed positions volume as a floor.
            if total_volume == 0 and closed_positions_volume > 0:
                total_volume = closed_positions_volume

            # 3. Calculate Scores
            
            # Safety Score = 1 - (|worst_loss| / total_vol)
            safety_score = 1.0
            if total_volume > 0:
                risk_ratio = abs(worst_loss) / total_volume
                safety_score = max(0.0, 1.0 - risk_ratio)
            elif total_volume == 0 and worst_loss < 0:
                 # Loss with no volume? Should be impossible, but strictly:
                 safety_score = 0.0 
            
            # Win Score
            # Formula: 0.5 * (WinCount / TotalTrades) + 0.5 * (WinVolume / TotalVolume)
            w_trade = 0.0
            w_stake = 0.0
            
            if total_trades > 0:
                w_trade = win_count / total_trades
            
            if total_volume > 0:
                 # WinVolume / TotalVolume
                 # Note: WinVolume is from closed positions. TotalVolume is from Leaderboard (includes Open).
                 # This ratio is "Volume of Winning Trades / Global Volume".
                 w_stake = win_volume / total_volume
            elif closed_positions_volume > 0:
                 w_stake = win_volume / closed_positions_volume

            win_score = (0.5 * w_trade) + (0.5 * w_stake)
            
            return {
                "worst_loss": worst_loss,
                "win_count": win_count,
                "win_volume": win_volume,
                "total_volume": total_volume,
                "num_trades": total_trades,
                "safety_score": safety_score,
                "win_score": win_score
            }

        except Exception as e:
            print(f"Error processing metrics for {addr}: {e}")
            return None

    # Run all users in parallel
    tasks = [fetch_single_user(addr) for addr in user_addresses]
    results_list = await asyncio.gather(*tasks)
    
    # Map results
    final_results = {}
    for addr, res in zip(user_addresses, results_list):
        if res:
            final_results[addr] = res
        else:
             # Default empty metrics on error
             final_results[addr] = {
                "worst_loss": 0.0,
                "win_count": 0,
                "win_volume": 0.0,
                "total_volume": 0.0,
                "num_trades": 0,
                "safety_score": 0.0,
                "win_score": 0.0
            }
            
    return final_results

async def fetch_category_stats(wallet_address: str, categories: List[str]) -> Dict[str, Dict[str, float]]:
    """
    Fetch leaderboard stats for multiple categories in parallel.
    used for the Market Distribution panel.
    
    Args:
        wallet_address: Wallet address
        categories: List of category slugs (e.g. ['politics', 'sports'])
        
    Returns:
        Dictionary mapping category -> {volume, pnl, rank}
    """
    url = "https://data-api.polymarket.com/v1/leaderboard"
    results = {}
    
    # Define a helper for a single fetch
    async def fetch_single_category(category: str):
        try:
            params = {
                "timePeriod": "all",  # Market distribution usually implies all-time
                "orderBy": "PNL",     # Order doesn't matter for single user lookup
                "limit": 1,
                "offset": 0,
                "category": category,
                "user": wallet_address
            }
            
            response = await async_client.get(url, params=params)
            
            # If 400 error (invalid category), just return empty stats safely
            if response.status_code == 400:
                print(f"Warning: Invalid category '{category}' for leaderboard API")
                return category, {"volume": 0.0, "pnl": 0.0, "rank": 0}
                
            response.raise_for_status()
            data = response.json()
            
            stats = {"volume": 0.0, "pnl": 0.0, "rank": 0}
            if isinstance(data, list) and len(data) > 0:
                item = data[0]
                stats["volume"] = float(item.get("vol", 0.0))
                stats["pnl"] = float(item.get("pnl", 0.0))
                try:
                    stats["rank"] = int(item.get("rank", 0))
                except (ValueError, TypeError):
                    stats["rank"] = 0
            
            return category, stats
            
        except Exception as e:
            print(f"Error fetching stats for category '{category}': {e}")
            return category, {"volume": 0.0, "pnl": 0.0, "rank": 0}

    # Execute all fetches in parallel
    tasks = [fetch_single_category(cat) for cat in categories]
    fetched_data = await asyncio.gather(*tasks)
    
    for category, stats in fetched_data:
        results[category] = stats
        
    return results
