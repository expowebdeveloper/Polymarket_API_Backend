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
DNS_CACHE: Dict[str, str] = {
    "data-api.polymarket.com": "104.18.34.205", 
    "gamma-api.polymarket.com": "104.18.34.205",
    "polymarket.com": "104.18.34.205"
}

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"

def get_standard_headers(host: Optional[str] = None) -> Dict[str, str]:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
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
                    "openInterest": float(event.get("openInterest", 0)),
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
            
        # Step 2: Parallel fetch (similar to closed positions)
        # Use moderate batch size to avoid rate limits
        PARALLEL_BATCH_SIZE = 10
        batch_offset = initial_offset + fetch_limit
        more_data_available = True
        # Safety cap
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
        PARALLEL_BATCH_SIZE = 10
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
            
        # Step 2: Parallel fetch for remaining
        # Aggressive parallel fetching to speed up "Full History" download
        MAX_BATCH_SIZE = 15  # Max parallel requests
        current_batch_size = 5  # Start smaller to avoid overhead for small wallets (~150-200 items)

        batch_offset = initial_offset + fetch_limit
        more_data_available = True
        
        while more_data_available:
            tasks = []
            for i in range(current_batch_size):
                current_req_offset = batch_offset + (i * fetch_limit)
                # Create a specific params dict for this request
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
                
                # If we filled this batch, ramp up to max speed
                current_batch_size = MAX_BATCH_SIZE
                
                # Safety break
                # Safety break removed as per user request to fetch ALL positions
                # if len(all_positions) > 10000:
                #    print("⚠️ Reached safety limit of 10k closed positions")
                #    break
        
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
            stats["rank"] = item.get("rank")
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


async def fetch_wallet_address_from_profile_page(username: str) -> Optional[str]:
    """
    Fallback method: Fetch user profile page HTML and regex search for wallet address.
    Used when username is not found in DB or Leaderboard API.
    """
    try:
        url = f"https://polymarket.com/@{username}"
        print(f"Fetching profile HTML: {url}")
        
        # Use a real browser-like user agent to avoid bot detection
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        
        resp = await async_client.get(url, headers=headers, follow_redirects=True)
        
        if resp.status_code == 200:
            html = resp.text
            
            # Patterns to find address in JSON blobs or JS variables in the HTML
            patterns = [
                r'"userAddress":"(0x[a-fA-F0-9]{40})"',
                r'"proxyWallet":"(0x[a-fA-F0-9]{40})"',
                r'"address":"(0x[a-fA-F0-9]{40})"'
            ]
            
            import re
            for p in patterns:
                match = re.search(p, html)
                if match:
                    print(f"✓ Found address via profile scrape: {match.group(1)}")
                    return match.group(1)
            
            # Check __NEXT_DATA__ as secondary robust check
            match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.+?)</script>', html)
            if match:
                import json
                try:
                    data = json.loads(match.group(1))
                    page_props = data.get("pageProps", {})
                    if "dehydratedState" in page_props:
                        queries = page_props["dehydratedState"].get("queries", [])
                        for q in queries:
                             # Look for profile query key usually containing the username
                             if "profile" in str(q.get("queryKey")):
                                  user_data = q.get("state", {}).get("data", {})
                                  addr = user_data.get('proxyWallet') or user_data.get('userAddress')
                                  if addr:
                                      print(f"✓ Found address via __NEXT_DATA__: {addr}")
                                      return addr
                except:
                    pass

    except Exception as e:
        print(f"Error scraping profile page: {e}")
        
    return None
