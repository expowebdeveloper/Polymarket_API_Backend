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
    """
    trades: List[Dict] = []
    base_url = settings.DOME_API_URL.rstrip("/")
    api_key = settings.DOME_API_KEY
    
    # Dome free tier: 1 QPS / 10 requests per 10 seconds – keep it to a single call
    try:
        url = f"{base_url}/polymarket/orders"
        
        # Dome docs show orders are filterable; use user/address filter for wallet
        params_candidates = [
            {"user": wallet_address, "limit": 100},
            {"address": wallet_address, "limit": 100},
        ]
        
        headers = {
            "Authorization": f"Bearer {api_key}"
        }
        
        last_error = None
        response = None
        
        for params in params_candidates:
            try:
                response = requests.get(url, headers=headers, params=params, timeout=10)
                if response.status_code == 200:
                    break
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
        
        if trades:
            print(f"✓ Fetched {len(trades)} trades (orders) for wallet {wallet_address} from Dome")
        else:
            print(f"⚠ Warning: No trades found for wallet {wallet_address} from Dome")
            print(f"  Response preview: {str(data)[:300]}")
    
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
    """Get market data by market ID."""
    for market in markets:
        m_id = market.get("id") or market.get("market_id") or market.get("slug")
        if m_id == market_id:
            return market
    return None


def get_market_resolution(market_id: str, markets: List[Dict]) -> Optional[str]:
    """Get the resolution (YES/NO) for a given market ID."""
    market = get_market_by_id(market_id, markets)
    if not market:
        return None
    
    resolution = market.get("resolution") or market.get("outcome")
    if resolution:
        return str(resolution).upper()
    
    # Check resolution source
    resolution_source = market.get("resolutionSource") or market.get("resolution_source")
    if resolution_source:
        return "YES" if "yes" in str(resolution_source).lower() else "NO"
    
    # Check if resolved to Yes/No
    if market.get("resolved") or market.get("isResolved"):
        # Try to infer from other fields
        outcome = market.get("outcome") or market.get("winningOutcome")
        if outcome:
            return "YES" if "yes" in str(outcome).lower() else "NO"
    
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

