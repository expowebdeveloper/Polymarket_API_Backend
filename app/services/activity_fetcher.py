"""Service to fetch activities from Polymarket API."""

import httpx
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import logging
import asyncio

logger = logging.getLogger(__name__)


class PolymarketActivityFetcher:
    """Fetches and filters activities from Polymarket."""
    
    def __init__(self):
        # Use Polymarket APIs
        self.gamma_url = "https://gamma-api.polymarket.com"
        self.data_url = "https://data-api.polymarket.com"
        self.min_amount_usd = 0  # Fetch ALL trades for a "live" feel
        self.last_fetch = None
        
        # Cache active markets and their titles
        self.active_markets = []
        self.active_token_ids = [] # Store token IDs for WS subscription
        self.market_cache = {} # conditionId -> title
        self.markets_last_fetch = None
    
    async def _fetch_active_markets(self) -> List[str]:
        """Fetch top active/trending markets and cache their titles."""
        try:
            # Only refresh every 2 minutes for live sync
            if self.markets_last_fetch:
                time_since = datetime.utcnow() - self.markets_last_fetch
                if time_since.total_seconds() < 120:
                    return self.active_markets
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                logger.info("📊 Refreshing active market cache...")
                response = await client.get(
                    f"{self.gamma_url}/markets",
                    params={
                        "closed": "false",
                        "active": "true",
                        "limit": 1000,  # "More and More" - Double the scope to 1000 markets
                        "order": "volume24hr", 
                        "ascending": "false" 
                    }
                )
                
                if response.status_code == 200:
                    markets_data = response.json()
                    self.active_markets = []
                    self.active_token_ids = [] # Store token IDs for WS subscription
                    self.top_market_ids = []   # Store top 50 market IDs for direct polling
                    
                    for i, m in enumerate(markets_data):
                        cid = m.get("conditionId")
                        if cid:
                            self.active_markets.append(cid)
                            self.market_cache[cid] = m.get("question", m.get("title", "Unknown Market"))
                            
                            # Capture top 50 markets for "Fast Lane" polling
                            if i < 50: 
                                # Use ID if available, else ConditionID (API usually accepts market=id)
                                m_id = m.get("id") or cid
                                self.top_market_ids.append(m_id)
                            
                            # Extract Token IDs for WS Subscription
                            # clobTokenIds is often a JSON string "[...]"
                            raw_tokens = m.get("clobTokenIds", "[]")
                            try:
                                import json as pyjson
                                if isinstance(raw_tokens, str):
                                    tokens = pyjson.loads(raw_tokens)
                                else:
                                    tokens = raw_tokens
                                    
                                if isinstance(tokens, list):
                                    self.active_token_ids.extend(tokens)
                            except:
                                pass
                            
                    self.markets_last_fetch = datetime.utcnow()
                    logger.info(f"✅ Cached {len(self.market_cache)} market titles & {len(self.active_token_ids)} tokens")
                else:
                    logger.warning(f"Failed to fetch markets: {response.status_code}")
                    
        except Exception as e:
            logger.error(f"Error fetching markets: {e}")
        
        return self.active_markets or []
    
    async def fetch_recent_activities(self, limit: int = 60) -> List[Dict]:
        """
        Fetch latest trades using a hybrid strategy (Global + Top Markets).
        Ensures a constant live stream even if global endpoint is cached.
        """
        try:
            # Top markets to poll directly for high-frequency activity
            # Use the dynamic list of top 50 active markets
            target_markets = getattr(self, "top_market_ids", [])
            
            # If fetch hasn't run yet, use fallback hardcoded heavily-traded IDs
            if not target_markets:
                 target_markets = [
                    "0xd0ba39eed1bd58dfa50f2f4189dbcb3162191cc2db5f31cf7287dafaca6388d8", # BTC
                    "0x9d499c069dad561d86257b7cba69760c73cdf1da1bd3de4224481a40d9e3e522", # XRP
                    "0x0cfecbb95c7c25c345b6db764379e4948a31826b010667e43681559837941566"  # ETH
                ]

            # Add a timestamp-based cache-buster to ensure we get the absolute latest data
            t = int(datetime.utcnow().timestamp() * 1000)
            
            async with httpx.AsyncClient(timeout=5.0) as client:
                # 1. Global Trades (Increased limit to catch rapid bursts)
                global_task = client.get(
                    f"{self.data_url}/trades", 
                    params={"limit": limit, "_t": t}
                )
                tasks = [global_task]
                
                # 2. Targeted High-Volume Markets (Fast Lane)
                for market_id in target_markets:
                    tasks.append(client.get(
                        f"{self.data_url}/trades", 
                        params={"market": market_id, "limit": 10, "_t": t} # Limit 10 is plenty for single market
                    ))
                
                responses = await asyncio.gather(*tasks, return_exceptions=True)
                
                trades_data = []
                for resp in responses:
                    if isinstance(resp, httpx.Response) and resp.status_code == 200:
                        trades_data.extend(resp.json())
                
                self.last_fetch = datetime.utcnow()
                
                # Parse and filter
                activities = []
                for item in trades_data:
                    act = self._parse_trade(item)
                    if act: 
                        activities.append(act)
                
                # Sort newest first
                activities.sort(key=lambda x: x["timestamp"], reverse=True)
                
                # De-duplicate
                seen = set()
                unique = []
                for a in activities:
                    if a["id"] not in seen:
                        seen.add(a["id"])
                        unique.append(a)
                
                return unique
                
        except Exception as e:
            logger.error(f"❌ Error in hybrid fetch: {e}")
            return []
    
    async def get_all_current_activities(self) -> List[Dict]:
        """Get all current activities (same as fetch_recent_activities)."""
        return await self.fetch_recent_activities()
    
    def _parse_trade(self, trade: Dict) -> Optional[Dict]:
        """
        Parse raw trade data into activity format.
        
        Args:
            trade: Raw trade data from API
            
        Returns:
            Formatted activity dict or None if invalid
        """
        try:
            # Extract trade details
            trade_id = trade.get("transactionHash", trade.get("id", str(trade.get("timestamp", ""))))
            
            # Get market/user info from trade
            market = trade.get("market", "")
            asset_id = trade.get("asset_id", "")
            
            # Maker vs Taker
            maker_address = trade.get("maker_address", "")
            taker_address = trade.get("taker_address", "")
            
            # Try multiple identity fields for maximum human-readability
            # Explicitly ignore Lteral "Anonymous" string from API
            raw_username = trade.get("pseudonym") or trade.get("displayName") or trade.get("trader") or ""
            user_address = trade.get("proxyWallet") or trade.get("taker_address") or trade.get("maker_address") or ""
            
            if not raw_username or raw_username.lower() == "anonymous":
                if user_address:
                    username = f"{user_address[:6]}...{user_address[-4:]}"
                else:
                    username = "Trader"
            else:
                username = raw_username
            
            # Clean up robotic names (e.g. 0x...-12345)
            if username.startswith("0x") and "-" in username:
                username = username.split("-")[0]
                if len(username) > 12:
                    username = f"{username[:6]}...{username[-4:]}"
            
            # Market info
            market_id = market
            # Look up title in cache first (crucial for WebSocket which usually lacks titles)
            market_title = trade.get("marketTitle") or trade.get("title") or self.market_cache.get(market_id, "Unknown Market")
            
            # Trade details
            side = trade.get("side", "BUY").upper()
            
            # Amount calculation (WS sends them as strings, might use shorthand p/s)
            price = float(trade.get("price", trade.get("p", 0)))
            size = float(trade.get("size", trade.get("s", 0)))
            amount_usd = price * size
            
            # Try direct amount field equivalents
            if "amount" in trade:
                amount_usd = float(trade.get("amount", 0))
            elif "total" in trade:
                amount_usd = float(trade.get("total", 0))
            elif "value" in trade:
                amount_usd = float(trade.get("value", 0))
            elif "cost" in trade:
                amount_usd = float(trade.get("cost", 0))
            elif "funds" in trade:
                amount_usd = float(trade.get("funds", 0))
            
            # Outcome (WS usually has outcomeIndex or side/price)
            # Default to Side if outcome field is missing
            outcome = trade.get("outcome") or ("Yes" if side == "BUY" else "No")
            
            # Timestamp (WS uses high precision or shorthands like t/match_time)
            ts_raw = trade.get("timestamp") or trade.get("t") or trade.get("match_time") or trade.get("matchtime")
            final_ts = int(datetime.utcnow().timestamp())
            
            if ts_raw:
                try:
                    ts_val = float(ts_raw)
                    # Handle milliseconds
                    if ts_val > 10000000000:
                        ts_val = ts_val / 1000
                    
                    # DIRECT FIX: Use the timestamp directly!
                    # Converting to datetime(utc) -> timestamp() implies local timezone assumption on some systems
                    final_ts = int(ts_val)
                except:
                    pass

            return {
                "id": str(trade_id),
                "user": username,
                "user_address": user_address,
                "market_id": market_id,
                "market": market_title,
                "side": side,
                "amount_usd": amount_usd,
                "price": price,
                "size": size,
                "outcome": outcome,
                "timestamp": final_ts
            }
            
        except Exception as e:
            logger.error(f"Error parsing trade: {e}")
            return None
    
    def _format_time_ago(self, activity_time: datetime) -> str:
        """
        Format datetime as '1s ago', '5m ago', etc.
        
        Args:
            activity_time: Time of the activity
            
        Returns:
            Formatted time string
        """
        now = datetime.utcnow()
        diff = now - activity_time
        
        seconds = int(diff.total_seconds())
        
        if seconds < 5:
            return "Just now"
        elif seconds < 60:
            return f"{seconds}s ago"
        elif seconds < 3600:
            minutes = seconds // 60
            return f"{minutes}m ago"
        elif seconds < 86400:
            hours = seconds // 3600
            return f"{hours}h ago"
        else:
            days = seconds // 86400
            return f"{days}d ago"
