"""Background service to fetch and broadcast activities via WebSocket."""

import asyncio
import httpx
import logging
from typing import List, Dict
from datetime import datetime, timedelta
from app.services.activity_fetcher import PolymarketActivityFetcher
from app.routers.websocket import manager

logger = logging.getLogger(__name__)


class ActivityBroadcaster:
    """Continuously fetches activities and broadcasts to WebSocket clients."""
    
    def __init__(self):
        self.fetcher = PolymarketActivityFetcher()
        self.is_running = False
        self.fetch_interval = 2.0  # Polling every 2s is safer for API stability
        self.recent_activities: List[Dict] = []
        self.seen_trade_ids: set = set()
        self.client = None # Lazy initialized in start()
    
    def get_recent_activities(self) -> List[Dict]:
        """Get cached recent activities for new connections."""
        return self.recent_activities.copy()
    
    async def start(self):
        """Start the background activity broadcaster."""
        if self.is_running:
            logger.warning("Activity broadcaster is already running")
            return
        
        # Lazy initialize client if needed
        if self.client is None:
            self.client = httpx.AsyncClient(timeout=10.0)
            
        self.is_running = True
        logger.info(f"🚀 Activity broadcaster started - Hybrid Mode (WS + Polling)")
        
        # Start both the WebSocket bridge and the Polling loop
        await asyncio.gather(
            self.run_ws_bridge(),
            self.run_polling_loop()
        )

    async def run_ws_bridge(self):
        """Bridge Polymarket's official CLOB WebSocket to our dashboard."""
        import websockets
        import json
        
        ws_url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
        
        while self.is_running:
            try:
                logger.info(f"🔌 Connecting to Polymarket CLOB WS...")
                # Ensure we have active tokens to subscribe to
                if not self.fetcher.active_token_ids:
                    logger.info("⏳ Waiting for active markets/tokens...")
                    await self.fetcher._fetch_active_markets()
                
                # Limit to 1000 tokens (Polymarket allows larger batches, 1000 covers top markets)
                tokens_to_sub = self.fetcher.active_token_ids[:1000]
                
                if not tokens_to_sub:
                     logger.warning("⚠️ No tokens found to subscribe to!")

                async with websockets.connect(ws_url, ping_interval=20, ping_timeout=10) as ws:
                    # Subscribe to specific assets (Simulated Global Feed)
                    if tokens_to_sub:
                        subscribe_msg = {
                            "type": "subscribe",
                            "channels": ["trades"],
                            "asset_ids": tokens_to_sub,
                            "token_ids": tokens_to_sub # Send both to be safe
                        }
                        await ws.send(json.dumps(subscribe_msg))
                        logger.info(f"✅ Subscribed to {len(tokens_to_sub)} active assets")
                    else:
                         # Fallback to older methods if no tokens (unlikely)
                         await ws.send(json.dumps({"type": "subscribe", "channels": ["trades"]}))
                    
                    async for message in ws:
                        if not self.is_running:
                            break
                        
                        try:
                            data = json.loads(message)
                        except:
                            # Ignore empty/keepalive messages
                            continue
                        
                        # DEBUG: Log everything once to see the structure
                        msg_type = str(data.get("type") or data.get("event") or data.get("event_type", "unknown"))
                        if msg_type not in ["pong", "subscription_succeeded"]:
                            logger.info(f"📥 WS Message: {msg_type} | Content: {str(message)[:200]}...")
                        
                        # Polymarket WS can send a single object or a list of events
                        events = data if isinstance(data, list) else [data]
                        new_trades = []
                        
                        for event in events:
                            # Official CLOB WS uses 'event_type' or 'type'
                            etype = str(event.get("event_type") or event.get("event") or event.get("type", "")).lower()
                            
                            # Catch any event that looks like a trade (has price/size or says trade)
                            if "trade" in etype or "match" in etype or (event.get("price") and event.get("size")):
                                act = self.fetcher._parse_trade(event)
                                if act and act["id"] not in self.seen_trade_ids:
                                    self.seen_trade_ids.add(act["id"])
                                    new_trades.append(act)
                        
                        if new_trades:
                            logger.info(f"📡 WS: Broadcasting {len(new_trades)} LIVE trades")
                            await manager.broadcast({
                                "type": "new_activity_batch",
                                "data": new_trades
                            })
                                
            except Exception as e:
                if self.is_running:
                    logger.error(f"❌ WS Bridge Error: {e}. Reconnecting in 5s...")
                    await asyncio.sleep(5)

    async def run_polling_loop(self):
        """Fallback polling loop to ensure data consistency and initial cache."""
        while self.is_running:
            try:
                # Keep seen_ids from bloating
                if len(self.seen_trade_ids) > 10000:
                    self.seen_trade_ids = set(list(self.seen_trade_ids)[-5000:])
                
                # Deep Poll: Fetch last 500 trades to find those rare >$1000 whales
                # (Standard poll of 60 might miss them if volume is high)
                recent_activity = await self.fetcher.fetch_recent_activities(limit=500)
                
                if not recent_activity:
                    await asyncio.sleep(2)
                    continue

                # Polling Fallback (Every 2s)
                # Since fetcher now provides correct UTC timestamps, we don't need complex offsets
                now_ts = int(datetime.utcnow().timestamp())
                
                # Filter recent window (keep last 5 mins as per user request)
                fresh_limit = now_ts - 300
                
                self.recent_activities = [
                    a for a in recent_activity 
                    if a['timestamp'] >= fresh_limit
                ][:100]
                
                # Identify NEW trades from polling (backup if WS missed any)
                new_trades = []
                for activity in recent_activity:
                    if activity["id"] not in self.seen_trade_ids:
                        self.seen_trade_ids.add(activity["id"])
                        new_trades.append(activity)
                
                if new_trades:
                    max_val = max([t.get("amount_usd", 0) for t in new_trades])
                    logger.info(f"🔄 Polling: Found {len(new_trades)} trades. Max Value: ${max_val:,.2f}")
                    await manager.broadcast({
                        "type": "new_activity_batch",
                        "data": new_trades
                    })

                # Heartbeat
                await manager.broadcast({
                    "type": "heartbeat",
                    "timestamp": now_ts
                })
                
                await asyncio.sleep(2.0) # Slower heartbeat as WS is active
                
            except Exception as e:
                logger.error(f"❌ Polling Error: {e}")
                await asyncio.sleep(5.0)
    
    async def stop(self):
        """Stop the background activity broadcaster."""
        self.is_running = False
        if hasattr(self, 'client') and self.client:
            await self.client.aclose()
        logger.info("🛑 Activity broadcaster stopped")


# Global broadcaster instance
broadcaster = ActivityBroadcaster()
