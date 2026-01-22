
import httpx
import logging
import asyncio
import re
from datetime import datetime
from bs4 import BeautifulSoup
from typing import Dict, Optional

logger = logging.getLogger(__name__)

class DashboardService:
    def __init__(self):
        self.url = "https://www.polydata.cc/dashboard"
        self._cache: Optional[Dict] = None
        self._last_fetch: datetime = datetime.min
        self._cache_duration_seconds = 15
        self._lock = asyncio.Lock()

    async def get_stats(self) -> Dict:
        """
        Returns the latest dashboard stats, using cache if fresh.
        """
        async with self._lock:
            # Check cache
            if self._cache and (datetime.utcnow() - self._last_fetch).total_seconds() < self._cache_duration_seconds:
                return self._cache

            # Fetch new data
            stats = await self._fetch_live_stats()
            if stats:
                self._cache = stats
                self._last_fetch = datetime.utcnow()
                return stats
            
            # Return old cache if fetch fails
            return self._cache or self._get_empty_stats()

    async def _fetch_live_stats(self) -> Optional[Dict]:
        """
        Scrapes polydata.cc for the real-time stats.
        """
        try:
            # Emulate real browser to avoid bot detection
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            }

            async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
                response = await client.get(self.url, headers=headers)
                
                if response.status_code != 200:
                    logger.error(f"Failed to fetch Polydata: {response.status_code}")
                    return None

                # Extract logic
                # Next.js usually puts data in text, even if obfuscated.
                # We will look for specific labels and the numbers immediately following/associated.
                text = response.text
                
                # Next.js hydration data structure via verified Index Mapping:
                # 0: Volume, 1: TVL, 2: OI, 3: Mkts Vol, 4: Mkts, 5: Trades (157M), 6: Traders (1.8M), 7: LP, 8: Buys, 9: Sells
                
                # Extract all "end" values
                pattern = r'\\"end\\":([\d\.]+)'
                vals = re.findall(pattern, text)
                
                if len(vals) < 10:
                    logger.warning(f"Dashboard structure changed? Found {len(vals)} items.")
                    # Fallback or partial
                    return self._get_empty_stats()

                stats = {
                    "total_volume": self._fmt(vals[0], is_curr=True),
                    "tvl": self._fmt(vals[1], is_curr=True),
                    "open_interest": self._fmt(vals[2], is_curr=True),
                    "markets_volume": self._fmt(vals[3], is_curr=True),
                    "total_markets": self._fmt(vals[4], is_curr=False),
                    "total_traders": self._fmt(vals[6], is_curr=False), # Index 6 is Traders (1.8M)
                    "lp_rewards": self._fmt(vals[7], is_curr=True),
                    "total_trades": self._fmt(vals[5], is_curr=False),  # Index 5 is Trades (157M)
                    "total_buys": self._fmt(vals[8], is_curr=False),
                    "total_sells": self._fmt(vals[9], is_curr=False),
                }
                
                return stats

        except Exception as e:
            logger.error(f"Error scraping dashboard: {e}")
            return None

    def _fmt(self, raw_val: str, is_curr: bool) -> str:
        """Helper to format string numbers."""
        try:
            val = float(raw_val)
            if is_curr:
                return f"${val:,.2f}"
            else:
                return f"{int(val):,}"
        except:
            return "$0.00" if is_curr else "0"

    # Deprecated regex extractors removed


    def _get_empty_stats(self):
        return {
            "total_volume": "$0.00",
            "tvl": "$0.00",
            "open_interest": "$0.00",
            "markets_volume": "$0.00",
            "total_markets": "0",
            "total_traders": "0",
            "lp_rewards": "$0.00",
            "total_trades": "0",
            "total_buys": "0",
            "total_sells": "0",
        }

dashboard_service = DashboardService()
