"""
In-memory cache for analytics data to avoid recalculating on every request.
"""

from typing import Dict, Optional, Any
from datetime import datetime, timedelta
import asyncio

class AnalyticsCache:
    """Simple in-memory cache for analytics data."""
    
    def __init__(self):
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._cache_timestamps: Dict[str, datetime] = {}
        self._cache_lock = asyncio.Lock()
        self._default_ttl = timedelta(minutes=5)  # Cache for 5 minutes
    
    def _get_cache_key(self, wallets: Optional[list] = None, max_traders: Optional[int] = None) -> str:
        """Generate cache key from parameters."""
        wallet_key = "all" if wallets is None else f"{len(wallets)}_{hash(tuple(sorted(wallets or [])))}"
        max_key = "all" if max_traders is None else str(max_traders)
        return f"analytics_{wallet_key}_{max_key}"
    
    async def get(self, wallets: Optional[list] = None, max_traders: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """Get cached analytics data if available and not expired."""
        async with self._cache_lock:
            cache_key = self._get_cache_key(wallets, max_traders)
            
            if cache_key not in self._cache:
                return None
            
            # Check if cache is expired
            if cache_key in self._cache_timestamps:
                age = datetime.now() - self._cache_timestamps[cache_key]
                if age > self._default_ttl:
                    # Cache expired, remove it
                    del self._cache[cache_key]
                    del self._cache_timestamps[cache_key]
                    return None
            
            return self._cache[cache_key].copy()  # Return a copy to avoid mutations
    
    async def set(self, data: Dict[str, Any], wallets: Optional[list] = None, max_traders: Optional[int] = None):
        """Store analytics data in cache."""
        async with self._cache_lock:
            cache_key = self._get_cache_key(wallets, max_traders)
            self._cache[cache_key] = data.copy()  # Store a copy
            self._cache_timestamps[cache_key] = datetime.now()
    
    async def clear(self):
        """Clear all cached data."""
        async with self._cache_lock:
            self._cache.clear()
            self._cache_timestamps.clear()
    
    async def invalidate(self, wallets: Optional[list] = None, max_traders: Optional[int] = None):
        """Invalidate specific cache entry."""
        async with self._cache_lock:
            cache_key = self._get_cache_key(wallets, max_traders)
            if cache_key in self._cache:
                del self._cache[cache_key]
            if cache_key in self._cache_timestamps:
                del self._cache_timestamps[cache_key]

# Global cache instance
analytics_cache = AnalyticsCache()



