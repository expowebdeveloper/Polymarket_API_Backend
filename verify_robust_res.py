import asyncio
import json
from unittest.mock import patch, MagicMock
from app.services.dashboard_service import get_profile_stat_data

async def verify_robust_resolution():
    print("--- Testing fetch_market_by_slug with conditionId ---")
    # Hillary Clinton win (NO)
    hillary_cid = "0x63634b4e14297a748923f86dca4fa0c6c659db0f5fadeeb8e419e48e20759c34"
    
    from app.services.data_fetcher import fetch_market_by_slug, get_market_resolution
    
    market = await fetch_market_by_slug(hillary_cid)
    if market:
        print(f"✓ Found market: {market.get('question') or market.get('title')}")
        print(f"  ID: {market.get('id')}")
        
        # Check resolution
        res = await get_market_resolution(hillary_cid, [])
        print(f"✓ Resolution outcome: {res}")
        
        if res is not None:
             print("\nSUCCESS: Target market resolution identified via conditionId lookup.")
        else:
             print("\nFAILED: Market found but resolution not identified.")
    else:
        print("FAILED: Could not find market by conditionId.")

if __name__ == "__main__":
    asyncio.run(verify_robust_resolution())
