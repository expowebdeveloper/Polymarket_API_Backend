
import asyncio
import sys
from app.services.data_fetcher import fetch_user_profile_data_v2 as fetch_trader_profile_stats, fetch_user_trades

async def test_fetch():
    # Failing wallet from user logs
    wallet = "0x74c38a8e46f0ca11079926962456906978b2c1a8"
    print(f"Testing fetch for {wallet}...")
    
    # 1. Test Profile (403 Source)
    print("\n--- Profile Stats ---")
    profile = await fetch_trader_profile_stats(wallet)
    if profile:
         print("✅ Profile Fetched Successfully!")
         print(profile)
    else:
         print("❌ Profile Fetch Failed")

    # 2. Test Trades (UTF-8 Error Source)
    print("\n--- Trades ---")
    trades = await fetch_user_trades(wallet)
    if trades:
        print(f"✅ Trades Fetched Successfully! Count: {len(trades)}")
    else:
        print("❌ Trades Fetch Failed")

if __name__ == "__main__":
    asyncio.run(test_fetch())
