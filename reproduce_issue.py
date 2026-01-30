
import asyncio
from app.services.data_fetcher import fetch_user_trades, fetch_resolved_markets, get_market_by_id, fetch_market_by_slug

async def main():
    wallet = "0x9d84ce0306f8551e02efef1680475fc0f1dc1344"
    print(f"Fetching trades for {wallet}...")
    trades = await fetch_user_trades(wallet, limit=1000)
    print(f"Fetched {len(trades)} trades.")

    print("Fetching resolved markets (limit 200)...")
    markets = await fetch_resolved_markets(limit=200)
    print(f"Fetched {len(markets)} markets.")

    # Check for missing markets
    missing_markets = set()
    total_skipped = 0
    total_processed = 0
    
    for trade in trades:
        market_id = (
            trade.get("market_id") or 
            trade.get("market") or 
            trade.get("marketId") or
            trade.get("market_slug") or
            trade.get("marketSlug") or
            trade.get("slug")
        )
        
        if not market_id:
            continue

        market = get_market_by_id(market_id, markets)
        if not market:
            missing_markets.add(market_id)
            total_skipped += 1
        else:
            total_processed += 1

    print(f"\nAnalysis:")
    print(f"Total processed: {total_processed}")
    print(f"Total skipped due to missing market: {total_skipped}")
    print(f"Unique missing markets: {len(missing_markets)}")

    if missing_markets:
        print("\nChecking first 5 missing markets to see if they can be resolved...")
        for slug in list(missing_markets)[:5]:
            print(f"Fetching {slug}...")
            market = await fetch_market_by_slug(slug)
            if market:
                print(f"✅ Found {slug} (Resolution: {market.get('resolution') or 'Unknown'})")
            else:
                print(f"❌ Could not find {slug}")

if __name__ == "__main__":
    asyncio.run(main())
