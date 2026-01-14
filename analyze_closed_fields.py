
import asyncio
import sys
import httpx

# Add the project root to the python path
sys.path.append("/home/dell/Desktop/Projects/Polymarket/backend")

async def analyze():
    wallet = "0x56687bf447db6ffa42ffe2204a05edaa20f55839"
    print(f"Analyzing {wallet}...")
    
    # 1. Direct API Check for Closed Positions (V1)
    url_v1 = f"https://data-api.polymarket.com/v1/closed-positions?user={wallet}&limit=10&sortBy=timestamp&sortDirection=DESC"
    async with httpx.AsyncClient() as client:
        print(f"\n1. Fetching V1 Closed Positions Raw: {url_v1}")
        try:
            r = await client.get(url_v1)
            if r.status_code == 200:
                data = r.json()
                print(f"   V1 Count: {len(data)}")
                for i, p in enumerate(data):
                     print(f"   [{i}] Title: {p.get('title')}")
                     print(f"       Slug: {p.get('slug')}")
                     print(f"       Asset: {p.get('asset')}")
                     print(f"       Size:   {p.get('size')} (Type: {type(p.get('size'))})")
                     print(f"       Bought: {p.get('totalBought')}")
                     print(f"       AvgP:   {p.get('avgPrice')}")
                     print(f"       ExitP:  {p.get('exitPrice')}")
                     print(f"       Pnl:    {p.get('realizedPnl')}")
            else:
                 print(f"   Error V1: {r.status_code} {r.text}")
        except Exception as e:
            print(f"   Ex V1: {e}")

if __name__ == "__main__":
    asyncio.run(analyze())
