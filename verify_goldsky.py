import asyncio
from app.services.goldsky_service import GoldskyService

async def verify():
    print("Testing Goldsky Connection (Volume Leaderboard)...")
    try:
        data = await GoldskyService.fetch_volume_leaderboard("day", limit=3)
        print(f"Result: {data}")
        if not data:
            print("❌ No data returned (likely due to invalid URL or empty subgraph)")
        else:
            print(f"✅ Success! Got {len(data)} entries.")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(verify())
