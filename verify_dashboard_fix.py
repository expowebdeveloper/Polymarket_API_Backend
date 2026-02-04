
import asyncio
from app.db.session import AsyncSessionLocal
from app.services.dashboard_service import get_global_dashboard_stats

async def verify():
    print("Verifying global dashboard stats...")
    async with AsyncSessionLocal() as session:
        stats = await get_global_dashboard_stats(session)
        print("Stats retrieved successfully:")
        for k, v in stats.items():
            print(f"- {k}: {v}")
            
        # Basic validation
        assert "total_volume" in stats
        assert "total_markets" in stats
        assert stats["total_volume"] != "Error"
        print("\nVerification Passed!")

if __name__ == "__main__":
    asyncio.run(verify())
