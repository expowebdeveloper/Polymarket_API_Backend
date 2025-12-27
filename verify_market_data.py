import asyncio
import sys
from app.db.session import init_db, AsyncSessionLocal
from app.services.market_service import update_all_markets
from sqlalchemy import text

async def verify_market_storage():
    print("Verifying market storage...")
    
    # 1. Initialize DB
    await init_db()
    
    # 2. Run update job (fetch just a few markets to be quick)
    # We'll monkeypatch the fetch limit in update_all_markets or just let it run for a bit
    # For now, we rely on the fact that update_all_markets prints output
    
    try:
        await update_all_markets()
    except Exception as e:
        print(f"Error running update_all_markets: {e}")
        return

    # 3. Check DB
    async with AsyncSessionLocal() as session:
        result = await session.execute(text("SELECT COUNT(*) FROM markets"))
        count = result.scalar()
        print(f"Total markets in DB: {count}")
        
        if count > 0:
            print("✓ Verification SUCCESS: Markets were fetched and stored.")
        else:
            print("✗ Verification FAILED: No markets found in DB.")

if __name__ == "__main__":
    asyncio.run(verify_market_storage())
