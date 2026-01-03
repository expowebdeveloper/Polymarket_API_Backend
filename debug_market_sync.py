import asyncio
import sys
import logging
from app.services.market_service import update_all_markets
from app.db.session import init_db

# Configure logging to see what's happening
logging.basicConfig(level=logging.INFO)

async def run_debug_sync():
    print("Initializing DB...")
    await init_db()
    print("Starting market sync...")
    try:
        await update_all_markets()
        print("Market sync finished.")
    except Exception as e:
        print(f"CRITICAL ERROR during sync: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(run_debug_sync())
