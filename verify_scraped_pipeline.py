
import asyncio
import sys
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from app.core.config import settings
from app.services.db_scoring_service import get_advanced_db_analytics
import json

async def verify_pipeline():
    print("🚀 Verifying Scraped Data Leaderboard Calculation...")
    
    # 1. Connect to DB
    engine = create_async_engine(settings.DATABASE_URL, echo=False)
    AsyncSessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    
    async with AsyncSessionLocal() as session:
        # 2. Run calculation using scraped data
        print("  Running get_advanced_db_analytics(use_scraped_data=True)...")
        results = await get_advanced_db_analytics(
            session, 
            limit=5, 
            use_scraped_data=True,
            max_traders=5 # Only process 5 for speed
        )
        
        traders = results.get("traders", [])
        print(f"  ✅ Calculation complete. Found {len(traders)} traders.")
        
        if not traders:
            print("  ⚠️  No traders found. Make sure trader_leaderboard and trader_details are populated.")
            return

        # 3. Inspect first trader
        t = traders[0]
        print(f"\n  Sample Trader: {t.get('wallet_address')}")
        print(f"  Name: {t.get('name')}")
        print(f"  Est. PnL: {t.get('total_pnl')}")
        print(f"  Est. ROI: {t.get('roi')}")
        print(f"  Final Score: {t.get('final_score')}")
        print(f"  Confidence: {t.get('confidence_score')}")
        
        # Check if we successfully got data from scraped tables
        # If scraped tables were empty, metrics would be all 0
        if t.get('total_trades', 0) > 0:
            print("  ✅ SUCCESS: Retrieved trades/stats from scraped tables!")
        else:
            print("  ⚠️  WARNING: Total trades is 0. Check data population.")

if __name__ == "__main__":
    asyncio.run(verify_pipeline())
