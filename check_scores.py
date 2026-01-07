import asyncio
import json
from decimal import Decimal
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from app.services.dashboard_service import get_db_dashboard_data
from app.core.config import settings

async def investigate_scores():
    engine = create_async_engine(settings.DATABASE_URL)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    # Target a wallet with many trades (from earlier check)
    target_wallet = "0x204f72f35326db932158cba6adff0b9a1da95e14"
    
    async with async_session() as session:
        print(f"Investigating scores for {target_wallet}...")
        try:
            data = await get_db_dashboard_data(session, target_wallet)
            scoring = data.get("scoring_metrics", {})
            
            print("\nScoring Metrics:")
            for k, v in scoring.items():
                print(f"  {k}: {v}")
                
            print("\nTrader Metrics (from Population Calculation):")
            # We don't easily have access to raw population metrics here without more deep diving
            
        except Exception as e:
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(investigate_scores())
