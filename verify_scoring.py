import asyncio
import json
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from app.services.dashboard_service import get_db_dashboard_data
from app.db.session import engine

# Settings for the test
WALLET_ADDRESS = "0x0d3b10b8eac8b089c6e4a695e65d8e044167c46b"

async def test_scoring():
    print(f"Testing scoring for wallet: {WALLET_ADDRESS}")
    
    async_session = sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )
    
    async with async_session() as session:
        try:
            data = await get_db_dashboard_data(session, WALLET_ADDRESS)
            
            # Print relevant metrics
            print("\n--- Portfolio Summary ---")
            perf = data.get("portfolio", {}).get("performance_metrics", {})
            print(f"Total PnL: {perf.get('total_pnl')}")
            print(f"ROI: {perf.get('roi')}%")
            print(f"Worst Loss: {perf.get('worst_loss')}")
            
            print("\n--- Scoring Metrics ---")
            scoring = data.get("scoring_metrics", {})
            print(json.dumps(scoring, indent=2))
            
            print("\n--- Streaks ---")
            print(json.dumps(data.get("streaks", {}), indent=2))
            
            # Check for specific zero issues
            risk_score = scoring.get("score_risk", 0)
            if risk_score == 0:
                print("\nWARNING: Risk Score is still 0!")
            else:
                print(f"\nSUCCESS: Risk Score is {risk_score}")
                
        except Exception as e:
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_scoring())
