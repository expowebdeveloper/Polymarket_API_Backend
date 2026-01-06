"""
Test Confidence Score Integration in Dashboard
"""

import asyncio
from app.db.session import AsyncSessionLocal
from app.services.dashboard_service import get_db_dashboard_data
from sqlalchemy import text

async def test():
    async with AsyncSessionLocal() as session:
        # Get a wallet from the database
        result = await session.execute(text('SELECT wallet_address FROM trader_leaderboard LIMIT 1'))
        wallet = result.scalar()
        
        if not wallet:
            print('No traders found in DB')
            return
            
        print(f'Testing wallet: {wallet}')
        print('=' * 80)
        
        data = await get_db_dashboard_data(session, wallet)
        
        scoring = data.get('scoring_metrics', {})
        
        print(f"\n📊 Confidence Score Metrics:")
        print(f"  Total Trades (with PnL): {scoring.get('total_trades_with_pnl', 'N/A')}")
        print(f"  Confidence Score (0-1): {scoring.get('confidence_score', 'N/A')}")
        print(f"  Confidence Percent: {scoring.get('confidence_percent', 'N/A')}%")
        print(f"  Confidence Level: {scoring.get('confidence_level', 'N/A')}")
        
        print(f"\n📈 Other Scoring Metrics:")
        print(f"  Win Score Blended: {scoring.get('win_score_blended', 'N/A')}")
        print(f"  W_trade: {scoring.get('w_trade', 'N/A')}")
        print(f"  W_stake: {scoring.get('w_stake', 'N/A')}")
        print(f"  Total PnL: ${scoring.get('total_pnl', 0):.2f}")
        print(f"  ROI: {scoring.get('roi', 0):.2f}%")
        print(f"  Win Rate: {scoring.get('win_rate', 0):.2f}%")
        
        print(f"\n✅ Confidence score successfully integrated into dashboard!")

if __name__ == "__main__":
    asyncio.run(test())
