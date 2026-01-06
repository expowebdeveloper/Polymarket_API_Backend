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
        print('=' * 60)
        
        data = await get_db_dashboard_data(session, wallet)
        
        scoring = data.get('scoring_metrics', {})
        
        print(f"\nWin Score Metrics:")
        print(f"  W_trade (winning trades / total trades): {scoring.get('w_trade', 'N/A')}")
        print(f"  W_stake (winning stakes / total stakes): {scoring.get('w_stake', 'N/A')}")
        print(f"  Win Score Blended (0.5*W_trade + 0.5*W_stake): {scoring.get('win_score_blended', 'N/A')}")
        
        print(f"\nUnderlying Data:")
        print(f"  Total Trades: {scoring.get('total_trades_with_pnl', 'N/A')}")
        print(f"  Winning Trades: {scoring.get('winning_trades', 'N/A')}")
        print(f"  Total Stakes: ${scoring.get('total_stakes', 0):.2f}")
        print(f"  Winning Stakes: ${scoring.get('winning_stakes', 0):.2f}")
        print(f"  Win Rate %: {scoring.get('win_rate', 0):.2f}%")
        
        # Verify calculation
        w_trade = scoring.get('w_trade', 0)
        w_stake = scoring.get('w_stake', 0)
        win_score = scoring.get('win_score_blended', 0)
        expected = 0.5 * w_trade + 0.5 * w_stake
        
        print(f"\nVerification:")
        print(f"  Expected: 0.5 * {w_trade} + 0.5 * {w_stake} = {expected:.4f}")
        print(f"  Actual: {win_score}")
        print(f"  Match: {'✓ YES' if abs(win_score - expected) < 0.001 else '✗ NO'}")

if __name__ == "__main__":
    asyncio.run(test())
