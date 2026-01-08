
import asyncio
from sqlalchemy import select
from app.db.session import AsyncSessionLocal
from app.db.models import Position, Trade, ClosedPosition

async def inspect_wallet(wallet_address: str):
    async with AsyncSessionLocal() as session:
        # PnL from Closed Positions
        stmt = select(ClosedPosition).where(ClosedPosition.proxy_wallet == wallet_address)
        result = await session.execute(stmt)
        closed = result.scalars().all()
        
        closed_pnl = sum(float(cp.realized_pnl or 0) for cp in closed)
        closed_stake = sum(float(cp.total_bought or 0) * float(cp.avg_price or 0) for cp in closed)
        closed_shares = sum(float(cp.total_bought or 0) for cp in closed)
        
        print(f"--- Closed Positions ({len(closed)}) ---")
        print(f"PnL: {closed_pnl}")
        print(f"Stake ($): {closed_stake}")
        print(f"Shares: {closed_shares}")

        # PnL from Active Positions
        stmt = select(Position).where(Position.proxy_wallet == wallet_address)
        result = await session.execute(stmt)
        active = result.scalars().all()
        
        active_cash_pnl = sum(float(p.cash_pnl or 0) for p in active)
        active_stake = sum(float(p.initial_value or 0) for p in active)
        
        print(f"\n--- Active Positions ({len(active)}) ---")
        print(f"Cash PnL: {active_cash_pnl}")
        print(f"Stake ($): {active_stake}")

        total_pnl = closed_pnl + active_cash_pnl
        total_stake = closed_stake + active_stake
        
        print(f"\n--- Total ---")
        print(f"Total PnL: {total_pnl}")
        print(f"Total Stake ($): {total_stake}")
        print(f"Global ROI: {total_pnl/total_stake*100 if total_stake > 0 else 0}%")
        print(f"Ratio PnL/ClosedStake: {total_pnl/closed_stake*100 if closed_stake > 0 else 0}%")

if __name__ == "__main__":
    wallet = "0x006cc834cc092684f1b56626e23bedb3835c16ea"
    asyncio.run(inspect_wallet(wallet))
