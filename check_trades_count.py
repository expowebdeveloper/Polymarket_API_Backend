
import asyncio
from sqlalchemy import select, distinct, func
from app.db.session import AsyncSessionLocal
from app.db.models import Trade

async def check_trades_count():
    async with AsyncSessionLocal() as session:
        # Count unique wallets in Trade
        stmt = select(func.count(distinct(Trade.proxy_wallet)))
        result = await session.execute(stmt)
        unique_wallets = result.scalar()
        print(f"Unique wallets in Trade table: {unique_wallets}")

        # Count total trades
        stmt = select(func.count(Trade.id))
        result = await session.execute(stmt)
        total_trades = result.scalar()
        print(f"Total trades: {total_trades}")

if __name__ == "__main__":
    asyncio.run(check_trades_count())
