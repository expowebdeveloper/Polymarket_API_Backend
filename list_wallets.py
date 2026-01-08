
import asyncio
from sqlalchemy import select, func
from app.db.session import AsyncSessionLocal
from app.db.models import Position, TraderLeaderboard

async def list_wallets():
    async with AsyncSessionLocal() as session:
        stmt = select(Position.proxy_wallet, func.count()).group_by(Position.proxy_wallet).limit(10)
        result = await session.execute(stmt)
        for row in result:
            print(f"Position Wallet: {row[0]}, Count: {row[1]}")

        stmt = select(TraderLeaderboard.wallet_address).limit(10)
        result = await session.execute(stmt)
        for row in result:
            print(f"Leaderboard Wallet: {row[row.keys()[0]]}")

if __name__ == "__main__":
    asyncio.run(list_wallets())
