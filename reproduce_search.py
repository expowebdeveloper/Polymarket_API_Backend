import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select
from app.db.models import Trader, TraderLeaderboard, Base
from app.core.config import settings

# Setup DB connection
engine = create_async_engine(settings.DATABASE_URL, echo=False)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

async def search_user(query: str):
    async with AsyncSessionLocal() as session:
        print(f"Searching for '{query}'...")
        
        # 1. Search in TraderLeaderboard (Polymarket Username)
        stmt = select(TraderLeaderboard).where(TraderLeaderboard.name.ilike(f"%{query}%"))
        result = await session.execute(stmt)
        traders = result.scalars().all()
        
        for t in traders:
            print(f"Found in Leaderboard (Name): {t.name} ({t.wallet_address})")

        # 2. Search in TraderLeaderboard (X Username)
        stmt = select(TraderLeaderboard).where(TraderLeaderboard.pseudonym.ilike(f"%{query}%"))
        result = await session.execute(stmt)
        traders = result.scalars().all()
        
        for t in traders:
            print(f"Found in Leaderboard (X): {t.pseudonym} ({t.wallet_address})")

        # 3. Search in Trader table
        stmt = select(Trader).where(Trader.name.ilike(f"%{query}%"))
        result = await session.execute(stmt)
        traders = result.scalars().all()
        
        for t in traders:
            print(f"Found in Trader: {t.name} ({t.wallet_address})")

async def main():
    # Test with some common string or a known username if possible
    # asking user for input or using a generic one
    await search_user("Don")
    await search_user("Trump")

if __name__ == "__main__":
    asyncio.run(main())
