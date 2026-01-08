import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select, text
from app.core.config import settings

async def check_screenshot_wallet():
    engine = create_async_engine(settings.DATABASE_URL)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    wallet = "0xd6b4b1910d4479184c4e17e01ec83ecbd376a278"
    
    async with async_session() as session:
        # Check leaderboard entry
        query = text("""
            SELECT * FROM leaderboard_entries WHERE wallet_address = :w
        """)
        result = await session.execute(query, {"w": wallet})
        row = result.fetchone()
        
        if row:
            print("Leaderboard Entry:")
            for k in row.keys():
                print(f"  {k}: {getattr(row, k)}")
        else:
            print("No leaderboard entry found for this wallet.")
            
        # Check trades count
        query = text("SELECT COUNT(*) FROM trades WHERE proxy_wallet = :w")
        result = await session.execute(query, {"w": wallet})
        print(f"Trades in DB: {result.scalar()}")
        
        # Check closed positions count
        query = text("SELECT COUNT(*) FROM closed_positions WHERE proxy_wallet = :w")
        result = await session.execute(query, {"w": wallet})
        print(f"Closed Positions in DB: {result.scalar()}")

if __name__ == "__main__":
    asyncio.run(check_screenshot_wallet())
