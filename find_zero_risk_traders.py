import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select, text
from app.core.config import settings

async def find_zero_risk():
    engine = create_async_engine(settings.DATABASE_URL)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    async with async_session() as session:
        # Use raw SQL to avoid schema issues in SQLAlchemy models
        query = text("""
            SELECT wallet_address, total_trades, total_stakes, worst_loss, score_risk 
            FROM leaderboard_entries 
            WHERE score_risk = 0 
            LIMIT 10
        """)
        
        result = await session.execute(query)
        rows = result.fetchall()
        
        print(f"Found {len(rows)} traders with 0 risk score:")
        for r in rows:
            print(f"Wallet: {r.wallet_address}")
            print(f"  Total Trades: {r.total_trades}")
            print(f"  Total Stakes: {r.total_stakes}")
            print(f"  Worst Loss: {r.worst_loss}")
            print(f"  Score Risk: {r.score_risk}")
            print("-" * 20)

if __name__ == "__main__":
    asyncio.run(find_zero_risk())
