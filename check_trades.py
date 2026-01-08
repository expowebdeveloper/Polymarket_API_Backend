import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select, text, func
from app.db.models import Trade
from app.core.config import settings

async def check_trades():
    engine = create_async_engine(settings.DATABASE_URL)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    async with async_session() as session:
        # Count trades per wallet
        query = text("""
            SELECT proxy_wallet, COUNT(*) as count 
            FROM trades 
            GROUP BY proxy_wallet 
            ORDER BY count DESC 
            LIMIT 5
        """)
        
        result = await session.execute(query)
        rows = result.fetchall()
        
        print("Top wallets by trade count:")
        for r in rows:
            print(f"Wallet: {r.proxy_wallet}, Count: {r.count}")

if __name__ == "__main__":
    asyncio.run(check_trades())
