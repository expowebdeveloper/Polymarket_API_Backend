import asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from app.core.config import settings

async def main():
    engine = create_async_engine(settings.DATABASE_URL)
    async with engine.connect() as conn:
        res = await conn.execute(text('SELECT wallet_address, final_score FROM leaderboard_entries LIMIT 10'))
        print(res.fetchall())

if __name__ == "__main__":
    asyncio.run(main())
