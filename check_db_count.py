import asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from app.core.config import settings

async def check():
    engine = create_async_engine(settings.DATABASE_URL)
    try:
        async with engine.connect() as conn:
            # Check if table exists first
            try:
                res = await conn.execute(text("SELECT COUNT(*) FROM trader_calculated_scores"))
                print(f"Row Count: {res.scalar()}")
            except Exception as e:
                print(f"Table might not exist or empty: {e}")
    finally:
        await engine.dispose()

if __name__ == "__main__":
    asyncio.run(check())
