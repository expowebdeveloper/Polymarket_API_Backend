import asyncio
from app.db.session import AsyncSessionLocal
from sqlalchemy import text

async def check_count():
    try:
        async with AsyncSessionLocal() as session:
            res = await session.execute(text('SELECT COUNT(*) FROM markets'))
            count = res.scalar()
            print(f"Total Markets in DB: {count}")
    except Exception as e:
        print(f"Error checking DB: {e}")

if __name__ == "__main__":
    asyncio.run(check_count())
