import asyncio
from sqlalchemy import text
from app.db.session import engine

async def main():
    async with engine.connect() as conn:
        res = await conn.execute(text("SELECT proxy_wallet, count(*) as loss_count FROM trades WHERE pnl < 0 GROUP BY proxy_wallet HAVING count(*) > 5 LIMIT 1"))
        row = res.fetchone()
        if row:
            print(f"Trader with losses: {row[0]}, Losses: {row[1]}")
        else:
            print("No trader with enough losses found.")

if __name__ == "__main__":
    asyncio.run(main())
