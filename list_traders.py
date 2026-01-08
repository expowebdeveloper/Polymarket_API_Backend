import asyncio
from sqlalchemy import text
from app.db.session import engine

async def main():
    async with engine.connect() as conn:
        res = await conn.execute(text("SELECT wallet_address, name FROM traders WHERE wallet_address LIKE '0x0d3b%'"))
        print("Existing Traders:")
        for row in res:
            print(f"Address: {row[0]}, Name: {row[1]}")

if __name__ == "__main__":
    asyncio.run(main())
