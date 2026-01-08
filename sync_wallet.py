import asyncio
from app.services.sync_service import sync_trader_full_data
from app.db.session import engine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker

WALLET_ADDRESS = "0x0d3b10b8eac8b089c6e4a695e65d8e044167c46b"

async def main():
    print(f"Syncing data for wallet: {WALLET_ADDRESS}...")
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with async_session() as session:
        try:
            await sync_trader_full_data(WALLET_ADDRESS, session)
            print("Sync complete.")
        except Exception as e:
            print(f"Sync failed: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
