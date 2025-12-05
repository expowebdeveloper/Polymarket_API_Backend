"""
Migration script to add missing columns to trades table.
Run this script to add entry_price, exit_price, and pnl columns if they don't exist.
"""

import asyncio
import sys
from sqlalchemy import text, inspect
from sqlalchemy.ext.asyncio import create_async_engine
from app.core.config import settings


async def check_column_exists(conn, table_name: str, column_name: str) -> bool:
    """Check if a column exists in a table."""
    query = text("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = :table_name AND column_name = :column_name
    """)
    result = await conn.execute(query, {"table_name": table_name, "column_name": column_name})
    return result.fetchone() is not None


async def migrate_trades_table():
    """Add missing columns to trades table if they don't exist."""
    engine = create_async_engine(settings.DATABASE_URL, echo=True)
    
    try:
        async with engine.begin() as conn:
            # Check which columns exist
            has_entry_price = await check_column_exists(conn, "trades", "entry_price")
            has_exit_price = await check_column_exists(conn, "trades", "exit_price")
            has_pnl = await check_column_exists(conn, "trades", "pnl")
            has_trader_id = await check_column_exists(conn, "trades", "trader_id")
            
            print("Checking trades table columns...")
            print(f"  entry_price exists: {has_entry_price}")
            print(f"  exit_price exists: {has_exit_price}")
            print(f"  pnl exists: {has_pnl}")
            print(f"  trader_id exists: {has_trader_id}")
            
            # Add missing columns
            if not has_entry_price:
                print("Adding entry_price column...")
                await conn.execute(text("""
                    ALTER TABLE trades 
                    ADD COLUMN entry_price NUMERIC(10, 8)
                """))
                print("✓ Added entry_price column")
            
            if not has_exit_price:
                print("Adding exit_price column...")
                await conn.execute(text("""
                    ALTER TABLE trades 
                    ADD COLUMN exit_price NUMERIC(10, 8)
                """))
                print("✓ Added exit_price column")
            
            if not has_pnl:
                print("Adding pnl column...")
                await conn.execute(text("""
                    ALTER TABLE trades 
                    ADD COLUMN pnl NUMERIC(20, 8)
                """))
                print("✓ Added pnl column")
            
            if not has_trader_id:
                print("Adding trader_id column...")
                await conn.execute(text("""
                    ALTER TABLE trades 
                    ADD COLUMN trader_id INTEGER
                """))
                print("✓ Added trader_id column")
            
            print("\n✅ Migration complete! All required columns now exist.")
            
    except Exception as e:
        print(f"❌ Error during migration: {e}")
        sys.exit(1)
    finally:
        await engine.dispose()


async def main():
    """Main function to run migration."""
    print("Starting trades table migration...")
    print(f"Database: {settings.DATABASE_URL.split('@')[1] if '@' in settings.DATABASE_URL else 'hidden'}\n")
    
    await migrate_trades_table()


if __name__ == "__main__":
    asyncio.run(main())

