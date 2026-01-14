"""
Comprehensive migration script to run on the server.

This script:
1. Creates all missing tables from models
2. Runs specific migrations (users table, etc.)
3. Ensures all new trader detail tables exist

Run this on the server after deploying new code:
    python run_migrations.py
"""

import asyncio
import sys
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from app.core.config import settings
from app.db.models import Base
from app.db.session import AsyncSessionLocal


async def check_table_exists(session, table_name: str) -> bool:
    """Check if a table exists."""
    try:
        if "sqlite" in settings.DATABASE_URL:
             result = await session.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name=:table_name"),
                {"table_name": table_name}
            )
             return bool(result.scalar())
        
        result = await session.execute(
            text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = :table_name
                )
            """),
            {"table_name": table_name}
        )
        return result.scalar()
    except Exception:
        return False


async def create_all_tables(engine):
    """Create all tables from models."""
    print("Creating all tables from models...")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("✅ All tables created!")


async def migrate_users_table(session):
    """Add missing columns to users table if needed."""
    try:
        # First check if table exists
        # First check if table exists
        table_exists = await check_table_exists(session, 'users')
        
        if not table_exists:
            print("⚠️  Users table doesn't exist. It will be created in Step 1.")
            return
        
        # Check if password_hash column exists
        if "sqlite" in settings.DATABASE_URL:
            result = await session.execute(text("PRAGMA table_info(users)"))
            # Row structure: (cid, name, type, notnull, dflt_value, pk)
            columns = [row[1] for row in result.fetchall()]
            exists = 'password_hash' in columns
        else:
            result = await session.execute(
                text("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'users' AND column_name = 'password_hash'
                """)
            )
            exists = result.scalar_one_or_none()
        
        if not exists:
            print("Adding password_hash, created_at, and updated_at columns to users table...")
            
            # Add password_hash column
            await session.execute(text("""
                ALTER TABLE users 
                ADD COLUMN IF NOT EXISTS password_hash VARCHAR NOT NULL DEFAULT ''
            """))
            
            # Add created_at column
            await session.execute(text("""
                ALTER TABLE users 
                ADD COLUMN IF NOT EXISTS created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            """))
            
            # Add updated_at column
            await session.execute(text("""
                ALTER TABLE users 
                ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            """))
            
            # Update existing rows
            await session.execute(text("""
                UPDATE users 
                SET password_hash = '' 
                WHERE password_hash IS NULL OR password_hash = ''
            """))
            
            # Remove default after setting values
            await session.execute(text("""
                ALTER TABLE users 
                ALTER COLUMN password_hash DROP DEFAULT
            """))
            
            await session.commit()
            print("✅ Successfully added columns to users table")
        else:
            print("✅ Users table columns already exist")
            
    except Exception as e:
        await session.rollback()
        print(f"⚠️  Error migrating users table: {e}")
        # Don't raise - continue with other migrations


async def ensure_trader_detail_tables(engine):
    """Ensure all trader detail tables exist."""
    required_tables = [
        "trader_profile",
        "trader_value",
        "trader_positions",
        "trader_activity",
        "trader_closed_positions",
        "trader_trades"
    ]
    
    async with AsyncSessionLocal() as session:
        missing_tables = []
        for table_name in required_tables:
            exists = await check_table_exists(session, table_name)
            if not exists:
                missing_tables.append(table_name)
        
        if missing_tables:
            print(f"Creating missing trader detail tables: {', '.join(missing_tables)}...")
            from app.db.models import (
                TraderProfile, TraderValue, TraderPosition,
                TraderActivity, TraderClosedPosition, TraderTrade
            )
            
            async with engine.begin() as conn:
                await conn.run_sync(
                    lambda sync_conn: Base.metadata.create_all(
                        sync_conn,
                        tables=[
                            TraderProfile.__table__,
                            TraderValue.__table__,
                            TraderPosition.__table__,
                            TraderActivity.__table__,
                            TraderClosedPosition.__table__,
                            TraderTrade.__table__
                        ]
                    )
                )
            print("✅ All trader detail tables created!")
        else:
            print("✅ All trader detail tables already exist")


async def main():
    """Run all migrations."""
    print("="*60)
    print("Database Migration Script")
    print("="*60)
    print(f"Database: {settings.DATABASE_URL.split('@')[-1] if '@' in settings.DATABASE_URL else 'N/A'}")
    print()
    
    # Create database engine
    engine = create_async_engine(
        settings.DATABASE_URL,
        echo=False
    )
    
    try:
        # Step 1: Create all tables from models
        print("Step 1: Creating all tables from models...")
        await create_all_tables(engine)
        print()
        
        # Step 2: Run specific migrations
        print("Step 2: Running specific migrations...")
        async with AsyncSessionLocal() as session:
            await migrate_users_table(session)
        print()
        
        # Step 3: Ensure trader detail tables exist
        print("Step 3: Ensuring trader detail tables exist...")
        await ensure_trader_detail_tables(engine)
        print()
        
        # Step 4: Verify critical tables
        print("Step 4: Verifying critical tables...")
        async with AsyncSessionLocal() as session:
            critical_tables = [
                "users",
                "trader_leaderboard",
                "trader_profile",
                "trader_value",
                "trader_positions",
                "trader_activity",
                "trader_closed_positions",
                "trader_trades"
            ]
            
            all_exist = True
            for table_name in critical_tables:
                exists = await check_table_exists(session, table_name)
                status = "✅" if exists else "❌"
                print(f"   {status} {table_name}")
                if not exists:
                    all_exist = False
        
        print()
        if all_exist:
            print("="*60)
            print("✅ All migrations completed successfully!")
            print("="*60)
        else:
            print("="*60)
            print("⚠️  Some tables are missing. Check errors above.")
            print("="*60)
            sys.exit(1)
        
    except Exception as e:
        print(f"\n❌ Fatal error during migration: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
