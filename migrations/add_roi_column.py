#!/usr/bin/env python3
"""
Migration script to add missing ROI column to volume leaderboard tables.
This fixes the "column roi does not exist" error.
"""

import asyncio
import sys
import os

# Add the project root to the python path so we can import app modules
# This assumes the script is located in <project_root>/migrations/
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from dotenv import load_dotenv

# Load environment variables
load_dotenv(os.path.join(project_root, ".env"))

try:
    from app.core.config import settings
    DATABASE_URL = settings.DATABASE_URL
except ImportError:
    # Fallback if app module is not found (though sys.path update should fix this)
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost/polymarket")
    print(f"⚠️ Could not import settings from app.core.config. Using env DATABASE_URL: {DATABASE_URL}")

async def run_migration():
    """Run the migration to add ROI column to volume leaderboard tables."""
    
    # Create async engine
    engine = create_async_engine(DATABASE_URL, echo=True)
    
    # Create async session
    async_session = sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )
    
    async with async_session() as session:
        try:
            print("\n" + "="*60)
            print("Starting migration: Adding ROI column to volume leaderboards")
            print("="*60 + "\n")
            
            # Add ROI column to daily_volume_leaderboard
            print("📊 Checking daily_volume_leaderboard...")
            await session.execute(text("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'daily_volume_leaderboard' AND column_name = 'roi'
                    ) THEN
                        ALTER TABLE daily_volume_leaderboard 
                        ADD COLUMN roi NUMERIC(10, 4);
                        RAISE NOTICE 'Added roi column to daily_volume_leaderboard';
                    ELSE
                        RAISE NOTICE 'roi column already exists in daily_volume_leaderboard';
                    END IF;
                END $$;
            """))
            
            # Add ROI column to weekly_volume_leaderboard
            print("📊 Checking weekly_volume_leaderboard...")
            await session.execute(text("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'weekly_volume_leaderboard' AND column_name = 'roi'
                    ) THEN
                        ALTER TABLE weekly_volume_leaderboard 
                        ADD COLUMN roi NUMERIC(10, 4);
                        RAISE NOTICE 'Added roi column to weekly_volume_leaderboard';
                    ELSE
                        RAISE NOTICE 'roi column already exists in weekly_volume_leaderboard';
                    END IF;
                END $$;
            """))
            
            # Add ROI column to monthly_volume_leaderboard
            print("📊 Checking monthly_volume_leaderboard...")
            await session.execute(text("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'monthly_volume_leaderboard' AND column_name = 'roi'
                    ) THEN
                        ALTER TABLE monthly_volume_leaderboard 
                        ADD COLUMN roi NUMERIC(10, 4);
                        RAISE NOTICE 'Added roi column to monthly_volume_leaderboard';
                    ELSE
                        RAISE NOTICE 'roi column already exists in monthly_volume_leaderboard';
                    END IF;
                END $$;
            """))
            
            # Commit the changes
            await session.commit()
            
            # Verify the columns were added
            print("\n" + "="*60)
            print("Verifying migration...")
            print("="*60 + "\n")
            
            result = await session.execute(text("""
                SELECT 
                    'daily_volume_leaderboard' as table_name,
                    EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'daily_volume_leaderboard' AND column_name = 'roi'
                    ) as has_roi_column
                UNION ALL
                SELECT 
                    'weekly_volume_leaderboard' as table_name,
                    EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'weekly_volume_leaderboard' AND column_name = 'roi'
                    ) as has_roi_column
                UNION ALL
                SELECT 
                    'monthly_volume_leaderboard' as table_name,
                    EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'monthly_volume_leaderboard' AND column_name = 'roi'
                    ) as has_roi_column;
            """))
            
            rows = result.fetchall()
            for row in rows:
                table_name, has_roi = row
                status = "✅ HAS ROI" if has_roi else "❌ MISSING ROI"
                print(f"{status}: {table_name}")
            
            print("\n" + "="*60)
            print("✅ Migration completed successfully!")
            print("="*60 + "\n")
            
        except Exception as e:
            print(f"\n❌ Error during migration: {e}")
            await session.rollback()
            raise
        finally:
            await engine.dispose()

if __name__ == "__main__":
    asyncio.run(run_migration())
