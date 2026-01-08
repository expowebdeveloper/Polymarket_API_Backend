#!/usr/bin/env python3
"""
Universal setup script for volume leaderboard tables.
This script ensures validation:
1. Tables exist (creates them if missing)
2. Schema is up-to-date (adds missing columns like 'roi' if needed)
"""

import asyncio
import sys
import os

# Add the project root to the python path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from dotenv import load_dotenv

# Load environment variables
load_dotenv(os.path.join(project_root, ".env"))

try:
    from app.core.config import settings
    DATABASE_URL = settings.DATABASE_URL
    from app.db.models import Base, DailyVolumeLeaderboard, WeeklyVolumeLeaderboard, MonthlyVolumeLeaderboard
except ImportError as e:
    print(f"⚠️ Error importing app modules: {e}")
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost/polymarket")
    sys.exit(1)

async def ensure_roi_column(conn, table_name):
    """Check if roi column exists and add it if missing."""
    print(f"📊 Checking schema for {table_name}...")
    
    # Add ROI column if it doesn't exist
    await conn.execute(text(f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = '{table_name}' AND column_name = 'roi'
            ) THEN
                ALTER TABLE {table_name} 
                ADD COLUMN roi NUMERIC(10, 4);
                RAISE NOTICE 'Added roi column to {table_name}';
            ELSE
                RAISE NOTICE 'roi column already exists in {table_name}';
            END IF;
        END $$;
    """))

async def run_setup():
    """Run the universal setup."""
    
    print("\n" + "="*60)
    print("Starting Setup: Volume Leaderboard Tables")
    print("="*60 + "\n")
    
    # Create async engine
    engine = create_async_engine(DATABASE_URL, echo=True)
    
    try:
        async with engine.begin() as conn:
            # 1. Create tables if they don't exist
            print("🏗️  Step 1: Ensuring tables exist...")
            await conn.run_sync(Base.metadata.create_all, tables=[
                DailyVolumeLeaderboard.__table__,
                WeeklyVolumeLeaderboard.__table__,
                MonthlyVolumeLeaderboard.__table__
            ])
            print("✅ Step 1 Complete: Tables created (if they were missing)")
            
            # 2. Update schema (add missing columns)
            print("\n🔧 Step 2: Verifying schema (adding missing columns)...")
            await ensure_roi_column(conn, "daily_volume_leaderboard")
            await ensure_roi_column(conn, "weekly_volume_leaderboard")
            await ensure_roi_column(conn, "monthly_volume_leaderboard")
            print("✅ Step 2 Complete: Schema Verified")
            
            # 3. Verification
            print("\n🔍 Step 3: Final Verification...")
            result = await conn.execute(text("""
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
            all_good = True
            for row in rows:
                table, has_roi = row
                status = "✅ READY" if has_roi else "❌ PENDING"
                print(f"{status}: {table}")
                if not has_roi:
                    all_good = False
            
            if all_good:
                print("\n✅ SUCCESS: All tables are ready and up-to-date!")
            else:
                print("\n⚠️ WARNING: Some tables are missing columns.")

    except Exception as e:
        print(f"\n❌ Error during setup: {e}")
        raise
    finally:
        await engine.dispose()
        
    print("\n" + "="*60)

if __name__ == "__main__":
    asyncio.run(run_setup())
