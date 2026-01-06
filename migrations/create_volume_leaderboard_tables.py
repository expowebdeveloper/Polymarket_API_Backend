#!/usr/bin/env python3
"""
Migration script to create daily, weekly, and monthly volume leaderboard tables.
This fixes the issue where these tables might be missing on the server.
"""

import asyncio
import sys
import os

# Add the project root to the python path so we can import app modules
# This assumes the script is located in <project_root>/migrations/
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Load environment variables
load_dotenv(os.path.join(project_root, ".env"))

try:
    from app.core.config import settings
    DATABASE_URL = settings.DATABASE_URL
    from app.db.models import Base, DailyVolumeLeaderboard, WeeklyVolumeLeaderboard, MonthlyVolumeLeaderboard
except ImportError as e:
    # Fallback if app module is not found
    print(f"⚠️ Error importing app modules: {e}")
    # We still need DATABASE_URL if import fails
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost/polymarket")
    # If imports fail, we can't create tables based on models... 
    # But usually sys.path fix above resolves this
    sys.exit(1)

async def run_migration():
    """Create volume leaderboard tables."""
    
    print("\n" + "="*60)
    print("Starting migration: Creating volume leaderboard tables")
    print("="*60 + "\n")
    
    # Create async engine
    engine = create_async_engine(DATABASE_URL, echo=True)
    
    try:
        async with engine.begin() as conn:
            # Check and create tables
            print("📊 Checking and creating volume leaderboard tables...")
            
            # This will create tables only if they don't exist
            # It uses the schema defined in the models imported above
            await conn.run_sync(Base.metadata.create_all, tables=[
                DailyVolumeLeaderboard.__table__,
                WeeklyVolumeLeaderboard.__table__,
                MonthlyVolumeLeaderboard.__table__
            ])
            
            print("✅ Tables check/creation complete!")
            
    except Exception as e:
        print(f"\n❌ Error during migration: {e}")
        raise
    finally:
        await engine.dispose()
        
    print("\n" + "="*60)
    print("✅ Migration script finished!")
    print("="*60 + "\n")

if __name__ == "__main__":
    asyncio.run(run_migration())
