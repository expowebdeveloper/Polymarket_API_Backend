#!/usr/bin/env python3
"""
Migration script to add portfolio_value column to aggregated_metrics.
Fixes: column aggregated_metrics.portfolio_value does not exist
"""

import asyncio
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.insert(0, project_root)
os.chdir(project_root)

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from dotenv import load_dotenv

load_dotenv(os.path.join(project_root, ".env"))

try:
    from app.core.config import settings
    DATABASE_URL = settings.DATABASE_URL
except ImportError:
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost/polymarket")

async def run_migration():
    engine = create_async_engine(DATABASE_URL, echo=True)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as session:
        try:
            print("\n" + "=" * 60)
            print("Migration: Add portfolio_value to aggregated_metrics")
            print("=" * 60 + "\n")

            if "sqlite" in DATABASE_URL:
                result = await session.execute(text("PRAGMA table_info(aggregated_metrics)"))
                columns = [row[1] for row in result.fetchall()]
                exists = "portfolio_value" in columns
            else:
                result = await session.execute(text("""
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = 'aggregated_metrics'
                      AND column_name = 'portfolio_value'
                """))
                exists = result.scalar_one_or_none() is not None

            if not exists:
                print("Adding portfolio_value column...")
                if "sqlite" in DATABASE_URL:
                    await session.execute(text("""
                        ALTER TABLE aggregated_metrics
                        ADD COLUMN portfolio_value NUMERIC(20, 8) NOT NULL DEFAULT 0
                    """))
                else:
                    await session.execute(text("""
                        ALTER TABLE aggregated_metrics
                        ADD COLUMN IF NOT EXISTS portfolio_value NUMERIC(20, 8) NOT NULL DEFAULT 0
                    """))
                await session.commit()
                print("✅ Added portfolio_value to aggregated_metrics")
            else:
                print("✅ portfolio_value already exists in aggregated_metrics")

            print("\n" + "=" * 60)
            print("Migration completed successfully!")
            print("=" * 60 + "\n")
        except Exception as e:
            print(f"\n❌ Error: {e}")
            await session.rollback()
            raise
        finally:
            await engine.dispose()

if __name__ == "__main__":
    asyncio.run(run_migration())
