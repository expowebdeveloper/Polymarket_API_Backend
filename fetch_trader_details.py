"""
Script to fetch detailed trader data for all traders in trader_leaderboard.

This script:
1. Reads all traders from trader_leaderboard table
2. Fetches detailed data from Polymarket APIs:
   - Profile stats
   - Value
   - Positions
   - Activity
   - Closed positions
   - Trades
3. Stores data in respective tables (trader_profile, trader_value, etc.)
"""

import asyncio
import sys
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from app.core.config import settings
from app.db.models import Base
from app.services.trader_detail_service import (
    fetch_and_save_trader_details,
    fetch_and_save_all_traders_details
)


async def check_tables_exist(engine):
    """Check if all required tables exist, create if not."""
    required_tables = [
        "trader_profile",
        "trader_value",
        "trader_positions",
        "trader_activity",
        "trader_closed_positions",
        "trader_trades"
    ]
    
    # Check which tables exist using SQL query
    async with engine.begin() as conn:
        existing_tables = []
        for table_name in required_tables:
            try:
                result = await conn.execute(
                    text("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_schema = 'public' 
                            AND table_name = :table_name
                        )
                    """),
                    {"table_name": table_name}
                )
                exists = result.scalar()
                if exists:
                    existing_tables.append(table_name)
            except Exception:
                pass  # Table doesn't exist
        
        missing_tables = [t for t in required_tables if t not in existing_tables]
        
        if missing_tables:
            print(f"Creating missing tables: {', '.join(missing_tables)}...")
            from app.db.models import (
                TraderProfile, TraderValue, TraderPosition,
                TraderActivity, TraderClosedPosition, TraderTrade
            )
            
            # Create tables using async method
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
            print("✅ All tables created successfully!")
        else:
            print("✅ All required tables exist")


async def main():
    """Main function to fetch trader details."""
    print("="*60)
    print("Polymarket Trader Details Fetching Script")
    print("="*60)
    
    # Create database engine
    engine = create_async_engine(
        settings.DATABASE_URL,
        echo=False
    )
    
    try:
        # Check and create tables
        await check_tables_exist(engine)
        
        # Check how many traders we have
        AsyncSessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=engine,
            class_=AsyncSession
        )
        
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                text("SELECT COUNT(*) FROM trader_leaderboard")
            )
            total_traders = result.scalar()
            
            print(f"\n📊 Found {total_traders} traders in trader_leaderboard table")
            
            if total_traders == 0:
                print("⚠️  No traders found in trader_leaderboard. Run fetch_leaderboard_data.py first.")
                return
            
            # Ask user if they want to process all or specific number
            print(f"\n🚀 Starting to fetch detailed data for all traders...")
            print(f"   This will fetch: profile, value, positions, activity, closed positions, trades")
            print(f"   Processing {total_traders} traders...\n")
            
            # Fetch details for all traders
            result = await fetch_and_save_all_traders_details(
                session=session,
                limit=None,  # Process all
                offset=0
            )
            
            await session.commit()
            
            # Print results
            print(f"\n{'='*60}")
            print(f"📊 FINAL STATISTICS")
            print(f"{'='*60}")
            print(f"Total traders processed:     {result['processed']}")
            print(f"Profile stats saved:         {result['summary']['profile_saved']}")
            print(f"Value data saved:            {result['summary']['value_saved']}")
            print(f"Positions saved:             {result['summary']['total_positions_saved']}")
            print(f"Activities saved:            {result['summary']['total_activities_saved']}")
            print(f"Closed positions saved:      {result['summary']['total_closed_positions_saved']}")
            print(f"Trades saved:                {result['summary']['total_trades_saved']}")
            
            if result['summary']['errors']:
                print(f"\n⚠️  Errors encountered: {len(result['summary']['errors'])}")
                print(f"   First 5 errors:")
                for error in result['summary']['errors'][:5]:
                    print(f"   - {error}")
            
            print(f"{'='*60}\n")
        
        print("✅ Script completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
