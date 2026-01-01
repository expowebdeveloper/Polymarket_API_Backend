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
        
        # Create session factory for individual trader sessions
        AsyncSessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=engine,
            class_=AsyncSession
        )
        
        # Create a session factory function that returns a context manager
        def create_trader_session():
            return AsyncSessionLocal()
        
        # Use a single session for reading trader list
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                text("SELECT COUNT(*) FROM trader_leaderboard")
            )
            total_traders = result.scalar()
            
            print(f"\n📊 Found {total_traders} traders in trader_leaderboard table")
            
            if total_traders == 0:
                print("⚠️  No traders found in trader_leaderboard. Run fetch_leaderboard_data.py first.")
                return
            
            # Check command line arguments
            force_refresh = '--force' in sys.argv or '--refresh-all' in sys.argv
            
            print(f"\n🚀 Starting to fetch detailed data for all traders...")
            if force_refresh:
                print(f"   ⚠️  FORCE REFRESH MODE: Will fetch all data regardless of what exists")
            else:
                print(f"   ✅ INCREMENTAL MODE: Will only fetch new/updated data")
                print(f"      - Profile/Value: Only if missing or older than 24 hours")
                print(f"      - Activities/Closed Positions/Trades: Only new records after latest timestamp")
            print(f"   This will fetch: profile, value, positions, activity, closed positions, trades")
            print(f"   Processing {total_traders} traders...\n")
            
            # Fetch details for all traders
            result = await fetch_and_save_all_traders_details(
                session=session,
                limit=None,  # Process all
                offset=0,
                force_refresh=force_refresh,
                session_factory=create_trader_session
            )
            
            # Print results
            print(f"\n{'='*60}")
            print(f"📊 FINAL STATISTICS")
            print(f"{'='*60}")
            print(f"Total traders processed:     {result['processed']}")
            print(f"\n📥 Data Saved:")
            print(f"   Profile stats saved:         {result['summary']['profile_saved']}")
            print(f"   Value data saved:            {result['summary']['value_saved']}")
            print(f"   Positions saved:             {result['summary']['total_positions_saved']}")
            print(f"   Activities saved:            {result['summary']['total_activities_saved']}")
            print(f"   Closed positions saved:      {result['summary']['total_closed_positions_saved']}")
            print(f"   Trades saved:                {result['summary']['total_trades_saved']}")
            
            if not force_refresh and result['summary'].get('skipped'):
                skipped = result['summary']['skipped']
                print(f"\n⏭️  Skipped (already up-to-date):")
                if skipped.get('profile', 0) > 0:
                    print(f"   Profiles skipped:             {skipped['profile']}")
                if skipped.get('value', 0) > 0:
                    print(f"   Values skipped:               {skipped['value']}")
                if skipped.get('activities', 0) > 0:
                    print(f"   Activities skipped (no new):   {skipped['activities']}")
                if skipped.get('closed_positions', 0) > 0:
                    print(f"   Closed positions skipped:     {skipped['closed_positions']}")
                if skipped.get('trades', 0) > 0:
                    print(f"   Trades skipped (no new):       {skipped['trades']}")
            
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
