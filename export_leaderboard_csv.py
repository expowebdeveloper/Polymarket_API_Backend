
import asyncio
import csv
import sys
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from app.core.config import settings

OUTPUT_FILE = "leaderboard_export.csv"

async def export_csv():
    print(f"Connecting to database: {settings.DATABASE_URL}")
    engine = create_async_engine(settings.DATABASE_URL)
    
    try:
        async with engine.connect() as conn:
            # Check if data exists
            count_res = await conn.execute(text("SELECT COUNT(*) FROM trader_calculated_scores"))
            count = count_res.scalar()
            
            if count == 0:
                print("⚠️  WARNING: trader_calculated_scores table is EMPTY.")
                print("   The CSV will be empty.")
                print("   Please run the following scripts first:")
                print("     1. python fetch_leaderboard_data.py")
                print("     2. python fetch_trader_active_positions.py")
                print("     3. python fetch_trader_closed_positions.py")
                print("     4. python calculate_trader_scores.py")
                return

            print(f"Found {count} records. Exporting...")

            # Query with join to get names from trader_leaderboard if needed
            # (Though TraderCalculatedScore might not have name/pseudonym directly populated depending on logic, 
            #  but the schema has them in TraderLeaderboard)
            query = text("""
                SELECT 
                    tcs.rank,
                    tl.name,
                    tl.pseudonym,
                    tcs.wallet_address,
                    tcs.final_score,
                    tcs.win_rate,
                    tcs.roi,
                    tcs.total_pnl,
                    tcs.trades,
                    tcs.risk_score,
                    tcs.w_shrunk,
                    tcs.roi_shrunk,
                    tcs.pnl_shrunk
                FROM trader_calculated_scores tcs
                JOIN trader_leaderboard tl ON tcs.trader_id = tl.id
                ORDER BY tcs.rank ASC
            """)
            
            result = await conn.execute(query)
            rows = result.fetchall()
            
            with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as csvfile:
                fieldnames = [
                    "Rank", "Name", "Pseudonym", "Wallet", "Final Score", 
                    "Win Rate (%)", "ROI (%)", "Total PnL", "Total Trades", 
                    "Risk Score", "W Shrunk", "ROI Shrunk", "PnL Shrunk"
                ]
                writer = csv.writer(csvfile)
                writer.writerow(fieldnames)
                
                for row in rows:
                    writer.writerow(row)
            
            print(f"✅ Successfully exported {len(rows)} traders to {OUTPUT_FILE}")

    except Exception as e:
        print(f"❌ Error exporting CSV: {e}")
    finally:
        await engine.dispose()

if __name__ == "__main__":
    asyncio.run(export_csv())
