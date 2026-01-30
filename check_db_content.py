
import asyncio
import sys
import os
sys.path.append(os.getcwd())
from sqlalchemy import text
from app.db.session import AsyncSessionLocal

async def check_data():
    async with AsyncSessionLocal() as session:
        print("--- Checking leaderboard_entries ---")
        result = await session.execute(text("SELECT count(*), avg(final_score), max(final_score) FROM leaderboard_entries"))
        row = result.fetchone()
        print(f"Count: {row[0]}, Avg Score: {row[1]}, Max Score: {row[2]}")
        
        print("\n--- Checking daily_volume_leaderboard ---")
        result = await session.execute(text("SELECT count(*), avg(final_score), max(final_score), count(final_score) FILTER (WHERE final_score > 0) FROM daily_volume_leaderboard"))
        row = result.fetchone()
        print(f"Count: {row[0]}, Avg Score: {row[1]}, Max Score: {row[2]}")
        print(f"Rows with Score > 0: {row[3]}")

if __name__ == "__main__":
    asyncio.run(check_data())
