import asyncio
import os
import sys
from decimal import Decimal

# Add project root to sys.path
sys.path.append(os.getcwd())

from app.db.session import AsyncSessionLocal
from app.services.live_leaderboard_service import fetch_raw_metrics_for_scoring
from app.services.leaderboard_service import (
    calculate_trader_metrics_with_time_filter,
    calculate_scores_and_rank_with_percentiles
)

async def compare_api_vs_db():
    print("🚀 Starting API vs DB Comparison Test...")
    file_path = "wallet_address.txt"
    
    if not os.path.exists(file_path):
        print(f"❌ Error: {file_path} not found.")
        return

    # 1. Fetch raw metrics from API
    print("📡 Fetching raw metrics from API...")
    api_raw_metrics = await fetch_raw_metrics_for_scoring(file_path)
    
    # 2. Fetch raw metrics from DB
    print("🗄️ Fetching raw metrics from DB...")
    db_raw_metrics = []
    async with AsyncSessionLocal() as session:
        for wallet_info in api_raw_metrics:
            wallet = wallet_info["wallet_address"]
            metrics = await calculate_trader_metrics_with_time_filter(session, wallet, period='all')
            if metrics:
                db_raw_metrics.append(metrics)
            else:
                # If no data in DB, we'll have a gap for this trader
                print(f"⚠️ No DB data for {wallet}")

    # To compare fairly, we should only compare wallets that exist in both sets
    api_map = {m["wallet_address"]: m for m in api_raw_metrics}
    db_map = {m["wallet_address"]: m for m in db_raw_metrics}
    common_wallets = set(api_map.keys()) & set(db_map.keys())
    
    if not common_wallets:
        print("❌ Error: No common wallets found between API and DB.")
        return

    print(f"📊 Found {len(common_wallets)} common wallets for comparison.")

    # 3. Calculate scores for both sets separately
    # Note: Population medians will depend on the set size, so we calculate for the common set
    common_api_raw = [api_map[w] for w in common_wallets]
    common_db_raw = [db_map[w] for w in common_wallets]
    
    api_results = calculate_scores_and_rank_with_percentiles(common_api_raw)
    db_results = calculate_scores_and_rank_with_percentiles(common_db_raw)
    
    api_final = {t["wallet_address"]: t for t in api_results["traders"]}
    db_final = {t["wallet_address"]: t for t in db_results["traders"]}

    # 4. Compare specific metrics
    print("\n" + "="*50)
    print(f"{'Wallet':<15} | {'Metric':<10} | {'API Val':<10} | {'DB Val':<10} | {'Diff':<10}")
    print("-"*50)

    discrepancies = 0
    metrics_to_check = ["total_pnl", "total_stakes", "W_shrunk", "final_score"]

    for wallet in common_wallets:
        a = api_final[wallet]
        d = db_final[wallet]
        
        for metric in metrics_to_check:
            a_val = a.get(metric, 0)
            d_val = d.get(metric, 0)
            diff = abs(a_val - d_val)
            
            # Allow for tiny floating point differences, but flag anything else
            if diff > 0.1:
                print(f"{wallet[:10]:<15} | {metric:<10} | {a_val:<10.2f} | {d_val:<10.2f} | {diff:<10.2f} ⚠️")
                discrepancies += 1
            else:
                # print(f"{wallet[:10]:<15} | {metric:<10} | {a_val:<10.2f} | {d_val:<10.2f} | {diff:<10.2f} ✅")
                pass

    print("="*50)
    if discrepancies == 0:
        print("✅ SUCCESS: API and DB data are perfectly aligned (within rounding error).")
    else:
        print(f"❌ Found {discrepancies} metric discrepancies.")
        print("Tip: Discrepancies are often due to DB sync delays or missing trades in local DB.")

if __name__ == "__main__":
    asyncio.run(compare_api_vs_db())
