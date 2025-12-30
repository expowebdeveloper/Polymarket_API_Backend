import asyncio
import sys
import os
from decimal import Decimal

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.db.session import AsyncSessionLocal
from app.services.db_scoring_service import get_advanced_db_analytics, get_db_leaderboard
from app.services.leaderboard_service import calculate_scores_and_rank_with_percentiles
from app.services.live_leaderboard_service import fetch_raw_metrics_for_scoring

async def verify_alignment():
    print("Starting verification of Leaderboard Alignment...")
    
    file_path = "wallet_address.txt"
    if not os.path.exists(file_path):
        print(f"Error: {file_path} not found.")
        return

    async with AsyncSessionLocal() as session:
        # 1. Fetch live metrics (API logic)
        print("Fetching live metrics for comparison...")
        live_metrics = await fetch_raw_metrics_for_scoring(file_path)
        if not live_metrics:
            print("No live metrics fetched. Check wallet_address.txt")
            return
            
        # Limit to first 10 for quick check
        test_wallets = [m['wallet_address'] for m in live_metrics[:10]]
        print(f"Testing with {len(test_wallets)} wallets: {test_wallets}")
        
        # Calculate live scores
        live_result = calculate_scores_and_rank_with_percentiles(live_metrics)
        live_traders = {t['wallet_address']: t for t in live_result['traders']}
        
        # 2. Fetch DB metrics (DB logic)
        print("Calculating DB-backed metrics...")
        db_result = await get_advanced_db_analytics(session, wallet_addresses=test_wallets)
        db_traders = {t['wallet_address']: t for t in db_result['traders']}
        
        # 3. Compare W_shrunk and final_score
        print("\n--- Alignment Comparison (Live vs DB) ---")
        match_count = 0
        total_count = 0
        
        for wallet in test_wallets:
            if wallet not in db_traders:
                print(f"Wallet {wallet} missing in DB results.")
                continue
                
            total_count += 1
            l_trader = live_traders[wallet]
            d_trader = db_traders[wallet]
            
            w_diff = abs(l_trader.get('W_shrunk', 0) - d_trader.get('W_shrunk', 0))
            s_diff = abs(l_trader.get('final_score', 0) - d_trader.get('final_score', 0))
            
            print(f"Wallet: {wallet[:10]}...")
            print(f"  Live W_shrunk: {l_trader.get('W_shrunk'):.6f} | DB W_shrunk: {d_trader.get('W_shrunk'):.6f} | Diff: {w_diff:.6f}")
            print(f"  Live Score:    {l_trader.get('final_score'):.6f} | DB Score:    {d_trader.get('final_score'):.6f} | Diff: {s_diff:.6f}")
            
            if w_diff < 0.001 and s_diff < 0.001:
                match_count += 1
            else:
                print(f"  [!] DISCREPANCY DETECTED for {wallet}")

        print(f"\nAlignment Result: {match_count}/{total_count} wallets aligned.")

        # 4. Verify Pagination
        print("\n--- Pagination Verification (DB) ---")
        # Fetch with offset 0, limit 5
        page1 = await get_db_leaderboard(session, wallet_addresses=None, limit=5, offset=0, metric="final_score")
        # Fetch with offset 5, limit 5
        page2 = await get_db_leaderboard(session, wallet_addresses=None, limit=5, offset=5, metric="final_score")
        
        if page1 and page2:
            print(f"Page 1 first trader: {page1[0]['wallet_address']} (Score: {page1[0].get('final_score'):.4f})")
            print(f"Page 1 last trader:  {page1[-1]['wallet_address']} (Score: {page1[-1].get('final_score'):.4f})")
            print(f"Page 2 first trader: {page2[0]['wallet_address']} (Score: {page2[0].get('final_score'):.4f})")
            
            # Ensure no overlap
            page1_wallets = set(t['wallet_address'] for t in page1)
            page2_wallets = set(t['wallet_address'] for t in page2)
            overlap = page1_wallets.intersection(page2_wallets)
            
            if not overlap:
                print("Pagination SUCCESS: No overlap between consecutive pages.")
                # Verify ranks
                if page2[0]['rank'] == 6:
                    print(f"Rank sequencing SUCCESS: Page 2 starts with rank {page2[0]['rank']}.")
                else:
                    print(f"Rank sequencing FAILURE: Page 2 expected rank 6, got {page2[0]['rank']}.")
            else:
                print(f"Pagination FAILURE: Overlap detected: {overlap}")
        else:
            print("Pagination skipping: insufficient data in DB.")

        # 5. Verify Sorting (Descending)
        print("\n--- Sorting Verification (Descending) ---")
        if page1:
            scores = [t.get('final_score', 0) for t in page1]
            if all(scores[i] >= scores[i+1] for i in range(len(scores)-1)):
                print("Sorting SUCCESS: Final scores are in descending order.")
            else:
                print(f"Sorting FAILURE: Scores not descending: {scores}")

if __name__ == "__main__":
    asyncio.run(verify_alignment())
