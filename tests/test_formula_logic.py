import os
import sys

# Add project root to sys.path
sys.path.append(os.getcwd())

from app.services.leaderboard_service import calculate_scores_and_rank_with_percentiles

def test_formula_unification():
    print("🧪 Testing Formula Unification (Logic Consistency)...")
    
    # Mock some raw metrics (same for both)
    # Including some with trades and one with 0 trades
    mock_raw_data = [
        {
            "wallet_address": "0x1",
            "total_pnl": 1000.0,
            "roi": 10.0,
            "win_rate": 60.0,
            "total_trades": 10,
            "total_stakes": 10000.0,
            "winning_stakes": 6000.0,
            "sum_sq_stakes": 1000000.0,
            "max_stake": 2000.0,
            "worst_loss": -500.0,
            "all_losses": [-500.0, -200.0],
            "portfolio_value": 5000.0
        },
        {
            "wallet_address": "0x2",
            "total_pnl": -500.0,
            "roi": -5.0,
            "win_rate": 40.0,
            "total_trades": 10,
            "total_stakes": 10000.0,
            "winning_stakes": 4000.0,
            "sum_sq_stakes": 1000000.0,
            "max_stake": 1000.0,
            "worst_loss": -1000.0,
            "all_losses": [-1000.0],
            "portfolio_value": 2000.0
        },
        {
            "wallet_address": "0x3", # 0 trades trader
            "total_pnl": 0.0,
            "roi": 0.0,
            "win_rate": 0.0,
            "total_trades": 0,
            "total_stakes": 0.0,
            "winning_stakes": 0.0,
            "sum_sq_stakes": 0.0,
            "max_stake": 0.0,
            "worst_loss": 0.0,
            "all_losses": [],
            "portfolio_value": 0.0
        }
    ]

    # Path 1: Process as "API Data"
    api_result = calculate_scores_and_rank_with_percentiles(mock_raw_data)
    
    # Path 2: Process as "DB Data" (same function)
    db_result = calculate_scores_and_rank_with_percentiles(mock_raw_data)

    # 1. Compare percentiles
    for k in api_result["percentiles"]:
        if api_result["percentiles"][k] != db_result["percentiles"][k]:
            print(f"❌ Error: Percentile mismatch for {k}")
            return False
            
    # 2. Compare medians
    for k in api_result["medians"]:
        if api_result["medians"][k] != db_result["medians"][k]:
            print(f"❌ Error: Median mismatch for {k}")
            return False

    # 3. Compare traders
    for i in range(len(api_result["traders"])):
        a = api_result["traders"][i]
        d = db_result["traders"][i]
        
        if a["wallet_address"] != d["wallet_address"]:
            print(f"❌ Error: Wallet mismatch at index {i}")
            return False
            
        if a["final_score"] != d["final_score"]:
            print(f"❌ Error: Score mismatch for {a['wallet_address']}: {a['final_score']} vs {d['final_score']}")
            return False
            
        print(f"✅ Metric check for {a['wallet_address']}: Final Score = {a['final_score']:.2f}")

    print("\n🏁 SUCCESS: The scoring formula logic is IDENTICAL across both paths.")
    return True

if __name__ == "__main__":
    if test_formula_unification():
        sys.exit(0)
    else:
        sys.exit(1)
