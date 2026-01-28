import os
from dune_client.client import DuneClient
from dune_client.query import QueryBase
from dune_client.types import QueryParameter

# 1. SETUP: Initialize the client with your API Key
# Replace "YOUR_DUNE_API_KEY" with your actual key
dune = DuneClient("YOUR_DUNE_API_KEY")

def fetch_leaderboard_metrics_dune(user_addresses):
    """
    Fetches pre-calculated Worst Loss and Win Stats for multiple users
    using a single SQL query on Dune Analytics.
    """
    
    # Format the list of addresses for the SQL IN clause
    # Example: '0x123...', '0xabc...'
    formatted_addresses = ",".join([f"'{addr.lower()}'" for addr in user_addresses])
    
    # 2. THE SQL QUERY
    # This runs on Dune's servers. It scans millions of rows instantly.
    # Note: We use the 'polymarket.trades' table (or equivalent decoded event table).
    sql_query = f"""
    WITH user_stats AS (
        SELECT
            trader AS user_address,
            -- Calculate Worst Loss (Min PnL)
            MIN(amount_usd * (CASE WHEN type = 'Sell' THEN 1 ELSE -1 END)) as worst_loss_estimate,
            
            -- Calculate Win Counts (This logic depends on how you define a 'Win' in raw trades)
            -- For accuracy, linking to outcome resolution tables is best, but here is a proxy:
            COUNT(CASE WHEN type = 'Redemption' AND amount_usd > 0 THEN 1 END) as win_count,
            SUM(CASE WHEN type = 'Redemption' THEN amount_usd ELSE 0 END) as win_volume
        FROM polymarket.trades
        WHERE trader IN ({formatted_addresses})
        GROUP BY 1
    )
    SELECT * FROM user_stats
    """

    # 3. EXECUTE
    try:
        # We use a known Query ID or create a new execution. 
        # For ad-hoc SQL, we can submit the query string directly.
        results = dune.run_sql(
            query_sql=sql_query,
            performance="medium" # Use 'large' for huge lists
        )
        
        # 4. PARSE RESULTS
        metrics_map = {}
        for row in results.get_rows():
            addr = row['user_address']
            metrics_map[addr] = {
                "worst_loss": row['worst_loss_estimate'],
                "win_count": row['win_count'],
                "win_volume": row['win_volume']
            }
            
        return metrics_map

    except Exception as e:
        print(f"Dune API Error: {e}")
        return {}

# ==========================================
# TEST RUN
# ==========================================
users = [
    "0x6a72f61820b26b1fe4d956e17b6dc2a1ea3033ee", 
    "0x17db3fcd93ba12d38382a0cade24b200185c5f6d"
]

print("🚀 Asking Dune to calculate scores...")
data = fetch_leaderboard_metrics_dune(users)

for user in users:
    m = data.get(user.lower(), {})
    print(f"User: {user[:10]}...")
    print(f" - Worst Loss: ${m.get('worst_loss', 0)}")
    print(f" - Win Count:  {m.get('win_count', 0)}")
    print("-" * 30)