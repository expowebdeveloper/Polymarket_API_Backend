
import re

path = "/home/dell/Desktop/Projects/Polymarket/backend/app/services/dashboard_service.py"
with open(path, "r") as f:
    content = f.read()

# Define the block to remove (the API fetch logic)
# We look for the start and a chunk of it
start_marker = '# --- Calculate Detailed Market Distribution with ROI and Win Rate (API BASED) ---'
end_marker = 'primary_edge = f"Highest volume in {top[\'category\']} markets"'

# We'll regex match roughly from start_marker to the end of the if/else block
# Because indentation and exact lines might vary, I'll identify the range using string find
start_idx = content.find(start_marker)

if start_idx != -1:
    # Find the end of the logic block. It ends with the primary_edge assignment block.
    # We can search for the next section header or end of function
    # The next section in the file (from previous view) was "# --- Calculate Profit Trend (Last 7 Days) ---"
    next_section = '# --- Calculate Profit Trend (Last 7 Days) ---'
    end_idx = content.find(next_section, start_idx)
    
    if end_idx != -1:
        # Extract the code block to move it to a new function
        code_block = content[start_idx:end_idx]
        
        # Create replacement (empty list)
        replacement = """    # --- Calculate Detailed Market Distribution ---
    # Moved to separate endpoint /dashboard/market-distribution
    market_distribution = []
    primary_edge = "See detailed distribution tab"
    
    """
        
        # Perform replacement
        new_content = content[:start_idx] + replacement + content[end_idx:]
        
        # Now append the new function at the end of the file
        new_function = """

async def get_market_distribution_api(wallet_address: str) -> Dict[str, Any]:
    \"\"\"
    Fetch market distribution stats directly from Polymarket Leaderboard API (Parallel).
    \"\"\"
    target_categories = ["politics", "sports", "crypto", "finance", "culture", "mentions", "weather", "economics", "tech"]
    api_stats_map = await fetch_category_stats(wallet_address, target_categories)
    
    market_distribution = []
    total_capital_api = sum(s["volume"] for s in api_stats_map.values())
    
    for cat, stats in api_stats_map.items():
        vol = stats["volume"]
        pnl = stats["pnl"]
        
        # Approximate metrics
        roi = (pnl / vol * 100) if vol > 0 else 0.0
        capital_percent = (vol / total_capital_api * 100) if total_capital_api > 0 else 0.0
        
        market_distribution.append({
            "category": cat.title(),
            "market": cat.title(),
            "capital": round(vol, 2),
            "capital_percent": round(capital_percent, 2),
            "roi_percent": round(roi, 2),
            "win_rate_percent": 0.0,
            "trades_count": 0,
            "wins": 0,
            "losses": 0,
            "total_pnl": round(pnl, 2),
            "risk_score": 0.0,
            "unique_markets": 0
        })
        
    market_distribution.sort(key=lambda x: x["capital"], reverse=True)
    
    return {"market_distribution": market_distribution}
"""
        new_content += new_function
        
        with open(path, "w") as f:
            f.write(new_content)
        print("Successfully extracted logic to get_market_distribution_api and cleaned get_db_dashboard_data.")
    else:
        print("Could not find end of logic block.")
else:
    print("Could not find start of logic block API BASED.")
