
original_text = """    # --- Calculate Detailed Market Distribution with ROI and Win Rate ---
    market_distribution, primary_edge = calculate_market_distribution(active_positions, closed_positions)"""

new_text = """    # --- Calculate Detailed Market Distribution with ROI and Win Rate (API BASED) ---
    target_categories = ["politics", "sports", "crypto", "finance", "culture", "mentions", "weather", "economics", "tech"]
    api_stats_map = await fetch_category_stats(wallet_address, target_categories)
    
    market_distribution = []
    total_capital_api = sum(s["volume"] for s in api_stats_map.values())
    
    for cat, stats in api_stats_map.items():
        vol = stats["volume"]
        pnl = stats["pnl"]
        
        # Approximate metrics since API doesn't provide them
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
    
    primary_edge = "Market data sourced from Leaderboard API"
    if market_distribution:
        top = market_distribution[0]
        if top['roi_percent'] > 0:
            primary_edge = f"Primary edge in {top['category']} markets with {top['roi_percent']}% ROI"
        else:
            primary_edge = f"Highest volume in {top['category']} markets"
"""

path = "/home/dell/Desktop/Projects/Polymarket/backend/app/services/dashboard_service.py"
with open(path, "r") as f:
    content = f.read()

if original_text in content:
    new_content = content.replace(original_text, new_text)
    with open(path, "w") as f:
        f.write(new_content)
    print("Successfully patched file.")
else:
    print("Original text not found in file.")
