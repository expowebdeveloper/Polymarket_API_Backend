
import math
import sys
import os
import json
from typing import List, Dict

# Ensure app can be imported
sys.path.append(os.getcwd())
import requests

def fetch_orders_for_market(market_slug: str, limit: int = 100):
    """
    Fetches the orderbook for a given Polymarket market using the DomeAPI `/orders` endpoint.
    Returns a dictionary with buy/sell orders or None if error.
    """
    url = f"https://api.domeapi.io/v1/polymarket/orders"
    params = {
        "market_slug": market_slug,
        "limit": limit
    }
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()  # Expected to contain buy/sell orders
    except Exception as e:
        print(f"Error fetching orders for market '{market_slug}': {e}")
        return None

def fetch_trades_for_market(market_slug: str, limit: int = 100):
    """
    Fetch trades history for a given market using DomeAPI or Polymarket Data API.
    DomeAPI doesn't seem to have trades endpoint; fallback to Polymarket Data API.
    """
    url = f"https://data-api.polymarket.com/trades"
    params = {
        "market": market_slug,
        "limit": limit
    }
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json().get('trades', [])
    except Exception as e:
        print(f"Error fetching trades for market '{market_slug}': {e}")
        return []

def fetch_best_traders_for_market(market_slug: str, limit: int = 10):
    """
    Dummy or placeholder function to simulate fetching of best traders for a market.
    In reality this data may require on-chain analysis or an analytics API.
    For now, returns an empty list structure.
    """
    # TODO: Replace with real endpoint or calculation if available
    return []

def fetch_market_votes(market_slug: str):
    """
    Placeholder for vote-fetching functionality. 
    Could be from a DB, a dedicated votes API, or submitted data.
    Returns a dict: {"win_votes": n, "lose_votes": m}
    """
    # Placeholder: This might come from a local database or external service.
    # For now, return demo numbers.
    return {"win_votes": 0, "lose_votes": 0}

def get_market_details(market_slug: str):
    """
    Given a market slug, fetches orderbook, trades, top traders, and voting summary.
    Returns a unified dictionary.
    """
    orders = fetch_orders_for_market(market_slug)
    trades = fetch_trades_for_market(market_slug)
    top_traders = fetch_best_traders_for_market(market_slug)
    vote_summary = fetch_market_votes(market_slug)
    return {
        "orders": orders,
        "trades": trades,
        "top_traders": top_traders,      # List of dicts, each with rating and vote counts
        "vote_summary": vote_summary     # Dict: {win_votes, lose_votes}
    }

# Usage Example (would be called e.g. when frontend requests market details upon click)
# details = get_market_details("henry-cavill-announced-as-next-james-bond")
# print(json.dumps(details, indent=2))


from app.services.data_fetcher import fetch_closed_positions
from scoring_script import get_percentile_value, clamp

# --- Constants ---
B = 0.5
KW = 50

def calculate_metrics_for_wallet(wallet: str):
    print(f"Fetching data for {wallet}...")
    try:
        positions = fetch_closed_positions(wallet)
    except Exception as e:
        print(f"Error fetching for {wallet}: {e}")
        return None

    # We treat each closed position as a 'trade' for the purpose of this score
    # s_i = stake = totalBought (shares) * avgPrice (price)
    # This represents the Cost Basis / Invested Amount.
    
    s_win = 0.0
    s_total = 0.0
    sum_sq_s = 0.0
    count = 0
    
    current_trades = []

    for p in positions:
        # Extract fields
        # Note: API keys might vary, checking expected keys from inspection
        total_bought = float(p.get('totalBought', 0.0))
        avg_price = float(p.get('avgPrice', 0.0))
        realized_pnl = float(p.get('realizedPnl', 0.0))
        
        # Stake s_i
        # If total_bought is units of "asset", and avg_price is price per unit (0-1)
        stake = total_bought * avg_price
        
        if stake <= 0:
            continue
            
        count += 1
        s_total += stake
        sum_sq_s += (stake ** 2)
        
        # Winning Trade?
        # Definition: Realized Profit > 0
        if realized_pnl > 0:
            s_win += stake
            
    # Step 1: Raw Win Rate
    # W = Sw / S
    w_raw = (s_win / s_total) if s_total > 0 else 0.0
    
    # Step 2: N_eff
    # ((Sum s)^2) / Sum(s^2)
    # = (S_total^2) / Sum_sq_s
    n_eff = (s_total ** 2) / sum_sq_s if sum_sq_s > 0 else 0.0
    
    # Step 3: Shrink Win Rate
    # W_shrunk = (W * N_eff + B * Kw) / (N_eff + KW)
    # Step 3: Shrink Win Rate
    # W_shrunk = (W * N_eff + B * Kw) / (N_eff + KW)
    w_shrunk = (w_raw * n_eff + B * KW) / (n_eff + KW)
    
    # --- ROI Calculation ---
    total_pnl = sum([float(p.get('realizedPnl', 0.0)) for p in positions])
        
    roi_raw = (total_pnl / s_total * 100) if s_total > 0 else 0.0
    
    max_stake = 0.0
    worst_loss = 0.0 # Track worst loss
    
    for p in positions:
        total_bought = float(p.get('totalBought', 0.0))
        avg_price = float(p.get('avgPrice', 0.0))
        stake = total_bought * avg_price
        if stake > max_stake:
            max_stake = stake
            
        realized_pnl = float(p.get('realizedPnl', 0.0))
        if realized_pnl < worst_loss:
            worst_loss = realized_pnl
            
    # Formula 3 Step 1: Adjust PnL
    # PnL_adj = PnL_total / (1 + alpha * (max_si / S))
    ALPHA = 4.0
    ratio = (max_stake / s_total) if s_total > 0 else 0.0
    pnl_adj = total_pnl / (1 + ALPHA * ratio)
    
    # --- Risk Score Calculation (Formula 4) ---
    # loss% = |worst_loss| / capital
    # We use s_total as 'capital' (Total Investment in Closed Trades)
    if s_total > 0:
        loss_pct = abs(worst_loss) / s_total
    else:
        loss_pct = 0.0
        
    # Risk Score = 1 - loss%
    # If loss% > 1 (lost more than invested? possible with leverage or calculation quirks), clamp to 0
    risk_score = 1.0 - loss_pct
    risk_score = max(0.0, min(1.0, risk_score)) # Clamp 0-1
    
    return {
        "wallet": wallet,
        "total_trades": count,
        "s_win": s_win,
        "s_total": s_total,
        "sum_sq_s": sum_sq_s,
        "w_raw": w_raw,
        "n_eff": n_eff,
        "w_shrunk": w_shrunk,
        "roi_raw": roi_raw,
        "total_pnl": total_pnl,
        "pnl_adj": pnl_adj,
        "risk_score": risk_score,
        "worst_loss": worst_loss
    }

def main():
    # 1. Read Wallets
    try:
        with open("wallet_address.txt", "r") as f:
            wallets = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print("wallet_address.txt not found.")
        return

    results = []
    
    # 2. Process each wallet
    for wallet in wallets:
        metrics = calculate_metrics_for_wallet(wallet)
        if metrics:
            results.append(metrics)
            
    # 3. Population Stats for Normalization
    population = [r for r in results if r['total_trades'] >= 5]
    
    if not population:
        print("Not enough traders with >= 5 trades. Using defaults.")
        w_1, w_99 = 0.0, 1.0
        roi_1, roi_99 = -100.0, 100.0
        pnl_1, pnl_99 = -100.0, 100.0
        roi_m = 0.0
        pnl_m = 0.0
    else:
        # Win Rate Anchors
        w_shrunk_values = [r['w_shrunk'] for r in population]
        w_1 = get_percentile_value(w_shrunk_values, 1)
        w_99 = get_percentile_value(w_shrunk_values, 99)
        
        # ROI Stats
        rois_pop = [r['roi_raw'] for r in population]
        rois_pop.sort()
        roi_m = rois_pop[len(rois_pop) // 2]
        
        # PnL Stats - Median of Adjusted PnL
        pnls_adj_pop = [r['pnl_adj'] for r in population]
        pnls_adj_pop.sort()
        pnl_m = pnls_adj_pop[len(pnls_adj_pop) // 2]
        
        # Calculate Shrunk Values for population
        KR = 50
        KP = 50
        
        for r in results:
            n_eff = r['n_eff']
            
            # ROI Shrink
            r['roi_shrunk'] = (r['roi_raw'] * n_eff + roi_m * KR) / (n_eff + KR)
            
            # PnL Shrink
            r['pnl_shrunk'] = (r['pnl_adj'] * n_eff + pnl_m * KP) / (n_eff + KP)
            
        # Anchors (from population)
        roi_shrunk_pop = [r['roi_shrunk'] for r in results if r['total_trades'] >= 5]
        roi_1 = get_percentile_value(roi_shrunk_pop, 1)
        roi_99 = get_percentile_value(roi_shrunk_pop, 99)
        
        pnl_shrunk_pop = [r['pnl_shrunk'] for r in results if r['total_trades'] >= 5]
        pnl_1 = get_percentile_value(pnl_shrunk_pop, 1)
        pnl_99 = get_percentile_value(pnl_shrunk_pop, 99)
        
    print(f"\nNormalization Anchors (from {len(population)} traders >= 5 trades):")
    print(f"Win Rate: 1%={w_1:.4f}, 99%={w_99:.4f}")
    print(f"ROI: Median={roi_m:.4f}, 1%={roi_1:.4f}, 99%={roi_99:.4f}")
    print(f"PnL: Median={pnl_m:.4f}, 1%={pnl_1:.4f}, 99%={pnl_99:.4f}")
    
    # 4. Final Scores
    leaderboard = []
    for r in results:
        # W Score
        if w_99 - w_1 != 0:
            w_score = (r['w_shrunk'] - w_1) / (w_99 - w_1)
        else:
            w_score = 0.5
        r['w_score'] = clamp(w_score, 0, 1)
        
        # ROI Score
        if roi_99 - roi_1 != 0:
            roi_score = (r['roi_shrunk'] - roi_1) / (roi_99 - roi_1)
        else:
            roi_score = 0.5
        r['roi_score'] = clamp(roi_score, 0, 1)
        
        # PnL Score
        if pnl_99 - pnl_1 != 0:
            pnl_score = (r['pnl_shrunk'] - pnl_1) / (pnl_99 - pnl_1)
        else:
            pnl_score = 0.5
        r['pnl_score'] = clamp(pnl_score, 0, 1)
        
        leaderboard.append(r)
        
    # 5. Output
    # Sort by Risk Score for this request
    leaderboard.sort(key=lambda x: x.get('risk_score', 0), reverse=True)
    
    print("\n" + "="*156)
    print(f"{'Wallet':<42} | {'Trades':<6} | {'W_scr':<6} | {'ROI_scr':<7} | {'PnL_scr':<7} | {'Risk_scr':<8} | {'Worst_Loss':<11} | {'PnL_raw':<10}")
    print("="*156)
    for row in leaderboard:
        # Avoid key error if Risk not calculated for some reason
        risk = row.get('risk_score', 0.0)
        wl = row.get('worst_loss', 0.0)
        print(f"{row['wallet']:<42} | {row['total_trades']:<6} | {row['w_score']:.4f} | {row['roi_score']:.4f}  | {row['pnl_score']:.4f}  | {risk:.4f}   | {wl:<11.2f} | {row['total_pnl']:<10.2f}")
    print("="*156)

if __name__ == "__main__":
    main()
