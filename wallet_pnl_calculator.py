#!/usr/bin/env python3
"""
Polymarket Wallet PnL & ROI Calculator
Upload wallet addresses in .txt file and get PnL/ROI analysis

Requirements:
- requests library: pip install requests

Usage:
1. Add wallet addresses to wallet_addresses.txt (one per line)
2. Run: python wallet_pnl_calculator.py
3. Get JSON report with PnL and ROI for each wallet
"""

import requests
import json
from datetime import datetime
import time
import os

# ============================================================================
# CONFIGURATION
# ============================================================================
DOMEAPI_BASE_URL = "https://api.domeapi.io/v1/polymarket"
WALLET_INPUT_FILE = "wallet_addresses.txt"
OUTPUT_FILE = "wallet_pnl_report.json"
REQUEST_DELAY = 0.5  # Respect rate limits

# ============================================================================
# CORE FUNCTIONS
# ============================================================================

def read_wallet_addresses(filename):
    """Read wallet addresses from txt file"""
    if not os.path.exists(filename):
        print(f"❌ File not found: {filename}")
        return []
    
    with open(filename, 'r') as f:
        addresses = [line.strip() for line in f if line.strip()]
    
    # Validate addresses
    valid = []
    for addr in addresses:
        if addr.startswith('0x') and len(addr) == 42:
            valid.append(addr.lower())
        else:
            print(f"⚠️  Invalid address: {addr}")
    
    return valid


def fetch_wallet_trades(wallet_address):
    """
    Fetch ALL trades for a specific wallet directly
    This is MUCH faster than scanning all markets
    """
    print(f"\n📊 Fetching trades for: {wallet_address[:10]}...{wallet_address[-8:]}")
    
    all_trades = []
    offset = 0
    limit = 1000
    
    while True:
        try:
            time.sleep(REQUEST_DELAY)
            
            response = requests.get(
                f"{DOMEAPI_BASE_URL}/orders",
                params={
                    'user': wallet_address,
                    'limit': limit,
                    'offset': offset
                },
                timeout=30
            )
            
            if response.status_code != 200:
                print(f"  ❌ API Error: {response.status_code}")
                break
            
            data = response.json()
            orders = data.get('orders', [])
            
            if not orders:
                break
            
            all_trades.extend(orders)
            print(f"  📈 Fetched {len(orders)} trades (Total: {len(all_trades)})")
            
            # Check pagination
            pagination = data.get('pagination', {})
            if not pagination.get('has_more', False):
                break
            
            offset += limit
            
        except Exception as e:
            print(f"  ❌ Error: {e}")
            break
    
    print(f"  ✅ Total trades found: {len(all_trades)}")
    return all_trades


def fetch_market_resolution(market_slug):
    """Get market resolution data (winning side)"""
    try:
        time.sleep(REQUEST_DELAY)
        
        response = requests.get(
            f"{DOMEAPI_BASE_URL}/markets",
            params={'slug': market_slug, 'limit': 1},
            timeout=30
        )
        
        if response.status_code == 200:
            data = response.json()
            markets = data.get('markets', [])
            if markets and markets[0].get('winning_side'):
                return markets[0]['winning_side']
        
        return None
    except Exception as e:
        return None


def calculate_trade_pnl(trade, winning_token_id):
    """Calculate PnL for a single trade"""
    cost = trade['shares_normalized'] * trade['price']
    
    if trade['token_id'] == winning_token_id:
        # WIN: Get $1 per share
        payout = trade['shares_normalized'] * 1.0
        pnl = payout - cost
    else:
        # LOSS: Lose entire bet
        pnl = -cost
    
    return pnl


def calculate_wallet_pnl_roi(wallet_address):
    """
    Calculate complete PnL and ROI for a wallet
    
    Returns dict with:
    - wallet_address
    - total_trades
    - total_wins, total_losses
    - win_rate_percent
    - total_pnl (Profit/Loss)
    - total_volume
    - roi_percent (ROI)
    """
    print(f"\n{'='*80}")
    print(f"💰 Analyzing Wallet: {wallet_address}")
    print(f"{'='*80}")
    
    # Step 1: Fetch all trades for this wallet
    trades = fetch_wallet_trades(wallet_address)
    
    if not trades:
        print("❌ No trades found for this wallet")
        return None
    
    # Step 2: Get unique market slugs
    market_slugs = list(set(trade['market_slug'] for trade in trades))
    print(f"\n📊 Markets traded: {len(market_slugs)}")
    
    # Step 3: Fetch market resolutions
    print(f"\n🔍 Fetching market resolutions...")
    market_resolutions = {}
    
    for idx, market_slug in enumerate(market_slugs, 1):
        print(f"  [{idx}/{len(market_slugs)}] {market_slug[:50]}...")
        winning_side = fetch_market_resolution(market_slug)
        if winning_side:
            market_resolutions[market_slug] = winning_side['id']
            print(f"    ✅ Winner: {winning_side['label']}")
        else:
            print(f"    ⚠️  No resolution data")
    
    # Step 4: Calculate PnL for each trade
    print(f"\n🧮 Calculating PnL...")
    
    total_trades = 0
    total_wins = 0
    total_losses = 0
    total_pnl = 0
    total_volume = 0
    resolved_trades = 0
    
    for trade in trades:
        market_slug = trade['market_slug']
        winning_token_id = market_resolutions.get(market_slug)
        
        # Skip unresolved markets
        if not winning_token_id:
            continue
        
        resolved_trades += 1
        volume = trade['shares_normalized'] * trade['price']
        pnl = calculate_trade_pnl(trade, winning_token_id)
        
        total_volume += volume
        total_pnl += pnl
        
        if pnl > 0:
            total_wins += 1
        else:
            total_losses += 1
    
    total_trades = total_wins + total_losses
    
    if total_trades == 0:
        print("❌ No resolved trades found")
        return None
    
    # Step 5: Calculate metrics
    win_rate = (total_wins / total_trades * 100) if total_trades > 0 else 0
    roi = (total_pnl / total_volume * 100) if total_volume > 0 else 0
    
    result = {
        'wallet_address': wallet_address,
        'total_trades': total_trades,
        'total_wins': total_wins,
        'total_losses': total_losses,
        'win_rate_percent': round(win_rate, 2),
        'total_pnl': round(total_pnl, 2),
        'total_volume': round(total_volume, 2),
        'roi_percent': round(roi, 2),
        'markets_traded': len(market_slugs),
        'resolved_markets': len(market_resolutions),
        'analysis_date': datetime.now().isoformat()
    }
    
    # Print summary
    print(f"\n{'='*80}")
    print(f"📊 RESULTS")
    print(f"{'='*80}")
    print(f"Total Trades (Resolved): {total_trades}")
    print(f"Wins: {total_wins} | Losses: {total_losses}")
    print(f"Win Rate: {result['win_rate_percent']}%")
    print(f"\n💰 Financial Performance:")
    print(f"Total Volume: ${result['total_volume']:,.2f}")
    print(f"Total PnL: ${result['total_pnl']:,.2f}")
    print(f"ROI: {result['roi_percent']}%")
    print(f"{'='*80}")
    
    return result


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    print("\n" + "="*80)
    print("💰 POLYMARKET WALLET PnL & ROI CALCULATOR")
    print("="*80)
    
    try:
        # Read wallet addresses
        print(f"\n📂 Reading: {WALLET_INPUT_FILE}")
        wallets = read_wallet_addresses(WALLET_INPUT_FILE)
        
        if not wallets:
            print("\n❌ No valid wallet addresses found")
            print(f"\n📝 Create {WALLET_INPUT_FILE} with addresses:")
            print("Example:")
            print("0x751a2b86cab503496efd325c8344e10159349ea1")
            print("0x644f99cdbbfe768a137207683f961fe0590da946")
            exit(1)
        
        print(f"✅ Found {len(wallets)} wallet(s)")
        
        # Analyze each wallet
        results = []
        
        for idx, wallet in enumerate(wallets, 1):
            print(f"\n\n{'#'*80}")
            print(f"WALLET {idx}/{len(wallets)}")
            print(f"{'#'*80}")
            
            result = calculate_wallet_pnl_roi(wallet)
            
            if result:
                results.append(result)
        
        # Save results
        if results:
            output = {
                'generated_at': datetime.now().isoformat(),
                'total_wallets': len(results),
                'wallets': results
            }
            
            with open(OUTPUT_FILE, 'w') as f:
                json.dump(output, f, indent=2)
            
            print(f"\n\n{'='*80}")
            print("✅ ANALYSIS COMPLETE")
            print(f"{'='*80}")
            print(f"📊 Analyzed: {len(results)} wallet(s)")
            print(f"💾 Report saved: {OUTPUT_FILE}")
            
            # Print summary table
            print(f"\n{'='*80}")
            print("📊 SUMMARY TABLE")
            print(f"{'='*80}")
            print(f"{'Wallet':<45} {'Trades':<10} {'Win%':<10} {'PnL $':<15} {'ROI%':<10}")
            print("-" * 80)
            
            for r in results:
                wallet_short = r['wallet_address'][:10] + "..." + r['wallet_address'][-8:]
                print(
                    f"{wallet_short:<45} "
                    f"{r['total_trades']:<10} "
                    f"{r['win_rate_percent']:<10.2f} "
                    f"${r['total_pnl']:<14,.2f} "
                    f"{r['roi_percent']:<10.2f}"
                )
            
            print("=" * 80)
        else:
            print("\n❌ No data collected")
            
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
