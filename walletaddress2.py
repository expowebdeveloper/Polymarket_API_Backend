#!/usr/bin/env python3
"""
Polymarket Wallet PnL & ROI Calculator
Upload wallet addresses in .txt file and get PnL/ROI analysis

Requirements:
- requests library: pip install requests

Usage:
1. Set API key: export DOMEAPI_KEY="your_api_key" (or edit script)
2. Add wallet addresses to wallet_address.txt (one per line)
3. Run: python wallet_pnl_calculator.py
4. Get JSON report with PnL and ROI for each wallet
"""

import requests
import json
from datetime import datetime, timedelta
import time
import os

DOMEAPI_BASE_URL = "https://api.domeapi.io/v1/polymarket"
WALLET_INPUT_FILE = "wallet_address.txt"
OUTPUT_FILE = "wallet_pnl_report.json"
RAW_API_RESPONSES_FILE = "raw_api_responses.json"
REQUEST_DELAY = 0.5

DOMEAPI_KEY = '4d8e5410-e3bf-4abf-838b-0d3b0312bdd9'

RAW_RESPONSES = {
    'wallet_trades': [],
    'market_resolutions': []
}

def get_headers():
    """Get API headers with authentication"""
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }
    if DOMEAPI_KEY:
        headers['Authorization'] = f'Bearer {DOMEAPI_KEY}'
    return headers


def read_wallet_addresses(filename):
    """Read wallet addresses from txt file"""
    if not os.path.exists(filename):
        print(f"❌ File not found: {filename}")
        return []

    with open(filename, 'r') as f:
        addresses = [line.strip() for line in f if line.strip()]

    valid = []
    for addr in addresses:
        if addr.startswith('0x') and len(addr) == 42:
            valid.append(addr.lower())
        else:
            print(f"⚠️  Invalid address: {addr}")
    return valid


def fetch_wallet_trades(wallet_address):
    """Fetch ALL trades for a specific wallet directly"""
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
                headers=get_headers(),
                timeout=30
            )
            print(f"  API Response Status: {response.status_code}")
            if response.status_code != 200:
                print(f"  ❌ API Error: {response.status_code}")
                print(f"  Response: {response.text[:200]}")
                break

            data = response.json()
            RAW_RESPONSES['wallet_trades'].append({
                'wallet': wallet_address,
                'offset': offset,
                'raw_data': data
            })

            orders = data.get('orders', [])

            if not orders:
                print(f"  No more orders found")
                break

            all_trades.extend(orders)
            print(f"  📈 Fetched {len(orders)} trades (Total: {len(all_trades)})")

            if offset == 0 and orders:
                print(f"\n  📋 Sample API Response Structure:")
                print(f"  Keys in response: {list(data.keys())}")
                print(f"  Sample order fields: {list(orders[0].keys())}")
                print(f"  First order sample:")
                print(f"    - market_slug: {orders[0].get('market_slug', 'N/A')}")
                print(f"    - side: {orders[0].get('side', 'N/A')}")
                print(f"    - price: {orders[0].get('price', 'N/A')}")
                print(f"    - shares_normalized: {orders[0].get('shares_normalized', 'N/A')}")
                print(f"    - token_id: {orders[0].get('token_id', 'N/A')[:20]}...")

            pagination = data.get('pagination', {})
            if not pagination.get('has_more', False):
                print(f"  ✅ All trades fetched (no more pages)")
                break

            offset += limit

        except Exception as e:
            print(f"  ❌ Error: {e}")
            break

    print(f"  ✅ Total trades found: {len(all_trades)}")
    return all_trades


def fetch_market_resolution(market_slug):
    """Get market resolution data (winning side) from API - ALL markets (not just closed)"""
    try:
        time.sleep(REQUEST_DELAY)
        # COMMENTED OUT: Previously only fetched closed markets
        # response = requests.get(
        #     f"{DOMEAPI_BASE_URL}/markets",
        #     params={'slug': market_slug, 'status': 'closed', 'limit': 1},
        #     headers=get_headers(),
        #     timeout=30
        # )
        
        # NEW: Fetch market data without status filter (includes open and closed markets)
        response = requests.get(
            f"{DOMEAPI_BASE_URL}/markets",
            params={'slug': market_slug, 'limit': 1},
            headers=get_headers(),
            timeout=30
        )

        if response.status_code == 200:
            data = response.json()
            RAW_RESPONSES['market_resolutions'].append({
                'market_slug': market_slug,
                'raw_data': data
            })
            markets = data.get('markets', [])
            if markets:
                market = markets[0]
                winning_side = market.get('winning_side')
                # For closed markets, use winning_side
                # For open markets, we'll use current price later
                if winning_side:
                    return {
                        'winning_side': winning_side,
                        'market_data': market,
                        'is_closed': market.get('status') == 'closed'
                    }
                else:
                    # Market is open, return market data for current price calculation
                    return {
                        'winning_side': None,
                        'market_data': market,
                        'is_closed': False
                    }
        return None
    except Exception as e:
        print(f"    ❌ Error fetching resolution: {e}")
        return None


def filter_trades_by_last_month(trades):
    """Filter trades to only include those from the last month"""
    now = datetime.now()
    one_month_ago = now - timedelta(days=30)
    
    filtered = []
    skipped = 0
    for trade in trades:
        timestamp = trade.get('timestamp')
        if not timestamp:
            skipped += 1
            continue
            
        trade_date = None
        # Convert timestamp to datetime if it's a string or unix timestamp
        try:
            if isinstance(timestamp, str):
                # Try ISO format first
                try:
                    # Handle various ISO formats
                    timestamp_clean = timestamp.replace('Z', '+00:00')
                    if '+' not in timestamp_clean and timestamp_clean.count(':') == 2:
                        # No timezone info, assume UTC
                        timestamp_clean += '+00:00'
                    trade_date = datetime.fromisoformat(timestamp_clean)
                except:
                    # Try Unix timestamp as string
                    try:
                        trade_date = datetime.fromtimestamp(float(timestamp))
                    except:
                        pass
            elif isinstance(timestamp, (int, float)):
                # Unix timestamp (seconds or milliseconds)
                ts = float(timestamp)
                # If timestamp is very large, it might be in milliseconds
                if ts > 1e10:
                    ts = ts / 1000
                trade_date = datetime.fromtimestamp(ts)
            
            if trade_date:
                # Compare dates (handle timezone-aware datetimes)
                if trade_date.tzinfo:
                    trade_date_naive = trade_date.replace(tzinfo=None)
                else:
                    trade_date_naive = trade_date
                
                if trade_date_naive >= one_month_ago:
                    filtered.append(trade)
            else:
                skipped += 1
        except Exception as e:
            skipped += 1
            continue
    
    if skipped > 0:
        print(f"  ⚠️  Skipped {skipped} trades with invalid/missing timestamps")
    
    return filtered


def calculate_trade_pnl(trade, market_data):
    """Calculate PnL for a single trade based on API data"""
    cost = trade['shares_normalized'] * trade['price']
    side = trade.get('side', 'BUY').upper()
    
    # If market is closed, use winning_side
    if market_data.get('is_closed') and market_data.get('winning_side'):
        winning_side = market_data['winning_side']
        winning_token_id = str(winning_side.get('id', ''))
        trade_token_id = str(trade.get('token_id', ''))
        
        # Check if we hold the winning token
        if trade_token_id == winning_token_id:
            # We hold the winning token
            if side == 'BUY':
                # Bought winning token: payout - cost
                payout = trade['shares_normalized'] * 1.0
                pnl = payout - cost
            else:  # SELL
                # Sold winning token: cost - payout (loss)
                payout = trade['shares_normalized'] * 1.0
                pnl = cost - payout
        else:
            # We hold the losing token
            if side == 'BUY':
                # Bought losing token: lose the cost
                pnl = -cost
            else:  # SELL
                # Sold losing token: keep the cost (profit)
                pnl = cost
    else:
        # Market is open - use current price from market data
        market = market_data.get('market_data', {})
        outcomes = market.get('outcomes', [])
        
        # Find the current price for the token we hold
        current_price = trade['price']  # Default to entry price if we can't find current price
        trade_token_id = str(trade.get('token_id', ''))
        for outcome in outcomes:
            outcome_token_id = str(outcome.get('token_id', ''))
            if outcome_token_id == trade_token_id:
                current_price = outcome.get('price', trade['price'])
                break
        
        # Calculate unrealized PnL
        if side == 'BUY':
            # Bought: (current_price - entry_price) * shares
            pnl = (current_price - trade['price']) * trade['shares_normalized']
        else:  # SELL
            # Sold: (entry_price - current_price) * shares
            pnl = (trade['price'] - current_price) * trade['shares_normalized']
    
    return pnl


def calculate_wallet_pnl_roi(wallet_address):
    """Calculate complete PnL and ROI for a wallet using last month's trades from API"""
    print(f"\n{'='*80}")
    print(f"💰 Analyzing Wallet: {wallet_address}")
    print(f"{'='*80}")

    trades = fetch_wallet_trades(wallet_address)
    if not trades:
        print("❌ No trades found for this wallet")
        return None

    print(f"\n📅 Filtering trades from last month...")
    trades_last_month = filter_trades_by_last_month(trades)
    print(f"✅ Found {len(trades_last_month)} trades from last month (out of {len(trades)} total)")
    
    if not trades_last_month:
        print("❌ No trades found in the last month")
        return None

    market_slugs = list(set(trade['market_slug'] for trade in trades_last_month))
    print(f"\n📊 Markets traded (last month): {len(market_slugs)}")

    # COMMENTED OUT: Previously only fetched closed markets
    # print(f"\n🔍 Fetching market resolutions from API (closed only)...")
    print(f"\n🔍 Fetching market data from API (all markets - open and closed)...")
    market_resolutions = {}
    market_details = {}
    for idx, market_slug in enumerate(market_slugs, 1):
        if idx % 50 == 0:
            print(f"  Progress: Checked {idx}/{len(market_slugs)} markets")
        resolution_data = fetch_market_resolution(market_slug)
        if resolution_data:
            market_resolutions[market_slug] = resolution_data
            market_details[market_slug] = resolution_data['market_data']
            if idx <= 10:
                if resolution_data.get('is_closed') and resolution_data.get('winning_side'):
                    print(f"  [{idx}] {market_slug} ✅ Closed - Winner: {resolution_data['winning_side']['label']}")
                else:
                    print(f"  [{idx}] {market_slug} 📊 Open market")
        else:
            if idx <= 10:
                print(f"  [{idx}] {market_slug} ⚠️  No market data from API")

    print(f"\n🧮 Calculating PnL from API data (last month trades)...")
    total_trades = total_wins = total_losses = total_pnl = total_volume = 0
    trade_details = []
    for trade in trades_last_month:
        market_slug = trade['market_slug']
        market_data = market_resolutions.get(market_slug)
        if not market_data:
            continue
        volume = trade['shares_normalized'] * trade['price']
        pnl = calculate_trade_pnl(trade, market_data)
        total_volume += volume
        total_pnl += pnl
        is_win = pnl > 0
        if is_win:
            total_wins += 1
        else:
            total_losses += 1
        trade_details.append({
            'market_slug': trade['market_slug'],
            'side': trade['side'],
            'price': trade['price'],
            'shares': trade['shares_normalized'],
            'cost': volume,
            'pnl': round(pnl, 2),
            'result': 'WIN' if is_win else 'LOSS',
            'timestamp': trade.get('timestamp'),
            'token_id': trade['token_id'],
            'market_status': 'closed' if market_data.get('is_closed') else 'open'
        })

    total_trades = total_wins + total_losses
    if total_trades == 0:
        print("❌ No trades with market data found")
        return None

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
        'markets_with_data': len(market_resolutions),
        'analysis_date': datetime.now().isoformat(),
        'analysis_period': 'Last 30 days',
        'trade_details': trade_details[:20],
        'data_source': 'DomeAPI - Last month trades (all markets)'
    }
    print(f"\n{'='*80}")
    print(f"📊 RESULTS (From API Data - Last Month)")
    print(f"{'='*80}")
    print(f"Data Source: DomeAPI")
    print(f"Analysis Period: Last 30 days")
    print(f"Total Trades: {total_trades}")
    print(f"Wins: {total_wins} | Losses: {total_losses}")
    print(f"Win Rate: {result['win_rate_percent']}%")
    print(f"\n💰 Financial Performance:")
    print(f"Total Volume: ${result['total_volume']:,.2f}")
    print(f"Total PnL: ${result['total_pnl']:,.2f}")
    print(f"ROI: {result['roi_percent']}%")
    print(f"{'='*80}")
    return result

if __name__ == "__main__":
    print("\n" + "="*80)
    print("💰 POLYMARKET WALLET PnL & ROI CALCULATOR")
    print("="*80)

    if DOMEAPI_KEY:
        print(f"✅ API Key present: {DOMEAPI_KEY[:10]}...{DOMEAPI_KEY[-5:]}")
    else:
        print("⚠️  No API key set (may be rate-limited or fail)")
        print("   Set DOMEAPI_KEY as an environment variable or edit script.")

    try:
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
        results = []
        for idx, wallet in enumerate(wallets, 1):
            print(f"\n\n{'#'*80}\nWALLET {idx}/{len(wallets)}\n{'#'*80}")
            result = calculate_wallet_pnl_roi(wallet)
            if result:
                results.append(result)

        if results:
            output = {
                'generated_at': datetime.now().isoformat(),
                'total_wallets': len(results),
                'data_source': 'DomeAPI - Last month trades (all markets)',
                'analysis_period': 'Last 30 days',
                'api_authenticated': bool(DOMEAPI_KEY),
                'wallets': results
            }
            with open(OUTPUT_FILE, 'w') as f:
                json.dump(output, f, indent=2)
            with open(RAW_API_RESPONSES_FILE, 'w') as f:
                json.dump(RAW_RESPONSES, f, indent=2)
            print(f"\n\n{'='*80}")
            print("✅ ANALYSIS COMPLETE")
            print(f"{'='*80}")
            print(f"📊 Analyzed: {len(results)} wallet(s)")
            print(f"💾 Report saved: {OUTPUT_FILE}")
            print(f"📄 Raw API responses saved: {RAW_API_RESPONSES_FILE}")
            print(f"\n{'='*80}")
            print("📊 SUMMARY TABLE (All from API)")
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
            print(f"\n📋 All calculations use DomeAPI data for last month's trades (all markets - open and closed).")
        else:
            print("\n❌ No data collected")

    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()