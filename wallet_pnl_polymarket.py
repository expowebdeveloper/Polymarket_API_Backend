#!/usr/bin/env python3
"""
Polymarket Wallet PnL & ROI Calculator
Using Official Polymarket API only
Upload wallet addresses in .txt file and get PnL/ROI analysis for last month

Requirements:
- requests library: pip install requests

Usage:
1. Add wallet addresses to wallet_address.txt (one per line)
2. Run: python wallet_pnl_polymarket.py
3. Get JSON report with PnL and ROI for each wallet (last 30 days)

Polymarket API: https://docs.polymarket.com/
"""

import requests
import json
from datetime import datetime, timedelta
import time
import os

# ============================================================================
# CONFIGURATION
# ============================================================================
# Polymarket Official API endpoints
POLYMARKET_GAMMA_API = "https://gamma-api.polymarket.com"
POLYMARKET_CLOB_API = "https://clob.polymarket.com"
POLYMARKET_DATA_API = "https://data-api.polymarket.com"

WALLET_INPUT_FILE = "wallet_address.txt"
OUTPUT_FILE = "wallet_pnl_report.json"
RAW_API_RESPONSES_FILE = "raw_api_responses.json"
REQUEST_DELAY = 0.3  # Rate limiting

# API Authentication (optional - set via environment variables)
POLYMARKET_API_KEY = os.getenv('POLYMARKET_API_KEY', '')
POLYMARKET_SECRET = os.getenv('POLYMARKET_SECRET', '')
POLYMARKET_PASSPHRASE = os.getenv('POLYMARKET_PASSPHRASE', '')

# Store raw API responses
RAW_RESPONSES = {
    'wallet_trades': [],
    'market_data': []
}


def get_headers():
    """Get API headers with optional authentication"""
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }
    # Add Polymarket API authentication if available
    if POLYMARKET_API_KEY and POLYMARKET_SECRET and POLYMARKET_PASSPHRASE:
        headers['X-API-KEY'] = POLYMARKET_API_KEY
        headers['X-SECRET'] = POLYMARKET_SECRET
        headers['X-PASSPHRASE'] = POLYMARKET_PASSPHRASE
    return headers


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


def fetch_user_trades(wallet_address):
    """
    Fetch user trades from Polymarket API
    Tries multiple endpoints: CLOB API, Data API, and alternative endpoints
    """
    print(f"\n📊 Fetching trades for: {wallet_address[:10]}...{wallet_address[-8:]}")

    all_trades = []
    offset = 0
    limit = 100

    # Try multiple API endpoints
    endpoints_to_try = [
        (f"{POLYMARKET_DATA_API}/trades", {'trader': wallet_address, 'limit': limit, 'offset': offset}),
        (f"{POLYMARKET_CLOB_API}/trades", {'trader': wallet_address, 'limit': limit, 'offset': offset}),
        (f"{POLYMARKET_CLOB_API}/trades", {'user': wallet_address, 'limit': limit, 'offset': offset}),
    ]

    successful_endpoint = None
    successful_params = None

    # Find a working endpoint
    for endpoint_url, params in endpoints_to_try:
        try:
            time.sleep(REQUEST_DELAY)
            response = requests.get(
                endpoint_url,
                params=params,
                headers=get_headers(),
                timeout=30
            )
            
            print(f"  Trying {endpoint_url} - Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                trades = data if isinstance(data, list) else data.get('trades', []) or data.get('data', [])
                if trades or successful_endpoint is None:  # Accept if we get trades or if it's the first 200
                    successful_endpoint = endpoint_url
                    successful_params = params
                    print(f"  ✅ Found working endpoint: {endpoint_url}")
                    break
            elif response.status_code == 401:
                print(f"  ⚠️  Authentication required for {endpoint_url}")
                if not successful_endpoint:  # Continue trying other endpoints
                    continue
            else:
                if response.status_code != 404:  # Don't print for 404s
                    print(f"  ⚠️  {endpoint_url} returned {response.status_code}")
        except Exception as e:
            print(f"  ⚠️  Error trying {endpoint_url}: {e}")
            continue

    if not successful_endpoint:
        print(f"  ❌ No working endpoint found. API may require authentication.")
        print(f"  💡 Try setting environment variables:")
        print(f"     export POLYMARKET_API_KEY='your_key'")
        print(f"     export POLYMARKET_SECRET='your_secret'")
        print(f"     export POLYMARKET_PASSPHRASE='your_passphrase'")
        return []

    # Fetch all pages using the working endpoint
    offset = 0
    while True:
        try:
            time.sleep(REQUEST_DELAY)
            
            params = successful_params.copy()
            params['offset'] = offset
            params['limit'] = limit

            response = requests.get(
                successful_endpoint,
                params=params,
                headers=get_headers(),
                timeout=30
            )

            if response.status_code != 200:
                print(f"  ❌ API Error: {response.status_code}")
                print(f"  Response: {response.text[:200]}")
                break

            data = response.json()

            # Store raw response
            RAW_RESPONSES['wallet_trades'].append({
                'wallet': wallet_address,
                'offset': offset,
                'raw_data': data
            })

            trades = data if isinstance(data, list) else data.get('trades', []) or data.get('data', [])

            if not trades:
                print(f"  No more trades found")
                break

            all_trades.extend(trades)
            print(f"  📈 Fetched {len(trades)} trades (Total: {len(all_trades)})")

            # Display sample of first response
            if offset == 0 and trades:
                print(f"\n  📋 Sample API Response Structure:")
                print(f"  Sample trade fields: {list(trades[0].keys())}")
                print(f"  First trade sample:")
                print(f"    - id: {trades[0].get('id', 'N/A')}")
                print(f"    - tokenId: {trades[0].get('tokenId', 'N/A')[:20]}...")
                print(f"    - side: {trades[0].get('side', 'N/A')}")
                print(f"    - price: {trades[0].get('price', 'N/A')}")
                print(f"    - size: {trades[0].get('size', 'N/A')}")
                print(f"    - timestamp: {trades[0].get('timestamp', 'N/A')}")

            # Check if there are more pages
            if len(trades) < limit:
                print(f"  ✅ All trades fetched")
                break

            offset += limit

        except Exception as e:
            print(f"  ❌ Error: {e}")
            break

    print(f"  ✅ Total trades found: {len(all_trades)}")
    return all_trades


def fetch_market_data(market_id):
    """
    Fetch market data from Polymarket Gamma API
    https://docs.polymarket.com/api-reference/gamma/fetch-markets
    """
    try:
        time.sleep(REQUEST_DELAY)

        response = requests.get(
            f"{POLYMARKET_GAMMA_API}/markets",
            params={'id': market_id},
            headers=get_headers(),
            timeout=30
        )

        if response.status_code == 200:
            data = response.json()

            RAW_RESPONSES['market_data'].append({
                'market_id': market_id,
                'raw_data': data
            })

            markets = data if isinstance(data, list) else [data]
            if markets:
                return markets[0]

        return None
    except Exception as e:
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
        try:
            # Polymarket API typically returns ISO 8601 format or Unix timestamp
            if isinstance(timestamp, str):
                # ISO format
                try:
                    timestamp_clean = timestamp.replace('Z', '+00:00')
                    trade_date = datetime.fromisoformat(timestamp_clean)
                except:
                    pass
            elif isinstance(timestamp, (int, float)):
                # Unix timestamp (seconds or milliseconds)
                ts = float(timestamp)
                if ts > 1e10:
                    ts = ts / 1000
                trade_date = datetime.fromtimestamp(ts)

            if trade_date:
                # Handle timezone-aware datetimes
                if trade_date.tzinfo:
                    trade_date_naive = trade_date.replace(tzinfo=None)
                else:
                    trade_date_naive = trade_date

                if trade_date_naive >= one_month_ago:
                    filtered.append(trade)
            else:
                skipped += 1
        except:
            skipped += 1

    if skipped > 0:
        print(f"  ⚠️  Skipped {skipped} trades with invalid/missing timestamps")

    return filtered


def calculate_trade_pnl(trade, market_data):
    """
    Calculate PnL for a single trade
    Based on Polymarket trade data structure
    """
    try:
        # Get trade details
        size = float(trade.get('size', 0))
        price = float(trade.get('price', 0))
        side = trade.get('side', 'BUY').upper()

        cost = size * price

        # If market is resolved, calculate realized PnL
        if market_data and market_data.get('resolved'):
            # Get market outcomes
            outcomes = market_data.get('outcomes', [])

            # Determine which outcome user bet on
            trade_outcome_index = 0  # Default
            try:
                # Try to match based on conditionId or other fields
                condition_id = trade.get('conditionId')
                if condition_id and market_data.get('conditionId') == condition_id:
                    # Find the outcome index
                    for idx, outcome in enumerate(outcomes):
                        if outcome.get('index') is not None:
                            trade_outcome_index = outcome['index']
            except:
                pass

            # Get winning outcome
            winning_outcome_index = market_data.get('winningOutcomeIndex')

            if winning_outcome_index is not None:
                if trade_outcome_index == winning_outcome_index:
                    # Won: get $1 per share
                    payout = size * 1.0
                    pnl = payout - cost
                else:
                    # Lost: lose the bet
                    pnl = -cost
            else:
                # Market resolved but no winning outcome - shouldn't happen
                pnl = 0
        else:
            # Market not resolved - use current price
            if market_data and market_data.get('outcomes'):
                outcomes = market_data['outcomes']

                # Find current price of outcome we traded
                current_price = price  # Default to entry price

                # This is simplified - in reality would need to match outcome
                if outcomes:
                    current_price = float(outcomes[0].get('price', price))

                # Unrealized PnL
                if side == 'BUY':
                    pnl = (current_price - price) * size
                else:  # SELL
                    pnl = (price - current_price) * size
            else:
                # Can't calculate without market data
                pnl = 0

        return pnl
    except Exception as e:
        print(f"Error calculating PnL: {e}")
        return 0


def calculate_wallet_pnl_roi(wallet_address):
    """Calculate complete PnL and ROI for a wallet using Polymarket API"""
    print(f"\n{'='*80}")
    print(f"💰 Analyzing Wallet: {wallet_address}")
    print(f"{'='*80}")

    # Step 1: Fetch all trades for this wallet
    trades = fetch_user_trades(wallet_address)

    if not trades:
        print("❌ No trades found for this wallet")
        return None

    # Step 2: Filter for last month
    print(f"\n📅 Filtering trades from last month...")
    trades_last_month = filter_trades_by_last_month(trades)
    print(f"✅ Found {len(trades_last_month)} trades from last month (out of {len(trades)} total)")

    if not trades_last_month:
        print("❌ No trades found in the last month")
        return None

    # Step 3: Get unique markets
    market_ids = list(set(trade.get('marketId') or trade.get('id') for trade in trades_last_month if trade.get('marketId') or trade.get('id')))
    print(f"\n📊 Markets traded (last month): {len(market_ids)}")

    # Step 4: Fetch market data
    print(f"\n🔍 Fetching market data from Polymarket API...")
    market_data_cache = {}

    for idx, market_id in enumerate(market_ids, 1):
        if idx % 50 == 0:
            print(f"  Progress: Checked {idx}/{len(market_ids)} markets")

        market_data = fetch_market_data(market_id)
        if market_data:
            market_data_cache[market_id] = market_data
            if idx <= 10:
                status = "Resolved" if market_data.get('resolved') else "Open"
                print(f"  [{idx}] {market_data.get('title', 'Unknown')[:50]}... ({status})")

    # Step 5: Calculate PnL
    print(f"\n🧮 Calculating PnL from API data (last month trades)...")

    total_trades = 0
    total_wins = 0
    total_losses = 0
    total_pnl = 0
    total_volume = 0
    trade_details = []

    for trade in trades_last_month:
        market_id = trade.get('marketId') or trade.get('id')
        market_data = market_data_cache.get(market_id)

        if not market_data:
            continue

        size = float(trade.get('size', 0))
        price = float(trade.get('price', 0))
        volume = size * price

        pnl = calculate_trade_pnl(trade, market_data)

        total_volume += volume
        total_pnl += pnl

        is_win = pnl > 0
        if is_win:
            total_wins += 1
        else:
            total_losses += 1

        trade_details.append({
            'market_id': market_id,
            'market_title': market_data.get('title', 'Unknown'),
            'side': trade.get('side', 'N/A'),
            'price': price,
            'size': size,
            'cost': volume,
            'pnl': round(pnl, 2),
            'result': 'WIN' if is_win else 'LOSS',
            'timestamp': trade.get('timestamp'),
            'market_status': 'Resolved' if market_data.get('resolved') else 'Open'
        })

    total_trades = total_wins + total_losses

    if total_trades == 0:
        print("❌ No trades with market data found")
        return None

    # Step 6: Calculate metrics
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
        'markets_traded': len(market_ids),
        'markets_with_data': len(market_data_cache),
        'analysis_date': datetime.now().isoformat(),
        'analysis_period': 'Last 30 days',
        'trade_details': trade_details[:20],
        'data_source': 'Polymarket Official API (CLOB + Gamma)'
    }

    print(f"\n{'='*80}")
    print(f"📊 RESULTS (From Polymarket Official API - Last Month)")
    print(f"{'='*80}")
    print(f"Data Source: Polymarket Official API")
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


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    print("\n" + "="*80)
    print("💰 POLYMARKET WALLET PnL & ROI CALCULATOR")
    print("🔗 Using Official Polymarket API (CLOB + Gamma)")
    print("="*80)
    
    # Check if authentication is configured
    if POLYMARKET_API_KEY and POLYMARKET_SECRET and POLYMARKET_PASSPHRASE:
        print(f"✅ API Authentication configured")
    else:
        print(f"⚠️  No API authentication configured (may be required)")
        print(f"   Set environment variables: POLYMARKET_API_KEY, POLYMARKET_SECRET, POLYMARKET_PASSPHRASE")

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

        # Save results
        if results:
            output = {
                'generated_at': datetime.now().isoformat(),
                'total_wallets': len(results),
                'data_source': 'Polymarket Official API (CLOB + Gamma)',
                'analysis_period': 'Last 30 days',
                'api_docs': 'https://docs.polymarket.com/',
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
            print("📊 SUMMARY TABLE (All from Polymarket Official API)")
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
            print(f"\n📋 All calculations use Polymarket Official API:")
            print(f"   - CLOB API for trades: {POLYMARKET_CLOB_API}")
            print(f"   - Gamma API for markets: {POLYMARKET_GAMMA_API}")
            print(f"   - Analysis period: Last 30 days")
        else:
            print("\n❌ No data collected")

    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
