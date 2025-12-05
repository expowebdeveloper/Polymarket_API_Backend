#!/usr/bin/env python3
"""
Polymarket Trader Leaderboard Generator
Using DomeAPI - Standalone Script for Testing

Requirements:
- requests library: pip install requests
"""

import requests
import json
from collections import defaultdict
from datetime import datetime
import time

# ============================================================================
# CONFIGURATION
# ============================================================================
DOMEAPI_BASE_URL = "https://api.domeapi.io/v1/polymarket"
LIMIT_MARKETS = 10  # Number of closed markets to analyze
LIMIT_ORDERS_PER_MARKET = 500  # Max orders to fetch per market
REQUEST_DELAY = 0.5  # Delay between requests (seconds) - respect rate limits

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def determine_win_or_loss(trade_token_id, winning_token_id):
    """
    Determine if a trade resulted in a win or loss

    Args:
        trade_token_id: Token ID from the trade
        winning_token_id: Token ID of the winning side

    Returns:
        "WIN" or "LOSS"
    """
    return "WIN" if trade_token_id == winning_token_id else "LOSS"


def calculate_trade_pnl(trade, winning_token_id):
    """
    Calculate profit/loss for a single trade

    Args:
        trade: Trade object with shares_normalized and price
        winning_token_id: Token ID of the winning side

    Returns:
        float: PnL in dollars
    """
    cost = trade['shares_normalized'] * trade['price']

    if determine_win_or_loss(trade['token_id'], winning_token_id) == "WIN":
        # Winner gets $1 per share
        payout = trade['shares_normalized'] * 1.0
        pnl = payout - cost
    else:
        # Loser loses their entire bet
        pnl = -cost

    return pnl


def fetch_closed_markets(limit=10):
    """
    Fetch closed markets with winning side data from DomeAPI

    Args:
        limit: Number of markets to fetch

    Returns:
        list: List of market objects
    """
    print(f"\n📊 Fetching {limit} closed markets from DomeAPI...")

    try:
        response = requests.get(
            f"{DOMEAPI_BASE_URL}/markets",
            params={
                'status': 'closed',
                'limit': limit
            },
            timeout=30
        )

        if response.status_code == 200:
            data = response.json()
            markets = data.get('markets', [])
            print(f"✅ Successfully fetched {len(markets)} closed markets")
            return markets
        else:
            print(f"❌ Error fetching markets: Status {response.status_code}")
            print(f"Response: {response.text[:200]}")
            return []
    except requests.exceptions.RequestException as e:
        print(f"❌ Network error fetching markets: {e}")
        return []
    except Exception as e:
        print(f"❌ Unexpected error fetching markets: {e}")
        return []


def fetch_market_orders(market_slug, limit=500):
    """
    Fetch all orders for a specific market

    Args:
        market_slug: Market identifier
        limit: Maximum orders to fetch

    Returns:
        list: List of order objects
    """
    print(f"  📈 Fetching orders for: {market_slug[:50]}...")

    try:
        time.sleep(REQUEST_DELAY)  # Rate limiting

        response = requests.get(
            f"{DOMEAPI_BASE_URL}/orders",
            params={
                'market_slug': market_slug,
                'limit': limit
            },
            timeout=30
        )

        if response.status_code == 200:
            data = response.json()
            orders = data.get('orders', [])
            print(f"    ✅ Found {len(orders)} orders")
            return orders
        else:
            print(f"    ❌ Error: Status {response.status_code}")
            return []
    except requests.exceptions.RequestException as e:
        print(f"    ❌ Network error: {e}")
        return []
    except Exception as e:
        print(f"    ❌ Unexpected error: {e}")
        return []


def calculate_trader_metrics(trader_data):
    """
    Calculate all scoring metrics for a trader

    Scoring Algorithm:
    - ROI: 40% weight
    - Win Rate: 30% weight
    - Consistency: 20% weight
    - Recency: 10% weight

    Args:
        trader_data: Dictionary with wallet_address and list of trades

    Returns:
        dict: Calculated metrics or None if insufficient data
    """
    trades = trader_data['trades']

    if not trades:
        return None

    total_trades = len(trades)
    total_wins = sum(1 for t in trades if t['result'] == 'WIN')
    total_losses = total_trades - total_wins
    total_pnl = sum(t['pnl'] for t in trades)
    total_volume = sum(t['volume'] for t in trades)

    # 1. Win Rate (30% weight)
    win_rate = (total_wins / total_trades * 100) if total_trades > 0 else 0

    # 2. ROI (40% weight)
    roi = (total_pnl / total_volume * 100) if total_volume > 0 else 0

    # 3. Consistency (20% weight) - performance in last 10 trades
    recent_trades = sorted(trades, key=lambda x: x['timestamp'], reverse=True)[:10]
    if len(recent_trades) >= 10:
        recent_wins = sum(1 for t in recent_trades if t['result'] == 'WIN')
        consistency = (recent_wins / 10) * 100
    else:
        consistency = win_rate  # Use overall win rate if < 10 trades

    # 4. Recency (10% weight) - time decay factor
    current_time = time.time()
    recency_scores = []

    for trade in trades:
        days_ago = (current_time - trade['timestamp']) / 86400  # Convert to days
        decay_factor = 1 / (1 + (days_ago / 30))  # Decay over 30 days
        if trade['result'] == 'WIN':
            recency_scores.append(decay_factor * 100)
        else:
            recency_scores.append(0)

    recency = sum(recency_scores) / len(recency_scores) if recency_scores else 0

    # 5. Final Score (weighted combination)
    final_score = (
        (0.4 * roi) +
        (0.3 * win_rate) +
        (0.2 * consistency) +
        (0.1 * recency)
    )

    return {
        'wallet_address': trader_data['wallet_address'],
        'total_trades': total_trades,
        'total_wins': total_wins,
        'total_losses': total_losses,
        'win_rate_percent': round(win_rate, 2),
        'pnl': round(total_pnl, 2),
        'roi': round(roi, 2),
        'total_volume': round(total_volume, 2),
        'consistency_score': round(consistency, 2),
        'recency_score': round(recency, 2),
        'final_score': round(final_score, 2)
    }


def generate_leaderboard():
    """
    Main function to generate complete leaderboard

    Returns:
        list: Sorted leaderboard of traders with scores
    """
    print("\n" + "="*80)
    print("🏆 POLYMARKET TRADER LEADERBOARD GENERATOR")
    print("="*80)

    # Step 1: Fetch closed markets
    markets = fetch_closed_markets(limit=LIMIT_MARKETS)

    if not markets:
        print("❌ No markets fetched. Exiting.")
        return None

    # Step 2: Aggregate trader data across all markets
    print(f"\n📊 Processing {len(markets)} markets...")
    traders_data = defaultdict(lambda: {
        'wallet_address': '',
        'trades': []
    })

    for idx, market in enumerate(markets, 1):
        market_slug = market.get('market_slug')
        winning_side = market.get('winning_side')

        if not winning_side:
            print(f"  ⚠️  Skipping {market_slug} - no winning_side data")
            continue

        winning_token_id = winning_side['id']
        winning_label = winning_side['label']

        print(f"\n  [{idx}/{len(markets)}] Processing: {market.get('title', 'Unknown')[:60]}")
        print(f"    Winner: {winning_label} (Token: {winning_token_id[:20]}...)")

        # Fetch orders for this market
        orders = fetch_market_orders(market_slug, limit=LIMIT_ORDERS_PER_MARKET)

        if not orders:
            print(f"    ⚠️  No orders found for this market")
            continue

        # Process each order
        for order in orders:
            wallet = order['user']

            # Calculate PnL and result for this trade
            pnl = calculate_trade_pnl(order, winning_token_id)
            result = determine_win_or_loss(order['token_id'], winning_token_id)
            volume = order['shares_normalized'] * order['price']

            # Store trade data
            traders_data[wallet]['wallet_address'] = wallet
            traders_data[wallet]['trades'].append({
                'market_slug': market_slug,
                'market_title': market.get('title', 'Unknown'),
                'side': order['side'],
                'token_id': order['token_id'],
                'shares_normalized': order['shares_normalized'],
                'price': order['price'],
                'timestamp': order['timestamp'],
                'pnl': pnl,
                'volume': volume,
                'result': result
            })

    print(f"\n✅ Data collection complete. Found {len(traders_data)} unique traders")

    # Step 3: Calculate metrics for each trader
    print(f"\n🧮 Calculating metrics for all traders...")

    leaderboard = []
    for wallet, data in traders_data.items():
        metrics = calculate_trader_metrics(data)
        if metrics:
            leaderboard.append(metrics)

    # Step 4: Sort by final_score (descending)
    leaderboard.sort(key=lambda x: x['final_score'], reverse=True)

    print(f"✅ Calculated metrics for {len(leaderboard)} traders")

    # Step 5: Add rank
    for rank, trader in enumerate(leaderboard, 1):
        trader['rank'] = rank

    return leaderboard


def display_leaderboard(leaderboard, top_n=20):
    """
    Display leaderboard in console (pretty print)

    Args:
        leaderboard: List of trader metrics
        top_n: Number of top traders to display
    """
    print("\n" + "="*100)
    print(f"🏆 TOP {top_n} TRADERS LEADERBOARD")
    print("="*100)
    print()

    # Header
    header = f"{'Rank':<6} {'Wallet Address':<45} {'Score':<10} {'ROI%':<10} {'Win%':<10} {'Trades':<10} {'PnL$':<12}"
    print(header)
    print("-" * 100)

    # Display top N traders
    for trader in leaderboard[:top_n]:
        wallet_short = trader['wallet_address'][:10] + "..." + trader['wallet_address'][-8:]
        row = (
            f"{trader['rank']:<6} "
            f"{wallet_short:<45} "
            f"{trader['final_score']:<10.2f} "
            f"{trader['roi']:<10.2f} "
            f"{trader['win_rate_percent']:<10.2f} "
            f"{trader['total_trades']:<10} "
            f"${trader['pnl']:<11.2f}"
        )
        print(row)

    print("\n" + "="*100)


def save_leaderboard_json(leaderboard, filename='polymarket_leaderboard.json'):
    """
    Save leaderboard to JSON file

    Args:
        leaderboard: List of trader metrics
        filename: Output filename
    """
    output = {
        'generated_at': datetime.now().isoformat(),
        'total_traders': len(leaderboard),
        'markets_analyzed': LIMIT_MARKETS,
        'scoring_weights': {
            'roi': '40%',
            'win_rate': '30%',
            'consistency': '20%',
            'recency': '10%'
        },
        'leaderboard': leaderboard
    }

    with open(filename, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"\n💾 Leaderboard saved to: {filename}")
    print(f"📊 Total traders: {len(leaderboard)}")
    print(f"📈 Markets analyzed: {LIMIT_MARKETS}")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    try:
        # Generate leaderboard
        leaderboard = generate_leaderboard()

        if leaderboard and len(leaderboard) > 0:
            # Display in console
            display_leaderboard(leaderboard, top_n=20)

            # Save to JSON
            save_leaderboard_json(leaderboard)

            print("\n✅ Leaderboard generation complete!")
            print("\n📋 Sample top trader data:")
            print(json.dumps(leaderboard[0], indent=2))
        else:
            print("\n❌ Failed to generate leaderboard or no traders found")

    except KeyboardInterrupt:
        print("\n\n⚠️  Process interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()