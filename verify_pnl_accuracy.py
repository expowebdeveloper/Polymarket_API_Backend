"""
PnL Calculation Verification Script

This script verifies the accuracy of PnL calculations by:
1. Fetching data directly from Polymarket API
2. Manually calculating all metrics
3. Comparing with your endpoint results
"""

import requests
import json
from decimal import Decimal
from typing import Dict, List


def fetch_positions(wallet_address: str) -> List[Dict]:
    """Fetch positions from Polymarket API."""
    url = "https://data-api.polymarket.com/positions"
    params = {"user": wallet_address, "limit": 100}
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_trades(wallet_address: str) -> List[Dict]:
    """Fetch trades from Polymarket API."""
    url = "https://data-api.polymarket.com/trades"
    params = {"user": wallet_address}
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_activities(wallet_address: str) -> List[Dict]:
    """Fetch activities from Polymarket API."""
    url = "https://data-api.polymarket.com/activity"
    params = {"user": wallet_address}
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def calculate_manual_pnl(wallet_address: str) -> Dict:
    """
    Manually calculate PnL metrics from API data.
    This should match your endpoint calculation.
    """
    print(f"\n{'='*80}")
    print(f"Verifying PnL Calculation for: {wallet_address}")
    print(f"{'='*80}\n")
    
    # Fetch data
    print("📊 Fetching data from Polymarket API...")
    positions = fetch_positions(wallet_address)
    trades = fetch_trades(wallet_address)
    activities = fetch_activities(wallet_address)
    
    print(f"  ✓ Fetched {len(positions)} positions")
    print(f"  ✓ Fetched {len(trades)} trades")
    print(f"  ✓ Fetched {len(activities)} activities\n")
    
    # Initialize metrics
    total_invested = Decimal('0')
    total_current_value = Decimal('0')
    total_realized_pnl = Decimal('0')
    total_unrealized_pnl = Decimal('0')
    total_rewards = Decimal('0')
    total_redemptions = Decimal('0')
    
    # Calculate from positions
    print("💰 Calculating from Positions...")
    for pos in positions:
        initial_value = Decimal(str(pos.get('initialValue', 0)))
        current_value = Decimal(str(pos.get('currentValue', 0)))
        cash_pnl = Decimal(str(pos.get('cashPnl', 0)))
        realized_pnl = Decimal(str(pos.get('realizedPnl', 0)))
        
        total_invested += initial_value
        total_current_value += current_value
        total_realized_pnl += realized_pnl
        
        # Unrealized = cash_pnl - realized_pnl
        unrealized = cash_pnl - realized_pnl
        total_unrealized_pnl += unrealized
    
    print(f"  Total Invested: ${total_invested:,.2f}")
    print(f"  Total Current Value: ${total_current_value:,.2f}")
    print(f"  Total Realized PnL: ${total_realized_pnl:,.2f}")
    print(f"  Total Unrealized PnL: ${total_unrealized_pnl:,.2f}\n")
    
    # Calculate from activities
    print("🎁 Calculating from Activities...")
    for activity in activities:
        activity_type = activity.get('type', '')
        usdc_size = Decimal(str(activity.get('usdcSize', 0)))
        
        if activity_type == 'REWARD':
            total_rewards += usdc_size
        elif activity_type == 'REDEEM':
            total_redemptions += usdc_size
    
    print(f"  Total Rewards: ${total_rewards:,.2f}")
    print(f"  Total Redemptions: ${total_redemptions:,.2f}\n")
    
    # Calculate total PnL
    total_pnl = total_realized_pnl + total_unrealized_pnl + total_rewards - total_redemptions
    pnl_percentage = (total_pnl / total_invested * 100) if total_invested > 0 else Decimal('0')
    
    print(f"📈 Total PnL: ${total_pnl:,.2f}")
    print(f"📊 PnL Percentage: {pnl_percentage:.2f}%\n")
    
    # Calculate trade metrics
    print("📊 Calculating Trade Metrics...")
    total_stakes = Decimal('0')
    total_trade_pnl = Decimal('0')
    winning_trades_count = 0
    total_trades_with_pnl = 0
    stakes_of_wins = Decimal('0')
    
    for trade in trades:
        size = Decimal(str(trade.get('size', 0)))
        price = Decimal(str(trade.get('price', 0)))
        stake = size * price
        total_stakes += stake
        
        pnl = trade.get('pnl')
        if pnl is not None:
            trade_pnl = Decimal(str(pnl))
            total_trade_pnl += trade_pnl
            total_trades_with_pnl += 1
            
            if trade_pnl > 0:
                winning_trades_count += 1
                stakes_of_wins += stake
    
    print(f"  Total Stakes: ${total_stakes:,.2f}")
    print(f"  Total Trade PnL: ${total_trade_pnl:,.2f}")
    print(f"  Trades with PnL: {total_trades_with_pnl} / {len(trades)}")
    print(f"  Winning Trades: {winning_trades_count}\n")
    
    # Calculate ROI
    roi = (total_trade_pnl / total_stakes * 100) if total_stakes > 0 else Decimal('0')
    
    # Calculate Win Rate
    win_rate = (winning_trades_count / total_trades_with_pnl * 100) if total_trades_with_pnl > 0 else Decimal('0')
    
    # Calculate Stake-Weighted Win Rate
    stake_weighted_win_rate = (stakes_of_wins / total_stakes * 100) if total_stakes > 0 else Decimal('0')
    
    print(f"📊 Trade Metrics:")
    print(f"  ROI: {roi:.2f}%")
    print(f"  Win Rate: {win_rate:.2f}%")
    print(f"  Stake-Weighted Win Rate: {stake_weighted_win_rate:.2f}%\n")
    
    # Statistics
    buy_trades = len([t for t in trades if t.get('side') == 'BUY'])
    sell_trades = len([t for t in trades if t.get('side') == 'SELL'])
    active_positions = len([p for p in positions if Decimal(str(p.get('currentValue', 0))) > 0])
    closed_positions = len([p for p in positions if Decimal(str(p.get('currentValue', 0))) == 0])
    
    avg_trade_size = (sum(Decimal(str(t.get('size', 0))) for t in trades) / len(trades)) if trades else Decimal('0')
    
    result = {
        "wallet_address": wallet_address,
        "total_invested": float(total_invested),
        "total_current_value": float(total_current_value),
        "total_realized_pnl": float(total_realized_pnl),
        "total_unrealized_pnl": float(total_unrealized_pnl),
        "total_rewards": float(total_rewards),
        "total_redemptions": float(total_redemptions),
        "total_pnl": float(total_pnl),
        "pnl_percentage": float(pnl_percentage),
        "key_metrics": {
            "total_trade_pnl": float(total_trade_pnl),
            "roi": float(roi),
            "win_rate": float(win_rate),
            "stake_weighted_win_rate": float(stake_weighted_win_rate),
            "winning_trades": winning_trades_count,
            "total_trades_with_pnl": total_trades_with_pnl,
            "total_stakes": float(total_stakes),
        },
        "statistics": {
            "total_trades": len(trades),
            "buy_trades": buy_trades,
            "sell_trades": sell_trades,
            "active_positions": active_positions,
            "closed_positions": closed_positions,
            "total_positions": len(positions),
            "avg_trade_size": float(avg_trade_size),
        }
    }
    
    return result


def compare_results(manual_result: Dict, api_result: Dict, tolerance: float = 0.01):
    """
    Compare manual calculation with API endpoint result.
    
    Args:
        manual_result: Result from manual calculation
        api_result: Result from your endpoint
        tolerance: Allowed difference (default 0.01 for rounding)
    """
    print(f"\n{'='*80}")
    print("COMPARISON RESULTS")
    print(f"{'='*80}\n")
    
    differences = []
    
    # Compare main metrics
    metrics_to_compare = [
        "total_invested",
        "total_current_value",
        "total_realized_pnl",
        "total_unrealized_pnl",
        "total_rewards",
        "total_redemptions",
        "total_pnl",
        "pnl_percentage",
    ]
    
    for metric in metrics_to_compare:
        manual_val = manual_result.get(metric, 0)
        api_val = api_result.get(metric, 0)
        diff = abs(manual_val - api_val)
        
        status = "✅" if diff <= tolerance else "❌"
        print(f"{status} {metric}:")
        print(f"   Manual: {manual_val:,.2f}")
        print(f"   API:    {api_val:,.2f}")
        print(f"   Diff:   {diff:,.2f}")
        
        if diff > tolerance:
            differences.append(f"{metric}: diff={diff:,.2f}")
        print()
    
    # Compare key metrics
    print("Key Metrics:")
    manual_metrics = manual_result.get("key_metrics", {})
    api_metrics = api_result.get("key_metrics", {})
    
    for metric in ["total_trade_pnl", "roi", "win_rate", "stake_weighted_win_rate", "total_stakes"]:
        manual_val = manual_metrics.get(metric, 0)
        api_val = api_metrics.get(metric, 0)
        diff = abs(manual_val - api_val)
        
        status = "✅" if diff <= tolerance else "❌"
        print(f"{status} {metric}:")
        print(f"   Manual: {manual_val:,.2f}")
        print(f"   API:    {api_val:,.2f}")
        print(f"   Diff:   {diff:,.2f}")
        
        if diff > tolerance:
            differences.append(f"key_metrics.{metric}: diff={diff:,.2f}")
        print()
    
    # Summary
    print(f"{'='*80}")
    if differences:
        print(f"❌ Found {len(differences)} differences:")
        for diff in differences:
            print(f"   - {diff}")
    else:
        print("✅ All calculations match!")
    print(f"{'='*80}\n")
    
    return len(differences) == 0


def main():
    """Main function to verify PnL calculation."""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python verify_pnl_accuracy.py <wallet_address> [api_endpoint_url]")
        print("\nExample:")
        print("  python verify_pnl_accuracy.py 0x17db3fcd93ba12d38382a0cade24b200185c5f6d")
        print("  python verify_pnl_accuracy.py 0x17db3fcd93ba12d38382a0cade24b200185c5f6d http://127.0.0.1:8000/pnl/calculate?user=")
        sys.exit(1)
    
    wallet_address = sys.argv[1]
    
    # Calculate manually
    manual_result = calculate_manual_pnl(wallet_address)
    
    # Save manual result
    with open(f"manual_pnl_{wallet_address[:10]}.json", "w") as f:
        json.dump(manual_result, f, indent=2)
    print(f"💾 Saved manual calculation to: manual_pnl_{wallet_address[:10]}.json\n")
    
    # If API endpoint provided, compare
    if len(sys.argv) >= 3:
        api_endpoint = sys.argv[2]
        try:
            response = requests.get(f"{api_endpoint}{wallet_address}", timeout=30)
            response.raise_for_status()
            api_result = response.json()
            
            compare_results(manual_result, api_result)
        except Exception as e:
            print(f"❌ Error fetching from API endpoint: {e}")
            print("\nManual calculation completed. Compare manually with your endpoint result.")
    else:
        print("ℹ️  No API endpoint provided. Compare manually with your endpoint result.")
        print(f"   Your endpoint: http://127.0.0.1:8000/pnl/calculate?user={wallet_address}")


if __name__ == "__main__":
    main()

