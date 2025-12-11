#!/usr/bin/env python3
"""
Polymarket Wallet PnL Calculator - Last 30 Days (FIXED)
Uses correct CLOB API endpoints and Data API for accurate wallet analysis
"""

import requests
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import pandas as pd
from decimal import Decimal

class PolymarketWalletAnalyzer:
    def __init__(self):
        """Initialize with correct Polymarket endpoints"""
        self.clob_endpoint = "https://clob.polymarket.com"
        self.data_api_endpoint = "https://data-api.polymarket.com"

    def get_wallet_trades_from_data_api(self, wallet_address: str, days: int = 30) -> List[Dict]:
        """
        Fetch trades using the Data API endpoint
        This is the correct public endpoint for wallet trade history

        Args:
            wallet_address: Polymarket wallet address (0x...)
            days: Number of days to look back (default: 30)

        Returns:
            List of trade records
        """
        try:
            # Calculate timestamp for N days ago
            now = datetime.utcnow()
            cutoff_date = now - timedelta(days=days)
            cutoff_timestamp = int(cutoff_date.timestamp())

            url = f"{self.data_api_endpoint}/trades"

            # Correct parameters for Data API
            params = {
                "userProfileAddress": wallet_address.lower(),
                "limit": 1000
            }

            print(f"📊 Fetching trades from Data API for {wallet_address}...")
            print(f"   Looking back {days} days (from {cutoff_date.strftime('%Y-%m-%d')})")
            print(f"   Endpoint: {url}")
            print(f"   Parameters: {params}\n")

            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()

            trades = response.json()
            if isinstance(trades, dict) and 'trades' in trades:
                trades = trades['trades']

            # Filter trades by date
            filtered_trades = []
            for trade in trades:
                try:
                    trade_time = int(trade.get('timestamp', 0))
                    if trade_time >= cutoff_timestamp:
                        filtered_trades.append(trade)
                except:
                    continue

            print(f"✅ Retrieved {len(filtered_trades)} trades from last {days} days")
            return filtered_trades

        except requests.exceptions.RequestException as e:
            print(f"❌ Error fetching trades: {e}")
            if hasattr(e.response, 'text'):
                print(f"   Response: {e.response.text[:200]}")
            return []

    def get_wallet_activity(self, wallet_address: str, days: int = 30) -> List[Dict]:
        """
        Fetch user activity (alternative endpoint)

        Args:
            wallet_address: Polymarket wallet address
            days: Number of days to look back

        Returns:
            List of activity records
        """
        try:
            now = datetime.utcnow()
            cutoff_date = now - timedelta(days=days)
            cutoff_timestamp = int(cutoff_date.timestamp())

            url = f"{self.data_api_endpoint}/user-activity"

            params = {
                "userProfileAddress": wallet_address.lower(),
                "limit": 1000,
                "type": "TRADE"
            }

            print(f"📍 Fetching user activity...")

            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()

            activity = response.json()
            if isinstance(activity, dict) and 'activity' in activity:
                activity = activity['activity']

            # Filter by date
            filtered_activity = []
            for item in activity:
                try:
                    item_time = int(item.get('timestamp', 0))
                    if item_time >= cutoff_timestamp:
                        filtered_activity.append(item)
                except:
                    continue

            print(f"✅ Retrieved {len(filtered_activity)} activity records")
            return filtered_activity

        except requests.exceptions.RequestException as e:
            print(f"⚠️  Could not fetch activity: {e}")
            return []

    def calculate_trade_pnl(self, trades: List[Dict]) -> Dict:
        """
        Calculate PnL from individual trades

        Args:
            trades: List of trade records

        Returns:
            Dictionary with PnL calculations
        """
        if not trades:
            return {
                "total_trades": 0,
                "buy_volume": 0,
                "sell_volume": 0,
                "total_fees": 0,
                "realized_pnl": 0,
                "trades_by_side": {}
            }

        total_buy_cost = 0
        total_sell_revenue = 0
        total_fees = 0
        buy_count = 0
        sell_count = 0

        buy_trades = []
        sell_trades = []

        for trade in trades:
            try:
                # Extract trade data - handle different API response formats
                side = trade.get("side", "").upper()
                size = float(trade.get("size", 0))
                price = float(trade.get("price", 0))

                # USDC size is the actual transaction size
                usdc_size = float(trade.get("usdcSize", size * price))

                # Some trades might have fee info
                fee_rate_bps = float(trade.get("fee_rate_bps", 0))
                fee = usdc_size * (fee_rate_bps / 10000) if fee_rate_bps else 0

                if side == "BUY":
                    total_buy_cost += usdc_size + fee
                    buy_count += 1
                    buy_trades.append({
                        "size": size,
                        "price": price,
                        "usdc_value": usdc_size,
                        "fee": fee,
                        "timestamp": datetime.fromtimestamp(int(trade.get("timestamp", 0))).strftime("%Y-%m-%d %H:%M:%S") if trade.get("timestamp") else "N/A"
                    })

                elif side == "SELL":
                    total_sell_revenue += usdc_size - fee
                    sell_count += 1
                    sell_trades.append({
                        "size": size,
                        "price": price,
                        "usdc_value": usdc_size,
                        "fee": fee,
                        "timestamp": datetime.fromtimestamp(int(trade.get("timestamp", 0))).strftime("%Y-%m-%d %H:%M:%S") if trade.get("timestamp") else "N/A"
                    })

                total_fees += fee

            except (ValueError, TypeError, KeyError) as e:
                print(f"⚠️  Error processing trade: {e}")
                continue

        realized_pnl = total_sell_revenue - total_buy_cost

        return {
            "total_trades": len(trades),
            "buy_trades_count": buy_count,
            "sell_trades_count": sell_count,
            "total_buy_cost": round(total_buy_cost, 2),
            "total_sell_revenue": round(total_sell_revenue, 2),
            "total_fees": round(total_fees, 2),
            "realized_pnl": round(realized_pnl, 2),
            "roi_percent": round((realized_pnl / total_buy_cost * 100) if total_buy_cost > 0 else 0, 2),
            "buy_trades": buy_trades,
            "sell_trades": sell_trades
        }

    def print_pnl_report(self, wallet_address: str, pnl_data: Dict):
        """Print formatted PnL report"""

        print("\n" + "="*65)
        print("📈 POLYMARKET WALLET PnL REPORT (LAST 30 DAYS)")
        print("="*65)
        print(f"Wallet Address: {wallet_address}")
        print(f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print("="*65)

        if pnl_data["total_trades"] == 0:
            print("❌ No trades found in the last 30 days")
            print("\nNote: If you have trades but they're not showing:")
            print("  1. Check if the wallet address is correct")
            print("  2. Ensure trades are from the last 30 days")
            print("  3. Try the wallet address in Polymarket UI directly")
            return

        print(f"\n📊 TRADE SUMMARY")
        print(f"  Total Trades:         {pnl_data['total_trades']}")
        print(f"  Buy Trades:           {pnl_data['buy_trades_count']}")
        print(f"  Sell Trades:          {pnl_data['sell_trades_count']}")

        print(f"\n💰 FINANCIAL METRICS")
        print(f"  Total Buy Cost:       ${pnl_data['total_buy_cost']:,.2f}")
        print(f"  Total Sell Revenue:   ${pnl_data['total_sell_revenue']:,.2f}")
        print(f"  Total Fees Paid:      ${pnl_data['total_fees']:,.2f}")

        print(f"\n🎯 PROFIT & LOSS")
        pnl = pnl_data['realized_pnl']
        roi = pnl_data['roi_percent']

        if pnl >= 0:
            print(f"  ✅ Realized PnL:      +${pnl:,.2f}")
            print(f"  ✅ ROI:               +{roi}%")
        else:
            print(f"  ❌ Realized PnL:      -${abs(pnl):,.2f}")
            print(f"  ❌ ROI:               {roi}%")

        print("\n" + "="*65)

    def export_to_csv(self, pnl_data: Dict, filename: str = "polymarket_pnl.csv"):
        """Export PnL data to CSV"""
        try:
            all_trades = []

            for trade in pnl_data.get("buy_trades", []):
                trade["side"] = "BUY"
                all_trades.append(trade)

            for trade in pnl_data.get("sell_trades", []):
                trade["side"] = "SELL"
                all_trades.append(trade)

            if all_trades:
                df = pd.DataFrame(all_trades)
                df = df.sort_values('timestamp')
                df.to_csv(filename, index=False)
                print(f"\n✅ Data exported to {filename}")
                print(f"   Total records: {len(df)}")

        except Exception as e:
            print(f"⚠️  Could not export to CSV: {e}")


def main():
    """Main execution"""

    print("\n" + "="*65)
    print("🔄 POLYMARKET WALLET PnL CALCULATOR")
    print("="*65)

    # User Configuration
    wallet_address = input("\nEnter your Polymarket wallet address (0x...): ").strip()

    # Validate address format
    if not wallet_address.startswith("0x") or len(wallet_address) != 42:
        print("❌ Invalid wallet address format. Must be 0x followed by 40 hex characters.")
        return

    # Initialize analyzer
    analyzer = PolymarketWalletAnalyzer()

    # Fetch and analyze
    print("\n🔄 Analyzing wallet...")

    # Try Data API first
    trades = analyzer.get_wallet_trades_from_data_api(wallet_address, days=30)

    # If no trades, try activity endpoint
    if not trades:
        print("\n📌 Trying alternative endpoint...")
        activity = analyzer.get_wallet_activity(wallet_address, days=30)
        if activity:
            trades = activity

    # Calculate PnL
    pnl_data = analyzer.calculate_trade_pnl(trades)

    # Display report
    analyzer.print_pnl_report(wallet_address, pnl_data)

    # Export options
    if pnl_data["total_trades"] > 0:
        export = input("\nExport detailed trade data to CSV? (y/n): ").strip().lower()
        if export == 'y':
            analyzer.export_to_csv(pnl_data)

        # Save JSON report
        save_json = input("Save full report as JSON? (y/n): ").strip().lower()
        if save_json == 'y':
            with open("polymarket_pnl_report.json", "w") as f:
                json.dump(pnl_data, f, indent=2)
            print("✅ Report saved to polymarket_pnl_report.json")


if __name__ == "__main__":
    main()