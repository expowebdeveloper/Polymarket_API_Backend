import requests
import sys
from typing import Dict, Any, List

BASE = "https://data-api.polymarket.com"
PROFILE = "https://polymarket.com/api/profile/stats"
PNL_API = "https://user-pnl-api.polymarket.com/user-pnl"


def fetch(url: str) -> Any:
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[ERROR] Cannot fetch {url}: {e}")
        return None


def get_positions(address: str) -> List[Dict]:
    return fetch(f"{BASE}/positions?user={address}") or []


def get_closed_positions(address: str) -> List[Dict]:
    return fetch(f"{BASE}/closed-positions?user={address}") or []


def get_value(address: str) -> float:
    data = fetch(f"{BASE}/value?user={address}")
    if data and isinstance(data, list):
        return data[0].get("value", 0)
    return 0


def get_activity(address: str) -> List[Dict]:
    return fetch(f"{BASE}/activity?user={address}") or []


def get_trades(address: str) -> List[Dict]:
    return fetch(f"{BASE}/trades?user={address}") or []


def get_profile_stats(address: str, username="") -> Dict:
    url = f"{PROFILE}?proxyAddress={address}&username={username}"
    return fetch(url) or {}


def get_pnl_timeseries(address: str) -> List[Dict]:
    url = f"{PNL_API}?user_address={address}&interval=1m&fidelity=1d"
    return fetch(url) or []


# -----------------------------------------
#   CALCULATIONS
# -----------------------------------------

def calculate_trade_stats(closed_positions: List[Dict]) -> Dict:
    wins = 0
    losses = 0
    realized = 0

    for p in closed_positions:
        pnl = p.get("cashPnl", 0)
        realized += pnl
        if pnl > 0:
            wins += 1
        else:
            losses += 1

    win_rate = (wins / (wins + losses)) * 100 if wins + losses > 0 else 0

    return {
        "wins": wins,
        "losses": losses,
        "win_rate": win_rate,
        "realized_pnl": realized
    }


def calculate_roi(total_realized: float, total_spent: float) -> float:
    if total_spent <= 0:
        return 0
    return (total_realized / total_spent) * 100


# -----------------------------------------
# MAIN FUNCTION
# -----------------------------------------

def run(address: str):
    print("\nFetching data...\n")

    open_positions = get_positions(address)
    closed_positions = get_closed_positions(address)
    portfolio_value = get_value(address)
    activity = get_activity(address)
    trades = get_trades(address)
    pnl_timeseries = get_pnl_timeseries(address)

    # Stats
    trade_stats = calculate_trade_stats(closed_positions)

    total_spent = sum(p.get("initialValue", 0) for p in closed_positions)
    roi = calculate_roi(trade_stats["realized_pnl"], total_spent)

    # -----------------------------------------
    # FINAL OUTPUT
    # -----------------------------------------
    print("=========== USER REPORT ===========")
    print(f"Wallet Address       : {address}")
    print(f"Open Positions       : {len(open_positions)}")
    print(f"Closed Positions     : {len(closed_positions)}")
    print(f"Total Trades         : {len(trades)}")
    print(f"Portfolio Value      : ${portfolio_value:,.2f}")

    print("\n----- PERFORMANCE -----")
    print(f"Total Realized PnL   : ${trade_stats['realized_pnl']:.2f}")
    print(f"Win Rate             : {trade_stats['win_rate']:.2f}%")
    print(f"ROI                  : {roi:.2f}%")

    print("\n----- PNL TIME SERIES -----")
    if pnl_timeseries:
        print(f"Entries Loaded       : {len(pnl_timeseries)}")
        print(f"Latest Portfolio     : ${pnl_timeseries[-1]['p']:,.2f}")
    else:
        print("No PNL time series available.")

    print("\nDone.\n")


# -----------------------------------------
# ENTRY POINT
# -----------------------------------------

if __name__ == "__main__":
    if len(sys.argv) == 2:
        wallet = sys.argv[1]
    else:
        wallet = input("Enter user wallet address: ").strip()

    if not wallet.startswith("0x") or len(wallet) < 10:
        print("Invalid wallet address.")
        sys.exit(1)

    run(wallet)
