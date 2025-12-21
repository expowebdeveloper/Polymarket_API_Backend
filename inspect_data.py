
import sys
import os
import json
# Add the current directory to sys.path so we can import app
sys.path.append(os.getcwd())

from app.services.data_fetcher import fetch_closed_positions, fetch_user_trades

def inspect_wallet(wallet):
    print(f"Inspecting wallet: {wallet}")
    
    print("\n--- Closed Positions (First 2) ---")
    try:
        positions = fetch_closed_positions(wallet, limit=2)
        print(json.dumps(positions, indent=2))
    except Exception as e:
        print(f"Error fetching closed positions: {e}")

    print("\n--- User Trades (First 2) ---")
    try:
        trades = fetch_user_trades(wallet) # No limit arg in this function, returns all?
        print(json.dumps(trades[:2] if trades else [], indent=2))
    except Exception as e:
        print(f"Error fetching trades: {e}")

if __name__ == "__main__":
    # Use the first wallet from the text file
    with open("wallet_address.txt", "r") as f:
        first_wallet = f.readline().strip()
    
    if first_wallet:
        inspect_wallet(first_wallet)
    else:
        print("No wallet found in wallet_address.txt")
