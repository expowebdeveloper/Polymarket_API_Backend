
import requests
import json
import time

def verify_api():
    base_url = "http://127.0.0.1:8000"
    
    print("Fetching leaderboard...")
    try:
        response = requests.get(f"{base_url}/leaderboard/pnl?period=all&limit=10")
        if response.status_code != 200:
            print(f"Error: {response.status_code} - {response.text}")
            return
            
        data = response.json()
        entries = data.get("entries", [])
        
        if not entries:
            print("No entries found in leaderboard.")
            return

        print(f"Found {len(entries)} entries.")
        first_entry = entries[0]
        
        # Check for new fields
        expected_fields = ["score_win_rate", "score_roi", "score_pnl", "score_risk"]
        missing = [f for f in expected_fields if f not in first_entry]
        
        if missing:
             print(f"FAILED: Missing fields: {missing}")
        else:
             print("SUCCESS: All score fields present.")
             print("Sample Entry Scores:")
             for f in expected_fields:
                 print(f"  {f}: {first_entry.get(f)}")

    except Exception as e:
        print(f"Exception: {e}")

if __name__ == "__main__":
    time.sleep(2) # Wait for reload
    
    # helper to add wallet
    print("Adding sample wallet...")
    try:
        requests.post("http://127.0.0.1:8000/leaderboard/add-wallet", json={"wallet_address": "0xd8379ba51c110cac3dd324f56f1766a5e1284451"}) 
        # Using a known active trader if possible, or just a random one. 
        # "0xd8379ba51c110cac3dd324f56f1766a5e1284451" is just an example. 
        # Let's try the one from the schema example: 0x17db3fcd93ba12d38382a0cade24b200185c5f6d
        requests.post("http://127.0.0.1:8000/leaderboard/add-wallet", json={"wallet_address": "0x17db3fcd93ba12d38382a0cade24b200185c5f6d"})
    except:
        pass
        
    verify_api()
