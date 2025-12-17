
import requests
import time

def verify_live_leaderboard():
    url = "https://polyrating.com/leaderboard/live"
    print(f"Calling live leaderboard: {url}")
    
    start = time.time()
    try:
        response = requests.post(url)
        duration = time.time() - start
        
        if response.status_code == 200:
            data = response.json()
            entries = data.get("entries", [])
            print(f"SUCCESS: Fetched {len(entries)} entries in {duration:.2f}s")
            for e in entries:
                print(f"Rank {e['rank']}: {e['wallet_address']} - PScore: {e.get('score_pnl')}")
        else:
            print(f"FAILED: {response.status_code} - {response.text}")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    time.sleep(2) # Wait for reload
    verify_live_leaderboard()
