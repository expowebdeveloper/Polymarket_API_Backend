import requests
import pandas as pd

# 1. CONFIGURATION
# 🔴 DOUBLE CHECK THIS URL 🔴
# It should look like: https://api.goldsky.com/api/public/project_xyz/subgraphs/polymarket-indexer/v1.0.0/gn
GOLDSKY_URL = "https://api.goldsky.com/api/public/project_xyz/subgraphs/polymarket-indexer/v1.0.0/gn"

def fetch_top_traders():
    # 2. THE QUERY
    # We use 'orderFilleds' (plural of the entity name in schema.graphql)
    query = """
    query {
      orderFilleds(first: 1000, orderBy: timestamp, orderDirection: desc) {
        maker
        makerAmount
        timestamp
      }
    }
    """
    
    print(f"🚀 Connecting to: {GOLDSKY_URL} ...")
    
    try:
        response = requests.post(GOLDSKY_URL, json={'query': query})
        
        # DEBUG: Print status code if it fails
        if response.status_code != 200:
            print(f"❌ Server Error ({response.status_code}): {response.text}")
            return

        raw_json = response.json()

        # 3. ERROR HANDLING (The Fix)
        # Check if Goldsky sent us an error message instead of data
        if "errors" in raw_json:
            print("\n⚠️  GOLDSKY QUERY ERROR:")
            print(raw_json['errors'])
            return

        if "data" not in raw_json or raw_json['data'] is None:
            print("\n⚠️  NO DATA RECEIVED.")
            print(f"Response: {raw_json}")
            return

        trades = raw_json['data'].get('orderFilleds', [])
        
        if not trades:
            print("✅ Connection successful, but found 0 trades.")
            print("   (The indexer might still be syncing. Wait 2 minutes.)")
            return

        print(f"✅ Downloaded {len(trades)} trades.\n")
        
        # 4. PROCESS DATA
        leaderboard = {}
        for t in trades:
            user = t['maker']
            # Convert BigInt string to float (assuming 6 decimals for USDC, but raw is fine for rank)
            vol = float(t['makerAmount']) 
            
            if user not in leaderboard:
                leaderboard[user] = {'volume': 0.0, 'count': 0}
            
            leaderboard[user]['volume'] += vol
            leaderboard[user]['count'] += 1

        # 5. DISPLAY
        df = pd.DataFrame.from_dict(leaderboard, orient='index')
        df.reset_index(inplace=True)
        df.columns = ['User', 'Volume', 'Count']
        df = df.sort_values(by='Volume', ascending=False).head(10)
        
        print("🏆 LEADERBOARD 🏆")
        print(df.to_string(index=False))

    except Exception as e:
        print(f"\n❌ Python Exception: {e}")

if __name__ == "__main__":
    fetch_top_traders()