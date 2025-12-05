# How to Add Wallet Addresses to Leaderboard

## Problem

The leaderboard only shows wallets that have data in the database. If you only see one wallet (e.g., `0xdbade4c82fb72780a0db9a38f821d8671aba9c95`), it means only that wallet has data stored.

## Solution

Use the new endpoints to fetch and save data for wallet addresses, which will make them appear in the leaderboard.

---

## Method 1: Add Single Wallet (Recommended)

### Using cURL

```bash
curl -X POST "http://127.0.0.1:8000/leaderboard/add-wallet" \
  -H "Content-Type: application/json" \
  -d '{
    "wallet_address": "0x17db3fcd93ba12d38382a0cade24b200185c5f6d"
  }'
```

### Using Python

```python
import requests

response = requests.post(
    "http://127.0.0.1:8000/leaderboard/add-wallet",
    json={
        "wallet_address": "0x17db3fcd93ba12d38382a0cade24b200185c5f6d"
    }
)

result = response.json()
print(f"Success: {result['success']}")
print(f"Trades saved: {result['trades_saved']}")
print(f"Positions saved: {result['positions_saved']}")
print(f"Activities saved: {result['activities_saved']}")
print(f"Message: {result['message']}")
```

### Response Example

```json
{
  "wallet_address": "0x17db3fcd93ba12d38382a0cade24b200185c5f6d",
  "success": true,
  "trades_saved": 150,
  "positions_saved": 25,
  "activities_saved": 200,
  "message": "Successfully added wallet. Saved: 150 trades, 25 positions, 200 activities"
}
```

---

## Method 2: Add Multiple Wallets at Once

### Using cURL

```bash
curl -X POST "http://127.0.0.1:8000/leaderboard/add-wallets" \
  -H "Content-Type: application/json" \
  -d '{
    "wallet_addresses": [
      "0x17db3fcd93ba12d38382a0cade24b200185c5f6d",
      "0x554ad2bc8a8f372d7e3376918fcb6e284387859a",
      "0x4fd9856c1cd3b014846c301174ec0b9e93b1a49e"
    ]
  }'
```

### Using Python

```python
import requests

wallets = [
    "0x17db3fcd93ba12d38382a0cade24b200185c5f6d",
    "0x554ad2bc8a8f372d7e3376918fcb6e284387859a",
    "0x4fd9856c1cd3b014846c301174ec0b9e93b1a49e"
]

response = requests.post(
    "http://127.0.0.1:8000/leaderboard/add-wallets",
    json={"wallet_addresses": wallets}
)

results = response.json()
for result in results:
    print(f"{result['wallet_address']}: {result['success']} - {result['message']}")
```

---

## Method 3: Using Existing Endpoints

You can also use the existing endpoints to fetch data for each wallet:

### Step 1: Fetch Trades
```bash
curl "http://127.0.0.1:8000/trades?user=0x17db3fcd93ba12d38382a0cade24b200185c5f6d"
```

### Step 2: Fetch Positions
```bash
curl "http://127.0.0.1:8000/positions?user=0x17db3fcd93ba12d38382a0cade24b200185c5f6d"
```

### Step 3: Fetch Activities
```bash
curl "http://127.0.0.1:8000/activity?user=0x17db3fcd93ba12d38382a0cade24b200185c5f6d"
```

After fetching all three, the wallet will appear in the leaderboard.

---

## Quick Script to Add Multiple Wallets

Create a file `add_wallets.py`:

```python
import requests
import time

# List of wallet addresses to add
WALLETS = [
    "0x17db3fcd93ba12d38382a0cade24b200185c5f6d",
    "0x554ad2bc8a8f372d7e3376918fcb6e284387859a",
    "0x4fd9856c1cd3b014846c301174ec0b9e93b1a49e",
    # Add more wallets here
]

BASE_URL = "http://127.0.0.1:8000"

print(f"Adding {len(WALLETS)} wallets to leaderboard...\n")

for wallet in WALLETS:
    print(f"Processing {wallet}...")
    
    try:
        response = requests.post(
            f"{BASE_URL}/leaderboard/add-wallet",
            json={"wallet_address": wallet},
            timeout=60
        )
        
        if response.status_code == 200:
            result = response.json()
            if result['success']:
                print(f"  ✅ Success: {result['trades_saved']} trades, "
                      f"{result['positions_saved']} positions, "
                      f"{result['activities_saved']} activities")
            else:
                print(f"  ⚠️  Warning: {result['message']}")
        else:
            print(f"  ❌ Error: {response.status_code} - {response.text}")
    
    except Exception as e:
        print(f"  ❌ Exception: {e}")
    
    # Small delay to avoid rate limiting
    time.sleep(1)

print("\n✅ Done! Check leaderboard at /leaderboard/pnl")
```

Run it:
```bash
python add_wallets.py
```

---

## Verify Wallets Are Added

After adding wallets, check the leaderboard:

```bash
# Check PnL leaderboard
curl "http://127.0.0.1:8000/leaderboard/pnl?limit=10"

# Check ROI leaderboard
curl "http://127.0.0.1:8000/leaderboard/roi?limit=10"

# Check Win Rate leaderboard
curl "http://127.0.0.1:8000/leaderboard/win-rate?limit=10"
```

---

## Common Issues

### Issue 1: "No data found for wallet"
**Cause:** The wallet has no trades, positions, or activities on Polymarket.

**Solution:** Verify the wallet address is correct and has activity on Polymarket.

### Issue 2: Wallet still not showing in leaderboard
**Cause:** The wallet has no trades (leaderboard requires at least one trade).

**Solution:** 
- Check if trades were saved: `GET /trades/from-db?user={wallet}`
- The wallet needs at least one trade to appear in leaderboards

### Issue 3: "Invalid wallet address format"
**Cause:** Wallet address doesn't match the required format (42 characters, starts with 0x).

**Solution:** Verify the wallet address format is correct.

---

## Example: Adding Known Polymarket Traders

Here are some example wallet addresses you can add:

```python
KNOWN_TRADERS = [
    "0x17db3fcd93ba12d38382a0cade24b200185c5f6d",  # Example trader 1
    "0x554ad2bc8a8f372d7e3376918fcb6e284387859a",  # Example trader 2
    "0x4fd9856c1cd3b014846c301174ec0b9e93b1a49e",  # Example trader 3
    # Add more from your wallet_address.txt file
]
```

---

## Notes

- **Data is fetched from Polymarket API** - Make sure you have internet connection
- **Processing may take time** - Each wallet fetches trades, positions, and activities
- **Rate limiting** - Add small delays between requests if processing many wallets
- **Data is saved to database** - Once saved, wallets will appear in all leaderboards
- **Updates are automatic** - If you add the same wallet again, data will be updated

---

## API Endpoints Summary

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/leaderboard/add-wallet` | POST | Add single wallet |
| `/leaderboard/add-wallets` | POST | Add multiple wallets |
| `/leaderboard/pnl` | GET | View PnL leaderboard |
| `/leaderboard/roi` | GET | View ROI leaderboard |
| `/leaderboard/win-rate` | GET | View Win Rate leaderboard |

