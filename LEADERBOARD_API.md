# Leaderboard API Documentation

## Overview

Three leaderboard endpoints are available to rank traders by different metrics:
1. **Total PnL** - Ranked by total profit and loss
2. **ROI** - Ranked by Return on Investment percentage
3. **Win Rate** - Ranked by win rate percentage

All leaderboards support time period filtering: **7-Day**, **30-Day**, and **All-Time**.

---

## Endpoints

### 1. Leaderboard by Total PnL

**Endpoint:** `GET /leaderboard/pnl`

**Description:** Returns traders ranked by their total profit and loss (PnL).

**Query Parameters:**
- `period` (optional): Time period filter
  - `"7d"` - Last 7 days
  - `"30d"` - Last 30 days
  - `"all"` - All time (default)
- `limit` (optional): Maximum number of traders to return (default: 100, max: 1000)

**Example Requests:**
```bash
# All-time leaderboard
curl "http://127.0.0.1:8000/leaderboard/pnl"

# 30-day leaderboard
curl "http://127.0.0.1:8000/leaderboard/pnl?period=30d"

# 7-day leaderboard with limit
curl "http://127.0.0.1:8000/leaderboard/pnl?period=7d&limit=50"
```

**Response:**
```json
{
  "period": "30d",
  "metric": "pnl",
  "count": 50,
  "entries": [
    {
      "rank": 1,
      "wallet_address": "0x17db3fcd93ba12d38382a0cade24b200185c5f6d",
      "name": "Trader Name",
      "pseudonym": "trader_pseudonym",
      "profile_image": "https://example.com/image.png",
      "total_pnl": 10000.50,
      "roi": 15.5,
      "win_rate": 65.0,
      "total_trades": 100,
      "total_trades_with_pnl": 95,
      "winning_trades": 62,
      "total_stakes": 50000.0
    }
  ]
}
```

---

### 2. Leaderboard by ROI

**Endpoint:** `GET /leaderboard/roi`

**Description:** Returns traders ranked by their Return on Investment (ROI) percentage.

**Query Parameters:**
- `period` (optional): Time period filter (`"7d"`, `"30d"`, `"all"` - default)
- `limit` (optional): Maximum number of traders to return (default: 100, max: 1000)

**Example Requests:**
```bash
# All-time ROI leaderboard
curl "http://127.0.0.1:8000/leaderboard/roi"

# 30-day ROI leaderboard
curl "http://127.0.0.1:8000/leaderboard/roi?period=30d&limit=50"
```

**Response:** Same format as PnL leaderboard, but sorted by ROI.

---

### 3. Leaderboard by Win Rate

**Endpoint:** `GET /leaderboard/win-rate`

**Description:** Returns traders ranked by their win rate percentage.

**Query Parameters:**
- `period` (optional): Time period filter (`"7d"`, `"30d"`, `"all"` - default)
- `limit` (optional): Maximum number of traders to return (default: 100, max: 1000)

**Example Requests:**
```bash
# All-time win rate leaderboard
curl "http://127.0.0.1:8000/leaderboard/win-rate"

# 7-day win rate leaderboard
curl "http://127.0.0.1:8000/leaderboard/win-rate?period=7d"
```

**Response:** Same format as PnL leaderboard, but sorted by win rate.

---

## Response Fields

Each leaderboard entry contains:

- `rank`: Position in the leaderboard (1-based)
- `wallet_address`: Ethereum wallet address
- `name`: Trader's name (if available)
- `pseudonym`: Trader's pseudonym (if available)
- `profile_image`: Profile image URL (if available)
- `total_pnl`: Total profit and loss
- `roi`: Return on Investment percentage
- `win_rate`: Win rate percentage
- `total_trades`: Total number of trades
- `total_trades_with_pnl`: Number of trades with calculated PnL
- `winning_trades`: Number of winning trades
- `total_stakes`: Total amount staked/invested

---

## Time Period Filtering

### How It Works

- **7-Day (`7d`)**: Only includes trades and activities from the last 7 days
- **30-Day (`30d`)**: Only includes trades and activities from the last 30 days
- **All-Time (`all`)**: Includes all trades and activities (default)

### Filtering Logic

- **Trades**: Filtered by `timestamp` field
- **Activities**: Filtered by `timestamp` field
- **Positions**: Filtered by `updated_at` timestamp (positions updated within the period)

---

## Performance Notes

- Leaderboards calculate metrics for all traders in the database
- For large datasets, consider caching results
- The calculation may take a few seconds for many traders
- Only traders with trades are included in the leaderboard

---

## Error Handling

All endpoints return standard HTTP status codes:
- `200 OK`: Success
- `400 Bad Request`: Invalid parameters (e.g., invalid period value)
- `500 Internal Server Error`: Server error during calculation

---

## Example Usage

```python
import requests

# Get top 10 traders by PnL (all-time)
response = requests.get(
    "http://127.0.0.1:8000/leaderboard/pnl",
    params={"limit": 10}
)
data = response.json()

for entry in data["entries"]:
    print(f"#{entry['rank']}: {entry['wallet_address']} - PnL: ${entry['total_pnl']:,.2f}")

# Get top 20 traders by ROI (30-day)
response = requests.get(
    "http://127.0.0.1:8000/leaderboard/roi",
    params={"period": "30d", "limit": 20}
)
data = response.json()

for entry in data["entries"]:
    print(f"#{entry['rank']}: {entry['wallet_address']} - ROI: {entry['roi']:.2f}%")
```

---

## Notes

- Traders must have at least one trade to appear in leaderboards
- ROI leaderboard only includes traders with `total_stakes > 0`
- Win Rate leaderboard only includes traders with trades that have calculated PnL
- Rankings are calculated in real-time (not cached)
- Time filtering is based on trade/activity timestamps, not position creation dates

