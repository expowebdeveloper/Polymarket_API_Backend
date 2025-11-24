# Polymarket Analytics Platform - Phase 1

A FastAPI application for fetching trader data from Polymarket's authenticated API and computing performance scores.

## Project Structure

- **`data_fetcher.py`**: Handles authenticated Polymarket API calls and data fetching
- **`scoring_engine.py`**: Computes all metrics and final score
- **`main.py`**: FastAPI server with CLI validation mode

## Features

- Authenticated Polymarket API integration
- Fetch resolved markets and wallet trades
- Calculate comprehensive performance metrics:
  - Total Positions & Active Positions
  - Total Wins & Total Losses
  - Win Rate (percentage)
  - Overall PnL
  - ROI (Return on Investment)
  - Consistency (weighted average of last 10 trades)
  - Recency (% of trades in last 7 days)
  - Final Score (weighted combination of all metrics)
- Category breakdown (e.g., Sports, Politics, etc.)
- CLI mode for validation
- REST API endpoint for analytics

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

## Running the Application

### CLI Mode (Validation)

Run the script directly to validate with the target wallet:
```bash
python main.py
```

This will:
- Fetch resolved markets
- Fetch trades for wallet `0x56687bf447db6ffa42ffe2204a05edaa20f55839`
- Calculate and display metrics in console format

### API Server Mode

Start the FastAPI server:
```bash
python main.py --server
```

Or using uvicorn directly:
```bash
uvicorn main:app --reload
```

The API will be available at `http://localhost:8000`

## API Endpoints

### `GET /`
Root endpoint with API information.

### `GET /analytics?wallet=<WALLET_ADDRESS>`
Get analytics for a specific wallet address.

**Example:**
```
GET /analytics?wallet=0x56687bf447db6ffa42ffe2204a05edaa20f55839
```

**Response (JSON):**
```json
{
  "wallet_id": "0x56687bf447db6ffa42ffe2204a05edaa20f55839",
  "total_positions": 14,
  "active_positions": 0,
  "total_wins": 12685747.18,
  "total_losses": -32326485.30,
  "win_rate_percent": 58.3,
  "win_count": 7,
  "loss_count": 5,
  "pnl": -19640738.12,
  "current_value": 0.01,
  "final_score": 42.6,
  "categories": {
    "Sports": {
      "total_wins": 12685747.18,
      "total_losses": -32326485.30,
      "win_rate_percent": 58.3,
      "pnl": -19640738.12
    }
  }
}
```

**Error Responses:**
- `400`: Invalid wallet address format
- `500`: API fetch or calculation error

## Scoring Algorithm

The final score is calculated as:
```
final_score = 0.4 * (roi/100) + 0.3 * (win_rate/100) + 0.2 * consistency + 0.1 * recency
```

Then scaled to 0-100.

### Metrics Explained

- **Total Positions**: Number of unique markets traded
- **Active Positions**: Number of unresolved markets
- **Total Wins**: Sum of all winning trade profits
- **Total Losses**: Sum of all losing trade losses (negative)
- **Win Rate**: (Winning trades / Total trades) × 100
- **Overall PnL**: Total Wins + Total Losses
- **ROI**: (Total Profit / Total Volume) × 100
- **Consistency**: Weighted average of last 10 trades (weights: 10, 9, 8, ..., 1)
- **Recency**: Percentage of trades in the last 7 days

### Trade Profit/Loss Calculation

- **Winning trade**: `profit = size * (1 - price)`
- **Losing trade**: `loss = -size`

## API Documentation

Once the server is running, visit:
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

## Authentication

The application uses authenticated Polymarket API requests with:
- API Key
- Secret
- Passphrase

Credentials are configured in `data_fetcher.py`.

## Notes

- Only resolved markets are used for win/loss determination
- API pagination is handled automatically
- Category grouping is performed when market data includes category information
- Falls back to Dome API if Polymarket API fails (1 QPS limit on free tier)
- Wallet address validation: must be 42 characters starting with "0x"

## Validation

The CLI mode validates against the target wallet:
`0x56687bf447db6ffa42ffe2204a05edaa20f55839`

Expected output format matches the console output specification with:
- Overall metrics
- Category breakdown (if available)
- Final score
