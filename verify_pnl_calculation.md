# PnL Calculation Verification Guide

## Step-by-Step Guide to Verify Data Accuracy

### Step 1: Verify Raw Data Sources

#### 1.1 Check Positions Data
```bash
# Query positions directly from API
curl "https://data-api.polymarket.com/positions?user=0x17db3fcd93ba12d38382a0cade24b200185c5f6d&limit=50" | jq '.[0]'
```

**Verify:**
- `initialValue` = amount initially invested
- `currentValue` = current value of position
- `cashPnl` = total PnL (realized + unrealized)
- `realizedPnl` = realized PnL (for closed positions)
- For closed positions: `currentValue = 0` and `cashPnl = realizedPnl`
- For open positions: `currentValue > 0` and `cashPnl = realizedPnl + unrealizedPnl`

#### 1.2 Check Trades Data
```bash
# Query trades directly from API
curl "https://data-api.polymarket.com/trades?user=0x17db3fcd93ba12d38382a0cade24b200185c5f6d" | jq '.[0:5]'
```

**Verify:**
- Each trade has `size` (shares) and `price` (price per share)
- Stake = `size * price` (money invested in that trade)
- Check if trades have `pnl` calculated (may be null)

#### 1.3 Check Activities Data
```bash
# Query activities directly from API
curl "https://data-api.polymarket.com/activity?user=0x17db3fcd93ba12d38382a0cade24b200185c5f6d" | jq '.[] | select(.type == "REWARD" or .type == "REDEEM")'
```

**Verify:**
- `REWARD` activities have positive `usdcSize`
- `REDEEM` activities have positive `usdcSize`

---

### Step 2: Manual Calculation Verification

#### 2.1 Calculate Total Invested (from Positions)
```python
total_invested = sum(position['initialValue'] for position in positions)
```

**Expected:** Should match `total_invested` in response

#### 2.2 Calculate Total Current Value (from Positions)
```python
total_current_value = sum(position['currentValue'] for position in positions)
```

**Expected:** Should match `total_current_value` in response

#### 2.3 Calculate Realized PnL (from Positions)
```python
total_realized_pnl = sum(position['realizedPnl'] for position in positions)
```

**Expected:** Should match `total_realized_pnl` in response

#### 2.4 Calculate Unrealized PnL (from Positions)
```python
# For each position: unrealized = cashPnl - realizedPnl
total_unrealized_pnl = sum(
    position['cashPnl'] - position['realizedPnl'] 
    for position in positions
)
```

**Expected:** Should match `total_unrealized_pnl` in response

**Note:** For closed positions, `cashPnl = realizedPnl`, so unrealized = 0 ✓

#### 2.5 Calculate Rewards and Redemptions
```python
total_rewards = sum(
    activity['usdcSize'] 
    for activity in activities 
    if activity['type'] == 'REWARD'
)

total_redemptions = sum(
    activity['usdcSize'] 
    for activity in activities 
    if activity['type'] == 'REDEEM'
)

```
**Expected:** Should match `total_rewards` and `total_redemptions` in response

#### 2.6 Calculate Total PnL
```python
total_pnl = total_realized_pnl + total_unrealized_pnl + total_rewards - total_redemptions
```

**Expected:** Should match `total_pnl` in response

#### 2.7 Calculate PnL Percentage
```python
pnl_percentage = (total_pnl / total_invested) * 100 if total_invested > 0 else 0
```

**Expected:** Should match `pnl_percentage` in response

---

### Step 3: Verify Trade-Based Metrics

#### 3.1 Calculate Total Stakes
```python
total_stakes = sum(trade['size'] * trade['price'] for trade in trades)
```

**Expected:** Should match `key_metrics.total_stakes` in response

#### 3.2 Calculate Total Trade PnL
```python
total_trade_pnl = sum(trade['pnl'] for trade in trades if trade.get('pnl') is not None)
```

**Expected:** Should match `key_metrics.total_trade_pnl` in response

**⚠️ WARNING:** If trades don't have `pnl` calculated, this will be 0!

#### 3.3 Calculate ROI
```python
roi = (total_trade_pnl / total_stakes) * 100 if total_stakes > 0 else 0
```

**Expected:** Should match `key_metrics.roi` in response

#### 3.4 Calculate Win Rate
```python
trades_with_pnl = [t for t in trades if t.get('pnl') is not None]
winning_trades = [t for t in trades_with_pnl if t['pnl'] > 0]

win_rate = (len(winning_trades) / len(trades_with_pnl)) * 100 if trades_with_pnl else 0
```

**Expected:** Should match `key_metrics.win_rate` in response

#### 3.5 Calculate Stake-Weighted Win Rate
```python
stakes_of_wins = sum(
    trade['size'] * trade['price'] 
    for trade in trades 
    if trade.get('pnl') is not None and trade['pnl'] > 0
)

stake_weighted_win_rate = (stakes_of_wins / total_stakes) * 100 if total_stakes > 0 else 0
```

**Expected:** Should match `key_metrics.stake_weighted_win_rate` in response

---

### Step 4: Cross-Verification Checks

#### 4.1 Position PnL vs Trade PnL
**Question:** Should `total_pnl` (from positions) equal `total_trade_pnl`?

**Answer:** Not necessarily! 
- Positions show current state (aggregated)
- Trades show individual transactions
- They may differ if:
  - Trades don't have PnL calculated
  - Some positions were created without corresponding trades
  - Trades were partially closed

#### 4.2 Total Invested Consistency
**Check:** `total_invested` should equal sum of all position `initialValue`

**Formula:**
```python
sum(position.initial_value for position in positions) == total_invested
```

#### 4.3 Unrealized PnL Check
**For each position:**
- If `current_value == 0` (closed): `unrealized_pnl` should be 0
- If `current_value > 0` (open): `unrealized_pnl = cash_pnl - realized_pnl`

**Verify:**
```python
for position in positions:
    if position.current_value == 0:
        assert position.cash_pnl == position.realized_pnl
        assert unrealized == 0
    else:
        assert position.cash_pnl == position.realized_pnl + unrealized
```

---

### Step 5: Potential Issues to Check

#### Issue 1: Trades Missing PnL Calculation
**Symptom:** `total_trade_pnl = 0` but there are trades

**Check:**
```python
trades_without_pnl = [t for t in trades if t.get('pnl') is None]
print(f"Trades without PnL: {len(trades_without_pnl)}")
```

**Solution:** Trades need PnL calculation (entry/exit price matching)

#### Issue 2: Double Counting
**Check:** Are we counting the same PnL twice?
- Positions already include realized + unrealized PnL
- Trades also calculate PnL
- These are separate metrics, not duplicates

#### Issue 3: Missing Data
**Check:** Are all positions/trades/activities fetched?
```python
# Compare counts
api_positions_count = len(api_positions)
db_positions_count = len(db_positions)
assert api_positions_count == db_positions_count
```

---

### Step 6: Automated Verification Script

Create a Python script to verify:

```python
import requests
from decimal import Decimal

def verify_pnl_calculation(wallet_address):
    # Fetch from API
    positions = requests.get(
        f"https://data-api.polymarket.com/positions?user={wallet_address}"
    ).json()
    
    trades = requests.get(
        f"https://data-api.polymarket.com/trades?user={wallet_address}"
    ).json()
    
    activities = requests.get(
        f"https://data-api.polymarket.com/activity?user={wallet_address}"
    ).json()
    
    # Calculate manually
    total_invested = sum(Decimal(str(p['initialValue'])) for p in positions)
    total_current_value = sum(Decimal(str(p['currentValue'])) for p in positions)
    total_realized_pnl = sum(Decimal(str(p['realizedPnl'])) for p in positions)
    total_unrealized_pnl = sum(
        Decimal(str(p['cashPnl'])) - Decimal(str(p['realizedPnl']))
        for p in positions
    )
    
    total_rewards = sum(
        Decimal(str(a['usdcSize']))
        for a in activities
        if a.get('type') == 'REWARD'
    )
    
    total_redemptions = sum(
        Decimal(str(a['usdcSize']))
        for a in activities
        if a.get('type') == 'REDEEM'
    )
    
    total_pnl = total_realized_pnl + total_unrealized_pnl + total_rewards - total_redemptions
    pnl_percentage = (total_pnl / total_invested * 100) if total_invested > 0 else 0
    
    # Trade metrics
    total_stakes = sum(
        Decimal(str(t['size'])) * Decimal(str(t['price']))
        for t in trades
    )
    
    trades_with_pnl = [t for t in trades if t.get('pnl') is not None]
    total_trade_pnl = sum(Decimal(str(t['pnl'])) for t in trades_with_pnl)
    
    winning_trades = [t for t in trades_with_pnl if Decimal(str(t['pnl'])) > 0]
    win_rate = (len(winning_trades) / len(trades_with_pnl) * 100) if trades_with_pnl else 0
    
    stakes_of_wins = sum(
        Decimal(str(t['size'])) * Decimal(str(t['price']))
        for t in winning_trades
    )
    stake_weighted_win_rate = (stakes_of_wins / total_stakes * 100) if total_stakes > 0 else 0
    
    roi = (total_trade_pnl / total_stakes * 100) if total_stakes > 0 else 0
    
    # Print results
    print("Manual Calculation Results:")
    print(f"  Total Invested: {total_invested}")
    print(f"  Total Current Value: {total_current_value}")
    print(f"  Total Realized PnL: {total_realized_pnl}")
    print(f"  Total Unrealized PnL: {total_unrealized_pnl}")
    print(f"  Total Rewards: {total_rewards}")
    print(f"  Total Redemptions: {total_redemptions}")
    print(f"  Total PnL: {total_pnl}")
    print(f"  PnL Percentage: {pnl_percentage}%")
    print(f"\nTrade Metrics:")
    print(f"  Total Stakes: {total_stakes}")
    print(f"  Total Trade PnL: {total_trade_pnl}")
    print(f"  ROI: {roi}%")
    print(f"  Win Rate: {win_rate}%")
    print(f"  Stake-Weighted Win Rate: {stake_weighted_win_rate}%")
    
    return {
        'total_invested': float(total_invested),
        'total_current_value': float(total_current_value),
        'total_realized_pnl': float(total_realized_pnl),
        'total_unrealized_pnl': float(total_unrealized_pnl),
        'total_rewards': float(total_rewards),
        'total_redemptions': float(total_redemptions),
        'total_pnl': float(total_pnl),
        'pnl_percentage': float(pnl_percentage),
        'key_metrics': {
            'total_stakes': float(total_stakes),
            'total_trade_pnl': float(total_trade_pnl),
            'roi': float(roi),
            'win_rate': float(win_rate),
            'stake_weighted_win_rate': float(stake_weighted_win_rate),
        }
    }

# Compare with API response
api_result = verify_pnl_calculation("0x17db3fcd93ba12d38382a0cade24b200185c5f6d")
# Compare api_result with your endpoint response
```

---

### Step 7: Common Calculation Errors

#### Error 1: Incorrect Unrealized PnL
**Wrong:**
```python
unrealized = position.current_value - position.initial_value  # ❌
```

**Correct:**
```python
unrealized = position.cash_pnl - position.realized_pnl  # ✅
```

#### Error 2: Double Counting Rewards
**Wrong:**
```python
total_pnl = total_realized_pnl + total_unrealized_pnl + total_rewards  # ❌ Missing redemptions
```

**Correct:**
```python
total_pnl = total_realized_pnl + total_unrealized_pnl + total_rewards - total_redemptions  # ✅
```

#### Error 3: Using Wrong Stake Calculation
**Wrong:**
```python
stake = trade.size  # ❌ Missing price
```

**Correct:**
```python
stake = trade.size * trade.price  # ✅
```

---

## Summary: Is the Calculation Correct?

### ✅ Correct Calculations:
1. **Total Invested** - Sum of `initial_value` from positions ✓
2. **Total Current Value** - Sum of `current_value` from positions ✓
3. **Realized PnL** - Sum of `realized_pnl` from positions ✓
4. **Unrealized PnL** - `cash_pnl - realized_pnl` for each position ✓
5. **Total PnL** - `realized + unrealized + rewards - redemptions` ✓
6. **PnL Percentage** - `(total_pnl / total_invested) * 100` ✓
7. **ROI** - `(total_trade_pnl / total_stakes) * 100` ✓
8. **Win Rate** - `(winning_trades / total_trades_with_pnl) * 100` ✓
9. **Stake-Weighted Win Rate** - `(stakes_of_wins / total_stakes) * 100` ✓

### ⚠️ Potential Issues:
1. **Trade PnL may be 0** if trades don't have `pnl` field calculated
2. **Total trade PnL ≠ Total position PnL** - These are different metrics (trades vs positions)
3. **Missing trades** - If trades aren't fetched/saved, trade metrics will be 0

### 🔍 Verification Priority:
1. **High Priority:** Verify positions data (this is the main PnL source)
2. **Medium Priority:** Verify activities (rewards/redemptions)
3. **Low Priority:** Verify trade metrics (only if trades have PnL calculated)

