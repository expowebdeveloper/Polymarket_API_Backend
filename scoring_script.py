
import math

def clamp(n, minn, maxn):
    return max(min(n, maxn), minn)

def calculate_percentile_rank(values, score):
    """
    Calculate the percentile rank of a score within a list of values.
    Returns 0-1 range.
    """
    if not values:
        return 0.0
    
    # Sort values
    sorted_values = sorted(values)
    
    # Find position
    # This is a naive implementation. For exact percentiles as requested (1% and 99% anchors),
    # we need to find the values AT 1% and 99%.
    pass

def get_percentile_value(values, percentile):
    """
    Get the value at a specific percentile (0-100).
    percentile: float 0-100
    """
    if not values:
        return 0.0
    sorted_values = sorted(values)
    k = (len(sorted_values) - 1) * (percentile / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_values[int(k)]
    d0 = sorted_values[int(f)] * (c - k)
    d1 = sorted_values[int(c)] * (k - f)
    return d0 + d1

def calculate_scores_and_rank(traders_metrics):
    """
    Calculate advanced scores for a list of traders.
    """
    # Filter valid traders for population stats (>= 5 trades)
    population_metrics = [t for t in traders_metrics if t.get('total_trades', 0) >= 5]
    
    # If not enough data, use defaults or what we have
    if not population_metrics:
        # Fallback if no "active" traders, use everyone or defaults?
        # Using everyone if < 5 active traders might be safer to avoid crashes,
        # but the rule says "ignore traders with < 5 trades" for anchors.
        # If NO one has > 5 trades, we can't compute meaningful percentiles.
        # We will use defaults or skip scaling.
        population_metrics = traders_metrics 

    # --- Formula 1: Win Rate Score ---
    # Win Rates
    win_rates = [t.get('win_rate_shrunk', 0.0) for t in population_metrics] # Wait, we need to compute shrunk first? 
    # The request says:
    # Step 3: Shrink Win Rate. W_shrunk = ...
    # Step 4: Percentile Normalization using W_shrunk.
    
    # So we calculate raw metrics for everyone first, then W_shrunk for everyone, 
    # THEN collect W_shrunk for population to find 1% and 99% anchors.
    
    processed_traders = []
    
    # Pass 1: Calculate Indiv Shrunk Metrics
    for trader in traders_metrics:
        # Extract raw vars
        s_w = trader.get('winning_stakes', 0.0) # Sum stake of winning trades
        S = trader.get('total_stakes', 0.0)     # Total stakes
        sum_sq_s = trader.get('sum_sq_stakes', 0.0) # Sum of squared stakes
        
        # Win Rate - Step 1
        W = (s_w / S) if S > 0 else 0.0
        
        # Step 2: N_eff
        N_eff = 0.0
        if sum_sq_s > 0:
            N_eff = (S**2) / sum_sq_s
        
        b = 0.5
        kW = 50
        W_shrunk = (W * N_eff + b * kW) / (N_eff + kW)
        trader['W_shrunk'] = W_shrunk
        
        pass

    # ROI population
    rois_pop = [t.get('roi', 0.0) for t in population_metrics]
    roi_m = sorted(rois_pop)[len(rois_pop) // 2] if rois_pop else 0.0
    
    # PnL population - but wait, PnL score uses PnL_adj first
    # PnL_adj depends on max_s_i / S
    # So we compute PnL_adj for everyone, THEN find median of PnL_adj?
    # Logic: "Shrink Based on Reliability... PnL_shrunk = ... PnL_m ... "
    # So PnL_m is likely median of PnL_adj (or PnL_total? usually PnL_total or PnL_adj, let's assume PnL_adj for consistency if shrinking PnL_adj)
    # Actually formula says: PnL_shrunk = (PnL_adj * N_eff + PnL_m * kp) / ... 
    # PnL_m defined as "baseline/median PnL". Probably raw PnL? 
    # Given the flow, median of the thing being shrunk (PnL_adj) makes most sense to pull towards the 'typical' adjusted PnL.
    # However, "baseline/median PnL" usually implies the simple median of raw PnL.
    # Let's use Median of PnL_adj to be safe as it's the same unit.
    
    pnl_adjs_pop = []
    
    # Calculate PnL_adj for Population to get Median
    for t in population_metrics:
        pnl_total = t.get('total_pnl', 0.0)
        S = t.get('total_stakes', 0.0)
        max_s = t.get('max_stake', 0.0)
        alpha = 4.0
        
        ratio = 0.0
        if S > 0:
            ratio = max_s / S
            
        pnl_adj = pnl_total / (1 + alpha * ratio)
        pnl_adjs_pop.append(pnl_adj)
        
    pnl_m = sorted(pnl_adjs_pop)[len(pnl_adjs_pop) // 2] if pnl_adjs_pop else 0.0

    # Calculate Shrunk Values for ALL traders
    for t in traders_metrics:
        # Reuse N_eff, W_shrunk calculated? No, need to do it cleanly.
        
        # Vars
        S = t.get('total_stakes', 0.0)
        sum_sq_s = t.get('sum_sq_stakes', 0.0)
        N_eff = (S**2) / sum_sq_s if sum_sq_s > 0 else 0.0
        
        # --- Formula 1 ---
        s_w = t.get('winning_stakes', 0.0)
        W = (s_w / S) if S > 0 else 0.0
        b = 0.5
        kW = 50
        W_shrunk = (W * N_eff + b * kW) / (N_eff + kW)
        t['W_shrunk'] = W_shrunk
        
        # --- Formula 2 ---
        roi_raw = t.get('roi', 0.0)
        kR = 50
        roi_shrunk = (roi_raw * N_eff + roi_m * kR) / (N_eff + kR)
        t['roi_shrunk'] = roi_shrunk
        
        # --- Formula 3 ---
        pnl_total = t.get('total_pnl', 0.0)
        max_s = t.get('max_stake', 0.0)
        alpha = 4.0
        ratio = (max_s / S) if S > 0 else 0.0
        pnl_adj = pnl_total / (1 + alpha * ratio)
        
        kp = 50
        pnl_shrunk = (pnl_adj * N_eff + pnl_m * kp) / (N_eff + kp)
        t['pnl_shrunk'] = pnl_shrunk
        
        # --- Formula 4: Risk Score (Fixed Formula) ---
        # Risk Score = |Worst Loss| / Total Stake
        # Output range: 0 → 1, Higher value = higher risk
        # This formula is not percentile-based
        worst_loss = t.get('worst_loss', 0.0)
        total_stakes = S  # Use total_stakes (S) as Total Stake
        
        if total_stakes <= 0:
            risk_score = 0.0
        else:
            # Base Formula: Risk Score = |Worst Loss| / Total Stake
            risk_score = abs(worst_loss) / total_stakes
        
        # Clamp to 0-1 range (as per specification)
        t['risk_score'] = clamp(risk_score, 0, 1)
        t['score_risk'] = t['risk_score']  # Alias for consistency
        
    
    # Now Percentile Normalization for W, R, P
    # Collect Shrunk values from POPULATION (>=5 trades)
    
    w_shrunk_pop = [t['W_shrunk'] for t in population_metrics]
    roi_shrunk_pop = [t['roi_shrunk'] for t in population_metrics]
    pnl_shrunk_pop = [t['pnl_shrunk'] for t in population_metrics]
    
    # Anchors
    w_1 = get_percentile_value(w_shrunk_pop, 1)
    w_99 = get_percentile_value(w_shrunk_pop, 99)
    
    r_1 = get_percentile_value(roi_shrunk_pop, 1)
    r_99 = get_percentile_value(roi_shrunk_pop, 99)
    
    p_1 = get_percentile_value(pnl_shrunk_pop, 1)
    p_99 = get_percentile_value(pnl_shrunk_pop, 99)
    
    # Final clamping
    for t in traders_metrics:
        # W score
        if w_99 - w_1 != 0:
            w_score = (t['W_shrunk'] - w_1) / (w_99 - w_1)
        else:
            w_score = 0.5 # Fallback
        t['score_win_rate'] = clamp(w_score, 0, 1)
        
        # R score
        if r_99 - r_1 != 0:
            r_score = (t['roi_shrunk'] - r_1) / (r_99 - r_1)
        else:
            r_score = 0.5
        t['score_roi'] = clamp(r_score, 0, 1)
        
        # P score
        if p_99 - p_1 != 0:
            p_score = (t['pnl_shrunk'] - p_1) / (p_99 - p_1)
        else:
            p_score = 0.5
        t['score_pnl'] = clamp(p_score, 0, 1)
        
        # Risk score is already set
        t['score_risk'] = clamp(t['risk_score'], 0, 1) # Ensure 0-1
        
        # Final Score: Weighted combination using configurable weights
        # Rating = 100 × [ wW · Wscore + wR · Rscore + wP · Pscore + wrisk · (1 − Risk Score) ]
        # Default weights: wW = 0.30, wR = 0.30, wP = 0.30, wrisk = 0.10
        w_score = t.get('score_win_rate', 0.0)
        r_score = t.get('score_roi', 0.0)
        p_score = t.get('score_pnl', 0.0)
        risk_score = t.get('score_risk', 0.0)
        
        # Final Rating Formula
        # Rating = 100 × [ wW · Wscore + wR · Rscore + wP · Pscore + wrisk · (1 − Risk Score) ]
        final_score = 100.0 * (
            0.30 * w_score + 
            0.30 * r_score + 
            0.30 * p_score + 
            0.10 * (1.0 - risk_score)  # Fixed: use (1 - risk_score), not divided by 4
        )
        t['final_score'] = clamp(final_score, 0, 100)
        
    return traders_metrics

# --- Test Case ---
if __name__ == "__main__":
    # Mock Data
    traders = [
        {
            "id": 1,
            "total_trades": 10,
            "winning_stakes": 500,
            "total_stakes": 1000,
            "sum_sq_stakes": 100000, # 10 trades of 100 each. 10 * 100^2 = 100,000. N_eff = 1000^2 / 100000 = 10.
            "total_pnl": 200,
            "roi": 20.0,
            "max_stake": 100,
            "worst_loss": -50,
            "portfolio_value": 1200
        },
        {
            "id": 2, # Newbie
            "total_trades": 2,
            "winning_stakes": 200,
            "total_stakes": 200,
            "sum_sq_stakes": 20000, # 2 trades of 100. N_eff = 2.
            "total_pnl": 100,
            "roi": 50.0,
            "max_stake": 100,
            "worst_loss": 0,
            "portfolio_value": 300
        },
        {
            "id": 3, # Whale
            "total_trades": 20,
            "winning_stakes": 15000,
            "total_stakes": 20000,
            "sum_sq_stakes": 20000000, # 20 trades of 1000. 
            "total_pnl": 5000,
            "roi": 25.0,
            "max_stake": 1000,
            "worst_loss": -500,
            "portfolio_value": 25000
        }
    ]
    
    results = calculate_scores_and_rank(traders)
    for r in results:
        print(f"ID: {r['id']}, W: {r['score_win_rate']:.4f}, R: {r['score_roi']:.4f}, P: {r['score_pnl']:.4f}, Risk: {r['score_risk']:.4f}")
