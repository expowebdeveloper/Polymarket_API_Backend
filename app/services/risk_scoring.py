"""
Risk Score Calculation from Equity Curve

Calculates risk score based on Maximum Drawdown (MDD) from portfolio equity curve.

Formula: RiskScore = 100 · e^(-k·MDD) where k = 2.1

Steps:
1. Track running peak (highest portfolio value seen so far)
2. Calculate drawdown at each step: DD_t = (Peak_t - Equity_t) / Peak_t
3. Find Maximum Drawdown: MDD = max(DD_1, DD_2, ..., DD_n)
4. Calculate Risk Score: 100 · e^(-2.1·MDD)

Output:
- Higher score = Lower risk (less drawdown)
- Lower score = Higher risk (more drawdown)
- Range: 0-100
"""

import math
from typing import List, Optional, Dict


def calculate_risk_score_from_equity(equity: List[float], k: float = 2.1) -> Optional[float]:
    """
    Calculate risk score from equity curve using Maximum Drawdown.
    
    Args:
        equity: List of portfolio values after each trade (chronological order)
        k: Risk severity factor (default: 2.1)
    
    Returns:
        Risk score (0-100) or None if invalid input
        
    Examples:
        >>> calculate_risk_score_from_equity([1000, 1200, 900, 1100, 1400])
        59.2
        >>> calculate_risk_score_from_equity([1000, 1100, 1200, 1300])  # No drawdown
        100.0
        >>> calculate_risk_score_from_equity([1000, 500])  # 50% drawdown
        34.99
    """
    # Validate input
    if not equity or not isinstance(equity, list) or len(equity) < 2:
        return None
    
    # Filter out invalid values
    valid_equity = [e for e in equity if e is not None and e > 0]
    if len(valid_equity) < 2:
        return None
    
    # Step 1 & 2: Calculate running peak and drawdowns
    peak = valid_equity[0]
    max_drawdown = 0.0
    
    for i in range(1, len(valid_equity)):
        # Update running peak
        peak = max(peak, valid_equity[i])
        
        # Calculate drawdown at this step
        drawdown = (peak - valid_equity[i]) / peak if peak > 0 else 0.0
        
        # Track maximum drawdown
        max_drawdown = max(max_drawdown, drawdown)
    
    # Step 4: Calculate risk score using exponential decay
    # RiskScore = 100 · e^(-k·MDD)
    risk_score = 100.0 * math.exp(-k * max_drawdown)
    
    return round(risk_score, 2)


def calculate_risk_score_with_details(equity: List[float], k: float = 2.1) -> Optional[Dict]:
    """
    Calculate risk score with detailed breakdown of the calculation.
    
    Args:
        equity: List of portfolio values after each trade
        k: Risk severity factor (default: 2.1)
    
    Returns:
        Dictionary with:
        - risk_score: Final risk score (0-100)
        - max_drawdown: Maximum drawdown (0-1)
        - max_drawdown_percent: Maximum drawdown as percentage
        - peak_value: Highest portfolio value achieved
        - trough_value: Lowest value during max drawdown
        - drawdown_history: List of drawdowns at each step
        - peak_history: List of running peaks
    """
    if not equity or not isinstance(equity, list) or len(equity) < 2:
        return None
    
    # Filter out invalid values
    valid_equity = [e for e in equity if e is not None and e > 0]
    if len(valid_equity) < 2:
        return None
    
    # Track history
    peak_history = []
    drawdown_history = []
    
    peak = valid_equity[0]
    max_drawdown = 0.0
    max_dd_peak = valid_equity[0]
    max_dd_trough = valid_equity[0]
    
    for i in range(len(valid_equity)):
        # Update running peak
        peak = max(peak, valid_equity[i])
        peak_history.append(peak)
        
        # Calculate drawdown
        drawdown = (peak - valid_equity[i]) / peak if peak > 0 else 0.0
        drawdown_history.append(drawdown)
        
        # Track maximum drawdown and its peak/trough
        if drawdown > max_drawdown:
            max_drawdown = drawdown
            max_dd_peak = peak
            max_dd_trough = valid_equity[i]
    
    # Calculate risk score
    risk_score = 100.0 * math.exp(-k * max_drawdown)
    
    return {
        "risk_score": round(risk_score, 2),
        "max_drawdown": round(max_drawdown, 4),
        "max_drawdown_percent": round(max_drawdown * 100, 2),
        "peak_value": max_dd_peak,
        "trough_value": max_dd_trough,
        "drawdown_history": [round(dd, 4) for dd in drawdown_history],
        "peak_history": peak_history,
        "num_trades": len(valid_equity),
    }


def calculate_equity_curve_from_trades(initial_capital: float, pnl_history: List[float]) -> List[float]:
    """
    Calculate equity curve from PnL history.
    
    Args:
        initial_capital: Starting portfolio value
        pnl_history: List of profit/loss values for each trade
    
    Returns:
        List of portfolio values after each trade
        
    Example:
        >>> calculate_equity_curve_from_trades(1000, [200, -300, 200, 300])
        [1000, 1200, 900, 1100, 1400]
    """
    equity = [initial_capital]
    
    for pnl in pnl_history:
        new_equity = equity[-1] + pnl
        equity.append(new_equity)
    
    return equity


def calculate_risk_score_from_pnl(
    initial_capital: float, 
    pnl_history: List[float], 
    k: float = 2.1
) -> Optional[float]:
    """
    Calculate risk score directly from PnL history.
    
    Args:
        initial_capital: Starting portfolio value
        pnl_history: List of profit/loss values for each trade
        k: Risk severity factor (default: 2.1)
    
    Returns:
        Risk score (0-100) or None if invalid
        
    Example:
        >>> calculate_risk_score_from_pnl(1000, [200, -300, 200, 300])
        59.2
    """
    if not pnl_history or initial_capital <= 0:
        return None
    
    # Build equity curve
    equity = calculate_equity_curve_from_trades(initial_capital, pnl_history)
    
    # Calculate risk score
    return calculate_risk_score_from_equity(equity, k)
