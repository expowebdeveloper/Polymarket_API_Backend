"""
ROI Scoring Formula Implementation

Formula: ROI_score = (1 + tanh(sign(ROI) · ln(1 + |ROI|) / s_ROI)) / 2

Where:
- ROI: Return on investment in decimal form (10% → 0.10)
- sign(ROI): +1 if ROI > 0, -1 if ROI < 0, 0 if ROI = 0
- ln: Natural logarithm
- tanh: Hyperbolic tangent (outputs −1 → +1)
- s_ROI: Sensitivity parameter (default 0.6)

Output range: [0, 1]
- 0 = extremely bad ROI
- 0.5 = break-even
- 1 = extremely good ROI
"""

import math


def calculate_roi_score(roi: float, s_roi: float = 0.6) -> float:
    """
    Calculate ROI score using logarithmic compression and tanh saturation.
    
    Args:
        roi: Return on investment in decimal form (e.g., 0.10 for 10%, 1.0 for 100%)
        s_roi: Sensitivity parameter (default 0.6). Smaller = more aggressive curve, larger = flatter curve
    
    Returns:
        Score between 0 and 1
        
    Examples:
        >>> calculate_roi_score(0.10)  # 10% ROI
        0.579
        >>> calculate_roi_score(0.50)  # 50% ROI
        0.794
        >>> calculate_roi_score(1.00)  # 100% ROI
        0.910
        >>> calculate_roi_score(-0.10)  # -10% ROI
        0.421
        >>> calculate_roi_score(0.0)  # Break-even
        0.5
    """
    # Handle break-even case
    if roi == 0:
        return 0.5
    
    # Step 1: Get sign of ROI (+1 for profit, -1 for loss)
    sign = math.copysign(1, roi)
    
    # Step 2: Apply logarithmic compression to prevent explosion
    # ln(1 + |ROI|) compresses large ROIs with diminishing returns
    log_term = math.log(1 + abs(roi))
    
    # Step 3: Preserve direction and scale by sensitivity
    scaled = (sign * log_term) / s_roi
    
    # Step 4: Apply tanh for smooth S-curve saturation
    # tanh outputs between -1 and +1
    tanh_value = math.tanh(scaled)
    
    # Step 5: Normalize to 0-1 range
    # (1 + tanh) / 2 maps [-1, 1] → [0, 1]
    score = (1 + tanh_value) / 2
    
    return score


def calculate_roi_score_from_percentage(roi_percentage: float, s_roi: float = 0.6) -> float:
    """
    Calculate ROI score from percentage format.
    
    Args:
        roi_percentage: ROI as percentage (e.g., 10.0 for 10%, 100.0 for 100%)
        s_roi: Sensitivity parameter (default 0.6)
    
    Returns:
        Score between 0 and 1
        
    Examples:
        >>> calculate_roi_score_from_percentage(10.0)  # 10%
        0.579
        >>> calculate_roi_score_from_percentage(50.0)  # 50%
        0.794
        >>> calculate_roi_score_from_percentage(100.0)  # 100%
        0.910
    """
    # Convert percentage to decimal
    roi_decimal = roi_percentage / 100.0
    return calculate_roi_score(roi_decimal, s_roi)
