"""
Confidence Score Calculation (Predictions-based)

Calculates trader confidence based on number of predictions/trades made.

Formula: Conf(Np) = 1 - exp(-(Np/16)^0.60)

Where:
- Np: Number of predictions/trades made by the trader
- 16: Scale constant (how quickly confidence increases)
- 0.60: Curvature exponent (controls diminishing returns)
- exp(x): Exponential function e^x

Properties:
- Low predictions → low confidence
- Confidence grows fast early → rewards active traders
- Growth slows at high predictions → prevents over-weighting whales
- Output always stays in [0, 1]
"""

import math
from typing import Optional


def calculate_confidence_score(
    num_predictions: int,
    scale: float = 16.0,
    exponent: float = 0.60
) -> float:
    """
    Calculate confidence score based on number of predictions/trades.
    
    Args:
        num_predictions: Number of predictions/trades made (Np)
        scale: Scale constant (default: 16.0)
        exponent: Curvature exponent for diminishing returns (default: 0.60)
    
    Returns:
        Confidence score between 0 and 1
        
    Examples:
        >>> calculate_confidence_score(10)
        0.5296
        >>> calculate_confidence_score(50)
        0.8621
        >>> calculate_confidence_score(100)
        0.9504
        >>> calculate_confidence_score(300)
        0.9970
    """
    if num_predictions < 0:
        return 0.0
    
    if num_predictions == 0:
        return 0.0
    
    # Step 1: Normalize prediction count
    # x = Np / scale
    x = num_predictions / scale
    
    # Step 2: Apply diminishing returns
    # y = x^exponent
    y = math.pow(x, exponent)
    
    # Step 3: Apply exponential decay
    # z = exp(-y)
    z = math.exp(-y)
    
    # Step 4: Final confidence score
    # Conf(Np) = 1 - z
    confidence = 1.0 - z
    
    return round(confidence, 4)


def calculate_confidence_score_percent(
    num_predictions: int,
    scale: float = 16.0,
    exponent: float = 0.60
) -> float:
    """
    Calculate confidence score as percentage (0-100).
    
    Args:
        num_predictions: Number of predictions/trades made
        scale: Scale constant (default: 16.0)
        exponent: Curvature exponent (default: 0.60)
    
    Returns:
        Confidence score between 0 and 100
        
    Examples:
        >>> calculate_confidence_score_percent(10)
        52.96
        >>> calculate_confidence_score_percent(50)
        86.21
        >>> calculate_confidence_score_percent(100)
        95.04
    """
    confidence = calculate_confidence_score(num_predictions, scale, exponent)
    return round(confidence * 100, 2)


def get_confidence_level(confidence_score: float) -> str:
    """
    Get human-readable confidence level from score.
    
    Args:
        confidence_score: Confidence score (0-1)
    
    Returns:
        Confidence level description
    """
    if confidence_score >= 0.95:
        return "Very High"
    elif confidence_score >= 0.85:
        return "High"
    elif confidence_score >= 0.70:
        return "Moderate-High"
    elif confidence_score >= 0.50:
        return "Moderate"
    elif confidence_score >= 0.30:
        return "Low"
    else:
        return "Very Low"


def calculate_confidence_with_details(num_predictions: int) -> dict:
    """
    Calculate confidence score with detailed breakdown.
    
    Args:
        num_predictions: Number of predictions/trades made
    
    Returns:
        Dictionary with confidence score and details
    """
    confidence = calculate_confidence_score(num_predictions)
    confidence_percent = confidence * 100
    level = get_confidence_level(confidence)
    
    return {
        "num_predictions": num_predictions,
        "confidence_score": confidence,
        "confidence_percent": round(confidence_percent, 2),
        "confidence_level": level,
        "scale": 16.0,
        "exponent": 0.60,
    }
