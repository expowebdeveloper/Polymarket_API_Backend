"""
Scoring Configuration - All configurable parameters for risk score and final rating calculations.

This module contains all configurable parameters that can be adjusted without refactoring core logic.
"""

from typing import Dict, Optional


class ScoringConfig:
    """
    Configuration class for scoring calculations.
    All parameters are configurable and can be changed without modifying core logic.
    """
    
    # ========== Final Rating Formula Weights ==========
    # Default weights (must sum to 1.0 for proper normalization)
    WEIGHT_W: float = 0.30  # Win-related score weight
    WEIGHT_R: float = 0.30  # ROI score weight
    WEIGHT_P: float = 0.30  # PnL score weight
    WEIGHT_RISK: float = 0.10  # Risk weight
    
    # Additional metric weights (for future extensibility)
    # Example: WEIGHT_WIN_RATE: float = 0.05
    # Add new weights here as needed
    
    @classmethod
    def get_all_weights(cls) -> Dict[str, float]:
        """
        Get all active weights as a dictionary.
        This allows easy iteration and validation.
        """
        weights = {
            'w': cls.WEIGHT_W,
            'r': cls.WEIGHT_R,
            'p': cls.WEIGHT_P,
            'risk': cls.WEIGHT_RISK,
        }
        # Add future weights here dynamically
        return weights
    
    @classmethod
    def validate_weights(cls) -> bool:
        """
        Validate that weights sum to approximately 1.0 (within tolerance).
        """
        total = sum(cls.get_all_weights().values())
        return abs(total - 1.0) < 0.01  # Allow small floating point errors
    
    # ========== Risk Score Configuration ==========
    
    # Base Risk Score Formula: Risk Score = |Worst Loss| / Total Stake
    # Output range: 0 → 1
    # Higher value = higher risk
    
    # Future Enhancement 1: Average of N Worst Losses
    # If enabled, use average of N worst losses instead of single worst loss
    RISK_USE_AVG_N_WORST: bool = False  # Set to True to enable
    RISK_N_WORST_LOSSES: int = 5  # Number of worst losses to average (configurable)
    
    # Future Enhancement 2: Minimum Activity Condition
    RISK_MIN_ACTIVITY_ENABLED: bool = False  # Set to True to enable
    RISK_MIN_TRADES_THRESHOLD: int = 10  # Minimum number of trades required (configurable)
    
    # Behavior when minimum activity not met:
    # Options: 'exclude', 'mark_insufficient', 'calculate_anyway'
    RISK_INSUFFICIENT_DATA_ACTION: str = 'exclude'  # 'exclude', 'mark_insufficient', 'calculate_anyway'
    
    # ========== Percentile Configuration ==========
    # Percentile ranges used for normalization (configurable, not hard-coded)
    
    PERCENTILE_LOWER: float = 1.0  # Lower percentile (e.g., 1%, 5%, 10%)
    PERCENTILE_UPPER: float = 99.0  # Upper percentile (e.g., 95%, 99%, 90%)
    
    # Future options:
    # PERCENTILE_LOWER: float = 10.0  # W10% - W90%
    # PERCENTILE_LOWER: float = 5.0   # W5% - W95%
    
    # ========== Population Filter Configuration ==========
    # Minimum trades required to be included in population statistics
    POPULATION_MIN_TRADES: int = 5
    
    # ========== Helper Methods ==========
    
    @classmethod
    def get_risk_config(cls) -> Dict:
        """Get risk score configuration as dictionary."""
        return {
            'use_avg_n_worst': cls.RISK_USE_AVG_N_WORST,
            'n_worst_losses': cls.RISK_N_WORST_LOSSES,
            'min_activity_enabled': cls.RISK_MIN_ACTIVITY_ENABLED,
            'min_trades_threshold': cls.RISK_MIN_TRADES_THRESHOLD,
            'insufficient_data_action': cls.RISK_INSUFFICIENT_DATA_ACTION,
        }
    
    @classmethod
    def get_percentile_config(cls) -> Dict:
        """Get percentile configuration as dictionary."""
        return {
            'lower': cls.PERCENTILE_LOWER,
            'upper': cls.PERCENTILE_UPPER,
        }


# Global configuration instance
scoring_config = ScoringConfig()








