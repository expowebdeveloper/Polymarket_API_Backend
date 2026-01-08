"""
<<<<<<< HEAD
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











=======
Configurable scoring configuration for leaderboard calculations.
All weights, percentiles, and thresholds are configurable.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class ScoringConfig:
    """
    Configuration class for scoring calculations.
    All values are configurable and can be adjusted without refactoring core logic.
    """
    
    # === Final Rating Formula Weights ===
    # Rating = 100 × [ wW · Wscore + wR · Rscore + wP · Pscore + wrisk · (1 − Risk Score) ]
    weight_win_rate: float = 0.30      # wW - Win Rate weight
    weight_roi: float = 0.30            # wR - ROI weight  
    weight_pnl: float = 0.30            # wP - PnL weight
    weight_risk: float = 0.10           # wrisk - Risk weight
    
    # === Percentile Ranges for Normalization ===
    # Used to compute score anchors (can be changed to 10%-90%, 5%-95%, etc.)
    percentile_lower: float = 1.0       # Lower percentile (e.g., 1%, 5%, 10%)
    percentile_upper: float = 99.0      # Upper percentile (e.g., 90%, 95%, 99%)
    
    # === Minimum Activity Threshold ===
    # Risk Score and final rating calculated only if trader meets minimum activity
    min_trades_threshold: int = 5       # Minimum number of trades required
    
    # === Risk Score Configuration ===
    # Risk Score = |Worst Loss| / Total Stake (output range: 0 → 1)
    risk_n_worst_losses: int = 1        # N for average of N worst losses (future enhancement)
                                        # If 1, uses single worst loss (current behavior)
    
    # === Shrinking Constants (for W, R, P calculations) ===
    shrink_kw: float = 50.0             # Shrink constant for Win Rate
    shrink_baseline_win_rate: float = 0.5  # Baseline Win Rate (50%)
    shrink_kr: float = 50.0             # Shrink constant for ROI
    shrink_kp: float = 50.0             # Shrink constant for PnL
    shrink_alpha: float = 4.0           # Whale penalty strength for PnL
    
    def validate(self) -> None:
        """Validate configuration values."""
        # Weights should sum to approximately 1.0 (allow small floating point errors)
        total_weight = self.weight_win_rate + self.weight_roi + self.weight_pnl + self.weight_risk
        if abs(total_weight - 1.0) > 0.01:
            raise ValueError(f"Weights must sum to 1.0, got {total_weight}")
        
        if not (0 <= self.percentile_lower < self.percentile_upper <= 100):
            raise ValueError(f"Percentiles must be in range [0, 100] with lower < upper")
        
        if self.min_trades_threshold < 0:
            raise ValueError("min_trades_threshold must be non-negative")
        
        if self.risk_n_worst_losses < 1:
            raise ValueError("risk_n_worst_losses must be >= 1")
    
    def get_weights_dict(self) -> dict:
        """Get weights as a dictionary for easy access."""
        return {
            'win_rate': self.weight_win_rate,
            'roi': self.weight_roi,
            'pnl': self.weight_pnl,
            'risk': self.weight_risk,
        }


# Default configuration instance
default_scoring_config = ScoringConfig()
>>>>>>> 7ffa6dd982d968bfe597ebd0f22d2268454ce1bc


