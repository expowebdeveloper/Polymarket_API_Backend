"""
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


