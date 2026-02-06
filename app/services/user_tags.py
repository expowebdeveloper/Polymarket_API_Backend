"""
User tagging service for PnL-based classification.

Tags are applied only when predictions < 30:
- $20 – $100 PnL → "Green Beginning" 🟢🌱
- $100 – $1,000 PnL → "Promising Start" 🚀✨
- $1,000 – $10,000 PnL → "Strong Debut" 💪🔥
- $10,000+ PnL → "Hot Start" 🔥🏁

Notes:
- Boundaries: inclusive on lower bound (>=), exclusive on upper bound (<), except last tier (>= 10k).
- If PnL < $20: no "new trader" tag.
"""

from typing import Optional, Dict


def calculate_user_tag(total_pnl: float, total_predictions: int) -> Optional[Dict[str, str]]:
    """
    Calculate user tag based on PnL and prediction count.
    
    Args:
        total_pnl: Total profit and loss
        total_predictions: Total number of predictions/trades
    
    Returns:
        Dictionary with tag info (title, emoji, style) or None if no tag applies
    """
    # Only apply tags when predictions < 30
    if total_predictions >= 30:
        return None
    
    # Apply PnL-based tags (inclusive lower, exclusive upper; last tier >= 10k)
    if total_pnl >= 10000:
        return {
            "title": "Hot Start",
            "emoji": "🔥🏁",
            "style": "bg-gradient-to-r from-red-500/10 to-orange-500/10 text-transparent bg-clip-text border-red-500/50 shadow-[0_0_20px_rgba(239,68,68,0.4)]"
        }
    if total_pnl >= 1000:  # 1000 <= PnL < 10000
        return {
            "title": "Strong Debut",
            "emoji": "💪🔥",
            "style": "bg-gradient-to-r from-orange-500/10 to-amber-500/10 text-transparent bg-clip-text border-orange-500/50 shadow-[0_0_20px_rgba(249,115,22,0.4)]"
        }
    if total_pnl >= 100:  # 100 <= PnL < 1000
        return {
            "title": "Promising Start",
            "emoji": "🚀✨",
            "style": "bg-gradient-to-r from-purple-500/10 to-pink-500/10 text-transparent bg-clip-text border-purple-500/50 shadow-[0_0_20px_rgba(168,85,247,0.4)]"
        }
    if total_pnl >= 20:  # 20 <= PnL < 100
        return {
            "title": "Green Beginning",
            "emoji": "🟢🌱",
            "style": "bg-gradient-to-r from-green-500/10 to-emerald-500/10 text-transparent bg-clip-text border-green-500/50 shadow-[0_0_20px_rgba(16,185,129,0.4)]"
        }
    
    # No tag if PnL < $20
    return None
