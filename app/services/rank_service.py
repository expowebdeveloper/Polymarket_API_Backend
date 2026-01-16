def get_rank_title(volume_usd: float) -> str:
    """
    Assigns a rank title with emoji based on trading volume in USD.
    
    Rules:
    < $5,000 → 🦐 Shrimp
    $5,000 – $20,000 → 🦀 Crab
    $20,000 – $50,000 → 🐟 Fish
    $50,000 – $200,000 → 🐬 Young Dolphin
    $200,000 – $500,000 → 🐬 Dolphin
    $500,000 – $1,000,000 → 🦈 Shark
    $1,000,000 – $10,000,000 → 🐋 Whale
    $10,000,000 – $100,000,000 → 🐋🔥 Mega Whale
    $100,000,000+ → 🐋👑 Elite Whale
    
    Args:
        volume_usd: Trading volume in USD
        
    Returns:
        Rank title string with emoji
    """
    if volume_usd < 5000:
        return "🦐 Shrimp"
    elif volume_usd < 20000:
        return "🦀 Crab"
    elif volume_usd < 50000:
        return "🐟 Fish"
    elif volume_usd < 200000:
        return "🐬 Young Dolphin"
    elif volume_usd < 500000:
        return "🐬 Dolphin"
    elif volume_usd < 1000000:
        return "🦈 Shark"
    elif volume_usd < 10000000:
        return "🐋 Whale"
    elif volume_usd < 100000000:
        return "🐋🔥 Mega Whale"
    else:
        return "🐋👑 Elite Whale"


def get_streak_title(streak_count: int) -> str:
    """
    Assigns a streak title with emoji based on consecutive wins.
    
    Rules:
    ≥ 3 — Warm Streak — 🔥
    ≥ 5 — Hot Streak — 🔥🔥
    ≥ 8 — Blazing Streak — 🔥🚀
    ≥ 10 — Scorching Streak — 🔥⚡
    ≥ 15 — Inferno Streak — 🌋🔥
    ≥ 20 — Unstoppable Streak — 🧨👑
    ≥ 30 — Legendary Streak — 🐉🔥
    
    Args:
        streak_count: Number of consecutive wins
        
    Returns:
        Streak title string with emoji or None if < 3
    """
    if streak_count >= 30:
        return "Legendary Streak — 🐉🔥"
    elif streak_count >= 20:
        return "Unstoppable Streak — 🧨👑"
    elif streak_count >= 15:
        return "Inferno Streak — 🌋🔥"
    elif streak_count >= 10:
        return "Scorching Streak — 🔥⚡"
    elif streak_count >= 8:
        return "Blazing Streak — 🔥🚀"
    elif streak_count >= 5:
        return "Hot Streak — 🔥🔥"
    elif streak_count >= 3:
        return "Warm Streak — 🔥"
    else:
        return None
