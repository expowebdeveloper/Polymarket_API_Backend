from typing import Dict, Any

# Limit for lightweight activity-only fetch (faster than full profile-stat)
ACTIVITY_TRADES_LIMIT = 150


async def get_activities_only(wallet_address: str) -> Dict[str, Any]:
    """
    Fetch only trades and map to activities for the Activity tab.
    Much faster than full profile-stat refetch (no positions, closed positions, PnL, profile, etc.).
    """
    from app.services.data_fetcher import fetch_user_trades

    trades_data = await fetch_user_trades(wallet_address, limit=ACTIVITY_TRADES_LIMIT)
    activities = []
    for t in trades_data or []:
        act = {
            "type": "TRADE",
            "title": t.get("title") or t.get("market_slug") or "Market",
            "slug": t.get("market_slug"),
            "side": t.get("side"),
            "size": t.get("size"),
            "usdcSize": float(t.get("size") or 0) * float(t.get("price") or 0),
            "usdc_size": float(t.get("size") or 0) * float(t.get("price") or 0),
            "price": t.get("price"),
            "timestamp": t.get("timestamp"),
            "transactionHash": t.get("match_id") or t.get("transactionHash") or "",
            "transaction_hash": t.get("match_id") or t.get("transactionHash") or "",
            "asset": t.get("asset"),
            "outcome": t.get("outcome"),
        }
        activities.append(act)
    return {"activities": activities}


async def get_filtered_trades(wallet_address: str, filter_type: str = "all") -> Dict[str, Any]:
    """
    Fetch trade history for a wallet address. All filter options return full trade history
    (no time or count limit) so market distribution and trade history always show complete data.
    
    Args:
        wallet_address: Wallet address to fetch trades for
        filter_type: Filter type - "recent10", "7days", "30days", "all" (1year removed)
    
    Returns:
        Dictionary with trades list (always full history)
    """
    from app.services.data_fetcher import fetch_user_trades, fetch_user_pnl

    # Always fetch full trade history for all filters (market distribution and trade history use full data)
    trades_data = await fetch_user_trades(wallet_address, limit=None)
    user_pnl = await fetch_user_pnl(wallet_address)

    # Use full PnL series for chart/history (from Polymarket User PnL API — real data only)
    pnl_data = list(user_pnl) if user_pnl else []

    return {
        "trades": pnl_data,
        "count": len(pnl_data),
        "filter": filter_type,
        "data_origin": {
            "live": True,
            "source": "Polymarket User PnL API",
        },
    }
