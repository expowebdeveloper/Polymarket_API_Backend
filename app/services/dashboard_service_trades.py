from typing import Dict, Any

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

    # Use full PnL series for chart/history
    pnl_data = list(user_pnl) if user_pnl else []

    return {
        "trades": pnl_data,
        "count": len(pnl_data),
        "filter": filter_type
    }
