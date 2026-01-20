from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

async def get_filtered_trades(wallet_address: str, filter_type: str = "all") -> Dict[str, Any]:
    """
    Fetch filtered trade history for a wallet address.
    
    Args:
        wallet_address: Wallet address to fetch trades for
        filter_type: Filter type - "recent10", "7days", "30days", "1year", "all"
    
    Returns:
        Dictionary with trades list
    """
    from app.services.data_fetcher import fetch_user_trades, fetch_user_pnl
    
    # Determine limit based on filter
    limit_map = {
        "recent10": 10,
        "7days": None,  # Fetch all, filter by date
        "30days": None,
        "1year": None,
        "all": None
    }
    
    limit = limit_map.get(filter_type, None)
    
    # Fetch trades
    trades_data = await fetch_user_trades(wallet_address, limit=limit if filter_type == "recent10" else None)
    user_pnl = await fetch_user_pnl(wallet_address)
    
    # Filter by date if needed
    if filter_type in ["7days", "30days", "1year"] and trades_data:
        now = datetime.utcnow()
        cutoff_map = {
            "7days": now - timedelta(days=7),
            "30days": now - timedelta(days=30),
            "1year": now - timedelta(days=365)
        }
        cutoff = cutoff_map.get(filter_type)
        
        if cutoff:
            cutoff_timestamp = int(cutoff.timestamp())
            trades_data = [
                t for t in trades_data 
                if t.get("timestamp", 0) >= cutoff_timestamp
            ]
    
    # Convert trades to PnL format for graph
    pnl_data = []
    if user_pnl:
        # Filter user_pnl based on filter_type
        if filter_type == "recent10":
            pnl_data = user_pnl[-10:] if len(user_pnl) >= 10 else user_pnl
        elif filter_type in ["7days", "30days", "1year"]:
            now = datetime.utcnow()
            cutoff_map = {
                "7days": now - timedelta(days=7),
                "30days": now - timedelta(days=30),
                "1year": now - timedelta(days=365)
            }
            cutoff = cutoff_map.get(filter_type)
            if cutoff:
                cutoff_timestamp = int(cutoff.timestamp())
                pnl_data = [
                    p for p in user_pnl 
                    if p.get("t", 0) >= cutoff_timestamp
                ]
        else:  # all
            pnl_data = user_pnl
    
    return {
        "trades": pnl_data,
        "count": len(pnl_data),
        "filter": filter_type
    }
