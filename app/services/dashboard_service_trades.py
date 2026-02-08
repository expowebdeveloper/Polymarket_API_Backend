from typing import Dict, Any, List
from datetime import datetime, timedelta

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


def _build_light_pnl_from_trades(trades: List[Dict], filter_type: str) -> List[Dict]:
    """
    Build a minimal PnL-like series from recent trades for fast initial load.
    Used only when filter != "all" to avoid calling the slow user-pnl API.
    Each point is (t=timestamp, p=cumulative volume as proxy for activity).
    """
    if not trades:
        return []
    # Sort by timestamp ascending
    sorted_trades = sorted(trades, key=lambda x: x.get("timestamp") or 0)
    points = []
    cumulative = 0.0
    for t in sorted_trades:
        ts = t.get("timestamp") or 0
        size = float(t.get("size") or 0)
        price = float(t.get("price") or 0)
        value = size * price
        cumulative += value
        points.append({"t": ts, "p": round(cumulative, 2)})
    # Apply same period filter for consistency
    now_sec = int(datetime.utcnow().timestamp())
    if filter_type == "recent10":
        points = points[-10:]
    elif filter_type == "7days":
        cutoff = now_sec - (7 * 24 * 3600)
        points = [p for p in points if (p.get("t") or 0) >= cutoff]
    elif filter_type == "30days":
        cutoff = now_sec - (30 * 24 * 3600)
        points = [p for p in points if (p.get("t") or 0) >= cutoff]
    return points


async def get_filtered_trades(wallet_address: str, filter_type: str = "all") -> Dict[str, Any]:
    """
    Fetch trade/PnL history for a wallet. Fast path for recent10/7days/30days (no heavy PnL API).
    Full PnL API is called only when filter_type == "all" for accurate graph.
    """
    from app.services.data_fetcher import fetch_user_pnl, fetch_user_trades

    if filter_type != "all":
        # Fast path: one page of trades only (no slow user-pnl API)
        limit = 100 if filter_type == "30days" else (50 if filter_type == "7days" else 15)
        trades_data = await fetch_user_trades(wallet_address, limit=limit)
        pnl_data = _build_light_pnl_from_trades(trades_data or [], filter_type)
        return {
            "trades": pnl_data,
            "count": len(pnl_data),
            "filter": filter_type,
            "data_origin": {
                "live": True,
                "source": "Polymarket Trades API (light)",
            },
        }

    # Full path: only when user clicks "All Trades"
    user_pnl = await fetch_user_pnl(wallet_address)
    full_pnl = list(user_pnl) if user_pnl else []
    pnl_data = sorted(full_pnl, key=lambda x: x.get("t") or 0)

    return {
        "trades": pnl_data,
        "count": len(pnl_data),
        "filter": filter_type,
        "data_origin": {
            "live": True,
            "source": "Polymarket User PnL API",
        },
    }
