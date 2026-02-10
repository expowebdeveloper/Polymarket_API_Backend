#!/usr/bin/env python3
"""
Fetch top 20 biggest winners of the month from Polymarket API, then enrich each
with all-time scoring (final score, win rate, stake yield) from profile-stat logic.

Usage (from backend directory):
    python biggest_winners_with_scoring.py

Output: Prints table to stdout and optionally writes JSON to biggest_winners_with_scoring.json
"""

import asyncio
import json
import os
import sys
from typing import Any, Dict, List, Optional

# Run from backend directory so app is importable
if __name__ == "__main__":
    backend_dir = os.path.dirname(os.path.abspath(__file__))
    if backend_dir not in sys.path:
        sys.path.insert(0, backend_dir)
    os.chdir(backend_dir)


async def fetch_biggest_winners_of_month(limit: int = 20) -> List[Dict[str, Any]]:
    """Get top N biggest winners of the month from Polymarket leaderboard API."""
    from app.services.data_fetcher import fetch_biggest_winners_of_month as _fetch
    return await _fetch(limit=limit)


RATE_LIMIT_DELAY = 2.5  # seconds between starting each wallet (avoid Polymarket block)


async def get_scoring_for_wallet(wallet: str, index: int, semaphore: asyncio.Semaphore) -> Dict[str, Any]:
    """
    Get all-time scoring and all-time PnL for a wallet (full fetch, no limit).
    Rate-limited: staggered start + semaphore 2.
    """
    await asyncio.sleep(index * RATE_LIMIT_DELAY)
    async with semaphore:
        try:
            from app.services.dashboard_service import get_profile_stat_data
            data = await asyncio.wait_for(
                get_profile_stat_data(wallet, force_refresh=True, skip_trades=True),
                timeout=90.0,
            )
            metrics = data.get("scoring_metrics") or data.get("metrics") or {}
            leaderboard = data.get("leaderboard") or {}
            all_time_pnl = metrics.get("total_pnl") or leaderboard.get("pnl")
            if all_time_pnl is not None:
                all_time_pnl = float(all_time_pnl)
            return {
                "final_score": metrics.get("final_score"),
                "win_rate": metrics.get("win_rate"),
                "stake_yield": metrics.get("roi"),
                "total_trades": metrics.get("total_trades"),
                "all_time_pnl": all_time_pnl,
            }
        except asyncio.TimeoutError:
            return {"final_score": None, "win_rate": None, "stake_yield": None, "total_trades": None, "all_time_pnl": None}
        except Exception as e:
            print(f"  [skip] {wallet[:10]}…: {e}", file=sys.stderr)
            return {"final_score": None, "win_rate": None, "stake_yield": None, "total_trades": None, "all_time_pnl": None}


async def main() -> List[Dict[str, Any]]:
    limit = 20
    print(f"Fetching top {limit} biggest winners of the month from Polymarket API…")
    winners = await fetch_biggest_winners_of_month(limit=limit)
    if not winners:
        print("No winners returned from API.")
        return []

    # Rate limit: 2 concurrent, 2.5s stagger between each (get full data without block)
    semaphore = asyncio.Semaphore(2)
    tasks = [get_scoring_for_wallet(w.get("user") or "", i, semaphore) for i, w in enumerate(winners)]
    scoring_list = await asyncio.gather(*tasks, return_exceptions=True)

    records: List[Dict[str, Any]] = []
    for i, w in enumerate(winners):
        wallet = w.get("user") or ""
        sc = scoring_list[i] if not isinstance(scoring_list[i], BaseException) else {}
        if isinstance(scoring_list[i], BaseException):
            sc = {}
        record = {
            "rank": w.get("rank") or (i + 1),
            "user": wallet,
            "userName": w.get("userName"),
            "xUsername": w.get("xUsername"),
            "profileImage": w.get("profileImage"),
            "pnl": w.get("pnl"),
            "all_time_pnl": sc.get("all_time_pnl"),
            "vol": w.get("vol"),
            "final_score": sc.get("final_score"),
            "win_rate": sc.get("win_rate"),
            "stake_yield": sc.get("stake_yield"),
            "total_trades": sc.get("total_trades"),
        }
        records.append(record)

    # Print table
    print()
    print("Top 20 Biggest Winners of the Month (with all-time scoring)")
    print("-" * 118)
    print(f"{'Rank':<5} {'Handle':<20} {'PnL(month)':>12} {'All-time PnL':>14} {'Final':>8} {'Win%':>8} {'Stake yield':>12}")
    print("-" * 118)
    for r in records:
        handle = (r.get("xUsername") or r.get("userName") or r.get("user") or "")[:20]
        if not handle and r.get("user"):
            handle = f"{r['user'][:6]}…{r['user'][-4:]}"
        pnl = r.get("pnl")
        pnl_str = f"+${pnl:,.0f}" if pnl is not None and pnl >= 0 else f"${pnl:,.0f}" if pnl is not None else "—"
        atp = r.get("all_time_pnl")
        atp_str = f"+${atp:,.0f}" if atp is not None and atp >= 0 else f"${atp:,.0f}" if atp is not None else "—"
        fs = r.get("final_score")
        wr = r.get("win_rate")
        sy = r.get("stake_yield")
        print(
            f"{r.get('rank', 0):<5} {handle:<20} {pnl_str:>12} {atp_str:>14} "
            f"{(f'{fs:.1f}' if fs is not None else '—'):>8} "
            f"{(f'{wr:.1f}%' if wr is not None else '—'):>8} "
            f"{(f'{sy:+.1f}%' if sy is not None else '—'):>12}"
        )
    print("-" * 118)
    print("PnL(month)=leaderboard API. All-time PnL & scoring from full profile-stat (all positions).")
    print()

    # Write JSON to backend/data/ (same path the 12h scheduler uses)
    _backend_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(_backend_dir, "data")
    os.makedirs(data_dir, exist_ok=True)
    out_path = os.path.join(data_dir, "biggest_winners_with_scoring.json")
    with open(out_path, "w") as f:
        json.dump(records, f, indent=2)
    print(f"Wrote {len(records)} records to {out_path}")

    return records


if __name__ == "__main__":
    records = asyncio.run(main())
    sys.exit(0 if records else 1)
