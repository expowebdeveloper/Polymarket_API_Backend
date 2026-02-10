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


async def get_scoring_for_wallet(wallet: str, semaphore: asyncio.Semaphore) -> Dict[str, Any]:
    """
    Get all-time scoring (final_score, win_rate, stake_yield/roi) for a wallet
    using the same profile-stat logic (positions → scoring).
    """
    async with semaphore:
        try:
            from app.services.dashboard_service import get_profile_stat_data
            data = await asyncio.wait_for(
                get_profile_stat_data(wallet, force_refresh=True, skip_trades=True),
                timeout=35.0,
            )
            metrics = data.get("scoring_metrics") or data.get("metrics") or {}
            return {
                "final_score": metrics.get("final_score"),
                "win_rate": metrics.get("win_rate"),
                "stake_yield": metrics.get("roi"),  # ROI = stake yield (all-time)
                "total_trades": metrics.get("total_trades"),
            }
        except asyncio.TimeoutError:
            return {"final_score": None, "win_rate": None, "stake_yield": None, "total_trades": None}
        except Exception as e:
            print(f"  [skip] {wallet[:10]}…: {e}", file=sys.stderr)
            return {"final_score": None, "win_rate": None, "stake_yield": None, "total_trades": None}


async def main() -> List[Dict[str, Any]]:
    limit = 20
    print(f"Fetching top {limit} biggest winners of the month from Polymarket API…")
    winners = await fetch_biggest_winners_of_month(limit=limit)
    if not winners:
        print("No winners returned from API.")
        return []

    # Enrich with scoring in parallel (max 5 concurrent profile-stat fetches to avoid rate limits)
    semaphore = asyncio.Semaphore(5)
    tasks = [get_scoring_for_wallet(w.get("user") or "", semaphore) for w in winners]
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
            "vol": w.get("vol"),
            "final_score": sc.get("final_score"),
            "win_rate": sc.get("win_rate"),
            "stake_yield": sc.get("stake_yield"),  # ROI all-time
            "total_trades": sc.get("total_trades"),
        }
        records.append(record)

    # Print table
    print()
    print("Top 20 Biggest Winners of the Month (with all-time scoring)")
    print("-" * 100)
    print(f"{'Rank':<5} {'Handle':<22} {'PnL':>14} {'Final':>8} {'Win%':>8} {'Stake yield':>12}")
    print("-" * 100)
    for r in records:
        handle = (r.get("xUsername") or r.get("userName") or r.get("user") or "")[:20]
        if not handle and r.get("user"):
            handle = f"{r['user'][:6]}…{r['user'][-4:]}"
        pnl = r.get("pnl")
        pnl_str = f"+${pnl:,.0f}" if pnl is not None and pnl >= 0 else f"${pnl:,.0f}" if pnl is not None else "—"
        fs = r.get("final_score")
        wr = r.get("win_rate")
        sy = r.get("stake_yield")
        print(
            f"{r.get('rank', 0):<5} {handle:<22} {pnl_str:>14} "
            f"{(f'{fs:.1f}' if fs is not None else '—'):>8} "
            f"{(f'{wr:.1f}%' if wr is not None else '—'):>8} "
            f"{(f'{sy:+.1f}%' if sy is not None else '—'):>12}"
        )
    print("-" * 100)
    print("Scoring: final score (0–100), win rate %, stake yield (ROI) all-time from profile-stat.")
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
