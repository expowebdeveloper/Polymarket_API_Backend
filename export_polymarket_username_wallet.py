"""
Fetch Polymarket leaderboard from the API and write username + wallet address to a .txt file.

API: https://data-api.polymarket.com/v1/leaderboard
Output: polymarket_username_wallet.txt (tab-separated: username	wallet_address)

Run from backend directory:
  python export_polymarket_username_wallet.py
  python export_polymarket_username_wallet.py --pages 5 --out my_list.txt
"""
import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.services.data_fetcher import fetch_traders_from_leaderboard


async def fetch_all_usernames_wallets(max_pages: int = 10, page_size: int = 50):
    """Fetch username and wallet from Polymarket leaderboard API (paginated). API typically caps at 50 per request."""
    seen_wallets = set()
    rows = []  # list of (username, wallet_address)
    for page in range(max_pages):
        offset = page * page_size
        traders, pagination = await fetch_traders_from_leaderboard(
            limit=page_size,
            offset=offset,
            time_period="all",
            category="overall",
        )
        if not traders:
            break
        for t in traders:
            wallet = (t.get("wallet_address") or t.get("user") or "").strip()
            if not wallet or wallet in seen_wallets:
                continue
            seen_wallets.add(wallet)
            # Prefer Polymarket username, then X username
            username = (t.get("userName") or t.get("xUsername") or "").strip()
            if not username:
                username = wallet  # fallback to wallet if no name
            rows.append((username, wallet))
        if not pagination.get("has_more"):
            break
    return rows


def main():
    parser = argparse.ArgumentParser(description="Export Polymarket username and wallet to a .txt file")
    parser.add_argument("--out", default="polymarket_username_wallet.txt", help="Output .txt file path")
    parser.add_argument("--pages", type=int, default=10, help="Number of leaderboard pages to fetch (50 per page)")
    parser.add_argument("--sep", default="\t", help="Separator between username and wallet (default: tab)")
    args = parser.parse_args()

    print(f"Fetching from Polymarket Leaderboard API (up to {args.pages} pages)...")
    rows = asyncio.run(fetch_all_usernames_wallets(max_pages=args.pages))
    out_path = os.path.join(os.path.dirname(__file__), args.out) if not os.path.isabs(args.out) else args.out

    with open(out_path, "w", encoding="utf-8") as f:
        # Header line (optional, comment out if you want only data)
        f.write(f"username{args.sep}wallet_address\n")
        for username, wallet in rows:
            # Normalize: one line per user, no newlines in fields
            u = username.replace("\n", " ").replace("\r", "")
            w = wallet.replace("\n", " ").replace("\r", "")
            f.write(f"{u}{args.sep}{w}\n")

    print(f"Wrote {len(rows)} entries to {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
