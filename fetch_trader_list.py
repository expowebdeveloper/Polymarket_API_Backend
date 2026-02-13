"""
Master Trader Registry Builder
- Fetches Polymarket leaderboards (day/week/month/all)
- Creates table automatically if not exists
- Stores all traders in one master registry
- Prints each trader as processed
"""

import asyncio
from datetime import datetime
from typing import Dict, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from app.core.config import settings
from app.services.data_fetcher import async_client


# ==========================
# CONFIG
# ==========================
API_BASE_URL = "https://data-api.polymarket.com/v1/leaderboard"
LIMIT = 50
# TIME_PERIODS = ["day", "week", "month", "all"]
TIME_PERIODS = ["all"]



# ==========================
# CREATE TABLE SQL
# ==========================
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS polymarket_traders (
    wallet_address TEXT PRIMARY KEY,

    username TEXT,
    pseudonym TEXT,
    profile_image TEXT,
    verified_badge BOOLEAN DEFAULT FALSE,

    daily_rank INTEGER,
    daily_volume DOUBLE PRECISION,
    daily_pnl DOUBLE PRECISION,

    weekly_rank INTEGER,
    weekly_volume DOUBLE PRECISION,
    weekly_pnl DOUBLE PRECISION,

    monthly_rank INTEGER,
    monthly_volume DOUBLE PRECISION,
    monthly_pnl DOUBLE PRECISION,

    all_time_rank INTEGER,
    all_time_volume DOUBLE PRECISION,
    all_time_pnl DOUBLE PRECISION,

    first_seen_at TIMESTAMP NOT NULL,
    last_updated_at TIMESTAMP NOT NULL
);
"""


# ==========================
# SAFE TYPE HELPERS
# ==========================
def safe_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def safe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def extract_wallet(trader: Dict) -> Optional[str]:
    wallet = (
        trader.get("proxyWallet")
        or trader.get("wallet")
        or trader.get("address")
    )

    if wallet and wallet.startswith("0x") and len(wallet) == 42:
        return wallet.lower()

    return None


# ==========================
# FETCH API PAGE
# ==========================
async def fetch_page(period: str, offset: int):
    params = {
        "timePeriod": period,
        "orderBy": "VOL",
        "limit": LIMIT,
        "offset": offset,
    }

    response = await async_client.get(API_BASE_URL, params=params)
    response.raise_for_status()
    return response.json()


# ==========================
# UPSERT LOGIC
# ==========================
async def upsert_trader(
    session: AsyncSession,
    trader: Dict,
    period: str,
):
    wallet = extract_wallet(trader)
    if not wallet:
        return

    period_map = {
        "day": ("daily_rank", "daily_volume", "daily_pnl"),
        "week": ("weekly_rank", "weekly_volume", "weekly_pnl"),
        "month": ("monthly_rank", "monthly_volume", "monthly_pnl"),
        "all": ("all_time_rank", "all_time_volume", "all_time_pnl"),
    }

    rank_col, vol_col, pnl_col = period_map[period]
    now = datetime.utcnow()

    rank = safe_int(trader.get("rank"))
    volume = safe_float(trader.get("vol"))
    pnl = safe_float(trader.get("pnl"))
    username = trader.get("userName") or trader.get("name")

    # 🔥 PRINT DEBUG OUTPUT
    print(
        f"[{period.upper()}] "
        f"Rank: {rank} | "
        f"User: {username} | "
        f"Wallet: {wallet} | "
        f"Vol: {volume} | "
        f"PnL: {pnl}"
    )

    await session.execute(
        text(f"""
            INSERT INTO polymarket_traders (
                wallet_address,
                username,
                pseudonym,
                profile_image,
                verified_badge,
                {rank_col},
                {vol_col},
                {pnl_col},
                first_seen_at,
                last_updated_at
            )
            VALUES (
                :wallet,
                :username,
                :pseudonym,
                :profile_image,
                :verified_badge,
                :rank,
                :volume,
                :pnl,
                :now,
                :now
            )
            ON CONFLICT (wallet_address)
            DO UPDATE SET
                username = EXCLUDED.username,
                pseudonym = EXCLUDED.pseudonym,
                profile_image = EXCLUDED.profile_image,
                verified_badge = EXCLUDED.verified_badge,
                {rank_col} = EXCLUDED.{rank_col},
                {vol_col} = EXCLUDED.{vol_col},
                {pnl_col} = EXCLUDED.{pnl_col},
                last_updated_at = :now
        """),
        {
            "wallet": wallet,
            "username": username,
            "pseudonym": trader.get("xUsername"),
            "profile_image": trader.get("profileImage"),
            "verified_badge": bool(trader.get("verifiedBadge", False)),
            "rank": rank,
            "volume": volume,
            "pnl": pnl,
            "now": now,
        }
    )


# ==========================
# INGEST PER PERIOD
# ==========================
async def ingest_period(session: AsyncSession, period: str):
    print(f"\n🚀 Ingesting {period.upper()} leaderboard")

    offset = 0
    total = 0

    while True:
        traders = await fetch_page(period, offset)

        if not traders:
            break

        for trader in traders:
            await upsert_trader(session, trader, period)
            total += 1

        await session.commit()

        if len(traders) < LIMIT:
            break

        offset += LIMIT
        await asyncio.sleep(0.25)

    print(f"✅ {period.upper()} done → {total} traders processed")


# ==========================
# MAIN
# ==========================
async def main():
    engine = create_async_engine(settings.DATABASE_URL, echo=False)

    async with engine.begin() as conn:
        await conn.execute(text(CREATE_TABLE_SQL))

    SessionLocal = sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    async with SessionLocal() as session:
        for period in TIME_PERIODS:
            await ingest_period(session, period)

    await engine.dispose()

    print("\n🎯 MASTER TRADER REGISTRY UPDATED SUCCESSFULLY")


if __name__ == "__main__":
    asyncio.run(main())