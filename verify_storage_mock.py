import asyncio
import sys
from datetime import datetime
from unittest.mock import patch, AsyncMock
from app.db.session import init_db, AsyncSessionLocal
from app.services.market_service import update_all_markets
from sqlalchemy import text

# Sample mock data
MOCK_MARKETS = [
    {
        "id": "0x123",
        "slug": "will-bitcoin-hit-100k",
        "question": "Will Bitcoin hit $100k in 2024?",
        "description": "Market description...",
        "status": "active",
        "endDate": "2024-12-31T23:59:59Z",
        "creationDate": "2024-01-01T00:00:00Z",
        "volume": 1000000,
        "liquidity": 50000,
        "openInterest": 25000,
        "image": "https://example.com/btc.png",
        "icon": "https://example.com/btc_icon.png",
        "category": "Crypto",
        "tags": [{"slug": "bitcoin"}, {"slug": "crypto"}],
        "outcomePrices": ["0.4", "0.6"],
    }
]

async def verify_storage_logic():
    print("Verifying storage logic with mock data...")
    
    # 1. Initialize DB
    await init_db()
    
    # 2. Mock fetch_markets
    with patch('app.services.market_service.fetch_markets', new_callable=AsyncMock) as mock_fetch:
        # Setup mock to return data on first call, then empty on subsequent calls
        # We need enough returns for "active", "closed", "resolved"
        mock_fetch.side_effect = [
            (MOCK_MARKETS, {"has_more": False}), # active
            ([], {"has_more": False}),           # closed
            ([], {"has_more": False})            # resolved
        ]
        
        # 3. Run update
        await update_all_markets()

    # 4. Check DB
    async with AsyncSessionLocal() as session:
        result = await session.execute(text("SELECT COUNT(*) FROM markets"))
        count = result.scalar()
        print(f"Total markets in DB: {count}")
        
        result_slug = await session.execute(text("SELECT slug FROM markets WHERE id='0x123'"))
        slug = result_slug.scalar()
        print(f"Fetched slug: {slug}")

        if count >= 1 and slug == "will-bitcoin-hit-100k":
            print("✓ Verification SUCCESS: Mock market stored correctly.")
        else:
            print("✗ Verification FAILED: Mock market not found.")

if __name__ == "__main__":
    asyncio.run(verify_storage_logic())
