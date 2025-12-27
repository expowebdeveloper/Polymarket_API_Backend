"""
Tests for DB-based final score leaderboard.
"""
import pytest
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, AsyncMock, patch
from app.main import app
from app.db.models import Trade
from decimal import Decimal

client = TestClient(app)

@pytest.mark.asyncio
async def test_db_scoring_service_logic():
    """Test the logic in db_scoring_service.calculate_db_trader_metrics"""
    from app.services.db_scoring_service import calculate_db_trader_metrics
    
    # Mock data
    wallet = "0x1234567890123456789012345678901234567890"
    mock_trade = Trade(
        proxy_wallet=wallet,
        side="BUY",
        size=Decimal("100"),
        price=Decimal("0.5"),
        timestamp=1731489409,
        slug="test-market",
        condition_id="cond-1",
        asset="asset-1"
    )
    
    mock_session = MagicMock()
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = [mock_trade]
    mock_session.execute = AsyncMock(return_value=mock_result)
    
    mock_markets = [
        {
            "id": "test-market",
            "slug": "test-market",
            "resolution": "YES",
            "resolved": True
        }
    ]
    
    # We need to mock scoring_engine.calculate_metrics as well because it fetches more stuff
    with patch("app.services.db_scoring_service.calculate_metrics", new_callable=AsyncMock) as mock_calc:
        mock_calc.return_value = {
            "pnl": 50.0,
            "win_rate_percent": 100.0,
            "win_count": 1,
            "loss_count": 0,
            "final_score": 85.5,
            "total_positions": 1,
            "active_positions": 0
        }
        
        metrics = await calculate_db_trader_metrics(mock_session, wallet, mock_markets)
        
        assert metrics["wallet_address"] == wallet
        assert metrics["total_pnl"] == 50.0
        assert metrics["final_score"] == 85.5
        assert metrics["total_trades"] == 1
        assert metrics["roi"] == 100.0 # 50 profit / 50 stake * 100
        print("✓ service logic test passed")

def test_db_leaderboard_router():
    """Test GET /leaderboard/db/final-score endpoint"""
    from app.routers import leaderboard
    
    fake_leaderboard = [
        {
            "rank": 1,
            "wallet_address": "0x1234567890123456789012345678901234567890",
            "total_pnl": 1000.0,
            "roi": 20.0,
            "win_rate": 75.0,
            "total_trades": 10,
            "total_trades_with_pnl": 10,
            "winning_trades": 7,
            "total_stakes": 5000.0,
            "final_score": 90.0
        }
    ]
    
    # Mock the service called by the router
    with patch("app.routers.leaderboard.get_db_leaderboard", new_callable=AsyncMock) as mock_service:
        mock_service.return_value = fake_leaderboard
        
        # Also need to mock opening of wallet_address.txt
        with patch("builtins.open", MagicMock()):
            resp = client.get("/leaderboard/db/final-score")
            
            assert resp.status_code == 200
            body = resp.json()
            assert body["metric"] == "final_score"
            assert len(body["entries"]) == 1
            assert body["entries"][0]["wallet_address"] == fake_leaderboard[0]["wallet_address"]
            assert body["entries"][0]["final_score"] == 90.0
            print("✓ router endpoint test passed")

if __name__ == "__main__":
    # For quick manual run if needed
    import asyncio
    asyncio.run(test_db_scoring_service_logic())
