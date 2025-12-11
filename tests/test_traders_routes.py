"""
Test FastAPI traders routes.
"""
import pytest
from fastapi.testclient import TestClient
from app.main import app
from app.services import trader_service, data_fetcher

client = TestClient(app)


def test_get_traders_list(monkeypatch):
    """Test GET /traders endpoint."""
    from app.routers import traders
    
    # Fake trader list response
    fake_traders_list = [
        {
            "wallet_address": "0x1111111111111111111111111111111111111111",
            "total_trades": 3,
            "total_positions": 1,
            "first_trade_date": "2024-01-01T00:00:00",
            "last_trade_date": "2024-01-02T00:00:00",
        }
    ]

    def fake_fetch_traders_list(limit=None):
        return fake_traders_list

    monkeypatch.setattr(traders, "fetch_traders_list", fake_fetch_traders_list)

    resp = client.get("/traders?limit=50")
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
    body = resp.json()
    assert body["count"] == 1
    assert body["traders"][0]["wallet_address"] == fake_traders_list[0]["wallet_address"]
    assert body["traders"][0]["total_trades"] == 3
    print(f"✓ Test passed: GET /traders returns {body['count']} trader(s)")


def test_get_trader_detail(monkeypatch):
    """Test GET /traders/{wallet} endpoint."""
    from app.routers import traders
    
    wallet = "0x56687bf447db6ffa42ffe2204a05edaa20f55839"

    # Fake detail from service
    def fake_get_trader_detail(addr: str):
        assert addr == wallet
        return {
            "wallet_address": wallet,
            "total_trades": 100,
            "total_positions": 5,
            "active_positions": 2,
            "total_wins": 10.0,
            "total_losses": -5.0,
            "win_rate_percent": 66.7,
            "pnl": 5.0,
            "final_score": 42.0,
            "first_trade_date": "2024-11-06T02:04:26",
            "last_trade_date": "2024-11-13T14:46:49",
            "categories": {},
        }

    monkeypatch.setattr(traders, "get_trader_detail", fake_get_trader_detail)

    resp = client.get(f"/traders/{wallet}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["wallet_address"] == wallet
    assert body["total_trades"] == 100
    assert body["total_positions"] == 5
    assert body["pnl"] == 5.0
    print(f"✓ Test passed: GET /traders/{wallet[:20]}... returns correct data")


def test_get_trader_trades(monkeypatch):
    """Test GET /traders/{wallet}/trades endpoint."""
    from app.routers import traders
    
    wallet = "0x56687bf447db6ffa42ffe2204a05edaa20f55839"

    fake_trades = [
        {
            "token_id": "1",
            "side": "BUY",
            "market_slug": "m1",
            "shares_normalized": 10.0,
            "price": 0.5,
            "timestamp": 1731489409,
        }
    ]

    def fake_fetch_trades_for_wallet(addr: str):
        assert addr == wallet
        return fake_trades

    monkeypatch.setattr(traders, "fetch_trades_for_wallet", fake_fetch_trades_for_wallet)

    resp = client.get(f"/traders/{wallet}/trades?limit=50")
    assert resp.status_code == 200
    body = resp.json()
    assert body["wallet_address"] == wallet
    assert body["count"] == 1
    assert body["trades"][0]["side"] == "BUY"
    print(f"✓ Test passed: GET /traders/{wallet[:20]}.../trades returns {body['count']} trade(s)")


def test_get_trader_invalid_wallet():
    """Test GET /traders/{wallet} with invalid wallet format."""
    resp = client.get("/traders/invalid-wallet")
    assert resp.status_code == 400
    print("✓ Test passed: Invalid wallet format returns 400")


def test_get_trader_not_found(monkeypatch):
    """Test GET /traders/{wallet} when trader has no trades."""
    wallet = "0x0000000000000000000000000000000000000000"

    def fake_get_trader_detail(addr: str):
        return {
            "wallet_address": addr,
            "total_trades": 0,  # No trades
            "total_positions": 0,
            "active_positions": 0,
            "total_wins": 0.0,
            "total_losses": 0.0,
            "win_rate_percent": 0.0,
            "pnl": 0.0,
            "final_score": 0.0,
            "first_trade_date": None,
            "last_trade_date": None,
            "categories": {},
        }

    monkeypatch.setattr(trader_service, "get_trader_detail", fake_get_trader_detail)

    resp = client.get(f"/traders/{wallet}")
    assert resp.status_code == 404
    print("✓ Test passed: Trader with 0 trades returns 404")

