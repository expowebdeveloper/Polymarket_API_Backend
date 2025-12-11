"""
Test trader service basic info with Dome trade format.
"""
import pytest
from app.services import trader_service
from app.services.data_fetcher import fetch_trades_for_wallet


def test_get_trader_basic_info_with_dome_trades(monkeypatch):
    """Test that get_trader_basic_info correctly processes Dome-format trades."""
    wallet = "0x56687bf447db6ffa42ffe2204a05edaa20f55839"

    # Fake resolved markets from Polymarket
    markets = [
        {
            "id": "henry-cavill-announced-as-next-james-bond",
            "slug": "henry-cavill-announced-as-next-james-bond",
            "resolution": "YES",
            "resolved": True,
        },
        {
            "id": "another-market",
            "slug": "another-market",
            "resolution": "NO",
            "resolved": True,
        },
    ]

    # Fake Dome trades for this wallet (similar to real response)
    trades = [
        {
            "token_id": "1",
            "side": "BUY",
            "market_slug": "henry-cavill-announced-as-next-james-bond",
            "shares": 199950000,
            "shares_normalized": 199.95,
            "price": 0.96,
            "timestamp": 1731489409,  # Unix timestamp
            "user": wallet,
        },
        {
            "token_id": "2",
            "side": "SELL",
            "market_slug": "another-market",
            "shares": 50000000,
            "shares_normalized": 50.0,
            "price": 0.40,
            "timestamp": 1731489409 + 86400,  # Next day
            "user": wallet,
        },
    ]

    # Monkeypatch fetch_trades_for_wallet to avoid network
    def fake_fetch_trades_for_wallet(addr: str):
        assert addr == wallet
        return trades

    monkeypatch.setattr(
        trader_service, "fetch_trades_for_wallet", fake_fetch_trades_for_wallet
    )

    info = trader_service.get_trader_basic_info(wallet, markets)

    assert info["wallet_address"] == wallet
    assert info["total_trades"] == 2
    # Two distinct market_slugs -> 2 positions
    assert info["total_positions"] == 2
    assert info["first_trade_date"] is not None
    assert info["last_trade_date"] is not None
    print(f"✓ Test passed: Basic info correctly extracted {info['total_trades']} trades and {info['total_positions']} positions")





