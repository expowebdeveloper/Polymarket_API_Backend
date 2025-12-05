"""
Test scoring engine metrics calculation with BUY/SELL and market resolution.
"""
import pytest
from app.services import scoring_engine


def test_calculate_metrics_buy_sell_with_resolution(monkeypatch):
    """Test that calculate_metrics correctly handles BUY/SELL trades with market resolution."""
    wallet = "0xabc1234567890123456789012345678901234567"

    # Markets must have matching slug/id to what trades reference
    markets = [
        {
            "id": "market-yes",
            "slug": "market-yes",
            "market_id": "market-yes",
            "resolution": "YES",
            "resolved": True,
        },
        {
            "id": "market-no",
            "slug": "market-no",
            "market_id": "market-no",
            "resolution": "NO",
            "resolved": True,
        },
    ]

    # Two trades: BUY on YES market (wins), SELL on NO market (wins)
    trades = [
        {
            "market_slug": "market-yes",
            "side": "BUY",
            "shares_normalized": 100.0,
            "price": 0.5,
            "timestamp": 1731489409,
        },
        {
            "market_slug": "market-no",
            "side": "SELL",
            "shares_normalized": 50.0,
            "price": 0.5,
            "timestamp": 1731489409 + 86400,
        },
    ]

    metrics = scoring_engine.calculate_metrics(wallet, trades, markets)

    assert metrics["total_positions"] == 2
    assert metrics["win_count"] == 2
    assert metrics["loss_count"] == 0
    assert metrics["total_wins"] > 0
    assert metrics["total_losses"] == 0
    assert metrics["pnl"] > 0
    assert metrics["win_rate_percent"] == 100.0
    assert metrics["final_score"] > 0
    print(f"✓ Test passed: Metrics calculated correctly - {metrics['win_count']} wins, {metrics['loss_count']} losses, PnL: {metrics['pnl']}")


def test_calculate_metrics_with_losses():
    """Test metrics calculation with both wins and losses."""
    wallet = "0xtest1234567890123456789012345678901234567"

    # Markets must have matching slug/id to what trades reference
    markets = [
        {
            "id": "market-yes",
            "slug": "market-yes",
            "market_id": "market-yes",
            "resolution": "YES",
            "resolved": True,
        },
        {
            "id": "market-no",
            "slug": "market-no",
            "market_id": "market-no",
            "resolution": "NO",
            "resolved": True,
        },
    ]

    # BUY on YES market (wins), BUY on NO market (loses)
    trades = [
        {
            "market_slug": "market-yes",
            "side": "BUY",
            "shares_normalized": 100.0,
            "price": 0.5,
            "timestamp": 1731489409,
        },
        {
            "market_slug": "market-no",
            "side": "BUY",  # BUY on NO market = loss
            "shares_normalized": 50.0,
            "price": 0.5,
            "timestamp": 1731489409 + 86400,
        },
    ]

    metrics = scoring_engine.calculate_metrics(wallet, trades, markets)

    assert metrics["total_positions"] == 2
    assert metrics["win_count"] == 1
    assert metrics["loss_count"] == 1
    assert metrics["total_wins"] > 0
    assert metrics["total_losses"] < 0  # Should be negative
    assert metrics["win_rate_percent"] == 50.0
    print(f"✓ Test passed: Mixed results - {metrics['win_count']} wins, {metrics['loss_count']} losses, Win rate: {metrics['win_rate_percent']}%")

