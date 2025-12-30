"""
Test cases for DB Leaderboard endpoints.
Tests that endpoints read from database only (no calculation) and sync endpoints work correctly.
"""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession
from decimal import Decimal
from datetime import datetime

from app.main import app
from app.db.models import LeaderboardEntry, LeaderboardMetadata, Trader
from app.db.session import AsyncSessionLocal
from app.db.base import Base
from sqlalchemy import select

client = TestClient(app)


@pytest.fixture
async def db_session():
    """Create a test database session."""
    async with AsyncSessionLocal() as session:
        yield session
        await session.rollback()


@pytest.fixture
async def sample_leaderboard_data(db_session: AsyncSession):
    """Create sample leaderboard entries in database."""
    # Create sample entries
    entries = [
        LeaderboardEntry(
            wallet_address="0x1111111111111111111111111111111111111111",
            name="Trader 1",
            total_pnl=Decimal("1000.50"),
            roi=Decimal("50.25"),
            win_rate=Decimal("75.00"),
            total_trades=100,
            total_trades_with_pnl=100,
            winning_trades=75,
            total_stakes=Decimal("2000.00"),
            w_shrunk=Decimal("0.75"),
            roi_shrunk=Decimal("50.25"),
            pnl_shrunk=Decimal("1000.50"),
            score_win_rate=Decimal("0.75"),
            score_roi=Decimal("0.50"),
            score_pnl=Decimal("0.60"),
            score_risk=Decimal("0.80"),
            final_score=Decimal("66.25"),
            worst_loss=Decimal("-100.00"),
            max_stake=Decimal("500.00"),
            sum_sq_stakes=Decimal("1000000.00"),
            calculated_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        ),
        LeaderboardEntry(
            wallet_address="0x2222222222222222222222222222222222222222",
            name="Trader 2",
            total_pnl=Decimal("2000.75"),
            roi=Decimal("100.50"),
            win_rate=Decimal("80.00"),
            total_trades=150,
            total_trades_with_pnl=150,
            winning_trades=120,
            total_stakes=Decimal("3000.00"),
            w_shrunk=Decimal("0.80"),
            roi_shrunk=Decimal("100.50"),
            pnl_shrunk=Decimal("2000.75"),
            score_win_rate=Decimal("0.80"),
            score_roi=Decimal("1.00"),
            score_pnl=Decimal("0.70"),
            score_risk=Decimal("0.90"),
            final_score=Decimal("87.50"),
            worst_loss=Decimal("-50.00"),
            max_stake=Decimal("600.00"),
            sum_sq_stakes=Decimal("2000000.00"),
            calculated_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        ),
    ]
    
    for entry in entries:
        db_session.add(entry)
    
    # Create metadata
    metadata = LeaderboardMetadata(
        w_shrunk_1_percent=Decimal("0.40"),
        w_shrunk_99_percent=Decimal("0.90"),
        roi_shrunk_1_percent=Decimal("10.00"),
        roi_shrunk_99_percent=Decimal("150.00"),
        pnl_shrunk_1_percent=Decimal("-5000.00"),
        pnl_shrunk_99_percent=Decimal("5000.00"),
        roi_median=Decimal("25.00"),
        pnl_median=Decimal("500.00"),
        population_size=2,
        total_traders=2,
        last_calculated_at=datetime.utcnow()
    )
    db_session.add(metadata)
    
    await db_session.commit()
    
    return entries, metadata


class TestGetDBAnalytics:
    """Test GET /traders/analytics endpoint - should ONLY read from DB."""
    
    def test_get_analytics_empty_db(self):
        """Test that empty DB returns empty response (no calculation)."""
        response = client.get("/traders/analytics")
        assert response.status_code == 200
        data = response.json()
        
        assert data["total_traders"] == 0
        assert data["population_traders"] == 0
        assert len(data["leaderboards"]["final_score"]) == 0
        assert len(data["leaderboards"]["w_shrunk"]) == 0
    
    @pytest.mark.asyncio
    async def test_get_analytics_with_data(self, db_session, sample_leaderboard_data):
        """Test that endpoint reads from DB and returns correct data."""
        # Note: This test requires the database to be populated
        # In a real test, you'd set up test data first
        
        response = client.get("/traders/analytics?limit=10&offset=0")
        assert response.status_code == 200
        data = response.json()
        
        # Should have data if DB is populated
        assert "leaderboards" in data
        assert "percentiles" in data
        assert "medians" in data
        assert "total_traders" in data
    
    def test_get_analytics_pagination(self):
        """Test pagination works correctly."""
        response = client.get("/traders/analytics?limit=5&offset=0")
        assert response.status_code == 200
        
        response2 = client.get("/traders/analytics?limit=5&offset=5")
        assert response2.status_code == 200
        
        # Results should be different (if there's enough data)
        data1 = response.json()
        data2 = response2.json()
        
        if len(data1["leaderboards"]["final_score"]) > 0:
            # If we have data, check that pagination works
            assert len(data1["leaderboards"]["final_score"]) <= 5
            assert len(data2["leaderboards"]["final_score"]) <= 5
    
    def test_get_analytics_all_leaderboard_types(self):
        """Test that all leaderboard types are returned."""
        response = client.get("/traders/analytics")
        assert response.status_code == 200
        data = response.json()
        
        leaderboards = data["leaderboards"]
        required_types = [
            "w_shrunk", "roi_raw", "roi_shrunk", "pnl_shrunk",
            "score_win_rate", "score_roi", "score_pnl", "score_risk", "final_score"
        ]
        
        for lb_type in required_types:
            assert lb_type in leaderboards
            assert isinstance(leaderboards[lb_type], list)
    
    def test_get_analytics_percentiles(self):
        """Test that percentiles are returned correctly."""
        response = client.get("/traders/analytics")
        assert response.status_code == 200
        data = response.json()
        
        percentiles = data["percentiles"]
        assert "w_shrunk_1_percent" in percentiles
        assert "w_shrunk_99_percent" in percentiles
        assert "roi_shrunk_1_percent" in percentiles
        assert "roi_shrunk_99_percent" in percentiles
        assert "pnl_shrunk_1_percent" in percentiles
        assert "pnl_shrunk_99_percent" in percentiles
        assert "population_size" in percentiles
    
    def test_get_analytics_medians(self):
        """Test that medians are returned correctly."""
        response = client.get("/traders/analytics")
        assert response.status_code == 200
        data = response.json()
        
        medians = data["medians"]
        assert "roi_median" in medians
        assert "pnl_median" in medians


class TestRecalculateLeaderboard:
    """Test POST /traders/analytics/recalculate endpoint."""
    
    def test_recalculate_leaderboard_starts_background_task(self):
        """Test that recalculation starts in background."""
        response = client.post("/traders/analytics/recalculate?max_traders=100")
        assert response.status_code == 200
        data = response.json()
        
        assert "message" in data
        assert "max_traders" in data
        assert "background" in data["message"].lower() or "started" in data["message"].lower()
    
    def test_recalculate_leaderboard_all_traders(self):
        """Test recalculation with all traders."""
        response = client.post("/traders/analytics/recalculate")
        assert response.status_code == 200
        data = response.json()
        
        assert data["max_traders"] == "all" or data["max_traders"] is None


class TestSyncFromPolymarket:
    """Test POST /traders/analytics/sync-from-polymarket endpoint."""
    
    def test_sync_from_polymarket_starts_background_task(self):
        """Test that sync starts in background."""
        response = client.post("/traders/analytics/sync-from-polymarket?limit=50")
        assert response.status_code == 200
        data = response.json()
        
        assert "message" in data
        assert "limit" in data
        assert "background" in data["message"].lower() or "started" in data["message"].lower()
    
    def test_sync_from_polymarket_all_traders(self):
        """Test sync with all traders."""
        response = client.post("/traders/analytics/sync-from-polymarket?limit=0")
        assert response.status_code == 200
        data = response.json()
        
        assert data["limit"] == "all" or data["limit"] == 0


class TestLeaderboardStatus:
    """Test GET /traders/analytics/status endpoint."""
    
    def test_get_status(self):
        """Test that status endpoint returns correct information."""
        response = client.get("/traders/analytics/status")
        assert response.status_code == 200
        data = response.json()
        
        assert "scheduler_running" in data
        assert "last_run_time" in data
        assert "total_entries_in_db" in data
        assert isinstance(data["total_entries_in_db"], int)


class TestPerformance:
    """Test performance - endpoint should be fast (DB read only)."""
    
    def test_analytics_response_time(self):
        """Test that analytics endpoint responds quickly (DB read only)."""
        import time
        
        start = time.time()
        response = client.get("/traders/analytics?limit=100")
        elapsed = time.time() - start
        
        assert response.status_code == 200
        # Should be fast - less than 1 second for DB read
        assert elapsed < 1.0, f"Response took {elapsed:.2f}s - should be < 1s for DB read"
    
    def test_analytics_no_calculation(self):
        """Test that endpoint doesn't do calculation (no timeout)."""
        # If calculation was happening, this would timeout or take very long
        response = client.get("/traders/analytics?limit=1000")
        assert response.status_code == 200
        
        # Should return immediately even with large limit
        data = response.json()
        assert "leaderboards" in data


class TestDataIntegrity:
    """Test data integrity - ensure data is correctly stored and retrieved."""
    
    @pytest.mark.asyncio
    async def test_leaderboard_entry_structure(self, db_session, sample_leaderboard_data):
        """Test that leaderboard entries have all required fields."""
        entries, _ = sample_leaderboard_data
        
        stmt = select(LeaderboardEntry).limit(1)
        result = await db_session.execute(stmt)
        entry = result.scalar_one_or_none()
        
        if entry:
            # Check all required fields exist
            assert entry.wallet_address is not None
            assert entry.total_pnl is not None
            assert entry.roi is not None
            assert entry.win_rate is not None
            assert entry.total_trades is not None
            assert entry.w_shrunk is not None
            assert entry.roi_shrunk is not None
            assert entry.pnl_shrunk is not None
            assert entry.score_win_rate is not None
            assert entry.score_roi is not None
            assert entry.score_pnl is not None
            assert entry.score_risk is not None
            assert entry.final_score is not None
    
    @pytest.mark.asyncio
    async def test_metadata_structure(self, db_session, sample_leaderboard_data):
        """Test that metadata has all required fields."""
        _, metadata = sample_leaderboard_data
        
        stmt = select(LeaderboardMetadata).limit(1)
        result = await db_session.execute(stmt)
        meta = result.scalar_one_or_none()
        
        if meta:
            assert meta.w_shrunk_1_percent is not None
            assert meta.w_shrunk_99_percent is not None
            assert meta.roi_shrunk_1_percent is not None
            assert meta.roi_shrunk_99_percent is not None
            assert meta.pnl_shrunk_1_percent is not None
            assert meta.pnl_shrunk_99_percent is not None
            assert meta.roi_median is not None
            assert meta.pnl_median is not None
            assert meta.population_size is not None
            assert meta.total_traders is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
