
import pytest
from unittest.mock import AsyncMock, MagicMock
from decimal import Decimal
from app.services.pnl_calculator_service import calculate_user_pnl

@pytest.mark.asyncio
async def test_calculate_user_pnl_with_none_values():
    """
    Test PnL calculation when database returns None for numeric fields.
    This simulates the condition causing the 500 error.
    """
    # Mock session
    session = AsyncMock()
    
    # Mock bad data
    # Trade with None size and price
    bad_trade = MagicMock()
    bad_trade.side = "BUY"
    bad_trade.size = None
    bad_trade.price = None 
    bad_trade.pnl = None
    
    # Position with None values
    bad_position = MagicMock()
    bad_position.initial_value = None
    bad_position.realized_pnl = None
    bad_position.cash_pnl = None 
    bad_position.current_value = None
    
    # Activity with None size
    bad_activity = MagicMock()
    bad_activity.type = "REWARD"
    bad_activity.usdc_size = None
    
    # Mock service responses
    # We need to patch the get_*_from_db functions imported in the service
    # easiest way is to mock them at the module level if we could, 
    # but here we can just rely on the fact that calculate_user_pnl calls them.
    # However, since they are imported directly in the module, we need to mock 
    # the functions in the module namespace.
    
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("app.services.pnl_calculator_service.get_trades_from_db", 
                  AsyncMock(return_value=[bad_trade]))
        mp.setattr("app.services.pnl_calculator_service.get_positions_from_db", 
                  AsyncMock(return_value=[bad_position]))
        mp.setattr("app.services.pnl_calculator_service.get_activities_from_db", 
                  AsyncMock(return_value=[bad_activity]))
        
        # Run calculation
        result = await calculate_user_pnl(session, "0x123")
        
        # Verify it didn't crash and handled Nones as 0
        assert result["total_invested"] == 0.0
        assert result["total_realized_pnl"] == 0.0
        assert result["total_rewards"] == 0.0
        
        # Verify key metrics
        assert result["key_metrics"]["total_stakes"] == 0.0
