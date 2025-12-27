
import asyncio
from sqlalchemy import create_engine, text
from app.core.config import settings

async def test_query():
    # Use synchronous engine for testing to get clear errors
    sync_url = settings.DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")
    engine = create_engine(sync_url)
    
    query = """
    SELECT aggregated_metrics.id, aggregated_metrics.trader_id, aggregated_metrics.total_trades, 
    aggregated_metrics.total_stake, aggregated_metrics.total_pnl, aggregated_metrics.realized_pnl, 
    aggregated_metrics.unrealized_pnl, aggregated_metrics.win_count, aggregated_metrics.loss_count, 
    aggregated_metrics.win_rate, aggregated_metrics.avg_trade_size, aggregated_metrics.largest_win, 
    aggregated_metrics.largest_loss, aggregated_metrics.total_volume, aggregated_metrics.portfolio_value, 
    aggregated_metrics.created_at, aggregated_metrics.updated_at 
    FROM aggregated_metrics 
    """
    
    print("Executing query to test columns...")
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            print("✓ Query successful!")
    except Exception as e:
        print(f"✗ Query failed: {e}")

if __name__ == "__main__":
    asyncio.run(test_query())
