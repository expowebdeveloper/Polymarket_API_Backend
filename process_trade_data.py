"""
Script to process and insert trade data into database.
Reads trade data, cleans it, calculates PnL, and inserts into database.
"""

import asyncio
import sys
from app.db.session import get_async_session
from app.services.trade_data_processor import process_and_insert_trade_data
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    """Main function to process trade data."""
    if len(sys.argv) < 2:
        print("Usage: python process_trade_data.py <wallet_address> [wallet_address2] ...")
        print("Example: python process_trade_data.py 0xdbade4c82fb72780a0db9a38f821d8671aba9c95")
        sys.exit(1)
    
    wallet_addresses = sys.argv[1:]
    
    async for session in get_async_session():
        try:
            for wallet_address in wallet_addresses:
                print(f"\n{'='*60}")
                print(f"Processing trade data for: {wallet_address}")
                print(f"{'='*60}\n")
                
                result = await process_and_insert_trade_data(session, wallet_address)
                
                print(f"\n✅ Processing complete for {wallet_address}:")
                print(f"  - Raw trades: {result['raw_trades_count']}")
                print(f"  - Cleaned trades: {result['cleaned_trades_count']}")
                print(f"  - Saved trades: {result['saved_trades_count']}")
                print(f"  - Trader ID: {result['trader_id']}")
                
                if 'metrics' in result:
                    print(f"\n  Aggregated Metrics:")
                    print(f"  - Total trades: {result['metrics']['total_trades']}")
                    print(f"  - Total stake: ${result['metrics']['total_stake']:.2f}")
                    print(f"  - Total PnL: ${result['metrics']['total_pnl']:.2f}")
                    print(f"  - Realized PnL: ${result['metrics']['realized_pnl']:.2f}")
                    print(f"  - Unrealized PnL: ${result['metrics']['unrealized_pnl']:.2f}")
                    print(f"  - Win rate: {result['metrics']['win_rate']:.2f}%")
                
                if 'error' in result:
                    print(f"  ⚠️  Error: {result['error']}")
        
        except Exception as e:
            logger.error(f"Error processing trade data: {e}", exc_info=True)
            print(f"\n❌ Error: {e}")
        finally:
            break  # Exit after first session


if __name__ == "__main__":
    asyncio.run(main())






