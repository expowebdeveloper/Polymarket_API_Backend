
import asyncio
import sys

# Add the project root to the python path
sys.path.append("/home/dell/Desktop/Projects/Polymarket/backend")
# Add user site-packages where dnspython is installed
sys.path.append("/home/dell/.local/lib/python3.10/site-packages")

from app.services.dashboard_service import get_live_dashboard_data

WALLET_ADDRESS = "0x56687bf447db6ffa42ffe2204a05edaa20f55839"

async def verify_values():
    print(f"Verifying values for {WALLET_ADDRESS}...")
    try:
        data = await get_live_dashboard_data(WALLET_ADDRESS)
        closed_pos = data.get("closed_positions", [])
        
        print(f"Fetched {len(closed_pos)} closed positions.")
        
        non_zero_size = 0
        non_zero_pnl = 0
        
        for i, p in enumerate(closed_pos[:5]):
            print(f"[{i}] {p.get('title')}")
            print(f"    Size: {p.get('size')}")
            print(f"    AvgP: {p.get('avgPrice')}")
            print(f"    ExitP: {p.get('exitPrice')}")
            print(f"    PnL: {p.get('realizedPnl')}")
            
            if p.get('size', 0) > 0:
                non_zero_size += 1
            if p.get('realizedPnl', 0) != 0:
                non_zero_pnl += 1
                
        if non_zero_size > 0 and non_zero_pnl > 0:
            print("\n✅ SUCCESS: Found non-zero Size and PnL values!")
        else:
            print("\n❌ FAILURE: Still seeing zero values.")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(verify_values())
