
import asyncio
import httpx
import json
import traceback

WALLET_ADDRESS = "0x6a72f61820b26b1fe4d956e17b6dc2a1ea3033ee"

async def inspect():
    url = f"https://data-api.polymarket.com/v1/closed-positions"
    params = {"user": WALLET_ADDRESS, "limit": 1}
    print(f"Requesting: {url} with params {params}")
    
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(url, params=params)
            print(f"Response Status: {resp.status_code}")
            
            if resp.status_code != 200:
                print(f"Error Body: {resp.text}")
                return

            data = resp.json()
            if isinstance(data, list) and len(data) > 0:
                print("Sample Position Data:")
                print(json.dumps(data[0], indent=2))
                keys = list(data[0].keys())
                print(f"\nKeys present: {keys}")
                print(f"Has 'slug': {'slug' in keys}")
                print(f"Has 'title': {'title' in keys}")
            else:
                print("Empty list returned.")
        except Exception:
            traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(inspect())
