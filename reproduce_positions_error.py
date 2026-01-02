
import asyncio
import httpx

async def main():
    wallet_address = "0x336151559e8c8b048de5231dc8313e196b314363"
    url = "https://data-api.polymarket.com/positions"
    params = {"user": wallet_address}
    
    async with httpx.AsyncClient() as client:
        try:
            print(f"Fetching from {url} with params {params}")
            response = await client.get(url, params=params)
            print(f"Status Code: {response.status_code}")
            response.raise_for_status()
            print("Response:", response.json())
        except Exception as e:
            print(f"Error fetching positions: {e}")
            # print detail if it is an http error
            if isinstance(e, httpx.HTTPStatusError):
                print(f"Response text: {e.response.text}")

if __name__ == "__main__":
    asyncio.run(main())
