import asyncio
import httpx
from app.core.config import settings

# Adjust base URL if needed, assuming default local dev port
BASE_URL = "http://localhost:8000"

async def test_search():
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0) as client:
        print("1. Testing valid wallet address...")
        wallet = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640" # Example address
        resp = await client.get(f"/dashboard/search/{wallet}")
        if resp.status_code == 200:
            print(f"✅ Wallet Address Found: {resp.json()}")
        else:
            print(f"❌ Wallet Address Failed: {resp.status_code} - {resp.text}")

        print("\n2. Testing Username (e.g., 'Don')...")
        username = "Don" 
        resp = await client.get(f"/dashboard/search/{username}")
        if resp.status_code == 200:
            print(f"✅ Username Found: {resp.json()}")
        elif resp.status_code == 404:
            print(f"⚠️ Username '{username}' Not Found (Expected if no such user in DB)")
        else:
            print(f"❌ Username Error: {resp.status_code} - {resp.text}")
    
    # 3. Test Username Search (Top Trader - Fallback)
    # "risk-manager" was found in top 5 so it should work via fallback
    print("\n3. Testing Username Search (Fallback to API)...")
    username = "risk-manager"
    try:
        response = httpx.get(f"{BASE_URL}/dashboard/search/{username}")
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Success: Resolved '{username}' to {data['wallet_address']}")
            print(f"   Name: {data.get('name')}")
        else:
            print(f"❌ Failed: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"❌ Error: {e}")

    # 4. Test Web Scraping Fallback (BetOnHope)
    print("\n4. Testing Web Scraping Fallback (BetOnHope)...")
    username = "BetOnHope"
    try:
        response = httpx.get(f"{BASE_URL}/dashboard/search/{username}")
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Success: Resolved '{username}' to {data['wallet_address']}")
            print(f"   Name via best effort: {data.get('name')}")
        else:
            print(f"❌ Failed: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"❌ Error: {e}")

    # 5. Test Invalid User (Final Check)
    print("\n5. Testing Invalid User...")
    invalid_user = "nonexistentuser123456789"
    try:
        response = httpx.get(f"{BASE_URL}/dashboard/search/{invalid_user}")
        if response.status_code == 404:
             print(f"✅ Success: Correctly returned 404 for invalid user (after checking scraping)")
        else:
             print(f"❌ Failed: Expected 404, got {response.status_code}")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(test_search())
