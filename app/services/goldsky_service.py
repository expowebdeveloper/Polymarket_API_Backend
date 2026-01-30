import httpx
import asyncio
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from app.core.config import settings

class GoldskyService:
    """Service to interact with Goldsky GraphQL API."""
    
    @staticmethod
    async def fetch_volume_leaderboard(time_period: str = "all", limit: int = 100) -> List[Dict]:
        """
        Fetch volume leaderboard from Goldsky subgraph.
        
        Args:
            time_period: 'day', 'week', 'month', 'all'
            limit: Number of traders to return
            
        Returns:
            List of dicts with keys: maker, volume, count
        """
        url = settings.GOLDSKY_SUBGRAPH_URL
        
        # Calculate timestamp threshold
        now = datetime.utcnow()
        if time_period == 'day':
            start_time = int((now - timedelta(days=1)).timestamp())
        elif time_period == 'week':
            start_time = int((now - timedelta(days=7)).timestamp())
        elif time_period == 'month':
            start_time = int((now - timedelta(days=30)).timestamp())
        else:
            start_time = 0

        query = """
        query GetLeaderboard($startTime: BigInt!, $first: Int!) {
            orderFilleds(
                first: $first, 
                where: { timestamp_gte: $startTime },
                orderBy: timestamp,
                orderDirection: desc
            ) {
                maker
                makerAmount
                timestamp
            }
        }
        """
        
        variables = {
            "startTime": str(start_time),
            "first": 1000  # Fetch enough to aggregate
        }

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    url, 
                    json={"query": query, "variables": variables},
                    headers={"Content-Type": "application/json"},
                    timeout=10.0
                )
                
                if response.status_code != 200:
                    print(f"Goldsky API Error: {response.status_code} - {response.text}")
                    return []

                data = response.json()
                
                if "errors" in data:
                    print(f"Goldsky GraphQL Errors: {data['errors']}")
                    return []
                
                orders = data.get("data", {}).get("orderFilleds", [])
                
                # Aggregate volume by maker
                leaderboard = {}
                for order in orders:
                    maker = order.get("maker")
                    amount = float(order.get("makerAmount", 0))
                    
                    if maker not in leaderboard:
                        leaderboard[maker] = {"volume": 0.0, "count": 0}
                    
                    leaderboard[maker]["volume"] += amount
                    leaderboard[maker]["count"] += 1
                
                # Convert to list and sort
                result = [
                    {"wallet_address": k, "volume": v["volume"], "total_trades": v["count"]}
                    for k, v in leaderboard.items()
                ]
                
                # Sort by volume desc
                result.sort(key=lambda x: x["volume"], reverse=True)
                
                return result[:limit]

        except Exception as e:
            print(f"Error fetching from Goldsky: {e}")
            return []
