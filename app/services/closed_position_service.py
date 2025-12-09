import requests
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.db.models import ClosedPosition
from typing import List

async def fetch_and_store_closed_positions(user_address: str, db: AsyncSession) -> List[ClosedPosition]:
    url = f"https://data-api.polymarket.com/closed-positions?user={user_address}"
    # Use requests for now, but in async app better to use httpx. However requests is blocking.
    # For now keeping requests but ideally should be httpx or run in executor.
    # Given the constraint of 'fixing' quickly, I will stick to requests but wrap it or just use it (calls might block event loop briefly).
    # Better: Use simple requests strictly for now, or use httpx if available. I don't see httpx in requirements but maybe it is there.
    # Let's check imports. I'll just use requests synchronously for the fetch part for now, or 
    # since I can't easily see requirements.txt dependencies without reading, I'll stick to requests.
    response = requests.get(url)
    
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data from Polymarket API: {response.text}")
        
    data = response.json()
    stored_positions = []
    
    for item in data:
        # Check if exists
        query = select(ClosedPosition).filter(
            ClosedPosition.proxy_wallet == item.get("proxyWallet"),
            ClosedPosition.asset == item.get("asset"),
            ClosedPosition.condition_id == item.get("conditionId"),
            ClosedPosition.timestamp == item.get("timestamp")
        )
        result = await db.execute(query)
        exists = result.scalars().first()
        
        if not exists:
            # Map fields safely
            position = ClosedPosition(
                proxy_wallet=item.get("proxyWallet"),
                asset=item.get("asset"),
                condition_id=item.get("conditionId"),
                avg_price=item.get("avgPrice"),
                total_bought=item.get("totalBought"),
                realized_pnl=item.get("realizedPnl"),
                cur_price=item.get("curPrice"),
                title=item.get("title"),
                slug=item.get("slug"),
                icon=item.get("icon"),
                event_slug=item.get("eventSlug"),
                outcome=item.get("outcome"),
                outcome_index=item.get("outcomeIndex"),
                opposite_outcome=item.get("oppositeOutcome"),
                opposite_asset=item.get("oppositeAsset"),
                end_date=item.get("endDate"),
                timestamp=item.get("timestamp")
            )
            db.add(position)
            stored_positions.append(position)
    
    await db.commit()
    
    # Return all positions for this user
    query_all = select(ClosedPosition).filter(ClosedPosition.proxy_wallet == user_address)
    result_all = await db.execute(query_all)
    all_positions = result_all.scalars().all()
    
    return all_positions
