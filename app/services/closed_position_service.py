import requests
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.db.models import ClosedPosition
from typing import List

async def fetch_and_store_closed_positions(user_address: str, db: AsyncSession) -> List[ClosedPosition]:
    """
    Fetch all closed positions from Polymarket API using pagination and store them in the database.
    This ensures we get ALL closed positions, not just the first batch.
    """
    url = "https://data-api.polymarket.com/closed-positions"
    params = {"user": user_address}
    
    # Fetch ALL data using pagination (similar to fetch_closed_positions in data_fetcher.py)
    all_positions_data = []
    fetch_limit = 1000  # Fetch in chunks
    current_offset = 0
    
    while True:
        params["limit"] = fetch_limit
        params["offset"] = current_offset
        
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code != 200:
            raise Exception(f"Failed to fetch data from Polymarket API: {response.text}")
        
        data = response.json()
        if not isinstance(data, list) or not data:
            # Only break when we get 0 items (empty list)
            # Don't break if we get fewer items than requested, as the API might cap the limit
            break
        
        all_positions_data.extend(data)
        
        # Increment offset by the number of received items
        # Polymarket API might cap limit at 50 even if we ask for 1000
        # So we only stop if we get 0 items (handled above)
        current_offset += len(data)
        
        # Continue fetching until we get an empty response
        # Don't break on len(data) < fetch_limit because the API might have server-side limits
    
    stored_positions = []
    
    for item in all_positions_data:
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
