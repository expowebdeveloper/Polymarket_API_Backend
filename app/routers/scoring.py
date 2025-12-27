from fastapi import APIRouter, HTTPException
from typing import Dict, Any

from app.services.user_scoring_service import UserScoringService

router = APIRouter(
    prefix="/scoring",
    tags=["Scoring"]
)

@router.get("/{user_address}", response_model=Dict[str, Any])
async def get_user_scores(user_address: str):
    """
    Get comprehensive user scores (Win Rate, ROI, PnL, Risk).
    
    Scores are normalized to a 0-1 range.
    """
    try:
        if not user_address.startswith("0x"):
            # Basic validation
            raise HTTPException(status_code=400, detail="Invalid wallet address format")
            
        scores = await UserScoringService.calculate_all_scores(user_address)
        return scores
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
