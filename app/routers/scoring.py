from fastapi import APIRouter, HTTPException, Depends
from typing import Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.user_scoring_service import UserScoringService
from app.services.leaderboard_service import (
    calculate_trader_metrics_with_time_filter,
    calculate_scores_and_rank
)
from app.db.session import get_db
from app.schemas.scoring import ScoringV2Response
from app.core.scoring_config import default_scoring_config

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

@router.get("/v2/{user_address}", response_model=ScoringV2Response)
async def get_user_scores_v2(user_address: str, db: AsyncSession = Depends(get_db)):
    """
    Get comprehensive user scores using the latest deterministic scoring logic from the database.
    
    Fields included:
    - w_shrunk: Win Rate with reliability correction (mapped from win_rate in latest logic)
    - pnl_score: PnL Score (0-1)
    - win_rate: Raw Win Rate (%)
    - win_rate_percent: Raw Win Rate (%)
    - final_rating: Weighted combination of scores (0-100)
    - confidence_score: Confidence multiplier (0-1)
    - roi_score: ROI Score (0-1)
    - risk_score: Risk Score (0-1)
    """
    try:
        if not user_address.startswith("0x"):
            raise HTTPException(status_code=400, detail="Invalid wallet address format")
            
        from app.services.data_fetcher import fetch_leaderboard_stats
        official_stats = await fetch_leaderboard_stats(user_address)
        
        # 1. Fetch raw metrics from DB (all time)
        metrics = await calculate_trader_metrics_with_time_filter(db, user_address, period='all')
        
        if not metrics or metrics.get('total_trades', 0) == 0:
            # If no data in DB, try to construct from official stats
            if official_stats and (official_stats.get("pnl") != 0 or official_stats.get("volume") != 0):
                metrics = {
                    "wallet_address": user_address,
                    "total_pnl": official_stats.get("pnl", 0.0),
                    "total_volume": official_stats.get("volume", 0.0),
                    "roi": (official_stats.get("pnl", 0.0) / official_stats.get("volume", 1.0) * 100) if official_stats.get("volume", 0) > 0 else 0.0,
                    "total_trades": 1, # Minimal trades to allow scoring
                    "winning_trades": 1 if official_stats.get("pnl", 0) > 0 else 0,
                    "avg_stake": 0.0,
                    "win_rate": 50.0 # Default
                }
            else:
                # Return empty/zero scores if no data at all
                return ScoringV2Response(
                    wallet_address=user_address,
                    w_shrunk=0.0,
                    pnl_score=0.0,
                    win_rate=0.0,
                    win_rate_percent=0.0,
                    final_rating=0.0,
                    confidence_score=0.0,
                    roi_score=0.0,
                    risk_score=0.0,
                    total_pnl=0.0,
                    roi=0.0,
                    total_trades=0,
                    winning_trades=0
                )

        # 2. OVERRIDE: Prioritize official PnL and Volume for scoring
        if official_stats:
            metrics["total_pnl"] = official_stats.get("pnl", metrics.get("total_pnl", 0.0))
            metrics["total_volume"] = official_stats.get("volume", metrics.get("total_volume", 0.0))
            if metrics["total_volume"] > 0:
                metrics["roi"] = (metrics["total_pnl"] / metrics["total_volume"]) * 100

        # 2. Calculate scores using the latest logic
        # calculate_scores_and_rank adds score_win_rate, score_roi, score_pnl, score_risk, confidence_score, final_score
        scored_metrics_list = calculate_scores_and_rank([metrics])
        scored_metrics = scored_metrics_list[0]
        
        # 3. Calculate final_rating (the weighted sum before confidence multiplier)
        # Use default_scoring_config attributes directly
        risk_val = max(0.0, min(1.0, scored_metrics.get('score_risk', 0.0)))
        
        base_rating = (
            default_scoring_config.weight_win_rate * scored_metrics.get('score_win_rate', 0.0) +
            default_scoring_config.weight_roi * scored_metrics.get('score_roi', 0.0) +
            default_scoring_config.weight_pnl * scored_metrics.get('score_pnl', 0.0) +
            default_scoring_config.weight_risk * (1.0 - risk_val)
        ) * 100.0
        
        return ScoringV2Response(
            wallet_address=user_address,
            w_shrunk=scored_metrics.get('W_shrunk', 0.0),
            pnl_score=scored_metrics.get('score_pnl', 0.0),
            win_rate=scored_metrics.get('win_rate', 0.0),
            win_rate_percent=scored_metrics.get('win_rate', 0.0),
            final_rating=round(base_rating, 2),
            confidence_score=scored_metrics.get('confidence_score', 0.0),
            roi_score=scored_metrics.get('score_roi', 0.0),
            risk_score=scored_metrics.get('score_risk', 0.0),
            total_pnl=scored_metrics.get('total_pnl', 0.0),
            roi=scored_metrics.get('roi', 0.0),
            total_trades=scored_metrics.get('total_trades', 0),
            winning_trades=scored_metrics.get('winning_trades', 0)
        )
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
