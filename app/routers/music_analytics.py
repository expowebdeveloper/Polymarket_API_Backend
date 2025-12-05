from typing import List
from fastapi import APIRouter, HTTPException, Body
from app.schemas.music_analytics import (
    TrackRequest, RemixPerformance, PredictiveAnalysisResult, 
    DemandAnalysisResult, PerformanceAnalysisResult
)
from app.services.music_analytics_service import music_analytics_service

router = APIRouter(
    prefix="/music-analytics",
    tags=["Music Analytics"]
)

@router.post("/predict", response_model=PredictiveAnalysisResult)
async def get_predictions(
    requests: List[TrackRequest],
    history: List[RemixPerformance]
):
    """
    Generate predictive recommendations for new remixes based on demand and historical performance.
    """
    try:
        return music_analytics_service.generate_recommendations(requests, history)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/demand", response_model=DemandAnalysisResult)
async def analyze_demand(requests: List[TrackRequest]):
    return music_analytics_service.analyze_demand(requests)

@router.post("/performance", response_model=PerformanceAnalysisResult)
async def analyze_performance(
    history: List[RemixPerformance], 
    requests: List[TrackRequest]
):
    return music_analytics_service.analyze_performance(history, requests)

@router.get("/mock-demo", response_model=PredictiveAnalysisResult)
async def get_mock_demo():
    """
    Returns a demonstration of the AI logic using mock data.
    """
    # Mock Data
    mock_requests = [
        TrackRequest(artist="Journey", title="Don't Stop Believin'", genre="Rock", era="80s", request_count=500, vote_count=1200, vote_velocity=2.0, category="Classic Rock"),
        TrackRequest(artist="Dire Straits", title="Sultans of Swing", genre="Rock", era="70s", request_count=300, vote_count=800, vote_velocity=15.0, category="Classic Rock"), # High velocity
        TrackRequest(artist="Fleetwood Mac", title="Dreams", genre="Pop Rock", era="70s", request_count=400, vote_count=900, vote_velocity=10.0, category="Soft Rock"),
        TrackRequest(artist="Phil Collins", title="In The Air Tonight", genre="Pop", era="80s", request_count=250, vote_count=600, vote_velocity=8.0, category="80s Pop"),
        TrackRequest(artist="The Weeknd", title="Blinding Lights", genre="Synthwave", era="2020s", request_count=600, vote_count=100, vote_velocity=1.0, category="Modern"),
    ]
    
    mock_history = [
        RemixPerformance(artist="Toto", title="Africa (Remix)", genre="Rock", era="80s", original_track_id="1", remix_title="Africa Club Mix", play_count=1000000, success_score=95.0, release_date="2023-01-01"),
        RemixPerformance(artist="Queen", title="Bohemian Rhapsody (Remix)", genre="Rock", era="70s", original_track_id="2", remix_title="Bohemian Trap", play_count=500000, success_score=85.0, release_date="2023-02-01"),
        RemixPerformance(artist="Eminem", title="Without Me", genre="Hip Hop", era="2000s", original_track_id="3", remix_title="Without Me House Mix", play_count=200000, success_score=60.0, release_date="2023-03-01"),
    ]
    
    return music_analytics_service.generate_recommendations(mock_requests, mock_history)

