from typing import List, Dict, Optional
from pydantic import BaseModel, Field

class Track(BaseModel):
    artist: str
    title: str
    genre: str
    era: str
    bpm: Optional[int] = None
    key: Optional[str] = None

class TrackRequest(Track):
    request_count: int
    vote_count: int
    vote_velocity: float  # e.g., votes per day
    category: str  # e.g., "Classic Rock", "90s Pop"

class RemixPerformance(Track):
    original_track_id: str
    remix_title: str
    play_count: int
    success_score: float  # 0-100
    release_date: str

class Recommendation(BaseModel):
    artist: str
    title: str
    reason: str
    confidence_score: float

class DemandAnalysisResult(BaseModel):
    top_requested_tracks: List[TrackRequest]
    top_voted_tracks: List[TrackRequest]
    rising_categories: List[str]

class PerformanceAnalysisResult(BaseModel):
    best_historical_remixes: List[RemixPerformance]
    top_eras: List[str]
    top_genres: List[str]
    demand_overlap_tracks: List[Track]

class PredictiveAnalysisResult(BaseModel):
    recommendations: List[Recommendation]

