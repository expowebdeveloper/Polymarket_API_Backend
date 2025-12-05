from typing import List, Dict, Counter
from app.schemas.music_analytics import (
    TrackRequest, RemixPerformance, Recommendation, 
    DemandAnalysisResult, PerformanceAnalysisResult, PredictiveAnalysisResult
)

class MusicAnalyticsService:
    def analyze_demand(self, requests: List[TrackRequest]) -> DemandAnalysisResult:
        """
        Step 1: Demand Analysis
        - Which tracks are requested most?
        - Which tracks receive the most votes?
        - Which request categories are rising?
        """
        # sort by request count
        top_requested = sorted(requests, key=lambda x: x.request_count, reverse=True)[:10]
        
        # sort by vote count
        top_voted = sorted(requests, key=lambda x: x.vote_count, reverse=True)[:10]
        
        # Identify rising categories based on vote velocity or aggregate velocity
        category_velocity = Counter()
        category_counts = Counter()
        
        for req in requests:
            category_velocity[req.category] += req.vote_velocity
            category_counts[req.category] += 1
            
        # Average velocity per category
        avg_category_velocity = {
            cat: vel / category_counts[cat] 
            for cat, vel in category_velocity.items()
        }
        
        rising_categories = sorted(avg_category_velocity.keys(), key=lambda x: avg_category_velocity[x], reverse=True)[:5]
        
        return DemandAnalysisResult(
            top_requested_tracks=top_requested,
            top_voted_tracks=top_voted,
            rising_categories=rising_categories
        )

    def analyze_performance(self, historical_remixes: List[RemixPerformance], current_demand: List[TrackRequest]) -> PerformanceAnalysisResult:
        """
        Step 2: Performance Analysis
        - Which historical remixes performed the best?
        - Which eras and genres historically do well?
        - Which songs overlap with current demand?
        """
        # Best historical remixes
        best_remixes = sorted(historical_remixes, key=lambda x: x.success_score, reverse=True)[:10]
        
        # Top eras and genres
        genre_scores = Counter()
        era_scores = Counter()
        
        for remix in historical_remixes:
            # Weight by success score
            genre_scores[remix.genre] += remix.success_score
            era_scores[remix.era] += remix.success_score
            
        top_genres = [g for g, _ in genre_scores.most_common(5)]
        top_eras = [e for e, _ in era_scores.most_common(5)]
        
        # Overlap: Demand tracks that match top genres/eras
        overlap_tracks = []
        for req in current_demand:
            if req.genre in top_genres or req.era in top_eras:
                overlap_tracks.append(req)
                
        return PerformanceAnalysisResult(
            best_historical_remixes=best_remixes,
            top_genres=top_genres,
            top_eras=top_eras,
            demand_overlap_tracks=overlap_tracks
        )

    def generate_recommendations(self, requests: List[TrackRequest], historical_remixes: List[RemixPerformance]) -> PredictiveAnalysisResult:
        """
        Step 3: Predictive Recommendations
        "Based on our entire dataset, which new tracks are highly likely to work well as remixes?"
        """
        demand_result = self.analyze_demand(requests)
        perf_result = self.analyze_performance(historical_remixes, requests)
        
        recommendations = []
        
        # Logic:
        # 1. Identify high-potential tracks from requests (high votes/velocity)
        # 2. Boost score if genre/era matches historical success
        # 3. Avoid just picking the #1 requested track ("surface-level")
        
        top_genres = set(perf_result.top_genres)
        top_eras = set(perf_result.top_eras)
        
        candidates = []
        
        for req in requests:
            score = 0.0
            reasons = []
            
            # Base score from demand
            score += req.vote_velocity * 2.0
            score += (req.vote_count / 100.0)
            
            # Boost for historical similarity
            if req.genre in top_genres:
                score *= 1.5
                reasons.append(f"Matches top performing genre: {req.genre}")
            
            if req.era in top_eras:
                score *= 1.3
                reasons.append(f"Matches top performing era: {req.era}")
                
            if req.vote_velocity > 5.0: # Arbitrary threshold for "rising"
                reasons.append("Rising demand in voting patterns")
                
            candidates.append({
                "track": req,
                "score": score,
                "reasons": reasons
            })
            
        # Sort by score
        candidates.sort(key=lambda x: x["score"], reverse=True)
        
        # Filter and format
        # Remove duplicates (by artist+title)
        seen = set()
        final_recs = []
        
        # Skip the absolute #1 requested track if it's too obvious (per "Avoid Surface-Level Results")
        # For this implementation, we'll just ensure we have a mix and good reasoning.
        # The requirement says: "Do not show: 'Today’s Top Prediction = Don’t Stop Believin’' This is simply the #1 requested track"
        # So we might skip the index 0 if it is just the highest requested, but here we are scoring by multiple factors.
        
        for cand in candidates:
            track = cand["track"]
            key = f"{track.artist}-{track.title}"
            
            if key in seen:
                continue
            seen.add(key)
            
            if not cand["reasons"]:
                continue # Skip if no strong reason other than just votes
                
            # Construct specific reason string
            reason_str = ", ".join(cand["reasons"])
            
            # Example logic adjustment: 
            # "Consider remixing more classic rock tracks similar to Rolling Stones and Tom Petty."
            # But MUST INCLUDE exact tracks.
            
            rec = Recommendation(
                artist=track.artist,
                title=track.title,
                reason=f"Recommended due to: {reason_str}",
                confidence_score=cand["score"]
            )
            final_recs.append(rec)
            
            if len(final_recs) >= 5:
                break
                
        return PredictiveAnalysisResult(recommendations=final_recs)

# Global instance
music_analytics_service = MusicAnalyticsService()

