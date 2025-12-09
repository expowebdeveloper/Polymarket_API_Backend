from typing import List, Dict, Any, Optional
import math
from app.services.data_fetcher import (
    fetch_positions_for_wallet,
    fetch_closed_positions,
    fetch_portfolio_value,
    fetch_leaderboard_stats
)

class UserScoringService:
    # --- Constants & Hyperparameters ---
    
    # Win Rate Constants
    K_W = 50       # Shrink constant for Win Rate
    B = 0.5        # Baseline Win Rate (50%)
    
    # ROI Constants
    K_R = 50       # Shrink constant for ROI
    ROI_MEDIAN = 0.0 # Baseline/Median ROI (0%)
    
    # PnL Constants
    K_P = 50       # Shrink constant for PnL
    ALPHA = 4      # Whale penalty strength
    PNL_MEDIAN = 0.0 # Baseline PnL ($0)

    # --- Percentile Anchors (Placeholders) ---
    # In a real system, these would be fetched from a DB of all traders.
    # We are using reasonable estimates for now.
    
    # Win Rate Anchors (0.30 - 0.70 range covers most active traders)
    W_1_PERCENT = 30.0
    W_99_PERCENT = 70.0
    
    # ROI Anchors (-50% to +100% range)
    ROI_1_PERCENT = -50.0
    ROI_99_PERCENT = 100.0
    
    # PnL Anchors (-$1000 to +$5000)
    PNL_1_PERCENT = -1000.0
    PNL_99_PERCENT = 5000.0
    
    @staticmethod
    def _clamp(value: float, min_val: float, max_val: float) -> float:
        return max(min_val, min(value, max_val))

    @staticmethod
    def _normalize(value: float, p1: float, p99: float) -> float:
        """Applies Min-Max normalization based on percentiles, clamped to [0, 1]."""
        if p99 == p1:
            return 0.5 # Avoid division by zero
        score = (value - p1) / (p99 - p1)
        return UserScoringService._clamp(score, 0.0, 1.0)
    
    @staticmethod
    def calculate_effective_trade_mass(stakes: List[float]) -> float:
        """
        Step 2: Effective Trade Mass (Hill Estimator).
        N_eff = (Sum s_i)^2 / Sum (s_i^2)
        """
        if not stakes:
            return 0.0
        
        sum_stakes = sum(stakes)
        sum_sq_stakes = sum(s**2 for s in stakes)
        
        if sum_sq_stakes == 0:
            return 0.0
            
        return (sum_stakes ** 2) / sum_sq_stakes

    @staticmethod
    def calculate_win_rate_score(closed_positions: List[Dict]) -> Dict[str, float]:
        """
        Formula 1: Win Rate Score (W_score)
        """
        if not closed_positions:
            return {"score": 0.0, "raw_win_rate": 0.0, "shrunk_win_rate": 0.0, "n_eff": 0.0}

        stakes = []
        winning_stakes = 0.0
        wins = 0
        total_closed = len(closed_positions)
        
        for pos in closed_positions:
            size = float(pos.get("totalBought", 0.0)) # approximated from totalBought (size)
            # improved stake extraction logic if available, currently using size * avgPrice if possible, 
            # but usually closed positions api gives realizedPnl, totalBought (shares), avgPrice (entry)
            avg_price = float(pos.get("avgPrice", 0.0))
            
            # Stake = Cost Basis
            stake = size * avg_price
            stakes.append(stake)
            
            realized_pnl = float(pos.get("realizedPnl", 0.0))
            
            if realized_pnl > 0:
                wins += 1
                winning_stakes += stake

        total_stake = sum(stakes)
        
        # Step 1: Stake-Weighted Effective Win Rate (W)
        w_raw = (winning_stakes / total_stake * 100) if total_stake > 0 else 0.0
        
        # Step 2: N_eff
        n_eff = UserScoringService.calculate_effective_trade_mass(stakes)
        
        # Step 3: Shrink Win Rate (Reliability Correction)
        # W_shrunk = (W * N_eff + B * K_W) / (N_eff + K_W)
        # Note: B is 0.5 (50%), so we use 50.0 for percentage calc
        b_percent = UserScoringService.B * 100
        w_shrunk = (w_raw * n_eff + b_percent * UserScoringService.K_W) / (n_eff + UserScoringService.K_W)
        
        # Step 4: Percentile Normalization
        # Ignore traders with < 5 trades
        if total_closed < 5:
            # Fallback for few trades: just return 0.5 (middle) or low score? 
            # Request says "Ignore traders with < 5 trades" for computing anchors, 
            # but for SCORING a specific trader, we should probably still score them, 
            # but essentially the shrink logic handles low reliability. 
            # However, usually we might want to penalize or neutral score.
            # Let's proceed with calculations as the Shrinkage (Step 3) ALREADY 
            # strongly pulls them to the mean (50%) if N_eff (correlated with count) is low.
            pass

        w_score = UserScoringService._normalize(
            w_shrunk, 
            UserScoringService.W_1_PERCENT, 
            UserScoringService.W_99_PERCENT
        )
        
        return {
            "score": w_score,
            "raw_win_rate": w_raw,
            "shrunk_win_rate": w_shrunk,
            "n_eff": n_eff
        }

    @staticmethod
    def calculate_roi_score(
        roi_percentage: float, 
        closed_positions: List[Dict]
    ) -> Dict[str, float]:
        """
        Formula 2: ROI Score (R_score)
        """
        # Calculate N_eff again (or pass it in if optimizing)
        stakes = []
        for pos in closed_positions:
            size = float(pos.get("totalBought", 0.0))
            avg_price = float(pos.get("avgPrice", 0.0))
            stakes.append(size * avg_price)
            
        n_eff = UserScoringService.calculate_effective_trade_mass(stakes)
        
        # Step 1: Shrink ROI
        # ROI_shrunk = (ROI * N_eff + ROI_m * K_R) / (N_eff + K_R)
        roi_shrunk = (roi_percentage * n_eff + UserScoringService.ROI_MEDIAN * UserScoringService.K_R) / (n_eff + UserScoringService.K_R)
        
        # Step 2: Normalize
        roi_score = UserScoringService._normalize(
            roi_shrunk,
            UserScoringService.ROI_1_PERCENT,
            UserScoringService.ROI_99_PERCENT
        )
        
        return {
            "score": roi_score,
            "shrunk_roi": roi_shrunk,
            "n_eff": n_eff
        }

    @staticmethod
    def calculate_pnl_score(
        total_pnl: float, 
        closed_positions: List[Dict]
    ) -> Dict[str, float]:
        """
        Formula 3: PnL Score (P_score)
        """
        if not closed_positions:
             return {"score": 0.0, "adj_pnl": 0.0, "shrunk_pnl": 0.0}

        stakes = []
        max_stake = 0.0
        sum_stakes = 0.0
        
        for pos in closed_positions:
            size = float(pos.get("totalBought", 0.0))
            avg_price = float(pos.get("avgPrice", 0.0))
            stake = size * avg_price
            stakes.append(stake)
            sum_stakes += stake
            if stake > max_stake:
                max_stake = stake
                
        # N_eff
        n_eff = UserScoringService.calculate_effective_trade_mass(stakes)
        
        # Step 1: Adjust PnL (Whale Distortion)
        # PnL_adj = PnL_total / (1 + Alpha * (max_s_i / S))
        # If sum_stakes is 0, avoid error
        ratio = (max_stake / sum_stakes) if sum_stakes > 0 else 0.0
        pnl_adj = total_pnl / (1 + UserScoringService.ALPHA * ratio)
        
        # Step 2: Shrink PnL
        # PnL_shrunk = (PnL_adj * N_eff + PnL_m * K_P) / (N_eff + K_P)
        pnl_shrunk = (pnl_adj * n_eff + UserScoringService.PNL_MEDIAN * UserScoringService.K_P) / (n_eff + UserScoringService.K_P)
        
        # Step 3: Normalize
        p_score = UserScoringService._normalize(
            pnl_shrunk,
            UserScoringService.PNL_1_PERCENT,
            UserScoringService.PNL_99_PERCENT
        )
        
        return {
            "score": p_score,
            "total_pnl": total_pnl,
            "adj_pnl": pnl_adj,
            "shrunk_pnl": pnl_shrunk,
            "whale_ratio": ratio
        }

    @staticmethod
    def calculate_risk_score(
        closed_positions: List[Dict], 
        current_portfolio_value: float,
        total_pnl: float = 0.0
    ) -> Dict[str, float]:
        """
        Formula 4: Risk Score (Risk_score)
        """
        if not closed_positions:
             return {"score": 0.5, "worst_loss": 0.0, "loss_pct": 0.0}
        
        # Step 1: Find Worst Loss
        worst_loss = 0.0 # Should be negative or 0
        
        for pos in closed_positions:
            pnl = float(pos.get("realizedPnl", 0.0))
            if pnl < worst_loss:
                worst_loss = pnl
                
        # Capital Approximation
        # If we use strict Current Portfolio Value, a user who lost 90% of funds
        # will have Loss / Current = Huge %.
        # We reconstruct "Effective Capital" as Current Value - Total PnL (Net Profit/Loss).
        # Example: Start 1000. Lost 400. PnL = -400. Current = 600.
        # Est Capital = 600 - (-400) = 1000.
        # Example: Start 1000. Won 200. PnL = +200. Current = 1200.
        # Est Capital = 1200 - 200 = 1000.
        
        adjusted_capital = current_portfolio_value
        if total_pnl < 0:
             adjusted_capital -= total_pnl # Add back the losses to find base capital
        
        # Ensure we don't divide by zero or negative (if withdrawn everything)
        capital = max(adjusted_capital, 1.0)
        
        # Loss % = |Worst Loss| / Capital
        loss_pct = abs(worst_loss) / capital
        
        # Step 2: Compute Score
        risk_score = 1.0 - loss_pct
        risk_score = max(0.0, risk_score)
        
        return {
            "score": float(risk_score),
            "worst_loss": worst_loss,
            "loss_pct": loss_pct,
            "capital_proxy": capital
        }

    @staticmethod
    def calculate_all_scores(user_address: str) -> Dict[str, Any]:
        """
        Aggregate all scores for a user.
        """
        # 1. Fetch Data
        closed_positions = fetch_closed_positions(user_address)
        leaderboard = fetch_leaderboard_stats(user_address)
        portfolio_val = fetch_portfolio_value(user_address)
        
        total_pnl = leaderboard.get("pnl", 0.0)
        total_vol = leaderboard.get("volume", 0.0)
        
        # Fallback: If Leaderboard is empty/zero but we have closed positions,
        # calculate PnL and Volume manually.
        if (total_pnl == 0.0 and total_vol == 0.0) and closed_positions:
            calc_pnl = 0.0
            calc_vol = 0.0
            for pos in closed_positions:
                size = float(pos.get("totalBought", 0.0))
                avg_price = float(pos.get("avgPrice", 0.0))
                pnl = float(pos.get("realizedPnl", 0.0))
                
                # Volume = value of buy side.
                # Strictly speaking volume is buy+sell, but "Investment" is usually stake.
                stake = size * avg_price
                calc_vol += stake
                calc_pnl += pnl
            
            total_pnl = calc_pnl
            total_vol = calc_vol
        
        # Calculate raw ROI for input
        raw_roi = (total_pnl / total_vol * 100) if total_vol > 0 else 0.0
        
        # 2. Calculate Individual Scores
        win_metrics = UserScoringService.calculate_win_rate_score(closed_positions)
        roi_metrics = UserScoringService.calculate_roi_score(raw_roi, closed_positions)
        pnl_metrics = UserScoringService.calculate_pnl_score(total_pnl, closed_positions)
        risk_metrics = UserScoringService.calculate_risk_score(closed_positions, portfolio_val, total_pnl)
        
        return {
            "user_address": user_address,
            "scores": {
                "win_score": round(win_metrics["score"], 4),
                "roi_score": round(roi_metrics["score"], 4),
                "pnl_score": round(pnl_metrics["score"], 4),
                "risk_score": round(risk_metrics["score"], 4)
            },
            "details": {
                "win": win_metrics,
                "roi": roi_metrics,
                "pnl": pnl_metrics,
                "risk": risk_metrics
            }
        }
