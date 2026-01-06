"""
Test Confidence Score Calculation
Verifies the prediction-based confidence scoring formula.
"""

from app.services.confidence_scoring import (
    calculate_confidence_score,
    calculate_confidence_score_percent,
    get_confidence_level,
    calculate_confidence_with_details
)
import math

def test_confidence_scoring():
    print("=" * 80)
    print("CONFIDENCE SCORE FORMULA VERIFICATION")
    print("=" * 80)
    print("\nFormula: Conf(Np) = 1 - exp(-(Np/16)^0.60)")
    print("=" * 80)
    
    # Test cases from specification
    print("\n📊 REFERENCE VALUES (from specification)")
    print("-" * 80)
    
    reference_cases = [
        (10, 0.5296, "10 predictions"),
        (50, 0.8621, "50 predictions"),
        (100, 0.9504, "100 predictions"),
        (300, 0.9970, "300 predictions"),
    ]
    
    print(f"{'Predictions':<15} {'Expected':<15} {'Actual':<15} {'Diff':<15} {'Status':<10}")
    print("-" * 70)
    
    all_pass = True
    for num_pred, expected, desc in reference_cases:
        actual = calculate_confidence_score(num_pred)
        diff = abs(actual - expected)
        status = "✓ PASS" if diff < 0.001 else "✗ FAIL"
        if status == "✗ FAIL":
            all_pass = False
        print(f"{num_pred:<15} {expected:<15.4f} {actual:<15.4f} {diff:<15.6f} {status:<10}")
    
    # Comprehensive test cases
    print("\n📊 COMPREHENSIVE CONFIDENCE TABLE")
    print("-" * 80)
    
    test_cases = [
        (0, "No predictions"),
        (1, "Single prediction"),
        (5, "Very few predictions"),
        (10, "Few predictions"),
        (16, "Scale threshold"),
        (20, "Moderate predictions"),
        (30, "Good activity"),
        (50, "Active trader"),
        (75, "Very active trader"),
        (100, "Highly active trader"),
        (150, "Expert trader"),
        (200, "Professional trader"),
        (300, "Whale trader"),
        (500, "Super whale"),
    ]
    
    print(f"{'Predictions':<15} {'Score':<10} {'Percent':<10} {'Level':<20} {'Description':<30}")
    print("-" * 85)
    
    for num_pred, desc in test_cases:
        score = calculate_confidence_score(num_pred)
        percent = calculate_confidence_score_percent(num_pred)
        level = get_confidence_level(score)
        print(f"{num_pred:<15} {score:<10.4f} {percent:<10.2f}% {level:<20} {desc:<30}")
    
    # Detailed breakdown for a specific case
    print("\n📊 DETAILED BREAKDOWN (50 predictions)")
    print("-" * 80)
    
    num_pred = 50
    details = calculate_confidence_with_details(num_pred)
    
    print(f"Number of Predictions: {details['num_predictions']}")
    print(f"Confidence Score (0-1): {details['confidence_score']}")
    print(f"Confidence Percent: {details['confidence_percent']}%")
    print(f"Confidence Level: {details['confidence_level']}")
    print(f"Scale Constant: {details['scale']}")
    print(f"Exponent: {details['exponent']}")
    
    # Step-by-step calculation
    print(f"\nStep-by-step calculation:")
    x = num_pred / 16.0
    print(f"  1. Normalize: x = {num_pred}/16 = {x:.4f}")
    
    y = math.pow(x, 0.60)
    print(f"  2. Apply exponent: y = x^0.60 = {y:.4f}")
    
    z = math.exp(-y)
    print(f"  3. Exponential decay: z = exp(-y) = {z:.4f}")
    
    conf = 1.0 - z
    print(f"  4. Final confidence: 1 - z = {conf:.4f}")
    
    # Key properties verification
    print("\n" + "=" * 80)
    print("✅ KEY PROPERTIES VERIFICATION")
    print("=" * 80)
    
    print(f"\n{'Property':<40} {'Test':<20} {'Status':<10}")
    print("-" * 70)
    
    # Test 1: Zero predictions
    score_0 = calculate_confidence_score(0)
    status = "✓ PASS" if score_0 == 0.0 else "✗ FAIL"
    print(f"{'Zero predictions → 0 confidence':<40} {f'{score_0:.4f}':<20} {status:<10}")
    
    # Test 2: Monotonic increase
    scores = [calculate_confidence_score(n) for n in [10, 20, 30, 40, 50]]
    is_monotonic = all(scores[i] < scores[i+1] for i in range(len(scores)-1))
    status = "✓ PASS" if is_monotonic else "✗ FAIL"
    print(f"{'Monotonic increase (more trades → higher conf)':<40} {str(is_monotonic):<20} {status:<10}")
    
    # Test 3: Bounded to [0, 1]
    extreme_scores = [calculate_confidence_score(n) for n in [0, 100, 1000, 10000]]
    all_bounded = all(0 <= s <= 1 for s in extreme_scores)
    status = "✓ PASS" if all_bounded else "✗ FAIL"
    print(f"{'Always bounded to [0, 1]':<40} {str(all_bounded):<20} {status:<10}")
    
    # Test 4: Diminishing returns
    score_100 = calculate_confidence_score(100)
    score_200 = calculate_confidence_score(200)
    score_300 = calculate_confidence_score(300)
    
    increment_1 = score_200 - score_100
    increment_2 = score_300 - score_200
    
    has_diminishing = increment_2 < increment_1
    status = "✓ PASS" if has_diminishing else "✗ FAIL"
    print(f"{'Diminishing returns (100→200 > 200→300)':<40} {f'Δ1={increment_1:.4f} > Δ2={increment_2:.4f}':<20} {status:<10}")
    
    # Test 5: Fast early growth
    score_10 = calculate_confidence_score(10)
    score_20 = calculate_confidence_score(20)
    early_growth = score_20 - score_10
    
    status = "✓ PASS" if early_growth > 0.2 else "✗ FAIL"
    print(f"{'Fast early growth (10→20 predictions)':<40} {f'Δ={early_growth:.4f}':<20} {status:<10}")
    
    # Visualization
    print("\n" + "=" * 80)
    print("📈 CONFIDENCE CURVE VISUALIZATION")
    print("=" * 80)
    print("\nPredictions → Confidence relationship:")
    print("-" * 80)
    
    vis_cases = [0, 5, 10, 20, 30, 50, 75, 100, 150, 200, 300, 500]
    for num_pred in vis_cases:
        score = calculate_confidence_score(num_pred)
        bar_length = int(score * 50)
        bar = "█" * bar_length
        print(f"Np {num_pred:>4}: {score:.4f} {bar}")
    
    # Confidence levels distribution
    print("\n" + "=" * 80)
    print("📊 CONFIDENCE LEVELS DISTRIBUTION")
    print("=" * 80)
    
    level_ranges = {
        "Very Low": (0, 0.30),
        "Low": (0.30, 0.50),
        "Moderate": (0.50, 0.70),
        "Moderate-High": (0.70, 0.85),
        "High": (0.85, 0.95),
        "Very High": (0.95, 1.0),
    }
    
    print(f"\n{'Level':<20} {'Score Range':<20} {'Approx. Predictions':<30}")
    print("-" * 70)
    
    for level, (min_score, max_score) in level_ranges.items():
        # Find approximate prediction count for this range
        approx_min = None
        approx_max = None
        
        for n in range(0, 501):
            score = calculate_confidence_score(n)
            if approx_min is None and score >= min_score:
                approx_min = n
            if score >= max_score:
                approx_max = n
                break
        
        if approx_max is None:
            approx_max = "500+"
        
        pred_range = f"{approx_min}-{approx_max}" if approx_min is not None else "N/A"
        print(f"{level:<20} {f'{min_score:.2f} - {max_score:.2f}':<20} {pred_range:<30}")
    
    print("\n" + "=" * 80)
    if all_pass:
        print("✅ ALL REFERENCE TESTS PASSED")
    else:
        print("⚠️  SOME REFERENCE TESTS FAILED")
    print("=" * 80)

if __name__ == "__main__":
    test_confidence_scoring()
