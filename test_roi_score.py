"""
Test ROI Score Formula Implementation
Verifies the tanh-based logarithmic ROI scoring across various ROI values.
"""

from app.services.roi_scoring import calculate_roi_score, calculate_roi_score_from_percentage
import math

def test_roi_scoring():
    print("=" * 80)
    print("ROI SCORE FORMULA VERIFICATION")
    print("=" * 80)
    print("\nFormula: ROI_score = (1 + tanh(sign(ROI) · ln(1 + |ROI|) / s_ROI)) / 2")
    print("Sensitivity parameter (s_ROI): 0.6")
    print("=" * 80)
    
    # Test cases covering various ROI scenarios
    test_cases = [
        # Extreme losses
        (-0.90, -90, "Extreme Loss (-90%)"),
        (-0.50, -50, "Large Loss (-50%)"),
        (-0.30, -30, "Moderate Loss (-30%)"),
        (-0.10, -10, "Small Loss (-10%)"),
        (-0.05, -5, "Tiny Loss (-5%)"),
        
        # Break-even
        (0.0, 0, "Break-even (0%)"),
        
        # Small gains
        (0.05, 5, "Tiny Profit (5%)"),
        (0.10, 10, "Small Profit (10%)"),
        (0.20, 20, "Moderate Profit (20%)"),
        (0.30, 30, "Good Profit (30%)"),
        
        # Medium gains
        (0.50, 50, "Strong Profit (50%)"),
        (0.75, 75, "Very Strong Profit (75%)"),
        (1.00, 100, "Excellent Profit (100%)"),
        
        # Large gains
        (1.50, 150, "Outstanding Profit (150%)"),
        (2.00, 200, "Exceptional Profit (200%)"),
        (3.00, 300, "Extraordinary Profit (300%)"),
        (5.00, 500, "Extreme Profit (500%)"),
        (10.00, 1000, "Massive Profit (1000%)"),
    ]
    
    print("\n📊 ROI SCORE TABLE")
    print("-" * 80)
    print(f"{'ROI Decimal':<15} {'ROI %':<10} {'Score':<10} {'Description':<40}")
    print("-" * 80)
    
    for roi_decimal, roi_pct, desc in test_cases:
        score = calculate_roi_score(roi_decimal)
        print(f"{roi_decimal:<15.2f} {roi_pct:<10}% {score:<10.4f} {desc:<40}")
    
    print("\n" + "=" * 80)
    print("✅ KEY PROPERTIES VERIFICATION")
    print("=" * 80)
    
    # Test key properties
    properties = [
        ("Break-even at 0.5", 0.0, 0.5, "ROI = 0 should give score = 0.5"),
        ("Symmetry check", None, None, "Losses and gains should be symmetric"),
        ("Diminishing returns", None, None, "Large ROIs should saturate near 1.0"),
        ("No explosion", 10.0, None, "1000% ROI should not explode"),
    ]
    
    print(f"\n{'Property':<30} {'Test':<20} {'Expected':<15} {'Actual':<15} {'Status':<10}")
    print("-" * 90)
    
    # Test 1: Break-even
    score_zero = calculate_roi_score(0.0)
    status = "✓ PASS" if abs(score_zero - 0.5) < 0.001 else "✗ FAIL"
    print(f"{'Break-even at 0.5':<30} {'ROI = 0':<20} {'0.5000':<15} {score_zero:<15.4f} {status:<10}")
    
    # Test 2: Symmetry
    roi_test = 0.5
    score_pos = calculate_roi_score(roi_test)
    score_neg = calculate_roi_score(-roi_test)
    symmetry_diff = abs((score_pos - 0.5) - (0.5 - score_neg))
    status = "✓ PASS" if symmetry_diff < 0.001 else "✗ FAIL"
    print(f"{'Symmetry check':<30} {'ROI = ±50%':<20} {'Symmetric':<15} {f'Diff: {symmetry_diff:.4f}':<15} {status:<10}")
    
    # Test 3: Positive ROI > 0.5
    score_10pct = calculate_roi_score(0.10)
    status = "✓ PASS" if score_10pct > 0.5 else "✗ FAIL"
    print(f"{'Positive ROI > 0.5':<30} {'ROI = 10%':<20} {'>0.5':<15} {score_10pct:<15.4f} {status:<10}")
    
    # Test 4: Negative ROI < 0.5
    score_neg10pct = calculate_roi_score(-0.10)
    status = "✓ PASS" if score_neg10pct < 0.5 else "✗ FAIL"
    print(f"{'Negative ROI < 0.5':<30} {'ROI = -10%':<20} {'<0.5':<15} {score_neg10pct:<15.4f} {status:<10}")
    
    # Test 5: Diminishing returns
    score_100 = calculate_roi_score(1.0)
    score_1000 = calculate_roi_score(10.0)
    increment = score_1000 - score_100
    status = "✓ PASS" if increment < 0.1 else "✗ FAIL"  # Small increment shows saturation
    print(f"{'Diminishing returns':<30} {'100% → 1000%':<20} {'Small Δ':<15} {f'Δ={increment:.4f}':<15} {status:<10}")
    
    # Test 6: No explosion (bounded output)
    score_extreme = calculate_roi_score(10.0)
    status = "✓ PASS" if 0 <= score_extreme <= 1 else "✗ FAIL"
    print(f"{'No explosion (bounded)':<30} {'ROI = 1000%':<20} {'0-1 range':<15} {score_extreme:<15.4f} {status:<10}")
    
    print("\n" + "=" * 80)
    print("📈 REFERENCE VALUES (from specification)")
    print("=" * 80)
    
    reference_cases = [
        (0.10, 0.579, "10% ROI"),
        (0.50, 0.794, "50% ROI"),
        (1.00, 0.910, "100% ROI"),
    ]
    
    print(f"\n{'ROI':<15} {'Expected':<15} {'Actual':<15} {'Diff':<15} {'Status':<10}")
    print("-" * 70)
    
    all_pass = True
    for roi, expected, desc in reference_cases:
        actual = calculate_roi_score(roi)
        diff = abs(actual - expected)
        status = "✓ PASS" if diff < 0.01 else "✗ FAIL"
        if status == "✗ FAIL":
            all_pass = False
        print(f"{roi:<15.2f} {expected:<15.3f} {actual:<15.3f} {diff:<15.4f} {status:<10} ({desc})")
    
    print("\n" + "=" * 80)
    print("🔄 PERCENTAGE FORMAT TEST")
    print("=" * 80)
    
    print(f"\n{'ROI %':<15} {'Score (decimal)':<20} {'Score (percentage)':<20} {'Match':<10}")
    print("-" * 65)
    
    pct_test_cases = [10.0, 50.0, 100.0, -10.0, -50.0]
    for pct in pct_test_cases:
        score_decimal = calculate_roi_score(pct / 100.0)
        score_pct = calculate_roi_score_from_percentage(pct)
        match = "✓ YES" if abs(score_decimal - score_pct) < 0.0001 else "✗ NO"
        print(f"{pct:<15.1f} {score_decimal:<20.4f} {score_pct:<20.4f} {match:<10}")
    
    print("\n" + "=" * 80)
    if all_pass:
        print("✅ ALL REFERENCE TESTS PASSED")
    else:
        print("⚠️  SOME REFERENCE TESTS FAILED - Review tolerance or implementation")
    print("=" * 80)
    
    # Visualization of the curve
    print("\n📉 SCORE CURVE VISUALIZATION")
    print("=" * 80)
    print("\nROI vs Score relationship:")
    print("-" * 80)
    
    roi_range = [-0.9, -0.5, -0.3, -0.1, 0.0, 0.1, 0.3, 0.5, 1.0, 2.0, 5.0]
    for roi in roi_range:
        score = calculate_roi_score(roi)
        bar_length = int(score * 50)  # Scale to 50 chars
        bar = "█" * bar_length
        roi_pct = roi * 100
        print(f"ROI {roi_pct:>6.0f}%: {score:.4f} {bar}")
    
    print("\n" + "=" * 80)
    print("VERIFICATION COMPLETE")
    print("=" * 80)

if __name__ == "__main__":
    test_roi_scoring()
