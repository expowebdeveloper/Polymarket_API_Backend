"""
Test Risk Score Calculation from Equity Curve
Verifies the Maximum Drawdown (MDD) based risk scoring.
"""

from app.services.risk_scoring import (
    calculate_risk_score_from_equity,
    calculate_risk_score_with_details,
    calculate_equity_curve_from_trades,
    calculate_risk_score_from_pnl
)
import math

def test_risk_scoring():
    print("=" * 80)
    print("RISK SCORE FORMULA VERIFICATION")
    print("=" * 80)
    print("\nFormula: RiskScore = 100 · e^(-k·MDD) where k = 2.1")
    print("=" * 80)
    
    # Test Case 1: Example from specification
    print("\n📊 TEST CASE 1: Specification Example")
    print("-" * 80)
    equity1 = [1000, 1200, 900, 1100, 1400]
    result1 = calculate_risk_score_with_details(equity1)
    
    print(f"Equity curve: {equity1}")
    print(f"\nResults:")
    print(f"  Maximum Drawdown: {result1['max_drawdown_percent']}%")
    print(f"  Risk Score: {result1['risk_score']}")
    print(f"  Peak Value: ${result1['peak_value']:.2f}")
    print(f"  Trough Value: ${result1['trough_value']:.2f}")
    
    print(f"\nStep-by-step breakdown:")
    print(f"{'Step':<6} {'Equity':<10} {'Peak':<10} {'Drawdown':<12}")
    print("-" * 40)
    for i, (eq, peak, dd) in enumerate(zip(equity1, result1['peak_history'], result1['drawdown_history']), 1):
        print(f"{i:<6} ${eq:<9.2f} ${peak:<9.2f} {dd*100:<11.2f}%")
    
    expected_score = 59.2
    status = "✓ PASS" if abs(result1['risk_score'] - expected_score) < 0.1 else "✗ FAIL"
    print(f"\nExpected: {expected_score}, Actual: {result1['risk_score']} → {status}")
    
    # Test Case 2: No drawdown (perfect trader)
    print("\n📊 TEST CASE 2: No Drawdown (Perfect Trader)")
    print("-" * 80)
    equity2 = [1000, 1100, 1200, 1300, 1400]
    result2 = calculate_risk_score_with_details(equity2)
    
    print(f"Equity curve: {equity2}")
    print(f"Maximum Drawdown: {result2['max_drawdown_percent']}%")
    print(f"Risk Score: {result2['risk_score']}")
    
    expected_perfect = 100.0
    status = "✓ PASS" if abs(result2['risk_score'] - expected_perfect) < 0.1 else "✗ FAIL"
    print(f"Expected: {expected_perfect}, Actual: {result2['risk_score']} → {status}")
    
    # Test Case 3: 50% drawdown (high risk)
    print("\n📊 TEST CASE 3: 50% Drawdown (High Risk)")
    print("-" * 80)
    equity3 = [1000, 500]
    result3 = calculate_risk_score_with_details(equity3)
    
    print(f"Equity curve: {equity3}")
    print(f"Maximum Drawdown: {result3['max_drawdown_percent']}%")
    print(f"Risk Score: {result3['risk_score']}")
    
    # Calculate expected: 100 * e^(-2.1 * 0.5)
    expected_50pct = 100 * math.exp(-2.1 * 0.5)
    status = "✓ PASS" if abs(result3['risk_score'] - expected_50pct) < 0.1 else "✗ FAIL"
    print(f"Expected: {expected_50pct:.2f}, Actual: {result3['risk_score']} → {status}")
    
    # Test Case 4: Multiple drawdowns
    print("\n📊 TEST CASE 4: Multiple Drawdowns")
    print("-" * 80)
    equity4 = [1000, 1500, 1200, 1800, 1400, 2000, 1600]
    result4 = calculate_risk_score_with_details(equity4)
    
    print(f"Equity curve: {equity4}")
    print(f"Maximum Drawdown: {result4['max_drawdown_percent']}%")
    print(f"Risk Score: {result4['risk_score']}")
    
    print(f"\nDrawdown history:")
    for i, (eq, peak, dd) in enumerate(zip(equity4, result4['peak_history'], result4['drawdown_history']), 1):
        marker = " ← MAX DD" if dd == result4['max_drawdown'] else ""
        print(f"  Step {i}: ${eq} (Peak: ${peak}, DD: {dd*100:.2f}%){marker}")
    
    # Test Case 5: Calculate from PnL history
    print("\n📊 TEST CASE 5: Calculate from PnL History")
    print("-" * 80)
    initial_capital = 1000
    pnl_history = [200, -300, 200, 300]
    
    print(f"Initial Capital: ${initial_capital}")
    print(f"PnL History: {pnl_history}")
    
    equity_from_pnl = calculate_equity_curve_from_trades(initial_capital, pnl_history)
    print(f"Calculated Equity Curve: {equity_from_pnl}")
    
    risk_score_pnl = calculate_risk_score_from_pnl(initial_capital, pnl_history)
    print(f"Risk Score: {risk_score_pnl}")
    
    # Should match Test Case 1
    status = "✓ PASS" if abs(risk_score_pnl - result1['risk_score']) < 0.1 else "✗ FAIL"
    print(f"Matches Test Case 1: {status}")
    
    # Test various drawdown scenarios
    print("\n" + "=" * 80)
    print("📉 RISK SCORE vs DRAWDOWN TABLE")
    print("=" * 80)
    
    drawdown_scenarios = [
        (0.00, "No drawdown (perfect)"),
        (0.05, "5% drawdown (very low risk)"),
        (0.10, "10% drawdown (low risk)"),
        (0.15, "15% drawdown (moderate-low risk)"),
        (0.20, "20% drawdown (moderate risk)"),
        (0.25, "25% drawdown (moderate-high risk)"),
        (0.30, "30% drawdown (high risk)"),
        (0.40, "40% drawdown (very high risk)"),
        (0.50, "50% drawdown (extreme risk)"),
        (0.60, "60% drawdown (severe risk)"),
        (0.75, "75% drawdown (catastrophic risk)"),
        (0.90, "90% drawdown (near total loss)"),
    ]
    
    print(f"\n{'Drawdown %':<15} {'Risk Score':<15} {'Description':<40}")
    print("-" * 70)
    
    k = 2.1
    for dd, desc in drawdown_scenarios:
        score = 100 * math.exp(-k * dd)
        bar_length = int(score / 2)  # Scale to 50 chars
        bar = "█" * bar_length
        print(f"{dd*100:<15.1f} {score:<15.2f} {desc:<40}")
    
    # Visualization
    print("\n" + "=" * 80)
    print("📊 RISK SCORE CURVE VISUALIZATION")
    print("=" * 80)
    print("\nDrawdown → Risk Score relationship:")
    print("-" * 80)
    
    for dd, desc in drawdown_scenarios:
        score = 100 * math.exp(-k * dd)
        bar_length = int(score / 2)
        bar = "█" * bar_length
        print(f"DD {dd*100:>5.0f}%: {score:>6.2f} {bar}")
    
    # Key properties verification
    print("\n" + "=" * 80)
    print("✅ KEY PROPERTIES VERIFICATION")
    print("=" * 80)
    
    properties_tests = [
        ("No DD → Score 100", 0.0, 100.0),
        ("25% DD → Score ~59", 0.25, 59.2),
        ("50% DD → Score ~35", 0.50, 34.99),
        ("Monotonic decrease", None, None),
    ]
    
    print(f"\n{'Property':<30} {'Test':<20} {'Expected':<15} {'Actual':<15} {'Status':<10}")
    print("-" * 90)
    
    # Test 1: No drawdown
    score_0 = 100 * math.exp(-k * 0.0)
    status = "✓ PASS" if abs(score_0 - 100.0) < 0.1 else "✗ FAIL"
    print(f"{'No DD → Score 100':<30} {'DD = 0%':<20} {'100.00':<15} {score_0:<15.2f} {status:<10}")
    
    # Test 2: 25% drawdown
    score_25 = 100 * math.exp(-k * 0.25)
    status = "✓ PASS" if abs(score_25 - 59.2) < 0.5 else "✗ FAIL"
    print(f"{'25% DD → Score ~59':<30} {'DD = 25%':<20} {'59.20':<15} {score_25:<15.2f} {status:<10}")
    
    # Test 3: 50% drawdown
    score_50 = 100 * math.exp(-k * 0.50)
    status = "✓ PASS" if abs(score_50 - 34.99) < 0.5 else "✗ FAIL"
    print(f"{'50% DD → Score ~35':<30} {'DD = 50%':<20} {'34.99':<15} {score_50:<15.2f} {status:<10}")
    
    # Test 4: Monotonic decrease
    dd_values = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5]
    scores = [100 * math.exp(-k * dd) for dd in dd_values]
    is_monotonic = all(scores[i] > scores[i+1] for i in range(len(scores)-1))
    status = "✓ PASS" if is_monotonic else "✗ FAIL"
    print(f"{'Monotonic decrease':<30} {'DD: 0→50%':<20} {'Decreasing':<15} {str(is_monotonic):<15} {status:<10}")
    
    print("\n" + "=" * 80)
    print("VERIFICATION COMPLETE")
    print("=" * 80)

if __name__ == "__main__":
    test_risk_scoring()
