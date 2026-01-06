"""
Test PnL Score Formula Implementation
Verifies the logarithmic interpolation scoring across all profit and loss zones.
"""

from app.services.scoring_engine import calculate_pnl_score
import math

def test_pnl_scoring():
    print("=" * 80)
    print("PnL SCORE FORMULA VERIFICATION")
    print("=" * 80)
    
    # Test cases covering all zones
    test_cases = [
        # Extreme losses
        (-50000, "Extreme Loss"),
        (-15000, "Large Loss"),
        
        # Loss zones
        (-5000, "Loss Zone 3: -10000 to -1000"),
        (-500, "Loss Zone 2: -1000 to -100"),
        (-50, "Loss Zone 1: -100 to 0"),
        
        # Zero/tiny profit
        (0, "Zero PnL"),
        (0.5, "Tiny Profit"),
        
        # Profit zones
        (50, "Profit Zone 1: 0 to 100"),
        (500, "Profit Zone 2: 100 to 1000"),
        (2500, "Profit Zone 3: 1000 to 5000"),
        (7500, "Profit Zone 4: 5000 to 10000"),
        (25000, "Profit Zone 5: 10000 to 50000"),
        (75000, "Profit Zone 6: 50000 to 100000"),
        (250000, "Profit Zone 7: 100000 to 500000"),
        (750000, "Profit Zone 8: 500000 to 1000000"),
        (1500000, "Extreme Profit (>1M)"),
    ]
    
    print("\n📊 LOSS ZONES (Updated to Final Spec)")
    print("-" * 80)
    print(f"{'PnL':<15} {'Score':<10} {'Expected Range':<20} {'Description':<30}")
    print("-" * 80)
    
    loss_cases = [tc for tc in test_cases if tc[0] < 0]
    for pnl, desc in loss_cases:
        score = calculate_pnl_score(pnl)
        
        # Determine expected range
        abs_pnl = abs(pnl)
        if abs_pnl < 100:
            exp_range = "0.20 → 0.15"
        elif abs_pnl < 1000:
            exp_range = "0.15 → 0.10"
        elif abs_pnl < 10000:
            exp_range = "0.10 → 0.05"
        else:
            exp_range = "0.05 → 0.00"
        
        print(f"{pnl:<15.2f} {score:<10.4f} {exp_range:<20} {desc:<30}")
    
    print("\n📈 PROFIT ZONES")
    print("-" * 80)
    print(f"{'PnL':<15} {'Score':<10} {'Expected Range':<20} {'Description':<30}")
    print("-" * 80)
    
    profit_cases = [tc for tc in test_cases if tc[0] >= 0]
    for pnl, desc in profit_cases:
        score = calculate_pnl_score(pnl)
        
        # Determine expected range
        if pnl < 1:
            exp_range = "~0.15"
        elif pnl < 100:
            exp_range = "0.15 → 0.25"
        elif pnl < 1000:
            exp_range = "0.25 → 0.40"
        elif pnl < 5000:
            exp_range = "0.40 → 0.60"
        elif pnl < 10000:
            exp_range = "0.60 → 0.75"
        elif pnl < 50000:
            exp_range = "0.75 → 0.85"
        elif pnl < 100000:
            exp_range = "0.85 → 0.92"
        elif pnl < 500000:
            exp_range = "0.92 → 0.98"
        elif pnl < 1000000:
            exp_range = "0.98 → 0.999"
        else:
            exp_range = "1.00"
        
        print(f"{pnl:<15.2f} {score:<10.4f} {exp_range:<20} {desc:<30}")
    
    print("\n" + "=" * 80)
    print("✅ BOUNDARY VERIFICATION")
    print("=" * 80)
    
    # Test key boundaries
    boundaries = [
        # Loss boundaries (updated)
        (-100, 0.15, "Loss boundary: -100 should be ~0.15"),
        (-1000, 0.10, "Loss boundary: -1000 should be ~0.10"),
        (-10000, 0.05, "Loss boundary: -10000 should be ~0.05"),
        
        # Profit boundaries
        (100, 0.25, "Profit boundary: 100 should be ~0.25"),
        (1000, 0.40, "Profit boundary: 1000 should be ~0.40"),
        (5000, 0.60, "Profit boundary: 5000 should be ~0.60"),
        (10000, 0.75, "Profit boundary: 10000 should be ~0.75"),
        (50000, 0.85, "Profit boundary: 50000 should be ~0.85"),
        (100000, 0.92, "Profit boundary: 100000 should be ~0.92"),
        (500000, 0.98, "Profit boundary: 500000 should be ~0.98"),
        (1000000, 0.999, "Profit boundary: 1000000 should be ~0.999"),
    ]
    
    print(f"\n{'PnL':<15} {'Actual':<10} {'Expected':<10} {'Diff':<10} {'Status':<10} {'Description':<40}")
    print("-" * 100)
    
    all_pass = True
    for pnl, expected, desc in boundaries:
        actual = calculate_pnl_score(pnl)
        diff = abs(actual - expected)
        # Allow small tolerance for logarithmic interpolation
        status = "✓ PASS" if diff < 0.01 else "✗ FAIL"
        if status == "✗ FAIL":
            all_pass = False
        
        print(f"{pnl:<15.2f} {actual:<10.4f} {expected:<10.4f} {diff:<10.4f} {status:<10} {desc:<40}")
    
    print("\n" + "=" * 80)
    if all_pass:
        print("✅ ALL BOUNDARY TESTS PASSED")
    else:
        print("⚠️  SOME BOUNDARY TESTS FAILED - Review tolerance or implementation")
    print("=" * 80)
    
    # Test clamping
    print("\n🔒 CLAMPING VERIFICATION")
    print("-" * 80)
    extreme_cases = [
        (-1000000000, "Extreme loss should clamp to >= 0"),
        (1000000000, "Extreme profit should clamp to 1.0"),
    ]
    
    for pnl, desc in extreme_cases:
        score = calculate_pnl_score(pnl)
        clamped = max(0.0, min(1.0, score))
        status = "✓ OK" if score == clamped and 0 <= score <= 1 else "✗ FAIL"
        print(f"PnL: {pnl:>15.2f} → Score: {score:.6f} → {status} ({desc})")
    
    print("\n" + "=" * 80)
    print("VERIFICATION COMPLETE")
    print("=" * 80)

if __name__ == "__main__":
    test_pnl_scoring()
