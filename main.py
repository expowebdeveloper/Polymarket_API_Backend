"""
CLI entry point for Polymarket Analytics Platform.
"""

import sys
import asyncio
from app.core.config import settings
from app.services.data_fetcher import fetch_resolved_markets, fetch_trades_for_wallet
from app.services.scoring_engine import calculate_metrics


def validate_wallet(wallet_address: str) -> bool:
    """Validate wallet address format."""
    if not wallet_address:
        return False
    if not wallet_address.startswith("0x"):
        return False
    if len(wallet_address) != 42:
        return False
    try:
        int(wallet_address[2:], 16)
        return True
    except:
        return False


def format_console_output(metrics: dict) -> str:
    """Format metrics for console output matching the exact format."""
    output = []
    output.append(f"\nFinal results for user {metrics['wallet_id']}\n")
    output.append("Overall metrics")
    output.append(f"- Total Positions: {metrics['total_positions']}")
    output.append(f"- Active Positions: {metrics['active_positions']}")
    output.append(f"- Total Wins: ${metrics['total_wins']:,.2f}")
    output.append(f"- Total Losses: ${metrics['total_losses']:,.2f}")
    output.append(f"- Win Rate: {metrics['win_rate_percent']:.1f}% ({metrics['win_count']} won, {metrics['loss_count']} lost)")
    output.append(f"- Current Value: ${metrics['current_value']:.2f}")
    output.append(f"- Overall PnL: ${metrics['pnl']:,.2f}")
    
    if metrics.get('categories'):
        output.append("\nCategory breakdown:")
        for category, cat_metrics in metrics['categories'].items():
            output.append(f"  {category}:")
            output.append(f"    - Total Wins: ${cat_metrics['total_wins']:,.2f}")
            output.append(f"    - Total Losses: ${cat_metrics['total_losses']:,.2f}")
            output.append(f"    - Win Rate: {cat_metrics['win_rate_percent']:.1f}%")
            output.append(f"    - PnL: ${cat_metrics['pnl']:,.2f}")
    
    output.append(f"\nFinal Score: {metrics['final_score']:.1f}\n")
    
    return "\n".join(output)


async def analyze_wallet(wallet_address: str) -> dict:
    """Main function to analyze a wallet and return metrics."""
    if not validate_wallet(wallet_address):
        raise ValueError(f"Invalid wallet address: {wallet_address}")
    
    print(f"Fetching resolved markets...")
    markets = fetch_resolved_markets()
    print(f"Found {len(markets)} resolved markets")
    
    print(f"Fetching trades for wallet {wallet_address}...")
    trades = await fetch_trades_for_wallet(wallet_address)
    print(f"Found {len(trades)} trades")
    
    print("Calculating metrics...")
    metrics = calculate_metrics(wallet_address, trades, markets)
    
    return metrics


def cli_mode():
    """CLI mode: validate with sample wallet and print console output."""
    target_wallet = settings.TARGET_WALLET
    
    print("=" * 60)
    print("Polymarket Analytics Platform - CLI Validation Mode")
    print("=" * 60)
    
    try:
        metrics = asyncio.run(analyze_wallet(target_wallet))
        output = format_console_output(metrics)
        print(output)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # Check if running as server (with --server flag) or CLI mode
    if len(sys.argv) > 1 and sys.argv[1] == "--server":
        import uvicorn
        from app.main import app
        uvicorn.run(app, host=settings.HOST, port=settings.PORT, reload=settings.RELOAD)
    else:
        # CLI mode: validate with sample wallet
        cli_mode()
