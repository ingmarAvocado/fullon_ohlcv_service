#!/usr/bin/env python3
"""
Example: Historic Trade Collection

Demonstrates historical trade data collection using the HistoricTradeCollector.
Shows bulk collection for all configured symbols with capability validation.

This example uses database-driven configuration via fullon_orm - symbols and
exchanges are loaded from the database automatically.
"""
import asyncio
from fullon_ohlcv_service.trade.historic_collector import HistoricTradeCollector


async def example_bulk_historic_collection():
    """Collect historical trades for all configured symbols from database."""
    print("=== Bulk Historical Collection Example ===\n")

    collector = HistoricTradeCollector()

    # Start bulk collection (database-driven via fullon_orm)
    print("Starting bulk historical collection...")
    print("Loading symbols from database (symbols with backtest > 0)...\n")

    results = await collector.start_collection()

    # Display results
    total_trades = sum(results.values())
    print(f"\n✅ Collection completed!")
    print(f"   Symbols processed: {len(results)}")
    print(f"   Total trades collected: {total_trades:,}\n")

    # Show per-symbol breakdown
    if results:
        print("Per-symbol results:")
        for symbol_key, trade_count in sorted(results.items()):
            status = "✅" if trade_count > 0 else "⚠️"
            print(f"  {status} {symbol_key}: {trade_count:,} trades")
    else:
        print("⚠️  No symbols configured for historical collection")
        print("   Configure symbols in database with backtest > 0")
        print("\nExample configuration:")
        print("   1. Add exchanges to database via fullon_orm")
        print("   2. Add symbols with backtest period (days)")
        print("   3. Run this example again")


async def main():
    """Run historical collection example."""
    try:
        await example_bulk_historic_collection()
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

        print("\nTroubleshooting:")
        print("  - Verify database connection (fullon_orm)")
        print("  - Check admin user exists (ADMIN_MAIL env variable)")
        print("  - Ensure exchanges configured for admin user")
        print("  - Verify symbols have backtest > 0")


if __name__ == "__main__":
    print("Historic Trade Collection Example")
    print("=" * 50)
    print()
    asyncio.run(main())
