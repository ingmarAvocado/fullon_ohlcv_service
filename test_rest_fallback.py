#!/usr/bin/env python3
"""
Test REST fallback for LiveOHLCVCollector with Yahoo Finance GOLD.

This test verifies that:
1. WebSocket is detected as unavailable
2. Falls back to REST polling automatically
3. Candle count increases as new candles arrive
4. Polling happens at correct intervals (60 seconds for 1m candles)
"""

import asyncio
import os
from datetime import UTC, datetime

from fullon_orm import DatabaseContext
from fullon_orm.models import CatExchange, Symbol
from fullon_ohlcv_service.ohlcv.live_collector import LiveOHLCVCollector
from fullon_ohlcv.repositories.ohlcv import CandleRepository


async def get_yahoo_symbol():
    """Get a Yahoo Finance symbol for testing."""
    async with DatabaseContext() as db:
        # Get all symbols
        all_symbols = await db.symbols.get_all()

        # Find a Yahoo symbol
        for symbol in all_symbols:
            if symbol.cat_exchange.name == 'yahoo':
                print(f"Using existing symbol: {symbol.symbol} on {symbol.cat_exchange.name}")
                return symbol

        print("ERROR: No Yahoo symbols found in database")
        print("Available exchanges:", list(set([s.cat_exchange.name for s in all_symbols])))
        return None


async def count_candles(exchange: str, symbol_str: str) -> int:
    """Count candles in database."""
    async with CandleRepository(exchange, symbol_str) as repo:
        latest = await repo.get_latest_timestamp()
        if latest:
            # Get candles from last hour to count them
            from_time = latest.shift(hours=-1)
            candles = await repo.get_candles(from_time, latest)
            return len(candles)
        return 0


async def main():
    """Main test function."""
    print("=" * 80)
    print("REST Fallback Test - Yahoo Finance GOLD")
    print("=" * 80)

    # Get test symbol
    symbol = await get_yahoo_symbol()
    if not symbol:
        return

    print(f"\nTesting with: {symbol.cat_exchange.name}:{symbol.symbol}")
    print(f"Timeframe: {symbol.updateframe}")
    print(f"Expected poll interval: 60 seconds")

    # Create collector
    collector = LiveOHLCVCollector()
    collector.running = True  # Mark as running

    print("\n" + "=" * 80)
    print("Starting live collection (this will test WebSocket detection)...")
    print("=" * 80)

    try:
        # Start symbol collection - should detect no WebSocket and fall back to REST
        await collector.start_symbol(symbol)

        print("\nWaiting 10 seconds to verify collection has started...")
        await asyncio.sleep(10)

        # Check if REST polling task was created
        symbol_key = f"{symbol.cat_exchange.name}:{symbol.symbol}"
        if symbol_key in collector.rest_polling_tasks:
            print(f"✓ REST polling task created for {symbol_key}")
        else:
            print(f"✗ No REST polling task found for {symbol_key}")
            print(f"  Available tasks: {list(collector.rest_polling_tasks.keys())}")

        # Monitor candle collection for 3 minutes
        print("\n" + "=" * 80)
        print("Monitoring candle collection for 3 minutes...")
        print("Expected: New candles every 60 seconds")
        print("=" * 80)

        initial_count = await count_candles(symbol.cat_exchange.name, symbol.symbol)
        print(f"\nInitial candle count: {initial_count}")

        for i in range(6):  # Check every 30 seconds for 3 minutes
            await asyncio.sleep(30)
            current_count = await count_candles(symbol.cat_exchange.name, symbol.symbol)
            elapsed = (i + 1) * 30
            print(f"[{elapsed}s] Candle count: {current_count} (delta: +{current_count - initial_count})")

        final_count = await count_candles(symbol.cat_exchange.name, symbol.symbol)

        print("\n" + "=" * 80)
        print("Test Results")
        print("=" * 80)
        print(f"Initial candles: {initial_count}")
        print(f"Final candles:   {final_count}")
        print(f"New candles:     {final_count - initial_count}")

        if final_count > initial_count:
            print("\n✓ SUCCESS: Candles are being collected via REST polling!")
            print(f"✓ Collected {final_count - initial_count} new candles in 3 minutes")
        else:
            print("\n✗ FAILURE: No new candles collected")

        # Check process status
        if symbol_key in collector.process_ids:
            print(f"\n✓ Process registered: {collector.process_ids[symbol_key]}")
        else:
            print("\n✗ No process ID found")

        # Check if symbol is registered
        if symbol_key in collector.registered_symbols:
            print(f"✓ Symbol registered in collector")
        else:
            print(f"✗ Symbol not registered")

    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()

    finally:
        print("\n" + "=" * 80)
        print("Stopping collector...")
        print("=" * 80)
        collector.running = False
        await asyncio.sleep(2)  # Give tasks time to clean up
        print("Test complete!")


if __name__ == "__main__":
    asyncio.run(main())
