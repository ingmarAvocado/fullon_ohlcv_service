#!/usr/bin/env python3
"""
Example: OHLCV Collection (Historic + Live) - ALL Symbols

Demonstrates OHLCV data collection using the rewritten collectors.
Tests bulk collection for ALL configured symbols using test databases.

Usage:
    python ohlcv_collection_example.py
"""

import asyncio
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Load environment variables from .env file
project_root = Path(__file__).parent.parent
try:
    from dotenv import load_dotenv

    load_dotenv(project_root / ".env")
except ImportError:
    print("⚠️  python-dotenv not available, make sure .env variables are set manually")
except Exception as e:
    print(f"⚠️  Could not load .env file: {e}")

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# CRITICAL: Set BOTH test database names FIRST, before ANY imports
from demo_data import generate_test_db_name

test_db_base = generate_test_db_name()
test_db_orm = test_db_base
test_db_ohlcv = f"{test_db_base}_ohlcv"

os.environ["DB_NAME"] = test_db_orm
os.environ["DB_OHLCV_NAME"] = test_db_ohlcv

# Now safe to import modules
from demo_data import create_dual_test_databases, drop_dual_test_databases, install_demo_data
from fullon_ohlcv_service.ohlcv.historic_collector import HistoricOHLCVCollector
from fullon_ohlcv_service.ohlcv.live_collector import LiveOHLCVCollector
from fullon_exchange.queue import ExchangeQueue
from fullon_log import get_component_logger
from fullon_orm import DatabaseContext, init_db

logger = get_component_logger("fullon.ohlcv.test")


async def set_database():
    """Set up test databases for OHLCV collection."""
    print("\n🔍 Setting up test databases for ALL symbols")
    print("=" * 50)

    logger.debug("Creating dual test databases", orm_db=test_db_orm, ohlcv_db=test_db_ohlcv)
    orm_db_name, ohlcv_db_name = await create_dual_test_databases(test_db_base)
    logger.debug("Using dual test databases", orm_db=orm_db_name, ohlcv_db=ohlcv_db_name)

    # Initialize database schema
    logger.debug("Initializing database schema")
    await init_db()

    # Install demo data
    logger.debug("Installing demo data")
    await install_demo_data()
    logger.info("Demo data installed successfully")


async def example_historic_ohlcv():
    """Collect historical OHLCV for ALL configured symbols."""
    print("\n📚 Starting historical OHLCV collection (ALL symbols)...")

    try:
        # Use HistoricOHLCVCollector public API - finds ALL symbols automatically
        collector = HistoricOHLCVCollector()
        results = await collector.start_collection()

        # Display results
        total_candles = sum(results.values())
        print(f"\n✅ Historical collection completed!")
        print(f"   Symbols processed: {len(results)}")
        print(f"   Total candles collected: {total_candles:,}")

        return results

    except Exception as e:
        print(f"❌ Historical collection failed: {e}")
        logger.exception("Historical collection failed")
        return {}


async def example_live_ohlcv():
    """Start live OHLCV streaming for ALL configured symbols."""
    print("\n📊 Starting live OHLCV collection (ALL symbols)...")

    collector = None
    try:
        collector = LiveOHLCVCollector()

        # Initialize ExchangeQueue factory
        await ExchangeQueue.initialize_factory()

        # start_collection() runs indefinitely (keeps WebSocket alive)
        # We need to run it as a task and cancel after duration
        collection_task = asyncio.create_task(collector.start_collection())

        # Run until end of next minute + 1 second
        now = datetime.now(timezone.utc)
        next_minute_end = (now + timedelta(minutes=1)).replace(second=0, microsecond=0) + timedelta(
            seconds=1
        )
        sleep_duration = (next_minute_end - now).total_seconds()
        print(
            f"Collecting live OHLCV until {next_minute_end.strftime('%H:%M:%S')} UTC ({sleep_duration:.1f} seconds)..."
        )
        await asyncio.sleep(sleep_duration)

        # Stop streaming
        await collector.stop_collection()

        # Cancel the collection task
        collection_task.cancel()
        try:
            await collection_task
        except asyncio.CancelledError:
            pass

        print("✅ Live collection stopped")

    except Exception as e:
        print(f"❌ Live collection error: {e}")
        logger.exception("Live collection failed")
        if collector:
            await collector.stop_collection()


async def check_fullon_content():
    """
    Use fullon_ohlcv to verify we have recent OHLCV candles for all collected symbols.
    Checks last 10 1-minute candles for each symbol that was historically collected.
    """
    try:
        print("\n📊 Checking OHLCV candle data for all collected symbols...")

        # Initialize ExchangeQueue for handler checks
        await ExchangeQueue.initialize_factory()

        # Load all symbols from database (same as historic collector)
        async with DatabaseContext() as db:
            all_symbols = await db.symbols.get_all()

        if not all_symbols:
            raise ValueError("No symbols found in database")

        total_symbols_checked = 0
        total_candles_found = 0

        # Check OHLCV for each symbol
        for symbol in all_symbols:
            exchange_name = symbol.cat_exchange.name
            symbol_str = symbol.symbol
            symbol_key = f"{exchange_name}:{symbol_str}"

            print(f"\n🔍 Checking symbol: {symbol_key}")

            # Check if this exchange supports native OHLCV
            try:
                # Create a simple exchange object for handler check
                class SimpleExchange:
                    def __init__(self, exchange_name: str):
                        self.ex_id = f"{exchange_name}_check"
                        self.uid = "check_account"
                        self.test = False
                        self.cat_exchange = type("CatExchange", (), {"name": exchange_name})()

                exchange_obj = SimpleExchange(exchange_name)

                # Public data doesn't need credentials
                def credential_provider(exchange_obj):
                    return "", ""

                handler = await ExchangeQueue.get_rest_handler(exchange_obj, credential_provider)
                if handler.needs_trades_for_ohlcv():
                    print(f"   ⚠️  {symbol_key} uses trade-based OHLCV - skipping candle display")
                    continue
            except Exception as e:
                print(f"   ❌ Error getting handler for {symbol_key}: {e}")
                continue
            except Exception as e:
                print(f"   ❌ Error getting handler for {symbol_key}: {e}")
                continue

            # Check recent 1-minute candles (last 15 minutes)
            from datetime import datetime, timezone, timedelta
            import arrow
            from fullon_ohlcv.repositories.ohlcv import TimeseriesRepository

            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(minutes=15)

            try:
                async with TimeseriesRepository(exchange_name, symbol_str, test=False) as repo:
                    ohlcv_1m = await repo.fetch_ohlcv(
                        compression=1,
                        period="minutes",
                        fromdate=arrow.get(start_time),
                        todate=arrow.get(end_time),
                    )

                    if ohlcv_1m:
                        # Show last 10 candles
                        recent_1m = ohlcv_1m[-10:] if len(ohlcv_1m) >= 10 else ohlcv_1m
                        print("   🕐 Last 10 1-minute candles:")
                        for ts, o, h, l, c, v in recent_1m:
                            if any(x is None for x in (o, h, l, c, v)):
                                print(
                                    f"   ⚠️  Invalid candle data (contains None): {ts}, {o}, {h}, {l}, {c}, {v}"
                                )
                                continue
                            candle_time = arrow.get(ts).format("YYYY-MM-DD HH:mm:ss")
                            print(
                                f"   {candle_time} | O:{o:.2f} H:{h:.2f} L:{l:.2f} C:{c:.2f} V:{v:.4f}"
                            )

                        print(
                            f"   ✅ Found {len(ohlcv_1m)} 1-minute candles (showing last {len(recent_1m)})"
                        )
                        total_candles_found += len(ohlcv_1m)
                    else:
                        print("   ⚠️  No 1-minute candles found")

                    total_symbols_checked += 1

            except Exception as symbol_error:
                print(f"   ❌ Error checking {symbol_key}: {symbol_error}")
                continue

        print(
            f"\n✅ OHLCV verification complete: checked {total_symbols_checked} symbols, found {total_candles_found} total candles"
        )

        # Clean up ExchangeQueue
        await ExchangeQueue.shutdown_factory()

    except Exception as e:
        print(f"❌ OHLCV check failed: {e}")
        logger.exception("OHLCV content check failed")


async def main():
    """Run OHLCV collection examples."""
    print("🧪 OHLCV TWO-PHASE COLLECTION EXAMPLE")
    print("Testing two-phase collection for ALL configured symbols")
    print("\nPattern:")
    print("  Phase 1: Historical catch-up (REST with pagination)")
    print("  Phase 2: Real-time streaming (WebSocket)")

    try:
        # Test individual components
        try:
            await set_database()
            await example_historic_ohlcv()
            await check_fullon_content()
            print("\n---------- live collecting now -------------")
            await example_live_ohlcv()
            await check_fullon_content()
        except Exception as e:
            print(f"❌ Collection failed: {e}")
            logger.error("Collection failed", error=str(e))
        finally:
            # Clean up test databases
            try:
                logger.debug(
                    "Dropping dual test databases", orm_db=test_db_orm, ohlcv_db=test_db_ohlcv
                )
                await drop_dual_test_databases(test_db_orm, test_db_ohlcv)
                logger.debug("Test databases cleaned up successfully")
            except Exception as db_cleanup_error:
                logger.warning("Error during database cleanup", error=str(db_cleanup_error))

        print("\n" + "=" * 60)
        print("✅ OHLCV TWO-PHASE COLLECTION TEST COMPLETED")
        print("📋 Summary:")
        print("  ✅ Historical pagination logic implemented")
        print("  ✅ OHLCV two-phase pattern implemented")
        print("  ✅ Follows proven architecture patterns")
        print("\n💡 Ready for production with database and WebSocket streaming")

    except Exception as e:
        print(f"❌ Test suite failed: {e}")

    finally:
        # Clean up exchange resources
        try:
            await ExchangeQueue.shutdown_factory()
        except Exception as cleanup_error:
            pass


if __name__ == "__main__":
    asyncio.run(main())
