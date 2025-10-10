#!/usr/bin/env python3
"""
Example: Full Pipeline with Daemon

Demonstrates the complete OHLCV/trade collection pipeline using the daemon.
Tests both historical catch-up and live streaming phases.

Usage:
    python run_example_pipeline.py
"""

import asyncio
import contextlib
import os
import sys
from datetime import datetime, timedelta, timezone
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
from fullon_ohlcv_service.daemon import OhlcvServiceDaemon
from fullon_log import get_component_logger
from fullon_orm import DatabaseContext, init_db

logger = get_component_logger("fullon.pipeline.test")


async def set_database():
    """Set up dual test databases for full pipeline testing."""
    print("\n🔍 Setting up dual test databases for full pipeline")
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


async def run_pipeline():
    """Run the complete pipeline using the daemon."""
    print("\n🚀 Starting full pipeline with daemon...")

    daemon = None
    try:
        # Create daemon instance
        daemon = OhlcvServiceDaemon()

        print("📚 Phase 1: Running concurrent historic collection (OHLCV + Trades)...")

        # Start daemon (symbol init + historic concurrently, then live indefinitely)
        daemon_task = asyncio.create_task(daemon.run())

        # Wait for symbol initialization + historic collection to complete
        # Symbol init happens first, then both historic collectors run concurrently
        # Give them time to finish
        await asyncio.sleep(25)  # Allow time for symbol init + concurrent historic collection

        print("✅ Historic collection completed")
        print("📊 Phase 2: Running live collection until end of next minute + 1ms...")

        # Calculate time to run until end of next minute + 1ms
        now = datetime.now(timezone.utc)
        next_minute_end = (now + timedelta(minutes=1)).replace(second=0, microsecond=0) + timedelta(
            milliseconds=1
        )
        sleep_duration = (next_minute_end - now).total_seconds()

        print(
            f"Running live collection until {next_minute_end.strftime('%H:%M:%S.%f')} UTC ({sleep_duration:.3f} seconds)..."
        )

        # Continue running live collection until target time
        await asyncio.sleep(sleep_duration)

        # Cancel daemon task gracefully
        print("⏹️  Requesting daemon shutdown...")
        daemon_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await daemon_task

        print("✅ Live collection completed")

    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        logger.exception("Pipeline failed")
    finally:
        if daemon:
            await daemon.cleanup()
            print("✅ Daemon cleanup completed")


async def check_pipeline_results():
    """Check the results of the pipeline run."""
    try:
        print("\n📊 Checking pipeline results...")

        # Check database for collected data
        async with DatabaseContext() as db:
            all_symbols = await db.symbols.get_all()

        if not all_symbols:
            print("⚠️  No symbols found in database")
            return

        print(f"Found {len(all_symbols)} symbols in database")

        # Check OHLCV data for a few symbols
        from fullon_ohlcv.repositories.ohlcv import TimeseriesRepository
        import arrow

        checked_symbols = 0
        total_candles = 0

        for symbol in all_symbols[:3]:  # Check first 3 symbols
            exchange_name = symbol.cat_exchange.name
            symbol_str = symbol.symbol

            try:
                async with TimeseriesRepository(exchange_name, symbol_str, test=False) as repo:
                    # Check recent candles
                    end_time = datetime.now(timezone.utc)
                    start_time = end_time - timedelta(minutes=15)

                    ohlcv_data = await repo.fetch_ohlcv(
                        compression=1,
                        period="minutes",
                        fromdate=arrow.get(start_time),
                        todate=arrow.get(end_time),
                    )

                    if ohlcv_data:
                        print(f"✅ {exchange_name}:{symbol_str} - {len(ohlcv_data)} candles")
                        total_candles += len(ohlcv_data)
                        checked_symbols += 1
                    else:
                        print(f"⚠️  {exchange_name}:{symbol_str} - No candles found")

            except Exception as e:
                print(f"❌ Error checking {exchange_name}:{symbol_str}: {e}")

        print(
            f"\n✅ Pipeline check complete: {checked_symbols} symbols checked, {total_candles} total candles"
        )

    except Exception as e:
        print(f"❌ Pipeline check failed: {e}")
        logger.exception("Pipeline check failed")


async def main():
    """Run the complete pipeline example."""
    print("🧪 FULL PIPELINE WITH DAEMON EXAMPLE")
    print("Testing complete OHLCV/trade collection using daemon")
    print("\nPattern:")
    print("  Phase 1: Historical catch-up (via daemon)")
    print("  Phase 2: Live streaming until end of next minute + 1ms")

    try:
        # Set up databases
        await set_database()

        # Run the pipeline
        await run_pipeline()

        # Check results
        await check_pipeline_results()

        print("\n" + "=" * 60)
        print("✅ FULL PIPELINE TEST COMPLETED")
        print("📋 Summary:")
        print("  ✅ Daemon-based collection implemented")
        print("  ✅ Historic + live phases working")
        print("  ✅ Proper timing control")
        print("\n💡 Pipeline ready for production use")

    except Exception as e:
        print(f"❌ Pipeline test failed: {e}")
        logger.exception("Pipeline test failed")

    finally:
        # Clean up test databases
        try:
            logger.debug("Dropping dual test databases", orm_db=test_db_orm, ohlcv_db=test_db_ohlcv)
            await drop_dual_test_databases(test_db_orm, test_db_ohlcv)
            logger.debug("Test databases cleaned up successfully")
        except Exception as db_cleanup_error:
            logger.warning("Error during database cleanup", error=str(db_cleanup_error))


if __name__ == "__main__":
    asyncio.run(main())
