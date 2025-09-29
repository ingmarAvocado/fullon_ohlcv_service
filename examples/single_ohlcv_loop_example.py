#!/usr/bin/env python3
"""
Single OHLCV Loop Example

This example demonstrates the simplest possible usage of fullon_ohlcv_service:
1. Get a symbol from the database (BTC/USDC from kraken with 30 days backtest)
2. Start processing just that one symbol for OHLCV collection
3. Loop forever reading and printing OHLCV data from cache
4. Handle Ctrl+C gracefully

Usage:
    python single_ohlcv_loop_example.py
"""

import asyncio
import os
import signal
import sys
import time
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

# Import demo data utilities
from demo_data import (
    generate_test_db_name,
    create_test_database,
    drop_test_database,
    install_demo_data
)

from fullon_ohlcv_service.ohlcv.collector_master import OhlcvCollectorMaster as OhlcvCollector
from fullon_orm import DatabaseContext
from fullon_ohlcv.repositories.ohlcv import CandleRepository
from fullon_log import get_component_logger


async def main():
    """Simple OHLCV processing example."""
    logger = get_component_logger("fullon.ohlcv.example")
    collector = None
    test_db_name = None

    try:
        # Always create test database and install demo data
        test_db_name = generate_test_db_name()
        logger.debug("Creating test database", database_name=test_db_name)
        await create_test_database(test_db_name)

        # Override DB_NAME environment variable
        os.environ['DB_NAME'] = test_db_name
        logger.debug("Using test database", database_name=test_db_name)

        # Initialize database schema
        logger.debug("Initializing database schema")
        from fullon_orm import init_db
        await init_db()

        # Install demo data
        logger.debug("Installing demo data")
        await install_demo_data()
        logger.info("Demo data installed successfully", symbol="BTC/USDC", backtest_days=30)

        logger.info("Starting single OHLCV loop example")

        # Get BTC/USDC symbol from kraken (30 days backtest)
        async with DatabaseContext() as db:
            # Look for BTC/USDC on kraken specifically
            symbols = await db.symbols.get_all()
            target_symbol = None

            for symbol in symbols:
                if (symbol.symbol == "BTC/USDC" and
                    symbol.exchange_name.lower() == "kraken"):
                    target_symbol = symbol
                    break

            if not target_symbol:
                # Fallback to any BTC/USDC symbol
                for symbol in symbols:
                    if "BTC/USDC" in symbol.symbol:
                        target_symbol = symbol
                        break

            if not target_symbol:
                logger.error("BTC/USDC symbol not found in database")
                return 1

            logger.info("Using symbol", symbol=target_symbol.symbol, exchange=target_symbol.exchange_name, backtest_days=target_symbol.backtest)

        # Create single OHLCV collector using the symbol object directly
        collector = OhlcvCollector(target_symbol)
        logger.info("Starting OHLCV collector", symbol=target_symbol.symbol, exchange=target_symbol.exchange_name)

        # Start with Phase 1: Historical OHLCV collection (REST)
        logger.info("Starting Phase 1: Historical OHLCV collection")
        await collector.collect_historical()

        logger.info("Historical OHLCV collection completed, starting monitoring loop")
        logger.info("Monitoring loop started - Press Ctrl+C to stop")

        # Set up graceful shutdown
        shutdown_event = asyncio.Event()

        def signal_handler(signum, frame):
            logger.info("Received shutdown signal", signal=signum)
            shutdown_event.set()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Main loop: read from OHLCV repository and print
        while not shutdown_event.is_set():
            try:
                async with CandleRepository(target_symbol.exchange_name, target_symbol.symbol, test=False) as repo:
                    # Get latest timestamp to see if we have recent data
                    latest_timestamp = await repo.get_latest_timestamp()
                    if latest_timestamp:
                        # Convert datetime to unix timestamp for age calculation
                        latest_unix = latest_timestamp.timestamp()  # Call method, not access property
                        age_seconds = time.time() - latest_unix
                        age_minutes = age_seconds / 60

                        # Format datetime for display
                        formatted_time = latest_timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')

                        if age_seconds < 300:  # Less than 5 minutes old
                            logger.info("Latest OHLCV data", symbol=target_symbol.symbol, timestamp=formatted_time, age_minutes=f"{age_minutes:.1f}")
                        else:
                            logger.warning("OHLCV data is stale", symbol=target_symbol.symbol, age_minutes=f"{age_minutes:.1f}")
                    else:
                        logger.debug("Waiting for OHLCV data", symbol=target_symbol.symbol)

                # Wait 10 seconds or until shutdown
                try:
                    await asyncio.wait_for(shutdown_event.wait(), timeout=10.0)
                except asyncio.TimeoutError:
                    continue  # Normal timeout, continue loop

            except Exception as e:
                logger.error("Error reading OHLCV data", error=str(e))
                await asyncio.sleep(10)

        return 0

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return 1
    except Exception as e:
        logger.error("Example failed", error=str(e))
        return 1
    finally:
        if collector:
            logger.debug("Cleaning up resources")
            # No streaming to stop for historical collection
            logger.debug("Cleanup complete")

        # Clean up test database if created
        if test_db_name:
            logger.debug("Dropping test database", database_name=test_db_name)
            await drop_test_database(test_db_name)


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("Script interrupted")
        sys.exit(1)