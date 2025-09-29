#!/usr/bin/env python3
"""
Single Trade Loop Example - Race Condition Fixed

This example demonstrates trade collection using fullon_ohlcv_service:
1. Get a symbol from the database (BTC/USDC from kraken with 30 days backtest)
2. Start processing just that one symbol for trade collection
3. Loop forever reading and printing trade data from cache
4. Handle Ctrl+C gracefully

Usage:
    python single_trade_loop_fixed.py
"""

import asyncio
import os
import signal
import sys
import time
from pathlib import Path
import uuid

# CRITICAL: Set BOTH test database names IMMEDIATELY, before ANY imports
# fullon_orm uses DB_NAME, fullon_ohlcv uses DB_OHLCV_NAME
test_db_base = f"fullon2_test_{uuid.uuid4().hex[:8]}"
test_db_orm = test_db_base
test_db_ohlcv = f"{test_db_base}_ohlcv"

os.environ['DB_NAME'] = test_db_orm
os.environ['DB_OHLCV_NAME'] = test_db_ohlcv

print(f"🔧 Set DB_NAME={test_db_orm}")
print(f"🔧 Set DB_OHLCV_NAME={test_db_ohlcv}")

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

# Now safe to import demo data utilities and other modules
from demo_data import (
    create_dual_test_databases,
    drop_dual_test_databases,
    install_demo_data
)

# Now safe to import modules that might cache database connections
from fullon_ohlcv_service.trade.historic_collector import HistoricTradeCollector
from fullon_orm import DatabaseContext
from fullon_ohlcv import TradeRepository
from fullon_log import get_component_logger



async def main():
    """Simple trade processing example."""
    logger = get_component_logger("fullon.trade.example")
    collector = None

    try:
        # Create both test databases with the names we already set in environment
        logger.debug("Creating dual test databases", orm_db=test_db_orm, ohlcv_db=test_db_ohlcv)
        orm_db_name, ohlcv_db_name = await create_dual_test_databases(test_db_base)
        logger.debug("Using dual test databases", orm_db=orm_db_name, ohlcv_db=ohlcv_db_name)

        # Initialize database schema
        logger.debug("Initializing database schema")
        from fullon_orm import init_db
        await init_db()

        # Install demo data
        logger.debug("Installing demo data")
        await install_demo_data()
        logger.info("Demo data installed successfully", symbol="BTC/USDC", backtest_days=30)

        logger.info("Starting single trade loop example")

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

        # HARDCODE: Force backtest to 3 days for this test only
        target_symbol.backtest = 3
        logger.info("HARDCODED: Forcing backtest to 3 days for this test", symbol=target_symbol.symbol, backtest_days=target_symbol.backtest)

        # Create historic trade collector using the new structure
        collector = HistoricTradeCollector(target_symbol)
        logger.info("Starting trade collector", symbol=target_symbol.symbol, exchange=target_symbol.exchange_name)

        # Start with historical trade collection (REST)
        logger.info("Starting historical trade collection")
        await collector.collect()

        logger.info("Historical trade collection completed, starting monitoring loop")
        logger.info("Monitoring loop started - Press Ctrl+C to stop")

        # Set up graceful shutdown
        shutdown_event = asyncio.Event()

        def signal_handler(signum, frame):
            logger.info("Received shutdown signal", signal=signum)
            shutdown_event.set()

            # Trigger immediate cleanup attempt
            try:
                # This is a sync call in signal handler, so we can't await
                # But we can set a flag for cleanup
                logger.debug("Signal handler triggered shutdown")
            except Exception as e:
                logger.warning("Error in signal handler", error=str(e))

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Store any background tasks that need cleanup
        background_tasks = set()

        # Main loop: read from trade repository and print
        while not shutdown_event.is_set():
            try:
                async with TradeRepository(target_symbol.exchange_name, target_symbol.symbol, test=False) as repo:
                    # Get latest timestamp to see if we have recent data
                    latest_timestamp = await repo.get_latest_timestamp()
                    if latest_timestamp:
                        # Convert datetime to unix timestamp for age calculation
                        latest_unix = latest_timestamp.timestamp()
                        age_seconds = time.time() - latest_unix
                        age_minutes = age_seconds / 60

                        # Format datetime for display
                        formatted_time = latest_timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')

                        if age_seconds < 300:  # Less than 5 minutes old
                            if age_seconds < 60:
                                age_display = f"{age_seconds:.0f} seconds ago"
                            else:
                                age_display = f"{age_minutes:.1f} minutes ago"
                            logger.info(f"Latest trade data: {target_symbol.symbol} last trade at {formatted_time} ({age_display})")
                        else:
                            if age_minutes < 60:
                                age_display = f"{age_minutes:.1f} minutes ago"
                            else:
                                age_hours = age_minutes / 60
                                age_display = f"{age_hours:.1f} hours ago"
                            logger.warning(f"Trade data is stale: {target_symbol.symbol} last trade at {formatted_time} ({age_display})")
                    else:
                        logger.debug("Waiting for trade data", symbol=target_symbol.symbol)

                # Wait 10 seconds or until shutdown
                try:
                    await asyncio.wait_for(shutdown_event.wait(), timeout=10.0)
                    # If we get here, shutdown was requested
                    logger.info("Shutdown detected, exiting monitoring loop")
                    break
                except asyncio.TimeoutError:
                    continue  # Normal timeout, continue loop

            except Exception as e:
                logger.error("Error reading trade data", error=str(e))
                await asyncio.sleep(10)

        # Perform immediate cleanup when exiting main loop
        logger.info("Main loop exited, starting immediate cleanup")

        # First, stop the collector to close WebSocket connections properly
        if collector:
            try:
                logger.debug("Stopping historic trade collector")
                await collector.stop()
                logger.info("Collector stopped successfully")
            except Exception as collector_error:
                logger.error("Error stopping collector", error=str(collector_error))

        # Then shutdown the ExchangeQueue factory with extra logging
        try:
            from fullon_exchange.queue import ExchangeQueue
            logger.debug("Shutting down ExchangeQueue factory")

            # Try to access and close individual handlers if possible
            try:
                if hasattr(ExchangeQueue, '_instance_registry'):
                    registry = getattr(ExchangeQueue, '_instance_registry', {})
                    logger.debug(f"Found {len(registry)} handlers in registry")
                    for handler_key, handler in registry.items():
                        try:
                            if hasattr(handler, 'disconnect'):
                                logger.debug(f"Disconnecting handler {handler_key}")
                                await handler.disconnect()
                        except Exception as handler_error:
                            logger.warning(f"Error disconnecting handler {handler_key}: {handler_error}")
            except Exception as registry_error:
                logger.debug(f"Could not access handler registry: {registry_error}")

            await ExchangeQueue.shutdown_factory()
            logger.info("ExchangeQueue factory shut down successfully")
        except Exception as cleanup_error:
            logger.error("Error during immediate ExchangeQueue cleanup", error=str(cleanup_error))

        return 0

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return 1
    except Exception as e:
        logger.error("Example failed", error=str(e))
        return 1
    finally:
        # Cancel any background tasks first
        try:
            logger.debug("Cancelling background tasks")
            for task in background_tasks:
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
            logger.debug("Background tasks cancelled")
        except Exception as task_cleanup_error:
            logger.warning("Error during task cleanup", error=str(task_cleanup_error))

        # Additional cleanup (if main loop cleanup failed)
        if collector:
            try:
                logger.debug("Final collector cleanup attempt")
                await collector.stop()
                logger.debug("Final collector cleanup successful")
            except Exception as cleanup_error:
                logger.warning("Error during final collector cleanup", error=str(cleanup_error))

        # Final ExchangeQueue cleanup (if main loop cleanup failed)
        try:
            logger.debug("Final ExchangeQueue cleanup attempt")
            from fullon_exchange.queue import ExchangeQueue
            await ExchangeQueue.shutdown_factory()
            logger.debug("Final ExchangeQueue cleanup successful")
        except Exception as cleanup_error:
            logger.warning("Error during final ExchangeQueue cleanup", error=str(cleanup_error))

        # Clean up both test databases
        try:
            logger.debug("Dropping dual test databases", orm_db=test_db_orm, ohlcv_db=test_db_ohlcv)
            await drop_dual_test_databases(test_db_orm, test_db_ohlcv)
            logger.debug("Test databases cleaned up successfully")
        except Exception as db_cleanup_error:
            logger.warning("Error during database cleanup", error=str(db_cleanup_error))


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger = get_component_logger("fullon.trade.example")
        logger.info("Script interrupted")
        sys.exit(1)