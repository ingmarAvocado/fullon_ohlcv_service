#!/usr/bin/env python3
"""
Init Symbol Example

Demonstrates the new init_symbol() method for explicit database object creation.
Shows how to initialize all required tables and views for a trading symbol.
"""

import asyncio
import os
import sys
import subprocess
from datetime import datetime, timezone, timedelta

# Add the src directory to Python path for imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), 'src'))

from fullon_ohlcv.repositories.ohlcv import TradeRepository, TimeseriesRepository
from fullon_ohlcv.models import Trade
from fullon_ohlcv.utils.logger import get_logger
import arrow

# Import demo functions for output formatting
try:
    sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'scripts'))
    from demo_install import Colors, print_success, print_error, print_info, print_header, print_warning
except ImportError:
    # Fallback if demo_install is not available
    def print_success(msg): print(f"✓ {msg}")
    def print_error(msg): print(f"✗ {msg}")
    def print_info(msg): print(f"ℹ {msg}")
    def print_header(msg): print(f"\n=== {msg} ===")
    def print_warning(msg): print(f"⚠ {msg}")

logger = get_logger(__name__)

# Database name for this example
EXAMPLE_DB_NAME = "init_symbol_example_db"


async def setup_database():
    """Create test database for the example."""
    print_info(f"Setting up example database: {EXAMPLE_DB_NAME}")
    try:
        # Run install script
        result = subprocess.run([
            sys.executable,
            os.path.join(os.path.dirname(__file__), '..', 'src', 'fullon_ohlcv', 'install_ohlcv.py'),
            EXAMPLE_DB_NAME
        ], capture_output=True, text=True, check=True)
        print_success(f"Database {EXAMPLE_DB_NAME} created successfully")
        return True
    except subprocess.CalledProcessError as e:
        print_error(f"Failed to create database: {e.stderr}")
        return False


async def cleanup_database():
    """Delete test database."""
    print_info(f"Cleaning up example database: {EXAMPLE_DB_NAME}")
    try:
        # Run install script with --delete
        result = subprocess.run([
            sys.executable,
            os.path.join(os.path.dirname(__file__), '..', 'src', 'fullon_ohlcv', 'install_ohlcv.py'),
            "--delete",
            EXAMPLE_DB_NAME
        ], capture_output=True, text=True, check=True)
        print_success(f"Database {EXAMPLE_DB_NAME} deleted successfully")
        return True
    except subprocess.CalledProcessError as e:
        print_warning(f"Failed to delete database: {e.stderr}")
        return False


async def main():
    """Demonstrate init_symbol() functionality."""
    print_header("Init Symbol Example")

    # Setup database
    if not await setup_database():
        print_error("Database setup failed - exiting")
        return

    try:
        # Override the test database name for this example
        os.environ['DB_TEST_NAME'] = EXAMPLE_DB_NAME

        # Initialize repository
        async with TradeRepository("binance", "BTC/USDT", test=True) as repo:
            print_info("1. Repository initialized successfully")

            # Initialize symbol with different main sources
            print_info("2. Testing init_symbol with different main sources...")

            # Test with view as main source (default, fastest)
            print_info("   Testing main='view' (continuous aggregate)...")
            success = await repo.init_symbol(main="view")
            print_success(f"   init_symbol(view) success: {success}")

            # Test with candles as main source
            print_info("   Testing main='candles' (pre-computed candles)...")
            success = await repo.init_symbol(main="candles")
            print_success(f"   init_symbol(candles) success: {success}")

            # Test with trades as main source
            print_info("   Testing main='trades' (aggregate from trades)...")
            success = await repo.init_symbol(main="trades")
            print_success(f"   init_symbol(trades) success: {success}")

            # Create sample trades
            base_time = datetime.now(timezone.utc)
            trades = [
                Trade(
                    timestamp=base_time,
                    price=50000.0,
                    volume=0.1,
                    side="BUY",
                    type="MARKET"
                ),
                Trade(
                    timestamp=base_time + timedelta(seconds=1),
                    price=50050.0,
                    volume=0.2,
                    side="SELL",
                    type="LIMIT"
                ),
                Trade(
                    timestamp=base_time + timedelta(seconds=2),
                    price=50025.0,
                    volume=0.15,
                    side="BUY",
                    type="MARKET"
                )
            ]

            # Save trades
            print_info("3. Saving trades...")
            success = await repo.save_trades(trades)
            print_success(f"Trades saved: {success}")

            # Test TimeseriesRepository with the new OHLCV view
            print_info("4. Testing TimeseriesRepository with OHLCV view...")

            ts_repo = TimeseriesRepository("binance", "BTC/USDT", test=True)
            await ts_repo.initialize()

            # Query data using the OHLCV view
            try:
                # Get some OHLCV data
                fromdate = base_time - timedelta(minutes=5)
                todate = base_time + timedelta(minutes=5)

                data = await ts_repo.fetch_ohlcv(
                    compression=1,
                    period="minute",
                    fromdate=arrow.get(fromdate),
                    todate=arrow.get(todate)
                )
                print_success(f"OHLCV data points fetched: {len(data)}")

                if data:
                    print_info("   Sample OHLCV data:")
                    for i, (ts, o, h, l, c, v) in enumerate(data[:3]):
                        print_info(f"   {ts}: O={o} H={h} L={l} C={c} V={v}")

            except Exception as e:
                print_warning(f"   Timeseries query failed (expected if no data): {e}")

            await ts_repo.close()

            # Verify idempotency - calling init_symbol multiple times should be safe
            print_info("5. Testing idempotency (calling init_symbol multiple times)...")
            for i in range(3):
                success = await repo.init_symbol(main="view")
                print_success(f"   Call {i+1}: {success}")

            print_success("Init Symbol example completed successfully!")

    finally:
        # Cleanup database
        await cleanup_database()


if __name__ == "__main__":
    try:
        # Setup uvloop for performance
        from fullon_ohlcv.utils import install_uvloop
        install_uvloop()

        asyncio.run(main())

    except Exception as e:
        print_error(f"Error during execution: {e}")
        raise