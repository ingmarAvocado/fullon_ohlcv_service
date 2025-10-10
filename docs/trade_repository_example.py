#!/usr/bin/env python3
"""
Simple TradeRepository Example

Shows basic usage of TradeRepository with real database operations.
"""

import asyncio
import os
import sys
import subprocess
from datetime import datetime, timezone, timedelta

# Set test database name before importing anything
os.environ['DB_TEST_NAME'] = "trade_example_db"

# Add the src directory to Python path for imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), 'src'))

from fullon_ohlcv.repositories.ohlcv import TradeRepository
from fullon_ohlcv.models import Trade
from fullon_ohlcv.utils.logger import get_logger

# Import demo functions for output formatting
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'scripts'))
from demo_install import Colors, print_success, print_error, print_info, print_header, print_warning

logger = get_logger(__name__)

# Database name for this example
EXAMPLE_DB_NAME = "trade_example_db"


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
    """Demonstrate TradeRepository functionality."""
    print_header("TradeRepository Example")

    # Setup database
    if not await setup_database():
        print_error("Database setup failed - exiting")
        return

    try:
        # Initialize repository
        async with TradeRepository("binance", "BTC/USDT", test=True) as repo:
            print_info("1. Repository initialized successfully")

            # Initialize symbol database objects
            print_info("2. Initializing symbol database objects...")
            success = await repo.init_symbol(main="view")
            print_success(f"Symbol initialization: {success}")

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
            print_info("2. Saving trades...")
            success = await repo.save_trades(trades)
            print_success(f"Trades saved: {success}")

            # Query recent trades
            print_info("3. Querying trades...")
            recent = await repo.get_recent_trades(limit=5)
            print_success(f"Found {len(recent)} trades")
            for i, trade in enumerate(recent[:3]):
                print_info(f"Trade {i+1}: {trade.price} {trade.volume} {trade.side}")

            # Get timestamp info
            oldest = await repo.get_oldest_timestamp()
            latest = await repo.get_latest_timestamp()
            print_info(f"4. Time range: {oldest} to {latest}")

            # Query by time range
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(hours=1)
            range_trades = await repo.get_trades_in_range(start_time, end_time, limit=10)
            print_info(f"5. Trades in last hour: {len(range_trades)}")

            print_success("TradeRepository example completed successfully!")

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