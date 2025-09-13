#!/usr/bin/env python3
"""
Demo Data Setup for fullon_ohlcv_service Examples

This module provides test database management and demo TRADE data setup for running examples.
It creates isolated TimescaleDB instances with realistic trade data (NOT pre-made candles).

The examples demonstrate how the OHLCV service converts raw trades into 1-minute candles.

Usage:
    python demo_data.py --setup test_db_123     # Create test database with demo trade data
    python demo_data.py --cleanup test_db_123   # Remove test database
    python demo_data.py --list                  # List existing test databases
"""

import asyncio
import argparse
import sys
import os
import random
import string
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
from typing import List, Dict, Any, Optional
from pathlib import Path

# Add project src to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

try:
    from fullon_ohlcv.repositories.ohlcv import TradeRepository
    from fullon_cache import ProcessCache
    from fullon_log import get_component_logger
    from fullon_orm import Database
    from tests.factories import (
        create_realistic_candles,
        create_realistic_trades,
        create_candle_list,
        create_trade_list
    )
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("⚠️ Make sure fullon libraries are installed and project src/ is in path")
    sys.exit(1)


# ============================================================================
# CONSOLE COLORS AND FORMATTING
# ============================================================================

class Colors:
    """ANSI color codes for console output"""
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'
    BOLD = '\033[1m'
    END = '\033[0m'


def print_header(message: str):
    """Print formatted header message"""
    print(f"\n{Colors.BOLD}{Colors.CYAN}{'='*60}")
    print(f"{message}")
    print(f"{'='*60}{Colors.END}\n")


def print_success(message: str):
    """Print success message in green"""
    print(f"{Colors.GREEN}{message}{Colors.END}")


def print_error(message: str):
    """Print error message in red"""
    print(f"{Colors.RED}{message}{Colors.END}")


def print_warning(message: str):
    """Print warning message in yellow"""
    print(f"{Colors.YELLOW}{message}{Colors.END}")


def print_info(message: str):
    """Print info message in blue"""
    print(f"{Colors.BLUE}{message}{Colors.END}")


# ============================================================================
# TEST DATABASE MANAGEMENT
# ============================================================================

def generate_test_db_name() -> str:
    """Generate random test database name"""
    suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
    return f"fullon_ohlcv_test_{suffix}"


async def create_test_database(db_name: str) -> bool:
    """Create test database with TimescaleDB extension"""
    logger = get_component_logger("demo_data.database")
    
    try:
        # Connect to postgres database to create new test database
        async with Database() as db:
            # Create database
            await db.execute(f"CREATE DATABASE {db_name}")
            logger.info(f"Created test database: {db_name}")
            
        # Connect to new database and setup TimescaleDB
        async with Database(database=db_name) as db:
            # Enable TimescaleDB extension
            await db.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
            
            # Create hypertables if they don't exist (fullon_ohlcv will handle this)
            # Just verify connection works
            await db.execute("SELECT 1")
            logger.info(f"Initialized TimescaleDB in: {db_name}")
            
        return True
        
    except Exception as e:
        logger.error(f"Failed to create test database: {e}")
        return False


async def drop_test_database(db_name: str) -> bool:
    """Drop test database"""
    logger = get_component_logger("demo_data.database")
    
    try:
        async with Database() as db:
            # Terminate connections to the database first
            await db.execute(f"""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = '{db_name}' AND pid <> pg_backend_pid()
            """)
            
            # Drop database
            await db.execute(f"DROP DATABASE IF EXISTS {db_name}")
            logger.info(f"Dropped test database: {db_name}")
            
        return True
        
    except Exception as e:
        logger.error(f"Failed to drop test database: {e}")
        return False


async def list_test_databases() -> List[str]:
    """List all test databases"""
    try:
        async with Database() as db:
            result = await db.execute("""
                SELECT datname FROM pg_database 
                WHERE datname LIKE 'fullon_ohlcv_test_%'
                ORDER BY datname
            """)
            return [row[0] for row in result.fetchall()]
            
    except Exception as e:
        print_error(f"Failed to list test databases: {e}")
        return []


@asynccontextmanager
async def test_database_context(db_name: str):
    """Context manager for test database lifecycle"""
    logger = get_component_logger("demo_data.context")
    
    try:
        # Create test database
        print_info(f"Creating test database: {db_name}")
        success = await create_test_database(db_name)
        if not success:
            raise RuntimeError(f"Failed to create test database: {db_name}")
            
        # Set environment variable for examples to use
        original_db = os.environ.get('POSTGRES_DATABASE')
        os.environ['POSTGRES_DATABASE'] = db_name
        
        logger.info(f"Test database ready: {db_name}")
        yield db_name
        
    finally:
        # Restore original database setting
        if original_db:
            os.environ['POSTGRES_DATABASE'] = original_db
        elif 'POSTGRES_DATABASE' in os.environ:
            del os.environ['POSTGRES_DATABASE']
            
        # Clean up test database
        print_info(f"Cleaning up test database: {db_name}")
        await drop_test_database(db_name)
        logger.info(f"Test database cleaned up: {db_name}")


# ============================================================================
# DEMO DATA GENERATION
# ============================================================================

async def install_demo_data():
    """Install demo trade data for examples to work with
    
    Note: This installs TRADE data only (not pre-made candles).
    The OHLCV service examples will demonstrate converting trades to 1-minute candles.
    """
    logger = get_component_logger("demo_data.install")
    
    print_info("Installing demo trade data (raw trades for conversion to candles)...")
    
    # Demo exchanges and symbols
    exchanges = ["binance", "kraken"]
    symbols = ["BTC/USDT", "ETH/USDT", "BTC/EUR"]
    
    try:
        # Install TRADE data only for each exchange/symbol
        for exchange in exchanges:
            for symbol in symbols:
                # Generate realistic trade data over last 24 hours
                # This simulates what the service would collect from WebSocket feeds
                trades = create_realistic_trades(
                    count=2000,  # 2000 trades over 24 hours (realistic for major pairs)
                    exchange=exchange,
                    symbol=symbol,
                    start_time=datetime.now(timezone.utc) - timedelta(hours=24),
                    price_volatility=0.001,
                    volume_range=(0.01, 5.0)
                )
                
                # Store trades using TradeRepository 
                async with TradeRepository(exchange=exchange, symbol=symbol, test=True) as repo:
                    await repo.save_trades(trades)
                    
                logger.info(f"Installed {len(trades)} trades for {exchange}:{symbol}")
                
                # Also add some very recent trades (last 10 minutes) for real-time examples
                recent_trades = create_realistic_trades(
                    count=50,  # Recent active trading
                    exchange=exchange,
                    symbol=symbol,
                    start_time=datetime.now(timezone.utc) - timedelta(minutes=10),
                    price_volatility=0.0005,
                    volume_range=(0.1, 2.0)
                )
                
                async with TradeRepository(exchange=exchange, symbol=symbol, test=True) as repo:
                    await repo.save_trades(recent_trades)
                    
                logger.info(f"Installed {len(recent_trades)} recent trades for {exchange}:{symbol}")
        
        # Update process cache to show services as available
        async with ProcessCache() as cache:
            # Register OHLCV service processes
            await cache.new_process(
                type="ohlcv",
                name="ohlcv_manager", 
                status="running",
                pid=os.getpid(),
                config={
                    "exchanges": exchanges,
                    "symbols": symbols,
                    "collection_interval": 60
                }
            )
            
            await cache.new_process(
                type="trade",
                name="trade_manager",
                status="running", 
                pid=os.getpid(),
                config={
                    "exchanges": exchanges,
                    "symbols": symbols,
                    "collection_enabled": True
                }
            )
            
        print_success("✅ Demo trade data installed successfully!")
        print_info(f"  📊 Exchanges: {', '.join(exchanges)}")
        print_info(f"  💹 Symbols: {', '.join(symbols)}")
        print_info(f"  🕐 Trade data: 24 hours historical + 10 minutes recent")
        print_info(f"  🔄 Ready for trade-to-candle conversion examples")
        
    except Exception as e:
        logger.error(f"Failed to install demo data: {e}")
        print_error(f"❌ Demo data installation failed: {e}")
        raise


async def cleanup_demo_data():
    """Clean up demo data and cache entries"""
    logger = get_component_logger("demo_data.cleanup")
    
    try:
        # Clean up process cache entries
        async with ProcessCache() as cache:
            processes = await cache.get_processes()
            for process in processes:
                if process.get("type") in ["ohlcv", "trade"]:
                    await cache.delete_process(process["name"])
                    
        print_success("✅ Demo data cleaned up!")
        
    except Exception as e:
        logger.error(f"Failed to clean up demo data: {e}")
        print_warning(f"⚠️ Demo data cleanup failed: {e}")


# ============================================================================
# MAIN CLI INTERFACE
# ============================================================================

async def main():
    """Main CLI function"""
    parser = argparse.ArgumentParser(
        description="Demo data management for fullon_ohlcv_service examples",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--setup', metavar='DB_NAME',
                        help='Create test database with demo data')
    parser.add_argument('--cleanup', metavar='DB_NAME', 
                        help='Remove test database')
    parser.add_argument('--list', action='store_true',
                        help='List existing test databases')
    parser.add_argument('--install-data', action='store_true',
                        help='Install demo data in current database')
    
    args = parser.parse_args()
    
    if args.list:
        print_header("TEST DATABASES")
        databases = await list_test_databases()
        if databases:
            for db in databases:
                print_info(f"  📊 {db}")
            print_info(f"\nTotal: {len(databases)} test databases")
        else:
            print_info("  No test databases found")
        return 0
        
    elif args.setup:
        print_header(f"SETTING UP TEST DATABASE: {args.setup}")
        
        success = await create_test_database(args.setup)
        if not success:
            print_error("❌ Failed to create test database")
            return 1
            
        # Set environment to use test database
        original_db = os.environ.get('POSTGRES_DATABASE')
        os.environ['POSTGRES_DATABASE'] = args.setup
        
        try:
            await install_demo_data()
            print_success(f"✅ Test database ready: {args.setup}")
            print_info(f"🔧 To use: export POSTGRES_DATABASE={args.setup}")
            return 0
        finally:
            # Restore original database setting
            if original_db:
                os.environ['POSTGRES_DATABASE'] = original_db
            elif 'POSTGRES_DATABASE' in os.environ:
                del os.environ['POSTGRES_DATABASE']
                
    elif args.cleanup:
        print_header(f"CLEANING UP TEST DATABASE: {args.cleanup}")
        
        success = await drop_test_database(args.cleanup)
        if success:
            print_success(f"✅ Test database removed: {args.cleanup}")
            return 0
        else:
            print_error("❌ Failed to remove test database")
            return 1
            
    elif args.install_data:
        print_header("INSTALLING DEMO DATA")
        await install_demo_data()
        return 0
        
    else:
        parser.print_help()
        return 0


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print_warning("\n⚠️ Operation interrupted by user")
        sys.exit(1)
    except Exception as e:
        print_error(f"\n❌ Unexpected error: {e}")
        sys.exit(1)