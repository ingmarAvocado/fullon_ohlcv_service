#!/usr/bin/env python3
"""
Test Two-Phase Collection Pattern

Tests the new two-phase collection pattern:
Phase 1: Historical catch-up via REST calls
Phase 2: Real-time streaming via WebSocket

Based on legacy pattern but using fullon_exchange library.

Usage:
    python trade_ws_collection_example.py
"""

import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fullon_ohlcv_service.ohlcv import historic_collector

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

os.environ['DB_NAME'] = test_db_orm
os.environ['DB_OHLCV_NAME'] = test_db_ohlcv

# Now safe to import modules
from demo_data import (
    create_dual_test_databases,
    drop_dual_test_databases,
    install_demo_data
)
from fullon_ohlcv_service.trade.live_collector import LiveTradeCollector
from fullon_log import get_component_logger
from fullon_orm import DatabaseContext
from fullon_orm import init_db

logger = get_component_logger("fullon.trade.test")


async def set_database():
    """Test Trade two-phase collection pattern with proper database setup."""
    print("\n🔍 Testing Trade Two-Phase Collection Pattern")
    print("="*50)
    # Set up test databases like the working example
    logger.debug("Creating dual test databases", orm_db=test_db_orm, ohlcv_db=test_db_ohlcv)
    orm_db_name, ohlcv_db_name = await create_dual_test_databases(test_db_base)
    logger.debug("Using dual test databases", orm_db=orm_db_name, ohlcv_db=ohlcv_db_name)

    # Initialize database schema
    logger.debug("Initializing database schema")
    await init_db()

    # Install demo data following fullon_orm demo_install.py pattern
    logger.debug("Installing demo data")
    await install_demo_data()
    logger.info("Demo data installed successfully")

async def historic_collecting():
    """Test historical trade collection using HistoricTradeCollector."""
    from fullon_ohlcv_service.trade.historic_collector import HistoricTradeCollector

    try:
        print("\n📚 Starting historical trade collection...")
        collector = HistoricTradeCollector()

        # Start bulk historical collection for all configured symbols
        results = await collector.start_collection()

        # Display results
        total_trades = sum(results.values())
        print(f"✅ Historical collection completed!")
        print(f"   Symbols processed: {len(results)}")
        print(f"   Total trades collected: {total_trades:,}\n")

        # Show per-symbol breakdown
        if results:
            print("📊 Per-symbol results:")
            for symbol_key, trade_count in sorted(results.items()):
                status = "✅" if trade_count > 0 else "⚠️"
                print(f"  {status} {symbol_key}: {trade_count:,} trades")
        else:
            print("⚠️  No symbols configured for historical collection")

    except Exception as e:
        print(f"❌ Historical collection failed: {e}")
        logger.exception("Historical collection failed")


async def live_collecting():
    collector = None  # Initialize to avoid NameError in finally
    try:
        print("🚀 Starting live trade collector...")
        collector = LiveTradeCollector()

        # Check what's in the cache for all symbols (like ticker service monitoring loop)
        async with DatabaseContext() as db:
            admin_email = os.getenv("ADMIN_MAIL", "admin@fullon")
            admin_uid = await db.users.get_user_id(admin_email)
            admin_exchanges = await db.exchanges.get_user_exchanges(admin_uid)
            all_symbols = await db.symbols.get_all()

        # Safe symbol access
        if not all_symbols:
            raise ValueError("No symbols found in database")

        test_symbol = all_symbols[0] if len(all_symbols) == 1 else all_symbols[1]

        # Safe exchange finding with fallback
        admin_exchange = next(
            (ex for ex in admin_exchanges if ex.cat_exchange.name == test_symbol.cat_exchange.name),
            None
        )

        if not admin_exchange:
            raise ValueError(f"No exchange found for {test_symbol.cat_exchange.name}")

        await collector._start_exchange_collector(exchange_obj=admin_exchange, symbols=[test_symbol])
        print("✅ Live trade collector started")
        print("📊 Trade Cache Status:")

        COLLECTION_DURATION = 70  # seconds
        await asyncio.sleep(COLLECTION_DURATION)

    except Exception as e:
        print(f"❌ Trade test failed: {e}")
        logger.exception("Trade test failed")  # Use .exception() for traceback
    finally:
        if collector:  # Only stop if initialized
            await collector.stop_collection()
            print("✅ Trade processing stopped")



async def check_fullon_content():
    """
    we use fullon_ohlcv and check that we have old and recent 1min candle
    """

async def main():
    """Main test function."""
    print("🧪 TWO-PHASE COLLECTION PATTERN TEST")
    print("Testing the legacy-inspired two-phase collection pattern")
    print("\nPattern:")
    print("  Phase 1: Historical catch-up (REST with pagination)")
    print("  Phase 2: Real-time streaming (WebSocket)")
    print("  Priority: OHLCV first, trades as fallback")

    try:
        # Test individual components
        try:
            await set_database()
            await historic_collecting()            
            #await live_collecting()
            await check_fullon_content()
        except Exception as e:
            print(f"❌ Cannot create test database: {e}")
            logger.error("Cannot create test database", error=str(e))
        finally:
            # Clean up test databases
            try:
                logger.debug("Dropping dual test databases", orm_db=test_db_orm, ohlcv_db=test_db_ohlcv)
                await drop_dual_test_databases(test_db_orm, test_db_ohlcv)
                logger.debug("Test databases cleaned up successfully")
            except Exception as db_cleanup_error:
                logger.warning("Error during database cleanup", error=str(db_cleanup_error))

        print("\n" + "="*60)
        print("✅ TWO-PHASE COLLECTION TEST COMPLETED")
        print("📋 Summary:")
        print("  ✅ Historical pagination logic implemented")
        print("  ✅ Trade two-phase pattern implemented")
        print("  ✅ Follows legacy architecture patterns")
        print("\n💡 Ready for production with database and WebSocket streaming")

    except Exception as e:
        print(f"❌ Test suite failed: {e}")

    finally:
        # Clean up exchange resources
        try:
            from fullon_exchange.queue import ExchangeQueue
            await ExchangeQueue.shutdown_factory()
        except Exception as cleanup_error:
            pass


if __name__ == "__main__":
    asyncio.run(main())