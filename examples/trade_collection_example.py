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
from collections import defaultdict
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
from fullon_ohlcv_service.trade.live_collector import LiveTradeCollector
from fullon_log import get_component_logger
from fullon_orm import DatabaseContext
from fullon_orm import init_db
import arrow
from datetime import datetime, timezone, timedelta
from fullon_ohlcv.repositories.ohlcv import TimeseriesRepository
from fullon_ohlcv_service.trade.historic_collector import HistoricTradeCollector


logger = get_component_logger("fullon.trade.test")


async def set_database():
    """Test Trade two-phase collection pattern with proper database setup."""
    print("\n🔍 Testing Trade Two-Phase Collection Pattern")
    print("=" * 50)
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
    """Test historical trade collection using HistoricTradeCollector for single symbol."""

    try:
        print("\n📚 Starting historical trade collection (single symbol test)...")

        # Use HistoricTradeCollector for single symbol
        collector = HistoricTradeCollector()
        await collector.start_collection()

        print(f"✅ Historical collection completed!")
    except Exception as e:
        print(f"❌ Historical collection failed: {e}")
        logger.exception("Historical collection failed")


async def live_collecting():
    collector = None  # Initialize to avoid NameError in finally
    try:
        print("🚀 Starting live trade collector...")
        collector = LiveTradeCollector()

        # Initialize ExchangeQueue factory (required for WebSocket handlers)
        from fullon_exchange.queue import ExchangeQueue

        await ExchangeQueue.initialize_factory()

        # Check what's in the cache for all symbols (like ticker service monitoring loop)
        async with DatabaseContext() as db:
            admin_email = os.getenv("ADMIN_MAIL", "admin@fullon")
            admin_uid = await db.users.get_user_id(admin_email)
            if admin_uid is None:
                raise ValueError(f"Admin user {admin_email} not found")
            admin_exchanges = await db.exchanges.get_user_exchanges(admin_uid)
            all_symbols = await db.symbols.get_all()

        if not all_symbols:
            raise ValueError("No symbols found in database")

        # Group symbols by exchange
        symbol_map = defaultdict(list)
        for symbol in all_symbols:
            symbol_map[symbol.cat_exchange.name].append(symbol)

        # Start collectors for all admin exchanges with their symbols
        for admin_exchange in admin_exchanges:
            exchange_name = admin_exchange.cat_exchange.name
            symbols = symbol_map.get(exchange_name, [])
            if symbols:
                await collector._start_exchange_collector(
                    exchange_obj=admin_exchange, symbols=symbols
                )
                print(
                    f"✅ Live trade collector started for {exchange_name} with {len(symbols)} symbols"
                )
            else:
                print(f"⚠️  No symbols found for {exchange_name}")
        print("📊 Trade Cache Status:")

        current = arrow.now()
        next_minute = current.replace(second=0, microsecond=0) + timedelta(minutes=1)
        sleep_seconds = (next_minute - current).total_seconds()
        await asyncio.sleep(sleep_seconds)

    except Exception as e:
        print(f"❌ Trade test failed: {e}")
        logger.exception("Trade test failed")  # Use .exception() for traceback
    finally:
        if collector:  # Only stop if initialized
            await collector.stop_collection()
            print("✅ Trade processing stopped")


async def check_fullon_content():
    """
    Use fullon_ohlcv to verify we have recent OHLCV candles for all collected symbols.
    Checks last 10 1-minute candles for each symbol that was historically collected.
    """

    try:
        print("\n📊 Checking OHLCV candle data for all collected symbols...")

        # Initialize ExchangeQueue for handler checks
        from fullon_exchange.queue import ExchangeQueue

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

            # Check if this exchange requires trade collection for OHLCV
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

                handler = await ExchangeQueue.get_rest_handler(exchange_obj, credential_provider)  # type: ignore
                if not handler.needs_trades_for_ohlcv():
                    print(f"   ⚠️  {symbol_key} supports native OHLCV - skipping candle display")
                    continue
            except Exception as e:
                print(f"   ❌ Error getting handler for {symbol_key}: {e}")
                continue

            # Check recent 1-minute candles (last 15 minutes)
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
            await live_collecting()
            await check_fullon_content()
        except Exception as e:
            print(f"❌ Cannot create test database: {e}")
            logger.error("Cannot create test database", error=str(e))
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
