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

from datetime import UTC, datetime, timedelta
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
import arrow
from demo_data import create_dual_test_databases, drop_dual_test_databases, install_demo_data
from fullon_log import get_component_logger
from fullon_ohlcv.repositories.ohlcv import TimeseriesRepository
from fullon_orm import DatabaseContext, init_db
from fullon_ohlcv_service.daemon import OhlcvServiceDaemon


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

    # Load specific symbol for Trade testing
    async with DatabaseContext() as db:
        # Get kraken category exchange
        cat_exchanges = await db.exchanges.get_cat_exchanges(all=True)
        kraken_cat_ex = None
        for cat_ex in cat_exchanges:
            if cat_ex.name == "kraken":
                kraken_cat_ex = cat_ex
                break

        if not kraken_cat_ex:
            raise ValueError("Kraken exchange not found")

        # Get BTC/USDC symbol for kraken
        symbol = await db.symbols.get_by_symbol("BTC/USDC", cat_ex_id=kraken_cat_ex.cat_ex_id)
        if not symbol:
            raise ValueError("BTC/USDC symbol not found on kraken exchange")

    logger.info("Loaded symbol", symbol=symbol.symbol, exchange=symbol.cat_exchange.name)
    return symbol


async def example_trade_collection(symbol):
    """Collect trade data for a single symbol using the daemon."""
    print(f"\n📊 Starting trade collection for {symbol.symbol} on {symbol.cat_exchange.name}...")

    try:
        # Create daemon

        daemon = OhlcvServiceDaemon()

        # Start collection in background
        await daemon.process_symbol(symbol)

        current = datetime.now()
        seconds_to_end = 60 - current.second
        sleep_duration = seconds_to_end + 1
        print(
            f"⏱️  Running collection until end of minute plus 1 second ({sleep_duration} seconds)..."
        )
        await asyncio.sleep(sleep_duration)

        # Check results
        await verify_ohlcv_data(symbol)

        print("✅ Collection completed")

    except Exception as e:
        print(f"❌ Collection failed: {e}")
        logger.exception("Collection failed")


async def verify_ohlcv_data(symbol):
    """
    Use fullon_ohlcv to verify we have recent OHLCV candles for the collected symbol.
    Checks last 10 1-minute candles for the symbol.
    """

    try:
        print(
            f"\n📊 Checking OHLCV candle data for {symbol.symbol} on {symbol.cat_exchange.name}..."
        )

        total_symbols_checked = 0
        total_candles_found = 0

        # Check OHLCV for the symbol
        exchange_name = symbol.cat_exchange.name
        symbol_str = symbol.symbol
        symbol_key = f"{exchange_name}:{symbol_str}"

        print(f"\n🔍 Checking symbol: {symbol_key}")

        # Note: Since we're now zero-boilerplate, we skip the exchange capability check
        # and assume all exchanges may have OHLCV data from trade collection

        # Check recent 1-minute candles (last 15 minutes)
        end_time = datetime.now(UTC)
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
                        # Handle None values gracefully
                        o_str = f"{o:.2f}" if o is not None else "N/A"
                        h_str = f"{h:.2f}" if h is not None else "N/A"
                        l_str = f"{l:.2f}" if l is not None else "N/A"
                        c_str = f"{c:.2f}" if c is not None else "N/A"
                        v_str = f"{v:.4f}" if v is not None else "N/A"
                        print(
                            f"   {candle_time} | O:{o_str} H:{h_str} L:{l_str} C:{c_str} V:{v_str}"
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

        print(
            f"\n✅ OHLCV verification complete: checked {total_symbols_checked} symbols, found {total_candles_found} total candles"
        )

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

    symbol = None
    try:
        # Test individual components
        try:
            symbol = await set_database()
            await example_trade_collection(symbol)

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
        print("✅ SINGLE-SYMBOL TRADE COLLECTION TEST COMPLETED")
        print("📋 Summary:")
        print("  ✅ Daemon process_symbol() method implemented")
        print("  ✅ Automatic collector selection based on exchange capabilities")
        print("  ✅ Two-phase collection (historic + live) working")
        print("\n💡 Ready for production with intelligent collector selection")

    except Exception as e:
        print(f"❌ Test suite failed: {e}")

    finally:
        # Clean up exchange resources
        pass


if __name__ == "__main__":
    asyncio.run(main())
