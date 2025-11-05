#!/usr/bin/env python3
"""
Example: Yahoo Finance Historical OHLCV Collection

Demonstrates historical OHLCV data collection from Yahoo Finance using the
HistoricOHLCVCollector. Tests 1-day timeframe data collection and storage.

Usage:
    python yahoo_historical_example.py       # Test SPX (S&P 500 - translates to ^GSPC)
    python yahoo_historical_example.py GOLD  # Test GOLD (Gold futures - translates to GC=F)

Note: Uses clean symbols (SPX, GOLD) that are translated by Yahoo adapter
      SPX → ^GSPC, GOLD → GC=F (see fullon_exchange Yahoo SYMBOL_TRANSLATION)
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
from demo_data import create_dual_test_databases, drop_dual_test_databases, install_demo_data
from fullon_log import get_component_logger
from fullon_orm import DatabaseContext, init_db
from fullon_ohlcv_service.utils.add_symbols import add_all_symbols

logger = get_component_logger("fullon.ohlcv.yahoo_example")


async def load_yahoo_symbol(symbol_str: str = "SPX"):
    """
    Load Yahoo Finance symbol from the database.

    Args:
        symbol_str: Clean Yahoo symbol to load (default: SPX → ^GSPC S&P 500)

    Returns:
        Symbol: The loaded symbol object
    """
    print(f"\n📊 Loading Yahoo Finance symbol: {symbol_str}")
    print("=" * 50)

    async with DatabaseContext() as db:
        # Get Yahoo category exchange
        cat_exchanges = await db.exchanges.get_cat_exchanges(all=True)
        yahoo_cat_ex = None
        for ce in cat_exchanges:
            if ce.name == "yahoo":
                yahoo_cat_ex = ce
                break

        if not yahoo_cat_ex:
            raise ValueError("Yahoo exchange not found. Demo data may not be installed.")

        # Get symbol from database
        symbol = await db.symbols.get_by_symbol(
            symbol_str,
            cat_ex_id=yahoo_cat_ex.cat_ex_id
        )

        if not symbol:
            raise ValueError(f"Symbol {symbol_str} not found for Yahoo exchange")

        print(f"  ✅ Loaded symbol: {symbol.symbol}")
        print(f"     Exchange: {symbol.cat_exchange.name}")
        print(f"     Backtest period: {symbol.backtest} days")
        print(f"     Timeframe: {symbol.updateframe}")

        return symbol


async def collect_yahoo_historical(symbol):
    """
    Collect historical OHLCV data for Yahoo Finance symbol using HistoricOHLCVCollector.

    Args:
        symbol: Symbol object from database
    """
    print(f"\n📈 Starting historical OHLCV collection for {symbol.symbol} on Yahoo Finance...")
    print("=" * 50)

    from fullon_ohlcv_service.ohlcv.historic_collector import HistoricOHLCVCollector

    try:
        # Initialize symbol tables in TimescaleDB (required before collection)
        print(f"\n🔧 Initializing TimescaleDB tables for {symbol.symbol}...")
        success = await add_all_symbols(symbols=[symbol])
        if not success:
            logger.warning(f"Symbol initialization failed for {symbol.symbol} - continuing anyway")
        else:
            print(f"✅ Symbol tables initialized")

        # Create collector
        collector = HistoricOHLCVCollector()

        # Collect historical data for the symbol
        print(f"\n⏳ Collecting {symbol.backtest} days of historical data...")
        candles_count = await collector.start_symbol(symbol)

        print(f"✅ Collection completed: {candles_count} candles collected")

        return candles_count

    except Exception as e:
        print(f"❌ Collection failed: {e}")
        logger.exception("Collection failed")
        raise


async def verify_yahoo_data(symbol):
    """
    Verify the collected Yahoo Finance OHLCV data in the database.

    Args:
        symbol: Symbol object from database
    """
    try:
        print(f"\n🔍 Verifying collected OHLCV data for {symbol.symbol}...")
        print("=" * 50)

        import arrow
        from fullon_ohlcv.repositories.ohlcv import TimeseriesRepository

        # Query the full historical range
        end_time = datetime.now(UTC)
        start_time = end_time - timedelta(days=symbol.backtest)

        exchange_name = symbol.cat_exchange.name
        symbol_str = symbol.symbol

        async with TimeseriesRepository(exchange_name, symbol_str, test=False) as repo:
            # Fetch 1-day candles
            ohlcv_1d = await repo.fetch_ohlcv(
                compression=1,
                period="days",
                fromdate=arrow.get(start_time),
                todate=arrow.get(end_time),
            )

            if not ohlcv_1d:
                print("⚠️  No data found in database")
                return

            # Calculate date range
            oldest_ts = ohlcv_1d[0][0]
            newest_ts = ohlcv_1d[-1][0]
            oldest_date = arrow.get(oldest_ts).format("YYYY-MM-DD")
            newest_date = arrow.get(newest_ts).format("YYYY-MM-DD")
            days_span = (arrow.get(newest_ts) - arrow.get(oldest_ts)).days

            print(f"\n📊 Data Summary:")
            print(f"  • Total candles: {len(ohlcv_1d)}")
            print(f"  • Date range: {oldest_date} to {newest_date}")
            print(f"  • Days span: {days_span} days")
            print(f"  • Timeframe: 1d (daily)")

            # Show last 10 candles
            recent_candles = ohlcv_1d[-10:] if len(ohlcv_1d) >= 10 else ohlcv_1d
            print(f"\n📅 Last {len(recent_candles)} daily candles:")
            print("  " + "-" * 70)
            print(f"  {'Date':<12} | {'Open':>10} | {'High':>10} | {'Low':>10} | {'Close':>10} | {'Volume':>12}")
            print("  " + "-" * 70)

            for ts, o, h, l, c, v in recent_candles:
                if any(x is None for x in (o, h, l, c, v)):
                    print(f"  ⚠️  Invalid candle data (contains None): {ts}")
                    continue

                candle_date = arrow.get(ts).format("YYYY-MM-DD")
                print(f"  {candle_date:<12} | {o:>10.2f} | {h:>10.2f} | {l:>10.2f} | {c:>10.2f} | {v:>12,.0f}")

            print("  " + "-" * 70)
            print(f"\n✅ OHLCV verification complete: {len(ohlcv_1d)} daily candles stored")

    except Exception as e:
        print(f"❌ OHLCV verification failed: {e}")
        logger.exception("OHLCV verification failed")


async def main():
    """Run Yahoo Finance historical OHLCV collection example."""
    # Get symbol from command line or use default
    symbol_str = sys.argv[1] if len(sys.argv) > 1 else "SPX"

    print("=" * 70)
    print("Yahoo Finance Historical OHLCV Collection Example".center(70))
    print("=" * 70)
    print(f"\nTarget Symbol: {symbol_str}")
    print(f"Timeframe: 1d (daily)")
    print(f"Historical Period: 365 days")

    try:
        # Create dual test databases
        logger.debug("Creating dual test databases", orm_db=test_db_orm, ohlcv_db=test_db_ohlcv)
        orm_db_name, ohlcv_db_name = await create_dual_test_databases(test_db_base)
        logger.debug("Using dual test databases", orm_db=orm_db_name, ohlcv_db=ohlcv_db_name)

        # Initialize database schema
        logger.debug("Initializing database schema")
        await init_db()
        print("\n✅ Test databases initialized")

        # Install demo data (includes Yahoo exchange and symbol)
        logger.debug("Installing demo data")
        await install_demo_data()
        print("\n✅ Demo data installed")

        # Load Yahoo symbol from database
        symbol = await load_yahoo_symbol(symbol_str)

        # Collect historical data
        candles_count = await collect_yahoo_historical(symbol)

        # Verify collected data
        await verify_yahoo_data(symbol)

        print("\n" + "=" * 70)
        print("✅ YAHOO FINANCE HISTORICAL COLLECTION EXAMPLE COMPLETED")
        print("=" * 70)
        print("\n📋 Summary:")
        print(f"  ✅ Symbol: {symbol.symbol}")
        print(f"  ✅ Candles collected: {candles_count}")
        print(f"  ✅ Historical period: {symbol.backtest} days")
        print(f"  ✅ Timeframe: 1d (daily)")
        print(f"  ✅ Data stored in: {ohlcv_db_name}")

    except Exception as e:
        print(f"\n❌ Example failed: {e}")
        logger.exception("Example failed")

    finally:
        # Clean up test databases
        try:
            logger.debug("Dropping dual test databases", orm_db=test_db_orm, ohlcv_db=test_db_ohlcv)
            await drop_dual_test_databases(test_db_orm, test_db_ohlcv)
            logger.debug("Test databases cleaned up successfully")
            print("\n✅ Test databases cleaned up")
        except Exception as db_cleanup_error:
            logger.warning("Error during database cleanup", error=str(db_cleanup_error))


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] in ["-h", "--help"]:
        print("Usage: python yahoo_historical_example.py [symbol]")
        print("\nExamples:")
        print("  python yahoo_historical_example.py          # Test SPX (S&P 500)")
        print("  python yahoo_historical_example.py GOLD     # Test GOLD (Gold futures)")
        print("\nAvailable symbols: SPX, GOLD")
        print("  SPX  → ^GSPC (S&P 500 index)")
        print("  GOLD → GC=F  (Gold futures)")
        print("\nNote: Symbol translation handled by Yahoo adapter (fullon_exchange)")
        print("      Clean symbols in database, special chars (^, =) only in Yahoo API")
        print("\nThis example demonstrates:")
        print("  • Installing demo data with Yahoo Finance exchange")
        print("  • Using HistoricOHLCVCollector for historical data collection")
        print("  • Collecting 365 days of 1d (daily) timeframe data")
        print("  • Symbol translation (clean DB symbols → Yahoo API symbols)")
        print("  • Storing data in fullon_ohlcv timeseries database")
        print("  • Verifying collected data with sample output")
        sys.exit(0)

    asyncio.run(main())
