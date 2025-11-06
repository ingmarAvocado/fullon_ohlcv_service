#!/usr/bin/env python3
"""
Complete Yahoo Finance Test - Historical + Live Collection

Tests the complete Yahoo Finance OHLCV collection workflow:
1. Historical collection for both 1d (SPX) and 1m (GOLD) timeframes
2. Transition to live collection using REST polling fallback
3. Data persistence and retrieval verification

Usage:
    python test_yahoo_complete.py           # Test both SPX (1d) and GOLD (1m)
    python test_yahoo_complete.py --symbol SPX   # Test only SPX (daily)
    python test_yahoo_complete.py --symbol GOLD  # Test only GOLD (1-minute)
"""

import argparse
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

logger = get_component_logger("fullon.ohlcv.yahoo_complete_test")


class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    END = '\033[0m'
    BOLD = '\033[1m'


def print_success(msg: str):
    print(f"{Colors.GREEN}✓ {msg}{Colors.END}")


def print_error(msg: str):
    print(f"{Colors.RED}✗ {msg}{Colors.END}")


def print_warning(msg: str):
    print(f"{Colors.YELLOW}⚠ {msg}{Colors.END}")


def print_info(msg: str):
    print(f"{Colors.CYAN}→ {msg}{Colors.END}")


def print_header(msg: str):
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'=' * 70}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{msg:^70}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'=' * 70}{Colors.END}\n")


async def load_yahoo_symbol(symbol_str: str = "SPX"):
    """
    Load Yahoo Finance symbol from the database.

    Args:
        symbol_str: Clean Yahoo symbol to load (SPX or GOLD)

    Returns:
        Symbol: The loaded symbol object
    """
    print_info(f"Loading Yahoo Finance symbol: {symbol_str}")

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

        print_success(f"Loaded symbol: {symbol.symbol}")
        print_info(f"  Exchange: {symbol.cat_exchange.name}")
        print_info(f"  Backtest period: {symbol.backtest} days")
        print_info(f"  Timeframe: {symbol.updateframe}")

        return symbol


async def collect_historical_data(symbol):
    """
    Collect historical OHLCV data using HistoricOHLCVCollector.

    Args:
        symbol: Symbol object from database

    Returns:
        int: Number of candles collected
    """
    print_header(f"HISTORICAL COLLECTION: {symbol.symbol} ({symbol.updateframe})")

    from fullon_ohlcv_service.ohlcv.historic_collector import HistoricOHLCVCollector

    try:
        # Initialize symbol tables in TimescaleDB
        print_info(f"Initializing TimescaleDB tables for {symbol.symbol}...")
        success = await add_all_symbols(symbols=[symbol])
        if not success:
            logger.warning(f"Symbol initialization failed for {symbol.symbol} - continuing anyway")
        else:
            print_success("Symbol tables initialized")

        # Create collector
        collector = HistoricOHLCVCollector()

        # Collect historical data
        print_info(f"Collecting {symbol.backtest} days of historical {symbol.updateframe} data...")
        candles_count = await collector.start_symbol(symbol)

        print_success(f"Historical collection completed: {candles_count} candles collected")

        return candles_count

    except Exception as e:
        print_error(f"Historical collection failed: {e}")
        logger.exception("Historical collection failed")
        raise


async def start_live_collection(symbol, duration_seconds: int = 120):
    """
    Start live collection using LiveOHLCVCollector with REST polling fallback.

    Args:
        symbol: Symbol object from database
        duration_seconds: How long to run live collection (default: 120 seconds)

    Returns:
        int: Number of new candles collected during live period
    """
    print_header(f"LIVE COLLECTION: {symbol.symbol} ({symbol.updateframe})")

    from fullon_ohlcv_service.ohlcv.live_collector import LiveOHLCVCollector
    import arrow
    from fullon_ohlcv.repositories.ohlcv import TimeseriesRepository

    try:
        # Get initial candle count
        exchange_name = symbol.cat_exchange.name
        symbol_str = symbol.symbol

        # Determine period for fetching (for counting)
        period = "days" if 'd' in symbol.updateframe else "minutes" if 'm' in symbol.updateframe else "hours"
        compression = int(symbol.updateframe.rstrip('mhd'))

        # Fetch all existing candles to count them
        end_time = datetime.now(UTC)
        start_time = end_time - timedelta(days=symbol.backtest + 1)

        async with TimeseriesRepository(exchange_name, symbol_str, test=False) as repo:
            initial_data = await repo.fetch_ohlcv(
                compression=compression,
                period=period,
                fromdate=arrow.get(start_time),
                todate=arrow.get(end_time),
            )
            initial_count = len(initial_data) if initial_data else 0
            print_info(f"Initial candle count in database: {initial_count}")

        # Create live collector
        collector = LiveOHLCVCollector()

        # Start live collection for this symbol
        print_info(f"Starting live collection (REST polling fallback)...")
        print_info(f"Will run for {duration_seconds} seconds...")

        # Start collection in background
        collector.running = True
        await collector.start_symbol(symbol)

        # Wait for specified duration
        print_info(f"Live collection running... waiting {duration_seconds}s")
        await asyncio.sleep(duration_seconds)

        # Stop collection
        collector.running = False
        print_success(f"Live collection stopped after {duration_seconds}s")

        # Get final candle count
        async with TimeseriesRepository(exchange_name, symbol_str, test=False) as repo:
            final_data = await repo.fetch_ohlcv(
                compression=compression,
                period=period,
                fromdate=arrow.get(start_time),
                todate=arrow.get(end_time),
            )
            final_count = len(final_data) if final_data else 0
            print_info(f"Final candle count in database: {final_count}")

        new_candles = final_count - initial_count
        print_success(f"New candles collected during live period: {new_candles}")

        return new_candles

    except Exception as e:
        print_error(f"Live collection failed: {e}")
        logger.exception("Live collection failed")
        raise


async def verify_data_persistence(symbol):
    """
    Verify the collected OHLCV data can be retrieved from database.

    Args:
        symbol: Symbol object from database

    Returns:
        dict: Statistics about the collected data
    """
    print_header(f"DATA VERIFICATION: {symbol.symbol}")

    try:
        import arrow
        from fullon_ohlcv.repositories.ohlcv import TimeseriesRepository

        exchange_name = symbol.cat_exchange.name
        symbol_str = symbol.symbol

        # Query the full historical range
        end_time = datetime.now(UTC)
        start_time = end_time - timedelta(days=symbol.backtest + 1)  # +1 day buffer

        async with TimeseriesRepository(exchange_name, symbol_str, test=False) as repo:
            # Fetch candles for the symbol's configured timeframe
            ohlcv_data = await repo.fetch_ohlcv(
                compression=int(symbol.updateframe.rstrip('mhd')),  # Extract numeric part
                period="days" if 'd' in symbol.updateframe else "minutes" if 'm' in symbol.updateframe else "hours",
                fromdate=arrow.get(start_time),
                todate=arrow.get(end_time),
            )

            if not ohlcv_data:
                print_warning("No data found in database")
                return {
                    'total_candles': 0,
                    'date_range': None,
                    'days_span': 0
                }

            # Calculate statistics
            oldest_ts = ohlcv_data[0][0]
            newest_ts = ohlcv_data[-1][0]
            oldest_date = arrow.get(oldest_ts).format("YYYY-MM-DD HH:mm:ss")
            newest_date = arrow.get(newest_ts).format("YYYY-MM-DD HH:mm:ss")
            days_span = (arrow.get(newest_ts) - arrow.get(oldest_ts)).days

            print_success("Data Summary:")
            print_info(f"  Total candles: {len(ohlcv_data)}")
            print_info(f"  Date range: {oldest_date} to {newest_date}")
            print_info(f"  Days span: {days_span} days")
            print_info(f"  Timeframe: {symbol.updateframe}")

            # Show sample candles (first 5 and last 5)
            print("\n  First 5 candles:")
            for ts, o, h, l, c, v in ohlcv_data[:5]:
                candle_date = arrow.get(ts).format("YYYY-MM-DD HH:mm:ss")
                # Handle None values
                if any(x is None for x in (o, h, l, c, v)):
                    print(f"    {candle_date} | Contains None values - skipping")
                    continue
                print(f"    {candle_date} | O:{o:>10.2f} H:{h:>10.2f} L:{l:>10.2f} C:{c:>10.2f} V:{v:>12,.0f}")

            print("\n  Last 5 candles:")
            for ts, o, h, l, c, v in ohlcv_data[-5:]:
                candle_date = arrow.get(ts).format("YYYY-MM-DD HH:mm:ss")
                # Handle None values
                if any(x is None for x in (o, h, l, c, v)):
                    print(f"    {candle_date} | Contains None values - skipping")
                    continue
                print(f"    {candle_date} | O:{o:>10.2f} H:{h:>10.2f} L:{l:>10.2f} C:{c:>10.2f} V:{v:>12,.0f}")

            print_success(f"\nVerification complete: {len(ohlcv_data)} candles stored and retrievable")

            return {
                'total_candles': len(ohlcv_data),
                'date_range': (oldest_date, newest_date),
                'days_span': days_span
            }

    except Exception as e:
        print_error(f"Data verification failed: {e}")
        logger.exception("Data verification failed")
        raise


async def test_symbol_complete_workflow(symbol_str: str, live_duration: int = 120):
    """
    Test complete workflow for a single symbol: historical -> live -> verify.

    Args:
        symbol_str: Symbol to test (SPX or GOLD)
        live_duration: How long to run live collection in seconds

    Returns:
        dict: Test results
    """
    print_header(f"COMPLETE WORKFLOW TEST: {symbol_str}")

    try:
        # Load symbol
        symbol = await load_yahoo_symbol(symbol_str)

        # Step 1: Historical collection
        historical_candles = await collect_historical_data(symbol)

        # Step 2: Live collection (REST polling fallback)
        live_candles = await start_live_collection(symbol, duration_seconds=live_duration)

        # Step 3: Verify data persistence
        verification_stats = await verify_data_persistence(symbol)

        # Summary
        print_header(f"TEST SUMMARY: {symbol_str}")
        print_success(f"Symbol: {symbol.symbol}")
        print_info(f"  Timeframe: {symbol.updateframe}")
        print_info(f"  Historical candles: {historical_candles}")
        print_info(f"  Live candles (REST polling): {live_candles}")
        print_info(f"  Total candles in database: {verification_stats['total_candles']}")
        print_info(f"  Date range: {verification_stats['date_range']}")
        print_info(f"  Days span: {verification_stats['days_span']} days")

        return {
            'symbol': symbol_str,
            'timeframe': symbol.updateframe,
            'historical_candles': historical_candles,
            'live_candles': live_candles,
            'total_candles': verification_stats['total_candles'],
            'success': True
        }

    except Exception as e:
        print_error(f"Test failed for {symbol_str}: {e}")
        logger.exception(f"Test failed for {symbol_str}")
        return {
            'symbol': symbol_str,
            'success': False,
            'error': str(e)
        }


async def main():
    """Run complete Yahoo Finance test workflow."""
    parser = argparse.ArgumentParser(
        description="Complete Yahoo Finance OHLCV Collection Test",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        '--symbol',
        choices=['SPX', 'GOLD', 'both'],
        default='both',
        help='Which symbol to test (default: both)'
    )
    parser.add_argument(
        '--live-duration',
        type=int,
        default=120,
        help='How long to run live collection in seconds (default: 120)'
    )

    args = parser.parse_args()

    print_header("YAHOO FINANCE COMPLETE OHLCV TEST")
    print_info(f"Test symbols: {args.symbol}")
    print_info(f"Live collection duration: {args.live_duration}s")

    try:
        # Create dual test databases
        logger.debug("Creating dual test databases", orm_db=test_db_orm, ohlcv_db=test_db_ohlcv)
        orm_db_name, ohlcv_db_name = await create_dual_test_databases(test_db_base)
        logger.debug("Using dual test databases", orm_db=orm_db_name, ohlcv_db=ohlcv_db_name)

        # Initialize database schema
        logger.debug("Initializing database schema")
        await init_db()
        print_success("Test databases initialized")

        # Install demo data (includes Yahoo exchange and symbols)
        logger.debug("Installing demo data")
        await install_demo_data()
        print_success("Demo data installed")

        # Determine which symbols to test
        if args.symbol == 'both':
            symbols_to_test = ['SPX', 'GOLD']
        else:
            symbols_to_test = [args.symbol]

        # Test each symbol
        results = []
        for symbol_str in symbols_to_test:
            result = await test_symbol_complete_workflow(symbol_str, live_duration=args.live_duration)
            results.append(result)

        # Final summary
        print_header("FINAL TEST RESULTS")
        successful = [r for r in results if r['success']]
        failed = [r for r in results if not r['success']]

        print_info(f"Tests run: {len(results)}")
        print_success(f"Successful: {len(successful)}")
        if failed:
            print_error(f"Failed: {len(failed)}")

        for result in successful:
            print_success(f"✓ {result['symbol']} ({result['timeframe']}): "
                         f"{result['historical_candles']} historical + {result['live_candles']} live = "
                         f"{result['total_candles']} total candles")

        for result in failed:
            print_error(f"✗ {result['symbol']}: {result.get('error', 'Unknown error')}")

        # Exit code
        if failed:
            print_error("\nSome tests failed!")
            return False
        else:
            print_success("\n🎉 All tests passed!")
            return True

    except Exception as e:
        print_error(f"\nTest suite failed: {e}")
        logger.exception("Test suite failed")
        return False

    finally:
        # Clean up test databases
        try:
            logger.debug("Dropping dual test databases", orm_db=test_db_orm, ohlcv_db=test_db_ohlcv)
            await drop_dual_test_databases(test_db_orm, test_db_ohlcv)
            logger.debug("Test databases cleaned up successfully")
            print_success("Test databases cleaned up")
        except Exception as db_cleanup_error:
            logger.warning("Error during database cleanup", error=str(db_cleanup_error))


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] in ["-h", "--help"]:
        print("Usage: python test_yahoo_complete.py [options]")
        print("\nOptions:")
        print("  --symbol {SPX,GOLD,both}  Which symbol(s) to test (default: both)")
        print("  --live-duration SECONDS   How long to run live collection (default: 120)")
        print("\nExamples:")
        print("  python test_yahoo_complete.py                    # Test both symbols")
        print("  python test_yahoo_complete.py --symbol SPX       # Test only SPX (daily)")
        print("  python test_yahoo_complete.py --symbol GOLD      # Test only GOLD (1-minute)")
        print("  python test_yahoo_complete.py --live-duration 60 # Run live for 1 minute")
        print("\nThis test demonstrates:")
        print("  1. Historical OHLCV collection for 1d (SPX) and 1m (GOLD) timeframes")
        print("  2. Automatic transition to live collection using REST polling fallback")
        print("  3. Data persistence and retrieval from TimescaleDB")
        sys.exit(0)

    success = asyncio.run(main())
    sys.exit(0 if success else 1)
