#!/usr/bin/env python3
"""
Complete OHLCV Pipeline Control

A clean, straightforward example showing how to start/stop the OHLCV collection daemon.
Can run with default environment or create isolated test database.

Usage:
    python run_example_pipeline.py        # Use default environment
    python run_example_pipeline.py test_db   # Create test database
"""

import asyncio
import os
import signal
import sys
import time
from pathlib import Path

from fullon_ohlcv_service.daemon import OhlcvServiceDaemon
from fullon_orm import DatabaseContext
from fullon_ohlcv.repositories.ohlcv import CandleRepository
from fullon_cache import ProcessCache
from demo_data import (
    generate_test_db_name,
    create_test_database,
    drop_test_database,
    install_demo_data
)

# Global daemon instance
daemon = None


def load_env():
    """Load environment variables from .env if DB_NAME not set"""
    if not os.getenv('DB_NAME'):
        try:
            from dotenv import load_dotenv
            env_path = Path(__file__).parent.parent / '.env'
            load_dotenv(env_path)
            print(f"📄 Loaded environment from {env_path}")
        except ImportError:
            print("⚠️  python-dotenv not available, using existing environment")
        except Exception as e:
            print(f"⚠️  Could not load .env file: {e}")


async def show_system_status():
    """Display collection status and process health"""
    global daemon

    print("\n" + "="*60)
    print("🔍 OHLCV SYSTEM STATUS REPORT")
    print("="*60)

    # Show daemon status
    if daemon:
        status = await daemon.status()
        print(f"🚀 Daemon Status: {'🟢 Running' if status.get('daemon_running') else '🔴 Stopped'}")

        # Show OHLCV service status
        ohlcv_status = status.get('ohlcv_service', {})
        if ohlcv_status:
            running = ohlcv_status.get('running', False)
            collectors = ohlcv_status.get('collectors', [])
            status_icon = "🟢" if running else "🔴"
            print(f"  {status_icon} OHLCV Service: {'running' if running else 'stopped'} ({len(collectors)} collectors)")

        # Show Trade service status
        trade_status = status.get('trade_service', {})
        if trade_status:
            collectors = list(trade_status.keys()) if isinstance(trade_status, dict) else []
            status_icon = "🟢" if collectors else "🔴"
            print(f"  {status_icon} Trade Service: {len(collectors)} collectors")

    # Show registered processes
    try:
        async with ProcessCache() as cache:
            processes = await cache.get_active_processes()
            if processes:
                print(f"⚙️  Registered Processes ({len(processes)}):")
                for process_info in processes[:3]:  # Show first 3
                    component = process_info.get('component', 'unknown')
                    message = process_info.get('message', 'running')
                    print(f"  🔄 {component}: {message}")
            else:
                print("⚙️  No registered processes found")
    except Exception as e:
        print(f"⚠️  Could not fetch process status: {e}")

    print("="*60 + "\n")


async def show_data_samples():
    """Show some sample OHLCV data from database"""
    print("📊 Recent OHLCV Data Samples:")

    try:
        # Get a few symbols to show data for
        async with DatabaseContext() as db:
            symbols = await db.symbols.get_all(limit=3)

            for symbol in symbols:
                try:
                    async with CandleRepository(symbol.exchange_name, symbol.symbol, test=False) as repo:
                        latest_timestamp = await repo.get_latest_timestamp()
                        if latest_timestamp:
                            age = time.time() - latest_timestamp.timestamp
                            print(f"  💰 {symbol.symbol} ({symbol.exchange_name}): "
                                  f"Latest data from {latest_timestamp.format('YYYY-MM-DD HH:mm:ss')} "
                                  f"({age/60:.1f}m ago)")
                        else:
                            print(f"  ⏳ {symbol.symbol} ({symbol.exchange_name}): No data yet")
                except Exception as e:
                    print(f"  ❌ {symbol.symbol} ({symbol.exchange_name}): Error - {e}")
    except Exception as e:
        print(f"  ❌ Database connection failed: {e}")
        print("  💡 Try running: python run_example_pipeline.py test_db")


async def start(use_test_db=False):
    """Start the OHLCV collection pipeline"""
    global daemon
    test_db_name = None

    try:
        if use_test_db:
            # Create test database and install demo data
            test_db_name = generate_test_db_name()
            print(f"🔧 Creating test database: {test_db_name}")
            await create_test_database(test_db_name)

            # Override DB_NAME environment variable
            os.environ['DB_NAME'] = test_db_name
            print(f"📄 Using test database: {test_db_name}")

            # Install demo data
            print("📊 Installing demo data...")
            await install_demo_data()
            print("✅ Demo data installed")
        else:
            # Load environment if needed (normal mode)
            load_env()

        print("🚀 Starting OHLCV collection pipeline...")

        # Create and configure daemon
        daemon = OhlcvServiceDaemon()

        # Show what symbols we'll be monitoring
        try:
            async with DatabaseContext() as db:
                admin_email = os.getenv("ADMIN_MAIL", "admin@fullon")
                admin_uid = await db.users.get_user_id(admin_email)

                if not admin_uid:
                    print(f"❌ Admin user not found: {admin_email}")
                    if not use_test_db:
                        print("💡 Try running: python run_example_pipeline.py test_db")
                    return

                exchanges = await db.exchanges.get_user_exchanges(admin_uid)
                print(f"📊 Found {len(exchanges)} exchange(s) for admin user")

                for exchange in exchanges:
                    ex_name = exchange.get('ex_named', 'unknown')
                    # Get symbols for this exchange
                    symbols = await db.symbols.get_by_exchange_id(exchange['cat_ex_id'])
                    print(f"  • {ex_name}: {len(symbols)} symbols")

            # Start the daemon (this will start both OHLCV and Trade managers with capability detection)
            print("⚙️ Starting OHLCV service daemon...")
            success = await daemon.start()
            if not success:
                print("❌ Failed to start daemon")
                return

        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            if not use_test_db:
                print("💡 Try running: python run_example_pipeline.py test_db")
            return

        print("✅ OHLCV collection pipeline started")
        print("🔄 Starting monitoring loop (Ctrl+C to stop)...")

        # Set up shutdown event for clean exit
        shutdown_event = asyncio.Event()

        def signal_handler(signum, frame):
            print(f"\n🛑 Received signal {signum}, stopping...")
            shutdown_event.set()

        # Register signal handlers
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Monitoring loop with status reports
        loop_count = 0
        while not shutdown_event.is_set():
            loop_count += 1

            # Show data samples
            await show_data_samples()

            # Every 10 seconds, show system status
            if loop_count % 2 == 0:  # Every 2 iterations (10 seconds)
                await show_system_status()

            # Wait with timeout so we can check shutdown_event
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=5.0)
                break  # shutdown_event was set
            except asyncio.TimeoutError:
                continue  # Normal timeout, continue loop

    finally:
        # Cleanup daemon
        if daemon:
            print("🛑 Stopping OHLCV collection pipeline...")
            await daemon.stop()
            print("✅ Pipeline stopped")

        # Clean up test database if we created one
        if test_db_name:
            print(f"🗑️ Cleaning up test database: {test_db_name}")
            await drop_test_database(test_db_name)
            print("✅ Test database cleaned up")


async def stop():
    """Stop the OHLCV collection pipeline"""
    global daemon

    if daemon:
        print("🛑 Stopping OHLCV collection pipeline...")
        await daemon.stop()
        print("✅ Pipeline stopped")
    else:
        print("⚠️  Pipeline is not running")


def main():
    """Main entry point with CLI argument handling"""
    use_test_db = len(sys.argv) > 1 and sys.argv[1] == "test_db"
    asyncio.run(start(use_test_db=use_test_db))


if __name__ == "__main__":
    main()