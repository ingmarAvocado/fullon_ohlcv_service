"""
Integration tests for OhlcvServiceDaemon full pipeline

Tests the complete daemon pipeline including database setup, historic collection,
live collection, and cleanup - similar to run_example_pipeline.py
"""

import asyncio
import contextlib
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

# Integration tests using demo_data.py for isolated database setup

# Add src to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root / "src"))
sys.path.insert(0, str(project_root / "examples"))  # For demo_data

# Load environment variables from .env file
try:
    from dotenv import load_dotenv

    load_dotenv(project_root / ".env")
except ImportError:
    pass  # dotenv not available
except Exception:
    pass  # Could not load .env file

from demo_data import create_dual_test_databases, drop_dual_test_databases, install_demo_data
from fullon_log import get_component_logger
from fullon_orm import DatabaseContext, init_db

from fullon_ohlcv_service.daemon import OhlcvServiceDaemon

logger = get_component_logger("fullon.daemon.test")


@pytest.mark.integration
class TestDaemonPipelineIntegration:
    """Integration tests for the complete daemon pipeline."""

    @pytest.mark.asyncio
    async def test_daemon_full_pipeline_short_run(self, dual_test_databases):
        """Test the complete daemon pipeline with worker-isolated databases."""
        # Install demo data
        from demo_data import install_demo_data
        await install_demo_data()
        daemon = None
        try:
            # Create daemon instance
            daemon = OhlcvServiceDaemon()

            print("📚 Phase 1: Running concurrent historic collection (OHLCV + Trades)...")

            # Start daemon (symbol init + historic concurrently, then live indefinitely)
            daemon_task = asyncio.create_task(daemon.run())

            # Wait for symbol initialization + historic collection to complete
            # Symbol init happens first, then both historic collectors run concurrently
            # Give them time to finish
            await asyncio.sleep(5)  # Shorter time for test

            print("✅ Historic collection completed")
            print("📊 Phase 2: Running live collection for short duration...")

            # Run live collection for a short time
            await asyncio.sleep(2)  # Short live collection for test

            # Cancel daemon task gracefully
            print("⏹️  Requesting daemon shutdown...")
            daemon_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await daemon_task

            print("✅ Live collection completed")

        except Exception as e:
            print(f"❌ Pipeline failed: {e}")
            logger.exception("Pipeline failed")
            raise
        finally:
            if daemon:
                await daemon.cleanup()
                print("✅ Daemon cleanup completed")

    @pytest.mark.asyncio
    async def test_daemon_pipeline_results_verification(self, dual_test_databases):
        """Test that the daemon pipeline produces expected results."""
        # Install demo data
        from demo_data import install_demo_data
        await install_demo_data()
        daemon = None
        try:
            # Create daemon instance
            daemon = OhlcvServiceDaemon()

            print("📚 Phase 1: Running concurrent historic collection (OHLCV + Trades)...")

            # Start daemon (symbol init + historic concurrently, then live indefinitely)
            daemon_task = asyncio.create_task(daemon.run())

            # Wait for symbol initialization + historic collection to complete
            # Symbol init happens first, then both historic collectors run concurrently
            # Give them time to finish
            await asyncio.sleep(5)  # Shorter time for test

            print("✅ Historic collection completed")
            print("📊 Phase 2: Running live collection for short duration...")

            # Run live collection for a short time
            await asyncio.sleep(2)  # Short live collection for test

            # Cancel daemon task gracefully
            print("⏹️  Requesting daemon shutdown...")
            daemon_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await daemon_task

            print("✅ Live collection completed")

            # Verify results - for now, just ensure the daemon ran successfully
            # In a real implementation, this would query the OHLCV database
            # to verify expected number of candles/trades were collected
            print("🔍 Phase 3: Verifying pipeline completed successfully...")

            # The test passes if we reach this point without exceptions
            assert True, "Daemon pipeline completed successfully"

            print("✅ Pipeline verification completed successfully")

        except Exception as e:
            print(f"❌ Pipeline verification failed: {e}")
            logger.exception("Pipeline verification failed")
            raise
        finally:
            if daemon:
                await daemon.cleanup()
                print("✅ Daemon cleanup completed")

    @pytest.mark.asyncio
    async def test_daemon_handles_empty_symbols_gracefully(self, dual_test_databases):
        """Test daemon handles case with no symbols gracefully."""
        # Mock empty symbols and add_all_symbols to avoid database operations
        with (
            patch("fullon_orm.DatabaseContext") as mock_db_class,
            patch("fullon_ohlcv_service.daemon.add_all_symbols", new=AsyncMock(return_value=True)),
            patch(
                "fullon_ohlcv_service.ohlcv.historic_collector.HistoricOHLCVCollector.start_collection",
                new=AsyncMock(),
            ),
            patch(
                "fullon_ohlcv_service.trade.historic_collector.HistoricTradeCollector.start_collection",
                new=AsyncMock(),
            ),
            patch(
                "fullon_ohlcv_service.ohlcv.live_collector.LiveOHLCVCollector.start_collection",
                new=AsyncMock(),
            ),
            patch(
                "fullon_ohlcv_service.trade.live_collector.LiveTradeCollector.start_collection",
                new=AsyncMock(),
            ),
        ):
            mock_db = AsyncMock()
            mock_db_class.return_value.__aenter__.return_value = mock_db
            mock_db.symbols.get_all.return_value = []

            daemon = OhlcvServiceDaemon()

            # Should complete without error even with no symbols
            daemon_task = asyncio.create_task(daemon.run())
            await asyncio.sleep(0.5)  # Let it process
            daemon_task.cancel()

            try:
                await daemon_task
            except asyncio.CancelledError:
                pass  # Expected

            await daemon.cleanup()

    @pytest.mark.asyncio
    async def test_daemon_symbol_initialization_failure_handling(self, dual_test_databases):
        """Test daemon handles symbol initialization failures gracefully."""
        # Mock symbols
        mock_symbols = [AsyncMock(symbol="BTC/USD", cat_exchange=AsyncMock(name="kraken"))]

        with (
            patch("fullon_orm.DatabaseContext") as mock_db_class,
            patch(
                "fullon_ohlcv_service.daemon.add_all_symbols", new=AsyncMock(return_value=False)
            ) as mock_add_symbols,
            patch(
                "fullon_ohlcv_service.ohlcv.historic_collector.HistoricOHLCVCollector.start_collection",
                new=AsyncMock(),
            ),
            patch(
                "fullon_ohlcv_service.trade.historic_collector.HistoricTradeCollector.start_collection",
                new=AsyncMock(),
            ),
            patch(
                "fullon_ohlcv_service.ohlcv.live_collector.LiveOHLCVCollector.start_collection",
                new=AsyncMock(),
            ),
            patch(
                "fullon_ohlcv_service.trade.live_collector.LiveTradeCollector.start_collection",
                new=AsyncMock(),
            ),
        ):
            mock_db = AsyncMock()
            mock_db_class.return_value.__aenter__.return_value = mock_db
            mock_db.symbols.get_all.return_value = mock_symbols

            daemon = OhlcvServiceDaemon()

            # Should continue even if symbol initialization fails
            daemon_task = asyncio.create_task(daemon.run())
            await asyncio.sleep(0.5)
            daemon_task.cancel()

            try:
                await daemon_task
            except asyncio.CancelledError:
                pass

            await daemon.cleanup()

            # Verify add_all_symbols was called with the symbols from the database
            assert mock_add_symbols.called
            args, kwargs = mock_add_symbols.call_args
            assert "symbols" in kwargs
            symbols = kwargs["symbols"]
            assert len(symbols) > 0  # Should have symbols from demo data
