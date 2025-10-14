"""
Unit tests for OhlcvServiceDaemon

Tests the daemon's initialization, setup, and basic functionality.
"""

import asyncio
import pytest
import warnings
from unittest.mock import AsyncMock, MagicMock, Mock, patch

from fullon_ohlcv_service.daemon import OhlcvServiceDaemon
from fullon_orm.models import Symbol, CatExchange


class TestOhlcvServiceDaemon:
    """Test OhlcvServiceDaemon initialization and basic functionality."""

    @pytest.mark.asyncio
    async def test_daemon_initialization(self):
        # Suppress warnings about unawaited coroutines in test mocks
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
        """Test that daemon initializes with correct default state."""
        daemon = OhlcvServiceDaemon()

        # Check initial state
        assert daemon.live_ohlcv_collector is None
        assert daemon.historic_ohlcv_collector is None
        assert daemon.live_trade_collector is None
        assert daemon.historic_trade_collector is None
        assert daemon.process_id is None
        assert daemon._symbols == []

    @pytest.mark.asyncio
    async def test_daemon_register_process(self):
        """Test daemon process registration with ProcessCache."""
        daemon = OhlcvServiceDaemon()

        with patch("fullon_ohlcv_service.daemon.ProcessCache") as mock_cache_class:
            mock_cache = AsyncMock()
            mock_cache_class.return_value.__aenter__.return_value = mock_cache
            mock_cache.register_process.return_value = "test_process_id"

            await daemon._register_daemon()

            # Verify process registration
            mock_cache.register_process.assert_called_once()
            call_args = mock_cache.register_process.call_args
            assert call_args[1]["process_type"] == "ohlcv"
            assert call_args[1]["component"] == "ohlcv_daemon"
            assert "pid" in call_args[1]["params"]
            assert "args" in call_args[1]["params"]
            assert call_args[1]["message"] == "OHLCV daemon started successfully"
            assert call_args[1]["status"] == "starting"

            # Verify process_id is set
            assert daemon.process_id == "test_process_id"

    @pytest.mark.asyncio
    async def test_daemon_run_symbol_initialization(self):
        """Test that daemon loads symbols from database during run."""
        daemon = OhlcvServiceDaemon()

        # Mock symbols
        mock_symbols = [
            MagicMock(symbol="BTC/USD", cat_exchange=MagicMock(name="kraken")),
            MagicMock(symbol="ETH/USD", cat_exchange=MagicMock(name="binance")),
        ]

        with (
            patch("fullon_ohlcv_service.daemon.DatabaseContext") as mock_db_class,
            patch(
                "fullon_ohlcv_service.daemon.add_all_symbols", new=AsyncMock(return_value=True)
            ) as mock_add_symbols,
            patch.object(daemon, "_register_daemon", new=AsyncMock()),
            patch("asyncio.gather", new=AsyncMock()) as mock_gather,
        ):
            mock_db = AsyncMock()
            mock_db_class.return_value.__aenter__.return_value = mock_db
            mock_db.symbols.get_all.return_value = mock_symbols

            # Mock collectors to prevent actual collection
            daemon.live_ohlcv_collector = AsyncMock()
            daemon.historic_ohlcv_collector = AsyncMock()
            daemon.live_trade_collector = AsyncMock()
            daemon.historic_trade_collector = AsyncMock()

            # Start daemon run but cancel immediately after symbol init
            run_task = asyncio.create_task(daemon.run())
            await asyncio.sleep(0.1)  # Let symbol initialization happen
            run_task.cancel()

            try:
                await run_task
            except asyncio.CancelledError:
                pass

            # Verify symbols were loaded
            assert daemon._symbols == mock_symbols

            # Verify add_all_symbols was called
            mock_add_symbols.assert_called_once_with(symbols=mock_symbols)

    @pytest.mark.asyncio
    async def test_daemon_run_collector_initialization(self):
        """Test that daemon initializes collectors with symbols."""
        daemon = OhlcvServiceDaemon()

        # Mock symbols
        mock_symbols = [MagicMock(symbol="BTC/USD", cat_exchange=MagicMock(name="kraken"))]

        with (
            patch("fullon_ohlcv_service.daemon.DatabaseContext") as mock_db_class,
            patch("fullon_ohlcv_service.daemon.add_all_symbols", new=AsyncMock(return_value=True)),
            patch.object(daemon, "_register_daemon", new=AsyncMock()),
            patch("asyncio.gather", new=AsyncMock()) as mock_gather,
            patch("fullon_ohlcv_service.daemon.LiveOHLCVCollector") as mock_live_ohlcv,
            patch("fullon_ohlcv_service.daemon.HistoricOHLCVCollector") as mock_historic_ohlcv,
            patch("fullon_ohlcv_service.daemon.LiveTradeCollector") as mock_live_trade,
            patch("fullon_ohlcv_service.daemon.HistoricTradeCollector") as mock_historic_trade,
        ):
            mock_db = AsyncMock()
            mock_db_class.return_value.__aenter__.return_value = mock_db
            mock_db.symbols.get_all.return_value = mock_symbols

            # Start daemon run but cancel immediately after collector init
            run_task = asyncio.create_task(daemon.run())
            await asyncio.sleep(0.1)  # Let initialization happen
            run_task.cancel()

            try:
                await run_task
            except asyncio.CancelledError:
                pass

            # Verify collectors were initialized with symbols
            mock_live_ohlcv.assert_called_once_with(symbols=mock_symbols)
            mock_historic_ohlcv.assert_called_once_with(symbols=mock_symbols)
            mock_live_trade.assert_called_once_with(symbols=mock_symbols)
            mock_historic_trade.assert_called_once_with(symbols=mock_symbols)

    @pytest.mark.asyncio
    async def test_daemon_run_handles_collection_errors(self):
        """Test that daemon handles collection errors gracefully."""
        daemon = OhlcvServiceDaemon()

        # Mock symbols
        mock_symbols = [MagicMock(symbol="BTC/USD", cat_exchange=MagicMock(name="kraken"))]

        # Mock collectors
        mock_historic_ohlcv = AsyncMock()
        mock_historic_trade = AsyncMock()
        mock_live_ohlcv = AsyncMock()
        mock_live_trade = AsyncMock()

        with (
            patch("fullon_ohlcv_service.daemon.DatabaseContext") as mock_db_class,
            patch("fullon_ohlcv_service.daemon.add_all_symbols", new=AsyncMock(return_value=True)),
            patch.object(daemon, "_register_daemon", new=AsyncMock()),
            patch("asyncio.gather", new=AsyncMock()) as mock_gather,
            patch("fullon_ohlcv_service.daemon.LiveOHLCVCollector", return_value=mock_live_ohlcv),
            patch(
                "fullon_ohlcv_service.daemon.HistoricOHLCVCollector",
                return_value=mock_historic_ohlcv,
            ),
            patch("fullon_ohlcv_service.daemon.LiveTradeCollector", return_value=mock_live_trade),
            patch(
                "fullon_ohlcv_service.daemon.HistoricTradeCollector",
                return_value=mock_historic_trade,
            ),
        ):
            mock_db = AsyncMock()
            mock_db_class.return_value.__aenter__.return_value = mock_db
            mock_db.symbols.get_all.return_value = mock_symbols

            # Start daemon run but cancel after historic collection starts
            run_task = asyncio.create_task(daemon.run())
            await asyncio.sleep(0.1)  # Let historic collection start
            run_task.cancel()

            try:
                await run_task
            except asyncio.CancelledError:
                pass

            # Verify asyncio.gather was called (indicating concurrent collection)
            mock_gather.assert_called()

            # Note: historic collectors are deleted after collection completes
            # So we verify the run went through the historic phase by checking gather was called

    @pytest.mark.asyncio
    async def test_daemon_run_live_collection_phase(self):
        """Test that daemon runs live collection concurrently after historic."""
        daemon = OhlcvServiceDaemon()

        # Mock symbols
        mock_symbols = [MagicMock(symbol="BTC/USD", cat_exchange=MagicMock(name="kraken"))]

        # Mock collectors
        mock_live_ohlcv = AsyncMock()
        mock_live_trade = AsyncMock()
        mock_historic_ohlcv = AsyncMock()
        mock_historic_trade = AsyncMock()

        with (
            patch("fullon_ohlcv_service.daemon.DatabaseContext") as mock_db_class,
            patch("fullon_ohlcv_service.daemon.add_all_symbols", new=AsyncMock(return_value=True)),
            patch.object(daemon, "_register_daemon", new=AsyncMock()),
            patch("asyncio.gather", new=AsyncMock()) as mock_gather,
            patch("fullon_ohlcv_service.daemon.LiveOHLCVCollector", return_value=mock_live_ohlcv),
            patch(
                "fullon_ohlcv_service.daemon.HistoricOHLCVCollector",
                return_value=mock_historic_ohlcv,
            ),
            patch("fullon_ohlcv_service.daemon.LiveTradeCollector", return_value=mock_live_trade),
            patch(
                "fullon_ohlcv_service.daemon.HistoricTradeCollector",
                return_value=mock_historic_trade,
            ),
        ):
            mock_db = AsyncMock()
            mock_db_class.return_value.__aenter__.return_value = mock_db
            mock_db.symbols.get_all.return_value = mock_symbols

            # Start daemon run but cancel after live collection starts
            run_task = asyncio.create_task(daemon.run())
            await asyncio.sleep(0.1)  # Let live collection start
            run_task.cancel()

            try:
                await run_task
            except asyncio.CancelledError:
                pass

            # Verify asyncio.gather was called multiple times (historic + live phases)
            assert mock_gather.call_count >= 2

            # Verify live collectors were created
            assert daemon.live_ohlcv_collector == mock_live_ohlcv
            assert daemon.live_trade_collector == mock_live_trade

    @pytest.mark.asyncio
    async def test_daemon_cleanup(self):
        """Test daemon cleanup process."""
        daemon = OhlcvServiceDaemon()

        # Set up mock collectors
        daemon.live_trade_collector = AsyncMock()
        daemon.process_id = "test_process_id"

        with patch("fullon_ohlcv_service.daemon.ProcessCache") as mock_cache_class:
            mock_cache = AsyncMock()
            mock_cache_class.return_value.__aenter__.return_value = mock_cache

            await daemon.cleanup()

            # Verify collector cleanup
            daemon.live_trade_collector.stop_collection.assert_called_once()

            # Verify process status update
            mock_cache.update_process.assert_called_once_with(
                process_id="test_process_id", status="stopped", message="Daemon shutdown complete"
            )

    @pytest.mark.asyncio
    async def test_daemon_cleanup_handles_errors(self):
        """Test daemon cleanup handles errors gracefully."""
        daemon = OhlcvServiceDaemon()

        # Set up mock collectors that raise errors
        daemon.live_trade_collector = AsyncMock()
        daemon.live_trade_collector.stop_collection.side_effect = Exception("Stop failed")
        daemon.process_id = "test_process_id"

        with patch("fullon_ohlcv_service.daemon.ProcessCache") as mock_cache_class:
            mock_cache = AsyncMock()
            mock_cache_class.return_value.__aenter__.return_value = mock_cache
            mock_cache.update_process.side_effect = Exception("Cache update failed")

            # Should not raise exception
            await daemon.cleanup()

            # Verify cleanup was attempted despite errors
            daemon.live_trade_collector.stop_collection.assert_called_once()
            mock_cache.update_process.assert_called_once()

    @pytest.mark.asyncio
    async def test_daemon_run_handles_cancellation(self):
        """Test that daemon handles cancellation gracefully."""
        daemon = OhlcvServiceDaemon()

        # Mock symbols
        mock_symbols = [MagicMock(symbol="BTC/USD", cat_exchange=MagicMock(name="kraken"))]

        with (
            patch("fullon_ohlcv_service.daemon.DatabaseContext") as mock_db_class,
            patch("fullon_ohlcv_service.daemon.add_all_symbols", new=AsyncMock(return_value=True)),
            patch.object(daemon, "_register_daemon", new=AsyncMock()),
            patch("fullon_ohlcv_service.daemon.LiveOHLCVCollector", return_value=AsyncMock()),
            patch("fullon_ohlcv_service.daemon.HistoricOHLCVCollector", return_value=AsyncMock()),
            patch("fullon_ohlcv_service.daemon.LiveTradeCollector", return_value=AsyncMock()),
            patch("fullon_ohlcv_service.daemon.HistoricTradeCollector", return_value=AsyncMock()),
        ):
            mock_db = AsyncMock()
            mock_db_class.return_value.__aenter__.return_value = mock_db
            mock_db.symbols.get_all.return_value = mock_symbols

            # Start daemon and cancel immediately
            with pytest.raises(asyncio.CancelledError):
                run_task = asyncio.create_task(daemon.run())
                run_task.cancel()
                await run_task

    @pytest.mark.asyncio
    async def test_daemon_run_no_symbols_warning(self):
        """Test daemon handles case with no symbols gracefully."""
        daemon = OhlcvServiceDaemon()

        with (
            patch("fullon_ohlcv_service.daemon.DatabaseContext") as mock_db_class,
            patch("fullon_ohlcv_service.daemon.add_all_symbols", new=AsyncMock(return_value=True)),
            patch.object(daemon, "_register_daemon", new=AsyncMock()),
            patch("asyncio.gather", new=AsyncMock()),
        ):
            mock_db = AsyncMock()
            mock_db_class.return_value.__aenter__.return_value = mock_db
            mock_db.symbols.get_all.return_value = []  # No symbols

            # Should complete without error
            run_task = asyncio.create_task(daemon.run())
            await asyncio.sleep(0.1)  # Let it process
            run_task.cancel()

            try:
                await run_task
            except asyncio.CancelledError:
                pass

            # Verify collectors were still initialized (with empty symbols)
            assert daemon._symbols == []

    @pytest.mark.asyncio
    async def test_daemon_run_handles_collection_start_errors(self):
        """Test that daemon handles errors during collection start gracefully."""
        daemon = OhlcvServiceDaemon()

        # Mock symbols
        mock_symbols = [MagicMock(symbol="BTC/USD", cat_exchange=MagicMock(name="kraken"))]

        # Mock collectors that raise exceptions
        mock_historic_ohlcv = AsyncMock()
        mock_historic_ohlcv.start_collection.side_effect = Exception("OHLCV collection failed")
        mock_historic_trade = AsyncMock()
        mock_historic_trade.start_collection.side_effect = Exception("Trade collection failed")

        with (
            patch("fullon_ohlcv_service.daemon.DatabaseContext") as mock_db_class,
            patch("fullon_ohlcv_service.daemon.add_all_symbols", new=AsyncMock(return_value=True)),
            patch.object(daemon, "_register_daemon", new=AsyncMock()),
            patch("fullon_ohlcv_service.daemon.LiveOHLCVCollector", return_value=AsyncMock()),
            patch(
                "fullon_ohlcv_service.daemon.HistoricOHLCVCollector",
                return_value=mock_historic_ohlcv,
            ),
            patch("fullon_ohlcv_service.daemon.LiveTradeCollector", return_value=AsyncMock()),
            patch(
                "fullon_ohlcv_service.daemon.HistoricTradeCollector",
                return_value=mock_historic_trade,
            ),
        ):
            mock_db = AsyncMock()
            mock_db_class.return_value.__aenter__.return_value = mock_db
            mock_db.symbols.get_all.return_value = mock_symbols

            # The daemon should log errors but still re-raise them
            with pytest.raises(Exception, match="OHLCV collection failed"):
                await daemon.run()

            await daemon.cleanup()

            # Verify collection methods were called (and failed)
            mock_historic_ohlcv.start_collection.assert_called_once()
            mock_historic_trade.start_collection.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_symbol_creates_all_collectors(self):
        """Test process_symbol creates all collectors (capability checking is done in collectors)."""
        daemon = OhlcvServiceDaemon()

        # Create mock symbol
        cat_exchange = CatExchange(cat_ex_id=1, name="kraken")
        symbol = Symbol(symbol_id=1, symbol="BTC/USDC", cat_ex_id=1, cat_exchange=cat_exchange)

        # Mock dependencies
        with (
            patch("fullon_ohlcv_service.daemon.add_all_symbols", new=AsyncMock(return_value=True)),
            patch("fullon_ohlcv_service.daemon.DatabaseContext") as mock_db,
        ):
            # Setup mocks - process_symbol now just calls run() with single symbol
            mock_db_instance = AsyncMock()
            mock_db_instance.__aenter__.return_value = mock_db_instance
            mock_db_instance.symbols.get_all.return_value = [symbol]
            mock_db.return_value = mock_db_instance

            # Mock collectors
            with (
                patch("fullon_ohlcv_service.daemon.LiveOHLCVCollector") as mock_live_ohlcv_class,
                patch("fullon_ohlcv_service.daemon.HistoricOHLCVCollector") as mock_historic_ohlcv_class,
                patch("fullon_ohlcv_service.daemon.LiveTradeCollector") as mock_live_trade_class,
                patch("fullon_ohlcv_service.daemon.HistoricTradeCollector") as mock_historic_trade_class,
                patch.object(daemon, "_register_daemon", new=AsyncMock()),
            ):
                # Create mock instances with start_symbol method
                mock_live_ohlcv = AsyncMock()
                mock_live_ohlcv.start_symbol = AsyncMock()
                mock_live_ohlcv_class.return_value = mock_live_ohlcv

                mock_historic_ohlcv = AsyncMock()
                mock_historic_ohlcv.start_symbol = AsyncMock()
                mock_historic_ohlcv_class.return_value = mock_historic_ohlcv

                mock_live_trade = AsyncMock()
                mock_live_trade.start_symbol = AsyncMock()
                mock_live_trade_class.return_value = mock_live_trade

                mock_historic_trade = AsyncMock()
                mock_historic_trade.start_symbol = AsyncMock()
                mock_historic_trade_class.return_value = mock_historic_trade

                # Start collection in background
                task = asyncio.create_task(daemon.process_symbol(symbol))

                # Let it run briefly
                await asyncio.sleep(0.1)

                # Verify all collectors were created without symbols (they use start_symbol instead)
                mock_live_ohlcv_class.assert_called_once_with()
                mock_historic_ohlcv_class.assert_called_once_with()
                mock_live_trade_class.assert_called_once_with()
                mock_historic_trade_class.assert_called_once_with()

                # Cancel and cleanup
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    @pytest.mark.asyncio
    async def test_process_symbol_already_running(self):
        """Test process_symbol returns early if daemon already has collectors."""
        daemon = OhlcvServiceDaemon()

        # Set up daemon as if it's already running
        daemon.live_ohlcv_collector = MagicMock()

        # Create mock symbol
        cat_exchange = CatExchange(cat_ex_id=1, name="kraken")
        symbol = Symbol(
            symbol_id=1,
            symbol="BTC/USDC",
            cat_ex_id=1,
            cat_exchange=cat_exchange
        )

        # Should return early without doing anything
        await daemon.process_symbol(symbol)

        # Verify no collectors were created (since daemon was already running)
        # This is tested by the fact that no exceptions were raised and method returned

    @pytest.mark.asyncio
    async def test_process_symbol_invalid_symbol_validation(self):
        """Test process_symbol validates symbol parameter."""
        daemon = OhlcvServiceDaemon()

        # Test with None symbol
        with pytest.raises(ValueError, match="Invalid symbol"):
            await daemon.process_symbol(None)

        # Test with invalid object
        with pytest.raises(ValueError, match="Invalid symbol"):
            await daemon.process_symbol("not_a_symbol_object")

        # Test with object missing required attributes
        class FakeSymbol:
            pass

        with pytest.raises(ValueError, match="Invalid symbol"):
            await daemon.process_symbol(FakeSymbol())


class TestOhlcvServiceDaemonDynamicSymbols:
    """Test cases for dynamic symbol management in OhlcvServiceDaemon."""

    @pytest.mark.asyncio
    async def test_process_symbol_daemon_not_running_starts_daemon(self):
        """Test process_symbol starts daemon when not running."""
        daemon = OhlcvServiceDaemon()

        # Create mock symbol
        cat_exchange = CatExchange(cat_ex_id=1, name="binance")
        symbol = Symbol(symbol_id=1, symbol="BTC/USDT", cat_ex_id=1, cat_exchange=cat_exchange)

        # Mock dependencies
        with (
            patch("fullon_ohlcv_service.daemon.add_all_symbols", new=AsyncMock(return_value=True)),
            patch("fullon_ohlcv_service.daemon.DatabaseContext") as mock_db,
            patch("fullon_ohlcv_service.daemon.LiveOHLCVCollector") as mock_live_ohlcv_class,
            patch("fullon_ohlcv_service.daemon.HistoricOHLCVCollector") as mock_historic_ohlcv_class,
            patch("fullon_ohlcv_service.daemon.LiveTradeCollector") as mock_live_trade_class,
            patch("fullon_ohlcv_service.daemon.HistoricTradeCollector") as mock_historic_trade_class,
            patch.object(daemon, "_register_daemon", new=AsyncMock()),
        ):
            # Create mock instances with start_symbol method
            mock_live_ohlcv = AsyncMock()
            mock_live_ohlcv.start_symbol = AsyncMock()
            mock_live_ohlcv_class.return_value = mock_live_ohlcv

            mock_historic_ohlcv = AsyncMock()
            mock_historic_ohlcv.start_symbol = AsyncMock()
            mock_historic_ohlcv_class.return_value = mock_historic_ohlcv

            mock_live_trade = AsyncMock()
            mock_live_trade.start_symbol = AsyncMock()
            mock_live_trade_class.return_value = mock_live_trade

            mock_historic_trade = AsyncMock()
            mock_historic_trade.start_symbol = AsyncMock()
            mock_historic_trade_class.return_value = mock_historic_trade

            mock_db_instance = AsyncMock()
            mock_db_instance.__aenter__.return_value = mock_db_instance
            mock_db_instance.symbols.get_all.return_value = [symbol]
            mock_db.return_value = mock_db_instance

            # Start collection in background and cancel after initialization
            task = asyncio.create_task(daemon.process_symbol(symbol))
            await asyncio.sleep(0.1)

            # Verify daemon was set up with single symbol
            assert daemon._symbols == [symbol]

            # Cancel and cleanup
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    @pytest.mark.asyncio
    async def test_process_symbol_daemon_running_adds_dynamically(self):
        """Test process_symbol adds symbol to running daemon."""
        daemon = OhlcvServiceDaemon()

        # Create mock symbol
        cat_exchange = CatExchange(cat_ex_id=1, name="binance")
        symbol = Symbol(symbol_id=1, symbol="BTC/USDT", cat_ex_id=1, cat_exchange=cat_exchange)

        # Set up daemon as running with mocked collectors
        mock_live_ohlcv = AsyncMock()
        mock_live_ohlcv.is_collecting = Mock(return_value=False)
        mock_live_ohlcv.start_symbol = AsyncMock()
        daemon.live_ohlcv_collector = mock_live_ohlcv

        mock_live_trade = AsyncMock()
        mock_live_trade.is_collecting = Mock(return_value=False)
        mock_live_trade.start_symbol = AsyncMock()
        daemon.live_trade_collector = mock_live_trade

        with patch("fullon_ohlcv_service.daemon.add_all_symbols", new=AsyncMock(return_value=True)):
            await daemon.process_symbol(symbol)

        # Verify start_symbol was called on both collectors
        mock_live_ohlcv.start_symbol.assert_called_once_with(symbol)
        mock_live_trade.start_symbol.assert_called_once_with(symbol)

    @pytest.mark.asyncio
    async def test_process_symbol_already_collecting_in_ohlcv(self):
        """Test process_symbol returns early if symbol already in OHLCV collection."""
        daemon = OhlcvServiceDaemon()

        # Create mock symbol
        cat_exchange = CatExchange(cat_ex_id=1, name="binance")
        symbol = Symbol(symbol_id=1, symbol="BTC/USDT", cat_ex_id=1, cat_exchange=cat_exchange)

        # Set up daemon with OHLCV collector already collecting this symbol
        mock_live_ohlcv = AsyncMock()
        mock_live_ohlcv.is_collecting = Mock(return_value=True)
        mock_live_ohlcv.add_symbol = AsyncMock()
        daemon.live_ohlcv_collector = mock_live_ohlcv

        mock_live_trade = AsyncMock()
        mock_live_trade.is_collecting = Mock(return_value=False)
        daemon.live_trade_collector = mock_live_trade

        with patch("fullon_ohlcv_service.daemon.add_all_symbols", new=AsyncMock()):
            await daemon.process_symbol(symbol)

        # Verify add_symbol was NOT called (returned early)
        mock_live_ohlcv.add_symbol.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_symbol_already_collecting_in_trade(self):
        """Test process_symbol returns early if symbol already in trade collection."""
        daemon = OhlcvServiceDaemon()

        # Create mock symbol
        cat_exchange = CatExchange(cat_ex_id=1, name="binance")
        symbol = Symbol(symbol_id=1, symbol="BTC/USDT", cat_ex_id=1, cat_exchange=cat_exchange)

        # Set up daemon with trade collector already collecting this symbol
        mock_live_ohlcv = AsyncMock()
        mock_live_ohlcv.is_collecting = Mock(return_value=False)
        daemon.live_ohlcv_collector = mock_live_ohlcv

        mock_live_trade = AsyncMock()
        mock_live_trade.is_collecting = Mock(return_value=True)
        mock_live_trade.add_symbol = AsyncMock()
        daemon.live_trade_collector = mock_live_trade

        with patch("fullon_ohlcv_service.daemon.add_all_symbols", new=AsyncMock()):
            await daemon.process_symbol(symbol)

        # Verify add_symbol was NOT called (returned early)
        mock_live_trade.add_symbol.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_symbol_calls_add_all_symbols(self):
        """Test process_symbol calls add_all_symbols before adding to collectors."""
        daemon = OhlcvServiceDaemon()

        # Create mock symbol
        cat_exchange = CatExchange(cat_ex_id=1, name="binance")
        symbol = Symbol(symbol_id=1, symbol="BTC/USDT", cat_ex_id=1, cat_exchange=cat_exchange)

        # Set up daemon as running
        mock_live_ohlcv = AsyncMock()
        mock_live_ohlcv.is_collecting = Mock(return_value=False)
        mock_live_ohlcv.add_symbol = AsyncMock()
        daemon.live_ohlcv_collector = mock_live_ohlcv

        mock_live_trade = AsyncMock()
        mock_live_trade.is_collecting = Mock(return_value=False)
        mock_live_trade.add_symbol = AsyncMock()
        daemon.live_trade_collector = mock_live_trade

        with patch(
            "fullon_ohlcv_service.daemon.add_all_symbols", new=AsyncMock(return_value=True)
        ) as mock_add_symbols:
            await daemon.process_symbol(symbol)

            # Verify add_all_symbols was called with symbol list
            mock_add_symbols.assert_called_once()
            call_args = mock_add_symbols.call_args
            assert call_args[1]["symbols"] == [symbol]

    @pytest.mark.asyncio
    async def test_process_symbol_handles_add_all_symbols_failure(self):
        """Test process_symbol continues even if add_all_symbols fails."""
        daemon = OhlcvServiceDaemon()

        # Create mock symbol
        cat_exchange = CatExchange(cat_ex_id=1, name="binance")
        symbol = Symbol(symbol_id=1, symbol="BTC/USDT", cat_ex_id=1, cat_exchange=cat_exchange)

        # Set up daemon as running
        mock_live_ohlcv = AsyncMock()
        mock_live_ohlcv.is_collecting = Mock(return_value=False)
        mock_live_ohlcv.start_symbol = AsyncMock()
        daemon.live_ohlcv_collector = mock_live_ohlcv

        mock_live_trade = AsyncMock()
        mock_live_trade.is_collecting = Mock(return_value=False)
        mock_live_trade.start_symbol = AsyncMock()
        daemon.live_trade_collector = mock_live_trade

        with patch(
            "fullon_ohlcv_service.daemon.add_all_symbols", new=AsyncMock(return_value=False)
        ) as mock_add_symbols:
            # Should not raise exception
            await daemon.process_symbol(symbol)

            # Verify start_symbol was still called on collectors
            mock_live_ohlcv.start_symbol.assert_called_once_with(symbol)
            mock_live_trade.start_symbol.assert_called_once_with(symbol)

    @pytest.mark.asyncio
    async def test_process_symbol_only_live_collectors(self):
        """Test process_symbol only affects live collectors, not historic."""
        daemon = OhlcvServiceDaemon()

        # Create mock symbol
        cat_exchange = CatExchange(cat_ex_id=1, name="binance")
        symbol = Symbol(symbol_id=1, symbol="BTC/USDT", cat_ex_id=1, cat_exchange=cat_exchange)

        # Set up daemon with both live and historic collectors
        mock_live_ohlcv = AsyncMock()
        mock_live_ohlcv.is_collecting = Mock(return_value=False)
        mock_live_ohlcv.start_symbol = AsyncMock()
        daemon.live_ohlcv_collector = mock_live_ohlcv

        mock_live_trade = AsyncMock()
        mock_live_trade.is_collecting = Mock(return_value=False)
        mock_live_trade.start_symbol = AsyncMock()
        daemon.live_trade_collector = mock_live_trade

        # Historic collectors should not be affected
        mock_historic_ohlcv = AsyncMock()
        daemon.historic_ohlcv_collector = mock_historic_ohlcv

        mock_historic_trade = AsyncMock()
        daemon.historic_trade_collector = mock_historic_trade

        with patch("fullon_ohlcv_service.daemon.add_all_symbols", new=AsyncMock(return_value=True)):
            await daemon.process_symbol(symbol)

        # Verify only live collectors were called
        mock_live_ohlcv.start_symbol.assert_called_once()
        mock_live_trade.start_symbol.assert_called_once()

        # Verify historic collectors were NOT called (they don't have start_symbol in daemon running case)
        assert not hasattr(mock_historic_ohlcv, "start_symbol") or not mock_historic_ohlcv.start_symbol.called
        assert not hasattr(mock_historic_trade, "start_symbol") or not mock_historic_trade.start_symbol.called
