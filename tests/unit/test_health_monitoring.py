"""Unit tests for health monitoring functionality."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch, call

import pytest

from fullon_ohlcv_service.ohlcv.collector import OhlcvCollector
from fullon_ohlcv_service.ohlcv.manager import OhlcvManager


class TestOhlcvManagerHealthMonitoring:
    """Test health monitoring functionality in OhlcvManager."""

    def setup_method(self):
        """Set up test fixtures."""
        self.manager = OhlcvManager()

    @pytest.mark.asyncio
    @patch('fullon_ohlcv_service.ohlcv.manager.ProcessCache')
    async def test_register_process(self, mock_process_cache_class):
        """Test process registration on start."""
        # Mock ProcessCache context manager
        mock_cache = AsyncMock()
        mock_process_cache_class.return_value.__aenter__.return_value = mock_cache
        mock_process_cache_class.return_value.__aexit__.return_value = None

        # Call _register_process
        await self.manager._register_process()

        # Verify ProcessCache was used correctly
        mock_process_cache_class.assert_called_once()
        mock_cache.new_process.assert_called_once_with(
            tipe="ohlcv_service",
            key="ohlcv_daemon",
            pid=f"async:{id(self.manager)}",
            params=["ohlcv_daemon"],
            message="Started"
        )

    @pytest.mark.asyncio
    @patch('fullon_ohlcv_service.ohlcv.manager.ProcessCache')
    async def test_update_health(self, mock_process_cache_class):
        """Test health status updates."""
        # Mock ProcessCache context manager
        mock_cache = AsyncMock()
        mock_process_cache_class.return_value.__aenter__.return_value = mock_cache
        mock_process_cache_class.return_value.__aexit__.return_value = None

        # Add some collectors
        self.manager.collectors = {
            "kraken:BTC/USD": MagicMock(),
            "binance:ETH/USDT": MagicMock()
        }

        # Call _update_health
        await self.manager._update_health()

        # Verify ProcessCache was used correctly
        mock_process_cache_class.assert_called_once()
        mock_cache.update_process.assert_called_once_with(
            tipe="ohlcv_service",
            key="ohlcv_daemon",
            message="Running - 2 collectors active"
        )

    @pytest.mark.asyncio
    @patch('fullon_ohlcv_service.ohlcv.manager.ProcessCache')
    async def test_cleanup_process(self, mock_process_cache_class):
        """Test process cleanup on shutdown."""
        # Mock ProcessCache context manager
        mock_cache = AsyncMock()
        mock_process_cache_class.return_value.__aenter__.return_value = mock_cache
        mock_process_cache_class.return_value.__aexit__.return_value = None

        # Call _cleanup_process
        await self.manager._cleanup_process()

        # Verify ProcessCache was used correctly
        mock_process_cache_class.assert_called_once()
        mock_cache.delete_process.assert_called_once_with(
            "ohlcv_service", "ohlcv_daemon"
        )

    @pytest.mark.asyncio
    @patch('fullon_ohlcv_service.ohlcv.manager.ProcessCache')
    async def test_cleanup_process_with_error(self, mock_process_cache_class):
        """Test process cleanup handles errors gracefully."""
        # Mock ProcessCache context manager with error
        mock_cache = AsyncMock()
        mock_cache.delete_process.side_effect = Exception("Cache error")
        mock_process_cache_class.return_value.__aenter__.return_value = mock_cache
        mock_process_cache_class.return_value.__aexit__.return_value = None

        # Call _cleanup_process - should not raise
        await self.manager._cleanup_process()

        # Verify error was handled
        mock_cache.delete_process.assert_called_once()

    @pytest.mark.asyncio
    @patch('fullon_ohlcv_service.ohlcv.manager.ProcessCache')
    @patch('fullon_ohlcv_service.ohlcv.manager.get_collection_targets')
    async def test_health_loop(self, mock_get_targets, mock_process_cache_class):
        """Test periodic health updates loop."""
        # Mock ProcessCache
        mock_cache = AsyncMock()
        mock_process_cache_class.return_value.__aenter__.return_value = mock_cache
        mock_process_cache_class.return_value.__aexit__.return_value = None

        # Mock get_collection_targets to return empty dict
        mock_get_targets.return_value = {}

        # Create a controlled health loop for testing
        update_count = 0

        async def mock_health_loop():
            """Mock health loop that runs twice then stops."""
            nonlocal update_count
            while self.manager.running and update_count < 2:
                await self.manager._update_health()
                update_count += 1
                await asyncio.sleep(0.01)  # Short sleep for testing

        # Replace _health_loop with our mock
        self.manager._health_loop = mock_health_loop

        # Start manager
        await self.manager.start()

        # Wait for health updates
        await asyncio.sleep(0.05)

        # Stop manager
        await self.manager.stop()

        # Verify health updates were called
        assert update_count == 2
        assert mock_cache.update_process.call_count >= 2

    @pytest.mark.asyncio
    @patch('fullon_ohlcv_service.ohlcv.manager.ProcessCache')
    @patch('fullon_ohlcv_service.ohlcv.manager.get_collection_targets')
    async def test_start_registers_process(self, mock_get_targets, mock_process_cache_class):
        """Test that start() registers the process."""
        # Mock ProcessCache
        mock_cache = AsyncMock()
        mock_process_cache_class.return_value.__aenter__.return_value = mock_cache
        mock_process_cache_class.return_value.__aexit__.return_value = None

        # Mock get_collection_targets
        mock_get_targets.return_value = {}

        # Start manager
        await self.manager.start()

        # Verify process was registered
        mock_cache.new_process.assert_called_once()

        # Stop manager
        await self.manager.stop()

    @pytest.mark.asyncio
    @patch('fullon_ohlcv_service.ohlcv.manager.ProcessCache')
    @patch('fullon_ohlcv_service.ohlcv.manager.get_collection_targets')
    async def test_stop_cleans_up_process(self, mock_get_targets, mock_process_cache_class):
        """Test that stop() cleans up the process."""
        # Mock ProcessCache
        mock_cache = AsyncMock()
        mock_process_cache_class.return_value.__aenter__.return_value = mock_cache
        mock_process_cache_class.return_value.__aexit__.return_value = None

        # Mock get_collection_targets
        mock_get_targets.return_value = {}

        # Start and stop manager
        await self.manager.start()
        await self.manager.stop()

        # Verify process was cleaned up
        mock_cache.delete_process.assert_called_once_with(
            "ohlcv_service", "ohlcv_daemon"
        )

    @pytest.mark.asyncio
    @patch('fullon_ohlcv_service.ohlcv.manager.ProcessCache')
    @patch('fullon_ohlcv_service.ohlcv.manager.get_collection_targets')
    async def test_health_task_cancelled_on_stop(self, mock_get_targets, mock_process_cache_class):
        """Test that health task is cancelled on stop."""
        # Mock ProcessCache
        mock_cache = AsyncMock()
        mock_process_cache_class.return_value.__aenter__.return_value = mock_cache
        mock_process_cache_class.return_value.__aexit__.return_value = None

        # Mock get_collection_targets
        mock_get_targets.return_value = {}

        # Start manager
        await self.manager.start()

        # Verify health task was created
        assert hasattr(self.manager, '_health_task')
        health_task = self.manager._health_task

        # Stop manager
        await self.manager.stop()

        # Verify health task was cancelled
        assert health_task.cancelled()


class TestOhlcvCollectorHealthMonitoring:
    """Test health monitoring functionality in OhlcvCollector."""

    def setup_method(self):
        """Set up test fixtures."""
        self.exchange = "kraken"
        self.symbol = "BTC/USD"
        self.collector = OhlcvCollector(self.exchange, self.symbol)

    @pytest.mark.asyncio
    @patch('fullon_ohlcv_service.ohlcv.collector.ProcessCache')
    async def test_update_collector_status(self, mock_process_cache_class):
        """Test individual collector status update."""
        # Mock ProcessCache context manager
        mock_cache = AsyncMock()
        mock_process_cache_class.return_value.__aenter__.return_value = mock_cache
        mock_process_cache_class.return_value.__aexit__.return_value = None

        # Call _update_collector_status
        test_message = "Collecting historical data"
        await self.collector._update_collector_status(test_message)

        # Verify ProcessCache was used correctly
        mock_process_cache_class.assert_called_once()
        mock_cache.update_process.assert_called_once_with(
            tipe="ohlcv",
            key=f"{self.exchange}:{self.symbol}",
            message=test_message
        )

    @pytest.mark.asyncio
    @patch('fullon_ohlcv_service.ohlcv.collector.ProcessCache')
    @patch('fullon_ohlcv_service.ohlcv.collector.ExchangeQueue')
    @patch('fullon_ohlcv_service.ohlcv.collector.CandleRepository')
    async def test_collect_historical_updates_status(
        self, mock_repo_class, mock_exchange_queue, mock_process_cache_class
    ):
        """Test that collect_historical updates status."""
        # Mock ProcessCache
        mock_cache = AsyncMock()
        mock_process_cache_class.return_value.__aenter__.return_value = mock_cache
        mock_process_cache_class.return_value.__aexit__.return_value = None

        # Mock exchange handler
        mock_handler = AsyncMock()
        mock_handler.get_ohlcv.return_value = [
            {'timestamp': 1634567890, 'open': 50000.0, 'high': 51000.0,
             'low': 49000.0, 'close': 50500.0, 'volume': 100.5}
        ]
        mock_exchange_queue.get_rest_handler = AsyncMock(return_value=mock_handler)
        mock_exchange_queue.initialize_factory = AsyncMock()
        mock_exchange_queue.shutdown_factory = AsyncMock()

        # Mock repository
        mock_repo = AsyncMock()
        mock_repo.save_candles.return_value = True
        mock_repo_class.return_value.__aenter__.return_value = mock_repo

        # Collect historical data
        await self.collector.collect_historical()

        # Verify status updates were called
        expected_calls = [
            call(tipe="ohlcv", key=f"{self.exchange}:{self.symbol}",
                 message="Collecting historical data"),
            call(tipe="ohlcv", key=f"{self.exchange}:{self.symbol}",
                 message="Historical collection completed - 1 candles")
        ]
        mock_cache.update_process.assert_has_calls(expected_calls)

    @pytest.mark.asyncio
    @patch('fullon_ohlcv_service.ohlcv.collector.ProcessCache')
    @patch('fullon_ohlcv_service.ohlcv.collector.ExchangeQueue')
    @patch('fullon_ohlcv_service.ohlcv.collector.CandleRepository')
    @patch('fullon_ohlcv_service.ohlcv.collector.Candle')
    async def test_start_streaming_updates_status(
        self, mock_candle_class, mock_repo_class, mock_exchange_queue, mock_process_cache_class
    ):
        """Test that start_streaming updates status."""
        # Mock ProcessCache
        mock_cache = AsyncMock()
        mock_process_cache_class.return_value.__aenter__.return_value = mock_cache
        mock_process_cache_class.return_value.__aexit__.return_value = None

        # Mock exchange handler
        mock_handler = AsyncMock()
        mock_exchange_queue.get_websocket_handler = AsyncMock(return_value=mock_handler)
        mock_exchange_queue.initialize_factory = AsyncMock()
        mock_exchange_queue.shutdown_factory = AsyncMock()

        # Mock repository
        mock_repo = AsyncMock()
        mock_repo_class.return_value.__aenter__.return_value = mock_repo

        # Mock ohlcv data
        ohlcv_data = {
            'timestamp': 1634567890,
            'open': 50000.0,
            'high': 51000.0,
            'low': 49000.0,
            'close': 50500.0,
            'volume': 100.5
        }

        # Mock subscription
        async def mock_subscribe_ohlcv(symbol, callback_func, interval):
            await callback_func(ohlcv_data)

        mock_handler.subscribe_ohlcv = mock_subscribe_ohlcv

        # Start streaming
        await self.collector.start_streaming()

        # Verify status update was called
        mock_cache.update_process.assert_any_call(
            tipe="ohlcv",
            key=f"{self.exchange}:{self.symbol}",
            message="Streaming started"
        )