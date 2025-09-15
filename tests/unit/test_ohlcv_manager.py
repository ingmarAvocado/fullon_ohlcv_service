"""Unit tests for OhlcvManager - Simple daemon coordination."""

import asyncio
import pytest
from unittest.mock import AsyncMock, patch, create_autospec, MagicMock
from fullon_ohlcv_service.ohlcv.manager import OhlcvManager
from fullon_ohlcv_service.ohlcv.collector import OhlcvCollector


def mock_process_cache():
    """Helper to create ProcessCache mock."""
    mock_cache = AsyncMock()
    mock_cache.new_process = AsyncMock()
    mock_cache.update_process = AsyncMock()
    mock_cache.delete_process = AsyncMock()

    mock_process_cache_class = MagicMock()
    mock_process_cache_class.return_value.__aenter__.return_value = mock_cache
    mock_process_cache_class.return_value.__aexit__.return_value = None

    return mock_process_cache_class, mock_cache


class TestOhlcvManager:
    """Test the OhlcvManager class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.manager = OhlcvManager()

    def test_init(self):
        """Test manager initialization."""
        assert self.manager.logger is not None
        assert self.manager.collectors == {}
        assert self.manager.tasks == {}
        assert self.manager.running is False
        assert self.manager._health_task is None

    @pytest.mark.asyncio
    @patch('fullon_ohlcv_service.ohlcv.manager.ProcessCache')
    @patch('fullon_ohlcv_service.ohlcv.manager.get_collection_targets')
    @patch('fullon_ohlcv_service.ohlcv.manager.OhlcvCollector')
    async def test_start_with_multiple_exchanges(self, mock_collector_class, mock_get_targets, mock_process_cache_class):
        """Test starting collectors for multiple exchanges and symbols."""
        # Mock ProcessCache
        mock_process_cache_class, mock_cache = mock_process_cache()

        # Mock database configuration
        mock_get_targets.return_value = {
            "kraken": {"symbols": ["BTC/USD", "ETH/USD"], "ex_id": 1},
            "binance": {"symbols": ["BTC/USDT"], "ex_id": 2}
        }

        # Mock collector instances
        mock_collectors = {}
        def create_mock_collector(exchange, symbol, exchange_id=None, config=None):
            mock = create_autospec(OhlcvCollector, instance=True)
            mock.exchange = exchange
            mock.symbol = symbol
            mock.exchange_id = exchange_id
            mock.config = config
            mock.start_streaming = AsyncMock()
            key = f"{exchange}:{symbol}"
            mock_collectors[key] = mock
            return mock

        mock_collector_class.side_effect = create_mock_collector

        # Start the manager
        await self.manager.start()

        # Verify state
        assert self.manager.running is True
        assert len(self.manager.collectors) == 3
        assert len(self.manager.tasks) == 3

        # Verify collectors were created correctly
        assert "kraken:BTC/USD" in self.manager.collectors
        assert "kraken:ETH/USD" in self.manager.collectors
        assert "binance:BTC/USDT" in self.manager.collectors

        # Verify each collector's start_streaming was called
        for collector in mock_collectors.values():
            collector.start_streaming.assert_called_once()

        # Clean up
        await self.manager.stop()

    @pytest.mark.asyncio
    @patch('fullon_ohlcv_service.ohlcv.manager.get_collection_targets')
    async def test_start_when_already_running(self, mock_get_targets):
        """Test that start() does nothing if already running."""
        # Set manager as already running
        self.manager.running = True

        # Try to start again
        await self.manager.start()

        # Verify get_collection_targets was not called
        mock_get_targets.assert_not_called()
        assert self.manager.running is True
        assert len(self.manager.collectors) == 0

    @pytest.mark.asyncio
    @patch('fullon_ohlcv_service.ohlcv.manager.ProcessCache')
    @patch('fullon_ohlcv_service.ohlcv.manager.get_collection_targets')
    @patch('fullon_ohlcv_service.ohlcv.manager.OhlcvCollector')
    async def test_start_with_no_targets(self, mock_collector_class, mock_get_targets, mock_process_cache_class):
        """Test starting with no configured targets."""
        # Mock ProcessCache
        mock_process_cache_class, mock_cache = mock_process_cache()

        # Mock empty configuration
        mock_get_targets.return_value = {}

        # Start the manager
        await self.manager.start()

        # Verify state
        assert self.manager.running is True
        assert len(self.manager.collectors) == 0
        assert len(self.manager.tasks) == 0

        # Clean up
        await self.manager.stop()

    @pytest.mark.asyncio
    @patch('fullon_ohlcv_service.ohlcv.manager.ProcessCache')
    @patch('fullon_ohlcv_service.ohlcv.manager.get_collection_targets')
    @patch('fullon_ohlcv_service.ohlcv.manager.OhlcvCollector')
    async def test_stop_all_collectors(self, mock_collector_class, mock_get_targets, mock_process_cache_class):
        """Test stopping all collectors and tasks."""
        # Mock ProcessCache
        mock_process_cache_class, mock_cache = mock_process_cache()

        # Setup and start first
        mock_get_targets.return_value = {
            "kraken": {"symbols": ["BTC/USD", "ETH/USD"], "ex_id": 1}
        }

        # Mock collector with a long-running task
        mock_collector = create_autospec(OhlcvCollector, instance=True)

        async def long_running_task():
            from contextlib import suppress
            with suppress(asyncio.CancelledError):
                await asyncio.sleep(10)

        mock_collector.start_streaming = long_running_task
        mock_collector_class.return_value = mock_collector

        # Start manager
        await self.manager.start()
        assert self.manager.running is True
        assert len(self.manager.tasks) == 2

        # Stop manager
        await self.manager.stop()

        # Verify state
        assert self.manager.running is False
        assert len(self.manager.collectors) == 0
        assert len(self.manager.tasks) == 0

        # Verify all tasks were cancelled
        for task in self.manager.tasks.values():
            assert task.cancelled() or task.done()

    @pytest.mark.asyncio
    @patch('fullon_ohlcv_service.ohlcv.manager.ProcessCache')
    async def test_stop_when_not_running(self, mock_process_cache_class):
        """Test stopping when manager is not running."""
        # Mock ProcessCache
        mock_process_cache_class, mock_cache = mock_process_cache()

        # Manager is not running initially
        assert self.manager.running is False

        # Stop should work without errors
        await self.manager.stop()

        # Verify state remains unchanged
        assert self.manager.running is False
        assert len(self.manager.collectors) == 0
        assert len(self.manager.tasks) == 0

    @pytest.mark.asyncio
    @patch('fullon_ohlcv_service.ohlcv.manager.ProcessCache')
    @patch('fullon_ohlcv_service.ohlcv.manager.get_collection_targets')
    @patch('fullon_ohlcv_service.ohlcv.manager.OhlcvCollector')
    async def test_status_reporting(self, mock_collector_class, mock_get_targets, mock_process_cache_class):
        """Test status reporting with active and completed tasks."""
        # Mock ProcessCache
        mock_process_cache_class, mock_cache = mock_process_cache()

        # Setup configuration
        mock_get_targets.return_value = {
            "kraken": {"symbols": ["BTC/USD"], "ex_id": 1},
            "binance": {"symbols": ["BTC/USDT"], "ex_id": 2}
        }

        # Create mock collectors
        mock_collector = create_autospec(OhlcvCollector, instance=True)

        # One task completes quickly, one stays running
        call_count = 0
        async def variable_task():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # First task completes quickly
                return
            else:
                # Second task keeps running
                await asyncio.sleep(10)

        mock_collector.start_streaming = variable_task
        mock_collector_class.return_value = mock_collector

        # Start manager
        await self.manager.start()

        # Give first task time to complete
        await asyncio.sleep(0.1)

        # Get status
        status = await self.manager.status()

        # Verify status structure
        assert status["running"] is True
        assert "kraken:BTC/USD" in status["collectors"]
        assert "binance:BTC/USDT" in status["collectors"]
        assert len(status["collectors"]) == 2

        # At least one task should still be active
        assert status["active_tasks"] >= 1

        # Clean up
        await self.manager.stop()

    @pytest.mark.asyncio
    async def test_status_when_not_running(self):
        """Test status reporting when manager is not running."""
        status = await self.manager.status()

        assert status["running"] is False
        assert status["collectors"] == []
        assert status["active_tasks"] == 0

    @pytest.mark.asyncio
    @patch('fullon_ohlcv_service.ohlcv.manager.ProcessCache')
    @patch('fullon_ohlcv_service.ohlcv.manager.get_collection_targets')
    @patch('fullon_ohlcv_service.ohlcv.manager.OhlcvCollector')
    async def test_task_exception_handling(self, mock_collector_class, mock_get_targets, mock_process_cache_class):
        """Test that exceptions in collector tasks are handled."""
        # Mock ProcessCache
        mock_process_cache_class, mock_cache = mock_process_cache()

        # Setup configuration
        mock_get_targets.return_value = {
            "kraken": {"symbols": ["BTC/USD"], "ex_id": 1}
        }

        # Mock collector that raises exception
        mock_collector = create_autospec(OhlcvCollector, instance=True)

        async def failing_task():
            raise ValueError("Test exception")

        mock_collector.start_streaming = failing_task
        mock_collector_class.return_value = mock_collector

        # Start manager
        await self.manager.start()

        # Give task time to fail
        await asyncio.sleep(0.1)

        # Task should be done (with exception) but not crash the manager
        task = list(self.manager.tasks.values())[0]
        assert task.done()

        # Manager should still be running
        assert self.manager.running is True

        # Status should show task as not active
        status = await self.manager.status()
        assert status["active_tasks"] == 0

        # Clean up
        await self.manager.stop()

    @pytest.mark.asyncio
    @patch('fullon_ohlcv_service.ohlcv.manager.ProcessCache')
    @patch('fullon_ohlcv_service.ohlcv.manager.get_collection_targets')
    @patch('fullon_ohlcv_service.ohlcv.manager.OhlcvCollector')
    async def test_concurrent_task_execution(self, mock_collector_class, mock_get_targets, mock_process_cache_class):
        """Test that multiple collectors run concurrently, not sequentially."""
        # Mock ProcessCache
        mock_process_cache_class, mock_cache = mock_process_cache()

        # Setup configuration
        mock_get_targets.return_value = {
            "kraken": {"symbols": ["BTC/USD", "ETH/USD", "XRP/USD"], "ex_id": 1}
        }

        # Track task start times
        start_times = []

        async def track_start_time():
            start_times.append(asyncio.get_event_loop().time())
            await asyncio.sleep(0.1)

        # Mock collector
        mock_collector = create_autospec(OhlcvCollector, instance=True)
        mock_collector.start_streaming = track_start_time
        mock_collector_class.return_value = mock_collector

        # Start manager
        await self.manager.start()

        # Wait for all tasks to start
        await asyncio.sleep(0.05)

        # Verify all tasks started nearly simultaneously (within 10ms)
        assert len(start_times) == 3
        max_diff = max(start_times) - min(start_times)
        assert max_diff < 0.01  # All should start within 10ms

        # Clean up
        await self.manager.stop()