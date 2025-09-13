"""Unit tests for TradeManager."""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, create_autospec

from fullon_ohlcv_service.trade.manager import TradeManager


@pytest.fixture
def manager():
    return TradeManager()


@pytest.mark.asyncio
async def test_get_collection_targets(manager):
    # Mock DatabaseContext to yield a fake db object
    with patch("fullon_ohlcv_service.trade.manager.DatabaseContext") as mock_db_ctx:
        fake_db = MagicMock()

        # exchanges.get_user_exchanges returns list of dicts
        fake_db.exchanges.get_user_exchanges = AsyncMock(
            return_value=[{"name": "kraken", "active": True, "cat_ex_id": 1}]
        )

        # symbols.get_by_exchange_id returns list of objects with symbol and active
        symbol_obj = MagicMock()
        symbol_obj.symbol = "BTC/USD"
        symbol_obj.active = True
        fake_db.symbols.get_by_exchange_id = AsyncMock(return_value=[symbol_obj])

        # Configure async context manager return
        mock_db_ctx.return_value.__aenter__.return_value = fake_db

        targets = await manager.get_collection_targets()

        assert targets == {"kraken": ["BTC/USD"]}


@pytest.mark.asyncio
async def test_start_collector(manager):
    """Test that start_collector creates background tasks properly."""
    with patch("fullon_ohlcv_service.trade.manager.TradeCollector") as mock_collector_cls:
        with patch("asyncio.create_task") as mock_create_task:
            mock_instance = MagicMock()
            mock_instance.start_streaming = AsyncMock()
            mock_instance.exchange = "kraken"
            mock_instance.symbol = "BTC/USD"
            mock_collector_cls.return_value = mock_instance

            # Mock task object
            mock_task = create_autospec(asyncio.Task, instance=True)
            mock_create_task.return_value = mock_task

            await manager.start_collector("kraken", "BTC/USD")

            key = "kraken:BTC/USD"
            assert key in manager.collectors
            # Verify task was created for streaming
            mock_create_task.assert_called_once()
            # Verify collector is stored with its task
            assert manager.collectors[key]["collector"] == mock_instance
            assert manager.collectors[key]["task"] == mock_task


@pytest.mark.asyncio
async def test_start_from_database(manager):
    """Test that start_from_database creates tasks for all targets."""
    with patch.object(manager, "get_collection_targets", return_value={"kraken": ["BTC/USD", "ETH/USD"]}):
        with patch("fullon_ohlcv_service.trade.manager.TradeCollector") as mock_collector_cls:
            with patch("asyncio.create_task") as mock_create_task:
                instances = {}

                def _build_instance(*args, **kwargs):
                    inst = MagicMock()
                    inst.start_streaming = AsyncMock()
                    inst.exchange = args[0]
                    inst.symbol = args[1]
                    instances[f"{inst.exchange}:{inst.symbol}"] = inst
                    return inst

                mock_collector_cls.side_effect = _build_instance

                # Mock task objects
                mock_tasks = [create_autospec(asyncio.Task, instance=True) for _ in range(2)]
                mock_create_task.side_effect = mock_tasks

                await manager.start_from_database()

                assert "kraken:BTC/USD" in manager.collectors
                assert "kraken:ETH/USD" in manager.collectors
                # Verify tasks were created
                assert mock_create_task.call_count == 2


@pytest.mark.asyncio
async def test_stop_collector(manager):
    """Test that stop_collector cancels task and calls stop_streaming."""
    with patch("fullon_ohlcv_service.trade.manager.TradeCollector") as mock_collector_cls:
        with patch("asyncio.create_task") as mock_create_task:
            mock_instance = MagicMock()
            mock_instance.start_streaming = AsyncMock()
            mock_instance.stop_streaming = AsyncMock()  # Add stop_streaming method
            mock_instance.exchange = "kraken"
            mock_instance.symbol = "BTC/USD"
            mock_collector_cls.return_value = mock_instance

            # Create a proper async task mock
            class MockTask:
                def __init__(self):
                    self.cancelled = False
                    self._done = False

                def cancel(self):
                    self.cancelled = True

                def done(self):
                    return self._done

                def __await__(self):
                    async def _await():
                        raise asyncio.CancelledError()
                    return _await().__await__()

            mock_task = MockTask()
            mock_create_task.return_value = mock_task

            await manager.start_collector("kraken", "BTC/USD")
            await manager.stop_collector("kraken", "BTC/USD")

            # Verify stop_streaming was called
            mock_instance.stop_streaming.assert_called_once()
            # Verify task was cancelled
            assert mock_task.cancelled is True
            # Verify collector was removed
            assert "kraken:BTC/USD" not in manager.collectors


@pytest.mark.asyncio
async def test_get_status(manager):
    """Test that get_status returns collector and task status."""
    with patch("fullon_ohlcv_service.trade.manager.TradeCollector") as mock_collector_cls:
        with patch("asyncio.create_task") as mock_create_task:
            mock_instance = MagicMock()
            mock_instance.start_streaming = AsyncMock()
            mock_instance.exchange = "kraken"
            mock_instance.symbol = "BTC/USD"
            mock_instance.running = True  # Add running state
            mock_collector_cls.return_value = mock_instance

            # Mock task object
            mock_task = create_autospec(asyncio.Task, instance=True)
            mock_task.done = MagicMock(return_value=False)
            mock_create_task.return_value = mock_task

            await manager.start_collector("kraken", "BTC/USD")
            status = await manager.get_status()

            assert status == {
                "kraken:BTC/USD": {
                    "exchange": "kraken",
                    "symbol": "BTC/USD",
                    "active": True,
                    "running": True,
                    "task_done": False
                }
            }


@pytest.mark.asyncio
async def test_stop_all_collectors(manager):
    """Test that stop method gracefully shuts down all collectors."""
    with patch("fullon_ohlcv_service.trade.manager.TradeCollector") as mock_collector_cls:
        with patch("asyncio.create_task") as mock_create_task:
            # Create two mock collectors
            instances = []
            tasks = []

            # Create a proper async task mock class
            class MockTask:
                def __init__(self):
                    self.cancelled = False
                    self._done = False

                def cancel(self):
                    self.cancelled = True

                def done(self):
                    return self._done

                def __await__(self):
                    async def _await():
                        raise asyncio.CancelledError()
                    return _await().__await__()

            for i, (exchange, symbol) in enumerate([("kraken", "BTC/USD"), ("binance", "ETH/USDT")]):
                mock_instance = MagicMock()
                mock_instance.start_streaming = AsyncMock()
                mock_instance.stop_streaming = AsyncMock()
                mock_instance.exchange = exchange
                mock_instance.symbol = symbol
                instances.append(mock_instance)

                mock_task = MockTask()
                tasks.append(mock_task)

            mock_collector_cls.side_effect = instances
            mock_create_task.side_effect = tasks

            # Start collectors
            await manager.start_collector("kraken", "BTC/USD")
            await manager.start_collector("binance", "ETH/USDT")

            # Stop all collectors
            await manager.stop()

            # Verify all stop_streaming methods were called
            for inst in instances:
                inst.stop_streaming.assert_called_once()

            # Verify all tasks were cancelled
            for task in tasks:
                assert task.cancelled is True

            # Verify collectors were cleared
            assert len(manager.collectors) == 0
            assert manager.running is False

