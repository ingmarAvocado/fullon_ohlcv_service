"""Unit tests for TradeCollector."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from fullon_ohlcv_service.trade.collector import TradeCollector


@pytest.fixture
def mock_exchange_queue():
    """Create properly managed mock for ExchangeQueue."""
    with patch("fullon_ohlcv_service.trade.collector.ExchangeQueue") as mock_eq:
        # Only async methods should be AsyncMock
        mock_eq.initialize_factory = AsyncMock()
        mock_eq.shutdown_factory = AsyncMock()
        mock_eq.get_rest_handler = AsyncMock()
        mock_eq.get_websocket_handler = AsyncMock()
        yield mock_eq


@pytest.fixture
def mock_trade_repository():
    """Create properly managed mock for TradeRepository."""
    with patch("fullon_ohlcv_service.trade.collector.TradeRepository") as mock_repo_class:
        mock_repo = MagicMock()
        mock_repo.save_trades = AsyncMock()
        mock_repo_class.return_value.__aenter__.return_value = mock_repo
        yield mock_repo_class, mock_repo


class TestTradeCollector:
    """Test the TradeCollector class."""

    def setup_method(self):
        self.exchange = "kraken"
        self.symbol = "BTC/USD"
        self.collector = TradeCollector(self.exchange, self.symbol)

    def test_init(self):
        assert self.collector.exchange == self.exchange
        assert self.collector.symbol == self.symbol
        assert self.collector.exchange_id is None  # No exchange_id provided
        assert self.collector.logger is not None
        assert self.collector.running is False  # Should have running state

    def test_init_with_exchange_id(self):
        """Test collector initialization with exchange ID."""
        exchange_id = 456
        collector = TradeCollector(self.exchange, self.symbol, exchange_id=exchange_id)
        assert collector.exchange == self.exchange
        assert collector.symbol == self.symbol
        assert collector.exchange_id == exchange_id
        assert collector.running is False

    @pytest.mark.asyncio
    async def test_collect_historical_success(self, mock_exchange_queue, mock_trade_repository):
        mock_repo_class, mock_repo = mock_trade_repository

        # Mock exchange handler - using get_rest_handler pattern from OhlcvCollector
        mock_handler = MagicMock()
        mock_trades = [
            {"timestamp": 1634567890, "price": 50000.0, "volume": 0.1, "side": "BUY"},
            {"timestamp": 1634567900, "price": 50010.0, "volume": 0.2, "side": "SELL"},
        ]
        # Use get_trades method (standard fullon_exchange API)
        mock_handler.get_trades = AsyncMock(return_value=mock_trades)
        mock_handler.connect = AsyncMock()
        mock_exchange_queue.get_rest_handler.return_value = mock_handler
        mock_repo.save_trades.return_value = True

        result = await self.collector.collect_historical_trades()

        mock_exchange_queue.initialize_factory.assert_called_once()
        # Check that get_rest_handler was called with exchange_obj and credential_provider
        assert mock_exchange_queue.get_rest_handler.call_count == 1
        call_args = mock_exchange_queue.get_rest_handler.call_args[0]
        assert len(call_args) == 2  # exchange_obj, credential_provider
        assert hasattr(call_args[0], 'ex_id')  # verify exchange object
        assert callable(call_args[1])  # verify credential provider
        mock_handler.connect.assert_called_once()
        mock_handler.get_trades.assert_called_once_with(self.symbol, limit=1000)
        mock_repo.save_trades.assert_called_once()
        mock_exchange_queue.shutdown_factory.assert_called_once()

        assert result is True

    @pytest.mark.asyncio
    async def test_collect_historical_failure(self, mock_exchange_queue, mock_trade_repository):
        mock_repo_class, mock_repo = mock_trade_repository

        mock_handler = MagicMock()
        mock_handler.get_trades = AsyncMock(return_value=[{"timestamp": 1, "price": 1.0, "volume": 1.0}])
        mock_handler.connect = AsyncMock()
        mock_exchange_queue.get_rest_handler.return_value = mock_handler
        mock_repo.save_trades.return_value = False

        result = await self.collector.collect_historical_trades()
        assert result is False

    @pytest.mark.asyncio
    async def test_start_streaming(self, mock_exchange_queue, mock_trade_repository):
        mock_repo_class, mock_repo = mock_trade_repository

        # Use get_websocket_handler pattern from OhlcvCollector
        mock_handler = MagicMock()
        mock_handler.connect = AsyncMock()
        mock_exchange_queue.get_websocket_handler.return_value = mock_handler
        mock_repo.save_trades.return_value = True

        trade_event = {"timestamp": 1634567890, "price": 50000.0, "volume": 0.1, "side": "BUY"}

        async def mock_subscribe_trades(symbol, cb):
            # Simulate receiving one trade immediately
            await cb(trade_event)

        mock_handler.subscribe_trades = mock_subscribe_trades

        await self.collector.start_streaming()

        mock_exchange_queue.initialize_factory.assert_called_once()
        # Check that get_websocket_handler was called with exchange_obj and credential_provider
        assert mock_exchange_queue.get_websocket_handler.call_count == 1
        call_args = mock_exchange_queue.get_websocket_handler.call_args[0]
        assert len(call_args) == 2  # exchange_obj, credential_provider
        assert hasattr(call_args[0], 'ex_id')  # verify exchange object
        assert callable(call_args[1])  # verify credential provider
        mock_handler.connect.assert_called_once()
        mock_repo.save_trades.assert_called_once()
        mock_exchange_queue.shutdown_factory.assert_called_once()

        # Verify running state is set
        assert self.collector.running is True

    @pytest.mark.asyncio
    async def test_exception_handling(self, mock_exchange_queue):
        mock_exchange_queue.get_rest_handler.side_effect = Exception("boom")

        with pytest.raises(Exception, match="boom"):
            await self.collector.collect_historical_trades()

        mock_exchange_queue.shutdown_factory.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_streaming(self, mock_exchange_queue):
        """Test stop_streaming method sets running to False."""
        self.collector.running = True

        await self.collector.stop_streaming()

        assert self.collector.running is False
        mock_exchange_queue.shutdown_factory.assert_called_once()

    @pytest.mark.asyncio
    async def test_streaming_with_callback(self, mock_exchange_queue, mock_trade_repository):
        """Test that start_streaming accepts and uses an optional callback."""
        mock_repo_class, mock_repo = mock_trade_repository

        mock_handler = MagicMock()
        mock_handler.connect = AsyncMock()
        mock_exchange_queue.get_websocket_handler.return_value = mock_handler
        mock_repo.save_trades.return_value = True

        trade_event = {"timestamp": 1634567890, "price": 50000.0, "volume": 0.1}

        # Track callback calls - create a proper async callback
        callback_calls = []

        async def callback_mock(trade_data):
            callback_calls.append(trade_data)

        async def mock_subscribe_trades(symbol, cb):
            await cb(trade_event)

        mock_handler.subscribe_trades = mock_subscribe_trades

        await self.collector.start_streaming(callback=callback_mock)

        # Verify callback was called with trade data
        assert len(callback_calls) == 1
        assert callback_calls[0] == trade_event

