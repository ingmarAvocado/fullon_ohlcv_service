"""Unit tests for TradeCollector."""

import pytest
from unittest.mock import AsyncMock, patch

from fullon_ohlcv_service.trade.collector import TradeCollector


class TestTradeCollector:
    """Test the TradeCollector class."""

    def setup_method(self):
        self.exchange = "kraken"
        self.symbol = "BTC/USD"
        self.collector = TradeCollector(self.exchange, self.symbol)

    def test_init(self):
        assert self.collector.exchange == self.exchange
        assert self.collector.symbol == self.symbol
        assert self.collector.logger is not None
        assert self.collector.running is False  # Should have running state

    @pytest.mark.asyncio
    @patch("fullon_ohlcv_service.trade.collector.ExchangeQueue")
    @patch("fullon_ohlcv_service.trade.collector.TradeRepository")
    async def test_collect_historical_success(self, mock_repo_class, mock_exchange_queue):
        # Mock exchange handler - using get_rest_handler pattern from OhlcvCollector
        mock_handler = AsyncMock()
        mock_trades = [
            {"timestamp": 1634567890, "price": 50000.0, "volume": 0.1, "side": "BUY"},
            {"timestamp": 1634567900, "price": 50010.0, "volume": 0.2, "side": "SELL"},
        ]
        # Use get_trades method (standard fullon_exchange API)
        mock_handler.get_trades = AsyncMock(return_value=mock_trades)

        mock_exchange_queue.get_rest_handler = AsyncMock(return_value=mock_handler)
        mock_exchange_queue.initialize_factory = AsyncMock()
        mock_exchange_queue.shutdown_factory = AsyncMock()

        # Mock repository
        mock_repo = AsyncMock()
        mock_repo.save_trades = AsyncMock(return_value=True)
        mock_repo_class.return_value.__aenter__.return_value = mock_repo

        result = await self.collector.collect_historical_trades()

        mock_exchange_queue.initialize_factory.assert_called_once()
        mock_exchange_queue.get_rest_handler.assert_called_once_with(self.exchange)
        mock_handler.get_trades.assert_called_once_with(self.symbol, limit=1000)
        mock_repo.save_trades.assert_called_once()
        mock_exchange_queue.shutdown_factory.assert_called_once()

        assert result is True

    @pytest.mark.asyncio
    @patch("fullon_ohlcv_service.trade.collector.ExchangeQueue")
    @patch("fullon_ohlcv_service.trade.collector.TradeRepository")
    async def test_collect_historical_failure(self, mock_repo_class, mock_exchange_queue):
        mock_handler = AsyncMock()
        mock_handler.get_trades = AsyncMock(return_value=[{"timestamp": 1, "price": 1.0, "volume": 1.0}])

        mock_exchange_queue.get_rest_handler = AsyncMock(return_value=mock_handler)
        mock_exchange_queue.initialize_factory = AsyncMock()
        mock_exchange_queue.shutdown_factory = AsyncMock()

        mock_repo = AsyncMock()
        mock_repo.save_trades = AsyncMock(return_value=False)
        mock_repo_class.return_value.__aenter__.return_value = mock_repo

        result = await self.collector.collect_historical_trades()
        assert result is False

    @pytest.mark.asyncio
    @patch("fullon_ohlcv_service.trade.collector.ExchangeQueue")
    @patch("fullon_ohlcv_service.trade.collector.TradeRepository")
    async def test_start_streaming(self, mock_repo_class, mock_exchange_queue):
        # Use get_websocket_handler pattern from OhlcvCollector
        mock_handler = AsyncMock()
        mock_handler.connect = AsyncMock()

        mock_exchange_queue.get_websocket_handler = AsyncMock(return_value=mock_handler)
        mock_exchange_queue.initialize_factory = AsyncMock()
        mock_exchange_queue.shutdown_factory = AsyncMock()

        # Repo mock to accept saved trades
        mock_repo = AsyncMock()
        mock_repo.save_trades = AsyncMock(return_value=True)
        mock_repo_class.return_value.__aenter__.return_value = mock_repo

        trade_event = {"timestamp": 1634567890, "price": 50000.0, "volume": 0.1, "side": "BUY"}

        async def mock_subscribe_trades(symbol, cb):
            # Simulate receiving one trade immediately
            await cb(trade_event)

        mock_handler.subscribe_trades = mock_subscribe_trades

        await self.collector.start_streaming()

        mock_exchange_queue.initialize_factory.assert_called_once()
        mock_exchange_queue.get_websocket_handler.assert_called_once_with(self.exchange)
        mock_handler.connect.assert_called_once()
        mock_repo.save_trades.assert_called_once()
        mock_exchange_queue.shutdown_factory.assert_called_once()

        # Verify running state is set
        assert self.collector.running is True

    @pytest.mark.asyncio
    @patch("fullon_ohlcv_service.trade.collector.ExchangeQueue")
    async def test_exception_handling(self, mock_exchange_queue):
        mock_exchange_queue.initialize_factory = AsyncMock()
        mock_exchange_queue.shutdown_factory = AsyncMock()
        mock_exchange_queue.get_rest_handler = AsyncMock(side_effect=Exception("boom"))

        with pytest.raises(Exception, match="boom"):
            await self.collector.collect_historical_trades()

        mock_exchange_queue.shutdown_factory.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_streaming(self):
        """Test stop_streaming method sets running to False."""
        self.collector.running = True

        with patch("fullon_ohlcv_service.trade.collector.ExchangeQueue") as mock_exchange_queue:
            mock_exchange_queue.shutdown_factory = AsyncMock()

            await self.collector.stop_streaming()

            assert self.collector.running is False
            mock_exchange_queue.shutdown_factory.assert_called_once()

    @pytest.mark.asyncio
    @patch("fullon_ohlcv_service.trade.collector.ExchangeQueue")
    @patch("fullon_ohlcv_service.trade.collector.TradeRepository")
    async def test_streaming_with_callback(self, mock_repo_class, mock_exchange_queue):
        """Test that start_streaming accepts and uses an optional callback."""
        mock_handler = AsyncMock()
        mock_handler.connect = AsyncMock()

        mock_exchange_queue.get_websocket_handler = AsyncMock(return_value=mock_handler)
        mock_exchange_queue.initialize_factory = AsyncMock()
        mock_exchange_queue.shutdown_factory = AsyncMock()

        mock_repo = AsyncMock()
        mock_repo.save_trades = AsyncMock(return_value=True)
        mock_repo_class.return_value.__aenter__.return_value = mock_repo

        trade_event = {"timestamp": 1634567890, "price": 50000.0, "volume": 0.1}

        # Track callback calls
        callback_mock = AsyncMock()

        async def mock_subscribe_trades(symbol, cb):
            await cb(trade_event)

        mock_handler.subscribe_trades = mock_subscribe_trades

        await self.collector.start_streaming(callback=callback_mock)

        # Verify callback was called with trade data
        callback_mock.assert_called_once_with(trade_event)

