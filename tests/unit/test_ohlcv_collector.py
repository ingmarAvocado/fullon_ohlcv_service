"""Unit tests for OhlcvCollector."""

import pytest
from unittest.mock import AsyncMock, patch
from fullon_ohlcv_service.ohlcv.collector import OhlcvCollector


class TestOhlcvCollector:
    """Test the OhlcvCollector class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.exchange = "kraken"
        self.symbol = "BTC/USD"
        self.collector = OhlcvCollector(self.exchange, self.symbol)

    def test_init(self):
        """Test collector initialization."""
        assert self.collector.exchange == self.exchange
        assert self.collector.symbol == self.symbol
        assert self.collector.exchange_id is None  # No exchange_id provided
        assert self.collector.running is False
        assert self.collector.logger is not None

    def test_init_with_exchange_id(self):
        """Test collector initialization with exchange ID."""
        exchange_id = 123
        collector = OhlcvCollector(self.exchange, self.symbol, exchange_id=exchange_id)
        assert collector.exchange == self.exchange
        assert collector.symbol == self.symbol
        assert collector.exchange_id == exchange_id
        assert collector.running is False

    def test_credential_provider_without_exchange_id(self):
        """Test credential provider returns empty credentials when no exchange_id."""
        collector = OhlcvCollector(self.exchange, self.symbol)
        credential_provider = collector._create_credential_provider()

        # Should return empty credentials for public data
        exchange_obj = type('MockExchange', (), {})()
        key, secret = credential_provider(exchange_obj)
        assert key == ""
        assert secret == ""

    @patch('fullon_credentials.fullon_credentials')
    def test_credential_provider_with_exchange_id(self, mock_fullon_credentials):
        """Test credential provider uses fullon_credentials when exchange_id is provided."""
        exchange_id = 123
        collector = OhlcvCollector(self.exchange, self.symbol, exchange_id=exchange_id)

        # Mock fullon_credentials to return test credentials
        mock_fullon_credentials.return_value = ("test_secret", "test_key")

        credential_provider = collector._create_credential_provider()
        exchange_obj = type('MockExchange', (), {})()
        key, secret = credential_provider(exchange_obj)

        # Should return credentials from fullon_credentials
        assert key == "test_key"
        assert secret == "test_secret"
        mock_fullon_credentials.assert_called_once_with(ex_id=exchange_id)

    def test_credential_provider_fallback_on_error(self):
        """Test credential provider falls back to empty credentials on error."""
        exchange_id = 123
        collector = OhlcvCollector(self.exchange, self.symbol, exchange_id=exchange_id)

        credential_provider = collector._create_credential_provider()
        exchange_obj = type('MockExchange', (), {})()

        # Since fullon_credentials is not available in test environment, should fallback
        key, secret = credential_provider(exchange_obj)
        assert key == ""
        assert secret == ""

    @pytest.mark.asyncio
    @patch('fullon_ohlcv_service.ohlcv.collector.ProcessCache')
    @patch('fullon_ohlcv_service.ohlcv.collector.ExchangeQueue')
    @patch('fullon_ohlcv_service.ohlcv.collector.CandleRepository')
    async def test_collect_historical_success(self, mock_repo_class, mock_exchange_queue, mock_process_cache_class):
        """Test successful historical data collection."""
        # Mock exchange handler
        mock_handler = AsyncMock()
        mock_candles = [
            {'timestamp': 1634567890, 'open': 50000.0, 'high': 51000.0, 'low': 49000.0, 'close': 50500.0, 'volume': 100.5},
            {'timestamp': 1634567950, 'open': 50500.0, 'high': 51500.0, 'low': 50000.0, 'close': 51000.0, 'volume': 110.2},
        ]
        mock_handler.get_ohlcv.return_value = mock_candles
        mock_handler.connect = AsyncMock()

        mock_exchange_queue.get_rest_handler = AsyncMock(return_value=mock_handler)
        mock_exchange_queue.initialize_factory = AsyncMock()
        mock_exchange_queue.shutdown_factory = AsyncMock()

        # Mock repository
        mock_repo = AsyncMock()
        mock_repo.save_candles.return_value = True
        mock_repo_class.return_value.__aenter__.return_value = mock_repo

        # Mock ProcessCache
        mock_cache = AsyncMock()
        mock_process_cache_class.return_value.__aenter__.return_value = mock_cache
        mock_process_cache_class.return_value.__aexit__.return_value = None

        # Test the method
        result = await self.collector.collect_historical()

        # Verify calls
        mock_exchange_queue.initialize_factory.assert_called_once()
        # Check that get_rest_handler was called with exchange_obj and credential_provider
        assert mock_exchange_queue.get_rest_handler.call_count == 1
        call_args = mock_exchange_queue.get_rest_handler.call_args[0]
        assert len(call_args) == 2  # exchange_obj, credential_provider
        assert hasattr(call_args[0], 'ex_id')  # verify exchange object
        assert callable(call_args[1])  # verify credential provider
        mock_handler.connect.assert_called_once()
        mock_handler.get_ohlcv.assert_called_once_with(self.symbol, "1m", limit=1000)
        mock_repo.save_candles.assert_called_once_with(mock_candles)
        mock_exchange_queue.shutdown_factory.assert_called_once()

        assert result is True

    @pytest.mark.asyncio
    @patch('fullon_ohlcv_service.ohlcv.collector.ProcessCache')
    @patch('fullon_ohlcv_service.ohlcv.collector.ExchangeQueue')
    @patch('fullon_ohlcv_service.ohlcv.collector.CandleRepository')
    async def test_collect_historical_failure(self, mock_repo_class, mock_exchange_queue, mock_process_cache_class):
        """Test historical data collection with repository failure."""
        # Mock exchange handler
        mock_handler = AsyncMock()
        mock_candles = [
            {'timestamp': 1634567890, 'open': 50000.0, 'high': 51000.0, 'low': 49000.0, 'close': 50500.0, 'volume': 100.5},
        ]
        mock_handler.get_ohlcv.return_value = mock_candles
        mock_handler.connect = AsyncMock()

        mock_exchange_queue.get_rest_handler = AsyncMock(return_value=mock_handler)
        mock_exchange_queue.initialize_factory = AsyncMock()
        mock_exchange_queue.shutdown_factory = AsyncMock()

        # Mock repository with failure
        mock_repo = AsyncMock()
        mock_repo.save_candles.return_value = False
        mock_repo_class.return_value.__aenter__.return_value = mock_repo

        # Mock ProcessCache
        mock_cache = AsyncMock()
        mock_process_cache_class.return_value.__aenter__.return_value = mock_cache
        mock_process_cache_class.return_value.__aexit__.return_value = None

        # Test the method
        result = await self.collector.collect_historical()

        # Verify result
        assert result is False

    @pytest.mark.asyncio
    @patch('fullon_ohlcv_service.ohlcv.collector.ProcessCache')
    @patch('fullon_ohlcv_service.ohlcv.collector.ExchangeQueue')
    @patch('fullon_ohlcv_service.ohlcv.collector.CandleRepository')
    @patch('fullon_ohlcv_service.ohlcv.collector.Candle')
    async def test_start_streaming(self, mock_candle_class, mock_repo_class, mock_exchange_queue, mock_process_cache_class):
        """Test WebSocket streaming functionality."""
        # Mock exchange handler
        mock_handler = AsyncMock()
        mock_handler.connect = AsyncMock()
        mock_exchange_queue.get_websocket_handler = AsyncMock(return_value=mock_handler)
        mock_exchange_queue.initialize_factory = AsyncMock()
        mock_exchange_queue.shutdown_factory = AsyncMock()

        # Mock repository
        mock_repo = AsyncMock()
        mock_repo_class.return_value.__aenter__.return_value = mock_repo

        # Mock callback
        mock_callback = AsyncMock()

        # Mock ohlcv data
        ohlcv_data = {
            'timestamp': 1634567890,
            'open': 50000.0,
            'high': 51000.0,
            'low': 49000.0,
            'close': 50500.0,
            'volume': 100.5
        }

        # Test streaming with mocked subscription
        async def mock_subscribe_ohlcv(symbol, callback_func, interval):
            # Simulate receiving data
            await callback_func(ohlcv_data)

        mock_handler.subscribe_ohlcv = mock_subscribe_ohlcv

        # Mock ProcessCache
        mock_cache = AsyncMock()
        mock_process_cache_class.return_value.__aenter__.return_value = mock_cache
        mock_process_cache_class.return_value.__aexit__.return_value = None

        # Start streaming
        await self.collector.start_streaming(callback=mock_callback)

        # Verify calls
        mock_exchange_queue.initialize_factory.assert_called_once()
        # Check that get_websocket_handler was called with exchange_obj and credential_provider
        assert mock_exchange_queue.get_websocket_handler.call_count == 1
        call_args = mock_exchange_queue.get_websocket_handler.call_args[0]
        assert len(call_args) == 2  # exchange_obj, credential_provider
        assert hasattr(call_args[0], 'ex_id')  # verify exchange object
        assert callable(call_args[1])  # verify credential provider
        mock_handler.connect.assert_called_once()
        mock_candle_class.assert_called_once()
        mock_repo.save_candles.assert_called_once()
        mock_callback.assert_called_once_with(ohlcv_data)
        mock_exchange_queue.shutdown_factory.assert_called_once()

        assert self.collector.running is True

    @pytest.mark.asyncio
    @patch('fullon_ohlcv_service.ohlcv.collector.ExchangeQueue')
    async def test_stop_streaming(self, mock_exchange_queue):
        """Test stopping the streaming collection."""
        mock_exchange_queue.shutdown_factory = AsyncMock()

        # Set running to True first
        self.collector.running = True

        # Stop streaming
        await self.collector.stop_streaming()

        # Verify state and calls
        assert self.collector.running is False
        mock_exchange_queue.shutdown_factory.assert_called_once()

    @pytest.mark.asyncio
    @patch('fullon_ohlcv_service.ohlcv.collector.ProcessCache')
    @patch('fullon_ohlcv_service.ohlcv.collector.ExchangeQueue')
    async def test_exception_handling(self, mock_exchange_queue, mock_process_cache_class):
        """Test that exceptions are properly handled and cleanup occurs."""
        # Mock exchange queue to raise exception
        mock_exchange_queue.initialize_factory = AsyncMock()
        mock_exchange_queue.shutdown_factory = AsyncMock()
        mock_exchange_queue.get_rest_handler = AsyncMock(side_effect=Exception("Connection failed"))

        # Mock ProcessCache
        mock_cache = AsyncMock()
        mock_process_cache_class.return_value.__aenter__.return_value = mock_cache
        mock_process_cache_class.return_value.__aexit__.return_value = None

        # Test that exception is raised but cleanup still happens
        with pytest.raises(Exception, match="Connection failed"):
            await self.collector.collect_historical()

        # Verify cleanup was called
        mock_exchange_queue.shutdown_factory.assert_called_once()
