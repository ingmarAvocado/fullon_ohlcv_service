"""Unit tests for trade/live_collector.py"""

from unittest.mock import AsyncMock, Mock, patch

import pytest

from fullon_ohlcv_service.trade.live_collector import LiveTradeCollector


class TestLiveTradeCollector:
    """Test cases for LiveTradeCollector class."""

    @pytest.mark.asyncio
    async def test_initialization(self):
        """Test collector initialization."""
        symbols = [Mock(symbol="BTC/USDT")]
        collector = LiveTradeCollector(symbols=symbols)

        assert collector.symbols == symbols
        assert hasattr(collector, "_load_data")
        assert hasattr(collector, "start_collection")
        assert hasattr(collector, "stop_collection")
        assert collector.running is False
        assert collector.websocket_handlers == {}
        assert collector.registered_symbols == set()
        assert collector.process_ids == {}

    @pytest.mark.asyncio
    async def test_initialization_empty_symbols(self):
        """Test collector initialization with empty symbols."""
        collector = LiveTradeCollector(symbols=[])

        assert collector.symbols == []
        assert collector.running is False
        assert collector.websocket_handlers == {}
        assert collector.registered_symbols == set()

    @pytest.mark.asyncio
    async def test_start_collection_already_running(self):
        """Test start_collection when already running."""
        collector = LiveTradeCollector(symbols=[])
        collector.running = True

        await collector.start_collection()

        assert collector.running is True

    @pytest.mark.asyncio
    async def test_stop_collection(self):
        """Test stop_collection."""
        collector = LiveTradeCollector(symbols=[])
        collector.running = True
        collector.registered_symbols = {"binance:BTC/USDT"}

        with patch(
            "fullon_ohlcv_service.trade.live_collector.GlobalTradeBatcher"
        ) as mock_batcher_class:
            mock_batcher = AsyncMock()
            mock_batcher_class.return_value = mock_batcher

            await collector.stop_collection()

            assert collector.running is False
            assert collector.registered_symbols == set()
            mock_batcher.unregister_symbol.assert_called_once_with("binance", "BTC/USDT")

    @patch("fullon_ohlcv_service.trade.live_collector.get_admin_exchanges")
    @patch("fullon_ohlcv_service.trade.live_collector.DatabaseContext")
    @pytest.mark.asyncio
    async def test_load_data_success(self, mock_db_class, mock_get_admin_exchanges):
        """Test successful data loading."""
        # Setup mock database context
        mock_db = AsyncMock()
        mock_db_class.return_value.__aenter__.return_value = mock_db

        # Mock admin helper
        mock_exchange = Mock()
        mock_exchange.cat_exchange.name = "binance"
        mock_get_admin_exchanges.return_value = (1, [mock_exchange])

        # Mock symbols
        mock_symbol = Mock(symbol="BTC/USDT")
        mock_symbol.cat_exchange.name = "binance"
        mock_db.symbols.get_all.return_value = [mock_symbol]

        collector = LiveTradeCollector(symbols=[mock_symbol])
        symbols_by_exchange, admin_exchanges = await collector._load_data()

        assert "binance" in symbols_by_exchange
        assert len(symbols_by_exchange["binance"]) == 1
        assert len(admin_exchanges) == 1
        mock_get_admin_exchanges.assert_called_once()
        mock_db.symbols.get_all.assert_called_once()

    @patch("fullon_ohlcv_service.trade.live_collector.get_admin_exchanges")
    @pytest.mark.asyncio
    async def test_load_data_no_admin_user(self, mock_get_admin_exchanges):
        """Test data loading when admin user is not found."""
        # Mock admin_helper to raise ValueError
        mock_get_admin_exchanges.side_effect = ValueError("Admin user admin@fullon not found")

        collector = LiveTradeCollector(symbols=[])

        with pytest.raises(ValueError, match="Admin user .* not found"):
            await collector._load_data()

    @patch("fullon_ohlcv_service.trade.live_collector.get_admin_exchanges")
    @pytest.mark.asyncio
    async def test_load_data_database_error(self, mock_get_admin_exchanges):
        """Test data loading with database error."""
        mock_get_admin_exchanges.side_effect = Exception("Database connection failed")

        collector = LiveTradeCollector(symbols=[])

        with pytest.raises(Exception, match="Database connection failed"):
            await collector._load_data()

    @patch("fullon_ohlcv_service.trade.live_collector.ExchangeQueue")
    @patch("fullon_ohlcv_service.trade.live_collector.ProcessCache")
    @patch("fullon_ohlcv_service.trade.live_collector.GlobalTradeBatcher")
    @pytest.mark.asyncio
    async def test_start_exchange_collector_success(
        self, mock_batcher_class, mock_process_cache, mock_exchange_queue_class
    ):
        """Test successful exchange collector startup."""
        # Setup mocks
        mock_handler = AsyncMock()
        mock_handler.subscribe_trades = AsyncMock(return_value=True)
        mock_exchange_queue_class.get_websocket_handler = AsyncMock(return_value=mock_handler)

        mock_cache = AsyncMock()
        mock_cache.register_process = AsyncMock(return_value="mock_process_id")
        mock_process_cache.return_value.__aenter__.return_value = mock_cache

        mock_batcher = AsyncMock()
        mock_batcher_class.return_value = mock_batcher

        # Mock exchange and symbols
        mock_exchange = Mock()
        mock_exchange.cat_exchange.name = "binance"

        mock_symbol = Mock(symbol="BTC/USDT")
        mock_symbol.cat_exchange.name = "binance"

        collector = LiveTradeCollector(symbols=[mock_symbol])

        await collector._start_exchange_collector(mock_exchange, [mock_symbol])

        # Verify handler was obtained and stored
        assert "binance" in collector.websocket_handlers
        assert collector.websocket_handlers["binance"] == mock_handler

        # Verify symbol was registered
        assert "binance:BTC/USDT" in collector.registered_symbols

        # Verify process was registered
        assert "binance:BTC/USDT" in collector.process_ids
        assert collector.process_ids["binance:BTC/USDT"] == "mock_process_id"

        # Verify batcher was called
        mock_batcher.register_symbol.assert_called_once_with("binance", "BTC/USDT")

        # Verify subscription was called
        mock_handler.subscribe_trades.assert_called_once()

    @patch("fullon_ohlcv_service.trade.live_collector.ExchangeQueue")
    @patch("fullon_ohlcv_service.trade.live_collector.ProcessCache")
    @pytest.mark.asyncio
    async def test_start_exchange_collector_subscription_failure(
        self, mock_process_cache, mock_exchange_queue_class
    ):
        """Test exchange collector when subscription fails."""
        # Setup mocks
        mock_handler = AsyncMock()
        mock_handler.subscribe_trades = AsyncMock(side_effect=Exception("Subscription failed"))
        mock_exchange_queue_class.get_websocket_handler = AsyncMock(return_value=mock_handler)

        mock_cache = AsyncMock()
        mock_cache.register_process = AsyncMock(return_value="mock_process_id")
        mock_process_cache.return_value.__aenter__.return_value = mock_cache

        # Mock exchange and symbols
        mock_exchange = Mock()
        mock_exchange.cat_exchange.name = "binance"

        mock_symbol = Mock(symbol="BTC/USDT")

        collector = LiveTradeCollector(symbols=[mock_symbol])

        await collector._start_exchange_collector(mock_exchange, [mock_symbol])

        # Verify handler was stored but symbol was not registered due to failure
        assert "binance" in collector.websocket_handlers
        assert "binance:BTC/USDT" not in collector.registered_symbols

    def test_create_exchange_callback(self):
        """Test creation of exchange callback."""
        collector = LiveTradeCollector()
        callback = collector._create_exchange_callback("binance")

        assert callable(callback)

    @patch("fullon_ohlcv_service.trade.live_collector.TradesCache")
    @patch("fullon_ohlcv_service.trade.live_collector.ProcessCache")
    @pytest.mark.asyncio
    async def test_trade_callback_success(self, mock_process_cache, mock_trades_cache):
        """Test trade callback with valid trade data."""
        # Setup mocks
        mock_cache = AsyncMock()
        mock_trades_cache.return_value.__aenter__.return_value = mock_cache

        mock_proc_cache = AsyncMock()
        mock_process_cache.return_value.__aenter__.return_value = mock_proc_cache

        collector = LiveTradeCollector()
        collector.process_ids["binance:BTC/USDT"] = "mock_process_id"

        callback = collector._create_exchange_callback("binance")

        # Mock trade object
        mock_trade = Mock()
        mock_trade.symbol = "BTC/USDT"
        mock_trade.time = "2023-01-01 12:00:00"

        await callback(mock_trade)

        # Verify trade was pushed to cache
        mock_cache.push_trade_list.assert_called_once_with(
            symbol="BTC/USDT", exchange="binance", trade=mock_trade
        )

        # Verify trade status was updated
        mock_cache.update_trade_status.assert_called_once_with(key="binance")

        # Verify process was updated
        mock_proc_cache.update_process.assert_called_once()

    @patch("fullon_ohlcv_service.trade.live_collector.ProcessCache")
    @pytest.mark.asyncio
    async def test_trade_callback_missing_symbol(self, mock_process_cache):
        """Test trade callback with trade missing symbol attribute."""
        collector = LiveTradeCollector()
        callback = collector._create_exchange_callback("binance")

        # Mock trade object without symbol
        mock_trade = Mock()
        del mock_trade.symbol  # Remove symbol attribute

        # Should not raise exception, just log warning
        await callback(mock_trade)

    @patch("fullon_ohlcv_service.trade.live_collector.TradesCache")
    @patch("fullon_ohlcv_service.trade.live_collector.ProcessCache")
    @pytest.mark.asyncio
    async def test_trade_callback_error_handling(self, mock_process_cache, mock_trades_cache):
        """Test trade callback error handling."""
        # Setup mocks
        mock_cache = AsyncMock()
        mock_cache.push_trade_list.side_effect = Exception("Cache error")
        mock_trades_cache.return_value.__aenter__.return_value = mock_cache

        mock_proc_cache = AsyncMock()
        mock_process_cache.return_value.__aenter__.return_value = mock_proc_cache

        collector = LiveTradeCollector()
        collector.process_ids["binance:BTC/USDT"] = "mock_process_id"

        callback = collector._create_exchange_callback("binance")

        # Mock trade object
        mock_trade = Mock()
        mock_trade.symbol = "BTC/USDT"
        mock_trade.time = "2023-01-01 12:00:00"

        await callback(mock_trade)

        # Verify process was updated with error status
        mock_proc_cache.update_process.assert_called_once()
        call_args = mock_proc_cache.update_process.call_args
        assert "Error:" in call_args[1]["message"]
        assert call_args[1]["status"].name == "ERROR"


class TestLiveTradeCollectorDynamicSymbols:
    """Test cases for dynamic symbol management in LiveTradeCollector."""

    @pytest.mark.asyncio
    async def test_is_collecting_symbol_active(self):
        """Test is_collecting returns True for active symbol."""
        collector = LiveTradeCollector()
        collector.registered_symbols = {"binance:BTC/USDT"}

        # Create mock symbol
        mock_symbol = Mock(symbol="BTC/USDT")
        mock_symbol.cat_exchange.name = "binance"

        assert collector.is_collecting(mock_symbol) is True

    @pytest.mark.asyncio
    async def test_is_collecting_symbol_not_active(self):
        """Test is_collecting returns False for inactive symbol."""
        collector = LiveTradeCollector()
        collector.registered_symbols = {"binance:BTC/USDT"}

        # Create mock symbol for different pair
        mock_symbol = Mock(symbol="ETH/USDT")
        mock_symbol.cat_exchange.name = "binance"

        assert collector.is_collecting(mock_symbol) is False

    @patch("fullon_ohlcv_service.trade.live_collector.get_admin_exchanges")
    @pytest.mark.asyncio
    async def test_add_symbol_collector_not_running(self, mock_get_admin_exchanges):
        """Test start_symbol raises ValueError when admin exchange not found."""
        collector = LiveTradeCollector()
        collector.running = False

        # Mock get_admin_exchanges to return empty list
        mock_get_admin_exchanges.return_value = (1, [])

        mock_symbol = Mock(symbol="BTC/USDT")
        mock_symbol.cat_exchange.name = "binance"

        with pytest.raises(ValueError, match="Admin exchange.*not found"):
            await collector.start_symbol(mock_symbol)

    @patch("fullon_ohlcv_service.trade.live_collector.ExchangeQueue")
    @patch("fullon_ohlcv_service.trade.live_collector.get_admin_exchanges")
    @patch("fullon_ohlcv_service.trade.live_collector.ProcessCache")
    @pytest.mark.asyncio
    async def test_add_symbol_already_collecting(self, mock_process_cache, mock_get_admin_exchanges, mock_exchange_queue):
        """Test start_symbol does not fail when symbol already being collected."""
        collector = LiveTradeCollector()
        collector.running = True
        collector.registered_symbols = {"binance:BTC/USDT"}

        # Mock handler
        mock_handler = AsyncMock()
        mock_handler.subscribe_trades = AsyncMock(return_value=True)
        collector.websocket_handlers["binance"] = mock_handler

        # Mock admin exchanges
        mock_exchange = Mock()
        mock_exchange.cat_exchange.name = "binance"
        mock_get_admin_exchanges.return_value = (1, [mock_exchange])

        # Mock ExchangeQueue to return existing handler
        mock_exchange_queue.get_websocket_handler = AsyncMock(return_value=mock_handler)

        # Mock ProcessCache
        mock_cache = AsyncMock()
        mock_cache.register_process = AsyncMock(return_value="test_process_id")
        mock_process_cache.return_value.__aenter__.return_value = mock_cache

        mock_symbol = Mock(symbol="BTC/USDT")
        mock_symbol.cat_exchange.name = "binance"

        # Should not raise exception
        await collector.start_symbol(mock_symbol)

        # Symbol still registered (now called twice with same symbol)
        assert "binance:BTC/USDT" in collector.registered_symbols

    @patch("fullon_ohlcv_service.trade.live_collector.ExchangeQueue")
    @patch("fullon_ohlcv_service.trade.live_collector.get_admin_exchanges")
    @patch("fullon_ohlcv_service.trade.live_collector.GlobalTradeBatcher")
    @pytest.mark.asyncio
    async def test_add_symbol_handler_exists_new_symbol(
        self, mock_batcher_class, mock_get_admin_exchanges, mock_exchange_queue
    ):
        """Test start_symbol reuses handler for new symbol on existing exchange."""
        collector = LiveTradeCollector()
        collector.running = True
        collector.registered_symbols = {"binance:BTC/USDT"}
        collector.symbols = [Mock(symbol="BTC/USDT")]

        # Mock existing handler
        mock_handler = AsyncMock()
        mock_handler.subscribe_trades = AsyncMock(return_value=True)
        collector.websocket_handlers["binance"] = mock_handler

        # Mock admin exchanges
        mock_exchange = Mock()
        mock_exchange.cat_exchange.name = "binance"
        mock_get_admin_exchanges.return_value = (1, [mock_exchange])

        # Mock ExchangeQueue to return existing handler
        mock_exchange_queue.get_websocket_handler = AsyncMock(return_value=mock_handler)

        # Mock batcher
        mock_batcher = AsyncMock()
        mock_batcher_class.return_value = mock_batcher

        # Create new symbol to add
        mock_symbol = Mock(symbol="ETH/USDT")
        mock_symbol.cat_exchange.name = "binance"

        with patch("fullon_ohlcv_service.trade.live_collector.ProcessCache") as mock_process_cache:
            mock_cache = AsyncMock()
            mock_cache.register_process = AsyncMock(return_value="new_process_id")
            mock_process_cache.return_value.__aenter__.return_value = mock_cache

            await collector.start_symbol(mock_symbol)

        # Verify handler was reused (not created again)
        assert collector.websocket_handlers["binance"] == mock_handler

        # Verify subscription was called with new symbol
        mock_handler.subscribe_trades.assert_called_once()
        call_args = mock_handler.subscribe_trades.call_args
        assert call_args[0][0] == "ETH/USDT"

        # Verify symbol was added to registered_symbols
        assert "binance:ETH/USDT" in collector.registered_symbols

        # Verify batcher registration
        mock_batcher.register_symbol.assert_called_once_with("binance", "ETH/USDT")

    @patch("fullon_ohlcv_service.trade.live_collector.get_admin_exchanges")
    @pytest.mark.asyncio
    async def test_add_symbol_no_handler_creates_new(self, mock_get_admin_exchanges):
        """Test start_symbol creates new handler if none exists for exchange."""
        collector = LiveTradeCollector()
        collector.running = True
        collector.registered_symbols = set()
        collector.symbols = []

        # Mock admin exchanges
        mock_exchange = Mock()
        mock_exchange.cat_exchange.name = "binance"
        mock_get_admin_exchanges.return_value = (1, [mock_exchange])

        # Create symbol to add
        mock_symbol = Mock(symbol="BTC/USDT")
        mock_symbol.cat_exchange.name = "binance"

        with patch.object(
            collector, "_start_exchange_collector", new=AsyncMock()
        ) as mock_start_collector:
            await collector.start_symbol(mock_symbol)

            # Verify _start_exchange_collector was called with admin_exchange and symbol list
            mock_start_collector.assert_called_once()
            call_args = mock_start_collector.call_args
            assert call_args[0][0] == mock_exchange
            assert call_args[0][1] == [mock_symbol]

    @patch("fullon_ohlcv_service.trade.live_collector.get_admin_exchanges")
    @pytest.mark.asyncio
    async def test_add_symbol_admin_exchange_not_found(self, mock_get_admin_exchanges):
        """Test start_symbol raises ValueError when admin exchange not found."""
        collector = LiveTradeCollector()
        collector.running = True

        # Mock admin exchanges (returning empty list or wrong exchange)
        mock_other_exchange = Mock()
        mock_other_exchange.cat_exchange.name = "kraken"
        mock_get_admin_exchanges.return_value = (1, [mock_other_exchange])

        # Create symbol for exchange that doesn't exist
        mock_symbol = Mock(symbol="BTC/USDT")
        mock_symbol.cat_exchange.name = "binance"

        with pytest.raises(ValueError, match="Admin exchange.*not found"):
            await collector.start_symbol(mock_symbol)

    @patch("fullon_ohlcv_service.trade.live_collector.ExchangeQueue")
    @patch("fullon_ohlcv_service.trade.live_collector.get_admin_exchanges")
    @patch("fullon_ohlcv_service.trade.live_collector.GlobalTradeBatcher")
    @patch("fullon_ohlcv_service.trade.live_collector.ProcessCache")
    @pytest.mark.asyncio
    async def test_add_symbol_updates_all_state(
        self, mock_process_cache, mock_batcher_class, mock_get_admin_exchanges, mock_exchange_queue
    ):
        """Test start_symbol updates all required state: registered_symbols, process_ids."""
        collector = LiveTradeCollector()
        collector.running = True
        collector.registered_symbols = set()
        collector.symbols = []
        collector.process_ids = {}

        # Mock handler
        mock_handler = AsyncMock()
        mock_handler.subscribe_trades = AsyncMock(return_value=True)
        collector.websocket_handlers["binance"] = mock_handler

        # Mock admin exchanges
        mock_exchange = Mock()
        mock_exchange.cat_exchange.name = "binance"
        mock_get_admin_exchanges.return_value = (1, [mock_exchange])

        # Mock ExchangeQueue to return handler
        mock_exchange_queue.get_websocket_handler = AsyncMock(return_value=mock_handler)

        # Mock ProcessCache
        mock_cache = AsyncMock()
        mock_cache.register_process = AsyncMock(return_value="test_process_id")
        mock_process_cache.return_value.__aenter__.return_value = mock_cache

        # Mock batcher
        mock_batcher = AsyncMock()
        mock_batcher_class.return_value = mock_batcher

        # Create symbol to add
        mock_symbol = Mock(symbol="BTC/USDT")
        mock_symbol.cat_exchange.name = "binance"

        await collector.start_symbol(mock_symbol)

        # Verify all state was updated
        assert "binance:BTC/USDT" in collector.registered_symbols
        assert "binance:BTC/USDT" in collector.process_ids
        assert collector.process_ids["binance:BTC/USDT"] == "test_process_id"
