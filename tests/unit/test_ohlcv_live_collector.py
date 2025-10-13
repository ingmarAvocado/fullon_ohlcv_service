"""Unit tests for ohlcv/live_collector.py"""

from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from fullon_ohlcv_service.ohlcv.live_collector import LiveOHLCVCollector


class TestLiveOHLCVCollector:
    """Test cases for LiveOHLCVCollector class."""

    @pytest.mark.asyncio
    async def test_initialization(self):
        """Test collector initialization."""
        symbols = [Mock(symbol="BTC/USDT")]
        collector = LiveOHLCVCollector(symbols=symbols)

        assert collector.symbols == symbols
        assert hasattr(collector, "_load_data")
        assert hasattr(collector, "start_collection")
        assert hasattr(collector, "stop_collection")
        assert collector.running is False
        assert collector.websocket_handlers == {}
        assert collector.registered_symbols == set()
        assert collector.last_candle_timestamps == {}
        assert collector.process_ids == {}

    @pytest.mark.asyncio
    async def test_initialization_empty_symbols(self):
        """Test collector initialization with empty symbols."""
        collector = LiveOHLCVCollector(symbols=[])

        assert collector.symbols == []
        assert collector.running is False
        assert collector.websocket_handlers == {}
        assert collector.registered_symbols == set()

    @pytest.mark.asyncio
    async def test_start_collection_already_running(self):
        """Test start_collection when already running."""
        collector = LiveOHLCVCollector(symbols=[])
        collector.running = True

        await collector.start_collection()

        assert collector.running is True

    @pytest.mark.asyncio
    async def test_stop_collection(self):
        """Test stop_collection."""
        collector = LiveOHLCVCollector(symbols=[])
        collector.running = True

        await collector.stop_collection()

        assert collector.running is False

    @patch("fullon_ohlcv_service.ohlcv.live_collector.get_admin_exchanges")
    @patch("fullon_ohlcv_service.ohlcv.live_collector.DatabaseContext")
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

        collector = LiveOHLCVCollector(symbols=[mock_symbol])
        symbols_by_exchange, admin_exchanges = await collector._load_data()

        assert "binance" in symbols_by_exchange
        assert len(symbols_by_exchange["binance"]) == 1
        assert len(admin_exchanges) == 1
        mock_get_admin_exchanges.assert_called_once()
        mock_db.symbols.get_all.assert_called_once()

    @patch("fullon_ohlcv_service.ohlcv.live_collector.get_admin_exchanges")
    @pytest.mark.asyncio
    async def test_load_data_no_admin_user(self, mock_get_admin_exchanges):
        """Test data loading when admin user is not found."""
        # Mock admin_helper to raise ValueError
        mock_get_admin_exchanges.side_effect = ValueError("Admin user admin@fullon not found")

        collector = LiveOHLCVCollector(symbols=[])

        with pytest.raises(ValueError, match="Admin user .* not found"):
            await collector._load_data()

    @patch("fullon_ohlcv_service.ohlcv.live_collector.DatabaseContext")
    @pytest.mark.asyncio
    async def test_load_data_database_error(self, mock_db_class):
        """Test data loading with database error."""
        mock_db_class.side_effect = Exception("Database connection failed")

        collector = LiveOHLCVCollector(symbols=[])

        with pytest.raises(Exception, match="Database connection failed"):
            await collector._load_data()

    @patch("fullon_ohlcv_service.ohlcv.live_collector.ExchangeQueue")
    @patch("fullon_ohlcv_service.ohlcv.live_collector.ProcessCache")
    @pytest.mark.asyncio
    async def test_start_exchange_collector_success(
        self, mock_process_cache, mock_exchange_queue_class
    ):
        """Test successful exchange collector startup."""
        # Setup mocks
        mock_handler = AsyncMock()
        mock_handler.needs_trades_for_ohlcv.return_value = False
        mock_handler.subscribe_ohlcv = AsyncMock(return_value=True)
        mock_exchange_queue_class.get_websocket_handler = AsyncMock(return_value=mock_handler)

        mock_cache = AsyncMock()
        mock_cache.register_process = AsyncMock(return_value="mock_process_id")
        mock_process_cache.return_value.__aenter__.return_value = mock_cache

        # Mock exchange and symbols
        mock_exchange = Mock()
        mock_exchange.cat_exchange.name = "binance"

        mock_symbol = Mock(symbol="BTC/USDT")
        mock_symbol.cat_exchange.name = "binance"

        collector = LiveOHLCVCollector(symbols=[mock_symbol])

        await collector._start_exchange_collector(mock_exchange, [mock_symbol])

        # Verify handler was obtained and stored
        assert "binance" in collector.websocket_handlers
        assert collector.websocket_handlers["binance"] == mock_handler

        # Verify symbol was registered
        assert "binance:BTC/USDT" in collector.registered_symbols

        # Verify process was registered
        assert "binance:BTC/USDT" in collector.process_ids
        assert collector.process_ids["binance:BTC/USDT"] == "mock_process_id"

        # Verify subscription was called
        mock_handler.subscribe_ohlcv.assert_called_once()

    @patch("fullon_ohlcv_service.ohlcv.live_collector.ExchangeQueue")
    @pytest.mark.asyncio
    async def test_start_exchange_collector_needs_trades(self, mock_exchange_queue_class):
        """Test exchange collector when exchange needs trades instead of OHLCV."""
        # Setup mocks
        mock_handler = AsyncMock()
        mock_handler.needs_trades_for_ohlcv.return_value = True
        mock_exchange_queue_class.get_websocket_handler = AsyncMock(return_value=mock_handler)

        # Mock exchange and symbols
        mock_exchange = Mock()
        mock_exchange.cat_exchange.name = "binance"

        mock_symbol = Mock(symbol="BTC/USDT")

        collector = LiveOHLCVCollector(symbols=[mock_symbol])

        await collector._start_exchange_collector(mock_exchange, [mock_symbol])

        # Verify handler was obtained but no subscription happened
        assert "binance" in collector.websocket_handlers
        mock_handler.subscribe_ohlcv.assert_not_called()

    @patch("fullon_ohlcv_service.ohlcv.live_collector.ExchangeQueue")
    @pytest.mark.asyncio
    async def test_start_exchange_collector_no_needs_trades_method(self, mock_exchange_queue_class):
        """Test exchange collector when handler doesn't have needs_trades_for_ohlcv method."""
        # Setup mocks
        mock_handler = AsyncMock()
        del mock_handler.needs_trades_for_ohlcv  # Remove the method
        mock_handler.subscribe_ohlcv = AsyncMock(return_value=True)
        mock_exchange_queue_class.get_websocket_handler = AsyncMock(return_value=mock_handler)

        # Mock exchange and symbols
        mock_exchange = Mock()
        mock_exchange.cat_exchange.name = "binance"

        mock_symbol = Mock(symbol="BTC/USDT")

        collector = LiveOHLCVCollector(symbols=[mock_symbol])

        await collector._start_exchange_collector(mock_exchange, [mock_symbol])

        # Verify subscription still happened (assumes supports OHLCV)
        mock_handler.subscribe_ohlcv.assert_called_once()

    @patch("fullon_ohlcv_service.ohlcv.live_collector.ExchangeQueue")
    @patch("fullon_ohlcv_service.ohlcv.live_collector.ProcessCache")
    @pytest.mark.asyncio
    async def test_start_exchange_collector_success(
        self, mock_process_cache, mock_exchange_queue_class
    ):
        """Test successful exchange collector startup."""
        # Setup mocks
        mock_handler = AsyncMock()
        mock_handler.needs_trades_for_ohlcv.return_value = False
        mock_handler.subscribe_ohlcv = AsyncMock(return_value=True)
        mock_exchange_queue_class.get_websocket_handler = AsyncMock(return_value=mock_handler)

        mock_cache = AsyncMock()
        mock_cache.register_process = AsyncMock(return_value="mock_process_id")
        mock_process_cache.return_value.__aenter__.return_value = mock_cache

        # Mock exchange and symbols
        mock_exchange = Mock()
        mock_exchange.cat_exchange.name = "binance"

        mock_symbol = Mock(symbol="BTC/USDT")

        collector = LiveOHLCVCollector(symbols=[mock_symbol])

        await collector._start_exchange_collector(mock_exchange, [mock_symbol])

        # Verify handler was stored but symbol was not registered due to failure
        assert "binance" in collector.websocket_handlers
        assert "binance:BTC/USDT" not in collector.registered_symbols

    def test_create_symbol_callback(self):
        """Test creation of symbol callback."""
        collector = LiveOHLCVCollector()
        callback = collector._create_symbol_callback("binance", "BTC/USDT")

        assert callable(callback)

    @patch("fullon_ohlcv_service.ohlcv.live_collector.CandleRepository")
    @patch("fullon_ohlcv_service.ohlcv.live_collector.ProcessCache")
    @pytest.mark.asyncio
    async def test_ohlcv_callback_new_candle(self, mock_process_cache, mock_candle_repo_class):
        """Test OHLCV callback with new candle data."""
        # Setup mocks
        mock_repo = AsyncMock()
        mock_repo.save_candles = AsyncMock(return_value=True)
        mock_candle_repo_class.return_value.__aenter__.return_value = mock_repo

        mock_cache = AsyncMock()
        mock_cache.update_process = AsyncMock()
        mock_process_cache.return_value.__aenter__.return_value = mock_cache

        collector = LiveOHLCVCollector()
        collector.process_ids["binance:BTC/USDT"] = "mock_process_id"

        callback = collector._create_symbol_callback("binance", "BTC/USDT")

        # Mock OHLCV data
        ohlcv_data = [[1640995200000, 50000, 51000, 49000, 50500, 100]]

        await callback(ohlcv_data)

        # Verify candle was saved
        mock_repo.save_candles.assert_called_once()
        saved_candle = mock_repo.save_candles.call_args[0][0][0]
        assert saved_candle.open == 50000
        assert saved_candle.close == 50500

        # Verify timestamp was tracked
        assert collector.last_candle_timestamps["binance:BTC/USDT"] is not None

        # Verify process was updated
        mock_cache.update_process.assert_called_once()

    @patch("fullon_ohlcv_service.ohlcv.live_collector.CandleRepository")
    @pytest.mark.asyncio
    async def test_ohlcv_callback_duplicate_candle(self, mock_candle_repo_class):
        """Test OHLCV callback with duplicate candle (same timestamp)."""
        # Setup mocks
        mock_repo = AsyncMock()
        mock_candle_repo_class.return_value.__aenter__.return_value = mock_repo

        collector = LiveOHLCVCollector()
        callback = collector._create_symbol_callback("binance", "BTC/USDT")

        # Mock OHLCV data
        ohlcv_data = [[1640995200000, 50000, 51000, 49000, 50500, 100]]

        # First call
        await callback(ohlcv_data)

        # Second call with same timestamp
        await callback(ohlcv_data)

        # Verify candle was only saved once
        mock_repo.save_candles.assert_called_once()

    @patch("fullon_ohlcv_service.ohlcv.live_collector.ProcessCache")
    @pytest.mark.asyncio
    async def test_ohlcv_callback_error_handling(self, mock_process_cache):
        """Test OHLCV callback error handling."""
        # Setup mocks
        mock_cache = AsyncMock()
        mock_cache.update_process = AsyncMock()
        mock_process_cache.return_value.__aenter__.return_value = mock_cache

        collector = LiveOHLCVCollector()
        collector.process_ids["binance:BTC/USDT"] = "mock_process_id"

        callback = collector._create_symbol_callback("binance", "BTC/USDT")

        # Mock invalid OHLCV data that will cause an error
        ohlcv_data = ["invalid_data"]

        await callback(ohlcv_data)

        # Verify process was updated with error status
        mock_cache.update_process.assert_called_once()
        call_args = mock_cache.update_process.call_args
        assert "Error:" in call_args[1]["message"]
        assert call_args[1]["status"].name == "ERROR"


class TestLiveOHLCVCollectorDynamicSymbols:
    """Test cases for dynamic symbol management in LiveOHLCVCollector."""

    @pytest.mark.asyncio
    async def test_is_collecting_symbol_active(self):
        """Test is_collecting returns True for active symbol."""
        collector = LiveOHLCVCollector()
        collector.registered_symbols = {"binance:BTC/USDT"}

        # Create mock symbol
        mock_symbol = Mock(symbol="BTC/USDT")
        mock_symbol.cat_exchange.name = "binance"

        assert collector.is_collecting(mock_symbol) is True

    @pytest.mark.asyncio
    async def test_is_collecting_symbol_not_active(self):
        """Test is_collecting returns False for inactive symbol."""
        collector = LiveOHLCVCollector()
        collector.registered_symbols = {"binance:BTC/USDT"}

        # Create mock symbol for different pair
        mock_symbol = Mock(symbol="ETH/USDT")
        mock_symbol.cat_exchange.name = "binance"

        assert collector.is_collecting(mock_symbol) is False

    @pytest.mark.asyncio
    async def test_add_symbol_collector_not_running(self):
        """Test add_symbol raises RuntimeError when collector not running."""
        collector = LiveOHLCVCollector()
        collector.running = False

        mock_symbol = Mock(symbol="BTC/USDT")
        mock_symbol.cat_exchange.name = "binance"

        with pytest.raises(RuntimeError, match="not running"):
            await collector.add_symbol(mock_symbol)

    @pytest.mark.asyncio
    async def test_add_symbol_already_collecting(self):
        """Test add_symbol returns early if symbol already collecting."""
        collector = LiveOHLCVCollector()
        collector.running = True
        collector.registered_symbols = {"binance:BTC/USDT"}

        mock_symbol = Mock(symbol="BTC/USDT")
        mock_symbol.cat_exchange.name = "binance"

        # Should return early without making any calls
        await collector.add_symbol(mock_symbol)

        # No exceptions raised, no changes made
        assert "binance:BTC/USDT" in collector.registered_symbols

    @patch("fullon_ohlcv_service.ohlcv.live_collector.get_admin_exchanges")
    @pytest.mark.asyncio
    async def test_add_symbol_handler_exists_new_symbol(self, mock_get_admin_exchanges):
        """Test add_symbol reuses handler for new symbol on existing exchange."""
        collector = LiveOHLCVCollector()
        collector.running = True
        collector.registered_symbols = {"binance:BTC/USDT"}
        collector.symbols = [Mock(symbol="BTC/USDT")]

        # Mock existing handler
        mock_handler = AsyncMock()
        mock_handler.needs_trades_for_ohlcv = Mock(return_value=False)
        mock_handler.subscribe_ohlcv = AsyncMock(return_value=True)
        collector.websocket_handlers["binance"] = mock_handler

        # Mock admin exchanges
        mock_exchange = Mock()
        mock_exchange.cat_exchange.name = "binance"
        mock_get_admin_exchanges.return_value = (1, [mock_exchange])

        # Create new symbol to add
        mock_symbol = Mock(symbol="ETH/USDT")
        mock_symbol.cat_exchange.name = "binance"

        with patch("fullon_ohlcv_service.ohlcv.live_collector.ProcessCache") as mock_process_cache:
            mock_cache = AsyncMock()
            mock_cache.register_process = AsyncMock(return_value="new_process_id")
            mock_process_cache.return_value.__aenter__.return_value = mock_cache

            await collector.add_symbol(mock_symbol)

        # Verify handler was reused (not created again)
        assert collector.websocket_handlers["binance"] == mock_handler

        # Verify subscription was called with new symbol
        mock_handler.subscribe_ohlcv.assert_called_once()
        call_args = mock_handler.subscribe_ohlcv.call_args
        assert call_args[0][0] == "ETH/USDT"
        assert call_args[0][1] == "1m"  # Default interval

        # Verify symbol was added to registered_symbols
        assert "binance:ETH/USDT" in collector.registered_symbols

        # Verify symbol was added to symbols list
        assert mock_symbol in collector.symbols

    @patch("fullon_ohlcv_service.ohlcv.live_collector.get_admin_exchanges")
    @pytest.mark.asyncio
    async def test_add_symbol_no_handler_creates_new(self, mock_get_admin_exchanges):
        """Test add_symbol creates new handler if none exists for exchange."""
        collector = LiveOHLCVCollector()
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
            await collector.add_symbol(mock_symbol)

            # Verify _start_exchange_collector was called with admin_exchange and symbol list
            mock_start_collector.assert_called_once()
            call_args = mock_start_collector.call_args
            assert call_args[0][0] == mock_exchange
            assert call_args[0][1] == [mock_symbol]

    @patch("fullon_ohlcv_service.ohlcv.live_collector.get_admin_exchanges")
    @pytest.mark.asyncio
    async def test_add_symbol_admin_exchange_not_found(self, mock_get_admin_exchanges):
        """Test add_symbol raises ValueError when admin exchange not found."""
        collector = LiveOHLCVCollector()
        collector.running = True

        # Mock admin exchanges (returning empty list or wrong exchange)
        mock_other_exchange = Mock()
        mock_other_exchange.cat_exchange.name = "kraken"
        mock_get_admin_exchanges.return_value = (1, [mock_other_exchange])

        # Create symbol for exchange that doesn't exist
        mock_symbol = Mock(symbol="BTC/USDT")
        mock_symbol.cat_exchange.name = "binance"

        with pytest.raises(ValueError, match="Admin exchange.*not found"):
            await collector.add_symbol(mock_symbol)

    @patch("fullon_ohlcv_service.ohlcv.live_collector.get_admin_exchanges")
    @patch("fullon_ohlcv_service.ohlcv.live_collector.ProcessCache")
    @pytest.mark.asyncio
    async def test_add_symbol_updates_all_state(
        self, mock_process_cache, mock_get_admin_exchanges
    ):
        """Test add_symbol updates all required state: registered_symbols, symbols, process_ids."""
        collector = LiveOHLCVCollector()
        collector.running = True
        collector.registered_symbols = set()
        collector.symbols = []
        collector.process_ids = {}

        # Mock handler
        mock_handler = AsyncMock()
        mock_handler.needs_trades_for_ohlcv = Mock(return_value=False)
        mock_handler.subscribe_ohlcv = AsyncMock(return_value=True)
        collector.websocket_handlers["binance"] = mock_handler

        # Mock admin exchanges
        mock_exchange = Mock()
        mock_exchange.cat_exchange.name = "binance"
        mock_get_admin_exchanges.return_value = (1, [mock_exchange])

        # Mock ProcessCache
        mock_cache = AsyncMock()
        mock_cache.register_process = AsyncMock(return_value="test_process_id")
        mock_process_cache.return_value.__aenter__.return_value = mock_cache

        # Create symbol to add
        mock_symbol = Mock(symbol="BTC/USDT")
        mock_symbol.cat_exchange.name = "binance"

        await collector.add_symbol(mock_symbol)

        # Verify all state was updated
        assert "binance:BTC/USDT" in collector.registered_symbols
        assert mock_symbol in collector.symbols
        assert "binance:BTC/USDT" in collector.process_ids
        assert collector.process_ids["binance:BTC/USDT"] == "test_process_id"

    @patch("fullon_ohlcv_service.ohlcv.live_collector.get_admin_exchanges")
    @pytest.mark.asyncio
    async def test_add_symbol_skips_if_exchange_needs_trades(self, mock_get_admin_exchanges):
        """Test add_symbol skips subscription if exchange needs trades for OHLCV."""
        collector = LiveOHLCVCollector()
        collector.running = True
        collector.registered_symbols = set()
        collector.symbols = []

        # Mock handler that needs trades instead of OHLCV
        mock_handler = AsyncMock()
        mock_handler.needs_trades_for_ohlcv = Mock(return_value=True)
        mock_handler.subscribe_ohlcv = AsyncMock()
        collector.websocket_handlers["binance"] = mock_handler

        # Mock admin exchanges
        mock_exchange = Mock()
        mock_exchange.cat_exchange.name = "binance"
        mock_get_admin_exchanges.return_value = (1, [mock_exchange])

        # Create symbol to add
        mock_symbol = Mock(symbol="BTC/USDT")
        mock_symbol.cat_exchange.name = "binance"

        await collector.add_symbol(mock_symbol)

        # Verify subscribe_ohlcv was NOT called
        mock_handler.subscribe_ohlcv.assert_not_called()

        # Symbol should NOT be registered since subscription was skipped
        assert "binance:BTC/USDT" not in collector.registered_symbols
