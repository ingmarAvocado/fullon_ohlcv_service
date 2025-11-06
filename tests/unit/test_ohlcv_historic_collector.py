"""
Unit tests for ohlcv/historic_collector.py

Tests the HistoricOHLCVCollector class and its functionality.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, Mock, patch

from fullon_ohlcv_service.ohlcv.historic_collector import (
    HistoricOHLCVCollector,
    _format_time_remaining,
)
from fullon_ohlcv_service.ohlcv.utils import convert_to_candle_objects
from fullon_orm.models import Symbol, Exchange, CatExchange
from fullon_cache.process_cache import ProcessStatus, ProcessType


@pytest.fixture
def mock_symbol():
    """Create a mock Symbol object."""
    symbol = Mock(spec=Symbol)
    symbol.symbol = "BTC/USD"
    symbol.backtest = 30
    symbol.cat_ex_id = 1

    cat_exchange = Mock(spec=CatExchange)
    cat_exchange.name = "kraken"
    symbol.cat_exchange = cat_exchange

    return symbol


@pytest.fixture
def mock_exchange():
    """Create a mock Exchange object."""
    exchange = Mock(spec=Exchange)
    exchange.ex_id = 1
    exchange.cat_ex_id = 1

    cat_exchange = Mock(spec=CatExchange)
    cat_exchange.name = "kraken"
    exchange.cat_exchange = cat_exchange

    return exchange


@pytest.fixture
def collector():
    """Create HistoricOHLCVCollector instance."""
    return HistoricOHLCVCollector()


class TestProcessTracking:
    """Test process registration and status updates in HistoricOHLCVCollector."""

    @pytest.mark.asyncio
    async def test_registers_process_on_collection_start(
        self, collector, mock_exchange, mock_symbol
    ):
        """Test that process is registered when collection starts."""
        # Mock handler that supports OHLCV
        mock_handler = AsyncMock()
        mock_handler.needs_trades_for_ohlcv = Mock(return_value=False)
        mock_handler.get_ohlcv = AsyncMock(return_value=[])

        # Mock ProcessCache
        mock_cache = AsyncMock()
        mock_cache.register_process = AsyncMock(return_value="mock_process_id")
        mock_cache.update_process = AsyncMock()

        with (
            patch(
                "fullon_ohlcv_service.ohlcv.historic_collector.ExchangeQueue.get_rest_handler",
                return_value=mock_handler,
            ),
            patch(
                "fullon_ohlcv_service.ohlcv.historic_collector.ProcessCache",
                return_value=mock_cache,
            ) as mock_cache_class,
        ):
            mock_cache_class.return_value.__aenter__ = AsyncMock(return_value=mock_cache)
            mock_cache_class.return_value.__aexit__ = AsyncMock(return_value=None)

            # Run collection
            results = await collector._start_exchange_historic_collector(
                mock_exchange, [mock_symbol]
            )

            # Verify process registration
            mock_cache.register_process.assert_called_with(
                process_type=ProcessType.OHLCV,
                component="kraken:BTC/USD",
                params={"exchange": "kraken", "symbol": "BTC/USD", "type": "historic"},
                message="Starting historic OHLCV collection",
                status=ProcessStatus.STARTING,
            )

    @pytest.mark.asyncio
    async def test_updates_process_status_during_collection(
        self, collector, mock_exchange, mock_symbol
    ):
        """Test that process status is updated during collection."""
        # Mock handler that supports OHLCV
        mock_handler = AsyncMock()
        mock_handler.needs_trades_for_ohlcv = Mock(return_value=False)
        mock_handler.get_ohlcv = AsyncMock(return_value=[])

        # Mock ProcessCache
        mock_cache = AsyncMock()
        mock_cache.register_process = AsyncMock(return_value="mock_process_id")
        mock_cache.update_process = AsyncMock()

        with (
            patch(
                "fullon_ohlcv_service.ohlcv.historic_collector.ExchangeQueue.get_rest_handler",
                return_value=mock_handler,
            ),
            patch(
                "fullon_ohlcv_service.ohlcv.historic_collector.ProcessCache",
                return_value=mock_cache,
            ) as mock_cache_class,
        ):
            mock_cache_class.return_value.__aenter__ = AsyncMock(return_value=mock_cache)
            mock_cache_class.return_value.__aexit__ = AsyncMock(return_value=None)

            # Run collection
            results = await collector._start_exchange_historic_collector(
                mock_exchange, [mock_symbol]
            )

            # Verify status updates
            assert mock_cache.update_process.call_count >= 2  # RUNNING and COMPLETED

            # Check RUNNING update
            running_call = None
            completed_call = None
            for call in mock_cache.update_process.call_args_list:
                if "Collecting historical OHLCV data" in str(call):
                    running_call = call
                elif "Collected" in str(call) and "candles" in str(call):
                    completed_call = call

            assert running_call is not None, "RUNNING status update not found"
            assert completed_call is not None, "COMPLETED status update not found"

    @pytest.mark.asyncio
    async def test_updates_process_status_on_error(self, collector, mock_exchange, mock_symbol):
        """Test that process status is updated to FAILED on error."""
        # Mock handler that raises exception
        mock_handler = AsyncMock()
        mock_handler.needs_trades_for_ohlcv = Mock(return_value=False)
        mock_handler.get_ohlcv = AsyncMock(side_effect=Exception("Collection failed"))

        # Mock ProcessCache
        mock_cache = AsyncMock()
        mock_cache.register_process = AsyncMock(return_value="mock_process_id")
        mock_cache.update_process = AsyncMock()

        with (
            patch(
                "fullon_ohlcv_service.ohlcv.historic_collector.ExchangeQueue.get_rest_handler",
                return_value=mock_handler,
            ),
            patch(
                "fullon_ohlcv_service.ohlcv.historic_collector.ProcessCache",
                return_value=mock_cache,
            ) as mock_cache_class,
        ):
            mock_cache_class.return_value.__aenter__ = AsyncMock(return_value=mock_cache)
            mock_cache_class.return_value.__aexit__ = AsyncMock(return_value=None)

            # Run collection (should handle exception gracefully)
            results = await collector._start_exchange_historic_collector(
                mock_exchange, [mock_symbol]
            )

            # Verify FAILED status update
            failed_calls = [
                call for call in mock_cache.update_process.call_args_list if "Error:" in str(call)
            ]
            assert len(failed_calls) > 0, "FAILED status update not found"


class TestFormatTimeRemaining:
    """Test cases for _format_time_remaining utility function."""

    def test_format_seconds_only(self):
        """Test formatting seconds when less than 1 minute."""
        result = _format_time_remaining(45.5)
        assert result == "46 seconds"  # Rounded

    def test_format_minutes(self):
        """Test formatting when between 1 minute and 1 hour."""
        result = _format_time_remaining(3661.0)  # 61 minutes, but > 3600 so shows as hours
        assert result == "3661 seconds (1.0 hours)"

    def test_format_hours(self):
        """Test formatting when between 1 hour and 1 day."""
        result = _format_time_remaining(7200.0)  # 2 hours
        assert result == "7200 seconds (2.0 hours)"

    def test_format_days(self):
        """Test formatting when more than 1 day."""
        result = _format_time_remaining(172800.0)  # 2 days
        assert result == "172800 seconds (2.0 days)"

    def test_format_edge_cases(self):
        """Test edge cases around boundaries."""
        # Exactly 60 seconds (1 minute)
        result = _format_time_remaining(60.0)
        assert result == "60 seconds (1.0 minutes)"

        # Exactly 3600 seconds (1 hour)
        result = _format_time_remaining(3600.0)
        assert result == "3600 seconds (1.0 hours)"

        # Exactly 86400 seconds (1 day)
        result = _format_time_remaining(86400.0)
        assert result == "86400 seconds (1.0 days)"


class TestHistoricOHLCVCollectorCore:
    """Test cases for core HistoricOHLCVCollector functionality."""

    @pytest.mark.asyncio
    async def test_initialization(self):
        """Test collector initialization."""
        symbols = [Mock(symbol="BTC/USDT")]
        collector = HistoricOHLCVCollector(symbols=symbols)

        assert collector.symbols == symbols
        assert hasattr(collector, "_load_data")
        assert hasattr(collector, "start_collection")
        assert collector.running is False

    @pytest.mark.asyncio
    async def test_initialization_empty_symbols(self):
        """Test collector initialization with empty symbols."""
        collector = HistoricOHLCVCollector(symbols=[])

        assert collector.symbols == []
        assert collector.running is False
        assert hasattr(collector, "_load_data")
        assert hasattr(collector, "start_collection")

    @pytest.mark.asyncio
    async def test_start_collection_already_running(self):
        """Test start_collection when already running."""
        collector = HistoricOHLCVCollector(symbols=[])
        collector.running = True

        result = await collector.start_collection()

        assert result == {}

    @patch("fullon_ohlcv_service.ohlcv.historic_collector.DatabaseContext")
    @pytest.mark.asyncio
    async def test_load_data_success(self, mock_db_class):
        """Test successful data loading."""
        # Setup mock database context
        mock_db = AsyncMock()
        mock_db_class.return_value.__aenter__.return_value = mock_db

        # Mock admin user
        mock_db.users.get_user_id.return_value = 1

        # Mock exchanges
        mock_exchange = Mock()
        mock_exchange.cat_exchange.name = "binance"
        mock_db.exchanges.get_user_exchanges.return_value = [mock_exchange]

        # Mock symbols
        mock_symbol = Mock(symbol="BTC/USDT")
        mock_symbol.cat_exchange.name = "binance"
        mock_db.symbols.get_all.return_value = [mock_symbol]

        collector = HistoricOHLCVCollector(symbols=[mock_symbol])
        symbols_by_exchange, admin_exchanges = await collector._load_data()

        assert "binance" in symbols_by_exchange
        assert len(symbols_by_exchange["binance"]) == 1
        assert len(admin_exchanges) == 1
        mock_db.users.get_user_id.assert_called_once()
        mock_db.exchanges.get_user_exchanges.assert_called_once()
        mock_db.symbols.get_all.assert_called_once()

    @patch("fullon_ohlcv_service.ohlcv.historic_collector.DatabaseContext")
    @pytest.mark.asyncio
    async def test_load_data_no_admin_user(self, mock_db_class):
        """Test data loading when admin user is not found."""
        mock_db = AsyncMock()
        mock_db_class.return_value.__aenter__.return_value = mock_db
        mock_db.users.get_user_id.return_value = None

        collector = HistoricOHLCVCollector(symbols=[])

        with pytest.raises(ValueError, match="Admin user .* not found"):
            await collector._load_data()

    @patch("fullon_ohlcv_service.ohlcv.historic_collector.DatabaseContext")
    @pytest.mark.asyncio
    async def test_load_data_database_error(self, mock_db_class):
        """Test data loading with database error."""
        mock_db_class.side_effect = Exception("Database connection failed")

        collector = HistoricOHLCVCollector(symbols=[])

        with pytest.raises(Exception, match="Database connection failed"):
            await collector._load_data()

    @pytest.mark.asyncio
    async def test_start_collection_no_symbols(self):
        """Test start_collection with no symbols."""
        collector = HistoricOHLCVCollector(symbols=[])

        with patch.object(collector, "_load_data", new_callable=AsyncMock) as mock_load_data:
            mock_load_data.return_value = ({}, [])

            result = await collector.start_collection()

            mock_load_data.assert_called_once()

    @patch("fullon_ohlcv_service.ohlcv.historic_collector.ExchangeQueue")
    @patch("fullon_ohlcv_service.ohlcv.historic_collector.CandleRepository")
    @patch("fullon_ohlcv_service.ohlcv.historic_collector.ProcessCache")
    @pytest.mark.asyncio
    async def test_start_exchange_historic_collector(
        self, mock_process_cache, mock_candle_repo_class, mock_exchange_queue_class
    ):
        """Test starting collection for a specific exchange."""
        # Setup mocks
        mock_repo = AsyncMock()
        mock_candle_repo_class.return_value.__aenter__.return_value = mock_repo

        mock_queue = MagicMock()
        mock_exchange_queue_class.return_value = mock_queue

        mock_cache = MagicMock()
        mock_process_cache.return_value = mock_cache

        # Mock exchange and symbols
        mock_exchange = Mock()
        mock_exchange.cat_exchange.name = "binance"

        mock_symbol = Mock(symbol="BTC/USDT")
        mock_symbol.cat_exchange.name = "binance"

        collector = HistoricOHLCVCollector(symbols=[mock_symbol])

        # Mock the internal methods
        with patch.object(collector, "_load_data", new_callable=AsyncMock) as mock_load_data:
            mock_load_data.return_value = ({"binance": [mock_symbol]}, [mock_exchange])

            # Call start_collection
            result = await collector.start_collection()

            # Verify the flow
            mock_load_data.assert_called_once()

    def test_collector_attributes(self):
        """Test that collector has required attributes."""
        symbols = [Mock(symbol="BTC/USDT")]
        collector = HistoricOHLCVCollector(symbols=symbols)

        assert hasattr(collector, "symbols")
        assert hasattr(collector, "running")
        assert hasattr(collector, "start_collection")
        assert hasattr(collector, "_load_data")

    def test_extract_timestamp_from_list(self):
        """Test timestamp extraction from list format."""
        collector = HistoricOHLCVCollector()
        candle = [1640995200000, 50000, 51000, 49000, 50500, 100]
        result = collector._extract_timestamp(candle)
        assert result == 1640995200000

    def test_extract_timestamp_from_dict(self):
        """Test timestamp extraction from dict format."""
        collector = HistoricOHLCVCollector()
        candle = {"timestamp": 1640995200000, "open": 50000}
        result = collector._extract_timestamp(candle)
        assert result == 1640995200000

    def test_extract_timestamp_from_object(self):
        """Test timestamp extraction from object format."""
        collector = HistoricOHLCVCollector()
        candle = Mock()
        candle.timestamp = 1640995200000
        result = collector._extract_timestamp(candle)
        assert result == 1640995200000

    def test_convert_to_candle_objects_list_format(self):
        """Test conversion from list format candles."""
        raw_candles = [
            [1640995200000, 50000, 51000, 49000, 50500, 100],
            [1640995260000, 50500, 51500, 49500, 51000, 150],
        ]
        candles = convert_to_candle_objects(raw_candles)

        assert len(candles) == 2
        assert candles[0].open == 50000.0
        assert candles[0].high == 51000.0
        assert candles[0].low == 49000.0
        assert candles[0].close == 50500.0
        assert candles[0].vol == 100.0

    def test_convert_to_candle_objects_dict_format(self):
        """Test conversion from dict format candles."""
        raw_candles = [
            {
                "timestamp": 1640995200000,
                "open": 50000,
                "high": 51000,
                "low": 49000,
                "close": 50500,
                "volume": 100,
            }
        ]
        candles = convert_to_candle_objects(raw_candles)

        assert len(candles) == 1
        assert candles[0].open == 50000.0
        assert candles[0].high == 51000.0
        assert candles[0].low == 49000.0
        assert candles[0].close == 50500.0
        assert candles[0].vol == 100.0

    def test_convert_to_candle_objects_with_none_values(self):
        """Test conversion skips candles with None values."""
        raw_candles = [
            [1640995200000, 50000, 51000, 49000, 50500, 100],
            [1640995260000, None, 51500, 49500, 51000, 150],  # None open
        ]
        candles = convert_to_candle_objects(raw_candles)

        assert len(candles) == 1  # Second candle should be skipped

    def test_convert_to_candle_objects_milliseconds_timestamp(self):
        """Test conversion handles millisecond timestamps."""
        raw_candles = [
            [1640995200000, 50000, 51000, 49000, 50500, 100]  # milliseconds
        ]
        candles = convert_to_candle_objects(raw_candles)

        assert len(candles) == 1
        # Should convert milliseconds to seconds for datetime
        assert candles[0].timestamp.timestamp() == 1640995200.0
