"""Unit tests for trade/historic_collector.py"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from fullon_ohlcv_service.trade.historic_collector import (
    HistoricTradeCollector,
    _format_time_remaining,
)


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


class TestHistoricTradeCollector:
    """Test cases for HistoricTradeCollector class."""

    @pytest.mark.asyncio
    async def test_initialization(self):
        """Test collector initialization."""
        symbols = [MagicMock(symbol="BTC/USDT")]
        collector = HistoricTradeCollector(symbols=symbols)

        assert collector.symbols == symbols
        assert hasattr(collector, "_load_data")
        assert hasattr(collector, "start_collection")
        assert collector.running is False

    @pytest.mark.asyncio
    async def test_initialization_empty_symbols(self):
        """Test collector initialization with empty symbols."""
        collector = HistoricTradeCollector(symbols=[])

        assert collector.symbols == []
        assert collector.running is False

    @patch("fullon_ohlcv_service.trade.historic_collector.DatabaseContext")
    @pytest.mark.asyncio
    async def test_load_data_success(self, mock_db_class):
        """Test successful data loading."""
        # Setup mock database context
        mock_db = AsyncMock()
        mock_db_class.return_value.__aenter__.return_value = mock_db

        # Mock admin user
        mock_db.users.get_user_id.return_value = 1

        # Mock exchanges
        mock_exchange = MagicMock()
        mock_exchange.cat_exchange.name = "binance"
        mock_db.exchanges.get_user_exchanges.return_value = [mock_exchange]

        # Mock symbols
        mock_symbol = MagicMock(symbol="BTC/USDT")
        mock_symbol.cat_exchange.name = "binance"
        mock_db.symbols.get_all.return_value = [mock_symbol]

        collector = HistoricTradeCollector(symbols=[mock_symbol])
        symbols_by_exchange, admin_exchanges = await collector._load_data()

        assert "binance" in symbols_by_exchange
        assert len(symbols_by_exchange["binance"]) == 1
        assert len(admin_exchanges) == 1
        mock_db.users.get_user_id.assert_called_once()
        mock_db.exchanges.get_user_exchanges.assert_called_once()
        mock_db.symbols.get_all.assert_called_once()

    @patch("fullon_ohlcv_service.trade.historic_collector.DatabaseContext")
    @pytest.mark.asyncio
    async def test_load_data_no_admin_user(self, mock_db_class):
        """Test data loading when admin user is not found."""
        mock_db = AsyncMock()
        mock_db_class.return_value.__aenter__.return_value = mock_db
        mock_db.users.get_user_id.return_value = None

        collector = HistoricTradeCollector(symbols=[])

        with pytest.raises(ValueError, match="Admin user .* not found"):
            await collector._load_data()

    @patch("fullon_ohlcv_service.trade.historic_collector.DatabaseContext")
    @pytest.mark.asyncio
    async def test_load_data_database_error(self, mock_db_class):
        """Test data loading with database error."""
        mock_db_class.side_effect = Exception("Database connection failed")

        collector = HistoricTradeCollector(symbols=[])

        with pytest.raises(Exception, match="Database connection failed"):
            await collector._load_data()

    @pytest.mark.asyncio
    async def test_start_collection_already_running(self):
        """Test start_collection when already running."""
        collector = HistoricTradeCollector(symbols=[])
        collector.running = True

        result = await collector.start_collection()

        assert result == {}

    def test_collector_attributes(self):
        """Test that collector has required attributes."""
        symbols = [MagicMock(symbol="BTC/USDT")]
        collector = HistoricTradeCollector(symbols=symbols)

        assert hasattr(collector, "symbols")
        assert hasattr(collector, "running")
        assert hasattr(collector, "start_collection")
        assert hasattr(collector, "_load_data")

    @patch("fullon_ohlcv_service.trade.historic_collector.ExchangeQueue")
    @patch("fullon_ohlcv_service.trade.historic_collector.TradeRepository")
    @patch("fullon_ohlcv_service.trade.historic_collector.ProcessCache")
    @pytest.mark.asyncio
    async def test_start_exchange_historic_collector(
        self, mock_process_cache, mock_trade_repo_class, mock_exchange_queue_class
    ):
        """Test starting collection for a specific exchange."""
        # Setup mocks
        mock_repo = AsyncMock()
        mock_trade_repo_class.return_value.__aenter__.return_value = mock_repo

        mock_queue = MagicMock()
        mock_exchange_queue_class.return_value = mock_queue

        mock_cache = MagicMock()
        mock_process_cache.return_value = mock_cache

        # Mock exchange and symbols
        mock_exchange = MagicMock()
        mock_exchange.name = "binance"
        mock_exchange.cat_ex_id = 1

        mock_symbol = MagicMock(symbol="BTC/USDT", cat_exchange=MagicMock(name="binance"))

        collector = HistoricTradeCollector(symbols=[mock_symbol])

        # Mock the internal methods
        with patch.object(collector, "_load_data", new_callable=AsyncMock) as mock_load_data:
            mock_load_data.return_value = ({"binance": [mock_symbol]}, [mock_exchange])

            # Call start_collection
            result = await collector.start_collection()

            # Verify the flow
            mock_load_data.assert_called_once()
            # The actual collection logic would be more complex, but we're testing the setup

    @pytest.mark.asyncio
    async def test_start_collection_no_symbols(self):
        """Test start_collection with no symbols."""
        collector = HistoricTradeCollector(symbols=[])

        with patch.object(collector, "_load_data", new_callable=AsyncMock) as mock_load_data:
            mock_load_data.return_value = ({}, [])

            result = await collector.start_collection()

            mock_load_data.assert_called_once()
