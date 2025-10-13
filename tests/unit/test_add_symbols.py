"""Unit tests for utils/add_symbols.py"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from fullon_ohlcv_service.utils.add_symbols import (
    add_all_symbols,
    add_symbol,
    add_symbols_for_exchange,
)


class TestAddSymbol:
    """Test cases for add_symbol function."""

    @pytest.mark.asyncio
    async def test_add_symbol_success(self):
        """Test successful symbol initialization."""
        with patch("fullon_ohlcv_service.utils.add_symbols.OHLCVBaseRepository") as mock_repo_class:
            mock_repo = AsyncMock()
            mock_repo_class.return_value.__aenter__.return_value = mock_repo
            mock_repo.init_symbol.return_value = True

            result = await add_symbol("binance", "BTC/USDT")

            assert result is True
            mock_repo_class.assert_called_once_with("binance", "BTC/USDT")
            mock_repo.init_symbol.assert_called_once()

    @pytest.mark.asyncio
    async def test_add_symbol_init_fails_but_continues(self):
        """Test symbol initialization fails but is considered successful."""
        with patch("fullon_ohlcv_service.utils.add_symbols.OHLCVBaseRepository") as mock_repo_class:
            mock_repo = AsyncMock()
            mock_repo_class.return_value.__aenter__.return_value = mock_repo
            mock_repo.init_symbol.side_effect = Exception("Init failed")

            result = await add_symbol("binance", "BTC/USDT")

            assert result is True  # Should be True due to exception handling
            mock_repo.init_symbol.assert_called_once()

    @pytest.mark.asyncio
    async def test_add_symbol_repository_failure(self):
        """Test symbol initialization fails at repository level."""
        with patch("fullon_ohlcv_service.utils.add_symbols.OHLCVBaseRepository") as mock_repo_class:
            mock_repo_class.side_effect = Exception("Repository creation failed")

            result = await add_symbol("binance", "BTC/USDT")

            assert result is False


class TestAddAllSymbols:
    """Test cases for add_all_symbols function."""

    @pytest.mark.asyncio
    async def test_add_all_symbols_with_provided_symbols(self):
        """Test add_all_symbols with provided symbol list."""
        mock_symbols = [
            MagicMock(symbol="BTC/USDT", exchange_name="binance"),
            MagicMock(symbol="ETH/USDT", exchange_name="binance"),
        ]

        with patch(
            "fullon_ohlcv_service.utils.add_symbols.add_symbol", new_callable=AsyncMock
        ) as mock_add_symbol:
            mock_add_symbol.return_value = True

            result = await add_all_symbols(symbols=mock_symbols)

            assert result is True
            assert mock_add_symbol.call_count == 2
            mock_add_symbol.assert_any_call(exchange="binance", symbol="BTC/USDT")
            mock_add_symbol.assert_any_call(exchange="binance", symbol="ETH/USDT")

    @pytest.mark.asyncio
    async def test_add_all_symbols_loads_from_database(self):
        """Test add_all_symbols loads symbols from database when none provided."""
        mock_symbols = [MagicMock(symbol="BTC/USDT", exchange_name="binance")]

        with (
            patch("fullon_ohlcv_service.utils.add_symbols.DatabaseContext") as mock_db_class,
            patch(
                "fullon_ohlcv_service.utils.add_symbols.add_symbol", new_callable=AsyncMock
            ) as mock_add_symbol,
        ):
            mock_db = AsyncMock()
            mock_db_class.return_value.__aenter__.return_value = mock_db
            mock_db.symbols.get_all.return_value = mock_symbols
            mock_add_symbol.return_value = True

            result = await add_all_symbols()

            assert result is True
            mock_db.symbols.get_all.assert_called_once()
            mock_add_symbol.assert_called_once_with(exchange="binance", symbol="BTC/USDT")

    @pytest.mark.asyncio
    async def test_add_all_symbols_no_symbols(self):
        """Test add_all_symbols with no symbols."""
        with patch("fullon_ohlcv_service.utils.add_symbols.DatabaseContext") as mock_db_class:
            mock_db = AsyncMock()
            mock_db_class.return_value.__aenter__.return_value = mock_db
            mock_db.symbols.get_all.return_value = []

            result = await add_all_symbols()

            assert result is False

    @pytest.mark.asyncio
    async def test_add_all_symbols_partial_failure(self):
        """Test add_all_symbols with some symbols failing."""
        mock_symbols = [
            MagicMock(symbol="BTC/USDT", cat_exchange=MagicMock(name="binance")),
            MagicMock(symbol="ETH/USDT", cat_exchange=MagicMock(name="binance")),
        ]

        with patch(
            "fullon_ohlcv_service.utils.add_symbols.add_symbol", new_callable=AsyncMock
        ) as mock_add_symbol:
            mock_add_symbol.side_effect = [True, False]  # First succeeds, second fails

            result = await add_all_symbols(symbols=mock_symbols)

            assert result is False  # Should fail because not all succeeded
            assert mock_add_symbol.call_count == 2

    @pytest.mark.asyncio
    async def test_add_all_symbols_database_error(self):
        """Test add_all_symbols handles database errors."""
        with patch("fullon_ohlcv_service.utils.add_symbols.DatabaseContext") as mock_db_class:
            mock_db_class.side_effect = Exception("Database connection failed")

            result = await add_all_symbols()

            assert result is False

    @pytest.mark.asyncio
    async def test_add_all_symbols_with_exchange_attribute(self):
        """Test add_all_symbols with symbols that have exchange_name attribute."""
        mock_symbols = [MagicMock(symbol="BTC/USDT", exchange_name="binance")]

        with patch(
            "fullon_ohlcv_service.utils.add_symbols.add_symbol", new_callable=AsyncMock
        ) as mock_add_symbol:
            mock_add_symbol.return_value = True

            result = await add_all_symbols(symbols=mock_symbols)

            assert result is True
            mock_add_symbol.assert_called_once_with(exchange="binance", symbol="BTC/USDT")


class TestAddSymbolsForExchange:
    """Test cases for add_symbols_for_exchange function."""

    @pytest.mark.asyncio
    async def test_add_symbols_for_exchange_success(self):
        """Test successful symbol initialization for an exchange."""
        mock_symbols = [MagicMock(symbol="BTC/USDT"), MagicMock(symbol="ETH/USDT")]

        with (
            patch("fullon_ohlcv_service.utils.add_symbols.DatabaseContext") as mock_db_class,
            patch(
                "fullon_ohlcv_service.utils.add_symbols.add_symbol", new_callable=AsyncMock
            ) as mock_add_symbol,
        ):
            mock_db = AsyncMock()
            mock_db_class.return_value.__aenter__.return_value = mock_db
            mock_db.symbols.get_by_exchange.return_value = mock_symbols
            mock_add_symbol.return_value = True

            result = await add_symbols_for_exchange("binance")

            assert result is True
            mock_db.symbols.get_by_exchange.assert_called_once_with("binance")
            assert mock_add_symbol.call_count == 2

    @pytest.mark.asyncio
    async def test_add_symbols_for_exchange_no_symbols(self):
        """Test add_symbols_for_exchange with no symbols for exchange."""
        with patch("fullon_ohlcv_service.utils.add_symbols.DatabaseContext") as mock_db_class:
            mock_db = AsyncMock()
            mock_db_class.return_value.__aenter__.return_value = mock_db
            mock_db.symbols.get_by_exchange.return_value = []

            result = await add_symbols_for_exchange("binance")

            assert result is False

    @pytest.mark.asyncio
    async def test_add_symbols_for_exchange_partial_failure(self):
        """Test add_symbols_for_exchange with some symbols failing."""
        mock_symbols = [MagicMock(symbol="BTC/USDT"), MagicMock(symbol="ETH/USDT")]

        with (
            patch("fullon_ohlcv_service.utils.add_symbols.DatabaseContext") as mock_db_class,
            patch(
                "fullon_ohlcv_service.utils.add_symbols.add_symbol", new_callable=AsyncMock
            ) as mock_add_symbol,
        ):
            mock_db = AsyncMock()
            mock_db_class.return_value.__aenter__.return_value = mock_db
            mock_db.symbols.get_by_exchange.return_value = mock_symbols
            mock_add_symbol.side_effect = [True, False]

            result = await add_symbols_for_exchange("binance")

            assert result is False
            assert mock_add_symbol.call_count == 2

    @pytest.mark.asyncio
    async def test_add_symbols_for_exchange_database_error(self):
        """Test add_symbols_for_exchange handles database errors."""
        with patch("fullon_ohlcv_service.utils.add_symbols.DatabaseContext") as mock_db_class:
            mock_db_class.side_effect = Exception("Database error")

            result = await add_symbols_for_exchange("binance")

            assert result is False
