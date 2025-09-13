"""Tests for database-driven configuration module.

This tests the fullon_orm integration for loading exchanges and symbols
from the database, following the ticker service pattern.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone


class TestGetCollectionTargets:
    """Tests for get_collection_targets function."""

    @pytest.mark.asyncio
    async def test_get_collection_targets_basic(self):
        """Test basic functionality of getting collection targets from database."""
        # Setup mock data
        mock_exchanges = [
            {'cat_ex_id': 1, 'name': 'kraken', 'active': True},
            {'cat_ex_id': 2, 'name': 'binance', 'active': True},
            {'cat_ex_id': 3, 'name': 'coinbase', 'active': False}  # Inactive, should be excluded
        ]

        mock_kraken_symbols = [
            MagicMock(symbol='BTC/USD', active=True),
            MagicMock(symbol='ETH/USD', active=True),
            MagicMock(symbol='LTC/USD', active=False)  # Inactive, should be excluded
        ]

        mock_binance_symbols = [
            MagicMock(symbol='BTC/USDT', active=True),
            MagicMock(symbol='ETH/USDT', active=True)
        ]

        # Mock the DatabaseContext
        with patch('fullon_ohlcv_service.config.database_config.DatabaseContext') as mock_ctx:
            mock_db = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_db

            # Setup mock returns
            mock_db.exchanges.get_user_exchanges.return_value = mock_exchanges

            # Mock symbols.get_by_exchange_id to return different symbols based on exchange_id
            async def get_symbols_by_exchange(exchange_id):
                if exchange_id == 1:  # kraken
                    return mock_kraken_symbols
                elif exchange_id == 2:  # binance
                    return mock_binance_symbols
                else:
                    return []

            mock_db.symbols.get_by_exchange_id.side_effect = get_symbols_by_exchange

            # Import and test the function
            from fullon_ohlcv_service.config.database_config import get_collection_targets

            targets = await get_collection_targets(user_id=1)

            # Assertions
            assert isinstance(targets, dict)
            assert len(targets) == 2  # Only active exchanges
            assert 'kraken' in targets
            assert 'binance' in targets
            assert 'coinbase' not in targets  # Inactive exchange excluded

            # Check kraken symbols (LTC/USD should be excluded as inactive)
            assert targets['kraken'] == ['BTC/USD', 'ETH/USD']

            # Check binance symbols
            assert targets['binance'] == ['BTC/USDT', 'ETH/USDT']

            # Verify correct database calls
            mock_db.exchanges.get_user_exchanges.assert_called_once_with(user_id=1)
            assert mock_db.symbols.get_by_exchange_id.call_count == 2  # Only for active exchanges

    @pytest.mark.asyncio
    async def test_get_collection_targets_empty_database(self):
        """Test when database has no exchanges configured."""
        with patch('fullon_ohlcv_service.config.database_config.DatabaseContext') as mock_ctx:
            mock_db = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_db

            # No exchanges in database
            mock_db.exchanges.get_user_exchanges.return_value = []

            from fullon_ohlcv_service.config.database_config import get_collection_targets

            targets = await get_collection_targets(user_id=1)

            # Should return empty dict
            assert targets == {}
            mock_db.exchanges.get_user_exchanges.assert_called_once_with(user_id=1)
            mock_db.symbols.get_by_exchange_id.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_collection_targets_no_active_symbols(self):
        """Test when exchange has no active symbols."""
        mock_exchanges = [
            {'cat_ex_id': 1, 'name': 'kraken', 'active': True}
        ]

        # All symbols are inactive
        mock_symbols = [
            MagicMock(symbol='BTC/USD', active=False),
            MagicMock(symbol='ETH/USD', active=False)
        ]

        with patch('fullon_ohlcv_service.config.database_config.DatabaseContext') as mock_ctx:
            mock_db = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_db

            mock_db.exchanges.get_user_exchanges.return_value = mock_exchanges
            mock_db.symbols.get_by_exchange_id.return_value = mock_symbols

            from fullon_ohlcv_service.config.database_config import get_collection_targets

            targets = await get_collection_targets(user_id=1)

            # Exchange should not be included if it has no active symbols
            assert targets == {}

    @pytest.mark.asyncio
    async def test_get_collection_targets_different_user(self):
        """Test getting targets for different user ID."""
        mock_exchanges = [
            {'cat_ex_id': 1, 'name': 'kraken', 'active': True}
        ]

        with patch('fullon_ohlcv_service.config.database_config.DatabaseContext') as mock_ctx:
            mock_db = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_db

            mock_db.exchanges.get_user_exchanges.return_value = mock_exchanges
            mock_db.symbols.get_by_exchange_id.return_value = [
                MagicMock(symbol='BTC/USD', active=True)
            ]

            from fullon_ohlcv_service.config.database_config import get_collection_targets

            targets = await get_collection_targets(user_id=42)

            # Should use the correct user_id
            mock_db.exchanges.get_user_exchanges.assert_called_once_with(user_id=42)


class TestShouldCollectOhlcv:
    """Tests for should_collect_ohlcv function."""

    @pytest.mark.asyncio
    async def test_should_collect_ohlcv_normal_symbol(self):
        """Test symbol that should have OHLCV collected."""
        with patch('fullon_ohlcv_service.config.database_config.DatabaseContext') as mock_ctx:
            mock_db = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_db

            # Symbol exists and only_ticker is False
            mock_symbol = MagicMock(only_ticker=False)
            mock_db.symbols.get_by_symbol.return_value = mock_symbol

            from fullon_ohlcv_service.config.database_config import should_collect_ohlcv

            result = await should_collect_ohlcv('kraken', 'BTC/USD')

            assert result is True
            mock_db.symbols.get_by_symbol.assert_called_once_with('BTC/USD')

    @pytest.mark.asyncio
    async def test_should_collect_ohlcv_ticker_only_symbol(self):
        """Test symbol marked as ticker-only (no OHLCV collection)."""
        with patch('fullon_ohlcv_service.config.database_config.DatabaseContext') as mock_ctx:
            mock_db = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_db

            # Symbol exists but only_ticker is True
            mock_symbol = MagicMock(only_ticker=True)
            mock_db.symbols.get_by_symbol.return_value = mock_symbol

            from fullon_ohlcv_service.config.database_config import should_collect_ohlcv

            result = await should_collect_ohlcv('kraken', 'BTC/USD')

            assert result is False
            mock_db.symbols.get_by_symbol.assert_called_once_with('BTC/USD')

    @pytest.mark.asyncio
    async def test_should_collect_ohlcv_symbol_not_found(self):
        """Test when symbol doesn't exist in database."""
        with patch('fullon_ohlcv_service.config.database_config.DatabaseContext') as mock_ctx:
            mock_db = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_db

            # Symbol doesn't exist
            mock_db.symbols.get_by_symbol.return_value = None

            from fullon_ohlcv_service.config.database_config import should_collect_ohlcv

            result = await should_collect_ohlcv('kraken', 'UNKNOWN/PAIR')

            assert result is False
            mock_db.symbols.get_by_symbol.assert_called_once_with('UNKNOWN/PAIR')

    @pytest.mark.asyncio
    async def test_should_collect_ohlcv_database_error(self):
        """Test error handling when database query fails."""
        with patch('fullon_ohlcv_service.config.database_config.DatabaseContext') as mock_ctx:
            mock_db = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_db

            # Database query raises exception
            mock_db.symbols.get_by_symbol.side_effect = Exception("Database connection failed")

            from fullon_ohlcv_service.config.database_config import should_collect_ohlcv

            # Should handle error gracefully
            with pytest.raises(Exception, match="Database connection failed"):
                await should_collect_ohlcv('kraken', 'BTC/USD')


class TestIntegrationDatabaseConfig:
    """Integration tests with actual database (when available)."""

    @pytest.mark.asyncio
    @pytest.mark.integration
    @pytest.mark.skip(reason="Requires database with proper ORM setup")
    async def test_get_collection_targets_with_real_database(self, db_session):
        """Test with real database connection (requires test database).

        This test is skipped by default as it requires a properly configured
        test database with all fullon_orm models set up. The test demonstrates
        how the functions work with real database data.
        """
        # This test would require proper database setup with fullon_orm models
        # It's provided as documentation for how the integration would work
        pass


class TestLogging:
    """Test logging behavior of database config functions."""

    @pytest.mark.asyncio
    async def test_get_collection_targets_logging(self):
        """Test that appropriate log messages are generated."""
        mock_exchanges = [
            {'cat_ex_id': 1, 'name': 'kraken', 'active': True},
            {'cat_ex_id': 2, 'name': 'binance', 'active': True}
        ]

        mock_symbols = [
            MagicMock(symbol='BTC/USD', active=True),
            MagicMock(symbol='ETH/USD', active=True)
        ]

        with patch('fullon_ohlcv_service.config.database_config.DatabaseContext') as mock_ctx, \
             patch('fullon_ohlcv_service.config.database_config.get_component_logger') as mock_logger:

            mock_db = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_db
            mock_db.exchanges.get_user_exchanges.return_value = mock_exchanges
            mock_db.symbols.get_by_exchange_id.return_value = mock_symbols

            logger_instance = MagicMock()
            mock_logger.return_value = logger_instance

            from fullon_ohlcv_service.config.database_config import get_collection_targets

            targets = await get_collection_targets(user_id=1)

            # Check logger was created with correct component name
            mock_logger.assert_called_once_with("fullon.ohlcv.config")

            # Check info log was called with configuration summary
            logger_instance.info.assert_called_once()
            call_args = logger_instance.info.call_args
            assert call_args[0][0] == "Loaded configuration"
            assert call_args[1]['exchanges'] == 2
            assert call_args[1]['total_symbols'] == 4  # 2 exchanges * 2 symbols each