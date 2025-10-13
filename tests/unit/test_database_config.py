"""Tests for database-driven configuration module.

This tests the fullon_orm integration for loading exchanges and symbols
from the database, following the ticker service pattern.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch


@pytest.fixture
def mock_database_context():
    """Create a properly configured mock database context."""
    with patch("fullon_ohlcv_service.config.database_config.DatabaseContext") as mock_ctx:
        mock_db = MagicMock()  # Use MagicMock instead of AsyncMock for non-async methods
        mock_db.exchanges = MagicMock()
        mock_db.symbols = MagicMock()
        mock_db.session = MagicMock()
        # Only the actual async methods should be AsyncMock
        mock_db.exchanges.get_user_exchanges = AsyncMock()
        mock_db.exchanges.get_cat_exchanges = AsyncMock()
        mock_db.symbols.get_by_exchange_id = AsyncMock()
        mock_db.symbols.get_by_symbol = AsyncMock()
        mock_db.symbols.get_all = AsyncMock()
        # Mock session.execute for the fallback query
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = []  # Return empty list for fallback query
        mock_result.scalars.return_value = mock_scalars
        mock_db.session.execute = AsyncMock(return_value=mock_result)
        mock_ctx.return_value.__aenter__.return_value = mock_db
        yield mock_db


class TestGetCollectionTargets:
    """Tests for get_collection_targets function."""

    @pytest.mark.asyncio
    async def test_get_collection_targets_basic(self, mock_database_context):
        """Test basic functionality of getting collection targets from database."""
        mock_db = mock_database_context

        # Setup mock data with correct field names
        mock_exchanges = [
            {"cat_ex_id": 1, "ex_named": "kraken", "ex_id": 1},
            {"cat_ex_id": 2, "ex_named": "binance", "ex_id": 2},
            {"cat_ex_id": 3, "ex_named": "coinbase", "ex_id": 3},
        ]

        mock_kraken_symbols = [
            MagicMock(symbol="BTC/USD", active=True),
            MagicMock(symbol="ETH/USD", active=True),
            MagicMock(symbol="LTC/USD", active=False),  # Inactive, should be excluded
        ]

        mock_binance_symbols = [
            MagicMock(symbol="BTC/USDT", active=True),
            MagicMock(symbol="ETH/USDT", active=True),
        ]

        # Setup mock returns
        mock_db.exchanges.get_user_exchanges.return_value = mock_exchanges

        # Mock cat_exchanges (needed for name lookup)
        kraken_cat = MagicMock()
        kraken_cat.cat_ex_id = 1
        kraken_cat.name = "kraken"

        binance_cat = MagicMock()
        binance_cat.cat_ex_id = 2
        binance_cat.name = "binance"

        coinbase_cat = MagicMock()
        coinbase_cat.cat_ex_id = 3
        coinbase_cat.name = "coinbase"

        mock_cat_exchanges = [kraken_cat, binance_cat, coinbase_cat]
        mock_db.exchanges.get_cat_exchanges.return_value = mock_cat_exchanges

        # Mock symbols.get_all to return different symbols based on exchange_name
        def get_symbols_by_exchange_name(exchange_name=None, **kwargs):
            if exchange_name == "kraken":
                return mock_kraken_symbols
            elif exchange_name == "binance":
                return mock_binance_symbols
            else:  # coinbase or other
                return []

        mock_db.symbols.get_all.side_effect = get_symbols_by_exchange_name

        # Import and test the function
        from fullon_ohlcv_service.config.database_config import get_collection_targets

        targets = await get_collection_targets(user_id=1)

        # Assertions
        assert isinstance(targets, dict)
        assert len(targets) == 2  # Only exchanges with symbols
        assert "kraken" in targets
        assert "binance" in targets
        assert targets["kraken"]["symbols"] == ["BTC/USD", "ETH/USD"]
        assert targets["kraken"]["ex_id"] == 1
        assert targets["binance"]["symbols"] == ["BTC/USDT", "ETH/USDT"]
        assert targets["binance"]["ex_id"] == 2

        # Verify the correct database calls were made
        mock_db.exchanges.get_user_exchanges.assert_called_once_with(uid=1)
        mock_db.exchanges.get_cat_exchanges.assert_called_once_with(all=True)
        # get_all is called for each exchange with valid cat_ex_id
        assert mock_db.symbols.get_all.call_count == 3  # Called for kraken, binance, and coinbase

    @pytest.mark.asyncio
    async def test_get_collection_targets_empty_database(self, mock_database_context):
        """Test when database has no exchanges configured."""
        mock_db = mock_database_context

        # No exchanges in database
        mock_db.exchanges.get_user_exchanges.return_value = []

        from fullon_ohlcv_service.config.database_config import get_collection_targets

        targets = await get_collection_targets(user_id=1)

        # Should return empty dict
        assert targets == {}
        mock_db.exchanges.get_user_exchanges.assert_called_once_with(uid=1)
        mock_db.symbols.get_by_exchange_id.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_collection_targets_no_active_symbols(self, mock_database_context):
        """Test when exchange has no active symbols."""
        mock_db = mock_database_context

        mock_exchanges = [{"cat_ex_id": 1, "name": "kraken", "active": True}]

        # All symbols are inactive
        mock_symbols = [
            MagicMock(symbol="BTC/USD", active=False),
            MagicMock(symbol="ETH/USD", active=False),
        ]

        mock_db.exchanges.get_user_exchanges.return_value = mock_exchanges
        mock_db.symbols.get_by_exchange_id.return_value = mock_symbols

        from fullon_ohlcv_service.config.database_config import get_collection_targets

        targets = await get_collection_targets(user_id=1)

        # Exchange should not be included if it has no active symbols
        assert targets == {}

    @pytest.mark.asyncio
    async def test_get_collection_targets_different_user(self, mock_database_context):
        """Test getting targets for different user ID."""
        mock_db = mock_database_context

        mock_exchanges = [{"cat_ex_id": 1, "name": "kraken", "active": True}]

        mock_db.exchanges.get_user_exchanges.return_value = mock_exchanges
        mock_db.symbols.get_by_exchange_id.return_value = [MagicMock(symbol="BTC/USD", active=True)]

        from fullon_ohlcv_service.config.database_config import get_collection_targets

        targets = await get_collection_targets(user_id=42)

        # Should use the correct user_id
        mock_db.exchanges.get_user_exchanges.assert_called_once_with(uid=42)


class TestShouldCollectOhlcv:
    """Tests for should_collect_ohlcv function."""

    @pytest.mark.asyncio
    async def test_should_collect_ohlcv_normal_symbol(self, mock_database_context):
        """Test symbol that should have OHLCV collected."""
        mock_db = mock_database_context

        # Symbol exists and only_ticker is False
        mock_symbol = MagicMock(only_ticker=False)
        mock_db.symbols.get_by_symbol.return_value = mock_symbol

        from fullon_ohlcv_service.config.database_config import should_collect_ohlcv

        result = await should_collect_ohlcv("kraken", "BTC/USD")

        assert result is True
        mock_db.symbols.get_by_symbol.assert_called_once_with("BTC/USD")

    @pytest.mark.asyncio
    async def test_should_collect_ohlcv_ticker_only_symbol(self, mock_database_context):
        """Test symbol marked as ticker-only (no OHLCV collection)."""
        mock_db = mock_database_context

        # Symbol exists but only_ticker is True
        mock_symbol = MagicMock(only_ticker=True)
        mock_db.symbols.get_by_symbol.return_value = mock_symbol

        from fullon_ohlcv_service.config.database_config import should_collect_ohlcv

        result = await should_collect_ohlcv("kraken", "BTC/USD")

        assert result is False
        mock_db.symbols.get_by_symbol.assert_called_once_with("BTC/USD")

    @pytest.mark.asyncio
    async def test_should_collect_ohlcv_symbol_not_found(self, mock_database_context):
        """Test when symbol doesn't exist in database."""
        mock_db = mock_database_context

        # Symbol doesn't exist
        mock_db.symbols.get_by_symbol.return_value = None

        from fullon_ohlcv_service.config.database_config import should_collect_ohlcv

        result = await should_collect_ohlcv("kraken", "UNKNOWN/PAIR")

        assert result is False
        mock_db.symbols.get_by_symbol.assert_called_once_with("UNKNOWN/PAIR")

    @pytest.mark.asyncio
    async def test_should_collect_ohlcv_database_error(self, mock_database_context):
        """Test error handling when database query fails."""
        mock_db = mock_database_context

        # Database query raises exception
        mock_db.symbols.get_by_symbol.side_effect = Exception("Database connection failed")

        from fullon_ohlcv_service.config.database_config import should_collect_ohlcv

        # Should handle error gracefully
        with pytest.raises(Exception, match="Database connection failed"):
            await should_collect_ohlcv("kraken", "BTC/USD")


class TestIntegrationDatabaseConfig:
    """Integration tests with actual database (when available)."""

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_get_collection_targets_with_real_database(self, db_context):
        """Test with real database using factories for realistic test data."""
        from fullon_ohlcv_service.config.database_config import get_collection_targets
        from tests.factories import UserFactory, ExchangeFactory
        from fullon_orm.models import Symbol
        from sqlalchemy import text

        # Use factory to create realistic test data
        user = UserFactory()

        # First create the user
        db_context.session.add(user)
        await db_context.commit()

        # Create cat_exchanges entry manually (since it doesn't have a model/factory)
        # This is the parent table that Exchange.cat_ex_id references
        await db_context.session.execute(
            text(
                "INSERT INTO cat_exchanges (cat_ex_id, name) VALUES (2001, 'test_exchange_cat') ON CONFLICT DO NOTHING"
            )
        )
        await db_context.commit()

        # Create exchange with the factory, linking to the user and cat_exchange
        exchange = ExchangeFactory(
            uid=user.uid,
            cat_ex_id=2001,  # Links to the cat_exchanges entry we just created
            name="test_exchange",
        )
        db_context.session.add(exchange)
        await db_context.commit()

        # Create symbol manually with required fields
        symbol = Symbol(
            cat_ex_id=exchange.cat_ex_id,
            symbol="BTC/USD",
            base="BTC",  # Required NOT NULL field
            quote="USD",  # Required NOT NULL field
            only_ticker=False,  # Allow OHLCV collection
        )
        db_context.session.add(symbol)
        await db_context.commit()

        # Test the real function with real data, passing the test database context
        targets = await get_collection_targets(user_id=user.uid, db_context=db_context)

        # Verify the results from real database operations
        assert isinstance(targets, dict)
        assert len(targets) > 0, "Expected at least one exchange, got empty targets"

        # Get the first (and should be only) exchange key
        first_exchange_key = list(targets.keys())[0]
        assert symbol.symbol in targets[first_exchange_key]["symbols"]
        assert len(targets[first_exchange_key]["symbols"]) == 1


class TestLogging:
    """Test logging behavior of database config functions."""

    @pytest.mark.asyncio
    async def test_get_collection_targets_logging(self, mock_database_context):
        """Test that appropriate log messages are generated."""
        mock_exchanges = [
            {"cat_ex_id": 1, "ex_named": "kraken", "ex_id": 1},
            {"cat_ex_id": 2, "ex_named": "binance", "ex_id": 2},
        ]

        mock_kraken_symbols = [
            MagicMock(symbol="BTC/USD", active=True),
            MagicMock(symbol="ETH/USD", active=True),
        ]

        mock_binance_symbols = [
            MagicMock(symbol="BTC/USDT", active=True),
            MagicMock(symbol="ETH/USDT", active=True),
        ]

        with patch(
            "fullon_ohlcv_service.config.database_config.get_component_logger"
        ) as mock_logger:
            mock_db = mock_database_context
            mock_db.exchanges.get_user_exchanges.return_value = mock_exchanges

            # Mock cat_exchanges (needed for name lookup)
            kraken_cat = MagicMock()
            kraken_cat.cat_ex_id = 1
            kraken_cat.name = "kraken"

            binance_cat = MagicMock()
            binance_cat.cat_ex_id = 2
            binance_cat.name = "binance"

            mock_cat_exchanges = [kraken_cat, binance_cat]
            mock_db.exchanges.get_cat_exchanges.return_value = mock_cat_exchanges

            # Mock symbols.get_all to return different symbols based on exchange_name
            def get_symbols_by_exchange_name(exchange_name=None, **kwargs):
                if exchange_name == "kraken":
                    return mock_kraken_symbols
                elif exchange_name == "binance":
                    return mock_binance_symbols
                else:
                    return []

            mock_db.symbols.get_all.side_effect = get_symbols_by_exchange_name

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
            assert call_args[1]["exchanges"] == 2
            assert call_args[1]["total_symbols"] == 4  # 2 exchanges * 2 symbols each
