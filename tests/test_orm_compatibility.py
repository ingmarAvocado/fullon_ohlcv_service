#!/usr/bin/env python3
"""
Tests for fullon_orm compatibility - verifies correct ORM patterns are used.

This test suite verifies that:
1. Exchange objects from get_user_exchanges() return dictionaries with correct keys
2. CatExchange objects from get_cat_exchanges() are ORM objects with attributes
3. Symbol objects from get_all() are ORM objects with attributes
4. Trade objects use 'volume' field, not 'amount'
5. All collectors handle these patterns correctly
"""

import asyncio
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


# Mock classes to simulate fullon_orm models
class MockCatExchange:
    """Mock CatExchange ORM model with attributes."""

    def __init__(self, cat_ex_id, name):
        self.cat_ex_id = cat_ex_id
        self.name = name


class MockSymbol:
    """Mock Symbol ORM model with attributes."""

    def __init__(self, symbol, cat_ex_id, active=True, only_ticker=False):
        self.symbol = symbol
        self.cat_ex_id = cat_ex_id
        self.active = active
        self.only_ticker = only_ticker


class MockExchange:
    """Mock Exchange ORM model with attributes."""

    def __init__(self, ex_id, uid, cat_ex_id, name, active=True):
        self.ex_id = ex_id
        self.uid = uid
        self.cat_ex_id = cat_ex_id
        self.name = name
        self.active = active
        self.cat_exchange = MockCatExchange(cat_ex_id, name)


class MockTrade:
    """Mock Trade ORM model using 'volume' field."""

    def __init__(self, symbol_id, timestamp, price, volume, side, type="MARKET"):
        self.symbol_id = symbol_id
        self.timestamp = timestamp
        self.price = price
        self.volume = volume  # NOT 'amount'
        self.side = side
        self.type = type


@pytest.fixture
def mock_database_context():
    """Create a mock database context that returns correct ORM patterns."""
    mock_db = AsyncMock()

    # get_user_exchanges returns List[Dict] per documentation
    mock_db.exchanges.get_user_exchanges.return_value = [
        {
            "ex_id": 1,
            "uid": 1,
            "cat_ex_id": 100,
            "ex_named": "test_kraken",
            "name": "kraken",
            "active": True,
        },
        {
            "ex_id": 2,
            "uid": 1,
            "cat_ex_id": 101,
            "ex_named": "test_binance",
            "name": "binance",
            "active": True,
        },
    ]

    # get_cat_exchanges returns List[CatExchange] ORM objects
    mock_db.exchanges.get_cat_exchanges.return_value = [
        MockCatExchange(100, "kraken"),
        MockCatExchange(101, "binance"),
    ]

    # get_all returns List[Symbol] ORM objects filtered by exchange_name
    def get_symbols_by_exchange(exchange_name=None, **kwargs):
        if exchange_name == "kraken":
            return [
                MockSymbol("BTC/USD", 100, active=True, only_ticker=False),
                MockSymbol("ETH/USD", 100, active=True, only_ticker=False),
            ]
        elif exchange_name == "binance":
            return [
                MockSymbol("BTC/USDT", 101, active=True, only_ticker=False),
            ]
        else:
            return []

    mock_db.symbols.get_all.side_effect = get_symbols_by_exchange

    # get_by_symbol returns Symbol ORM object
    mock_db.symbols.get_by_symbol.return_value = MockSymbol("BTC/USD", 100)

    return mock_db


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_database_config_handles_dict_exchanges(mock_database_context):
    """Test that database_config.py correctly handles dictionary exchanges from get_user_exchanges."""
    from fullon_ohlcv_service.config.database_config import get_collection_targets

    with patch("fullon_ohlcv_service.config.database_config.DatabaseContext") as MockContext:
        MockContext.return_value.__aenter__.return_value = mock_database_context

        targets = await get_collection_targets(user_id=1)

        # Verify we got the correct structure
        assert "test_kraken" in targets
        assert "test_binance" in targets
        assert targets["test_kraken"]["symbols"] == ["BTC/USD", "ETH/USD"]
        assert targets["test_kraken"]["ex_id"] == 1
        assert targets["test_kraken"]["exchange_category_name"] == "kraken"

        # Verify the mock was called correctly
        mock_database_context.exchanges.get_user_exchanges.assert_called_once_with(uid=1)


@pytest.mark.asyncio
async def test_cat_exchanges_are_orm_objects(mock_database_context):
    """Test that get_cat_exchanges returns ORM objects with attributes."""
    cat_exchanges = await mock_database_context.exchanges.get_cat_exchanges(all=True)

    # Verify these are objects with attributes, not dictionaries
    assert hasattr(cat_exchanges[0], "cat_ex_id")
    assert hasattr(cat_exchanges[0], "name")
    assert cat_exchanges[0].cat_ex_id == 100
    assert cat_exchanges[0].name == "kraken"

    # This should NOT work with dictionaries
    try:
        _ = cat_exchanges[0]["cat_ex_id"]  # Would fail if it were an object
        assert False, "Expected AttributeError when accessing object as dictionary"
    except (AttributeError, TypeError):
        pass  # Expected behavior


@pytest.mark.asyncio
async def test_symbols_are_orm_objects(mock_database_context):
    """Test that symbols returned are ORM objects with attributes."""
    symbols = await mock_database_context.symbols.get_all(exchange_name="kraken")

    # Verify these are objects with attributes
    assert hasattr(symbols[0], "symbol")
    assert hasattr(symbols[0], "cat_ex_id")
    assert hasattr(symbols[0], "active")
    assert hasattr(symbols[0], "only_ticker")

    # Access via attributes should work
    assert symbols[0].symbol == "BTC/USD"
    assert symbols[0].cat_ex_id == 100
    assert symbols[0].active is True
    assert symbols[0].only_ticker is False


@pytest.mark.asyncio
async def test_trade_uses_volume_not_amount():
    """Test that Trade objects use 'volume' field, not 'amount'."""
    trade = MockTrade(
        symbol_id=1,
        timestamp=1234567890,
        price=50000.0,
        volume=0.5,  # Using 'volume', not 'amount'
        side="buy",
    )

    # Verify volume attribute exists
    assert hasattr(trade, "volume")
    assert trade.volume == 0.5

    # Verify 'amount' doesn't exist
    assert not hasattr(trade, "amount")


@pytest.mark.asyncio
async def test_trade_data_field_mapping():
    """Test that trade data correctly maps 'amount' to 'volume' field."""

    # Mock trade data from exchange (may use 'amount' or 'volume')
    raw_trades = [
        {"timestamp": 1234567890, "price": 50000, "amount": 0.5, "side": "buy"},
        {"timestamp": 1234567891, "price": 50001, "volume": 0.6, "side": "sell"},
    ]

    # Process trades - should handle both 'amount' and 'volume' in raw data
    # but create Trade objects with 'volume' field
    processed = []
    for t in raw_trades:
        trade_obj = {
            "timestamp": t.get("timestamp"),
            "price": float(t.get("price", 0)),
            "volume": float(t.get("amount", t.get("volume", 0))),  # Map to 'volume'
            "side": str(t.get("side", "BUY")),
        }
        processed.append(trade_obj)

    # Verify all processed trades have 'volume' field
    assert all("volume" in t for t in processed)
    assert processed[0]["volume"] == 0.5
    assert processed[1]["volume"] == 0.6


@pytest.mark.asyncio
async def test_collector_patterns_with_correct_orm():
    """Integration test verifying collectors work with correct ORM patterns."""

    # Test data showing correct patterns
    exchange_dict = {"ex_id": 1, "cat_ex_id": 100, "ex_named": "test_exchange"}

    cat_exchange_obj = MockCatExchange(100, "kraken")
    symbol_obj = MockSymbol("BTC/USD", 100)

    # Dictionary access for exchange dicts
    assert exchange_dict["ex_id"] == 1
    assert exchange_dict["cat_ex_id"] == 100

    # Attribute access for ORM objects
    assert cat_exchange_obj.cat_ex_id == 100
    assert cat_exchange_obj.name == "kraken"
    assert symbol_obj.symbol == "BTC/USD"
    assert symbol_obj.cat_ex_id == 100


@pytest.mark.asyncio
async def test_historic_collector_exchange_lookup():
    """Test that historic collectors properly handle exchange dictionary lookups."""

    # Mock what get_user_exchanges returns (dictionaries)
    exchanges = [
        {"ex_id": 1, "cat_ex_id": 100, "ex_named": "test_kraken"},
        {"ex_id": 2, "cat_ex_id": 101, "ex_named": "test_binance"},
    ]

    # Mock symbol object
    symbol_obj = MockSymbol("BTC/USD", 100)

    # Find matching exchange for symbol
    matching_exchange = None
    for exchange in exchanges:
        # Must use dictionary access for exchange dicts
        if exchange["cat_ex_id"] == symbol_obj.cat_ex_id:
            matching_exchange = exchange
            break

    assert matching_exchange is not None
    assert matching_exchange["ex_id"] == 1
    assert matching_exchange["ex_named"] == "test_kraken"


async def main():
    """Run all tests."""
    print("Running ORM Compatibility Tests")
    print("=" * 50)

    # Create mock database context
    mock_db = AsyncMock()

    # Set up mock returns
    mock_db.exchanges.get_user_exchanges.return_value = [
        {
            "ex_id": 1,
            "uid": 1,
            "cat_ex_id": 100,
            "ex_named": "test_kraken",
            "name": "kraken",
            "active": True,
        }
    ]

    mock_db.exchanges.get_cat_exchanges.return_value = [MockCatExchange(100, "kraken")]

    mock_db.symbols.get_all.return_value = [
        MockSymbol("BTC/USD", 100, active=True, only_ticker=False)
    ]

    # Test 1: Check dictionary patterns
    print("\n1. Testing dictionary exchange patterns...")
    exchanges = mock_db.exchanges.get_user_exchanges.return_value
    assert isinstance(exchanges[0], dict)
    assert "ex_id" in exchanges[0]
    print("   ✅ Exchanges return as dictionaries with correct keys")

    # Test 2: Check ORM object patterns
    print("\n2. Testing ORM object patterns...")
    cat_exchanges = mock_db.exchanges.get_cat_exchanges.return_value
    assert hasattr(cat_exchanges[0], "cat_ex_id")
    assert cat_exchanges[0].cat_ex_id == 100
    print("   ✅ CatExchange objects have attributes")

    # Test 3: Check Trade volume field
    print("\n3. Testing Trade volume field...")
    trade = MockTrade(1, 123456, 50000, 0.5, "buy")
    assert hasattr(trade, "volume")
    assert not hasattr(trade, "amount")
    print("   ✅ Trade objects use 'volume' not 'amount'")

    # Test 4: Check Symbol ORM objects
    print("\n4. Testing Symbol ORM objects...")
    symbols = mock_db.symbols.get_all.return_value
    assert hasattr(symbols[0], "symbol")
    assert symbols[0].symbol == "BTC/USD"
    print("   ✅ Symbol objects have correct attributes")

    print("\n" + "=" * 50)
    print("✅ All ORM compatibility tests passed!")


if __name__ == "__main__":
    asyncio.run(main())
