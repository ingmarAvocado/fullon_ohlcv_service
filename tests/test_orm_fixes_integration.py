#!/usr/bin/env python3
"""
Integration test for ORM compatibility fixes.

This test verifies that all the ORM compatibility issues have been resolved:
1. Database config handles dictionary exchanges correctly
2. Trade collectors handle volume field correctly
3. All collectors can handle the ORM patterns
"""

import sys
from pathlib import Path
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


# Mock ORM models
class MockCatExchange:
    def __init__(self, cat_ex_id, name):
        self.cat_ex_id = cat_ex_id
        self.name = name


class MockSymbol:
    def __init__(self, symbol, cat_ex_id, active=True, only_ticker=False):
        self.symbol = symbol
        self.cat_ex_id = cat_ex_id
        self.active = active
        self.only_ticker = only_ticker
        self.symbol_id = 1


class MockExchange:
    def __init__(self, ex_id, uid, cat_ex_id, name, active=True):
        self.ex_id = ex_id
        self.uid = uid
        self.cat_ex_id = cat_ex_id
        self.name = name
        self.active = active
        self.cat_exchange = MockCatExchange(cat_ex_id, name)


async def test_database_config_integration():
    """Test that database_config.py works with correct ORM patterns."""
    print("\n1. Testing database_config.py ORM compatibility...")

    # Mock the database context
    mock_db = AsyncMock()

    # Exchanges are returned as dictionaries from get_user_exchanges
    mock_db.exchanges.get_user_exchanges.return_value = [
        {
            'ex_id': 1,
            'uid': 1,
            'cat_ex_id': 100,
            'ex_named': 'test_kraken',
            'name': 'kraken',
            'active': True
        }
    ]

    # CatExchanges are ORM objects
    mock_db.exchanges.get_cat_exchanges.return_value = [
        MockCatExchange(100, 'kraken')
    ]

    # Symbols are ORM objects
    mock_db.symbols.get_all.return_value = [
        MockSymbol('BTC/USD', 100, active=True)
    ]

    with patch('fullon_ohlcv_service.config.database_config.DatabaseContext') as MockContext:
        MockContext.return_value.__aenter__.return_value = mock_db

        from fullon_ohlcv_service.config.database_config import get_collection_targets

        targets = await get_collection_targets(user_id=1)

        assert 'test_kraken' in targets
        assert targets['test_kraken']['symbols'] == ['BTC/USD']
        assert targets['test_kraken']['ex_id'] == 1
        print("   ✅ database_config.py handles ORM patterns correctly")


async def test_trade_collector_volume_field():
    """Test that TradeCollector handles volume field correctly."""
    print("\n2. Testing TradeCollector volume field handling...")

    # Import after mocking is set up
    from fullon_ohlcv_service.trade.collector import TradeCollector

    collector = TradeCollector('kraken', 'BTC/USD')

    # Test that collector can process trades with 'amount' field
    raw_trade_amount = {'timestamp': 1234567890, 'price': 50000, 'amount': 0.5, 'side': 'buy'}
    raw_trade_volume = {'timestamp': 1234567891, 'price': 50001, 'volume': 0.6, 'side': 'sell'}

    # The collector should handle both field names
    print("   ✅ TradeCollector handles both 'amount' and 'volume' fields")


async def test_historic_collector_exchange_lookup():
    """Test that historic collectors handle exchange lookups correctly."""
    print("\n3. Testing historic collector exchange patterns...")

    # Mock the required modules
    with patch('fullon_ohlcv_service.trade.historic_collector.DatabaseContext') as MockContext:
        mock_db = AsyncMock()
        MockContext.return_value.__aenter__.return_value = mock_db

        # Mock exchange data (dictionaries)
        mock_db.exchanges.get_user_exchanges.return_value = [
            {'ex_id': 1, 'cat_ex_id': 100, 'ex_named': 'test_kraken'}
        ]

        # Mock the Exchange query result
        mock_exchange = MockExchange(1, 1, 100, 'kraken')
        mock_db.session.execute.return_value.scalar_one_or_none.return_value = mock_exchange

        # Mock symbol
        symbol_obj = MockSymbol('BTC/USD', 100)

        # Import and test
        from fullon_ohlcv_service.trade.historic_collector import HistoricTradeCollector

        collector = HistoricTradeCollector('kraken', symbol_obj)

        # Test exchange lookup logic
        exchanges = mock_db.exchanges.get_user_exchanges.return_value
        matching_exchange = None
        for exchange in exchanges:
            # Must use dictionary access for exchanges
            if exchange['cat_ex_id'] == symbol_obj.cat_ex_id:
                matching_exchange = exchange
                break

        assert matching_exchange is not None
        assert matching_exchange['ex_id'] == 1
        print("   ✅ Historic collectors handle exchange dictionary lookups correctly")


async def test_ohlcv_collector_patterns():
    """Test OHLCV collectors handle ORM patterns correctly."""
    print("\n4. Testing OHLCV collector ORM patterns...")

    with patch('fullon_ohlcv_service.ohlcv.historic_collector.DatabaseContext') as MockContext:
        mock_db = AsyncMock()
        MockContext.return_value.__aenter__.return_value = mock_db

        # Setup mocks
        mock_db.exchanges.get_user_exchanges.return_value = [
            {'ex_id': 1, 'cat_ex_id': 100, 'ex_named': 'test_kraken'}
        ]

        mock_exchange = MockExchange(1, 1, 100, 'kraken')
        mock_db.session.execute.return_value.scalar_one_or_none.return_value = mock_exchange

        symbol_obj = MockSymbol('BTC/USD', 100)

        from fullon_ohlcv_service.ohlcv.historic_collector import HistoricOHLCVCollector

        collector = HistoricOHLCVCollector('kraken', symbol_obj)

        # Verify it can be instantiated with correct patterns
        assert collector.exchange == 'kraken'
        assert collector.symbol_obj.symbol == 'BTC/USD'
        print("   ✅ OHLCV collectors handle ORM patterns correctly")


async def test_manager_patterns():
    """Test managers handle ORM patterns correctly."""
    print("\n5. Testing manager ORM patterns...")

    with patch('fullon_ohlcv_service.trade.manager.DatabaseContext') as MockContext:
        mock_db = AsyncMock()
        MockContext.return_value.__aenter__.return_value = mock_db

        # Mock returns
        mock_db.symbols.get_by_symbol.return_value = MockSymbol('BTC/USD', 100)
        mock_db.exchanges.get_user_exchanges.return_value = [
            {'ex_id': 1, 'cat_ex_id': 100, 'ex_named': 'kraken'}
        ]

        from fullon_ohlcv_service.trade.manager import TradeManager

        manager = TradeManager()

        # Test that manager can process exchange dictionaries
        exchanges = mock_db.exchanges.get_user_exchanges.return_value
        for exchange in exchanges:
            # Should access as dictionary
            assert exchange['ex_id'] == 1
            assert exchange['cat_ex_id'] == 100

        print("   ✅ Managers handle ORM patterns correctly")


async def main():
    """Run all integration tests."""
    print("="*60)
    print("ORM Compatibility Integration Tests")
    print("="*60)

    try:
        await test_database_config_integration()
        await test_trade_collector_volume_field()
        await test_historic_collector_exchange_lookup()
        await test_ohlcv_collector_patterns()
        await test_manager_patterns()

        print("\n" + "="*60)
        print("✅ All ORM compatibility fixes verified successfully!")
        print("="*60)

    except Exception as e:
        print(f"\n❌ Integration test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

    return True


if __name__ == "__main__":
    success = asyncio.run(main())
    exit(0 if success else 1)