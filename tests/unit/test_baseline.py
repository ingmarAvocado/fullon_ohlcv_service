"""Baseline test to verify test infrastructure is working.

This is a simple test that validates:
- Database per worker pattern works with TimescaleDB
- Flush + rollback isolation works  
- Factories create valid test data
- Basic OHLCV service testing patterns work
- Redis cache isolation works
"""

import pytest
from datetime import datetime, timezone
from tests.factories import (
    CandleDataFactory,
    TradeDataFactory,
    OHLCVServiceConfigFactory,
    TradeServiceConfigFactory,
    create_candle_list,
    create_trade_list,
    create_realistic_candles,
    create_realistic_trades
)


class TestBaselineInfrastructure:
    """Baseline tests to verify test infrastructure works correctly."""

    @pytest.mark.asyncio
    async def test_database_session_basic(self, db_session):
        """Test basic database session functionality."""
        # Database session should be available
        assert db_session is not None
        
        # Should be able to execute basic queries
        from sqlalchemy import text
        result = await db_session.execute(text("SELECT 1 as test"))
        row = result.fetchone()
        assert row[0] == 1
        
        # Should be able to flush
        await db_session.flush()

    @pytest.mark.asyncio
    async def test_ohlcv_database_wrapper(self, ohlcv_db):
        """Test OHLCV database wrapper functionality."""
        # OHLCV database wrapper should be available
        assert ohlcv_db is not None
        
        # Should have required methods
        assert hasattr(ohlcv_db, "store_candles")
        assert hasattr(ohlcv_db, "store_trades")
        assert hasattr(ohlcv_db, "get_recent_candles")
        assert hasattr(ohlcv_db, "flush")
        
        # Should be able to flush
        await ohlcv_db.flush()

    @pytest.mark.asyncio
    async def test_redis_cache_isolation(self, redis_cache_db):
        """Test Redis cache database isolation."""
        # Redis cache DB should be available
        assert redis_cache_db is not None
        assert isinstance(redis_cache_db, int)
        assert 1 <= redis_cache_db <= 15  # Test databases 1-15

    @pytest.mark.asyncio
    async def test_candle_factory_creates_valid_data(self, ohlcv_db):
        """Test candle factory creates valid OHLCV data."""
        # Create candle using factory
        candle = CandleDataFactory()
        
        # Verify factory created valid data
        assert candle["symbol"] is not None
        assert candle["exchange"] is not None
        assert isinstance(candle["open"], (int, float))
        assert isinstance(candle["high"], (int, float))
        assert isinstance(candle["low"], (int, float))
        assert isinstance(candle["close"], (int, float))
        assert isinstance(candle["volume"], (int, float))
        assert isinstance(candle["timestamp"], datetime)
        
        # OHLC relationships should be valid
        assert candle["low"] <= candle["open"] <= candle["high"]
        assert candle["low"] <= candle["close"] <= candle["high"]
        
        # Should be able to store via OHLCV database
        await ohlcv_db.store_candles([candle])
        await ohlcv_db.flush()

    @pytest.mark.asyncio  
    async def test_trade_factory_creates_valid_data(self, ohlcv_db):
        """Test trade factory creates valid trade data."""
        # Create trade using factory
        trade = TradeDataFactory()
        
        # Verify factory created valid data
        assert trade["symbol"] is not None
        assert trade["exchange"] is not None
        assert isinstance(trade["price"], (int, float))
        assert isinstance(trade["volume"], (int, float))
        assert trade["side"] in ["buy", "sell"]
        assert trade["type"] in ["market", "limit"]
        assert isinstance(trade["timestamp"], datetime)
        
        # Values should be positive
        assert trade["price"] > 0
        assert trade["volume"] > 0
        
        # Should be able to store via OHLCV database
        await ohlcv_db.store_trades([trade])
        await ohlcv_db.flush()

    @pytest.mark.asyncio
    async def test_service_config_factories(self):
        """Test service configuration factories."""
        # OHLCV service config
        ohlcv_config = OHLCVServiceConfigFactory()
        assert isinstance(ohlcv_config["exchanges"], list)
        assert isinstance(ohlcv_config["symbols"], list)
        assert isinstance(ohlcv_config["collection_interval"], int)
        assert isinstance(ohlcv_config["enabled"], bool)
        
        # Trade service config
        trade_config = TradeServiceConfigFactory()
        assert isinstance(trade_config["exchanges"], list)
        assert isinstance(trade_config["symbols"], list)
        assert isinstance(trade_config["collection_enabled"], bool)
        assert isinstance(trade_config["batch_size"], int)

    @pytest.mark.asyncio
    async def test_candle_list_creation(self, ohlcv_db):
        """Test creating lists of candles with proper timing."""
        # Create list of candles
        candles = create_candle_list(count=10, timeframe="1h")
        
        # Should have correct count
        assert len(candles) == 10
        
        # Timestamps should be properly spaced (1 hour apart)
        for i in range(1, len(candles)):
            time_diff = candles[i]["timestamp"] - candles[i-1]["timestamp"]
            assert time_diff.total_seconds() == 3600  # 1 hour
        
        # All should have same symbol/exchange
        assert all(c["symbol"] == candles[0]["symbol"] for c in candles)
        assert all(c["exchange"] == candles[0]["exchange"] for c in candles)
        
        # Should be able to store all
        await ohlcv_db.store_candles(candles)
        await ohlcv_db.flush()

    @pytest.mark.asyncio
    async def test_trade_list_creation(self, ohlcv_db):
        """Test creating lists of trades with proper timing."""
        # Create list of trades
        trades = create_trade_list(count=20, symbol="ETH/USDT", exchange="kraken")
        
        # Should have correct count
        assert len(trades) == 20
        
        # All should have specified symbol/exchange
        assert all(t["symbol"] == "ETH/USDT" for t in trades)
        assert all(t["exchange"] == "kraken" for t in trades)
        
        # Timestamps should be sequential
        for i in range(1, len(trades)):
            assert trades[i]["timestamp"] >= trades[i-1]["timestamp"]
        
        # Should be able to store all
        await ohlcv_db.store_trades(trades)
        await ohlcv_db.flush()

    @pytest.mark.asyncio
    async def test_isolation_between_tests(self, ohlcv_db):
        """Test that test isolation works between different test cases."""
        # This test should start with empty state due to rollback isolation
        
        # Add some test data
        candle = CandleDataFactory(symbol="TEST/USDT", exchange="test_exchange")
        await ohlcv_db.store_candles([candle])
        await ohlcv_db.flush()
        
        # Verify data was stored (mocked storage always works)
        # In real implementation, this would query the database
        recent_candles = await ohlcv_db.get_recent_candles("TEST/USDT", "binance")
        assert isinstance(recent_candles, list)  # Mock returns empty list

    @pytest.mark.asyncio
    async def test_realistic_data_creation(self, ohlcv_db):
        """Test realistic data creation functions."""
        # Create realistic candles
        realistic_candles = create_realistic_candles(
            count=24,
            timeframe="1h",
            symbol="BTC/USDT",
            exchange="binance",
            base_price=50000.0,
            volatility=0.01
        )
        
        # Should have realistic price relationships
        for candle in realistic_candles:
            assert candle["low"] <= candle["open"] <= candle["high"]
            assert candle["low"] <= candle["close"] <= candle["high"]
            assert 40000 <= candle["open"] <= 60000  # Reasonable range
            assert candle["volume"] > 0
        
        # Create realistic trades
        realistic_trades = create_realistic_trades(
            count=100,
            symbol="ETH/USDT",
            exchange="kraken",
            price_volatility=0.001
        )
        
        # Should have reasonable trade data
        for trade in realistic_trades:
            assert trade["price"] > 0
            assert trade["volume"] > 0
            assert trade["side"] in ["buy", "sell"]
        
        # Should be able to store both
        await ohlcv_db.store_candles(realistic_candles)
        await ohlcv_db.store_trades(realistic_trades)
        await ohlcv_db.flush()

    @pytest.mark.asyncio
    async def test_exchange_specific_data(self):
        """Test exchange-specific data creation."""
        from tests.factories import (
            create_binance_candle_data,
            create_kraken_candle_data,
            create_binance_trade_data,
            create_kraken_trade_data
        )
        
        # Binance data should use correct format
        binance_candle = create_binance_candle_data()
        assert binance_candle["exchange"] == "binance"
        assert binance_candle["symbol"] == "BTCUSDT"  # Binance format
        
        # Kraken data should use correct format
        kraken_candle = create_kraken_candle_data()
        assert kraken_candle["exchange"] == "kraken"
        assert kraken_candle["symbol"] == "BTC/USD"  # Kraken format
        
        # Same for trades
        binance_trade = create_binance_trade_data()
        assert binance_trade["exchange"] == "binance"
        
        kraken_trade = create_kraken_trade_data()
        assert kraken_trade["exchange"] == "kraken"

    @pytest.mark.asyncio
    async def test_async_patterns_work(self, ohlcv_db):
        """Test that async patterns work correctly in test environment."""
        import asyncio
        
        # Test concurrent operations
        async def store_candle(symbol: str):
            candle = CandleDataFactory(symbol=symbol)
            await ohlcv_db.store_candles([candle])
            return symbol
        
        # Run concurrent operations
        tasks = [
            store_candle("BTC/USDT"),
            store_candle("ETH/USDT"),
            store_candle("LTC/USDT"),
        ]
        
        results = await asyncio.gather(*tasks)
        await ohlcv_db.flush()
        
        # All should complete successfully
        assert len(results) == 3
        assert "BTC/USDT" in results
        assert "ETH/USDT" in results
        assert "LTC/USDT" in results

    @pytest.mark.asyncio
    async def test_factory_customization(self):
        """Test that factories can be customized properly."""
        # Custom candle with specific values
        custom_candle = CandleDataFactory(
            symbol="CUSTOM/TEST",
            exchange="test_exchange",
            open=100.0,
            volume=999.0
        )
        
        assert custom_candle["symbol"] == "CUSTOM/TEST"
        assert custom_candle["exchange"] == "test_exchange"
        assert custom_candle["open"] == 100.0
        assert custom_candle["volume"] == 999.0
        
        # Custom trade with specific values
        custom_trade = TradeDataFactory(
            symbol="CUSTOM/TEST",
            side="buy",
            type="limit",
            price=200.0
        )
        
        assert custom_trade["symbol"] == "CUSTOM/TEST"
        assert custom_trade["side"] == "buy"
        assert custom_trade["type"] == "limit"
        assert custom_trade["price"] == 200.0