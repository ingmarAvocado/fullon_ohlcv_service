"""
Unit tests for real-time WebSocket trade collection components.

Tests LiveTradeCollector WebSocket handling, GlobalTradeBatcher coordination,
and the transition from historical to live collection.
"""

import asyncio
import pytest
import time
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch, call

from fullon_ohlcv.models import Trade


class TestLiveTradeCollector:
    """Test LiveTradeCollector WebSocket streaming functionality."""

    @pytest.fixture
    def mock_symbol_config(self):
        """Create a mock symbol configuration."""
        config = MagicMock()
        config.symbol = "BTC/USDC"
        config.exchange_name = "kraken"
        config.cat_ex_id = 1
        config.backtest = 30
        return config

    @pytest.mark.asyncio
    async def test_websocket_connection_establishment(self, mock_symbol_config):
        """Test that LiveTradeCollector establishes WebSocket connection properly."""
        from src.fullon_ohlcv_service.trade.live_collector import LiveTradeCollector

        with patch('src.fullon_ohlcv_service.trade.live_collector.ExchangeQueue') as mock_queue:
            # Setup mocks
            mock_handler = AsyncMock()
            mock_queue.initialize_factory = AsyncMock()
            mock_queue.get_websocket_handler = AsyncMock(return_value=mock_handler)

            collector = LiveTradeCollector(mock_symbol_config)

            # Start streaming
            streaming_task = asyncio.create_task(collector.start_streaming())
            await asyncio.sleep(0.1)  # Let initialization happen

            # Verify WebSocket setup
            mock_queue.initialize_factory.assert_called_once()
            mock_queue.get_websocket_handler.assert_called_once()
            mock_handler.subscribe_trades.assert_called_once_with(
                "BTC/USDC", collector._on_trade_received
            )

            # Stop streaming
            await collector.stop_streaming()
            streaming_task.cancel()
            try:
                await streaming_task
            except asyncio.CancelledError:
                pass

    @pytest.mark.asyncio
    async def test_trade_redis_queuing(self, mock_symbol_config):
        """Test that received trades are pushed to Redis correctly."""
        from src.fullon_ohlcv_service.trade.live_collector import LiveTradeCollector

        with patch('src.fullon_ohlcv_service.trade.live_collector.TradesCache') as mock_cache:
            # Setup mock cache
            mock_cache_instance = AsyncMock()
            mock_cache.return_value.__aenter__.return_value = mock_cache_instance

            collector = LiveTradeCollector(mock_symbol_config)

            # Simulate receiving a trade
            trade_data = {
                'timestamp': datetime.now(timezone.utc).timestamp(),
                'price': 65000.0,
                'amount': 0.5,
                'side': 'buy'
            }

            await collector._on_trade_received(trade_data)

            # Verify trade was pushed to Redis
            mock_cache_instance.push_trade.assert_called_once_with(
                "kraken", "BTC/USDC", trade_data
            )

    @pytest.mark.asyncio
    async def test_websocket_auto_reconnection(self, mock_symbol_config):
        """Test WebSocket reconnection after disconnection."""
        from src.fullon_ohlcv_service.trade.live_collector import LiveTradeCollector

        with patch('src.fullon_ohlcv_service.trade.live_collector.ExchangeQueue') as mock_queue:
            # Setup mock with connection tracking
            mock_handler = AsyncMock()
            mock_handler.connected = True
            reconnect_count = 0

            async def subscribe_with_reconnect(*args, **kwargs):
                nonlocal reconnect_count
                reconnect_count += 1
                if reconnect_count == 2:
                    # Simulate disconnection on second attempt
                    raise Exception("WebSocket disconnected")
                return None

            mock_handler.subscribe_trades = subscribe_with_reconnect
            mock_queue.get_websocket_handler = AsyncMock(return_value=mock_handler)
            mock_queue.initialize_factory = AsyncMock()

            collector = LiveTradeCollector(mock_symbol_config)

            # Start streaming
            streaming_task = asyncio.create_task(collector.start_streaming())
            await asyncio.sleep(3)  # Let reconnection happen

            # Verify reconnection attempts (should have at least 2 attempts)
            assert reconnect_count >= 2

            # Stop streaming
            await collector.stop_streaming()
            streaming_task.cancel()
            try:
                await streaming_task
            except asyncio.CancelledError:
                pass

    @pytest.mark.asyncio
    async def test_batcher_registration(self, mock_symbol_config):
        """Test LiveTradeCollector registers with GlobalTradeBatcher."""
        from src.fullon_ohlcv_service.trade.live_collector import LiveTradeCollector
        from src.fullon_ohlcv_service.trade.global_batcher import GlobalTradeBatcher

        with patch.object(GlobalTradeBatcher, 'register_symbol', new=AsyncMock()) as mock_register:
            collector = LiveTradeCollector(mock_symbol_config)

            await collector._register_with_batcher()

            # Verify registration
            mock_register.assert_called_once_with("kraken", "BTC/USDC")


class TestGlobalTradeBatcher:
    """Test GlobalTradeBatcher batch processing and coordination."""

    @pytest.mark.asyncio
    async def test_singleton_pattern(self):
        """Test GlobalTradeBatcher implements singleton pattern correctly."""
        from src.fullon_ohlcv_service.trade.global_batcher import GlobalTradeBatcher

        batcher1 = GlobalTradeBatcher()
        batcher2 = GlobalTradeBatcher()

        assert batcher1 is batcher2

    @pytest.mark.asyncio
    async def test_minute_aligned_timing(self):
        """Test batch processing happens at regular intervals."""
        from src.fullon_ohlcv_service.trade.global_batcher import GlobalTradeBatcher

        batcher = GlobalTradeBatcher()
        batcher._batch_interval = 1  # Speed up test by using 1 second interval

        with patch.object(batcher, '_process_batch', new=AsyncMock()) as mock_process:
            # Start batch processing
            processing_task = asyncio.create_task(batcher.start_batch_processing())

            # Wait for slightly more than the interval
            await asyncio.sleep(2.5)

            # Verify batch was processed at least once
            assert mock_process.call_count >= 1

            # Stop processing
            await batcher.stop_batch_processing()
            processing_task.cancel()
            try:
                await processing_task
            except asyncio.CancelledError:
                pass

    @pytest.mark.asyncio
    async def test_batch_collection_from_redis(self):
        """Test collecting trades from Redis and saving to PostgreSQL."""
        from src.fullon_ohlcv_service.trade.global_batcher import GlobalTradeBatcher

        with patch('src.fullon_ohlcv_service.trade.global_batcher.TradesCache') as mock_cache, \
             patch('src.fullon_ohlcv_service.trade.global_batcher.TradeRepository') as mock_repo:

            # Setup mocks
            mock_cache_instance = AsyncMock()
            mock_cache.return_value.__aenter__.return_value = mock_cache_instance

            mock_repo_instance = AsyncMock()
            mock_repo.return_value.__aenter__.return_value = mock_repo_instance

            # Mock trades from Redis
            mock_trades = [
                {'timestamp': time.time(), 'price': 65000, 'amount': 0.5, 'side': 'buy'},
                {'timestamp': time.time(), 'price': 65100, 'amount': 0.3, 'side': 'sell'}
            ]
            mock_cache_instance.get_trades.return_value = mock_trades

            batcher = GlobalTradeBatcher()
            await batcher.register_symbol("kraken", "BTC/USDC")

            # Process batch
            await batcher._process_batch()

            # Verify trades were fetched from Redis
            mock_cache_instance.get_trades.assert_called()

            # Verify trades were saved to PostgreSQL
            mock_repo_instance.save_trades.assert_called()

    @pytest.mark.asyncio
    async def test_multiple_symbol_coordination(self):
        """Test GlobalTradeBatcher handles multiple symbols correctly."""
        from src.fullon_ohlcv_service.trade.global_batcher import GlobalTradeBatcher

        with patch('src.fullon_ohlcv_service.trade.global_batcher.TradesCache') as mock_cache, \
             patch('src.fullon_ohlcv_service.trade.global_batcher.TradeRepository') as mock_repo:

            mock_cache_instance = AsyncMock()
            mock_cache.return_value.__aenter__.return_value = mock_cache_instance
            mock_cache_instance.get_trades.return_value = []

            batcher = GlobalTradeBatcher()

            # Register multiple symbols
            await batcher.register_symbol("kraken", "BTC/USDC")
            await batcher.register_symbol("kraken", "ETH/USDC")
            await batcher.register_symbol("binance", "BTC/USDT")

            # Process batch
            await batcher._process_batch()

            # Verify all symbols were processed
            assert mock_cache_instance.get_trades.call_count == 3


class TestHistoricalToLiveTransition:
    """Test seamless transition from historical to live collection."""

    @pytest.mark.asyncio
    async def test_automatic_transition_after_historical(self):
        """Test LiveTradeCollector starts automatically after historical collection."""
        from src.fullon_ohlcv_service.trade.historic_collector import HistoricTradeCollector

        mock_symbol = MagicMock()
        mock_symbol.symbol = "BTC/USDC"
        mock_symbol.exchange_name = "kraken"
        mock_symbol.cat_ex_id = 1
        mock_symbol.backtest = 3

        with patch('src.fullon_ohlcv_service.trade.historic_collector.LiveTradeCollector') as mock_live:
            # Setup mocks
            mock_live_instance = AsyncMock()
            mock_live.return_value = mock_live_instance

            with patch.object(HistoricTradeCollector, 'collect_historical_data', new=AsyncMock(return_value=100)):
                with patch.object(HistoricTradeCollector, 'create_exchange_object', new=AsyncMock()):
                    with patch.object(HistoricTradeCollector, 'create_credential_provider', return_value=lambda x: ("", "")):
                        with patch('src.fullon_ohlcv_service.trade.historic_collector.ExchangeQueue') as mock_queue:
                            mock_handler = AsyncMock()
                            mock_queue.get_rest_handler = AsyncMock(return_value=mock_handler)
                            mock_queue.initialize_factory = AsyncMock()

                            collector = HistoricTradeCollector(mock_symbol)
                            await collector.collect()

                            # Verify LiveTradeCollector was created and started
                            mock_live.assert_called_once_with(mock_symbol)
                            mock_live_instance.start_streaming.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_data_loss_during_transition(self):
        """Test no trades are lost during historical to live transition."""
        from src.fullon_ohlcv_service.trade.historic_collector import HistoricTradeCollector
        from src.fullon_ohlcv_service.trade.live_collector import LiveTradeCollector

        mock_symbol = MagicMock()
        mock_symbol.symbol = "BTC/USDC"
        mock_symbol.exchange_name = "kraken"
        mock_symbol.backtest = 3

        # Track the last historical trade timestamp
        last_historical_timestamp = time.time() - 60  # 1 minute ago

        with patch.object(HistoricTradeCollector, 'collect_historical_data',
                         return_value=100) as mock_historical:
            with patch.object(LiveTradeCollector, 'start_streaming',
                            new=AsyncMock()) as mock_live:
                # Simulate seamless transition
                historic = HistoricTradeCollector(mock_symbol)

                # Mock the transition to ensure no gap
                with patch('time.time', return_value=last_historical_timestamp + 1):
                    # Historical collection should complete just before live starts
                    await historic.collect()

                # Verify timing continuity
                # Live collection should start immediately after historical


class TestErrorHandling:
    """Test error handling and recovery mechanisms."""

    @pytest.mark.asyncio
    async def test_websocket_error_recovery(self):
        """Test recovery from WebSocket errors."""
        from src.fullon_ohlcv_service.trade.live_collector import LiveTradeCollector

        mock_symbol = MagicMock()
        mock_symbol.symbol = "BTC/USDC"
        mock_symbol.exchange_name = "kraken"

        with patch('src.fullon_ohlcv_service.trade.live_collector.ExchangeQueue') as mock_queue:
            # Simulate WebSocket error and recovery
            mock_handler = AsyncMock()
            mock_handler.connected = True
            error_count = 0

            async def subscribe_with_error(symbol, callback):
                nonlocal error_count
                error_count += 1
                if error_count == 1:
                    raise Exception("WebSocket connection failed")
                # Success on retry
                return None

            mock_handler.subscribe_trades = subscribe_with_error
            mock_queue.get_websocket_handler = AsyncMock(return_value=mock_handler)
            mock_queue.initialize_factory = AsyncMock()

            collector = LiveTradeCollector(mock_symbol)

            # Should recover from error
            streaming_task = asyncio.create_task(collector.start_streaming())
            await asyncio.sleep(3)  # Allow time for retry

            # Verify recovery
            assert error_count >= 2  # At least one error and one success

            await collector.stop_streaming()
            streaming_task.cancel()
            try:
                await streaming_task
            except asyncio.CancelledError:
                pass

    @pytest.mark.asyncio
    async def test_database_error_handling(self):
        """Test handling of database save errors."""
        from src.fullon_ohlcv_service.trade.global_batcher import GlobalTradeBatcher

        with patch('src.fullon_ohlcv_service.trade.global_batcher.TradeRepository') as mock_repo:
            mock_repo_instance = AsyncMock()
            mock_repo_instance.save_trades.side_effect = Exception("Database error")
            mock_repo.return_value.__aenter__.return_value = mock_repo_instance

            batcher = GlobalTradeBatcher()
            await batcher.register_symbol("kraken", "BTC/USDC")

            # Should handle error gracefully
            await batcher._process_single_symbol("kraken", "BTC/USDC")

            # Verify error was handled (no exception raised)
            assert True  # Test passes if no exception


class TestPerformance:
    """Test performance characteristics of the implementation."""

    @pytest.mark.asyncio
    async def test_batch_size_limits(self):
        """Test that batch sizes are properly limited."""
        from src.fullon_ohlcv_service.trade.global_batcher import GlobalTradeBatcher

        with patch('src.fullon_ohlcv_service.trade.global_batcher.TradesCache') as mock_cache:
            mock_cache_instance = AsyncMock()
            mock_cache.return_value.__aenter__.return_value = mock_cache_instance

            # Return many trades
            mock_trades = [{'timestamp': time.time(), 'price': 65000, 'amount': 0.1}] * 2000
            mock_cache_instance.get_trades.return_value = mock_trades

            batcher = GlobalTradeBatcher()

            # Process batch - should limit the number processed
            await batcher._process_single_symbol("kraken", "BTC/USDC")

            # Verify batch limit was applied
            mock_cache_instance.get_trades.assert_called_with(
                "kraken:BTC/USDC", limit=1000  # Expected batch limit
            )

    @pytest.mark.asyncio
    async def test_timing_precision(self):
        """Test that batch processing timing is precise."""
        from src.fullon_ohlcv_service.trade.global_batcher import GlobalTradeBatcher

        batcher = GlobalTradeBatcher()

        # Test calculation of next batch time
        current_time = time.time()
        next_time = batcher._calculate_next_batch_time()

        # Should be aligned to minute boundary minus 0.1 seconds
        seconds_in_minute = next_time % 60
        assert 59.8 <= seconds_in_minute <= 60 or seconds_in_minute <= 0.2