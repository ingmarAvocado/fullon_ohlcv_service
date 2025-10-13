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
    async def test_websocket_connection_establishment(self):
        """Test that LiveTradeCollector establishes WebSocket connection properly."""
        from fullon_ohlcv_service.trade.live_collector import LiveTradeCollector
        from fullon_ohlcv_service.trade.batcher import GlobalTradeBatcher

        # Mock symbol and exchange objects
        mock_symbol = MagicMock()
        mock_symbol.symbol = "BTC/USDC"
        mock_symbol.cat_exchange.name = "kraken"

        mock_exchange = MagicMock()
        mock_exchange.cat_exchange.name = "kraken"

        with (
            patch("fullon_ohlcv_service.trade.live_collector.ExchangeQueue") as mock_queue,
            patch.object(LiveTradeCollector, "_load_data") as mock_load_data,
            patch(
                "fullon_ohlcv_service.trade.live_collector.GlobalTradeBatcher"
            ) as mock_batcher_class,
        ):
            # Setup mocks
            mock_handler = AsyncMock()
            mock_queue.get_websocket_handler = AsyncMock(return_value=mock_handler)

            # Mock data loading
            symbols_by_exchange = {"kraken": [mock_symbol]}
            admin_exchanges = [mock_exchange]
            mock_load_data.return_value = (symbols_by_exchange, admin_exchanges)

            # Mock batcher
            mock_batcher = AsyncMock()
            mock_batcher_class.return_value = mock_batcher

            collector = LiveTradeCollector()

            # Start collection
            collection_task = asyncio.create_task(collector.start_collection())
            await asyncio.sleep(0.1)  # Let initialization happen

            # Verify data loading
            mock_load_data.assert_called_once()

            # Verify WebSocket handler was obtained
            mock_queue.get_websocket_handler.assert_called_once_with(mock_exchange)

            # Verify subscribe_trades was called with symbol and callback
            mock_handler.subscribe_trades.assert_called_once()
            call_args = mock_handler.subscribe_trades.call_args
            assert call_args[0][0] == "BTC/USDC"  # symbol
            assert callable(call_args[0][1])  # callback function

            # Verify symbol was registered with batcher
            mock_batcher.register_symbol.assert_called_once_with("kraken", "BTC/USDC")

            # Stop collection
            await collector.stop_collection()
            collection_task.cancel()
            try:
                await collection_task
            except asyncio.CancelledError:
                pass

    @pytest.mark.asyncio
    async def test_trade_redis_queuing(self):
        """Test that received trades are pushed to Redis correctly via callback."""
        from fullon_ohlcv_service.trade.live_collector import LiveTradeCollector
        from fullon_orm.models import Trade as ORMTrade

        # Create mock ORM Trade object
        mock_trade = MagicMock(spec=ORMTrade)
        mock_trade.symbol = "BTC/USDC"
        mock_trade.time = datetime.now(timezone.utc).timestamp()
        mock_trade.price = 65000.0
        mock_trade.volume = 0.5
        mock_trade.side = "buy"

        with patch("fullon_ohlcv_service.trade.live_collector.TradesCache") as mock_cache:
            # Setup mock cache
            mock_cache_instance = AsyncMock()
            mock_cache.return_value.__aenter__.return_value = mock_cache_instance

            collector = LiveTradeCollector()

            # Create the callback (this is what gets called by WebSocket)
            callback = collector._create_exchange_callback("kraken")

            # Simulate receiving a trade via the callback
            await callback(mock_trade)

            # Verify trade was pushed to Redis cache
            mock_cache_instance.push_trade_list.assert_called_once()
            call_args = mock_cache_instance.push_trade_list.call_args
            assert call_args[1]["symbol"] == "BTC/USDC"
            assert call_args[1]["exchange"] == "kraken"
            assert call_args[1]["trade"] is mock_trade

            # Verify trade status was updated
            mock_cache_instance.update_trade_status.assert_called_once_with(key="kraken")

    @pytest.mark.asyncio
    async def test_websocket_error_handling_during_startup(self):
        """Test WebSocket error handling during collection startup."""
        from fullon_ohlcv_service.trade.live_collector import LiveTradeCollector

        # Mock symbol and exchange objects
        mock_symbol = MagicMock()
        mock_symbol.symbol = "BTC/USDC"
        mock_symbol.cat_exchange.name = "kraken"

        mock_exchange = MagicMock()
        mock_exchange.cat_exchange.name = "kraken"

        with (
            patch("fullon_ohlcv_service.trade.live_collector.ExchangeQueue") as mock_queue,
            patch.object(LiveTradeCollector, "_load_data") as mock_load_data,
        ):
            # Setup mocks
            mock_handler = AsyncMock()
            # Simulate WebSocket subscription failure
            mock_handler.subscribe_trades.side_effect = Exception("WebSocket connection failed")
            mock_queue.get_websocket_handler = AsyncMock(return_value=mock_handler)

            # Mock data loading
            symbols_by_exchange = {"kraken": [mock_symbol]}
            admin_exchanges = [mock_exchange]
            mock_load_data.return_value = (symbols_by_exchange, admin_exchanges)

            collector = LiveTradeCollector()

            # Start collection - should handle the error gracefully
            collection_task = asyncio.create_task(collector.start_collection())
            await asyncio.sleep(0.1)  # Let initialization happen

            # Verify subscribe_trades was attempted with correct symbol
            mock_handler.subscribe_trades.assert_called_once()
            call_args = mock_handler.subscribe_trades.call_args
            assert call_args[0][0] == "BTC/USDC"  # symbol
            assert callable(call_args[0][1])  # callback function

            # Collection should complete (even with errors for individual symbols)
            await collection_task

            # Stop collection
            await collector.stop_collection()

    @pytest.mark.asyncio
    async def test_batcher_registration(self):
        """Test LiveTradeCollector registers symbols with GlobalTradeBatcher during startup."""
        from fullon_ohlcv_service.trade.live_collector import LiveTradeCollector

        # Mock symbol and exchange objects
        mock_symbol = MagicMock()
        mock_symbol.symbol = "BTC/USDC"
        mock_symbol.cat_exchange.name = "kraken"

        mock_exchange = MagicMock()
        mock_exchange.cat_exchange.name = "kraken"

        with (
            patch("fullon_ohlcv_service.trade.live_collector.ExchangeQueue") as mock_queue,
            patch.object(LiveTradeCollector, "_load_data") as mock_load_data,
            patch(
                "fullon_ohlcv_service.trade.live_collector.GlobalTradeBatcher"
            ) as mock_batcher_class,
        ):
            # Setup mocks
            mock_handler = AsyncMock()
            mock_queue.get_websocket_handler = AsyncMock(return_value=mock_handler)

            # Mock data loading
            symbols_by_exchange = {"kraken": [mock_symbol]}
            admin_exchanges = [mock_exchange]
            mock_load_data.return_value = (symbols_by_exchange, admin_exchanges)

            # Mock batcher
            mock_batcher = AsyncMock()
            mock_batcher_class.return_value = mock_batcher

            collector = LiveTradeCollector()

            # Start collection
            await collector.start_collection()

            # Verify symbol was registered with batcher during startup
            mock_batcher.register_symbol.assert_called_once_with("kraken", "BTC/USDC")

            # Verify symbol is tracked in registered_symbols
            assert "kraken:BTC/USDC" in collector.registered_symbols

            # Stop collection and verify unregistration
            await collector.stop_collection()
            mock_batcher.unregister_symbol.assert_called_once_with("kraken", "BTC/USDC")


class TestGlobalTradeBatcher:
    """Test GlobalTradeBatcher batch processing and coordination."""

    @pytest.mark.asyncio
    async def test_singleton_pattern(self):
        """Test GlobalTradeBatcher implements singleton pattern correctly."""
        from fullon_ohlcv_service.trade.batcher import GlobalTradeBatcher

        batcher1 = GlobalTradeBatcher()
        batcher2 = GlobalTradeBatcher()

        assert batcher1 is batcher2

    @pytest.mark.asyncio
    async def test_minute_aligned_timing(self):
        """Test batch processing happens at regular intervals."""
        from fullon_ohlcv_service.trade.batcher import GlobalTradeBatcher

        batcher = GlobalTradeBatcher()

        with (
            patch.object(batcher, "_process_batch", new=AsyncMock()) as mock_process,
            patch.object(
                batcher, "_calculate_next_batch_time", return_value=time.time() + 0.1
            ) as mock_calc,
        ):
            # Start batch processing
            processing_task = asyncio.create_task(batcher.start_batch_processing())

            # Wait for slightly more than the interval
            await asyncio.sleep(0.5)

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
        from fullon_ohlcv_service.trade.batcher import GlobalTradeBatcher
        from fullon_orm.models import Trade as ORMTrade

        # Create mock ORM Trade objects
        mock_trade1 = MagicMock(spec=ORMTrade)
        mock_trade1.time = time.time()
        mock_trade1.price = 65000.0
        mock_trade1.volume = 0.5
        mock_trade1.side = "buy"

        mock_trade2 = MagicMock(spec=ORMTrade)
        mock_trade2.time = time.time()
        mock_trade2.price = 65100.0
        mock_trade2.volume = 0.3
        mock_trade2.side = "sell"

        with (
            patch("fullon_ohlcv_service.trade.batcher.TradesCache") as mock_cache,
            patch("fullon_ohlcv_service.trade.batcher.TradeRepository") as mock_repo,
        ):
            # Setup mocks
            mock_cache_instance = AsyncMock()
            mock_cache.return_value.__aenter__.return_value = mock_cache_instance
            mock_cache_instance.get_trades.return_value = [mock_trade1, mock_trade2]

            mock_repo_instance = AsyncMock()
            mock_repo.return_value.__aenter__.return_value = mock_repo_instance
            mock_repo_instance.save_trades.return_value = True

            batcher = GlobalTradeBatcher()
            await batcher.register_symbol("kraken", "BTC/USDC")

            # Process batch
            await batcher._process_batch()

            # Verify trades were fetched from Redis for the correct symbol
            mock_cache_instance.get_trades.assert_called_once_with("BTC/USDC", "kraken")

            # Verify trades were saved to PostgreSQL
            mock_repo_instance.save_trades.assert_called_once()
            saved_trades = mock_repo_instance.save_trades.call_args[0][0]
            assert len(saved_trades) == 2

    @pytest.mark.asyncio
    async def test_multiple_symbol_coordination(self):
        """Test GlobalTradeBatcher handles multiple symbols correctly."""
        from fullon_ohlcv_service.trade.batcher import GlobalTradeBatcher

        with (
            patch("fullon_ohlcv_service.trade.batcher.TradesCache") as mock_cache,
            patch("fullon_ohlcv_service.trade.batcher.TradeRepository") as mock_repo,
        ):
            mock_cache_instance = AsyncMock()
            mock_cache.return_value.__aenter__.return_value = mock_cache_instance
            mock_cache_instance.get_trades.return_value = []

            mock_repo_instance = AsyncMock()
            mock_repo.return_value.__aenter__.return_value = mock_repo_instance

            batcher = GlobalTradeBatcher()

            # Register multiple symbols
            await batcher.register_symbol("kraken", "BTC/USDC")
            await batcher.register_symbol("kraken", "ETH/USDC")
            await batcher.register_symbol("binance", "BTC/USDT")

            # Process batch
            await batcher._process_batch()

            # Verify all symbols were processed (get_trades called for each)
            assert mock_cache_instance.get_trades.call_count == 3
            expected_calls = [
                (("BTC/USDC", "kraken"), {}),
                (("ETH/USDC", "kraken"), {}),
                (("BTC/USDT", "binance"), {}),
            ]
            mock_cache_instance.get_trades.assert_has_calls(expected_calls, any_order=True)


class TestHistoricalToLiveTransition:
    """Test seamless transition from historical to live collection."""

    @pytest.mark.asyncio
    async def test_historical_collection_standalone(self):
        """Test HistoricTradeCollector works as standalone process."""
        from fullon_ohlcv_service.trade.historic_collector import HistoricTradeCollector

        # Mock symbol and exchange objects
        mock_symbol = MagicMock()
        mock_symbol.symbol = "BTC/USDC"
        mock_symbol.cat_exchange.name = "kraken"
        mock_symbol.backtest = 3

        mock_exchange = MagicMock()
        mock_exchange.cat_exchange.name = "kraken"

        with (
            patch("fullon_ohlcv_service.trade.historic_collector.ExchangeQueue") as mock_queue,
            patch.object(HistoricTradeCollector, "_load_data") as mock_load_data,
            patch.object(HistoricTradeCollector, "_collect_symbol_historical") as mock_collect,
        ):
            # Setup mocks
            mock_handler = AsyncMock()
            mock_queue.get_rest_handler = AsyncMock(return_value=mock_handler)

            # Mock data loading
            symbols_by_exchange = {"kraken": [mock_symbol]}
            admin_exchanges = [mock_exchange]
            mock_load_data.return_value = (symbols_by_exchange, admin_exchanges)

            # Mock successful collection
            mock_collect.return_value = 100

            collector = HistoricTradeCollector()

            # Start collection
            results = await collector.start_collection()

            # Verify collection completed and returned results
            assert results == {"kraken:BTC/USDC": 100}
            mock_collect.assert_called_once()

    @pytest.mark.asyncio
    async def test_historical_and_live_separate_processes(self):
        """Test that historical and live collection are separate processes."""
        from fullon_ohlcv_service.trade.historic_collector import HistoricTradeCollector
        from fullon_ohlcv_service.trade.live_collector import LiveTradeCollector

        # Mock symbol and exchange objects
        mock_symbol = MagicMock()
        mock_symbol.symbol = "BTC/USDC"
        mock_symbol.cat_exchange.name = "kraken"
        mock_symbol.backtest = 3

        mock_exchange = MagicMock()
        mock_exchange.cat_exchange.name = "kraken"

        with (
            patch(
                "fullon_ohlcv_service.trade.historic_collector.ExchangeQueue"
            ) as mock_queue_historic,
            patch("fullon_ohlcv_service.trade.live_collector.ExchangeQueue") as mock_queue_live,
            patch.object(HistoricTradeCollector, "_load_data") as mock_load_data_historic,
            patch.object(LiveTradeCollector, "_load_data") as mock_load_data_live,
            patch.object(HistoricTradeCollector, "_collect_symbol_historical") as mock_collect,
        ):
            # Setup historical mocks
            mock_handler_historic = MagicMock()  # Use MagicMock since we don't need async behavior
            mock_queue_historic.get_rest_handler = AsyncMock(return_value=mock_handler_historic)
            symbols_by_exchange = {"kraken": [mock_symbol]}
            admin_exchanges = [mock_exchange]
            mock_load_data_historic.return_value = (symbols_by_exchange, admin_exchanges)
            mock_collect.return_value = 100

            # Setup live mocks
            mock_handler_live = MagicMock()  # Use MagicMock since we don't need async behavior
            mock_queue_live.get_websocket_handler = AsyncMock(return_value=mock_handler_live)
            mock_load_data_live.return_value = (symbols_by_exchange, admin_exchanges)

            # Test historical collection
            historic_collector = HistoricTradeCollector()
            historic_results = await historic_collector.start_collection()
            assert historic_results == {"kraken:BTC/USDC": 100}

            # Test live collection separately
            live_collector = LiveTradeCollector()
            await live_collector.start_collection()

            # Verify they use different handlers (REST vs WebSocket)
            mock_queue_historic.get_rest_handler.assert_called_once()
            mock_queue_live.get_websocket_handler.assert_called_once()

            # Clean up
            await live_collector.stop_collection()


class TestErrorHandling:
    """Test error handling and recovery mechanisms."""

    @pytest.mark.asyncio
    async def test_callback_error_handling(self):
        """Test error handling in the trade callback."""
        from fullon_ohlcv_service.trade.live_collector import LiveTradeCollector
        from fullon_orm.models import Trade as ORMTrade

        # Create mock ORM Trade object
        mock_trade = MagicMock(spec=ORMTrade)
        mock_trade.symbol = "BTC/USDC"
        mock_trade.time = datetime.now(timezone.utc).timestamp()

        with (
            patch("fullon_ohlcv_service.trade.live_collector.TradesCache") as mock_cache,
            patch("fullon_ohlcv_service.trade.live_collector.ProcessCache") as mock_process_cache,
        ):
            # Setup mocks to simulate errors
            mock_cache_instance = AsyncMock()
            mock_cache_instance.push_trade_list.side_effect = Exception("Redis connection failed")
            mock_cache.return_value.__aenter__.return_value = mock_cache_instance

            mock_process_instance = AsyncMock()
            mock_process_cache.return_value.__aenter__.return_value = mock_process_instance

            collector = LiveTradeCollector()
            collector.process_ids = {"kraken:BTC/USDC": "test_process_id"}

            # Create the callback
            callback = collector._create_exchange_callback("kraken")

            # Call callback with trade - should handle error gracefully
            await callback(mock_trade)

            # Verify error was handled (no exception raised)
            # Verify process status was updated on error
            from fullon_cache.process_cache import ProcessStatus

            mock_process_instance.update_process.assert_called_with(
                process_id="test_process_id",
                status=ProcessStatus.ERROR,
                message="Error: Redis connection failed",
            )

    @pytest.mark.asyncio
    async def test_database_error_handling(self):
        """Test handling of database save errors."""
        from fullon_ohlcv_service.trade.batcher import GlobalTradeBatcher

        with patch("fullon_ohlcv_service.trade.batcher.TradeRepository") as mock_repo:
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
    async def test_batch_processing_handles_large_trade_volumes(self):
        """Test that batch processing handles large volumes of trades."""
        from fullon_ohlcv_service.trade.batcher import GlobalTradeBatcher
        from fullon_orm.models import Trade as ORMTrade

        # Create many mock trades
        mock_trades = []
        for i in range(150):
            mock_trade = MagicMock(spec=ORMTrade)
            mock_trade.time = time.time() + i
            mock_trade.price = 65000.0 + i
            mock_trade.volume = 0.1
            mock_trade.side = "buy"
            mock_trades.append(mock_trade)

        with (
            patch("fullon_ohlcv_service.trade.batcher.TradesCache") as mock_cache,
            patch("fullon_ohlcv_service.trade.batcher.TradeRepository") as mock_repo,
        ):
            mock_cache_instance = AsyncMock()
            mock_cache.return_value.__aenter__.return_value = mock_cache_instance
            mock_cache_instance.get_trades.return_value = mock_trades

            mock_repo_instance = AsyncMock()
            mock_repo.return_value.__aenter__.return_value = mock_repo_instance
            mock_repo_instance.save_trades.return_value = True

            batcher = GlobalTradeBatcher()
            await batcher.register_symbol("kraken", "BTC/USDC")

            # Process batch
            trades_processed = await batcher._process_single_symbol("kraken", "BTC/USDC")

            # Verify all trades were processed
            assert trades_processed == 150
            mock_repo_instance.save_trades.assert_called_once()
            saved_trades = mock_repo_instance.save_trades.call_args[0][0]
            assert len(saved_trades) == 150

    @pytest.mark.asyncio
    async def test_timing_precision(self):
        """Test that batch processing timing is precise."""
        from fullon_ohlcv_service.trade.batcher import GlobalTradeBatcher

        batcher = GlobalTradeBatcher()

        # Test calculation of next batch time
        current_time = time.time()
        next_time = batcher._calculate_next_batch_time()

        # Should be aligned to minute boundary minus 0.1 seconds
        seconds_in_minute = next_time % 60
        assert 59.8 <= seconds_in_minute <= 60 or seconds_in_minute <= 0.2
