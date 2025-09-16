from __future__ import annotations

"""Trade Collector - Single symbol trade data collection.

Simple integration that uses fullon_exchange for data retrieval/streaming and
fullon_ohlcv repositories for persistence. Mirrors the structure used by the
OHLCV collector to keep patterns consistent across the service.
"""

import arrow
from fullon_exchange.queue import ExchangeQueue
from fullon_log import get_component_logger
from fullon_ohlcv.models import Trade
from fullon_ohlcv.repositories.ohlcv import TradeRepository


class TradeCollector:
    """Simple trade collector - integrates exchange + storage."""

    def __init__(self, exchange: str, symbol: str, exchange_id: int = None):
        self.exchange = exchange
        self.symbol = symbol
        self.exchange_id = exchange_id
        self.running = False

    def _format_timestamp_range(self, trades: list[Trade]) -> str:
        """Format trade timestamp range for logging."""
        if not trades:
            return "No trades"

        timestamps = [t.timestamp for t in trades]
        start_time = min(timestamps)
        end_time = max(timestamps)
        start_str = arrow.get(start_time).format('YYYY-MM-DD HH:mm:ss UTC')
        end_str = arrow.get(end_time).format('YYYY-MM-DD HH:mm:ss UTC')

        # Calculate time span
        time_span = end_time - start_time
        hours = time_span / 3600

        if hours < 1:
            span_str = f"{time_span/60:.1f} minutes"
        elif hours < 24:
            span_str = f"{hours:.1f} hours"
        else:
            span_str = f"{hours/24:.1f} days"

        return f"{start_str} to {end_str} ({span_str})"

    def _calculate_lag_info(self, trades: list[Trade]) -> dict:
        """Calculate how far behind we are from current time."""
        if not trades:
            return {"seconds_behind": 0, "lag_description": "No trades"}

        import time
        now = time.time()
        latest_trade_time = max(t.timestamp for t in trades)
        seconds_behind = now - latest_trade_time

        # Format lag description
        if seconds_behind < 60:
            lag_desc = f"{seconds_behind:.0f} seconds behind"
        elif seconds_behind < 3600:
            lag_desc = f"{seconds_behind/60:.1f} minutes behind"
        elif seconds_behind < 86400:
            lag_desc = f"{seconds_behind/3600:.1f} hours behind"
        else:
            lag_desc = f"{seconds_behind/86400:.1f} days behind"

        return {
            "seconds_behind": int(seconds_behind),
            "lag_description": lag_desc
        }

    async def collect_historical_trades(self, symbol_obj=None) -> bool:
        """Collect a recent batch of trades and persist them.

        Mirrors the legacy "fetch_individual_trades" flow using fullon_exchange
        and saves via TradeRepository.

        Args:
            symbol_obj: Optional Symbol model with backtest parameter
        """
        await ExchangeQueue.initialize_factory()
        try:
            # Create exchange object for new API
            exchange_obj = self._create_exchange_object()
            credential_provider = self._create_credential_provider()

            # Use get_rest_handler pattern from OhlcvCollector
            handler = await ExchangeQueue.get_rest_handler(exchange_obj, credential_provider)
            await handler.connect()

            # Use get_trades method (standard fullon_exchange API)
            trades_data = await handler.get_trades(self.symbol, limit=1000)

            trades = [
                Trade(
                    timestamp=t["timestamp"],
                    price=t["price"],
                    volume=t["volume"],
                    side=t.get("side", "BUY"),
                    type=t.get("type", "MARKET"),
                )
                for t in trades_data
            ]

            # Log the date range before saving
            if trades:
                date_range = self._format_timestamp_range(trades)
                lag_info = self._calculate_lag_info(trades)
                logger = get_component_logger(f"fullon.trade.{self.exchange}.{self.symbol}")
                logger.info("Saving historical trade batch",
                           symbol=self.symbol,
                           count=len(trades),
                           date_range=date_range,
                           seconds_behind=lag_info["seconds_behind"],
                           lag=lag_info["lag_description"])

            async with TradeRepository(self.exchange, self.symbol, test=False) as repo:
                success = await repo.save_trades(trades)

            # Log the result with date range
            logger = get_component_logger(f"fullon.trade.{self.exchange}.{self.symbol}")
            if trades:
                date_range = self._format_timestamp_range(trades)
                lag_info = self._calculate_lag_info(trades)
                logger.info("Historical trade batch saved",
                           symbol=self.symbol,
                           count=len(trades),
                           success=success,
                           date_range=date_range,
                           seconds_behind=lag_info["seconds_behind"],
                           lag=lag_info["lag_description"])
            else:
                logger.info("No historical trades to save",
                           symbol=self.symbol)
            return success
        finally:
            await ExchangeQueue.shutdown_factory()

    async def collect_historical_range(self, symbol_obj, test_mode: bool = False) -> bool:
        """Collect historical trades from symbol.backtest days ago until caught up.

        Implements the legacy loop pattern:
        1. Get latest timestamp from database OR use symbol.backtest days ago
        2. Loop fetching trades since last timestamp until caught up
        3. Handle errors gracefully and log progress

        Args:
            symbol_obj: Symbol model with backtest parameter (days of history)
            test_mode: If True, only run one iteration for testing

        Returns:
            bool: Success status
        """
        logger = get_component_logger(f"fullon.trade.{self.exchange}.{self.symbol}")

        # Get backtest days from symbol (default 30 if not specified)
        backtest_days = getattr(symbol_obj, 'backtest', 30) if symbol_obj else 30

        # Calculate start timestamp (backtest days ago)
        start_time = arrow.utcnow().shift(days=-backtest_days).timestamp()
        start_time_str = arrow.get(start_time).format('YYYY-MM-DD HH:mm:ss UTC')

        logger.info("Starting historical trade collection",
                   symbol=self.symbol,
                   backtest_days=backtest_days,
                   start_time=start_time_str,
                   test_mode=test_mode)

        await ExchangeQueue.initialize_factory()
        try:
            # Check latest timestamp from database
            from fullon_ohlcv.repositories.ohlcv import TradeRepository
            async with TradeRepository(self.exchange, self.symbol, test=False) as repo:
                latest_timestamp = await repo.get_latest_timestamp()

            # Use latest from database or start from backtest days ago
            if latest_timestamp:
                since_timestamp = latest_timestamp
                logger.info("Found existing trades in database",
                           symbol=self.symbol,
                           latest_trade=arrow.get(latest_timestamp).format('YYYY-MM-DD HH:mm:ss UTC'))
            else:
                since_timestamp = start_time
                logger.info("No existing trades found, starting from backtest date",
                           symbol=self.symbol,
                           start_date=start_time_str)

            # Create exchange objects
            exchange_obj = self._create_exchange_object()
            credential_provider = self._create_credential_provider()
            handler = await ExchangeQueue.get_rest_handler(exchange_obj, credential_provider)
            await handler.connect()

            batch_count = 0
            total_trades_collected = 0

            # Historical collection loop (like legacy pattern)
            while True:
                batch_count += 1

                # Convert timestamp to milliseconds for exchange API
                since_ms = int(since_timestamp * 1000)

                logger.info("Fetching historical trade batch",
                           symbol=self.symbol,
                           batch=batch_count,
                           since=arrow.get(since_timestamp).format('YYYY-MM-DD HH:mm:ss UTC'))

                # Fetch trades since timestamp
                trades_data = await handler.get_trades(self.symbol, since=since_ms, limit=1000)

                if not trades_data:
                    logger.info("No more trades available, historical collection complete",
                               symbol=self.symbol,
                               total_batches=batch_count,
                               total_trades=total_trades_collected)
                    break

                # Convert to Trade models
                trades = [
                    Trade(
                        timestamp=t["timestamp"],
                        price=t["price"],
                        volume=t["volume"],
                        side=t.get("side", "BUY"),
                        type=t.get("type", "MARKET"),
                    )
                    for t in trades_data
                ]

                # Log batch info with lag calculation
                if trades:
                    date_range = self._format_timestamp_range(trades)
                    lag_info = self._calculate_lag_info(trades)

                    logger.info("Saving historical trade batch",
                               symbol=self.symbol,
                               batch=batch_count,
                               count=len(trades),
                               date_range=date_range,
                               seconds_behind=lag_info["seconds_behind"],
                               lag=lag_info["lag_description"])

                    # Save to database
                    async with TradeRepository(self.exchange, self.symbol, test=False) as repo:
                        success = await repo.save_trades(trades)

                    if success:
                        total_trades_collected += len(trades)
                        # Update since_timestamp to latest trade timestamp
                        latest_trade_timestamp = max(t.timestamp for t in trades)

                        # Check if we're caught up (within 55 seconds like legacy)
                        import time
                        now = time.time()
                        time_difference = now - latest_trade_timestamp

                        logger.info("Historical trade batch saved",
                                   symbol=self.symbol,
                                   batch=batch_count,
                                   count=len(trades),
                                   success=success,
                                   date_range=date_range,
                                   seconds_behind=lag_info["seconds_behind"],
                                   lag=lag_info["lag_description"],
                                   time_difference=int(time_difference))

                        # Check if caught up or if we should continue
                        if since_timestamp == latest_trade_timestamp:
                            logger.info("No new trades in batch, collection complete",
                                       symbol=self.symbol)
                            break

                        if time_difference < 55:  # Like legacy threshold
                            logger.info("Caught up to recent trades (within 55 seconds)",
                                       symbol=self.symbol,
                                       seconds_behind=int(time_difference))
                            break

                        # Update timestamp for next iteration
                        since_timestamp = latest_trade_timestamp

                        # Break if in test mode (single batch)
                        if test_mode:
                            logger.info("Test mode: stopping after single batch",
                                       symbol=self.symbol)
                            break
                    else:
                        logger.error("Failed to save trade batch",
                                   symbol=self.symbol,
                                   batch=batch_count)
                        break
                else:
                    logger.warning("Empty trade batch received",
                                 symbol=self.symbol,
                                 batch=batch_count)
                    break

            logger.info("Historical trade collection completed",
                       symbol=self.symbol,
                       total_batches=batch_count,
                       total_trades=total_trades_collected,
                       backtest_days=backtest_days)

            return total_trades_collected > 0

        except Exception as e:
            logger.error("Error in historical trade collection",
                        symbol=self.symbol,
                        error=str(e))
            import traceback
            logger.error("Full traceback", traceback=traceback.format_exc())
            return False
        finally:
            await ExchangeQueue.shutdown_factory()

    async def start_streaming(self, callback=None) -> None:
        """Subscribe to trade stream and persist incoming trades."""
        await ExchangeQueue.initialize_factory()
        try:
            # Create exchange object for new API
            exchange_obj = self._create_exchange_object()
            credential_provider = self._create_credential_provider()

            # Use get_websocket_handler pattern from OhlcvCollector
            handler = await ExchangeQueue.get_websocket_handler(exchange_obj, credential_provider)
            await handler.connect()

            async def on_trade(trade_data):
                trade = Trade(
                    timestamp=trade_data["timestamp"],
                    price=trade_data["price"],
                    volume=trade_data["volume"],
                    side=trade_data.get("side", "BUY"),
                    type=trade_data.get("type", "MARKET"),
                )

                # Log before saving with timestamp
                logger = get_component_logger(f"fullon.trade.{self.exchange}.{self.symbol}")
                trade_time = arrow.get(trade.timestamp).format('YYYY-MM-DD HH:mm:ss UTC')
                lag_info = self._calculate_lag_info([trade])

                logger.info("Saving real-time trade",
                           symbol=self.symbol,
                           price=trade.price,
                           volume=trade.volume,
                           side=trade.side,
                           timestamp=trade_time,
                           seconds_behind=lag_info["seconds_behind"],
                           lag=lag_info["lag_description"])

                async with TradeRepository(self.exchange, self.symbol, test=False) as repo:
                    await repo.save_trades([trade])

                logger.info("Real-time trade saved",
                           symbol=self.symbol,
                           price=trade.price,
                           volume=trade.volume,
                           timestamp=trade_time,
                           seconds_behind=lag_info["seconds_behind"],
                           lag=lag_info["lag_description"])

                if callback:
                    await callback(trade_data)

            await handler.subscribe_trades(self.symbol, on_trade)
            self.running = True

        finally:
            await ExchangeQueue.shutdown_factory()

    async def stop_streaming(self):
        """Stop the streaming collection."""
        self.running = False
        await ExchangeQueue.shutdown_factory()
        logger = get_component_logger(f"fullon.trade.{self.exchange}.{self.symbol}")
        logger.info("Streaming stopped", symbol=self.symbol)

    def _create_exchange_object(self):
        """Create exchange object for new fullon_exchange API."""
        class SimpleExchange:
            def __init__(self, exchange_name: str, account_id: str):
                self.ex_id = f"{exchange_name}_{account_id}"
                self.uid = account_id
                self.test = False
                self.cat_exchange = type('CatExchange', (), {'name': exchange_name})()

        return SimpleExchange(self.exchange, "trade_account")

    def _create_credential_provider(self):
        """Create credential provider for new fullon_exchange API.

        Uses fullon_credentials service when exchange_id is available,
        falls back to empty credentials for public data.
        """
        def credential_provider(exchange_obj):
            # Use exchange_id for credentials if available, otherwise try to get admin's exchange ID
            if self.exchange_id:
                try:
                    from fullon_credentials import fullon_credentials
                    secret, key = fullon_credentials(ex_id=self.exchange_id)
                    logger = get_component_logger(f"fullon.trade.{self.exchange}.{self.symbol}")
                    logger.info(f"Using credentials for exchange_id {self.exchange_id}")
                    return key, secret  # Note: fullon_credentials returns (secret, key) but exchange expects (key, secret)

                except (ImportError, ValueError) as e:
                    logger = get_component_logger(f"fullon.trade.{self.exchange}.{self.symbol}")
                    logger.error(f"Could not get credentials for exchange_id {self.exchange_id}: {e}")
                    logger.error(f"Cannot collect private data for {self.exchange}:{self.symbol} without credentials")
                    return "", ""
            else:
                # No exchange_id provided - log this and use empty credentials for public data
                import os
                logger = get_component_logger(f"fullon.trade.{self.exchange}.{self.symbol}")
                admin_mail = os.getenv('ADMIN_MAIL', 'admin@fullon')
                logger.warning(f"No exchange_id provided for {self.exchange}:{self.symbol}")
                logger.warning(f"Admin {admin_mail} should have proper exchange_id mapping for private data access")
                logger.info(f"Using empty credentials for public data collection only")
                return "", ""

        return credential_provider
