from __future__ import annotations

"""Trade Collector - Single symbol trade data collection.

Two-phase collection following legacy pattern:
Phase 1: Historical catch-up via REST calls (like legacy fetch_individual_trades)
Phase 2: Real-time streaming via WebSocket (like legacy fetch_individual_trades_ws)
"""

import arrow
import asyncio
from datetime import datetime, timezone
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

    def _convert_timestamp(self, timestamp):
        """Convert timestamp to datetime object, handling both seconds and milliseconds."""
        if not isinstance(timestamp, (int, float)):
            return timestamp

        # If timestamp is in milliseconds (> 1e10), convert to seconds
        if timestamp > 1e10:
            timestamp = timestamp / 1000

        return datetime.fromtimestamp(timestamp, tz=timezone.utc)

    def _format_timestamp_range(self, trades: list[Trade]) -> str:
        """Format trade timestamp range for logging."""
        if not trades:
            return "No trades"

        timestamps = [t.timestamp for t in trades]
        start_time = min(timestamps)
        end_time = max(timestamps)

        # Convert to arrow for formatting
        start_str = arrow.get(start_time).format('YYYY-MM-DD HH:mm:ss UTC')
        end_str = arrow.get(end_time).format('YYYY-MM-DD HH:mm:ss UTC')

        # Calculate time span in seconds
        if hasattr(start_time, 'timestamp'):  # datetime object
            time_span_seconds = (end_time - start_time).total_seconds()
        else:  # unix timestamp
            time_span_seconds = end_time - start_time

        hours = time_span_seconds / 3600

        if hours < 1:
            span_str = f"{time_span_seconds/60:.1f} minutes"
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
        now_utc = datetime.fromtimestamp(time.time(), tz=timezone.utc)
        latest_trade_time = max(t.timestamp for t in trades)

        # Convert latest trade time to datetime if it's not already
        if hasattr(latest_trade_time, 'timestamp'):  # datetime object
            latest_dt = latest_trade_time
        else:  # unix timestamp
            latest_dt = datetime.fromtimestamp(latest_trade_time, tz=timezone.utc)

        # Calculate difference in seconds
        time_diff = now_utc - latest_dt
        seconds_behind = time_diff.total_seconds()

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

            # Phase 1: Historical trade collection (REST)
            trades_data = await self._collect_historical_trades(handler)

            trades = [
                Trade(
                    timestamp=self._convert_timestamp(t["timestamp"]),
                    price=float(t["price"]),
                    volume=float(t.get("amount", t.get("volume", 0))),  # Handle different field names
                    side=str(t.get("side", "BUY")),
                    type=str(t.get("type", "MARKET")),
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
            # Note: ExchangeQueue.shutdown_factory() should only be called at application shutdown
            pass

    async def collect_historical_range(self, symbol_obj, test_mode: bool = False) -> bool:
        """Collect historical trades from the oldest available timestamp until caught up.

        Automatically determines the best starting point by comparing:
        1. Oldest timestamp in database (get_oldest_timestamp)
        2. Symbol.backtest days ago
        Uses whichever is older to ensure complete historical coverage.

        Args:
            symbol_obj: Symbol model with backtest parameter (days of history)
            test_mode: If True, only run one iteration for testing

        Returns:
            bool: Success status
        """
        logger = get_component_logger(f"fullon.trade.{self.exchange}.{self.symbol}")

        # Get backtest days from symbol (default 30 if not specified)
        backtest_days = getattr(symbol_obj, 'backtest', 30) if symbol_obj else 30
        backtest_timestamp = arrow.utcnow().shift(days=-backtest_days).timestamp()

        logger.info("Starting historical trade collection",
                   symbol=self.symbol,
                   backtest_days=backtest_days,
                   test_mode=test_mode)

        await ExchangeQueue.initialize_factory()
        try:
            # Get both oldest and latest timestamps from database
            from fullon_ohlcv.repositories.ohlcv import TradeRepository
            async with TradeRepository(self.exchange, self.symbol, test=False) as repo:
                oldest_timestamp = await repo.get_oldest_timestamp()
                latest_timestamp = await repo.get_latest_timestamp()

            # Determine the starting timestamp: older of database oldest vs backtest
            if oldest_timestamp and oldest_timestamp < backtest_timestamp:
                since_timestamp = oldest_timestamp
                start_reason = f"database oldest ({arrow.get(oldest_timestamp).format('YYYY-MM-DD HH:mm:ss UTC')})"
            else:
                since_timestamp = backtest_timestamp
                start_reason = f"symbol.backtest {backtest_days} days ({arrow.get(backtest_timestamp).format('YYYY-MM-DD HH:mm:ss UTC')})"

            # If we have latest timestamp and it's newer than our start, use latest instead
            if latest_timestamp and latest_timestamp > since_timestamp:
                since_timestamp = latest_timestamp
                start_reason = f"database latest ({arrow.get(latest_timestamp).format('YYYY-MM-DD HH:mm:ss UTC')})"

            logger.info("Historical collection starting from",
                       symbol=self.symbol,
                       start_timestamp=arrow.get(since_timestamp).format('YYYY-MM-DD HH:mm:ss UTC'),
                       reason=start_reason)

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
                        timestamp=self._convert_timestamp(t["timestamp"]),
                        price=float(t["price"]),
                        volume=float(t["volume"]),
                        side=str(t.get("side", "BUY")),
                        type=str(t.get("type", "MARKET")),
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
            # Note: ExchangeQueue.shutdown_factory() should only be called at application shutdown
            pass

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
                # Check if trade_data is already a Trade object or a dictionary
                if hasattr(trade_data, 'timestamp') and hasattr(trade_data, 'price'):  # It's already a Trade object
                    trade = trade_data
                elif isinstance(trade_data, dict):  # It's a dictionary
                    trade = Trade(
                        timestamp=self._convert_timestamp(trade_data["timestamp"]),
                        price=float(trade_data["price"]),
                        volume=float(trade_data["volume"]),
                        side=str(trade_data.get("side", "BUY")),
                        type=str(trade_data.get("type", "MARKET")),
                    )
                else:
                    # Log what we received and skip
                    logger = get_component_logger(f"fullon.trade.{self.exchange}.{self.symbol}")
                    logger.debug("Unexpected trade data type",
                               data_type=type(trade_data).__name__,
                               has_timestamp=hasattr(trade_data, 'timestamp'),
                               has_price=hasattr(trade_data, 'price'),
                               attributes=dir(trade_data) if hasattr(trade_data, '__dict__') else 'no __dict__')
                    return

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
            # Note: ExchangeQueue.shutdown_factory() should only be called at application shutdown
            pass

    async def stop_streaming(self):
        """Stop the streaming collection."""
        self.running = False
        # Note: ExchangeQueue.shutdown_factory() should only be called at application shutdown
        logger = get_component_logger(f"fullon.trade.{self.exchange}.{self.symbol}")
        logger.info("Streaming stopped", symbol=self.symbol)

    def _create_exchange_object(self):
        """Create exchange object following modern fullon_exchange pattern."""
        from fullon_orm.models import CatExchange, Exchange

        # Create a CatExchange instance
        cat_exchange = CatExchange()
        cat_exchange.name = self.exchange
        cat_exchange.id = 1  # Mock ID for examples

        # Create Exchange instance with proper ORM structure
        exchange = Exchange()
        # Use exchange_id if available, otherwise default mapping
        exchange_id_mapping = {"kraken": 1, "bitmex": 2, "hyperliquid": 3}
        exchange.ex_id = self.exchange_id or exchange_id_mapping.get(self.exchange, 1)
        exchange.uid = "trade_account"
        exchange.test = False
        exchange.cat_exchange = cat_exchange

        return exchange

    def _create_credential_provider(self):
        """Create credential provider following modern fullon_exchange pattern."""
        from fullon_orm.models import Exchange

        def credential_provider(exchange_obj: Exchange) -> tuple[str, str]:
            try:
                from fullon_credentials import fullon_credentials
                secret, api_key = fullon_credentials(ex_id=exchange_obj.ex_id)
                return api_key, secret
            except ValueError as e:
                # Log warning but continue with empty credentials for public data
                logger = get_component_logger(f"fullon.trade.{self.exchange}.{self.symbol}")
                logger.warning(f"Failed to resolve credentials for exchange ID {exchange_obj.ex_id}: {e}")
                logger.info("Using empty credentials for public data collection")
                return "", ""

        return credential_provider

    async def _collect_historical_trades(self, handler, start_time=None, end_time=None) -> list:
        """Collect historical trades with pagination (Phase 1: REST)."""
        all_trades = []

        if start_time and end_time:
            # Paginated historical collection (point A to point B)
            current_time = start_time
            while current_time < end_time:
                try:
                    batch = await handler.get_public_trades(
                        self.symbol,
                        since=current_time,
                        limit=1000
                    )

                    if not batch:
                        break

                    all_trades.extend(batch)

                    # Update current_time to last trade timestamp + 1ms
                    if batch:
                        last_timestamp = batch[-1].get('timestamp', current_time)
                        current_time = last_timestamp + 1
                    else:
                        break

                    # Rate limiting
                    await asyncio.sleep(0.1)

                except Exception as e:
                    logger = get_component_logger(f"fullon.trade.{self.exchange}.{self.symbol}")
                    logger.error("Historical trades batch failed",
                               current_time=current_time, error=str(e))
                    break
        else:
            # Simple recent data collection (legacy style)
            all_trades = await handler.get_public_trades(self.symbol, limit=1000)

        logger = get_component_logger(f"fullon.trade.{self.exchange}.{self.symbol}")
        logger.info("Historical trade collection completed",
                   trades_collected=len(all_trades))
        return all_trades

    async def start_streaming(self) -> None:
        """Phase 2: Start WebSocket streaming (like legacy fetch_individual_trades_ws)."""
        await ExchangeQueue.initialize_factory()
        try:
            # Create exchange object for WebSocket streaming
            exchange_obj = self._create_exchange_object()
            credential_provider = self._create_credential_provider()

            handler = await ExchangeQueue.get_websocket_handler(exchange_obj, credential_provider)
            await handler.connect()

            async def on_trade_data(trade_data):
                # Check if trade_data is already a Trade object or a dictionary
                if hasattr(trade_data, 'timestamp') and hasattr(trade_data, 'price'):  # It's already a Trade object
                    trade = trade_data
                elif isinstance(trade_data, dict):  # It's a dictionary
                    trade = Trade(
                        timestamp=self._convert_timestamp(trade_data.get("timestamp")),
                        price=float(trade_data.get("price", 0)),
                        volume=float(trade_data.get("amount", trade_data.get("volume", 0))),  # Handle both field names
                        side=str(trade_data.get("side", "BUY")),
                        type=str(trade_data.get("type", "MARKET")),
                    )
                else:
                    # Log what we received and skip
                    logger = get_component_logger(f"fullon.trade.{self.exchange}.{self.symbol}")
                    logger.debug("Unexpected trade data type",
                               data_type=type(trade_data).__name__,
                               has_timestamp=hasattr(trade_data, 'timestamp'),
                               has_price=hasattr(trade_data, 'price'),
                               attributes=dir(trade_data) if hasattr(trade_data, '__dict__') else 'no __dict__')
                    return

                async with TradeRepository(self.exchange, self.symbol, test=False) as repo:
                    success = await repo.save_trades([trade])

                logger = get_component_logger(f"fullon.trade.{self.exchange}.{self.symbol}")
                logger.info("Real-time trade data saved",
                           symbol=self.symbol,
                           price=trade.price,
                           side=trade.side,
                           success=success)

            # Subscribe to trade stream
            await handler.subscribe_trades(self.symbol, on_trade_data)
            self.running = True

            logger = get_component_logger(f"fullon.trade.{self.exchange}.{self.symbol}")
            logger.info("Trade WebSocket streaming started", symbol=self.symbol)

        finally:
            # Note: ExchangeQueue.shutdown_factory() should only be called at application shutdown
            pass

    async def stop_streaming(self) -> None:
        """Stop trade streaming."""
        self.running = False
        logger = get_component_logger(f"fullon.trade.{self.exchange}.{self.symbol}")
        logger.info("Trade streaming stopped", symbol=self.symbol)
