"""
Historic Trade Collector

Handles historical trade data collection via REST APIs.
Automatically transitions to live WebSocket streaming after completion.
"""

import asyncio
import os
import time
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Any

import arrow
from fullon_exchange.queue import ExchangeQueue
from fullon_log import get_component_logger
from fullon_ohlcv.models import Trade
from fullon_ohlcv.repositories.ohlcv import TradeRepository
from fullon_orm import DatabaseContext
from fullon_orm.models import Symbol, Exchange
from fullon_credentials import fullon_credentials

from .live_collector import LiveTradeCollector


class HistoricTradeCollector:
    """
    Historical trade data collector using REST APIs.

    Collects historical public trade data from exchanges and stores it
    in the fullon_ohlcv database using the symbol's backtest period.
    """

    def __init__(self, symbol_obj: Symbol) -> None:
        """
        Initialize historic trade collector for a specific symbol.

        Args:
            symbol_obj: Symbol object from fullon_orm database
        """
        self.symbol_obj = symbol_obj
        self.exchange = symbol_obj.exchange_name
        self.symbol = symbol_obj.symbol
        self.exchange_id: Optional[int] = None

        # Live streaming task management
        self._live_collector: Optional['LiveTradeCollector'] = None
        self._live_streaming_task: Optional[asyncio.Task] = None

        # Setup logging
        self.logger = get_component_logger(
            f"fullon.trade.historic.{self.exchange}.{self.symbol}"
        )

        self.logger.debug(
            "Historic trade collector created",
            symbol=self.symbol,
            exchange=self.exchange,
            backtest_days=self.symbol_obj.backtest
        )

    async def collect(self) -> bool:
        """
        Collect historical trade data and store in database.
        Automatically starts live streaming after historical collection completes.

        Returns:
            bool: True if collection successful, False otherwise
        """
        self.logger.info("Starting historical trade collection", symbol=self.symbol)

        await ExchangeQueue.initialize_factory()
        try:
            # Get admin's exchange object and credentials
            exchange_obj = await self.create_exchange_object()
            credential_provider = self.create_credential_provider()

            # Create REST handler
            handler = await ExchangeQueue.get_rest_handler(
                exchange_obj, credential_provider
            )
            await handler.connect()

            # Collect historical data with immediate batch saving
            total_trades_saved = await self.collect_historical_data(handler)

            self.logger.info(
                "Historical trade collection completed",
                symbol=self.symbol,
                total_trades_saved=total_trades_saved
            )

            # Start live streaming after historical collection
            if total_trades_saved > 0:
                await self._start_live_streaming()

            return total_trades_saved > 0

        except Exception as e:
            self.logger.error(
                "Historical trade collection failed",
                symbol=self.symbol,
                error=str(e),
                error_type=type(e).__name__
            )
            import traceback
            self.logger.debug("Full traceback", traceback=traceback.format_exc())
            return False

    async def create_exchange_object(self) -> Exchange:
        """Get admin user's exchange object for this symbol."""
        admin_email = os.getenv("ADMIN_MAIL", "admin@fullon")

        async with DatabaseContext() as db:
            admin_uid = await db.users.get_user_id(admin_email)
            if not admin_uid:
                raise ValueError(f"Admin user {admin_email} not found in database")

            user_exchanges = await db.exchanges.get_user_exchanges(admin_uid)

            # Find admin's exchange for this symbol's exchange type
            for exchange in user_exchanges:
                if exchange.get('cat_ex_id') == self.symbol_obj.cat_ex_id:
                    # Use selectinload to eagerly load the cat_exchange relationship
                    from sqlalchemy.orm import selectinload
                    from sqlalchemy import select

                    stmt = select(Exchange).options(selectinload(Exchange.cat_exchange)).where(Exchange.ex_id == exchange.get('ex_id'))
                    result = await db.session.execute(stmt)
                    exchange_obj = result.scalar_one_or_none()

                    if exchange_obj:
                        self.exchange_id = exchange_obj.ex_id
                        return exchange_obj

        # No exchange found - this is a configuration error
        raise ValueError(
            f"No exchange configured for admin user {admin_email} "
            f"with cat_ex_id {self.symbol_obj.cat_ex_id} ({self.exchange})"
        )

    def create_credential_provider(self):
        """Create credential provider - uses admin's credentials if available."""
        def credential_provider(exchange_obj: Any) -> tuple[str, str]:
            if self.exchange_id:
                try:
                    secret, key = fullon_credentials(ex_id=self.exchange_id)
                    return key, secret  # fullon_credentials returns (secret, key), exchange expects (key, secret)
                except Exception:
                    pass
            return "", ""  # Empty credentials for public data

        return credential_provider

    async def collect_historical_data(self, handler: Any) -> int:
        """
        Collect historical trade data using REST API and save each batch immediately.

        Args:
            handler: Exchange handler from fullon_exchange

        Returns:
            Total number of trades saved to database
        """
        start_collection = time.time()

        # Calculate the theoretical start date (for informational purposes)
        now = datetime.now()
        theoretical_start_date = now - timedelta(days=self.symbol_obj.backtest)

        # Log what we're requesting
        self.logger.info(
            "Historical trade data request details",
            symbol=self.symbol,
            backtest_days=self.symbol_obj.backtest,
            theoretical_start=theoretical_start_date.strftime('%Y-%m-%d %H:%M:%S'),
            theoretical_end=now.strftime('%Y-%m-%d %H:%M:%S')
        )

        self.logger.info(
            f"Collecting historical trade data for {self.symbol} from {theoretical_start_date.strftime('%Y-%m-%d')} ({self.symbol_obj.backtest} days)",
            symbol=self.symbol,
            exchange=self.exchange,
            backtest_days=self.symbol_obj.backtest,
            theoretical_start_date=theoretical_start_date.strftime('%Y-%m-%d %H:%M:%S')
        )

        try:
            # Calculate since timestamp in milliseconds for the backtest period
            start_timestamp = int((datetime.now() - timedelta(days=self.symbol_obj.backtest)).timestamp() * 1000)
            current_timestamp = int(datetime.now().timestamp() * 1000)

            total_trades_saved = 0
            since_timestamp = start_timestamp
            max_iterations = 100  # Prevent infinite loops
            iteration = 0

            self.logger.info(f"Starting progressive collection from {start_timestamp} to {current_timestamp}")

            while since_timestamp < current_timestamp and iteration < max_iterations:
                iteration += 1

                # Get trades from current timestamp onwards (convert to seconds for API)
                since_seconds = since_timestamp / 1000
                batch_trades = await handler.get_public_trades(
                    self.symbol, since=since_seconds, limit=1000
                )

                if not batch_trades:
                    self.logger.debug(f"No more trades available from timestamp {since_timestamp}")
                    break

                # Convert batch to trade objects
                trades = self.convert_to_trade_objects(batch_trades)

                # Save this batch immediately to database
                try:
                    async with TradeRepository(
                        self.exchange, self.symbol, test=False
                    ) as repo:
                        success = await repo.save_trades(trades)
                        if success:
                            total_trades_saved += len(trades)
                        else:
                            self.logger.warning(f"Batch {iteration}: failed to save {len(trades)} trades")
                except Exception as db_error:
                    self.logger.error(f"Batch {iteration}: database save failed: {db_error}")
                    # Continue with next batch instead of failing completely
                    continue

                # Get the timestamp of the last trade for next iteration
                last_trade = batch_trades[-1]
                last_timestamp = self.extract_timestamp(last_trade)

                # Convert to milliseconds if needed
                if last_timestamp <= 1e12:  # seconds
                    last_timestamp_ms = int(last_timestamp * 1000)
                else:  # already milliseconds
                    last_timestamp_ms = int(last_timestamp)

                # Calculate progress: how many seconds left until current time
                seconds_left = (current_timestamp - last_timestamp_ms) / 1000

                # Show progress every few batches or when close to completion
                if iteration % 3 == 0 or seconds_left < 3600:  # Every 3rd batch or when less than 1 hour left
                    if seconds_left > 86400:  # More than 1 day
                        time_left = f"{seconds_left / 86400:.1f} days"
                    elif seconds_left > 3600:  # More than 1 hour
                        time_left = f"{seconds_left / 3600:.1f} hours"
                    elif seconds_left > 60:  # More than 1 minute
                        time_left = f"{seconds_left / 60:.1f} minutes"
                    else:  # Less than 1 minute
                        time_left = f"{seconds_left:.0f} seconds"

                    self.logger.info(f"Getting trades for {self.symbol}:{self.exchange} - {time_left} left (before catching up to the present)")

                # Follow legacy pattern: advance by small increment (0.000001 seconds = 0.001 ms)
                next_since_seconds = last_timestamp + 0.000001 if last_timestamp <= 1e12 else (last_timestamp / 1000) + 0.000001
                next_since_timestamp = int(next_since_seconds * 1000)

                # Break if no progression (same timestamp returned)
                if next_since_timestamp <= since_timestamp:
                    self.logger.debug(f"No timestamp progression: {since_timestamp} -> {next_since_timestamp}")
                    break

                # Update since_timestamp to continue from last trade + small increment
                since_timestamp = next_since_timestamp

                # Break if we've reached current time
                if since_timestamp >= current_timestamp:
                    break

                # Small delay to avoid rate limiting
                await asyncio.sleep(0.1)

            collection_time = time.time() - start_collection

            self.logger.info(f"Trade collection completed for {self.symbol}:{self.exchange} - {total_trades_saved} trades saved in {collection_time:.1f}s")

            return total_trades_saved

        except Exception as e:
            self.logger.error(
                "Error collecting historical trade data",
                symbol=self.symbol,
                error=str(e)
            )
            return 0

    def extract_timestamp(self, trade: Any) -> float:
        """Extract timestamp from trade data regardless of format."""
        if isinstance(trade, dict):
            return trade.get('timestamp', trade.get('datetime', 0))
        elif hasattr(trade, 'timestamp'):
            return trade.timestamp
        elif hasattr(trade, 'datetime'):
            return trade.datetime
        return 0

    def convert_to_trade_objects(self, raw_trades: List[Any]) -> List[Trade]:
        """
        Convert raw trade data to Trade objects for database storage.

        Args:
            raw_trades: List of raw trade data from exchange

        Returns:
            List of Trade objects ready for database storage
        """
        trade_objects = []

        for raw_trade in raw_trades:
            try:
                # Handle both dict format and object format
                if isinstance(raw_trade, dict):
                    timestamp = raw_trade.get('timestamp', raw_trade.get('datetime', 0))
                    price = raw_trade.get('price', 0.0)
                    amount = raw_trade.get('amount', 0.0)
                    side = raw_trade.get('side', 'unknown')
                else:
                    # Handle object format (CCXT trade object)
                    timestamp = getattr(raw_trade, 'timestamp', getattr(raw_trade, 'datetime', 0))
                    price = getattr(raw_trade, 'price', 0.0)
                    amount = getattr(raw_trade, 'amount', 0.0)
                    side = getattr(raw_trade, 'side', 'unknown')

                # Convert timestamp to datetime object
                if timestamp > 1e12:  # milliseconds
                    timestamp_dt = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
                else:  # seconds
                    timestamp_dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)

                trade = Trade(
                    timestamp=timestamp_dt,
                    price=float(price),
                    volume=float(amount),  # Trade model uses 'volume' not 'amount'
                    side=str(side),
                    type='market'  # Default trade type
                )
                trade_objects.append(trade)
            except (ValueError, KeyError, AttributeError) as e:
                self.logger.warning("Failed to convert trade", error=str(e), raw_trade=str(raw_trade)[:100])
                continue

        return trade_objects

    async def _start_live_streaming(self) -> None:
        """
        Start live WebSocket streaming after historical collection completes.

        This provides seamless transition from historical to real-time data.
        """
        try:
            self.logger.info(
                "Starting live trade streaming",
                symbol=self.symbol,
                exchange=self.exchange
            )

            # Create and start live collector
            self._live_collector = LiveTradeCollector(self.symbol_obj)

            # Start streaming in background and store task reference
            self._live_streaming_task = asyncio.create_task(
                self._live_collector.start_streaming()
            )

            self.logger.info(
                "Live streaming initiated successfully",
                symbol=self.symbol,
                exchange=self.exchange
            )

        except Exception as e:
            self.logger.error(
                "Failed to start live streaming",
                symbol=self.symbol,
                exchange=self.exchange,
                error=str(e)
            )
            # Don't fail the historical collection if live streaming fails
            # Historical data is still valuable on its own

    async def stop(self) -> None:
        """
        Stop the historic collector and any associated live streaming.

        This ensures proper cleanup of WebSocket connections and background tasks.
        """
        try:
            self.logger.info(
                "Stopping historic trade collector",
                symbol=self.symbol,
                exchange=self.exchange
            )

            # Stop live streaming if it's running
            if self._live_collector:
                self.logger.debug("Stopping live trade collector")
                await self._live_collector.stop_streaming()

            # Cancel live streaming task if it exists
            if self._live_streaming_task and not self._live_streaming_task.done():
                self.logger.debug("Cancelling live streaming task")
                self._live_streaming_task.cancel()
                try:
                    await self._live_streaming_task
                except asyncio.CancelledError:
                    self.logger.debug("Live streaming task cancelled successfully")

            self.logger.info(
                "Historic trade collector stopped successfully",
                symbol=self.symbol,
                exchange=self.exchange
            )

        except Exception as e:
            self.logger.error(
                "Error stopping historic trade collector",
                symbol=self.symbol,
                exchange=self.exchange,
                error=str(e)
            )