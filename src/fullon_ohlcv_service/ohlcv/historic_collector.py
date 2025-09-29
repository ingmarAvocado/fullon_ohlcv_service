"""
Historic OHLCV Collector

Handles historical OHLCV data collection via REST APIs.
"""

import asyncio
import os
import time
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Any

import arrow
from fullon_exchange.queue import ExchangeQueue
from fullon_log import get_component_logger
from fullon_ohlcv.models import Candle
from fullon_ohlcv.repositories.ohlcv import CandleRepository
from fullon_orm import DatabaseContext
from fullon_orm.models import Symbol, Exchange
from fullon_credentials import fullon_credentials


class HistoricCollector:
    """
    Historical OHLCV data collector using REST APIs.

    Collects historical candle data from exchanges and stores it
    in the fullon_ohlcv database using the symbol's backtest period.
    """

    def __init__(self, symbol_obj: Symbol) -> None:
        """
        Initialize historic collector for a specific symbol.

        Args:
            symbol_obj: Symbol object from fullon_orm database
        """
        self.symbol_obj = symbol_obj
        self.exchange = symbol_obj.exchange_name
        self.symbol = symbol_obj.symbol
        self.exchange_id: Optional[int] = None

        # Setup logging
        self.logger = get_component_logger(
            f"fullon.ohlcv.historic.{self.exchange}.{self.symbol}"
        )

        self.logger.debug(
            "Historic collector created",
            symbol=self.symbol,
            exchange=self.exchange,
            backtest_days=self.symbol_obj.backtest
        )

    async def collect(self) -> bool:
        """
        Collect historical OHLCV data and store in database.

        Returns:
            bool: True if collection successful, False otherwise
        """
        self.logger.info("Starting historical OHLCV collection", symbol=self.symbol)

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

            # Collect historical data
            raw_candles = await self.collect_historical_data(handler)

            # Convert and store candles
            candles = self.convert_to_candle_objects(raw_candles)

            async with CandleRepository(
                self.exchange, self.symbol, test=False
            ) as repo:
                success = await repo.save_candles(candles)

            self.logger.info(
                "Historical collection completed",
                symbol=self.symbol,
                count=len(candles),
                success=success
            )
            return success

        except Exception as e:
            self.logger.error(
                "Historical collection failed",
                symbol=self.symbol,
                error=str(e)
            )
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

    async def collect_historical_data(self, handler: Any) -> List[List[Any]]:
        """
        Collect historical OHLCV data using REST API.

        Args:
            handler: Exchange handler from fullon_exchange

        Returns:
            List of raw candle data from exchange
        """
        if not handler.supports_ohlcv():
            self.logger.warning(
                "Exchange does not support OHLCV collection",
                exchange=self.exchange
            )
            return []

        start_collection = time.time()

        # Calculate historical limit from symbol.backtest (days to minutes)
        historical_limit = self.symbol_obj.backtest * 24 * 60

        # Calculate the theoretical start date (for informational purposes)
        now = datetime.now()
        theoretical_start_date = now - timedelta(days=self.symbol_obj.backtest)

        # Log what we're requesting
        self.logger.info(
            "Historical data request details",
            symbol=self.symbol,
            backtest_days=self.symbol_obj.backtest,
            requesting_minutes=historical_limit,
            theoretical_start=theoretical_start_date.strftime('%Y-%m-%d %H:%M:%S'),
            theoretical_end=now.strftime('%Y-%m-%d %H:%M:%S')
        )

        self.logger.info(
            f"Collecting historical OHLCV data for {self.symbol} from {theoretical_start_date.strftime('%Y-%m-%d')} ({self.symbol_obj.backtest} days)",
            symbol=self.symbol,
            exchange=self.exchange,
            backtest_days=self.symbol_obj.backtest,
            theoretical_start_date=theoretical_start_date.strftime('%Y-%m-%d %H:%M:%S')
        )

        try:
            # Calculate since timestamp in milliseconds for the backtest period
            since_timestamp = int((datetime.now() - timedelta(days=self.symbol_obj.backtest)).timestamp() * 1000)

            all_candles = await handler.get_ohlcv(
                self.symbol, "1m", since=since_timestamp
            )

            if all_candles:
                # Log collection summary with time range details
                first_candle_time = all_candles[0][0]
                last_candle_time = all_candles[-1][0]

                start_date = arrow.get(first_candle_time / 1000).format(
                    'YYYY-MM-DD HH:mm:ss UTC'
                )
                end_date = arrow.get(last_candle_time / 1000).format(
                    'YYYY-MM-DD HH:mm:ss UTC'
                )
                duration = (last_candle_time - first_candle_time) / 1000 / 3600

                collection_time = time.time() - start_collection
                rate = len(all_candles) / collection_time if collection_time > 0 else 0

                # Log the actual data received
                self.logger.info(
                    f"Historical data actually received: {len(all_candles)} candles from {start_date} to {end_date}",
                    symbol=self.symbol,
                    candles_received=len(all_candles),
                    duration_hours=f"{duration:.1f}",
                    actual_start=start_date,
                    actual_end=end_date
                )

                self.logger.info(
                    "Historical data range collected",
                    symbol=self.symbol,
                    start_date=start_date,
                    end_date=end_date,
                    duration_hours=f"{duration:.1f}",
                    candle_count=len(all_candles),
                    collection_time_seconds=f"{collection_time:.1f}",
                    rate_candles_per_second=f"{rate:.1f}"
                )

            return all_candles

        except Exception as e:
            self.logger.error(
                "Error collecting historical OHLCV data",
                symbol=self.symbol,
                error=str(e)
            )
            return []

    def convert_to_candle_objects(self, raw_candles: List[List[Any]]) -> List[Candle]:
        """
        Convert raw OHLCV data to Candle objects for database storage.

        Args:
            raw_candles: List of raw candle data from exchange

        Returns:
            List of Candle objects ready for database storage
        """
        candle_objects = []

        for raw_candle in raw_candles:
            if len(raw_candle) >= 6:  # [timestamp, open, high, low, close, volume]
                # Convert timestamp from milliseconds to datetime object
                timestamp_dt = datetime.fromtimestamp(
                    raw_candle[0] / 1000, tz=timezone.utc
                )

                candle = Candle(
                    timestamp=timestamp_dt,
                    open=float(raw_candle[1]),
                    high=float(raw_candle[2]),
                    low=float(raw_candle[3]),
                    close=float(raw_candle[4]),
                    vol=float(raw_candle[5]) if raw_candle[5] is not None else 0.0
                )
                candle_objects.append(candle)

        return candle_objects