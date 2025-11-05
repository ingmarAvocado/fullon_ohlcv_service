"""
Historic OHLCV Collector

Clean rewrite following live_collector.py patterns for parallel historical
OHLCV data collection using REST APIs. Collects historical candle data
and stores it in the database using symbol backtest periods.
"""

import asyncio
import os
from datetime import UTC, datetime, timedelta

from fullon_cache import ProcessCache
from fullon_cache.process_cache import ProcessStatus, ProcessType
from fullon_exchange.queue import ExchangeQueue
from fullon_log import get_component_logger
from fullon_ohlcv.models import Candle
from fullon_ohlcv.repositories.ohlcv import CandleRepository
from fullon_orm import DatabaseContext
from fullon_orm.models import Exchange, Symbol

from .utils import timeframe_to_seconds, convert_to_candle_objects

logger = get_component_logger("fullon.ohlcv.historic")


def _format_time_remaining(seconds: float) -> str:
    """
    Format time remaining in human-readable format.

    Args:
        seconds: Time remaining in seconds

    Returns:
        Formatted string like "111597 seconds (1.3 days)"
    """
    if seconds >= 86400:  # >= 1 day
        days = seconds / 86400
        return f"{seconds:.0f} seconds ({days:.1f} days)"
    elif seconds >= 3600:  # >= 1 hour
        hours = seconds / 3600
        return f"{seconds:.0f} seconds ({hours:.1f} hours)"
    elif seconds >= 60:  # >= 1 minute
        minutes = seconds / 60
        return f"{seconds:.0f} seconds ({minutes:.1f} minutes)"
    else:
        return f"{seconds:.0f} seconds"


class HistoricOHLCVCollector:
    """
    Clean historical OHLCV collector following live_collector.py patterns.

    Orchestrates parallel collection of historical OHLCV data from all configured
    exchanges and symbols using database-driven configuration.
    """

    def __init__(self, symbols: list | None = None):
        self.symbols = symbols or []
        self.running = False

    async def start_collection(self) -> dict[str, int]:
        """
        Start historical collection for all configured symbols.

        Returns:
            Dict mapping symbol keys to number of candles collected
        """
        if self.running:
            logger.warning("Historical collection already running")
            return {}

        self.running = True
        logger.info("Starting historical OHLCV collection")

        results = {}

        try:
            # Load symbols and admin exchanges
            symbols_by_exchange, admin_exchanges = await self._load_data()

            # Collect for each exchange in parallel
            exchange_tasks = []
            for exchange_name, symbols in symbols_by_exchange.items():
                # Find matching admin exchange
                admin_exchange = None
                for exchange in admin_exchanges:
                    if exchange.cat_exchange.name == exchange_name:
                        admin_exchange = exchange
                        break

                if not admin_exchange:
                    logger.warning("No admin exchange found for collection", exchange=exchange_name)
                    continue

                task = asyncio.create_task(
                    self._start_exchange_historic_collector(admin_exchange, symbols)
                )
                exchange_tasks.append(task)

            # Wait for all exchanges to complete
            exchange_results = await asyncio.gather(*exchange_tasks, return_exceptions=True)

            # Aggregate results
            for result in exchange_results:
                if isinstance(result, Exception):
                    logger.error("Exchange collection failed", error=str(result))
                elif isinstance(result, dict):
                    results.update(result)

        except Exception as e:
            logger.error("Error in historical collection orchestration", error=str(e))
            raise

        total_candles = sum(results.values())
        logger.info(
            "Historical collection completed",
            total_symbols=len(results),
            total_candles=total_candles,
        )

        return results

    async def start_symbol(self, symbol: Symbol) -> int:
        """Start historic collection for a specific symbol.

        Args:
            symbol: Symbol to collect data for

        Returns:
            Number of candles collected

        Raises:
            ValueError: If admin exchange not found
        """
        from ..utils.admin_helper import get_admin_exchanges

        # Get admin exchanges
        _, admin_exchanges = await get_admin_exchanges()

        # Find admin exchange for this symbol
        admin_exchange = None
        for exchange in admin_exchanges:
            if exchange.cat_exchange.name == symbol.cat_exchange.name:
                admin_exchange = exchange
                break

        if not admin_exchange:
            raise ValueError(f"Admin exchange {symbol.cat_exchange.name} not found")

        # Call _start_exchange_historic_collector with single symbol
        results = await self._start_exchange_historic_collector(admin_exchange, [symbol])

        # Return count for this symbol
        symbol_key = f"{symbol.cat_exchange.name}:{symbol.symbol}"
        return results.get(symbol_key, 0)

    async def _load_data(self) -> tuple[dict[str, list[Symbol]], list[Exchange]]:
        """Load admin exchanges and group symbols by exchange."""
        admin_email = os.getenv("ADMIN_MAIL", "admin@fullon")

        async with DatabaseContext() as db:
            # Get admin user
            admin_uid = await db.users.get_user_id(admin_email)
            if not admin_uid:
                raise ValueError(f"Admin user {admin_email} not found")

            # Load exchanges (symbols are already provided)
            admin_exchanges = await db.exchanges.get_user_exchanges(admin_uid)
            self.symbols = await db.symbols.get_all()
        logger.info(
            "Loaded data", symbol_count=len(self.symbols), exchange_count=len(admin_exchanges)
        )

        # Group symbols by exchange
        symbols_by_exchange = {}
        for symbol in self.symbols:
            exchange_name = symbol.cat_exchange.name
            if exchange_name not in symbols_by_exchange:
                symbols_by_exchange[exchange_name] = []
            symbols_by_exchange[exchange_name].append(symbol)
        return symbols_by_exchange, admin_exchanges

    async def _start_exchange_historic_collector(
        self, exchange_obj: Exchange, symbols: list[Symbol]
    ) -> dict[str, int]:
        """Collect historical data for all symbols in one exchange."""
        exchange_name = exchange_obj.cat_exchange.name
        results = {}

        logger.info(
            "Starting historical collection for exchange",
            exchange=exchange_name,
            symbol_count=len(symbols),
        )

        # Get REST handler
        try:
            handler = await ExchangeQueue.get_rest_handler(exchange_obj)
        except Exception as e:
            logger.error(
                "Failed to get REST handler for exchange",
                exchange=exchange_name,
                error=str(e),
            )
            return results

        # Check if this exchange needs trade collection instead of OHLCV
        if handler.needs_trades_for_ohlcv():
            logger.info(
                "Exchange requires trade collection instead of OHLCV - skipping OHLCV collection",
                exchange=exchange_name,
                symbol_count=len(symbols),
            )
            return results

        logger.info(
            "Exchange supports native OHLCV - proceeding with collection",
            exchange=exchange_name,
            symbol_count=len(symbols),
        )

        # Collect for all symbols in parallel
        symbol_tasks = []
        for symbol in symbols:
            task = asyncio.create_task(self._collect_symbol_historical(handler, symbol))
            symbol_tasks.append(task)

        # Wait for all symbols to complete
        symbol_results = await asyncio.gather(*symbol_tasks, return_exceptions=True)

        # Process results
        for i, result in enumerate(symbol_results):
            symbol = symbols[i]
            symbol_key = f"{exchange_name}:{symbol.symbol}"
            if isinstance(result, Exception):
                logger.error(
                    "Historical collection failed for symbol", symbol=symbol_key, error=str(result)
                )
                results[symbol_key] = 0
            else:
                results[symbol_key] = result
                logger.info(
                    "Historical collection completed for symbol",
                    symbol=symbol_key,
                    candles_collected=result,
                )

        return results

    async def _collect_symbol_historical(self, handler, symbol: Symbol) -> int:
        """Collect historical OHLCV data for a single symbol."""
        exchange_name = symbol.cat_exchange.name
        symbol_str = symbol.symbol

        # Register process
        async with ProcessCache() as cache:
            process_id = await cache.register_process(
                process_type=ProcessType.OHLCV,
                component=f"{exchange_name}:{symbol_str}",
                params={"exchange": exchange_name, "symbol": symbol_str, "type": "historic"},
                message="Starting historic OHLCV collection",
                status=ProcessStatus.STARTING,
            )

        # Calculate time range - check for existing data first
        async with CandleRepository(exchange_name, symbol_str) as repo:
            latest_timestamp = await repo.get_latest_timestamp()

            if latest_timestamp:
                # Resume from last collected data (add 1 second to avoid duplicate)
                start_timestamp = int((latest_timestamp.timestamp() + 1) * 1000)
                logger.info(
                    "Resuming collection from existing data",
                    symbol=f"{exchange_name}:{symbol_str}",
                    resume_from=latest_timestamp.format('YYYY-MM-DD HH:mm:ss')
                )
            else:
                # No data exists, start from backtest period
                start_timestamp = int(
                    (datetime.now(UTC) - timedelta(days=symbol.backtest)).timestamp() * 1000
                )
                logger.info(
                    "Starting fresh collection from backtest period",
                    symbol=f"{exchange_name}:{symbol_str}",
                    backtest_days=symbol.backtest,
                    start_from=datetime.fromtimestamp(start_timestamp/1000, tz=UTC).strftime('%Y-%m-%d %H:%M:%S')
                )

        current_timestamp = int(datetime.now(UTC).timestamp() * 1000)

        total_candles = 0
        since_timestamp = start_timestamp
        last_since_timestamp = None  # Track to detect infinite loop

        logger.debug(
            "Starting collection for symbol",
            symbol=f"{exchange_name}:{symbol_str}",
            backtest_days=symbol.backtest,
            start_timestamp=start_timestamp,
        )

        try:
            # Update status to running
            async with ProcessCache() as cache:
                await cache.update_process(
                    process_id=process_id,
                    status=ProcessStatus.RUNNING,
                    message="Collecting historical OHLCV data",
                )

            while since_timestamp < current_timestamp:
                try:
                    # Get batch of candles using symbol's configured timeframe
                    batch_candles = await handler.get_ohlcv(
                        symbol_str, timeframe=symbol.updateframe, since=since_timestamp, limit=1000
                    )

                    # Filter out None values that may be returned by the API
                    batch_candles = [c for c in batch_candles if c is not None]

                    # Drop incomplete current candle if present
                    if batch_candles:
                        current_minute = datetime.now(UTC).replace(second=0, microsecond=0)
                        last_candle = batch_candles[-1]
                        if isinstance(last_candle, list) and len(last_candle) >= 6:
                            ts = last_candle[0]
                            candle_dt = datetime.fromtimestamp(
                                ts / 1000 if ts > 1e12 else ts, tz=UTC
                            ).replace(second=0, microsecond=0)
                            # Drop if it's the current minute and volume is 0 (incomplete)
                            if candle_dt == current_minute and last_candle[5] == 0:
                                batch_candles = batch_candles[:-1]
                                logger.debug(
                                    "Dropped incomplete current candle",
                                    symbol=f"{exchange_name}:{symbol_str}",
                                    timestamp=candle_dt,
                                )

                    if not batch_candles:
                        logger.debug(
                            "No more candles available", symbol=f"{exchange_name}:{symbol_str}"
                        )
                        break

                    # Check if we received less than requested - indicates end of available data
                    if len(batch_candles) < 1000:  # Requested limit was 1000
                        logger.debug(
                            f"Received partial batch ({len(batch_candles)} < 1000 candles) - reached end of available data",
                            symbol=f"{exchange_name}:{symbol_str}"
                        )
                        # Process this final batch, then exit loop after saving
                        # (Don't break here - we want to save the final batch)

                    # Validate candle spacing AND check if exchange honored the 'since' parameter
                    if batch_candles:
                        first_ts = self._extract_timestamp(batch_candles[0])
                        first_ts_sec = first_ts / 1000 if first_ts > 1e12 else first_ts
                        requested_ts_sec = since_timestamp / 1000

                        # Check if exchange ignored our 'since' parameter (returned data much later than requested)
                        # Allow 1 hour tolerance for clock drift/rounding
                        time_gap_sec = first_ts_sec - requested_ts_sec
                        if time_gap_sec > 3600:  # More than 1 hour gap
                            days_gap = time_gap_sec / 86400
                            # Calculate available history: from first candle to now
                            current_time_sec = datetime.now(UTC).timestamp()
                            available_history_sec = current_time_sec - first_ts_sec
                            available_history_days = available_history_sec / 86400

                            logger.warning(
                                f"⚠️  {exchange_name} has limited historical OHLCV data availability. "
                                f"Requested data from {datetime.fromtimestamp(requested_ts_sec, tz=UTC).strftime('%Y-%m-%d %H:%M:%S')}, "
                                f"but exchange only returned data from {datetime.fromtimestamp(first_ts_sec, tz=UTC).strftime('%Y-%m-%d %H:%M:%S')} "
                                f"({days_gap:.1f} days later). "
                                f"This exchange appears to only maintain ~{available_history_days:.0f} days of {symbol.updateframe} data.",
                                symbol=f"{exchange_name}:{symbol_str}",
                                timeframe=symbol.updateframe,
                                requested_date=datetime.fromtimestamp(requested_ts_sec, tz=UTC).strftime('%Y-%m-%d'),
                                actual_date=datetime.fromtimestamp(first_ts_sec, tz=UTC).strftime('%Y-%m-%d'),
                                gap_days=days_gap,
                                available_days=available_history_days
                            )
                            # Update since_timestamp to continue from where data is actually available
                            since_timestamp = int(first_ts_sec * 1000)

                        # Validate candle spacing for configured timeframe (sparse data detection)
                        # Note: For daily (1d) and longer timeframes, weekends/holidays create natural gaps
                        # so we only validate minute/hour timeframes where continuous data is expected
                        if len(batch_candles) >= 2 and symbol.updateframe in ["1m", "5m", "15m", "30m", "1h", "4h"]:
                            last_ts = self._extract_timestamp(batch_candles[-1])
                            last_ts_sec = last_ts / 1000 if last_ts > 1e12 else last_ts

                            time_span_sec = last_ts_sec - first_ts_sec
                            timeframe_sec = timeframe_to_seconds(symbol.updateframe)
                            expected_span_sec = (len(batch_candles) - 1) * timeframe_sec

                            # Check if data is TOO SPARSE (downsampled) - allow 50% tolerance for gaps
                            # If actual span is more than 1.5x expected, data might be downsampled
                            if time_span_sec > expected_span_sec * 1.5:
                                logger.warning(
                                    f"⚠️  Exchange returned sparse data - candles may have gaps. "
                                    f"Got {len(batch_candles)} candles spanning {time_span_sec/3600:.1f} hours "
                                    f"(expected ~{expected_span_sec/3600:.1f} hours for continuous {symbol.updateframe} data). "
                                    f"Continuing collection but data may have missing periods.",
                                    symbol=f"{exchange_name}:{symbol_str}",
                                    timeframe=symbol.updateframe,
                                    span_hours=time_span_sec/3600,
                                    expected_hours=expected_span_sec/3600,
                                    ratio=time_span_sec/expected_span_sec
                                )
                                # Don't break - continue collection for sparse data

                    # Convert and save batch
                    candles = convert_to_candle_objects(batch_candles)
                    saved_successfully = False

                    if candles:
                        async with CandleRepository(exchange_name, symbol_str) as repo:
                            success = await repo.save_candles(candles)
                            if success:
                                total_candles += len(candles)
                                saved_successfully = True

                    # Check for partial batch (indicates end of available data)
                    partial_batch = len(batch_candles) < 1000

                    # If partial batch detected, break immediately regardless of save success
                    # This prevents infinite loops when single candles fail to save
                    if partial_batch:
                        if saved_successfully:
                            logger.info(
                                f"Processed final partial batch successfully - collection complete",
                                symbol=f"{exchange_name}:{symbol_str}",
                                total_candles=total_candles,
                                batch_size=len(batch_candles)
                            )
                        else:
                            logger.warning(
                                f"Partial batch ({len(batch_candles)} candles) failed to save - "
                                f"reached end of available data. This may occur when the exchange returns "
                                f"insufficient data for timeframe detection (minimum 2 candles required).",
                                symbol=f"{exchange_name}:{symbol_str}",
                                total_candles=total_candles
                            )
                        break

                    # Advance timestamp only if we haven't reached end of data
                    if batch_candles:
                        last_candle = batch_candles[-1]
                        last_timestamp = self._extract_timestamp(last_candle)
                        if last_timestamp > 1e12:  # milliseconds
                            last_timestamp_sec = last_timestamp / 1000
                        else:
                            last_timestamp_sec = last_timestamp

                        # Calculate time remaining
                        current_time_sec = current_timestamp / 1000
                        time_remaining_sec = current_time_sec - last_timestamp_sec

                        # Log progress with ACTUAL time range from candles (not requested time)
                        first_candle = batch_candles[0]
                        first_candle_ts = self._extract_timestamp(first_candle)
                        first_candle_ts_sec = first_candle_ts / 1000 if first_candle_ts > 1e12 else first_candle_ts

                        from_time = datetime.fromtimestamp(first_candle_ts_sec, tz=UTC).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                        to_time = datetime.fromtimestamp(last_timestamp_sec, tz=UTC).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )

                        logger.info(
                            f"Retrieved {len(batch_candles)} candles for {symbol_str}:{exchange_name} "
                            f"from {from_time} to {to_time}. "
                            f"{_format_time_remaining(time_remaining_sec)} left"
                        )

                        # Advance by timeframe interval instead of 1 second
                        # For daily data, this means advancing by 1 day (86400s) instead of 1s
                        timeframe_seconds = timeframe_to_seconds(symbol.updateframe)
                        new_since_timestamp = int((last_timestamp_sec + timeframe_seconds) * 1000)

                        # Check if timestamp hasn't advanced (stuck in loop)
                        if last_since_timestamp is not None and new_since_timestamp <= last_since_timestamp:
                            logger.warning(
                                f"No progress in timestamp advancement - stopping collection",
                                symbol=f"{exchange_name}:{symbol_str}",
                                last_timestamp=last_since_timestamp,
                                new_timestamp=new_since_timestamp
                            )
                            break

                        last_since_timestamp = since_timestamp
                        since_timestamp = new_since_timestamp

                    # Rate limiting
                    await asyncio.sleep(0.1)

                except Exception as e:
                    logger.error(
                        "Error collecting batch for symbol",
                        symbol=f"{exchange_name}:{symbol_str}",
                        error=str(e),
                    )
                    raise

            # Update process status on completion
            async with ProcessCache() as cache:
                await cache.update_process(
                    process_id=process_id,
                    status=ProcessStatus.STOPPED,
                    message=f"Collected {total_candles} candles",
                )

        except Exception as e:
            # Update process status on error
            async with ProcessCache() as cache:
                await cache.update_process(
                    process_id=process_id, status=ProcessStatus.ERROR, message=f"Error: {str(e)}"
                )
            raise

        logger.debug(
            "Collection completed for symbol",
            symbol=f"{exchange_name}:{symbol_str}",
            total_candles=total_candles,
        )

        return total_candles

    def _extract_timestamp(self, candle) -> float:
        """Extract timestamp from candle data."""
        if isinstance(candle, list) and len(candle) > 0:
            return candle[0]
        if isinstance(candle, dict):
            return candle.get("timestamp", candle.get("datetime", 0))
        return getattr(candle, "timestamp", getattr(candle, "datetime", 0))
