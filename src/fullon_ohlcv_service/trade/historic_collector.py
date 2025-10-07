"""
Historic Trade Collector

Clean rewrite following live_collector.py patterns for parallel historical
trade data collection using REST APIs. Collects historical public trade data
and stores it in the database using symbol backtest periods.
"""

import asyncio
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

from fullon_exchange.queue import ExchangeQueue
from fullon_log import get_component_logger
from fullon_ohlcv.models import Trade
from fullon_ohlcv.repositories.ohlcv import TradeRepository
from fullon_orm import DatabaseContext
from fullon_orm.models import Symbol, Exchange

logger = get_component_logger("fullon.trade.historic")


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


class HistoricTradeCollector:
    """
    Clean historical trade collector following live_collector.py patterns.

    Orchestrates parallel collection of historical trade data from all configured
    exchanges and symbols using database-driven configuration.
    """

    def __init__(self):
        self.running = False

    async def start_collection(self) -> Dict[str, int]:
        """
        Start historical collection for all configured symbols.

        Returns:
            Dict mapping symbol keys to number of trades collected
        """
        if self.running:
            logger.warning("Historical collection already running")
            return {}

        self.running = True
        logger.info("Starting historical trade collection")

        results = {}

        try:
            # Initialize ExchangeQueue factory
            await ExchangeQueue.initialize_factory()

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
        finally:
            await self._cleanup()

        total_trades = sum(results.values())
        logger.info(
            "Historical collection completed", total_symbols=len(results), total_trades=total_trades
        )

        return results

    async def _load_data(self) -> tuple[Dict[str, List[Symbol]], List[Exchange]]:
        """Load symbols and admin exchanges from database."""
        admin_email = os.getenv("ADMIN_MAIL", "admin@fullon")

        async with DatabaseContext() as db:
            # Get admin user
            admin_uid = await db.users.get_user_id(admin_email)
            if not admin_uid:
                raise ValueError(f"Admin user {admin_email} not found")

            # Load symbols and exchanges
            all_symbols = await db.symbols.get_all()
            admin_exchanges = await db.exchanges.get_user_exchanges(admin_uid)

            logger.info(f"Loaded {len(all_symbols)} symbols from database")

            # Group symbols by exchange
            symbols_by_exchange = {}
            for symbol in all_symbols:
                exchange_name = symbol.cat_exchange.name
                if exchange_name not in symbols_by_exchange:
                    symbols_by_exchange[exchange_name] = []
                symbols_by_exchange[exchange_name].append(symbol)

            return symbols_by_exchange, admin_exchanges

    async def _start_exchange_historic_collector(
        self, exchange_obj: Exchange, symbols: List[Symbol]
    ) -> Dict[str, int]:
        """Collect historical data for all symbols in one exchange."""
        exchange_name = exchange_obj.cat_exchange.name
        results = {}

        logger.info(
            "Starting historical collection for exchange",
            exchange=exchange_name,
            symbol_count=len(symbols),
        )

        # Ensure ExchangeQueue factory is initialized
        try:
            await ExchangeQueue.initialize_factory()
        except RuntimeError:
            # Already initialized - this is fine
            pass

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

        # Check if this exchange needs trade collection for OHLCV
        if not handler.needs_trades_for_ohlcv():
            logger.info(
                "Exchange has native OHLCV support - skipping trade collection",
                exchange=exchange_name,
                symbol_count=len(symbols)
            )
            return results

        logger.info(
            "Exchange requires trade collection for OHLCV",
            exchange=exchange_name,
            symbol_count=len(symbols)
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
                    trades_collected=result,
                )

        return results

    async def _collect_symbol_historical(self, handler, symbol: Symbol) -> int:
        """Collect historical trade data for a single symbol."""
        exchange_name = symbol.cat_exchange.name
        symbol_str = symbol.symbol

        # Calculate time range
        start_timestamp = int(
            (datetime.now(timezone.utc) - timedelta(days=symbol.backtest)).timestamp() * 1000
        )
        current_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)

        total_trades = 0
        since_timestamp = start_timestamp

        logger.debug(
            "Starting collection for symbol",
            symbol=f"{exchange_name}:{symbol_str}",
            backtest_days=symbol.backtest,
            start_timestamp=start_timestamp,
        )

        while since_timestamp < current_timestamp:
            try:
                # Get batch of trades
                since_seconds = since_timestamp / 1000
                batch_trades = await handler.get_public_trades(
                    symbol_str, since=since_seconds, limit=1000
                )

                # Filter out None values that may be returned by the API
                batch_trades = [t for t in batch_trades if t is not None]

                if not batch_trades:
                    logger.debug("No more trades available", symbol=f"{exchange_name}:{symbol_str}")
                    break

                # Convert and save batch
                trades = self._convert_to_trade_objects(batch_trades)
                async with TradeRepository(exchange_name, symbol_str, test=False) as repo:
                    success = await repo.save_trades(trades)
                    if success:
                        total_trades += len(trades)

                # Advance timestamp
                if batch_trades:
                    last_trade = batch_trades[-1]
                    last_timestamp = self._extract_timestamp(last_trade)
                    if last_timestamp > 1e12:  # milliseconds
                        last_timestamp_sec = last_timestamp / 1000
                    else:
                        last_timestamp_sec = last_timestamp

                    # Calculate time remaining
                    current_time_sec = current_timestamp / 1000
                    time_remaining_sec = current_time_sec - last_timestamp_sec

                    # Log progress with time range
                    from_time = datetime.fromtimestamp(since_seconds, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                    to_time = datetime.fromtimestamp(last_timestamp_sec, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

                    logger.info(
                        f"Retrieved {len(batch_trades)} trades for {symbol_str}:{exchange_name} "
                        f"from {from_time} to {to_time}. "
                        f"{_format_time_remaining(time_remaining_sec)} left"
                    )

                    since_timestamp = int((last_timestamp_sec + 1) * 1000)

                # Rate limiting
                await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(
                    "Error collecting batch for symbol",
                    symbol=f"{exchange_name}:{symbol_str}",
                    error=str(e),
                )
                break

        logger.debug(
            "Collection completed for symbol",
            symbol=f"{exchange_name}:{symbol_str}",
            total_trades=total_trades,
        )

        return total_trades

    def _extract_timestamp(self, trade) -> float:
        """Extract timestamp from trade data."""
        if isinstance(trade, dict):
            return trade.get("timestamp", trade.get("datetime", 0))
        return getattr(trade, "timestamp", getattr(trade, "datetime", 0))

    def _convert_to_trade_objects(self, raw_trades: List) -> List[Trade]:
        """Convert raw trades to Trade objects."""
        trade_objects = []

        for raw_trade in raw_trades:
            try:
                # Extract data (handle both dict and object formats)
                if isinstance(raw_trade, dict):
                    timestamp = raw_trade.get("timestamp", raw_trade.get("datetime", 0))
                    price = raw_trade.get("price", 0.0)
                    amount = raw_trade.get("amount", 0.0)
                    side = raw_trade.get("side", "unknown")
                else:
                    timestamp = getattr(raw_trade, "timestamp", getattr(raw_trade, "datetime", 0))
                    price = getattr(raw_trade, "price", 0.0)
                    amount = getattr(raw_trade, "amount", 0.0)
                    side = getattr(raw_trade, "side", "unknown")

                # Convert timestamp to datetime
                timestamp_dt = datetime.fromtimestamp(
                    timestamp / 1000 if timestamp > 1e12 else timestamp,
                    tz=timezone.utc
                )

                trade_objects.append(Trade(
                    timestamp=timestamp_dt,
                    price=float(price),
                    volume=float(amount),
                    side=str(side),
                    type="market",
                ))
            except Exception as e:
                logger.warning("Failed to convert trade", error=str(e))
                continue

        return trade_objects

    async def _cleanup(self):
        """Clean up resources."""
        self.running = False
        try:
            await ExchangeQueue.shutdown_factory()
        except Exception as e:
            logger.error("Error shutting down ExchangeQueue", error=str(e))
