"""
Live OHLCV Collector

Handles real-time OHLCV data collection using bulk initialization.
Implements clean fullon ecosystem integration patterns.
"""

import asyncio
import os
from datetime import UTC, datetime

from fullon_exchange.queue import ExchangeQueue
from fullon_log import get_component_logger
from fullon_ohlcv.models import Candle
from fullon_ohlcv.repositories.ohlcv import CandleRepository
from fullon_orm import DatabaseContext
from fullon_orm.models import Exchange, Symbol

logger = get_component_logger("fullon.ohlcv.live")


class LiveOHLCVCollector:
    """
    Bulk real-time OHLCV collector using WebSocket connections.

    Loads all symbols from database and starts WebSocket collection
    for each exchange with shared handlers.
    """

    def __init__(self):
        self.running = False
        self.websocket_handlers = {}
        self.registered_symbols = set()
        self.last_candle_timestamps = {}  # Track last saved candle timestamp per symbol

    async def start_collection(self) -> None:
        """Start live OHLCV collection for all configured symbols."""
        if self.running:
            logger.warning("Live collection already running")
            return

        self.running = True
        logger.info("Starting live OHLCV collection")

        try:
            # Initialize ExchangeQueue factory
            await ExchangeQueue.initialize_factory()

            # Load symbols and admin exchanges in single database session
            symbols_by_exchange, admin_exchanges = await self._load_data()

            # Start WebSocket collection for each exchange
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

                # Start WebSocket for this exchange
                await self._start_exchange_collector(admin_exchange, symbols)

            # Keep connections alive
            while self.running:
                await asyncio.sleep(30)
                # Check all handlers are still connected
                for exchange_name, handler in self.websocket_handlers.items():
                    if not handler.connected:
                        logger.warning("WebSocket disconnected", exchange=exchange_name)

        except Exception as e:
            logger.error("Error in live collection startup", error=str(e))
            raise
        finally:
            await self._cleanup()

    async def stop_collection(self) -> None:
        """Stop live OHLCV collection gracefully."""
        logger.info("Stopping live OHLCV collection")
        self.running = False

        await self._cleanup()

    async def _load_data(self) -> tuple[dict[str, list[Symbol]], list[Exchange]]:
        """Load symbols and admin exchanges in single database session."""
        admin_email = os.getenv("ADMIN_MAIL", "admin@fullon")

        async with DatabaseContext() as db:
            # Get admin user
            admin_uid = await db.users.get_user_id(admin_email)
            if not admin_uid:
                raise ValueError(f"Admin user {admin_email} not found")

            # Load symbols and exchanges in same session
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

    async def _start_exchange_collector(
        self, exchange_obj: Exchange, symbols: list[Symbol]
    ) -> None:
        """Start WebSocket collection for one exchange with symbol list."""

        exchange_name = exchange_obj.cat_exchange.name

        logger.info(
            "Starting WebSocket for exchange", exchange=exchange_name, symbol_count=len(symbols)
        )

        try:
            # Get WebSocket handler (auto-connects on creation)
            handler = await ExchangeQueue.get_websocket_handler(exchange_obj)
            # Store handler for cleanup
            self.websocket_handlers[exchange_name] = handler

            logger.debug("WebSocket handler obtained", exchange=exchange_name)

            # Check if this exchange needs trade collection instead of OHLCV
            if handler.needs_trades_for_ohlcv():
                logger.info(
                    "Exchange requires trade collection instead of OHLCV - skipping OHLCV subscription",
                    exchange=exchange_name,
                    symbol_count=len(symbols)
                )
                return

            logger.info(
                "Exchange supports native OHLCV - proceeding with subscription",
                exchange=exchange_name,
                symbol_count=len(symbols)
            )

            # Subscribe each symbol with its own callback
            try:
                for symbol in symbols:
                    try:
                        symbol_str = symbol.symbol
                        logger.debug(
                            "Subscribing to OHLCV", exchange=exchange_name, symbol=symbol_str
                        )
                        # Create per-symbol callback (not shared - each needs to know its symbol)
                        symbol_callback = self._create_symbol_callback(exchange_name, symbol_str)
                        result = await handler.subscribe_ohlcv(symbol_str, "1m", symbol_callback)
                        logger.info(
                            "Subscription result",
                            exchange=exchange_name,
                            symbol=symbol_str,
                            success=result,
                        )

                        # Register symbol
                        self.registered_symbols.add(f"{exchange_name}:{symbol_str}")
                    except Exception as e:
                        logger.warning(
                            "Failed to subscribe to OHLCV",
                            exchange=exchange_name,
                            symbol=symbol.symbol if hasattr(symbol, "symbol") else str(symbol),
                            error=str(e),
                        )
            finally:
                logger.info(
                    "Finished subscribing to OHLCV",
                    exchange=exchange_name,
                    symbol_count=len(symbols),
                )

        except Exception as e:
            logger.error(
                "Error starting WebSocket for exchange", exchange=exchange_name, error=str(e)
            )
            raise

    def _create_symbol_callback(self, exchange_name: str, symbol_str: str):
        """Create per-symbol callback with symbol context."""

        async def ohlcv_callback(ohlcv_data) -> None:
            try:
                # ohlcv_data can be:
                # 1. List of lists: [[timestamp, o, h, l, c, v], ...]
                # 2. Single list: [timestamp, o, h, l, c, v]
                # 3. Object with attributes

                # Handle list format (most common for OHLCV WebSocket)
                if isinstance(ohlcv_data, list):
                    # If it's a list of lists, take the first one
                    if ohlcv_data and isinstance(ohlcv_data[0], list):
                        candle_data = ohlcv_data[0]
                    else:
                        candle_data = ohlcv_data

                    # Convert array format to Candle
                    if len(candle_data) >= 6:
                        timestamp_ms = candle_data[0]
                        timestamp_dt = datetime.fromtimestamp(
                            timestamp_ms / 1000 if timestamp_ms > 1e12 else timestamp_ms,
                            tz=UTC
                        )

                        candle = Candle(
                            timestamp=timestamp_dt,
                            open=float(candle_data[1]),
                            high=float(candle_data[2]),
                            low=float(candle_data[3]),
                            close=float(candle_data[4]),
                            vol=float(candle_data[5])
                        )
                    else:
                        logger.warning(
                            "Invalid OHLCV array format",
                            exchange=exchange_name,
                            symbol=symbol_str,
                            data=str(ohlcv_data)[:100]
                        )
                        return
                else:
                    # Handle object format
                    candle = self._convert_to_candle(ohlcv_data)

                # Only save if this is a NEW candle (different minute)
                symbol_key = f"{exchange_name}:{symbol_str}"
                if candle.timestamp != self.last_candle_timestamps.get(symbol_key):
                    # New minute - save the candle
                    async with CandleRepository(exchange_name, symbol_str, test=False) as repo:
                        await repo.save_candles([candle])
                    self.last_candle_timestamps[symbol_key] = candle.timestamp
                    logger.debug(
                        "Saved new candle",
                        exchange=exchange_name,
                        symbol=symbol_str,
                        timestamp=candle.timestamp
                    )
                # else: Same minute - skip save (in-progress update)

            except Exception as e:
                logger.error(
                    "Error processing OHLCV",
                    exchange=exchange_name,
                    symbol=symbol_str,
                    error=str(e)
                )

        return ohlcv_callback

    def _convert_to_candle(self, ohlcv_data) -> Candle:
        """Convert raw OHLCV data to Candle object."""
        # Handle both dict and object formats
        if isinstance(ohlcv_data, dict):
            timestamp = ohlcv_data.get("timestamp", ohlcv_data.get("datetime", 0))
            open_price = ohlcv_data.get("open", 0.0)
            high = ohlcv_data.get("high", 0.0)
            low = ohlcv_data.get("low", 0.0)
            close = ohlcv_data.get("close", 0.0)
            volume = ohlcv_data.get("volume", 0.0)
        else:
            timestamp = getattr(ohlcv_data, "timestamp", getattr(ohlcv_data, "datetime", 0))
            open_price = getattr(ohlcv_data, "open", 0.0)
            high = getattr(ohlcv_data, "high", 0.0)
            low = getattr(ohlcv_data, "low", 0.0)
            close = getattr(ohlcv_data, "close", 0.0)
            volume = getattr(ohlcv_data, "volume", 0.0)

        # Convert timestamp to datetime if needed
        from datetime import datetime

        if isinstance(timestamp, int | float):
            timestamp_dt = datetime.fromtimestamp(
                timestamp / 1000 if timestamp > 1e12 else timestamp, tz=UTC
            )
        else:
            timestamp_dt = timestamp

        return Candle(
            timestamp=timestamp_dt,
            open=float(open_price),
            high=float(high),
            low=float(low),
            close=float(close),
            vol=float(volume),
        )

    async def _cleanup(self) -> None:
        """Clean up WebSocket connections."""
        self.websocket_handlers.clear()

        try:
            await ExchangeQueue.shutdown_factory()
        except Exception as e:
            logger.error("Error shutting down ExchangeQueue", error=str(e))
