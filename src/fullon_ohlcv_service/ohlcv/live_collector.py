"""
Live OHLCV Collector

Handles real-time OHLCV data collection using bulk initialization.
Implements clean fullon ecosystem integration patterns.
"""

import asyncio
import os
from typing import Optional

import arrow
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
                    try:
                        if not handler.connected:
                            logger.warning("WebSocket disconnected", exchange=exchange_name)
                    except AttributeError:
                        # Handler may not have 'connected' attribute
                        pass

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

            # Initialize OHLCV symbols for this collector
            if all_symbols:
                try:
                    from fullon_ohlcv_service.utils.add_symbols import add_all_symbols

                    success = await add_all_symbols(symbols=all_symbols, main="candles", test=False)
                    if success:
                        logger.info(f"Initialized {len(all_symbols)} OHLCV symbols")
                    else:
                        logger.warning("Some OHLCV symbols failed to initialize")
                except Exception as e:
                    logger.error(f"OHLCV symbol initialization failed: {e}")

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
            try:
                if handler.needs_trades_for_ohlcv():
                    logger.info(
                        "Exchange requires trade collection instead of OHLCV - skipping OHLCV subscription",
                        exchange=exchange_name,
                        symbol_count=len(symbols),
                    )
                    return
            except AttributeError:
                # WebSocket handler doesn't have this method, assume supports OHLCV
                pass

            logger.info(
                "Exchange supports native OHLCV - proceeding with subscription",
                exchange=exchange_name,
                symbol_count=len(symbols),
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
                candle_data = ohlcv_data[0] if isinstance(ohlcv_data[0], list) else ohlcv_data
                ts, o, h, l, c, v = candle_data
                ts_sec = ts / 1000 if ts > 1e12 else ts
                timestamp = arrow.get(ts_sec)
                candle = Candle(
                    timestamp=timestamp.datetime,
                    open=o,
                    high=h,
                    low=l,
                    close=c,
                    vol=v,
                )

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
                        timestamp=candle.timestamp,
                    )
                # else: Same minute - skip save (in-progress update)

            except Exception as e:
                logger.error(
                    "Error processing OHLCV",
                    exchange=exchange_name,
                    symbol=symbol_str,
                    error=str(e),
                )

        return ohlcv_callback

    async def _cleanup(self) -> None:
        """Clean up WebSocket connections."""
        self.websocket_handlers.clear()

        try:
            await ExchangeQueue.shutdown_factory()
        except Exception as e:
            logger.error("Error shutting down ExchangeQueue", error=str(e))
