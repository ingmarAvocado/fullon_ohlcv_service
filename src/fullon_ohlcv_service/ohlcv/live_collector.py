"""
Live OHLCV Collector

Handles real-time OHLCV data collection using bulk initialization.
Implements clean fullon ecosystem integration patterns.
"""

import asyncio
import arrow
from fullon_cache import ProcessCache
from fullon_cache.process_cache import ProcessStatus, ProcessType
from fullon_exchange.queue import ExchangeQueue
from fullon_log import get_component_logger
from fullon_ohlcv.models import Candle
from fullon_ohlcv.repositories.ohlcv import CandleRepository
from fullon_orm import DatabaseContext
from fullon_orm.models import Exchange, Symbol

from ..utils.admin_helper import get_admin_exchanges
from .utils import timeframe_to_seconds, convert_to_candle_objects

logger = get_component_logger("fullon.ohlcv.live")


class LiveOHLCVCollector:
    """
    Bulk real-time OHLCV collector using WebSocket connections.

    Loads all symbols from database and starts WebSocket collection
    for each exchange with shared handlers.
    """

    def __init__(self, symbols: list | None = None):
        self.symbols = symbols or []
        self.running = False
        self.websocket_handlers = {}
        self.registered_symbols = set()
        self.last_candle_timestamps = {}  # Track last saved candle timestamp per symbol
        self.process_ids = {}  # Track process IDs per symbol
        self.rest_polling_tasks = {}  # Track REST polling tasks per symbol

    async def start_collection(self) -> None:
        """Start live OHLCV collection for all configured symbols."""
        if self.running:
            logger.warning("Live collection already running")
            return

        self.running = True
        logger.info("Starting live OHLCV collection")

        try:
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

        except Exception as e:
            logger.error("Error in live collection startup", error=str(e))
            raise

    async def stop_collection(self) -> None:
        """Stop live OHLCV collection gracefully."""
        logger.info("Stopping live OHLCV collection")
        self.running = False

    async def _load_data(self) -> tuple[dict[str, list[Symbol]], list[Exchange]]:
        """Load admin exchanges and group symbols by exchange."""
        # Use shared admin helper
        admin_uid, admin_exchanges = await get_admin_exchanges()

        # Load all symbols
        async with DatabaseContext() as db:
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
                        symbol_key = f"{exchange_name}:{symbol_str}"

                        # Register process for this symbol
                        async with ProcessCache() as cache:
                            process_id = await cache.register_process(
                                process_type=ProcessType.OHLCV,
                                component=symbol_key,
                                params={
                                    "exchange": exchange_name,
                                    "symbol": symbol_str,
                                    "type": "live",
                                },
                                message="Starting live OHLCV collection",
                                status=ProcessStatus.STARTING,
                            )
                        self.process_ids[symbol_key] = process_id

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
                        self.registered_symbols.add(symbol_key)
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

    def _supports_websocket(self, handler) -> bool:
        """Check if exchange handler supports WebSocket OHLCV.

        Args:
            handler: WebSocket handler from ExchangeQueue

        Returns:
            True if WebSocket is supported, False otherwise
        """
        try:
            # Check if subscribe_ohlcv method exists
            if not hasattr(handler, 'subscribe_ohlcv'):
                logger.debug("Handler missing subscribe_ohlcv method")
                return False

            # Check if handler has WebSocket capabilities
            if hasattr(handler, 'has'):
                has_ws = handler.has.get('ws', False)
                has_watch_ohlcv = handler.has.get('watchOHLCV', False)
                if not (has_ws and has_watch_ohlcv):
                    logger.debug(
                        "Handler lacks WebSocket capabilities",
                        has_ws=has_ws,
                        has_watch_ohlcv=has_watch_ohlcv
                    )
                    return False

            # Check if handler is connected (Yahoo stub fails at connection)
            if hasattr(handler, 'is_connected'):
                if not handler.is_connected():
                    logger.debug("Handler WebSocket not connected")
                    return False

            return True
        except Exception as e:
            logger.warning(f"Error checking WebSocket support: {e}")
            return False

    async def _start_rest_polling(
        self, exchange_obj: Exchange, symbol: Symbol, handler
    ) -> None:
        """Start REST polling for a symbol that doesn't support WebSocket.

        Args:
            exchange_obj: Exchange object
            symbol: Symbol to poll
            handler: REST handler from ExchangeQueue
        """
        exchange_name = exchange_obj.cat_exchange.name
        symbol_str = symbol.symbol
        symbol_key = f"{exchange_name}:{symbol_str}"

        # Get timeframe (default to 1m)
        timeframe = getattr(symbol, 'updateframe', '1m')
        poll_interval = timeframe_to_seconds(timeframe)

        logger.info(
            "Starting REST polling fallback",
            exchange=exchange_name,
            symbol=symbol_str,
            timeframe=timeframe,
            poll_interval=poll_interval
        )

        async def poll_loop():
            """Continuous polling loop for REST OHLCV data."""
            while self.running:
                try:
                    # Fetch last 2 candles (use second-to-last as it's guaranteed complete)
                    candles = await handler.get_ohlcv(
                        symbol_str,
                        timeframe=timeframe,
                        limit=2
                    )

                    if candles and len(candles) >= 2:
                        # Use second-to-last candle (guaranteed complete)
                        candle_data = candles[-2]

                        # Convert to Candle object
                        candle_list = convert_to_candle_objects([candle_data])

                        if candle_list:
                            candle = candle_list[0]

                            # Only save if this is a NEW candle (different timestamp)
                            if candle.timestamp != self.last_candle_timestamps.get(symbol_key):
                                async with CandleRepository(exchange_name, symbol_str, test=False) as repo:
                                    await repo.save_candles([candle])

                                self.last_candle_timestamps[symbol_key] = candle.timestamp

                                logger.debug(
                                    "Saved new candle via REST polling",
                                    exchange=exchange_name,
                                    symbol=symbol_str,
                                    timestamp=candle.timestamp
                                )

                                # Update process status
                                if symbol_key in self.process_ids:
                                    async with ProcessCache() as cache:
                                        await cache.update_process(
                                            process_id=self.process_ids[symbol_key],
                                            status=ProcessStatus.RUNNING,
                                            message=f"REST polling - received OHLCV at {candle.timestamp}"
                                        )

                    # Wait for next poll interval
                    await asyncio.sleep(poll_interval)

                except Exception as e:
                    logger.error(
                        "Error in REST polling loop",
                        exchange=exchange_name,
                        symbol=symbol_str,
                        error=str(e)
                    )

                    # Update process status on error
                    if symbol_key in self.process_ids:
                        async with ProcessCache() as cache:
                            await cache.update_process(
                                process_id=self.process_ids[symbol_key],
                                status=ProcessStatus.ERROR,
                                message=f"REST polling error: {str(e)}"
                            )

                    # Wait before retrying
                    await asyncio.sleep(poll_interval)

        # Start the polling task
        task = asyncio.create_task(poll_loop())
        self.rest_polling_tasks[symbol_key] = task

    def _create_symbol_callback(self, exchange_name: str, symbol_str: str):
        """Create per-symbol callback with symbol context."""

        async def ohlcv_callback(ohlcv_data) -> None:
            try:
                candle_data = ohlcv_data[0] if isinstance(ohlcv_data[0], list) else ohlcv_data
                ts, o, h, low, c, v = candle_data
                ts_sec = ts / 1000 if ts > 1e12 else ts
                timestamp = arrow.get(ts_sec)
                candle = Candle(
                    timestamp=timestamp.datetime,
                    open=o,
                    high=h,
                    low=low,
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

                    # Update process status
                    if symbol_key in self.process_ids:
                        async with ProcessCache() as cache:
                            await cache.update_process(
                                process_id=self.process_ids[symbol_key],
                                status=ProcessStatus.RUNNING,
                                message=f"Received OHLCV at {candle.timestamp}",
                            )
                # else: Same minute - skip save (in-progress update)

            except Exception as e:
                logger.error(
                    "Error processing OHLCV",
                    exchange=exchange_name,
                    symbol=symbol_str,
                    error=str(e),
                )

                # Update process status on error
                symbol_key = f"{exchange_name}:{symbol_str}"
                if symbol_key in self.process_ids:
                    async with ProcessCache() as cache:
                        await cache.update_process(
                            process_id=self.process_ids[symbol_key],
                            status=ProcessStatus.ERROR,
                            message=f"Error: {str(e)}",
                        )

        return ohlcv_callback

    def is_collecting(self, symbol: Symbol) -> bool:
        """Check if symbol is currently being collected.

        Args:
            symbol: Symbol to check

        Returns:
            True if symbol is being collected, False otherwise
        """
        symbol_key = f"{symbol.cat_exchange.name}:{symbol.symbol}"
        return symbol_key in self.registered_symbols

    async def add_symbol(self, symbol: Symbol) -> None:
        """Add symbol dynamically to running collector.

        Reuses existing WebSocket handler if available for the exchange,
        otherwise creates new handler. Checks if exchange needs trades
        for OHLCV before subscribing.

        Args:
            symbol: Symbol to add

        Raises:
            RuntimeError: If collector is not running
            ValueError: If admin exchange not found
        """
        if not self.running:
            raise RuntimeError("Collector not running - call start_collection() first")

        exchange_name = symbol.cat_exchange.name
        symbol_key = f"{exchange_name}:{symbol.symbol}"

        # Check if already collecting
        if symbol_key in self.registered_symbols:
            logger.info("Symbol already collecting", symbol_key=symbol_key)
            return

        # Get admin exchanges
        _, admin_exchanges = await get_admin_exchanges()

        # Find admin exchange for this symbol
        admin_exchange = None
        for exchange in admin_exchanges:
            if exchange.cat_exchange.name == exchange_name:
                admin_exchange = exchange
                break

        if not admin_exchange:
            raise ValueError(f"Admin exchange {exchange_name} not found")

        # Check if handler exists for this exchange
        if exchange_name in self.websocket_handlers:
            # Reuse existing handler
            logger.info("Reusing existing handler", exchange=exchange_name, symbol=symbol.symbol)
            handler = self.websocket_handlers[exchange_name]

            # Check if exchange needs trades for OHLCV
            if hasattr(handler, 'needs_trades_for_ohlcv'):
                try:
                    if handler.needs_trades_for_ohlcv():
                        logger.info(
                            "Exchange needs trades for OHLCV, skipping",
                            exchange=exchange_name,
                            symbol=symbol.symbol
                        )
                        return
                except Exception as e:
                    logger.warning(
                        "Could not check exchange capabilities",
                        exchange=exchange_name,
                        error=str(e)
                    )

            # Register process for this symbol
            async with ProcessCache() as cache:
                process_id = await cache.register_process(
                    process_type=ProcessType.OHLCV,
                    component=symbol_key,
                    params={
                        "exchange": exchange_name,
                        "symbol": symbol.symbol,
                        "type": "live_ohlcv",
                    },
                    message="Starting live OHLCV collection",
                    status=ProcessStatus.STARTING,
                )
            self.process_ids[symbol_key] = process_id

            # Subscribe to OHLCV for this symbol
            callback = self._create_symbol_callback(exchange_name, symbol.symbol)
            await handler.subscribe_ohlcv(symbol.symbol, "1m", callback)

            # Update state
            self.registered_symbols.add(symbol_key)
            self.symbols.append(symbol)

            logger.info("Added symbol to collector", symbol=symbol.symbol, exchange=exchange_name)
        else:
            # No handler yet, create new one for this exchange
            logger.info("Creating new handler", exchange=exchange_name, symbol=symbol.symbol)
            await self._start_exchange_collector(admin_exchange, [symbol])

    async def start_symbol(self, symbol: Symbol) -> None:
        """Start live collection for a specific symbol.

        Detects if WebSocket is available. If not, falls back to REST polling.

        Args:
            symbol: Symbol to start collecting

        Raises:
            ValueError: If admin exchange not found
        """
        exchange_name = symbol.cat_exchange.name
        symbol_str = symbol.symbol
        symbol_key = f"{exchange_name}:{symbol_str}"

        # Get admin exchanges
        _, admin_exchanges = await get_admin_exchanges()

        # Find admin exchange for this symbol
        admin_exchange = None
        for exchange in admin_exchanges:
            if exchange.cat_exchange.name == exchange_name:
                admin_exchange = exchange
                break

        if not admin_exchange:
            raise ValueError(f"Admin exchange {exchange_name} not found")

        # Register process for this symbol
        async with ProcessCache() as cache:
            process_id = await cache.register_process(
                process_type=ProcessType.OHLCV,
                component=symbol_key,
                params={
                    "exchange": exchange_name,
                    "symbol": symbol_str,
                    "type": "live",
                },
                message="Starting live OHLCV collection",
                status=ProcessStatus.STARTING,
            )
        self.process_ids[symbol_key] = process_id

        try:
            # Try to get WebSocket handler
            ws_handler = await ExchangeQueue.get_websocket_handler(admin_exchange)

            # Check if exchange needs trades for OHLCV
            if hasattr(ws_handler, 'needs_trades_for_ohlcv'):
                if ws_handler.needs_trades_for_ohlcv():
                    logger.info(
                        "Exchange needs trades for OHLCV, skipping",
                        exchange=exchange_name,
                        symbol=symbol_str
                    )
                    return

            # Check if WebSocket is supported
            if self._supports_websocket(ws_handler):
                logger.info(
                    "WebSocket available - using WebSocket for live OHLCV",
                    exchange=exchange_name,
                    symbol=symbol_str
                )
                # Use WebSocket
                await self._start_exchange_collector(admin_exchange, [symbol])
            else:
                # Fallback to REST polling
                logger.info(
                    "WebSocket unavailable - falling back to REST polling",
                    exchange=exchange_name,
                    symbol=symbol_str
                )

                # Get REST handler
                rest_handler = await ExchangeQueue.get_rest_handler(admin_exchange)

                # Start REST polling
                await self._start_rest_polling(admin_exchange, symbol, rest_handler)

                # Register symbol as collecting
                self.registered_symbols.add(symbol_key)

        except Exception as e:
            logger.error(
                "Error starting symbol collection",
                exchange=exchange_name,
                symbol=symbol_str,
                error=str(e)
            )

            # Update process status on error
            if symbol_key in self.process_ids:
                async with ProcessCache() as cache:
                    await cache.update_process(
                        process_id=self.process_ids[symbol_key],
                        status=ProcessStatus.ERROR,
                        message=f"Error: {str(e)}"
                    )
            raise
