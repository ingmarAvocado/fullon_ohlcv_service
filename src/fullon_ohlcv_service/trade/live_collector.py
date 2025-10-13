"""
Live Trade Collector

Handles real-time WebSocket trade collection using bulk initialization.
Implements clean fullon ecosystem integration patterns.
"""

import asyncio

from fullon_cache import ProcessCache, TradesCache
from fullon_cache.process_cache import ProcessStatus, ProcessType
from fullon_exchange.queue import ExchangeQueue
from fullon_log import get_component_logger
from fullon_orm import DatabaseContext
from fullon_orm.models import Exchange, Symbol

from ..utils.admin_helper import get_admin_exchanges
from .batcher import GlobalTradeBatcher

logger = get_component_logger("fullon.trade.live")


class LiveTradeCollector:
    """
    Bulk real-time trade collector using WebSocket connections.

    Loads all symbols from database and starts WebSocket collection
    for each exchange with shared handlers.
    """

    def __init__(self, symbols: list | None = None):
        self.symbols = symbols or []
        self.running = False
        self.websocket_handlers = {}
        self.registered_symbols = set()
        self.process_ids = {}  # Track process IDs per symbol

    async def start_collection(self) -> None:
        """Start live trade collection for all configured symbols."""
        if self.running:
            logger.warning("Live collection already running")
            return

        self.running = True
        logger.info("Starting live trade collection")

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
        """Stop live trade collection gracefully."""
        logger.info("Stopping live trade collection")
        self.running = False

        # Unregister symbols from batcher
        batcher = GlobalTradeBatcher()
        for symbol_key in self.registered_symbols:
            exchange_name, symbol_str = symbol_key.split(":", 1)
            await batcher.unregister_symbol(exchange_name, symbol_str)
        self.registered_symbols.clear()

    async def _load_data(self) -> tuple[dict[str, list[Symbol]], list[Exchange]]:
        """Load admin exchanges and group symbols by exchange."""
        # Use shared admin helper
        _, admin_exchanges = await get_admin_exchanges()

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

            # Create shared callback for this exchange
            shared_callback = self._create_exchange_callback(exchange_name)
            try:
                for symbol in symbols:
                    try:
                        symbol_str = symbol.symbol
                        symbol_key = f"{exchange_name}:{symbol_str}"

                        # Register process for this symbol
                        async with ProcessCache() as cache:
                            process_id = await cache.register_process(
                                process_type=ProcessType.OHLCV,  # Note: using OHLCV type for trade collection too
                                component=symbol_key,
                                params={
                                    "exchange": exchange_name,
                                    "symbol": symbol_str,
                                    "type": "live_trade",
                                },
                                message="Starting live trade collection",
                                status=ProcessStatus.STARTING,
                            )
                        self.process_ids[symbol_key] = process_id

                        logger.debug(
                            "Subscribing to trades", exchange=exchange_name, symbol=symbol_str
                        )
                        result = await handler.subscribe_trades(symbol_str, shared_callback)
                        logger.info(
                            "Subscription result",
                            exchange=exchange_name,
                            symbol=symbol_str,
                            success=result,
                        )

                        # Register symbol with batcher for periodic processing
                        batcher = GlobalTradeBatcher()
                        await batcher.register_symbol(exchange_name, symbol_str)
                        self.registered_symbols.add(symbol_key)
                    except Exception as e:
                        logger.warning(
                            "Failed to subscribe to trades",
                            exchange=exchange_name,
                            symbol=symbol.symbol if hasattr(symbol, "symbol") else str(symbol),
                            error=str(e),
                        )
            finally:
                logger.info(
                    "Finished subscribing to trades",
                    exchange=exchange_name,
                    symbol_count=len(symbols),
                )

        except Exception as e:
            logger.error(
                "Error starting WebSocket for exchange", exchange=exchange_name, error=str(e)
            )
            raise

    def _create_exchange_callback(self, exchange_name: str):
        """Create shared callback for an exchange."""

        async def trade_callback(trade_obj) -> None:
            try:
                # trade_obj is a fullon_orm.models.Trade object
                if not hasattr(trade_obj, "symbol"):
                    logger.warning(
                        "Trade object missing symbol attribute",
                        exchange=exchange_name,
                        trade_obj=str(trade_obj)[:100],
                    )
                    return

                # Push Trade object directly to Redis cache (it has to_dict() method)
                async with TradesCache() as cache:
                    await cache.push_trade_list(
                        symbol=trade_obj.symbol, exchange=exchange_name, trade=trade_obj
                    )

                    # Update trade status for exchange
                    await cache.update_trade_status(key=exchange_name)

                # Update process status
                symbol_key = f"{exchange_name}:{trade_obj.symbol}"
                if symbol_key in self.process_ids:
                    async with ProcessCache() as cache:
                        await cache.update_process(
                            process_id=self.process_ids[symbol_key],
                            status=ProcessStatus.RUNNING,
                            message=f"Received trade at {trade_obj.time}",
                        )

            except Exception as e:
                logger.error("Error processing trade", exchange=exchange_name, error=str(e))

                # Update process status on error
                symbol_key = f"{exchange_name}:{trade_obj.symbol}"
                if symbol_key in self.process_ids:
                    async with ProcessCache() as cache:
                        await cache.update_process(
                            process_id=self.process_ids[symbol_key],
                            status=ProcessStatus.ERROR,
                            message=f"Error: {str(e)}",
                        )

        return trade_callback

    def is_collecting(self, symbol: Symbol) -> bool:
        """Check if symbol is currently being collected.

        Args:
            symbol: Symbol to check

        Returns:
            True if symbol is being collected, False otherwise
        """
        symbol_key = f"{symbol.cat_exchange.name}:{symbol.symbol}"
        return symbol_key in self.registered_symbols

    async def start_symbol(self, symbol: Symbol) -> None:
        """Start live collection for a specific symbol.

        Simple method that gets admin exchange and calls _start_exchange_collector.

        Args:
            symbol: Symbol to start collecting

        Raises:
            ValueError: If admin exchange not found
        """
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

        # Let _start_exchange_collector handle everything
        await self._start_exchange_collector(admin_exchange, [symbol])
