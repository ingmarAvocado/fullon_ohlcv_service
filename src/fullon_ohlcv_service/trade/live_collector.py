"""
Live Trade Collector

Handles real-time WebSocket trade collection using bulk initialization.
Implements clean fullon ecosystem integration patterns.
"""

import asyncio
import os
from typing import Any, Dict, List

from fullon_exchange.queue import ExchangeQueue
from fullon_log import get_component_logger
from fullon_cache import TradesCache
from fullon_orm.models import Symbol, Exchange
from fullon_orm import DatabaseContext
from .batcher import GlobalTradeBatcher


logger = get_component_logger("fullon.trade.live")


class LiveTradeCollector:
    """
    Bulk real-time trade collector using WebSocket connections.

    Loads all symbols from database and starts WebSocket collection
    for each exchange with shared handlers.
    """

    def __init__(self):
        self.running = False
        self.websocket_handlers = {}
        self.registered_symbols = set()

    async def start_collection(self) -> None:
        """Start live trade collection for all configured symbols."""
        if self.running:
            logger.warning("Live collection already running")
            return

        self.running = True
        logger.info("Starting live trade collection")

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
        """Stop live trade collection gracefully."""
        logger.info("Stopping live trade collection")
        self.running = False

        # Unregister symbols from batcher
        batcher = GlobalTradeBatcher()
        for symbol_key in self.registered_symbols:
            exchange_name, symbol_str = symbol_key.split(":", 1)
            await batcher.unregister_symbol(exchange_name, symbol_str)
        self.registered_symbols.clear()

        await self._cleanup()

    async def _load_data(self) -> tuple[Dict[str, List[Symbol]], List[Exchange]]:
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
                    success = await add_all_symbols(
                        symbols=all_symbols,
                        main="view",
                        test=False
                    )
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
        self, exchange_obj: Exchange, symbols: List[Symbol]
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
                        self.registered_symbols.add(f"{exchange_name}:{symbol_str}")
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

            except Exception as e:
                logger.error("Error processing trade", exchange=exchange_name, error=str(e))

        return trade_callback

    async def _cleanup(self) -> None:
        """Clean up WebSocket connections."""
        self.websocket_handlers.clear()

        try:
            await ExchangeQueue.shutdown_factory()
        except Exception as e:
            logger.error("Error shutting down ExchangeQueue", error=str(e))
