"""
TradeDaemon: Simple orchestrator for trade data collection.

Provides basic start/stop/health functionality for trade collection examples.
Based on fullon_ticker_service TickerDaemon pattern with direct ExchangeQueue calls.
"""

import asyncio
import os
from enum import Enum
from typing import Any, Dict

from fullon_cache import ProcessCache
from fullon_cache.process_cache import ProcessType
from fullon_exchange.queue import ExchangeQueue
from fullon_credentials import fullon_credentials
from fullon_log import get_component_logger
from fullon_orm import DatabaseContext
from fullon_orm.models import Trade

from .manager import TradeManager

logger = get_component_logger("fullon.trade.daemon")


class DaemonStatus(Enum):
    """Basic daemon status."""
    STOPPED = "stopped"
    RUNNING = "running"
    ERROR = "error"


class TradeDaemon:
    """
    Simple trade service daemon.

    Provides basic functionality needed by examples:
    - start() - start trade collection
    - stop() - stop trade collection
    - process_trade(symbol) - single symbol processing
    - get_health() - basic health info
    """

    def __init__(self) -> None:
        """Initialize the trade daemon."""
        self._status = DaemonStatus.STOPPED
        self._websocket_handlers: Dict[str, Any] = {}
        self._running = False
        self._trade_manager: TradeManager | None = None
        self._process_id: str | None = None

    async def start(self) -> None:
        """Start the trade daemon - get exchanges from database and start collection."""
        if self._running:
            return

        logger.info("Starting trade daemon")
        self._status = DaemonStatus.RUNNING

        try:
            # Initialize trade manager
            self._trade_manager = TradeManager()

            # Initialize ExchangeQueue factory
            await ExchangeQueue.initialize_factory()

            # Get exchanges and symbols from database
            async with DatabaseContext() as db:
                # Use active cat exchanges
                all_symbols = await db.symbols.get_all()
            if all_symbols:
                try:
                    logger.info(f"Bulk loaded {len(all_symbols)} total symbols from database")
                    _symbols = {}
                    for symbol in all_symbols:
                        if symbol.cat_exchange.name not in _symbols:
                            _symbols[symbol.cat_exchange.name] = []
                        _symbols[symbol.cat_exchange.name].append(symbol)
                    for exchange, symbols in _symbols:
                        launch_shit()
                        logger.info(f"Started trade handler for {exchange} with {len(symbols)} symbols")
                except Exception as e:
                    logger.error(f"Failed to start trade_daemon: {e}")

            # Register process
            await self._register_process()

            self._running = True
            logger.info(f"Trade daemon started with {len(self._websocket_handlers)} exchanges")

        except Exception as e:
            logger.error(f"Failed to start trade daemon: {e}")
            self._status = DaemonStatus.ERROR
            raise

    async def _start_exchange_websocket(self, exchange_name: str, symbols: list[str]) -> None:
        """Start websocket connection for an exchange with direct ExchangeQueue calls."""

        # Create exchange object for fullon_exchange
        class SimpleExchange:
            def __init__(self, exchange_name: str, account_id: str):
                self.ex_id = f"{exchange_name}_{account_id}"
                self.uid = account_id
                self.test = False
                self.cat_exchange = type('CatExchange', (), {'name': exchange_name})()

        exchange_obj = SimpleExchange(exchange_name, "trade_account")

        # Credential provider (try to get credentials, fallback to public)
        def credential_provider(exchange):
            try:
                # Try to get credentials for this exchange (may not exist for demo data)
                secret, key = fullon_credentials(ex_id=1)
                return (key, secret)
            except ValueError:
                # Fallback to public access for trade data (most exchanges support this)
                logger.info(f"No credentials found for {exchange_name}, using public access")
                return ("", "")

        # Get websocket handler directly from ExchangeQueue
        handler = await ExchangeQueue.get_websocket_handler(exchange_obj, credential_provider)
        await handler.connect()

        # Subscribe to symbols
        for symbol in symbols:
            # Fix closure bug: capture symbol and exchange_name values
            async def trade_callback(trade: Trade, captured_symbol=symbol, captured_exchange=exchange_name) -> None:
                if self._trade_manager:
                    await self._trade_manager.process_trade(captured_exchange, trade)

            # Subscribe to trades (fullon_exchange handles subscription management)
            await handler.subscribe_trades(symbol, callback=trade_callback)

        # Store handler for cleanup later
        self._websocket_handlers[exchange_name] = handler

    async def stop(self) -> None:
        """Stop the trade daemon."""
        if not self._running:
            return

        logger.info("Stopping trade daemon")

        # Stop all websocket handlers
        for exchange_name, handler in self._websocket_handlers.items():
            try:
                await handler.disconnect()
            except Exception as e:
                logger.error(f"Error disconnecting {exchange_name}: {e}")

        self._websocket_handlers.clear()

        # Shutdown ExchangeQueue factory
        try:
            await ExchangeQueue.shutdown_factory()
        except Exception as e:
            logger.error(f"Error shutting down ExchangeQueue: {e}")

        # Unregister process
        await self._unregister_process()

        self._running = False
        self._trade_manager = None
        self._status = DaemonStatus.STOPPED

        logger.info("Trade daemon stopped")

    def is_running(self) -> bool:
        """Check if daemon is running."""
        return self._running

    async def get_health(self) -> dict[str, Any]:
        """Get basic health status for status display."""
        health = {
            "status": self._status.value,
            "running": self._running,
            "exchanges": {},
            "process_id": self._process_id
        }

        # Get websocket handler status
        for exchange_name, handler in self._websocket_handlers.items():
            try:
                # Basic connection status (websocket handlers don't have get_status method)
                health["exchanges"][exchange_name] = {
                    "connected": handler is not None,
                    "handler_active": True
                }
            except Exception as e:
                health["exchanges"][exchange_name] = {
                    "connected": False,
                    "error": str(e)
                }

        # Get trade stats
        if self._trade_manager:
            health["trade_stats"] = self._trade_manager.get_trade_stats()

        return health

    async def process_trade(self, symbol) -> None:
        """
        Single symbol processing for examples.

        Used by single_trade_loop_example.py for simple cases.
        """
        if self._running:
            await self.stop()

        logger.info(f"Starting single trade processing for {symbol.symbol} on {symbol.exchange_name}")

        try:
            # Initialize trade manager
            self._trade_manager = TradeManager()

            # Initialize ExchangeQueue factory
            await ExchangeQueue.initialize_factory()

            # Start single exchange websocket for one symbol
            await self._start_exchange_websocket(symbol.exchange_name, [symbol.symbol])

            # Register process
            await self._register_process()

            self._running = True
            self._status = DaemonStatus.RUNNING

            logger.info(f"Single trade processing started for {symbol.symbol}")

        except Exception as e:
            logger.error(f"Failed to start single trade processing: {e}")
            self._status = DaemonStatus.ERROR
            raise

    async def _register_process(self) -> None:
        """Register process for health monitoring."""
        try:
            async with ProcessCache() as cache:
                self._process_id = await cache.register_process(
                    process_type=ProcessType.TRADE,  # Assuming TRADE type exists
                    component="trade_daemon",
                    params={"daemon_id": id(self)},
                    message="Started"
                )
        except Exception as e:
            logger.error(f"Failed to register process: {e}")

    async def _unregister_process(self) -> None:
        """Unregister process."""
        if not self._process_id:
            return

        try:
            async with ProcessCache() as cache:
                await cache.delete_from_top(component="trade_service:trade_daemon")
        except Exception as e:
            logger.error(f"Failed to unregister process: {e}")
        finally:
            self._process_id = None