"""
Live Trade Collector

Handles real-time WebSocket trade collection and Redis queuing.
Automatically starts after historical collection completes.
"""

import asyncio
import os
from typing import Any, Dict, Optional
from datetime import datetime, timezone

from fullon_exchange.queue import ExchangeQueue
from fullon_log import get_component_logger
from fullon_cache import TradesCache
from fullon_orm.models import Symbol, Exchange
from fullon_orm import DatabaseContext
from fullon_credentials import fullon_credentials


class LiveTradeCollector:
    """
    Real-time trade collector using WebSocket connections.

    Streams live trades via WebSocket and pushes them to Redis
    for batch processing by GlobalTradeBatcher.
    """

    def __init__(self, symbol_obj: Symbol) -> None:
        """
        Initialize live trade collector for a specific symbol.

        Args:
            symbol_obj: Symbol object from fullon_orm database
        """
        self.symbol_obj = symbol_obj
        self.exchange = symbol_obj.exchange_name
        self.symbol = symbol_obj.symbol
        self.exchange_id: Optional[int] = None

        self.logger = get_component_logger(
            f"fullon.trade.live.{self.exchange}.{self.symbol}"
        )

        self.running = False
        self._websocket_handler = None
        self._reconnect_delay = 1.0  # Start with 1 second
        self._max_reconnect_delay = 30.0

        self.logger.debug(
            "Live trade collector created",
            symbol=self.symbol,
            exchange=self.exchange
        )

    async def start_streaming(self) -> None:
        """Start WebSocket streaming with auto-reconnection."""
        if self.running:
            self.logger.warning("Streaming already running", symbol=self.symbol)
            return

        self.running = True
        self.logger.info("Starting live trade streaming", symbol=self.symbol)

        # Register with GlobalTradeBatcher for batch processing
        await self._register_with_batcher()

        await ExchangeQueue.initialize_factory()

        while self.running:
            try:
                await self._setup_websocket()
                await self._maintain_connection()
            except Exception as e:
                self.logger.error(
                    "WebSocket error, will reconnect",
                    symbol=self.symbol,
                    error=str(e),
                    reconnect_delay=self._reconnect_delay
                )

                if self.running:
                    await asyncio.sleep(self._reconnect_delay)
                    # Exponential backoff
                    self._reconnect_delay = min(
                        self._reconnect_delay * 2,
                        self._max_reconnect_delay
                    )

    async def stop_streaming(self) -> None:
        """Stop WebSocket streaming gracefully."""
        self.logger.info("Stopping live trade streaming", symbol=self.symbol)
        self.running = False

        if self._websocket_handler:
            try:
                # Try disconnect method first (proper WebSocket cleanup)
                if hasattr(self._websocket_handler, 'disconnect'):
                    self.logger.debug("Calling disconnect on WebSocket handler")
                    await self._websocket_handler.disconnect()

                # Also try close method if available
                if hasattr(self._websocket_handler, 'close'):
                    self.logger.debug("Calling close on WebSocket handler")
                    await self._websocket_handler.close()

                # Try CCXT cleanup method
                if hasattr(self._websocket_handler, '_cleanup_ccxt_instance'):
                    self.logger.debug("Calling _cleanup_ccxt_instance")
                    await self._websocket_handler._cleanup_ccxt_instance()

            except Exception as e:
                self.logger.error("Error closing WebSocket", error=str(e))

    async def _setup_websocket(self) -> None:
        """Setup WebSocket connection using fullon_exchange."""
        exchange_obj = await self.create_exchange_object()
        credential_provider = self.create_credential_provider()

        self._websocket_handler = await ExchangeQueue.get_websocket_handler(
            exchange_obj, credential_provider
        )

        # Connect and authenticate following the websocket_example.py pattern
        await self._websocket_handler.connect()
        await self._websocket_handler.authenticate()

        # Wait for connection to be established before subscribing
        max_wait = 10  # Maximum wait time in seconds
        wait_time = 0
        while wait_time < max_wait:
            if hasattr(self._websocket_handler, 'is_connected') and self._websocket_handler.is_connected():
                break
            await asyncio.sleep(0.5)
            wait_time += 0.5

        if not (hasattr(self._websocket_handler, 'is_connected') and self._websocket_handler.is_connected()):
            self.logger.warning(
                "WebSocket not connected after waiting, attempting subscription anyway",
                symbol=self.symbol,
                wait_time=wait_time
            )

        await self._websocket_handler.subscribe_trades(
            self.symbol,
            self._on_trade_received
        )

        # Reset reconnect delay on successful connection
        self._reconnect_delay = 1.0

        self.logger.info(
            "WebSocket connection established",
            symbol=self.symbol,
            exchange=self.exchange
        )

    async def _maintain_connection(self) -> None:
        """Maintain WebSocket connection until stopped."""
        while self.running:
            # Check connection health periodically
            await asyncio.sleep(30)

            if not self._websocket_handler or not self._websocket_handler.connected:
                self.logger.warning("WebSocket disconnected, reconnecting")
                break

    async def _on_trade_received(self, trade_data: Dict[str, Any]) -> None:
        """
        Handle incoming trade from WebSocket.

        Args:
            trade_data: Raw trade data from exchange
        """
        try:
            # Push to Redis for batch processing
            async with TradesCache() as cache:
                await cache.push_trade(self.exchange, self.symbol, trade_data)

            # Log periodically to avoid spam
            if hasattr(self, '_trade_count'):
                self._trade_count += 1
            else:
                self._trade_count = 1

            if self._trade_count % 100 == 0:
                self.logger.debug(
                    "Trades received",
                    symbol=self.symbol,
                    count=self._trade_count
                )

        except Exception as e:
            self.logger.error(
                "Error processing trade",
                symbol=self.symbol,
                exchange=self.exchange,
                error=str(e),
                error_type=type(e).__name__,
                trade_data=str(trade_data)[:200]
            )
            # Also log the full traceback for debugging
            import traceback
            self.logger.debug("Full trade processing error traceback", traceback=traceback.format_exc())

    async def _register_with_batcher(self) -> None:
        """Register this symbol with GlobalTradeBatcher for processing."""
        try:
            from .global_batcher import GlobalTradeBatcher
            batcher = GlobalTradeBatcher()
            await batcher.register_symbol(self.exchange, self.symbol)
            self.logger.info(
                "Registered with GlobalTradeBatcher",
                symbol=self.symbol,
                exchange=self.exchange
            )
        except Exception as e:
            self.logger.error(
                "Failed to register with batcher",
                symbol=self.symbol,
                error=str(e)
            )

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