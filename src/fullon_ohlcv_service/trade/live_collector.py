"""
Live Trade Collector

Handles real-time WebSocket trade collection and Redis queuing.
Automatically starts after historical collection completes.
"""

import asyncio
from typing import Any, Dict, Optional
from datetime import datetime, timezone

from fullon_exchange.queue import ExchangeQueue
from fullon_log import get_component_logger
from fullon_cache import TradesCache
from fullon_orm.models import Symbol


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
                await self._websocket_handler.close()
            except Exception as e:
                self.logger.error("Error closing WebSocket", error=str(e))

    async def _setup_websocket(self) -> None:
        """Setup WebSocket connection using fullon_exchange."""
        exchange_obj = self._create_exchange_object()
        credential_provider = self._create_credential_provider()

        self._websocket_handler = await ExchangeQueue.get_websocket_handler(
            exchange_obj, credential_provider
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
                error=str(e),
                trade=str(trade_data)[:100]
            )

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

    def _create_exchange_object(self) -> Any:
        """Create exchange object for WebSocket connection."""
        class SimpleExchange:
            def __init__(self, exchange_name: str, symbol: str):
                self.ex_id = f"{exchange_name}_websocket"
                self.uid = "websocket"
                self.test = False
                self.cat_exchange = type('CatExchange', (), {'name': exchange_name})()

        return SimpleExchange(self.exchange, self.symbol)

    def _create_credential_provider(self):
        """Create credential provider for public WebSocket data."""
        def credential_provider(exchange_obj: Any) -> tuple[str, str]:
            return "", ""  # Empty credentials for public data

        return credential_provider