"""
Live OHLCV Collector

Handles real-time OHLCV data collection via WebSocket streaming.
"""

import os
from typing import Optional, Callable, Any

from fullon_exchange.queue import ExchangeQueue
from fullon_log import get_component_logger
from fullon_ohlcv.models import Candle
from fullon_ohlcv.repositories.ohlcv import CandleRepository
from fullon_orm import DatabaseContext
from fullon_orm.models import Symbol, Exchange
from fullon_credentials import fullon_credentials

from fullon_cache import ProcessCache
from fullon_cache.process_cache import ProcessType, ProcessStatus


class LiveCollector:
    """
    Live OHLCV data collector using WebSocket streaming.

    Handles real-time data collection from exchanges and stores
    candles as they arrive via WebSocket connections.
    """

    def __init__(self, symbol_obj: Symbol) -> None:
        """
        Initialize live collector for a specific symbol.

        Args:
            symbol_obj: Symbol object from fullon_orm database
        """
        self.symbol_obj = symbol_obj
        self.exchange = symbol_obj.exchange_name
        self.symbol = symbol_obj.symbol
        self.exchange_id: Optional[int] = None

        # State management
        self.running = False
        self._process_id: Optional[str] = None

        # Setup logging
        self.logger = get_component_logger(
            f"fullon.ohlcv.live.{self.exchange}.{self.symbol}"
        )

        self.logger.debug(
            "Live collector created",
            symbol=self.symbol,
            exchange=self.exchange
        )

    async def start_streaming(self, callback: Optional[Callable] = None) -> bool:
        """
        Start real-time WebSocket streaming for OHLCV data.

        Args:
            callback: Optional callback function for received data

        Returns:
            bool: True if streaming started successfully, False otherwise
        """
        self.logger.info("Starting live OHLCV streaming", symbol=self.symbol)

        await self.update_collector_status("Real-time streaming started")

        await ExchangeQueue.initialize_factory()
        try:
            # Get admin's exchange object and credentials
            exchange_obj = await self.create_exchange_object()
            credential_provider = self.create_credential_provider()

            # Create WebSocket handler
            handler = await ExchangeQueue.get_websocket_handler(
                exchange_obj, credential_provider
            )
            await handler.connect()

            # Start appropriate streaming method
            if handler.supports_ohlcv():
                self.logger.info("Starting OHLCV WebSocket streaming")
                await self.start_ohlcv_streaming(handler, callback)
            else:
                self.logger.info("Starting trade WebSocket streaming (fallback)")
                await self.start_trade_streaming(handler, callback)

            self.running = True
            return True

        except Exception as e:
            self.logger.error(
                "Live streaming startup failed",
                symbol=self.symbol,
                error=str(e)
            )
            return False

    async def stop_streaming(self) -> None:
        """Stop the streaming collection and cleanup resources."""
        self.running = False
        await self.cleanup_process_status()
        self.logger.info("Live streaming stopped", symbol=self.symbol)

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

    async def start_ohlcv_streaming(
        self,
        handler: Any,
        callback: Optional[Callable] = None
    ) -> None:
        """
        Start OHLCV WebSocket streaming.

        Args:
            handler: WebSocket handler from fullon_exchange
            callback: Optional callback function for received data
        """
        async def on_ohlcv_data(ohlcv_data: dict) -> None:
            try:
                async with CandleRepository(
                    self.exchange, self.symbol, test=False
                ) as repo:
                    candle = Candle(
                        timestamp=ohlcv_data['timestamp'],
                        open=ohlcv_data['open'],
                        high=ohlcv_data['high'],
                        low=ohlcv_data['low'],
                        close=ohlcv_data['close'],
                        vol=ohlcv_data['volume']
                    )
                    await repo.save_candles([candle])

                self.logger.info(
                    "Real-time OHLCV data saved",
                    symbol=self.symbol,
                    timestamp=ohlcv_data['timestamp']
                )

                if callback:
                    await callback(ohlcv_data)

            except Exception as e:
                self.logger.error(
                    "Error processing OHLCV data",
                    symbol=self.symbol,
                    error=str(e)
                )

        await handler.subscribe_ohlcv(self.symbol, on_ohlcv_data, interval="1m")

    async def start_trade_streaming(
        self,
        handler: Any,
        callback: Optional[Callable] = None
    ) -> None:
        """
        Start trade WebSocket streaming (fallback method).

        Args:
            handler: WebSocket handler from fullon_exchange
            callback: Optional callback function for received data
        """
        async def on_trade_data(trade_data: dict) -> None:
            try:
                # Log trade data received (actual trade aggregation handled elsewhere)
                self.logger.info(
                    "Real-time trade data received (fallback)",
                    symbol=self.symbol,
                    price=trade_data.get('price', 0)
                )

                if callback:
                    await callback(trade_data)

            except Exception as e:
                self.logger.error(
                    "Error processing trade data",
                    symbol=self.symbol,
                    error=str(e)
                )

        await handler.subscribe_trades(self.symbol, on_trade_data)

    async def update_collector_status(self, message: str) -> None:
        """
        Update collector status using ProcessCache.

        Args:
            message: Status message to record
        """

        try:
            async with ProcessCache() as cache:
                component = f"ohlcv.live.{self.exchange}.{self.symbol}"

                if not hasattr(self, '_process_id') or not self._process_id:
                    # Register new process
                    self._process_id = await cache.register_process(
                        process_type=ProcessType.OHLCV,
                        component=component,
                        message=message,
                        status=ProcessStatus.RUNNING
                    )
                else:
                    # Update existing process
                    await cache.update_process(
                        process_id=self._process_id,
                        message=message,
                        heartbeat=True
                    )

        except Exception as e:
            self.logger.warning(
                "Could not update process status",
                error=str(e)
            )

    async def cleanup_process_status(self) -> None:
        """Clean up ProcessCache registration when stopping."""
        if hasattr(self, '_process_id') and self._process_id:
            try:
                async with ProcessCache() as cache:
                    await cache.stop_process(
                        process_id=self._process_id,
                        message="Live collector stopped"
                    )
                    self._process_id = None
            except Exception as e:
                self.logger.warning(
                    "Could not cleanup process status",
                    error=str(e)
                )