import asyncio
from fullon_exchange.queue import ExchangeQueue
from fullon_log import get_component_logger
from fullon_ohlcv.models import Candle
from fullon_ohlcv.repositories.ohlcv import CandleRepository
from fullon_ohlcv_service.config.settings import OhlcvServiceConfig

# Use mock ProcessCache until fullon_cache is available
try:
    from fullon_cache import ProcessCache
except ImportError:
    from fullon_ohlcv_service.utils.process_cache import ProcessCache


class OhlcvCollector:
    """Simple OHLCV collector - integrates exchange + storage"""

    def __init__(self, exchange: str, symbol: str, exchange_id: int = None, config: OhlcvServiceConfig = None):
        self.logger = get_component_logger(f"fullon.ohlcv.{exchange}.{symbol}")
        self.exchange = exchange
        self.symbol = symbol
        self.exchange_id = exchange_id
        self.running = False
        self._process_id = None  # ProcessCache process ID
        # Use provided config or load from environment
        self.config = config or OhlcvServiceConfig.from_env()

    async def collect_historical(self):
        """Collect historical OHLCV data (like legacy fetch_candles)"""
        # Check if historical collection is enabled
        if not self.config.enable_historical:
            self.logger.info("Historical collection disabled", symbol=self.symbol)
            return False

        # Update status - collecting
        await self._update_collector_status("Collecting historical data")

        await ExchangeQueue.initialize_factory()
        try:
            # Create exchange object for new API
            exchange_obj = self._create_exchange_object()
            credential_provider = self._create_credential_provider()

            handler = await ExchangeQueue.get_rest_handler(exchange_obj, credential_provider)
            await handler.connect()

            # Implement two-phase collection strategy (Phase 1: Historical REST)
            candles = await self._collect_historical_data(handler)

            async with CandleRepository(self.exchange, self.symbol, test=False) as repo:
                success = await repo.save_candles(candles)

            # Update status - completed
            await self._update_collector_status(
                f"Historical collection completed - {len(candles)} candles"
            )

            self.logger.info("Historical collection completed",
                           symbol=self.symbol, count=len(candles), success=success)
            return success

        finally:
            await ExchangeQueue.shutdown_factory()

    async def start_streaming(self, callback=None):
        """Phase 2: Start WebSocket streaming (following legacy pattern)"""
        # Check if streaming is enabled
        if not self.config.enable_streaming:
            self.logger.info("Streaming disabled", symbol=self.symbol)
            return False

        # Update status - starting
        await self._update_collector_status("Phase 2: Real-time streaming started")

        await ExchangeQueue.initialize_factory()
        try:
            # Create exchange object for new API
            exchange_obj = self._create_exchange_object()
            credential_provider = self._create_credential_provider()

            handler = await ExchangeQueue.get_websocket_handler(exchange_obj, credential_provider)
            await handler.connect()

            # Simple priority: try OHLCV streaming first, fallback to trades
            if handler.supports_ohlcv():
                self.logger.info("Starting OHLCV WebSocket streaming (primary)")
                await self._start_ohlcv_streaming(handler, callback)
            else:
                self.logger.info("Starting trade WebSocket streaming (fallback)")
                await self._start_trade_streaming(handler, callback)

            self.running = True

        finally:
            await ExchangeQueue.shutdown_factory()

    async def stop_streaming(self):
        """Stop the streaming collection"""
        self.running = False
        await ExchangeQueue.shutdown_factory()
        self.logger.info("Streaming stopped", symbol=self.symbol)

    def _create_exchange_object(self):
        """Create exchange object for new fullon_exchange API."""
        class SimpleExchange:
            def __init__(self, exchange_name: str, account_id: str):
                self.ex_id = f"{exchange_name}_{account_id}"
                self.uid = account_id
                self.test = False
                self.cat_exchange = type('CatExchange', (), {'name': exchange_name})()

        # Use test mode from config
        exchange_obj = SimpleExchange(self.exchange, "ohlcv_account")
        exchange_obj.test = self.config.test_mode
        return exchange_obj

    def _create_credential_provider(self):
        """Create credential provider for new fullon_exchange API.

        Uses fullon_credentials service when exchange_id is available,
        falls back to empty credentials for public data.
        """
        def credential_provider(exchange_obj):
            # Use exchange_id for credentials if available, otherwise try to get admin's exchange ID
            if self.exchange_id:
                try:
                    from fullon_credentials import fullon_credentials
                    secret, key = fullon_credentials(ex_id=self.exchange_id)
                    self.logger.info(f"Using credentials for exchange_id {self.exchange_id}")
                    return key, secret  # Note: fullon_credentials returns (secret, key) but exchange expects (key, secret)

                except (ImportError, ValueError) as e:
                    self.logger.error(f"Could not get credentials for exchange_id {self.exchange_id}: {e}")
                    self.logger.error(f"Cannot collect private data for {self.exchange}:{self.symbol} without credentials")
                    return "", ""
            else:
                # No exchange_id provided - log this and use empty credentials for public data
                import os
                admin_mail = os.getenv('ADMIN_MAIL', 'admin@fullon')
                self.logger.warning(f"No exchange_id provided for {self.exchange}:{self.symbol}")
                self.logger.warning(f"Admin {admin_mail} should have proper exchange_id mapping for private data access")
                self.logger.info(f"Using empty credentials for public data collection only")
                return "", ""

        return credential_provider

    async def _collect_historical_data(self, handler, start_time=None, end_time=None) -> list:
        """Two-phase historical collection following legacy pattern.

        Phase 1: REST-based historical catch-up
        Uses simple priority: OHLCV first (if supported), trades as fallback
        """
        try:
            # Simple priority check - OHLCV first, trades as fallback
            if handler.supports_ohlcv():
                self.logger.info("Using OHLCV collection (primary method)", exchange=self.exchange)
                return await self._collect_historical_ohlcv(handler, start_time, end_time)
            else:
                self.logger.info("Using trade collection (fallback method)", exchange=self.exchange)
                return await self._collect_historical_trades(handler, start_time, end_time)

        except Exception as e:
            self.logger.error("Historical collection failed",
                            exchange=self.exchange, symbol=self.symbol, error=str(e))
            return []

    async def _collect_historical_ohlcv(self, handler, start_time=None, end_time=None) -> list:
        """Collect historical OHLCV data with pagination (like legacy fetch_candles)."""
        all_candles = []

        if start_time and end_time:
            # Paginated historical collection (point A to point B)
            current_time = start_time
            while current_time < end_time:
                try:
                    batch = await handler.get_ohlcv(
                        self.symbol,
                        timeframe="1m",
                        since=current_time,
                        limit=1000  # Max per call
                    )

                    if not batch:
                        break

                    all_candles.extend(batch)

                    # Update current_time to last timestamp + 1 minute
                    if batch:
                        last_timestamp = batch[-1][0]  # Timestamp is at index 0
                        current_time = last_timestamp + 60000  # Add 1 minute in ms
                    else:
                        break

                    # Rate limiting
                    await asyncio.sleep(0.1)

                except Exception as e:
                    self.logger.error("Historical OHLCV batch failed",
                                    current_time=current_time, error=str(e))
                    break
        else:
            # Simple recent data collection (legacy style)
            all_candles = await handler.get_ohlcv(self.symbol, "1m", limit=self.config.historical_limit)

        self.logger.info("Historical OHLCV collection completed",
                       candles_collected=len(all_candles))
        return all_candles

    async def _collect_historical_trades(self, handler, start_time=None, end_time=None) -> list:
        """Collect historical trades with pagination (fallback when OHLCV not supported)."""
        all_trades = []

        if start_time and end_time:
            # Paginated historical collection
            current_time = start_time
            while current_time < end_time:
                try:
                    batch = await handler.get_public_trades(
                        self.symbol,
                        since=current_time,
                        limit=1000
                    )

                    if not batch:
                        break

                    all_trades.extend(batch)

                    # Update current_time to last trade timestamp + 1ms
                    if batch:
                        last_timestamp = batch[-1].get('timestamp', current_time)
                        current_time = last_timestamp + 1
                    else:
                        break

                    # Rate limiting
                    await asyncio.sleep(0.1)

                except Exception as e:
                    self.logger.error("Historical trades batch failed",
                                    current_time=current_time, error=str(e))
                    break
        else:
            # Simple recent data collection
            all_trades = await handler.get_public_trades(self.symbol, limit=self.config.historical_limit)

        self.logger.info("Historical trades collection completed (fallback)",
                       trades_collected=len(all_trades))

        # Convert trades to candles if needed (simplified)
        # For now, return empty list and let trade collector handle trades
        return []

    async def _start_ohlcv_streaming(self, handler, callback=None):
        """Start OHLCV WebSocket streaming (primary method)."""
        async def on_ohlcv_data(ohlcv_data):
            async with CandleRepository(self.exchange, self.symbol, test=False) as repo:
                candle = Candle(
                    timestamp=ohlcv_data['timestamp'],
                    open=ohlcv_data['open'],
                    high=ohlcv_data['high'],
                    low=ohlcv_data['low'],
                    close=ohlcv_data['close'],
                    vol=ohlcv_data['volume']
                )
                await repo.save_candles([candle])

            self.logger.info("Real-time OHLCV data saved (primary)",
                           symbol=self.symbol, timestamp=ohlcv_data['timestamp'])

            if callback:
                await callback(ohlcv_data)

        await handler.subscribe_ohlcv(self.symbol, on_ohlcv_data, interval="1m")

    async def _start_trade_streaming(self, handler, callback=None):
        """Start trade WebSocket streaming (fallback method)."""
        async def on_trade_data(trade_data):
            # Convert trade to candle format for consistency
            # This is a simplified conversion - in practice you'd aggregate trades into candles
            self.logger.info("Real-time trade data received (fallback)",
                           symbol=self.symbol, price=trade_data.get('price', 0))

            if callback:
                await callback(trade_data)

        await handler.subscribe_trades(self.symbol, on_trade_data)

    async def _update_collector_status(self, message: str) -> None:
        """Update individual collector status using ProcessCache."""
        try:
            from fullon_cache import ProcessType, ProcessStatus

            async with ProcessCache() as cache:
                component = f"ohlcv.{self.exchange}.{self.symbol}"

                # Store process_id as instance variable if not exists
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

        except ImportError:
            # Fallback for development/testing
            self.logger.debug(f"ProcessCache not available, status: {message}")
        except Exception as e:
            self.logger.warning(f"Could not update process status: {e}")

    async def _cleanup_process_status(self) -> None:
        """Clean up ProcessCache registration when stopping."""
        if hasattr(self, '_process_id') and self._process_id:
            try:
                async with ProcessCache() as cache:
                    await cache.stop_process(
                        process_id=self._process_id,
                        message="Collector stopped"
                    )
                    self._process_id = None
            except Exception as e:
                self.logger.warning(f"Could not cleanup process status: {e}")
