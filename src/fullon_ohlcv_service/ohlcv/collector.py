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
            # Use configured historical limit
            candles = await handler.get_ohlcv(self.symbol, "1m", limit=self.config.historical_limit)

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
        """Start WebSocket streaming (replaces legacy WebSocket loop)"""
        # Check if streaming is enabled
        if not self.config.enable_streaming:
            self.logger.info("Streaming disabled", symbol=self.symbol)
            return False

        # Update status - starting
        await self._update_collector_status("Streaming started")

        await ExchangeQueue.initialize_factory()
        try:
            # Create exchange object for new API
            exchange_obj = self._create_exchange_object()
            credential_provider = self._create_credential_provider()

            handler = await ExchangeQueue.get_websocket_handler(exchange_obj, credential_provider)
            await handler.connect()

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

                self.logger.info("Real-time OHLCV data saved",
                               symbol=self.symbol, timestamp=ohlcv_data['timestamp'])

                if callback:
                    await callback(ohlcv_data)

            await handler.subscribe_ohlcv(self.symbol, on_ohlcv_data, interval="1m")
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
            if self.exchange_id:
                try:
                    from fullon_credentials import fullon_credentials
                    secret, key = fullon_credentials(ex_id=self.exchange_id)
                    return key, secret  # Note: fullon_credentials returns (secret, key) but exchange expects (key, secret)
                except (ImportError, ValueError) as e:
                    self.logger.warning(f"Could not get credentials for exchange {self.exchange_id}, using empty credentials for public data: {e}")
                    return "", ""
            else:
                # No exchange ID provided, use empty credentials for public data
                return "", ""

        return credential_provider

    async def _update_collector_status(self, message: str) -> None:
        """Update individual collector status (like legacy _update_process)."""
        async with ProcessCache() as cache:
            key = f"{self.exchange}:{self.symbol}"
            await cache.update_process(
                tipe="ohlcv",
                key=key,
                message=message
            )
