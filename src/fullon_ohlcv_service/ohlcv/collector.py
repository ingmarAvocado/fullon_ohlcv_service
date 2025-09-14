from fullon_exchange.queue import ExchangeQueue
from fullon_log import get_component_logger
from fullon_ohlcv.models import Candle
from fullon_ohlcv.repositories.ohlcv import CandleRepository

# Use mock ProcessCache until fullon_cache is available
try:
    from fullon_cache import ProcessCache
except ImportError:
    from fullon_ohlcv_service.utils.process_cache import ProcessCache


class OhlcvCollector:
    """Simple OHLCV collector - integrates exchange + storage"""

    def __init__(self, exchange: str, symbol: str):
        self.logger = get_component_logger(f"fullon.ohlcv.{exchange}.{symbol}")
        self.exchange = exchange
        self.symbol = symbol
        self.running = False

    async def collect_historical(self):
        """Collect historical OHLCV data (like legacy fetch_candles)"""
        # Update status - collecting
        await self._update_collector_status("Collecting historical data")

        await ExchangeQueue.initialize_factory()
        try:
            handler = await ExchangeQueue.get_rest_handler(self.exchange)
            candles = await handler.get_ohlcv(self.symbol, "1m", limit=100)

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
        # Update status - starting
        await self._update_collector_status("Streaming started")

        await ExchangeQueue.initialize_factory()
        try:
            handler = await ExchangeQueue.get_websocket_handler(self.exchange)
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

    async def _update_collector_status(self, message: str) -> None:
        """Update individual collector status (like legacy _update_process)."""
        async with ProcessCache() as cache:
            key = f"{self.exchange}:{self.symbol}"
            await cache.update_process(
                tipe="ohlcv",
                key=key,
                message=message
            )
