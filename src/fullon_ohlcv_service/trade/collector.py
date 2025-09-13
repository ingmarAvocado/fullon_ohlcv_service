from __future__ import annotations

"""Trade Collector - Single symbol trade data collection.

Simple integration that uses fullon_exchange for data retrieval/streaming and
fullon_ohlcv repositories for persistence. Mirrors the structure used by the
OHLCV collector to keep patterns consistent across the service.
"""

from fullon_log import get_component_logger
from fullon_exchange.queue import ExchangeQueue
from fullon_ohlcv.repositories.ohlcv import TradeRepository
from fullon_ohlcv.models import Trade


class TradeCollector:
    """Simple trade collector - integrates exchange + storage."""

    def __init__(self, exchange: str, symbol: str):
        self.logger = get_component_logger(f"fullon.trade.{exchange}.{symbol}")
        self.exchange = exchange
        self.symbol = symbol
        self.running = False

    async def collect_historical_trades(self) -> bool:
        """Collect a recent batch of trades and persist them.

        Mirrors the legacy "fetch_individual_trades" flow using fullon_exchange
        and saves via TradeRepository.
        """
        await ExchangeQueue.initialize_factory()
        try:
            # Use get_rest_handler pattern from OhlcvCollector
            handler = await ExchangeQueue.get_rest_handler(self.exchange)

            # Use get_trades method (standard fullon_exchange API)
            trades_data = await handler.get_trades(self.symbol, limit=1000)

            trades = [
                Trade(
                    timestamp=t["timestamp"],
                    price=t["price"],
                    volume=t["volume"],
                    side=t.get("side", "BUY"),
                    type=t.get("type", "MARKET"),
                )
                for t in trades_data
            ]

            async with TradeRepository(self.exchange, self.symbol, test=False) as repo:
                success = await repo.save_trades(trades)

            self.logger.info(
                "Historical trades saved",
                symbol=self.symbol,
                count=len(trades),
                success=success,
            )
            return success
        finally:
            await ExchangeQueue.shutdown_factory()

    async def start_streaming(self, callback=None) -> None:
        """Subscribe to trade stream and persist incoming trades."""
        await ExchangeQueue.initialize_factory()
        try:
            # Use get_websocket_handler pattern from OhlcvCollector
            handler = await ExchangeQueue.get_websocket_handler(self.exchange)
            await handler.connect()

            async def on_trade(trade_data):
                trade = Trade(
                    timestamp=trade_data["timestamp"],
                    price=trade_data["price"],
                    volume=trade_data["volume"],
                    side=trade_data.get("side", "BUY"),
                    type=trade_data.get("type", "MARKET"),
                )
                async with TradeRepository(self.exchange, self.symbol, test=False) as repo:
                    await repo.save_trades([trade])

                self.logger.info(
                    "Trade saved",
                    symbol=self.symbol,
                    price=trade.price,
                    volume=trade.volume,
                )

                if callback:
                    await callback(trade_data)

            await handler.subscribe_trades(self.symbol, on_trade)
            self.running = True

        finally:
            await ExchangeQueue.shutdown_factory()

    async def stop_streaming(self):
        """Stop the streaming collection."""
        self.running = False
        await ExchangeQueue.shutdown_factory()
        self.logger.info("Streaming stopped", symbol=self.symbol)
