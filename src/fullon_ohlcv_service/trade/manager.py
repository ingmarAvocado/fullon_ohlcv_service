"""Trade Manager - Multi-symbol trade collection coordination."""

import asyncio
from fullon_log import get_component_logger
from fullon_orm.database_context import DatabaseContext
from fullon_ohlcv_service.trade.collector import TradeCollector


class TradeManager:
    """Trade manager coordinating multiple TradeCollector instances."""

    def __init__(self):
        self.logger = get_component_logger("fullon.trade.manager")
        self.collectors: dict[str, dict] = {}  # Will store {"collector": collector, "task": task}
        self.running = False

    async def start(self):
        """Start the Trade manager."""
        self.running = True
        self.logger.info("TradeManager started")

    async def stop(self):
        """Stop the Trade manager and all collectors."""
        self.running = False

        # Stop all collectors gracefully
        for key in list(self.collectors.keys()):
            exchange, symbol = key.split(":")
            await self.stop_collector(exchange, symbol)

        self.logger.info("TradeManager stopped")

    async def get_collection_targets(self) -> dict[str, list[str]]:
        """Get trade collection targets from fullon database - like ticker service."""
        async with DatabaseContext() as db:
            exchanges = await db.exchanges.get_user_exchanges(user_id=1)

            targets: dict[str, list[str]] = {}
            for exchange in exchanges:
                if exchange["active"]:
                    symbols = await db.symbols.get_by_exchange_id(exchange["cat_ex_id"])
                    targets[exchange["name"]] = [s.symbol for s in symbols if getattr(s, "active", True)]

            return targets

    async def start_collector(self, exchange: str, symbol: str) -> None:
        """Start a trade collector for specific exchange/symbol."""
        key = f"{exchange}:{symbol}"
        if key in self.collectors:
            return

        collector = TradeCollector(exchange, symbol)
        # Create background task for streaming
        task = asyncio.create_task(collector.start_streaming())

        # Store both collector and task
        self.collectors[key] = {
            "collector": collector,
            "task": task
        }

        self.logger.info("Trade collector started", exchange=exchange, symbol=symbol)

    async def stop_collector(self, exchange: str, symbol: str) -> None:
        """Stop a specific trade collector and cancel its task."""
        key = f"{exchange}:{symbol}"
        if key in self.collectors:
            collector_info = self.collectors[key]
            collector = collector_info["collector"]
            task = collector_info["task"]

            # Stop the collector
            await collector.stop_streaming()

            # Cancel the task
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass  # Expected when task is cancelled

            del self.collectors[key]
            self.logger.info("Trade collector stopped", exchange=exchange, symbol=symbol)

    async def start_from_database(self) -> None:
        """Start collectors based on database configuration."""
        targets = await self.get_collection_targets()
        for exchange, symbols in targets.items():
            for symbol in symbols:
                await self.start_collector(exchange, symbol)

    async def get_status(self) -> dict[str, dict]:
        """Get status of all active trade collectors."""
        status: dict[str, dict] = {}
        for key, collector_info in self.collectors.items():
            collector = collector_info["collector"]
            task = collector_info["task"]
            status[key] = {
                "exchange": collector.exchange,
                "symbol": collector.symbol,
                "active": True,
                "running": getattr(collector, "running", False),
                "task_done": task.done() if task else True
            }
        return status
