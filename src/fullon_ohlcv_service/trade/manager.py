"""Trade Manager - Multi-symbol trade collection coordination."""

import asyncio

from fullon_log import get_component_logger
from fullon_orm.database_context import DatabaseContext
from fullon_ohlcv_service.config.database_config import get_collection_targets

from fullon_ohlcv_service.trade.collector import TradeCollector


class TradeManager:
    """Trade manager coordinating multiple TradeCollector instances."""

    def __init__(self):
        self.logger = get_component_logger("fullon.trade.manager")
        self.collectors: dict[str, dict] = {}  # Will store {"collector": collector, "task": task}
        self.running = False

    async def start(self):
        """Start the Trade manager and collectors for database-configured symbols."""
        if self.running:
            return

        self.running = True

        # Start collectors based on database configuration
        await self.start_from_database()

        self.logger.info("TradeManager started", collectors=len(self.collectors))

    async def stop(self):
        """Stop the Trade manager and all collectors."""
        self.running = False

        # Stop all collectors gracefully
        for key in list(self.collectors.keys()):
            exchange, symbol = key.split(":")
            await self.stop_collector(exchange, symbol)

        self.logger.info("TradeManager stopped")

    async def get_collection_targets(self) -> dict[str, dict]:
        """Get trade collection targets from fullon database - like ticker service."""
        # Use the same logic as OHLCV service for consistency
        return await get_collection_targets()

    async def start_collector(self, exchange: str, symbol: str, exchange_id: int = None) -> None:
        """Start a trade collector for specific exchange/symbol."""
        key = f"{exchange}:{symbol}"
        if key in self.collectors:
            return

        collector = TradeCollector(exchange, symbol, exchange_id=exchange_id)
        # Create background task for streaming
        task = asyncio.create_task(collector.start_streaming())

        # Store both collector and task
        self.collectors[key] = {
            "collector": collector,
            "task": task
        }

        self.logger.info("Trade collector started", exchange=exchange, symbol=symbol)

    async def start_collector_with_historical(self, exchange: str, symbol: str, symbol_obj, exchange_id: int = None) -> None:
        """Start a trade collector with historical collection first, then streaming.

        Args:
            exchange: Exchange name (category name for fullon_exchange)
            symbol: Symbol name
            symbol_obj: Symbol model object with backtest parameter
            exchange_id: Exchange ID for credentials
        """
        key = f"{exchange}:{symbol}"
        if key in self.collectors:
            return

        collector = TradeCollector(exchange, symbol, exchange_id=exchange_id)

        # First run historical collection using symbol.backtest parameter
        if symbol_obj:
            backtest_days = getattr(symbol_obj, 'backtest', 30)
            self.logger.info("Starting historical collection before streaming",
                           exchange=exchange,
                           symbol=symbol,
                           backtest_days=backtest_days)

            try:
                # Run historical collection in test mode for now (single batch)
                historical_success = await collector.collect_historical_range(symbol_obj, test_mode=True)
                if historical_success:
                    self.logger.info("Historical collection completed successfully",
                                   exchange=exchange,
                                   symbol=symbol)
                else:
                    self.logger.warning("Historical collection had issues",
                                      exchange=exchange,
                                      symbol=symbol)
            except Exception as e:
                self.logger.error("Historical collection failed",
                                exchange=exchange,
                                symbol=symbol,
                                error=str(e))

        # Then start streaming collection
        task = asyncio.create_task(collector.start_streaming())

        # Store both collector and task
        self.collectors[key] = {
            "collector": collector,
            "task": task
        }

        self.logger.info("Trade collector started with historical collection",
                        exchange=exchange,
                        symbol=symbol,
                        backtest_days=getattr(symbol_obj, 'backtest', 30) if symbol_obj else 30)

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

        # Get symbol objects from database to access backtest parameter
        async with DatabaseContext() as db:
            for exchange, exchange_info in targets.items():
                symbols = exchange_info["symbols"]
                ex_id = exchange_info.get("ex_id")
                exchange_category_name = exchange_info.get("exchange_category_name", exchange)  # Use category name for fullon_exchange

                for symbol_name in symbols:
                    # Get the Symbol object from database to access backtest parameter
                    symbol_obj = await db.symbols.get_by_symbol(symbol_name)
                    await self.start_collector_with_historical(exchange_category_name, symbol_name, symbol_obj, exchange_id=ex_id)

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
