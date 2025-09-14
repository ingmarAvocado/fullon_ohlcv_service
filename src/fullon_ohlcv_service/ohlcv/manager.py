"""OHLCV Manager - Multi-symbol OHLCV collection coordination."""

import asyncio
from typing import Any

from fullon_log import get_component_logger

from fullon_ohlcv_service.config.database_config import get_collection_targets
from fullon_ohlcv_service.ohlcv.collector import OhlcvCollector


class OhlcvManager:
    """Simple manager - coordinates collectors from database config."""

    def __init__(self) -> None:
        self.logger = get_component_logger("fullon.ohlcv.manager")
        self.collectors: dict[str, OhlcvCollector] = {}
        self.tasks: dict[str, asyncio.Task] = {}
        self.running = False

    async def start(self) -> None:
        """Start collectors for database-configured symbols (like legacy run_loop)."""
        if self.running:
            return

        # Get configuration from database (replaces legacy Database().get_symbols())
        targets = await get_collection_targets()

        # Start collector for each exchange/symbol (replaces legacy threading)
        for exchange, symbols in targets.items():
            for symbol in symbols:
                key = f"{exchange}:{symbol}"
                collector = OhlcvCollector(exchange, symbol)
                self.collectors[key] = collector

                # Start collection task (replaces legacy thread)
                task = asyncio.create_task(collector.start_streaming())
                self.tasks[key] = task

        self.running = True
        self.logger.info("Started collectors", count=len(self.collectors))

    async def stop(self) -> None:
        """Stop all collectors (like legacy stop_all)."""
        # Cancel all tasks
        for task in self.tasks.values():
            task.cancel()

        # Wait for cleanup
        if self.tasks:
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)

        self.collectors.clear()
        self.tasks.clear()
        self.running = False
        self.logger.info("Stopped all collectors")

    async def status(self) -> dict[str, Any]:
        """Get status of all collectors (like legacy status reporting)."""
        return {
            "running": self.running,
            "collectors": list(self.collectors.keys()),
            "active_tasks": len([t for t in self.tasks.values() if not t.done()])
        }
