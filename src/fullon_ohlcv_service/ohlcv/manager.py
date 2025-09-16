"""OHLCV Manager - Multi-symbol OHLCV collection coordination."""

import asyncio
from typing import Any

from fullon_log import get_component_logger

from fullon_ohlcv_service.config.database_config import get_collection_targets
from fullon_ohlcv_service.config.settings import OhlcvServiceConfig
from fullon_ohlcv_service.ohlcv.collector import OhlcvCollector

# Use mock ProcessCache until fullon_cache is available
try:
    from fullon_cache import ProcessCache
except ImportError:
    from fullon_ohlcv_service.utils.process_cache import ProcessCache


class OhlcvManager:
    """Simple manager - coordinates collectors from database config."""

    def __init__(self, config: OhlcvServiceConfig = None) -> None:
        self.logger = get_component_logger("fullon.ohlcv.manager")
        self.collectors: dict[str, OhlcvCollector] = {}
        self.tasks: dict[str, asyncio.Task] = {}
        self.running = False
        self._health_task: asyncio.Task | None = None
        # Use provided config or load from environment
        self.config = config or OhlcvServiceConfig.from_env()

    async def start(self) -> None:
        """Start collectors for database-configured symbols (like legacy run_loop)."""
        if self.running:
            return

        # Register process in cache
        # await self._register_process()  # Temporarily disabled until ProcessCache API is clarified

        # Get configuration from database using configured user_id
        targets = await get_collection_targets(user_id=self.config.user_id)

        # Start collector for each exchange/symbol (replaces legacy threading)
        for exchange, exchange_info in targets.items():
            symbols = exchange_info["symbols"]
            ex_id = exchange_info.get("ex_id")
            exchange_category_name = exchange_info.get("exchange_category_name", exchange)  # Use category name for fullon_exchange

            for symbol in symbols:
                key = f"{exchange}:{symbol}"
                # Pass config to each collector - use category exchange name for fullon_exchange compatibility
                collector = OhlcvCollector(exchange_category_name, symbol, exchange_id=ex_id, config=self.config)
                self.collectors[key] = collector

                # Start collection task (replaces legacy thread)
                task = asyncio.create_task(collector.start_streaming())
                self.tasks[key] = task

        self.running = True

        # Start health monitoring
        self._health_task = asyncio.create_task(self._health_loop())

        self.logger.info("Started collectors", count=len(self.collectors))

    async def stop(self) -> None:
        """Stop all collectors (like legacy stop_all)."""
        self.running = False

        # Cancel health task
        if self._health_task and not self._health_task.done():
            self._health_task.cancel()
            try:
                await self._health_task
            except asyncio.CancelledError:
                pass

        # Cancel all collector tasks
        for task in self.tasks.values():
            task.cancel()

        # Wait for cleanup
        if self.tasks:
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)

        self.collectors.clear()
        self.tasks.clear()

        # Clean up process registration
        # await self._cleanup_process()  # Temporarily disabled until ProcessCache API is clarified

        self.logger.info("Stopped all collectors")

    async def status(self) -> dict[str, Any]:
        """Get status of all collectors (like legacy status reporting)."""
        return {
            "running": self.running,
            "collectors": list(self.collectors.keys()),
            "active_tasks": len([t for t in self.tasks.values() if not t.done()])
        }

    async def _register_process(self) -> None:
        """Register daemon in ProcessCache (like legacy cache.new_process)."""
        async with ProcessCache() as cache:
            await cache.new_process(
                tipe="ohlcv_service",
                key="ohlcv_daemon",
                pid=f"async:{id(self)}",
                params=["ohlcv_daemon"],
                message="Started"
            )

    async def _update_health(self) -> None:
        """Update health status in cache (like legacy cache.update_process)."""
        async with ProcessCache() as cache:
            await cache.update_process(
                tipe="ohlcv_service",
                key="ohlcv_daemon",
                message=f"Running - {len(self.collectors)} collectors active"
            )

    async def _cleanup_process(self) -> None:
        """Clean up process registration on shutdown."""
        async with ProcessCache() as cache:
            try:
                await cache.delete_process("ohlcv_service", "ohlcv_daemon")
            except Exception as e:
                self.logger.warning("Error cleaning up process", error=str(e))

    async def _health_loop(self) -> None:
        """Periodic health updates (like legacy _update_process)."""
        while self.running:
            try:
                await self._update_health()
                # Use configured health update interval
                await asyncio.sleep(self.config.health_update_interval)
            except Exception as e:
                self.logger.error("Health update error", error=str(e))
                break
