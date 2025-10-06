"""
Trade Manager

Coordinates trade collection across multiple exchanges and symbols.
Follows the same pattern as OhlcvManager for consistency.
"""

import asyncio
from typing import Any, Dict

from fullon_log import get_component_logger
from fullon_cache import ProcessCache

from .live_collector import LiveTradeCollector
from .historic_collector import HistoricTradeCollector


class TradeManager:
    """
    Simple manager that coordinates trade collectors.

    Manages both live and historic trade collection across exchanges.
    """

    def __init__(self):
        self.logger = get_component_logger("fullon.trade.manager")
        self.live_collector: LiveTradeCollector | None = None
        self.historic_collector: HistoricTradeCollector | None = None
        self.running = False

    async def start_live_collection(self) -> None:
        """Start live trade collection for all configured symbols."""
        if self.running:
            return

        self.logger.info("Starting live trade collection")
        self.live_collector = LiveTradeCollector()
        await self.live_collector.start_collection()
        self.running = True

    async def start_historic_collection(self) -> Dict[str, int]:
        """Start historic trade collection for all configured symbols."""
        self.logger.info("Starting historic trade collection")
        self.historic_collector = HistoricTradeCollector()
        return await self.historic_collector.start_collection()

    async def stop(self) -> None:
        """Stop all trade collection."""
        if not self.running:
            return

        self.logger.info("Stopping trade collection")

        if self.live_collector:
            await self.live_collector.stop_collection()

        self.running = False

    async def health_check(self) -> Dict[str, Any]:
        """Return health status of trade collection."""
        return {
            "trade_manager": {
                "running": self.running,
                "live_collector": self.live_collector is not None,
                "historic_collector": self.historic_collector is not None,
            }
        }