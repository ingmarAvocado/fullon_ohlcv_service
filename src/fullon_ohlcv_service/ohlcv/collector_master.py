"""
OHLCV Collector Master

Coordinates between historical and live OHLCV data collection.
"""

import asyncio
from typing import Optional, Callable

from fullon_log import get_component_logger
from fullon_orm.models import Symbol

from .historic_collector import HistoricCollector
from .live_collector import LiveCollector


class OhlcvCollectorMaster:
    """
    Master OHLCV collector that coordinates historical and live data collection.

    Manages the two-phase collection process:
    1. Historical data collection via REST APIs
    2. Real-time data streaming via WebSockets
    """

    def __init__(self, symbol_obj: Symbol) -> None:
        """
        Initialize master collector for a specific symbol.

        Args:
            symbol_obj: Symbol object from fullon_orm database
        """
        self.symbol_obj = symbol_obj
        self.exchange = symbol_obj.exchange_name
        self.symbol = symbol_obj.symbol

        # Setup logging
        self.logger = get_component_logger(
            f"fullon.ohlcv.master.{self.exchange}.{self.symbol}"
        )

        # Initialize sub-collectors
        self.historic_collector = HistoricCollector(symbol_obj)
        self.live_collector = LiveCollector(symbol_obj)

        self.logger.debug(
            "OHLCV master collector created",
            symbol=self.symbol,
            exchange=self.exchange,
            backtest_days=self.symbol_obj.backtest
        )

    async def collect_historical(self) -> bool:
        """
        Collect historical OHLCV data using REST API.

        Returns:
            bool: True if collection successful, False otherwise
        """
        self.logger.info("Starting historical collection", symbol=self.symbol)
        return await self.historic_collector.collect()

    async def start_streaming(self, callback: Optional[Callable] = None) -> bool:
        """
        Start real-time WebSocket streaming for OHLCV data.

        Args:
            callback: Optional callback function for received data

        Returns:
            bool: True if streaming started successfully, False otherwise
        """
        self.logger.info("Starting live streaming", symbol=self.symbol)
        return await self.live_collector.start_streaming(callback)

    async def stop_streaming(self) -> None:
        """Stop the streaming collection and cleanup resources."""
        self.logger.info("Stopping live streaming", symbol=self.symbol)
        await self.live_collector.stop_streaming()

    async def run_full_collection(self, callback: Optional[Callable] = None) -> bool:
        """
        Run complete two-phase collection: historical then streaming.

        Args:
            callback: Optional callback function for live data

        Returns:
            bool: True if both phases started successfully
        """
        self.logger.info("Starting full collection process", symbol=self.symbol)

        # Phase 1: Historical collection
        historical_success = await self.collect_historical()
        if not historical_success:
            self.logger.error("Historical collection failed", symbol=self.symbol)
            return False

        # Phase 2: Live streaming
        streaming_success = await self.start_streaming(callback)
        if not streaming_success:
            self.logger.error("Live streaming failed", symbol=self.symbol)
            return False

        self.logger.info("Full collection process started", symbol=self.symbol)
        return True