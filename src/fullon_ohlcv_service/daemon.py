"""
OHLCV Service Daemon

Lightweight coordinator for OHLCV and trade data collection services.
Manages two-phase collection pattern: historic catch-up followed by live streaming.

Usage:
    daemon = OhlcvServiceDaemon()
    await daemon.run()  # Full collection for all symbols
    await daemon.process_symbol(symbol)  # Single symbol collection
"""

import asyncio
import os

from fullon_cache import ProcessCache
from fullon_cache.process_cache import ProcessStatus, ProcessType
from fullon_log import get_component_logger
from fullon_orm import DatabaseContext

from fullon_ohlcv_service.ohlcv.historic_collector import HistoricOHLCVCollector
from fullon_ohlcv_service.ohlcv.live_collector import LiveOHLCVCollector
from fullon_ohlcv_service.trade.historic_collector import HistoricTradeCollector
from fullon_ohlcv_service.trade.live_collector import LiveTradeCollector
from fullon_ohlcv_service.utils.add_symbols import add_all_symbols


class OhlcvServiceDaemon:
    """
    Lightweight coordinator for OHLCV and trade data collection.

    Manages collectors for historic and live data collection phases.
    Integrates with fullon_cache for health monitoring and process tracking.
    """

    def __init__(self) -> None:
        self.logger = get_component_logger("fullon.ohlcv.daemon")
        self.live_ohlcv_collector: LiveOHLCVCollector | None = None
        self.historic_ohlcv_collector: HistoricOHLCVCollector | None = None
        self.live_trade_collector: LiveTradeCollector | None = None
        self.historic_trade_collector: HistoricTradeCollector | None = None
        self.process_id: str | None = None
        self._symbols: list = []

    async def _register_daemon(self) -> None:
        """Register daemon with ProcessCache for health monitoring and tracking."""
        async with ProcessCache() as cache:
            self.process_id = await cache.register_process(
                process_type=ProcessType.OHLCV,
                component="ohlcv_daemon",
                params={"pid": os.getpid(), "args": ["ohlcv_daemon"]},
                message="OHLCV daemon started successfully",
                status=ProcessStatus.STARTING,
            )

    async def run(self) -> None:
        """
        Main entry point for full daemon operation.

        Executes two-phase collection:
        1. Historic phase: REST-based catch-up for all symbols
        2. Live phase: WebSocket-based real-time streaming

        Raises:
            Exception: Propagates exceptions after cleanup
        """
        try:
            self.logger.info("Starting OHLCV Service Daemon")

            # Load symbols from database
            async with DatabaseContext() as db:
                self._symbols = await db.symbols.get_all()

            if not self._symbols:
                self.logger.warning("No symbols found in database - nothing to collect")
                return

            self.logger.info("Loaded symbols from database", symbol_count=len(self._symbols))

            # Initialize symbol tables in TimescaleDB
            success = await add_all_symbols(symbols=self._symbols)
            if not success:
                self.logger.warning("Some symbol initialization failed - continuing with available symbols")

            # Initialize collectors (exchange capability checking done internally)
            self.live_ohlcv_collector = LiveOHLCVCollector(symbols=self._symbols)
            self.historic_ohlcv_collector = HistoricOHLCVCollector(symbols=self._symbols)
            self.live_trade_collector = LiveTradeCollector(symbols=self._symbols)
            self.historic_trade_collector = HistoricTradeCollector(symbols=self._symbols)

            # Register with ProcessCache
            await self._register_daemon()

            # Phase 1: Historic data collection via REST
            self.logger.info("Phase 1: Starting historic data collection (REST)")
            await asyncio.gather(
                self.historic_ohlcv_collector.start_collection(),
                self.historic_trade_collector.start_collection(),
            )
            self.logger.info("Phase 1: Historic collection completed")

            # Cleanup historic collectors to free memory
            del self.historic_ohlcv_collector
            del self.historic_trade_collector
            self.historic_ohlcv_collector = None
            self.historic_trade_collector = None

            # Phase 2: Live data streaming via WebSocket
            self.logger.info("Phase 2: Starting live data streaming (WebSocket)")
            await asyncio.gather(
                self.live_ohlcv_collector.start_collection(),
                self.live_trade_collector.start_collection(),
            )

        except asyncio.CancelledError:
            self.logger.info("Daemon shutdown requested")
            raise
        except Exception as e:
            self.logger.error("Daemon error", error=str(e))
            raise
        finally:
            await self.cleanup()

    async def process_symbol(self, symbol) -> None:
        """
        Process a single symbol for data collection.

        Behavior depends on daemon state:
        - If daemon is running: Adds symbol to existing collectors dynamically
        - If daemon is not running: Starts fresh daemon with historic + live phases

        Args:
            symbol: Symbol model instance from fullon_orm

        Raises:
            ValueError: If symbol parameter is invalid
        """
        # Validate symbol structure
        if not symbol or not hasattr(symbol, 'symbol') or not hasattr(symbol, 'cat_exchange'):
            raise ValueError("Invalid symbol - must be Symbol model instance with symbol and cat_exchange attributes")

        # Initialize symbol table in TimescaleDB
        success = await add_all_symbols(symbols=[symbol])
        if not success:
            self.logger.warning(
                "Symbol initialization failed - continuing anyway",
                symbol=symbol.symbol,
                exchange=symbol.cat_exchange.name
            )

        # Check daemon state and handle accordingly
        if self.live_ohlcv_collector and self.live_trade_collector:
            # Daemon is running - check if already collecting
            if self.live_ohlcv_collector.is_collecting(symbol):
                self.logger.info("Symbol already collecting", symbol=symbol.symbol)
                return

            if self.live_trade_collector.is_collecting(symbol):
                self.logger.info("Symbol already collecting", symbol=symbol.symbol)
                return

            self.logger.info(
                "Adding symbol to running daemon",
                symbol=symbol.symbol,
                exchange=symbol.cat_exchange.name,
            )
            # Fall through to start live collection

        elif not self.live_ohlcv_collector and not self.live_trade_collector:
            # Daemon not running - execute full two-phase collection
            self.logger.info(
                "Starting two-phase collection for single symbol",
                symbol=symbol.symbol,
                exchange=symbol.cat_exchange.name,
            )

            self._symbols = [symbol]

            # Phase 1: Historic data collection (REST)
            self.logger.info("Phase 1: Starting historic collection")
            self.historic_ohlcv_collector = HistoricOHLCVCollector()
            self.historic_trade_collector = HistoricTradeCollector()

            await asyncio.gather(
                self.historic_ohlcv_collector.start_symbol(symbol),
                self.historic_trade_collector.start_symbol(symbol),
            )
            self.logger.info("Phase 1: Historic collection completed")

            # Cleanup historic collectors to free memory
            del self.historic_ohlcv_collector
            del self.historic_trade_collector
            self.historic_ohlcv_collector = None
            self.historic_trade_collector = None

            # Phase 2: Live data streaming (WebSocket)
            self.logger.info("Phase 2: Starting live streaming")
            self.live_ohlcv_collector = LiveOHLCVCollector()
            self.live_trade_collector = LiveTradeCollector()

            # Register with ProcessCache
            await self._register_daemon()
            # Continue to start live collection

        else:
            # Partially running state - should not happen in normal operation
            self.logger.error("Daemon in inconsistent state - cannot proceed")
            return

        # Start live collection (common final step for both paths)
        self.logger.info("Starting live collectors for symbol", symbol=symbol.symbol)
        await self.live_ohlcv_collector.start_symbol(symbol)
        await self.live_trade_collector.start_symbol(symbol)

    async def cleanup(self) -> None:
        """
        Gracefully cleanup all daemon resources.

        Stops all active collectors and updates ProcessCache status.
        Suppresses errors during cleanup to ensure complete shutdown.
        """
        self.logger.info("Starting daemon cleanup")

        # Stop all active collectors
        for collector in [self.live_trade_collector]:
            if collector:
                try:
                    await collector.stop_collection()
                except Exception as e:
                    self.logger.warning("Error stopping collector during cleanup", error=str(e))

        # Update ProcessCache with final status
        if self.process_id:
            try:
                async with ProcessCache() as cache:
                    await cache.update_process(
                        process_id=self.process_id,
                        status=ProcessStatus.STOPPED,
                        message="Daemon shutdown complete",
                    )
            except Exception as e:
                self.logger.warning("Failed to update ProcessCache during cleanup", error=str(e))

        self.logger.info("Daemon cleanup completed successfully")
