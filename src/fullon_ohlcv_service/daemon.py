"""
OHLCV Service Daemon

Simple library for coordinating OHLCV and Trade collection services.
Used by parent daemon for process lifecycle management.
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
    """Simple daemon coordinator for OHLCV and Trade services"""

    def __init__(self) -> None:
        self.logger = get_component_logger("fullon.ohlcv.daemon")
        self.live_ohlcv_collector: LiveOHLCVCollector | None = None
        self.historic_ohlcv_collector: HistoricOHLCVCollector | None = None
        self.live_trade_collector: LiveTradeCollector | None = None
        self.historic_trade_collector: HistoricTradeCollector | None = None
        self.process_id: str | None = None
        self._symbols: list = []

    async def _register_daemon(self) -> None:
        """Register daemon in ProcessCache for health monitoring"""
        async with ProcessCache() as cache:
            self.process_id = await cache.register_process(
                process_type=ProcessType.OHLCV,
                component="ohlcv_daemon",
                params={"pid": os.getpid(), "args": ["ohlcv_daemon"]},
                message="Started",
                status=ProcessStatus.STARTING,
            )

    async def run(self) -> None:
        """Main entry point - runs historic then live phases"""
        try:
            self.logger.info("Starting OHLCV Service Daemon")

            # Load symbols from database
            async with DatabaseContext() as db:
                self._symbols = await db.symbols.get_all()

            if not self._symbols:
                self.logger.warning("No symbols found in database")
                return

            self.logger.info("Loaded symbols", symbol_count=len(self._symbols))

            # Initialize symbols in TimescaleDB
            success = await add_all_symbols(symbols=self._symbols)
            if not success:
                self.logger.warning("Some symbol initialization failed, but continuing")

            # Initialize collectors (they handle their own exchange capability checking)
            self.live_ohlcv_collector = LiveOHLCVCollector(symbols=self._symbols)
            self.historic_ohlcv_collector = HistoricOHLCVCollector(symbols=self._symbols)
            self.live_trade_collector = LiveTradeCollector(symbols=self._symbols)
            self.historic_trade_collector = HistoricTradeCollector(symbols=self._symbols)

            # Register daemon for health monitoring
            await self._register_daemon()

            # Historic phase
            self.logger.info("Starting historic collection phase")
            await asyncio.gather(
                self.historic_ohlcv_collector.start_collection(),
                self.historic_trade_collector.start_collection(),
            )

            # Cleanup historic collectors
            del self.historic_ohlcv_collector
            del self.historic_trade_collector
            self.historic_ohlcv_collector = None
            self.historic_trade_collector = None

            # Live phase
            self.logger.info("Starting live collection phase")
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
        """Start collection for a single specific symbol or add to running daemon.

        If daemon is running: Adds symbol to existing collectors dynamically
        If daemon is not running: Starts fresh daemon with just this symbol

        Args:
            symbol: Symbol model instance to collect data for

        Raises:
            ValueError: If symbol is invalid
        """
        # Validate symbol
        if not symbol or not hasattr(symbol, 'symbol') or not hasattr(symbol, 'cat_exchange'):
            raise ValueError("Invalid symbol parameter - must be a Symbol model instance")

        # Initialize symbol in TimescaleDB (common to all paths)
        success = await add_all_symbols(symbols=[symbol])
        if not success:
            self.logger.warning("Symbol initialization failed, but continuing")

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
            # Daemon not running - run historic first then set up live
            self.logger.info(
                "Starting daemon for single symbol",
                symbol=symbol.symbol,
                exchange=symbol.cat_exchange.name,
            )

            self._symbols = [symbol]

            # Historic phase (blocks until complete)
            self.historic_ohlcv_collector = HistoricOHLCVCollector()
            self.historic_trade_collector = HistoricTradeCollector()

            await asyncio.gather(
                self.historic_ohlcv_collector.start_symbol(symbol),
                self.historic_trade_collector.start_symbol(symbol),
            )

            # Cleanup historic collectors
            del self.historic_ohlcv_collector
            del self.historic_trade_collector
            self.historic_ohlcv_collector = None
            self.historic_trade_collector = None

            # Create live collectors
            self.live_ohlcv_collector = LiveOHLCVCollector()
            self.live_trade_collector = LiveTradeCollector()

            # Register daemon for health monitoring
            await self._register_daemon()
            # Fall through to start live collection

        else:
            # Partially running - cannot proceed
            self.logger.warning("Daemon partially running - cannot proceed")
            return

        # Start live collection (common final step for both success paths)
        await self.live_ohlcv_collector.start_symbol(symbol)
        await self.live_trade_collector.start_symbol(symbol)

    async def cleanup(self) -> None:
        """Cleanup daemon resources gracefully"""
        self.logger.info("Starting daemon cleanup...")

        # Stop collectors
        for collector in [self.live_trade_collector]:
            if collector:
                try:
                    await collector.stop_collection()
                except Exception as e:
                    self.logger.warning("Error stopping collector", error=str(e))

        # Update ProcessCache status
        if self.process_id:
            try:
                async with ProcessCache() as cache:
                    await cache.update_process(
                        process_id=self.process_id,
                        status=ProcessStatus.STOPPED,
                        message="Daemon shutdown complete",
                    )
            except Exception as e:
                self.logger.warning("Could not update ProcessCache", error=str(e))

        self.logger.info("Daemon cleanup completed")
