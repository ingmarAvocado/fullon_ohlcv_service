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

from fullon_ohlcv_service.config.settings import OhlcvServiceConfig
from fullon_ohlcv_service.ohlcv.historic_collector import HistoricOHLCVCollector
from fullon_ohlcv_service.ohlcv.live_collector import LiveOHLCVCollector
from fullon_ohlcv_service.trade.historic_collector import HistoricTradeCollector
from fullon_ohlcv_service.trade.live_collector import LiveTradeCollector
from fullon_ohlcv_service.utils.add_symbols import add_all_symbols


class OhlcvServiceDaemon:
    """Simple daemon coordinator for OHLCV and Trade services"""

    def __init__(self) -> None:
        self.logger = get_component_logger("fullon.ohlcv.daemon")
        self.config = OhlcvServiceConfig.from_env()
        self.live_ohlcv_collector: LiveOHLCVCollector | None = None
        self.historic_ohlcv_collector: HistoricOHLCVCollector | None = None
        self.live_trade_collector: LiveTradeCollector | None = None
        self.historic_trade_collector: HistoricTradeCollector | None = None
        self.process_id: str | None = None

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

            # Load and initialize all symbols before starting collectors
            # This prevents conflicts when collectors run concurrently
            async with DatabaseContext() as db:
                all_symbols = await db.symbols.get_all()

            if all_symbols:
                self.logger.info(
                    f"Initializing {len(all_symbols)} symbols for OHLCV and trade operations"
                )

                # Initialize symbols for both OHLCV (candles) and trade (view) operations
                candles_success = await add_all_symbols(
                    symbols=all_symbols, main="candles", test=False
                )
                view_success = await add_all_symbols(symbols=all_symbols, main="view", test=False)

                if candles_success and view_success:
                    self.logger.info("All symbols initialized successfully")
                else:
                    self.logger.warning("Some symbol initialization failed, but continuing")
            else:
                self.logger.warning("No symbols found in database")

            # Initialize collectors
            self.live_ohlcv_collector = LiveOHLCVCollector()
            self.historic_ohlcv_collector = HistoricOHLCVCollector()
            self.live_trade_collector = LiveTradeCollector()
            self.historic_trade_collector = HistoricTradeCollector()

            # Register with ProcessCache for health monitoring
            await self._register_daemon()

            # Phase 1: Historical collection (concurrent)
            if self.config.enable_historical:
                self.logger.info("Starting Historic Collectors (concurrent)...")

                # Run both historic collectors concurrently
                ohlcv_results, trade_results = await asyncio.gather(
                    self.historic_ohlcv_collector.start_collection(),
                    self.historic_trade_collector.start_collection(),
                )

                ohlcv_count = len(ohlcv_results)
                trade_count = len(trade_results)
                self.logger.info(
                    "Historical collection completed",
                    ohlcv_symbols=ohlcv_count,
                    trade_symbols=trade_count,
                )

            # Phase 2: Live streaming (starts after historic completes)
            if self.config.enable_streaming:
                self.logger.info("Starting Live Collectors...")

                # Run both live collectors concurrently (blocks indefinitely)
                await asyncio.gather(
                    self.live_ohlcv_collector.start_collection(),
                    self.live_trade_collector.start_collection(),
                )

        except asyncio.CancelledError:
            self.logger.info("Daemon shutdown requested by parent")
            # Don't re-raise CancelledError here - let cleanup run in finally block
            pass
        except Exception as e:
            self.logger.error(f"Daemon error: {e}")
            raise
        finally:
            await self.cleanup()

    async def cleanup(self) -> None:
        """Cleanup daemon resources gracefully"""
        self.logger.info("Starting daemon cleanup...")

        # Stop live collectors first (they have active WebSocket connections)
        cleanup_tasks = []

        if self.live_trade_collector:
            cleanup_tasks.append(
                ("live_trade_collector", self.live_trade_collector.stop_collection())
            )

        if self.live_ohlcv_collector:
            cleanup_tasks.append(
                ("live_ohlcv_collector", self.live_ohlcv_collector.stop_collection())
            )

        # Run cleanup tasks concurrently but handle exceptions individually
        for name, task in cleanup_tasks:
            try:
                await task
                self.logger.debug(f"Successfully stopped {name}")
            except asyncio.CancelledError:
                self.logger.debug(f"{name} was cancelled during shutdown")
            except Exception as e:
                self.logger.warning(f"Error stopping {name}: {e}")

        # Update ProcessCache status before cleanup
        try:
            async with ProcessCache() as cache:
                if hasattr(self, "process_id") and self.process_id:
                    # Try the documented API first, fall back to alternatives
                    try:
                        await cache.update_process(
                            process_id=self.process_id,
                            status=ProcessStatus.STOPPED,
                            message="Daemon shutdown complete",
                        )
                    except (AttributeError, TypeError):
                        # Fallback: try alternative APIs if available
                        try:
                            await cache.stop_process(self.process_id, "Daemon shutdown")
                        except (AttributeError, TypeError):
                            self.logger.debug("ProcessCache stop API not available, skipping")
        except Exception as e:
            self.logger.warning(f"Could not update ProcessCache: {e}")

        # Clean up ExchangeQueue factory (this handles WebSocket cleanup)
        try:
            from fullon_exchange.queue import ExchangeQueue

            await ExchangeQueue.shutdown_factory()
            self.logger.debug("ExchangeQueue factory shut down")
        except Exception as e:
            self.logger.warning(f"Could not shutdown ExchangeQueue factory: {e}")

        self.logger.info("Daemon cleanup completed")
