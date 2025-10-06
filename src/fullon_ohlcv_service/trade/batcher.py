"""
Global Trade Batcher

Singleton that coordinates minute-aligned batch processing for all active symbols.
Collects trades from Redis and saves them to PostgreSQL in coordinated batches.
"""

import asyncio
import time
import traceback
from typing import Dict, Set, List, Any
from datetime import datetime, timezone

from fullon_log import get_component_logger
from fullon_cache import TradesCache
from fullon_ohlcv.models import Trade
from fullon_ohlcv.repositories.ohlcv import TradeRepository
from fullon_orm.models import Trade as ORMTrade


class GlobalTradeBatcher:
    """
    Singleton batcher that processes trades from Redis to PostgreSQL.

    Implements minute-aligned batch processing (59.9s intervals)
    for efficient database writes across all active symbols.
    """

    _instance = None
    _lock = asyncio.Lock()

    def __new__(cls):
        """Implement singleton pattern."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        """Initialize batcher if not already initialized."""
        if self._initialized:
            return

        self.logger = get_component_logger("fullon.trade.batcher")
        self.active_symbols: Set[str] = set()
        self.running = False
        self._batch_task = None
        self._batch_interval = 59.9  # Process just before minute boundary

        self._initialized = True
        self.logger.info("GlobalTradeBatcher initialized")

    async def register_symbol(self, exchange: str, symbol: str) -> None:
        """
        Register a symbol for batch processing.

        Args:
            exchange: Exchange name
            symbol: Symbol name
        """
        symbol_key = f"{exchange}:{symbol}"
        async with self._lock:
            self.active_symbols.add(symbol_key)
            self.logger.debug(
                "Symbol registered for batch processing",
                exchange=exchange,
                symbol=symbol,
                total_symbols=len(self.active_symbols)
            )

        # Start batch processing if not already running
        if not self.running:
            await self.start_batch_processing()

    async def unregister_symbol(self, exchange: str, symbol: str) -> None:
        """
        Unregister a symbol from batch processing.

        Args:
            exchange: Exchange name
            symbol: Symbol name
        """
        symbol_key = f"{exchange}:{symbol}"
        async with self._lock:
            self.active_symbols.discard(symbol_key)
            self.logger.info(
                "Symbol unregistered from batch processing",
                exchange=exchange,
                symbol=symbol,
                remaining_symbols=len(self.active_symbols)
            )

        # Stop batch processing if no symbols remain
        if not self.active_symbols and self.running:
            await self.stop_batch_processing()

    async def start_batch_processing(self) -> None:
        """Start the batch processing loop."""
        if self.running:
            return

        self.running = True
        self._batch_task = asyncio.create_task(self._batch_processing_loop())
        self.logger.info("Batch processing started")

    async def stop_batch_processing(self) -> None:
        """Stop the batch processing loop."""
        self.running = False

        if self._batch_task and not self._batch_task.done():
            self._batch_task.cancel()
            try:
                await self._batch_task
            except asyncio.CancelledError:
                pass

        self.logger.info("Batch processing stopped")

    async def _batch_processing_loop(self) -> None:
        """Main batch processing loop with minute-aligned timing."""
        while self.running:
            try:
                # Calculate time until next batch
                next_batch_time = self._calculate_next_batch_time()
                current_time = time.time()
                wait_time = next_batch_time - current_time

                # Debug logging to understand the issue
                if wait_time <= 0:
                    self.logger.debug(
                        "Batch time calculation issue",
                        current_time=current_time,
                        next_batch_time=next_batch_time,
                        wait_time=wait_time
                    )
                    # Force minimum wait to prevent tight loop
                    wait_time = self._batch_interval  # Use full interval (59.9s)
                    next_batch_time = current_time + wait_time

                # Log countdown to next batch
                if wait_time > 1:
                    wait_display = f"{wait_time:.1f}s"
                else:
                    wait_display = f"{wait_time*1000:.0f}ms"

                next_batch_datetime = datetime.fromtimestamp(next_batch_time, tz=timezone.utc)

                self.logger.debug(
                    "Waiting for next batch cycle",
                    wait_time=wait_display,
                    next_batch_at=next_batch_datetime.strftime('%H:%M:%S.%f')[:-3] + ' UTC',
                    active_symbols=len(self.active_symbols)
                )
                await asyncio.sleep(wait_time)

                # Process all active symbols
                await self._process_batch()

            except Exception as e:
                self.logger.error(
                    "Batch processing error",
                    error=str(e)
                )
                await asyncio.sleep(5)  # Brief pause before retry

    def _calculate_next_batch_time(self) -> float:
        """
        Calculate the next minute-aligned batch time.

        Returns:
            Unix timestamp for next batch processing time
        """
        current_time = time.time()
        current_minute = int(current_time / 60)
        next_minute = current_minute + 1
        # Process at 59.9 seconds into the minute
        next_batch_time = (next_minute * 60) - 0.1
        return next_batch_time

    async def _process_batch(self) -> None:
        """Process trades for all active symbols."""
        if not self.active_symbols:
            self.logger.debug("No active symbols to process")
            return

        batch_start = time.time()
        symbols_processed = 0
        total_trades = 0
        symbol_results = []

        # Get snapshot of symbols to process
        async with self._lock:
            symbols_to_process = list(self.active_symbols)

        self.logger.debug(
            "Starting batch processing cycle",
            symbols_count=len(symbols_to_process),
            symbols=symbols_to_process
        )

        # Process each symbol
        for symbol_key in symbols_to_process:
            try:
                exchange, symbol = symbol_key.split(":", 1)
                trades_count = await self._process_single_symbol(exchange, symbol)

                # Track results for detailed logging
                symbol_results.append({
                    'symbol_key': symbol_key,
                    'exchange': exchange,
                    'symbol': symbol,
                    'trades_count': trades_count
                })

                if trades_count > 0:
                    symbols_processed += 1
                    total_trades += trades_count

            except Exception as e:
                self.logger.error(
                    "Error processing symbol",
                    symbol=symbol_key,
                    error=str(e),
                    error_type=type(e).__name__
                )
                symbol_results.append({
                    'symbol_key': symbol_key,
                    'trades_count': 0,
                    'error': str(e)
                })

        batch_duration = time.time() - batch_start

        # Log detailed results for each symbol
        for result in symbol_results:
            if result['trades_count'] > 0:
                self.logger.info(
                    f"✓ {result['symbol_key']} - {result['trades_count']} trades processed from Redis → PostgreSQL"
                )
            elif 'error' in result:
                self.logger.error(
                    f"✗ {result['symbol_key']} - processing failed: {result['error']}"
                )
            else:
                # Debug level since no trades is a normal occurrence
                self.logger.debug(
                    f"○ {result['symbol_key']} - no trades found in Redis buffer"
                )

        # Summary log
        if symbols_processed > 0:
            self.logger.info(
                "📊 Batch cycle completed",
                symbols_with_trades=symbols_processed,
                total_symbols=len(symbols_to_process),
                total_trades=total_trades,
                duration=f"{batch_duration:.2f}s"
            )
        else:
            self.logger.info(
                "📊 Batch cycle completed - no trades to process",
                total_symbols=len(symbols_to_process),
                duration=f"{batch_duration:.2f}s"
            )

    async def _process_single_symbol(self, exchange: str, symbol: str) -> int:
        """
        Process trades for a single symbol.

        Args:
            exchange: Exchange name
            symbol: Symbol name

        Returns:
            Number of trades processed
        """
        try:
            # Collect trades from Redis
            async with TradesCache() as cache:
                trades_data = await cache.get_trades(symbol, exchange)

            if not trades_data:
                return 0

            # Convert to Trade objects
            trades = self._convert_to_trade_objects(trades_data)

            # Save to PostgreSQL
            async with TradeRepository(exchange, symbol, test=False) as repo:
                success = await repo.save_trades(trades)

                if success:
                    self.logger.debug(f"{symbol}:{exchange} - {len(trades)} trades saved")
                    return len(trades)
                else:
                    self.logger.warning(
                        "Failed to save trades",
                        exchange=exchange,
                        symbol=symbol,
                        count=len(trades)
                    )
                    return 0

        except Exception as e:
            self.logger.error(
                "Error processing symbol trades",
                exchange=exchange,
                symbol=symbol,
                error=str(e),
                error_type=type(e).__name__
            )
            self.logger.debug("Full traceback", traceback=traceback.format_exc())
            return 0

    def _convert_to_trade_objects(self, trades_data: List[Any]) -> List[Trade]:
        """
        Convert raw trade data to Trade objects.

        Args:
            trades_data: List of trade items (dictionaries or ORM Trade objects) from Redis

        Returns:
            List of Trade objects
        """
        trade_objects = []

        for trade_item in trades_data:
            try:
                if isinstance(trade_item, ORMTrade):
                    # Handle fullon_orm Trade objects
                    timestamp = trade_item.time
                    if isinstance(timestamp, (int, float)):
                        # Convert timestamp to datetime if it's numeric
                        timestamp = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                    trade = Trade(
                        timestamp=timestamp,
                        price=float(trade_item.price),
                        volume=float(trade_item.volume),
                        side=str(trade_item.side),
                        type='market'
                    )
                else:
                    # Handle dictionary format (legacy/fallback)
                    timestamp = trade_item.get('timestamp', 0)
                    if timestamp > 1e12:  # milliseconds
                        timestamp_dt = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
                    else:  # seconds
                        timestamp_dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)

                    trade = Trade(
                        timestamp=timestamp_dt,
                        price=float(trade_item.get('price', 0)),
                        volume=float(trade_item.get('amount', trade_item.get('volume', 0))),
                        side=str(trade_item.get('side', 'unknown')),
                        type='market'
                    )
                trade_objects.append(trade)

            except Exception as e:
                self.logger.warning(
                    "Failed to convert trade",
                    error=str(e),
                    trade=str(trade_item)[:100]
                )
                continue

        return trade_objects