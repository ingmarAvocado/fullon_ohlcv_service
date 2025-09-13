#!/usr/bin/env python3
"""TradeCollector streaming demonstration.

This example shows how to use TradeCollector to stream real-time trades
with proper lifecycle management (start/stop).

Note: This script expects configured credentials/handlers for the target
exchange via fullon_exchange. For CI/demo environments without credentials,
set DRY_RUN=1 to skip live calls while demonstrating structure.
"""

import asyncio
import os
import signal

from fullon_log import get_component_logger
from fullon_ohlcv_service.trade.collector import TradeCollector


logger = get_component_logger("examples.trade_streaming_demo")


async def main() -> int:
    exchange = os.getenv("EXCHANGE", "kraken")
    symbol = os.getenv("SYMBOL", "BTC/USD")
    dry_run = os.getenv("DRY_RUN", "0") == "1"

    logger.info("Starting Trade streaming demo", exchange=exchange, symbol=symbol, dry_run=dry_run)

    if dry_run:
        logger.info("DRY_RUN enabled: skipping live streaming")
        return 0

    collector = TradeCollector(exchange=exchange, symbol=symbol)

    # Custom callback to demonstrate receiving trade events
    async def on_trade_received(trade_data):
        logger.info(
            "Trade received via callback",
            price=trade_data.get("price"),
            volume=trade_data.get("volume"),
            timestamp=trade_data.get("timestamp"),
        )

    # Set up signal handler for graceful shutdown
    stop_event = asyncio.Event()

    def signal_handler(sig, frame):
        logger.info("Received signal, stopping...")
        stop_event.set()

    signal.signal(signal.SIGINT, signal_handler)

    try:
        # Start streaming with callback
        logger.info("Starting trade streaming...")
        streaming_task = asyncio.create_task(
            collector.start_streaming(callback=on_trade_received)
        )

        # Wait for stop signal or timeout
        await asyncio.wait_for(stop_event.wait(), timeout=30)

    except asyncio.TimeoutError:
        logger.info("Timeout reached, stopping...")

    finally:
        # Stop streaming gracefully
        logger.info("Stopping trade collector...")
        await collector.stop_streaming()

        # Cancel the streaming task if it's still running
        if not streaming_task.done():
            streaming_task.cancel()
            try:
                await streaming_task
            except asyncio.CancelledError:
                pass

        logger.info("Trade streaming demo completed")

    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))