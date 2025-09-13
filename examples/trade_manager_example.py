#!/usr/bin/env python3
"""Example: TradeManager with Database-Driven Configuration

Demonstrates how TradeManager reads exchange/symbol configuration from fullon_orm
and starts trade collectors automatically. For demo environments, set DRY_RUN=1
to avoid opening live connections.
"""

import asyncio
import os

from fullon_log import get_component_logger
from fullon_ohlcv_service.trade.manager import TradeManager


logger = get_component_logger("examples.trade_manager_example")


async def main():
    dry_run = os.getenv("DRY_RUN", "0") == "1"
    manager = TradeManager()

    logger.info("Starting TradeManager example", dry_run=dry_run)

    if dry_run:
        logger.info("DRY_RUN=1; skipping live start_from_database()")
        return 0

    try:
        # Start the manager
        await manager.start()

        # Start collectors from database configuration
        await manager.start_from_database()
        status = await manager.get_status()
        logger.info("TradeManager status", status=status)

        # Run briefly to demonstrate active streaming
        await asyncio.sleep(10)

        # Check status again to show tasks are running
        status = await manager.get_status()
        logger.info("TradeManager status after running", status=status)

    finally:
        # Properly shutdown all collectors
        logger.info("Shutting down TradeManager")
        await manager.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

