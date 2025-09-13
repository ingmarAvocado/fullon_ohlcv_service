#!/usr/bin/env python3
"""TradeCollector demonstration.

This example shows how to use TradeCollector to fetch a small batch of recent
trades and save them using fullon_ohlcv's TradeRepository.

Note: This script expects configured credentials/handlers for the target
exchange via fullon_exchange. For CI/demo environments without credentials,
set DRY_RUN=1 to skip live calls while demonstrating structure.
"""

import asyncio
import os

from fullon_log import get_component_logger
from fullon_ohlcv_service.trade.collector import TradeCollector


logger = get_component_logger("examples.trade_collector_demo")


async def main() -> int:
    exchange = os.getenv("EXCHANGE", "kraken")
    symbol = os.getenv("SYMBOL", "BTC/USD")
    dry_run = os.getenv("DRY_RUN", "0") == "1"

    logger.info("Starting TradeCollector demo", exchange=exchange, symbol=symbol, dry_run=dry_run)

    if dry_run:
        # Demonstrate the flow without hitting the network
        logger.info("DRY_RUN enabled: skipping live calls")
        return 0

    collector = TradeCollector(exchange=exchange, symbol=symbol)
    success = await collector.collect_historical_trades()
    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

