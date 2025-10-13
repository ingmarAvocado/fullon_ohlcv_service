"""
Demonstrate process monitoring using ProcessCache.

Shows how to:
1. Query active processes
2. Check component status
3. Get system health
4. Monitor per-symbol collection status
"""

import asyncio
from fullon_cache import ProcessCache
from fullon_cache.process_cache import ProcessType


async def monitor_collection():
    """Monitor active collection processes."""
    async with ProcessCache() as cache:
        # Get all OHLCV processes
        ohlcv_processes = await cache.get_active_processes(
            process_type=ProcessType.OHLCV, since_minutes=5
        )

        print("Active OHLCV Processes:")
        print("=" * 50)
        for process in ohlcv_processes:
            print(f"Process: {process['component']}")
            print(f"  Status: {process['status']}")
            print(f"  Message: {process['message']}")
            print(f"  Last update: {process['timestamp']}")
            print()

        # Get system health
        health = await cache.get_system_health()
        print(f"System Health: {health}")


async def monitor_specific_symbol(exchange: str, symbol: str):
    """Monitor a specific symbol's collection process."""
    component = f"{exchange}:{symbol}"

    async with ProcessCache() as cache:
        status = await cache.get_component_status(component)
        if status:
            print(f"Status for {component}:")
            print(f"  Status: {status['status']}")
            print(f"  Message: {status['message']}")
            print(f"  Last update: {status['timestamp']}")
        else:
            print(f"No active process found for {component}")


async def main():
    """Main monitoring function."""
    print("Process Monitoring Demo")
    print("=" * 30)

    # Monitor all collection processes
    await monitor_collection()

    print("\n" + "=" * 50)

    # Monitor specific symbols (uncomment and modify as needed)
    # await monitor_specific_symbol("binance", "BTC/USDT")
    # await monitor_specific_symbol("kraken", "ETH/USD")


if __name__ == "__main__":
    asyncio.run(main())
