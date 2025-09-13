#!/usr/bin/env python3
"""Example demonstrating OhlcvCollector usage.

This script shows how to use the OhlcvCollector for both historical
data collection and real-time streaming.
"""

import asyncio
import signal
import sys
from fullon_ohlcv_service.ohlcv.collector import OhlcvCollector


class GracefulExit:
    """Handle graceful shutdown on SIGINT/SIGTERM."""
    
    def __init__(self):
        self.exit_now = False
        signal.signal(signal.SIGINT, self._exit_gracefully)
        signal.signal(signal.SIGTERM, self._exit_gracefully)
    
    def _exit_gracefully(self, signum, frame):
        print(f"\nReceived signal {signum}, shutting down gracefully...")
        self.exit_now = True


async def demo_historical_collection():
    """Demonstrate historical OHLCV data collection."""
    print("=== Historical OHLCV Collection Demo ===")
    
    # Create collector for Kraken BTC/USD
    collector = OhlcvCollector("kraken", "BTC/USD")
    
    try:
        print("Collecting historical OHLCV data...")
        success = await collector.collect_historical()
        
        if success:
            print("✅ Historical data collection completed successfully")
        else:
            print("❌ Historical data collection failed")
            
    except Exception as e:
        print(f"❌ Error during historical collection: {e}")


async def demo_streaming_collection():
    """Demonstrate real-time OHLCV streaming."""
    print("\n=== Real-time OHLCV Streaming Demo ===")
    
    # Create collector for Binance BTC/USDT
    collector = OhlcvCollector("binance", "BTC/USDT")
    exit_handler = GracefulExit()
    
    async def on_ohlcv_data(data):
        """Callback for processing incoming OHLCV data."""
        print(f"📊 New OHLCV data: {data['symbol']} @ {data['timestamp']}")
        print(f"   O: {data['open']} H: {data['high']} L: {data['low']} C: {data['close']} V: {data['volume']}")
    
    try:
        print("Starting real-time OHLCV streaming...")
        print("Press Ctrl+C to stop streaming")
        
        # Start streaming with callback
        await collector.start_streaming(callback=on_ohlcv_data)
        
        # Keep running until interrupted
        while not exit_handler.exit_now and collector.running:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        print("\nKeyboard interrupt received")
    except Exception as e:
        print(f"❌ Error during streaming: {e}")
    finally:
        if collector.running:
            print("Stopping streaming...")
            await collector.stop_streaming()
        print("✅ Streaming stopped")


async def demo_multiple_collectors():
    """Demonstrate running multiple collectors simultaneously."""
    print("\n=== Multiple Collectors Demo ===")
    
    # Create multiple collectors
    collectors = [
        OhlcvCollector("kraken", "BTC/USD"),
        OhlcvCollector("kraken", "ETH/USD"),
        OhlcvCollector("binance", "BTC/USDT"),
    ]
    
    try:
        print("Running historical collection for multiple symbols...")
        
        # Run historical collection for all collectors concurrently
        tasks = [collector.collect_historical() for collector in collectors]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Report results
        for i, (collector, result) in enumerate(zip(collectors, results)):
            if isinstance(result, Exception):
                print(f"❌ {collector.exchange} {collector.symbol}: {result}")
            elif result:
                print(f"✅ {collector.exchange} {collector.symbol}: Success")
            else:
                print(f"⚠️  {collector.exchange} {collector.symbol}: Failed")
                
    except Exception as e:
        print(f"❌ Error in multiple collectors demo: {e}")


async def main():
    """Main demo function."""
    print("🚀 OHLCV Collector Demo")
    print("=" * 50)
    
    try:
        # Demo 1: Historical collection
        await demo_historical_collection()
        
        # Demo 2: Multiple collectors
        await demo_multiple_collectors()
        
        # Demo 3: Real-time streaming (commented out to avoid long running)
        # Uncomment the line below to test real-time streaming
        # await demo_streaming_collection()
        
        print("\n🎉 All demos completed!")
        
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())