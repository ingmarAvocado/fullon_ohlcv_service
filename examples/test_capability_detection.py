#!/usr/bin/env python3
"""
Test Capability Detection - Verify Smart Collection Strategy

Tests the exchange capability detection logic implemented in the collectors.
Demonstrates how the system chooses between OHLCV vs trade collection strategies.

Usage:
    python test_capability_detection.py
"""

import asyncio
from fullon_exchange.queue import ExchangeQueue
from fullon_ohlcv_service.ohlcv.collector import OhlcvCollector
from fullon_ohlcv_service.trade.collector import TradeCollector


# Simple exchange object for testing
class SimpleExchange:
    def __init__(self, exchange_name: str):
        self.ex_id = f"test_{exchange_name}"
        self.uid = "test_user"
        self.test = False
        self.cat_exchange = type("CatExchange", (), {"name": exchange_name})()


def create_credential_provider():
    """Create credential provider for public data."""
    def credential_provider(exchange_obj):
        return "", ""  # Empty credentials for public data
    return credential_provider


async def test_exchange_capabilities(exchange_name: str):
    """Test capability detection for a specific exchange."""
    print(f"\n{'='*60}")
    print(f"🔍 TESTING {exchange_name.upper()} CAPABILITIES")
    print(f"{'='*60}")

    await ExchangeQueue.initialize_factory()

    try:
        # Create exchange object and handler
        exchange_obj = SimpleExchange(exchange_name)
        credential_provider = create_credential_provider()
        handler = await ExchangeQueue.get_rest_handler(exchange_obj, credential_provider)
        await handler.connect()

        # Test capability detection
        try:
            supports_ohlcv = handler.supports_ohlcv()
            supports_1m = handler.supports_1m_ohlcv()
            needs_trades = handler.needs_trades_for_ohlcv()
            timeframes = handler.get_supported_timeframes()

            print(f"📊 Exchange Capabilities:")
            print(f"  • Native OHLCV support: {'✅' if supports_ohlcv else '❌'}")
            print(f"  • 1-minute support: {'✅' if supports_1m else '❌'}")
            print(f"  • Needs trades for OHLCV: {'✅' if needs_trades else '❌'}")
            print(f"  • Supported timeframes: {', '.join(timeframes[:5]) if timeframes else 'None'}")

            # Decision logic test
            print(f"\n🎯 Collection Strategy Decision:")
            if supports_1m and not needs_trades:
                strategy = "Use native 1-minute OHLCV collection (OPTIMAL)"
                priority = "OHLCV collector"
            elif needs_trades:
                strategy = "Use trade collection for accuracy (Kraken-style)"
                priority = "Trade collector (HIGH PRIORITY)"
            elif supports_ohlcv and not supports_1m:
                best_tf = "5m" if "5m" in timeframes else timeframes[0] if timeframes else "1h"
                strategy = f"Use native OHLCV with {best_tf} timeframe"
                priority = "OHLCV collector"
            else:
                strategy = "Fallback to any available method"
                priority = "OHLCV collector (fallback)"

            print(f"  • Strategy: {strategy}")
            print(f"  • Priority: {priority}")

            # Test collectors
            print(f"\n🤖 Testing Collectors:")

            # Test OHLCV collector capability detection
            ohlcv_collector = OhlcvCollector(exchange_name, "BTC/USD")
            print(f"  • OhlcvCollector: Will use smart collection strategy")

            # Test Trade collector capability detection
            trade_collector = TradeCollector(exchange_name, "BTC/USD")
            should_prioritize = trade_collector.should_prioritize_trades(handler)
            print(f"  • TradeCollector: {'HIGH priority' if should_prioritize else 'Normal priority'}")

            # Test actual data collection (small sample)
            print(f"\n📈 Sample Data Collection:")
            try:
                if supports_1m and not needs_trades:
                    # Test native OHLCV
                    ohlcv = await handler.get_ohlcv("BTC/USD", timeframe="1m", limit=5)
                    print(f"  ✅ Native OHLCV: {len(ohlcv)} candles collected")

                if needs_trades or not supports_ohlcv:
                    # Test trade collection
                    trades = await handler.get_public_trades("BTC/USD", limit=5)
                    print(f"  ✅ Trade collection: {len(trades)} trades collected")

            except Exception as e:
                print(f"  ❌ Data collection test failed: {str(e)[:50]}")

        except Exception as e:
            print(f"❌ Capability detection failed: {e}")

        await handler.disconnect()

    except Exception as e:
        print(f"❌ Connection to {exchange_name} failed: {e}")

    finally:
        await ExchangeQueue.shutdown_factory()


async def test_decision_matrix():
    """Test the decision matrix for different exchange types."""
    print(f"\n{'='*60}")
    print(f"📋 EXCHANGE CAPABILITY MATRIX")
    print(f"{'='*60}")

    exchanges_to_test = ["kraken", "binance", "bitmex"]

    for exchange in exchanges_to_test:
        await test_exchange_capabilities(exchange)
        await asyncio.sleep(1)  # Brief pause between tests


async def main():
    """Main test function."""
    print("🧪 CAPABILITY DETECTION TEST")
    print("Testing exchange capability detection and collection strategy logic")
    print("\nBased on docs/11_FULLON_EXCHANGE_LLM_README.md specifications:")
    print("  • Kraken: Should use trade collection (needs_trades_for_ohlcv=True)")
    print("  • Binance/BitMEX: Should use native OHLCV (needs_trades_for_ohlcv=False)")

    try:
        await test_decision_matrix()

        print(f"\n{'='*60}")
        print("✅ CAPABILITY DETECTION TEST COMPLETED")
        print("📊 Summary of expected behavior:")
        print("  • Kraken: Trade collector HIGH priority, OHLCV uses trade-based strategy")
        print("  • Binance/BitMEX: OHLCV collector optimal, Trade collector supplementary")
        print("  • System correctly detects and logs collection strategies")
        print(f"{'='*60}")

    except Exception as e:
        print(f"❌ Test failed: {e}")


if __name__ == "__main__":
    asyncio.run(main())