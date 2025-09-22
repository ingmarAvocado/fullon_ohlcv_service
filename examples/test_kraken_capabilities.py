#!/usr/bin/env python3
"""
Test Kraken Capabilities - Deep Dive

Specifically tests Kraken's capability detection and compares with documentation claims.
The docs say Kraken needs_trades_for_ohlcv=True, but our test shows False.

Usage:
    python test_kraken_capabilities.py
"""

import asyncio
from fullon_exchange.queue import ExchangeQueue


class SimpleExchange:
    def __init__(self, exchange_name: str):
        self.ex_id = f"test_{exchange_name}"
        self.uid = "test_user"
        self.test = False
        self.cat_exchange = type("CatExchange", (), {"name": exchange_name})()


async def test_kraken_detailed():
    """Detailed test of Kraken's actual capabilities."""
    print("🔍 KRAKEN CAPABILITY DEEP DIVE")
    print("="*50)

    await ExchangeQueue.initialize_factory()

    try:
        exchange_obj = SimpleExchange("kraken")

        def credential_provider(exchange_obj):
            return "", ""  # Public data

        handler = await ExchangeQueue.get_rest_handler(exchange_obj, credential_provider)
        await handler.connect()

        # Test all capability methods
        print("📊 Raw Capability Detection:")
        try:
            supports_ohlcv = handler.supports_ohlcv()
            supports_1m = handler.supports_1m_ohlcv()
            needs_trades = handler.needs_trades_for_ohlcv()
            timeframes = handler.get_supported_timeframes()

            print(f"  handler.supports_ohlcv(): {supports_ohlcv}")
            print(f"  handler.supports_1m_ohlcv(): {supports_1m}")
            print(f"  handler.needs_trades_for_ohlcv(): {needs_trades}")
            print(f"  handler.get_supported_timeframes(): {timeframes}")

        except Exception as e:
            print(f"  ❌ Capability detection error: {e}")

        # Test actual data quality comparison
        print("\n⚖️  Data Quality Comparison:")

        # Test 1: Get OHLCV data
        try:
            ohlcv_1m = await handler.get_ohlcv("BTC/USD", timeframe="1m", limit=3)
            print(f"  Native 1m OHLCV: {len(ohlcv_1m)} candles")
            if ohlcv_1m:
                latest = ohlcv_1m[-1]
                print(f"    Latest: O:{latest[1]:.2f} H:{latest[2]:.2f} L:{latest[3]:.2f} C:{latest[4]:.2f} V:{latest[5]:.2f}")
        except Exception as e:
            print(f"  ❌ Native OHLCV failed: {e}")

        # Test 2: Get trades data
        try:
            trades = await handler.get_public_trades("BTC/USD", limit=10)
            print(f"  Public trades: {len(trades)} trades")
            if trades:
                latest = trades[0]
                print(f"    Latest trade: {latest.get('side', 'unknown')} {latest.get('amount', 0):.6f} @ ${latest.get('price', 0):.2f}")
        except Exception as e:
            print(f"  ❌ Trades failed: {e}")

        # Test 3: Check if there are differences in data precision/accuracy
        print("\n🎯 Documentation vs Reality:")
        print("  📚 Documentation says: Kraken needs_trades_for_ohlcv=True")
        print(f"  🔍 Actual detection: needs_trades_for_ohlcv={needs_trades}")

        if not needs_trades:
            print("  ⚠️  DISCREPANCY: fullon_exchange reports Kraken doesn't need trades")
            print("  💡 This suggests either:")
            print("     - fullon_exchange capability detection needs updating")
            print("     - Documentation assumptions need revision")
            print("     - Kraken's API has improved since documentation was written")

        await handler.disconnect()

    except Exception as e:
        print(f"❌ Test failed: {e}")

    finally:
        await ExchangeQueue.shutdown_factory()


async def main():
    """Main test function."""
    print("Testing Kraken capabilities to resolve documentation discrepancy...\n")

    await test_kraken_detailed()

    print("\n" + "="*50)
    print("📋 CONCLUSIONS:")
    print("1. Test actual Kraken data quality vs documentation claims")
    print("2. Update either fullon_exchange detection or documentation")
    print("3. Current implementation will use native OHLCV for Kraken")
    print("4. Monitor if this affects data accuracy in practice")


if __name__ == "__main__":
    asyncio.run(main())