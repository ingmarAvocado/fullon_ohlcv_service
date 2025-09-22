#!/usr/bin/env python3
"""
Modern REST API Example - Post Issue #321/#322 Architecture

This example demonstrates the unified REST API across different exchanges
using the modern RESTInterface pattern with fullon_orm models.

Key features:
- Multi-exchange support with symbol mapping
- Modern RESTInterface.create_handler() pattern
- ExchangeCredentials/ExchangeConfig usage
- Comprehensive API testing with clear status reporting
- OHLCV/Trade collection via REST
- Clean output for human and LLM readability

Usage:
    python rest_example.py kraken
    python rest_example.py hyperliquid
    python rest_example.py bitmex
"""

import asyncio
import os
import sys
from typing import Any, Dict, List

from dotenv import load_dotenv

from fullon_exchange.queue.exchange_queue import ExchangeQueue



# Simple exchange object to work with queue system
class SimpleExchange:
    """Simple exchange object for queue system compatibility."""

    def __init__(self, exchange_name: str):
        self.ex_id = f"example_{exchange_name}"
        self.uid = "example_user"
        self.test = False
        # Create cat_exchange with name attribute
        self.cat_exchange = type("CatExchange", (), {"name": exchange_name})()


def create_credential_provider(api_key: str, secret: str):
    """Create a credential provider function for the queue system."""

    def credential_provider(exchange_obj):  # noqa: ARG001
        return api_key, secret

    return credential_provider


# Symbol mapping for cross-exchange compatibility
SYMBOL_MAPPING = {
    "BTC/USD": {
        "kraken": "BTC/USD",  # Kraken supports BTC/USD directly
        "bitmex": "XBTUSD",  # BitMEX perpetual format
        "binance": "BTC/USDT",  # Binance uses USDT
        "hyperliquid": "BTC/USDC:USDC",  # Hyperliquid futures format
        "default": "BTC/USD",
    },
    "ETH/USD": {
        "kraken": "ETH/USD",
        "bitmex": "ETHUSD",
        "binance": "ETH/USDT",
        "hyperliquid": "ETH/USDC:USDC",
        "default": "ETH/USD",
    },
}


def get_exchange_symbol(base_symbol: str, exchange_name: str) -> str:
    """Get exchange-specific symbol format."""
    if base_symbol in SYMBOL_MAPPING:
        return SYMBOL_MAPPING[base_symbol].get(
            exchange_name.lower(), SYMBOL_MAPPING[base_symbol]["default"]
        )
    return base_symbol


def get_credentials(exchange_name: str) -> tuple[str, str]:
    """Load credentials from environment variables."""
    api_key = os.getenv(f"{exchange_name.upper()}_API_KEY", "")
    secret = os.getenv(f"{exchange_name.upper()}_SECRET", "")
    return api_key, secret


async def test_ohlcv_data(handler, symbol: str) -> Dict[str, Any]:
    """Test OHLCV/candlestick data operations."""
    results = {}

    # Test different timeframes
    timeframes = [("1m", "1m_candles"), ("1h", "1h_candles"), ("1d", "1d_candles")]

    for timeframe, result_key in timeframes:
        try:
            ohlcv = await handler.get_ohlcv(symbol, timeframe=timeframe, limit=100)
            if ohlcv:
                latest = ohlcv[-1] if ohlcv else None
                if latest:
                    # Format: [timestamp, open, high, low, close, volume]
                    close_price = latest[4]
                    results[result_key] = (
                        f"✅ {len(ohlcv)} candles (latest: ${close_price:,.2f} OHLC)"
                    )
                else:
                    results[result_key] = f"✅ {len(ohlcv)} candles"
            else:
                results[result_key] = "✅ No data available"
        except Exception as e:
            results[result_key] = f"❌ {str(e)[:50]}..."

    return results


async def test_bulk_operations(
    handler, symbols: List[str] | None = None
) -> Dict[str, Any]:
    """Test bulk operations like multiple tickers and available symbols."""
    results = {}

    # Test bulk tickers
    try:
        tickers = await handler.get_tickers(symbols)
        if tickers:
            # Show count and sample prices
            sample_tickers = list(tickers.items())[:3]
            prices_str = ", ".join(
                [
                    f"{symbol}: ${ticker.get('last', 0):,.2f}"
                    for symbol, ticker in sample_tickers
                    if ticker.get("last")
                ]
            )
            if prices_str:
                results["bulk_tickers"] = f"✅ {len(tickers)} tickers ({prices_str})"
            else:
                results["bulk_tickers"] = f"✅ {len(tickers)} tickers retrieved"
        else:
            results["bulk_tickers"] = "✅ No tickers available"
    except Exception as e:
        results["bulk_tickers"] = f"❌ {str(e)[:50]}..."

    # Test available symbols
    try:
        available_symbols = await handler.get_available_symbols()
        if available_symbols:
            # Show count and sample symbols
            sample_symbols = available_symbols[:5]
            symbols_str = ", ".join(sample_symbols)
            results["available_symbols"] = (
                f"✅ {len(available_symbols)} symbols ({symbols_str}...)"
            )
        else:
            results["available_symbols"] = "✅ No symbols available"
    except Exception as e:
        results["available_symbols"] = f"❌ {str(e)[:50]}..."

    return results


async def test_market_data(handler, symbol: str, exchange_name: str) -> Dict[str, Any]:  # noqa: ARG001
    """Test market data operations."""
    results = {}

    # Test ticker
    try:
        ticker = await handler.get_ticker(symbol)
        price = ticker.get("last") or ticker.get("close", 0)
        results["ticker"] = f"✅ ${price:,.2f}" if price else "✅ Retrieved"
    except Exception as e:
        results["ticker"] = f"❌ {str(e)[:50]}..."

    # Test order book
    try:
        orderbook = await handler.get_order_book(symbol, limit=5)
        bids = orderbook.get("bids", [])
        asks = orderbook.get("asks", [])
        if bids and asks:
            best_bid = bids[0][0] if len(bids[0]) > 0 else 0
            best_ask = asks[0][0] if len(asks[0]) > 0 else 0
            spread = ((best_ask - best_bid) / best_ask * 100) if best_ask > 0 else 0
            results["orderbook"] = (
                f"✅ ${best_bid:,.2f}/${best_ask:,.2f} ({spread:.3f}% spread)"
            )
        else:
            results["orderbook"] = f"✅ {len(bids)} bids, {len(asks)} asks"
    except Exception as e:
        results["orderbook"] = f"❌ {str(e)[:50]}..."

    # Test recent public trades (general market trades)
    try:
        public_trades = await handler.get_public_trades(symbol, limit=10)
        if public_trades:
            latest_trade = public_trades[0] if public_trades else None
            if latest_trade:
                price = latest_trade.get("price", 0)
                amount = latest_trade.get("amount", 0)
                side = latest_trade.get("side", "unknown")
                results["public_trades"] = (
                    f"✅ {len(public_trades)} recent trades (last: {side} {amount:.4f} @ ${price:.2f})"
                )
            else:
                results["public_trades"] = f"✅ {len(public_trades)} recent trades"
        else:
            results["public_trades"] = "✅ No recent public trades"
    except Exception as e:
        results["public_trades"] = f"❌ {str(e)[:50]}..."

    return results


async def test_account_data(handler) -> Dict[str, Any]:
    """Test account data operations (requires credentials)."""
    results = {}

    # Test balance - show actual balances
    try:
        balance = await handler.get_balance()
        non_zero = {k: v for k, v in balance.items() if v.total > 0}
        if non_zero:
            # Show top 3 balances
            sorted_balances = sorted(
                non_zero.items(), key=lambda x: x[1].total, reverse=True
            )[:3]
            balance_str = ", ".join([f"{k}: {v.total:.6f}" for k, v in sorted_balances])
            results["balance"] = f"✅ {balance_str}"
        else:
            results["balance"] = "✅ No non-zero balances"
    except Exception as e:
        results["balance"] = f"❌ {str(e)[:50]}..."

    # Test my trade history (private trades)
    try:
        from fullon_exchange.core.orm_utils import TradeHistoryRequest

        trade_request = TradeHistoryRequest(limit=5)
        trades = await handler.get_trades(trade_request)
        if trades:
            latest_trade = trades[0] if trades else None
            if (
                latest_trade
                and hasattr(latest_trade, "symbol")
                and hasattr(latest_trade, "side")
            ):
                results["my_trades"] = (
                    f"✅ {len(trades)} trades (latest: {latest_trade.side} {latest_trade.symbol})"
                )
            else:
                results["my_trades"] = f"✅ {len(trades)} my trades"
        else:
            results["my_trades"] = "✅ No recent trades"
    except Exception as e:
        results["my_trades"] = f"❌ {str(e)[:50]}..."

    # Test open orders
    try:
        orders = await handler.get_open_orders()
        if orders:
            # Show summary of order types
            buy_orders = sum(
                1
                for order in orders
                if hasattr(order, "side")
                and (
                    str(order.side).lower() == "buy"
                    or (
                        hasattr(order.side, "value")
                        and order.side.value.lower() == "buy"
                    )
                )
            )
            sell_orders = len(orders) - buy_orders
            results["orders"] = (
                f"✅ {len(orders)} orders ({buy_orders} buy, {sell_orders} sell)"
            )
        else:
            results["orders"] = "✅ No open orders"
    except Exception as e:
        results["orders"] = f"❌ {str(e)[:50]}..."

    return results


async def test_exchange_features(handler) -> Dict[str, Any]:
    """Test exchange-specific features."""
    results = {}

    # Test positions support with details
    try:
        supports_positions = handler.supports_positions()
        if supports_positions:
            positions = await handler.get_positions()
            if positions:
                # Show position details
                pos_summary = []
                for pos in positions[:3]:  # Show top 3 positions
                    try:
                        # Try different attribute names that different exchanges might use
                        symbol = getattr(pos, "symbol", None) or getattr(
                            pos, "info", {}
                        ).get("symbol", "UNKNOWN")
                        side = getattr(pos, "side", None) or getattr(
                            pos, "info", {}
                        ).get("side", "UNKNOWN")
                        size = (
                            getattr(pos, "size", None)
                            or getattr(pos, "contracts", None)
                            or getattr(pos, "info", {}).get("size", 0)
                        )

                        # Handle different size formats
                        if size is not None:
                            size_val = float(size) if size != 0 else 0
                        else:
                            size_val = 0

                        # PnL information
                        pnl = (
                            getattr(pos, "unrealized_pnl", None)
                            or getattr(pos, "unrealizedPnl", None)
                            or getattr(pos, "info", {}).get("unrealizedPnl", None)
                        )

                        # Always show position size, whether active or flat
                        pnl_str = (
                            f" (PnL: ${float(pnl):.2f})"
                            if pnl is not None and float(pnl) != 0
                            else ""
                        )

                        if size_val != 0:
                            pos_summary.append(
                                f"{side} {size_val:.4f} {symbol}{pnl_str}"
                            )
                        else:
                            pos_summary.append(
                                f"{side} 0.0000 {symbol} (flat){pnl_str}"
                            )

                    except Exception as e:
                        # Fallback to show raw position info
                        pos_summary.append(f"Position: {type(pos).__name__}")

                if pos_summary:
                    results["positions"] = (
                        f"✅ {len(positions)} positions: " + "; ".join(pos_summary)
                    )
                else:
                    results["positions"] = f"✅ {len(positions)} positions (all flat)"
            else:
                results["positions"] = "✅ No open positions"
        else:
            results["positions"] = "✅ Not supported (spot exchange)"
    except Exception as e:
        results["positions"] = f"❌ {str(e)[:50]}..."

    # Test trading fees
    try:
        fees = handler.get_trading_fees()
        maker = (fees.get("maker") or 0) * 100
        taker = (fees.get("taker") or 0) * 100
        results["fees"] = f"✅ {maker:.2f}% maker, {taker:.2f}% taker"
    except Exception as e:
        results["fees"] = f"❌ {str(e)[:50]}..."

    return results


async def test_ohlcv_collection(handler, symbol: str, exchange_name: str) -> Dict[str, Any]:
    """Test OHLCV collection via REST API."""
    results = {}

    # Collection timeframes to test
    timeframes = [("1m", 50), ("1h", 24), ("1d", 7)]

    for timeframe, limit in timeframes:
        try:
            # Collect OHLCV data via REST API (through ExchangeQueue)
            ohlcv_data = await handler.get_ohlcv(symbol, timeframe=timeframe, limit=limit)

            if ohlcv_data:
                # Display first and last candle for verification
                first_candle = ohlcv_data[0] if ohlcv_data else None
                last_candle = ohlcv_data[-1] if ohlcv_data else None

                if first_candle and len(first_candle) >= 6:
                    results[f"ohlcv_{timeframe}_collection"] = f"✅ {len(ohlcv_data)} {timeframe} candles (O:{first_candle[1]:.2f} H:{first_candle[2]:.2f} L:{first_candle[3]:.2f} C:{first_candle[4]:.2f})"
                else:
                    results[f"ohlcv_{timeframe}_collection"] = f"✅ {len(ohlcv_data)} {timeframe} records retrieved"
            else:
                results[f"ohlcv_{timeframe}_collection"] = f"✅ No {timeframe} OHLCV data available"

        except Exception as e:
            results[f"ohlcv_{timeframe}_collection"] = f"❌ {str(e)[:50]}..."

    return results


async def test_trade_collection(handler, symbol: str, exchange_name: str) -> Dict[str, Any]:
    """Test trade collection via REST API."""
    results = {}

    try:
        # Collect recent trades via REST API (through ExchangeQueue)
        public_trades = await handler.get_public_trades(symbol, limit=100)

        if public_trades:
            # Display sample trade data for verification
            recent_trades = public_trades[:3]  # First 3 trades

            trade_summary = []
            for trade_data in recent_trades:
                price = trade_data.get('price', 0)
                amount = trade_data.get('amount', 0)
                side = trade_data.get('side', 'unknown')
                trade_summary.append(f"{side} {amount:.4f}@{price:.2f}")

            results["trade_collection"] = f"✅ {len(public_trades)} trades collected [{', '.join(trade_summary)}]"
        else:
            results["trade_collection"] = "✅ No recent trades available"

    except Exception as e:
        results["trade_collection"] = f"❌ {str(e)[:50]}..."

    return results


async def main():
    """Main function to test REST API."""
    # Get exchange from command line
    exchange_name = sys.argv[1].lower() if len(sys.argv) > 1 else "kraken"

    print(f"=== {exchange_name.upper()} REST API Test ===")

    # Load environment variables
    load_dotenv()

    # Get credentials
    api_key, secret = get_credentials(exchange_name)
    has_credentials = bool(api_key and secret and api_key != "your_api_key")

    # Get test symbol
    base_symbol = "BTC/USD"
    symbol = get_exchange_symbol(base_symbol, exchange_name)
    print(f"Testing symbol: {base_symbol} → {symbol}")

    if has_credentials:
        print("🔑 Credentials found - testing private endpoints")
    else:
        print("⚠️  No credentials - testing public endpoints only")

    try:
        # Initialize ExchangeQueue factory
        await ExchangeQueue.initialize_factory()

        # Create exchange object for queue system
        exchange_obj = SimpleExchange(exchange_name)

        # Create credential provider
        credential_provider = create_credential_provider(api_key, secret)

        # Create REST handler through queue system
        handler = await ExchangeQueue.get_rest_handler(
            exchange_obj, credential_provider
        )

        # Connect to exchange
        await handler.connect()
        print(f"✅ Connected to {exchange_name}")

        # Test OHLCV capabilities (new feature)
        print("\n🔍 OHLCV Capabilities:")
        try:
            supports_ohlcv = handler.supports_ohlcv()
            supports_1m = handler.supports_1m_ohlcv()
            needs_trades = handler.needs_trades_for_ohlcv()
            timeframes = handler.get_supported_timeframes()

            print(f"  • Native OHLCV support: {'✅' if supports_ohlcv else '❌'}")
            print(f"  • 1-minute support: {'✅' if supports_1m else '❌'}")
            print(f"  • Needs trades for OHLCV: {'✅' if needs_trades else '❌'}")
            if timeframes:
                print(f"  • Supported timeframes: {', '.join(timeframes[:5])}{'...' if len(timeframes) > 5 else ''}")
            else:
                print("  • Supported timeframes: None")

            # Decision logic demonstration
            if supports_1m and not needs_trades:
                print("  • Recommendation: Use native OHLCV collection for 1-minute data")
            elif supports_ohlcv and not supports_1m:
                print("  • Recommendation: Use native OHLCV for higher timeframes, trades for 1-minute")
            elif needs_trades:
                print("  • Recommendation: Use trade collection and construct OHLCV bars")
            else:
                print("  • Recommendation: Use trade collection for all OHLCV data")

        except Exception as e:
            print(f"  • Capability detection failed: {e}")

        # Test OHLCV data (new feature)
        print("\n📊 OHLCV Data:")
        ohlcv_results = await test_ohlcv_data(handler, symbol)
        for test, result in ohlcv_results.items():
            timeframe_label = test.replace("_", " ").title()
            print(f"  • {timeframe_label}: {result}")

        # Test bulk operations (new feature)
        print("\n🔍 Bulk Operations:")
        bulk_results = await test_bulk_operations(handler)
        for test, result in bulk_results.items():
            test_label = test.replace("_", " ").title()
            print(f"  • {test_label}: {result}")

        # Test market data (always available)
        print("\n📈 Market Data:")
        market_results = await test_market_data(handler, symbol, exchange_name)
        for test, result in market_results.items():
            print(f"  • {test.title()}: {result}")

        # Add market count
        try:
            markets = handler.get_markets()
            print(f"  • Markets: ✅ {len(markets)} available")
        except Exception as e:
            print(f"  • Markets: ❌ {str(e)[:50]}...")

        # Test account data (requires credentials)
        if has_credentials:
            print("\n💰 Account Data:")
            account_results = await test_account_data(handler)
            for test, result in account_results.items():
                print(f"  • {test.title()}: {result}")
        else:
            print("\n💰 Account Data: Skipped (no credentials)")
            account_results = {}

        # Test exchange features
        print("\n🔧 Exchange Features:")
        feature_results = await test_exchange_features(handler)
        for test, result in feature_results.items():
            print(f"  • {test.title()}: {result}")

        # Test OHLCV/Trade collection via REST
        print("\n📊 OHLCV/Trade Collection via REST:")

        # Test OHLCV collection
        ohlcv_collection_results = await test_ohlcv_collection(handler, symbol, exchange_name)
        for test, result in ohlcv_collection_results.items():
            test_label = test.replace("_", " ").replace("ohlcv", "OHLCV").title()
            print(f"  • {test_label}: {result}")

        # Test trade collection
        trade_collection_results = await test_trade_collection(handler, symbol, exchange_name)
        for test, result in trade_collection_results.items():
            test_label = test.replace("_", " ").title()
            print(f"  • {test_label}: {result}")

        # Calculate success rate (include new test results)
        all_results = {
            **ohlcv_results,
            **bulk_results,
            **market_results,
            **account_results,
            **feature_results,
            **ohlcv_collection_results,
            **trade_collection_results,
        }
        success_count = sum(
            1 for result in all_results.values() if result.startswith("✅")
        )
        total_count = len(all_results)

        print(f"\n📈 Result: {success_count}/{total_count} APIs working")

        if success_count == total_count:
            print("🎉 All APIs working perfectly!")
        elif success_count >= total_count * 0.8:
            print("✅ Most APIs working - good integration")
        else:
            print("⚠️  Several APIs failing - check credentials/network")

        # Clean disconnect
        await handler.disconnect()
        print(f"✅ Disconnected from {exchange_name}")

    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("   This could indicate exchange not supported or network issues")
    finally:
        # Clean up queue system
        await ExchangeQueue.shutdown_factory()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] in ["-h", "--help"]:
        print("Usage: python rest_example.py [exchange_name]")
        print("Examples:")
        print("  python rest_example.py kraken")
        print("  python rest_example.py hyperliquid")
        print("  python rest_example.py bitmex")
        print(
            "\nFeatures:"
        )
        print("  • REST API testing (market data, account data, exchange features)")
        print("  • OHLCV collection and database storage (requires fullon_ohlcv)")
        print("  • Trade collection and database storage (requires fullon_ohlcv)")
        print(
            "\nCredentials: Set EXCHANGE_API_KEY and EXCHANGE_SECRET environment variables"
        )
        sys.exit(0)

    asyncio.run(main())
