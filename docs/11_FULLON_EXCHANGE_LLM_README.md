# Fullon Exchange - LLM Integration Guide

🤖 **This documentation is specifically designed for Large Language Models (LLMs) and AI assistants to understand and use the Fullon Exchange library effectively.**

## 📦 Repository Access

### Git Clone (SSH)
```bash
git clone git+ssh://git@github.com/ingmarAvocado/fullon_exchange.git
cd fullon_exchange
```

### Git Clone (HTTPS - Alternative)
```bash
git clone https://github.com/ingmarAvocado/fullon_exchange.git
cd fullon_exchange
```

### Setup
```bash
# Install using Poetry (recommended)
poetry install

# Or install with pip
pip install -e .

# Install dependencies for development
poetry install --with dev
```

## 🎯 Library Overview

**Fullon Exchange** is a professional Python library for cryptocurrency trading that provides:

- **Unified API** - Same interface across all exchanges (Kraken, BitMEX, HyperLiquid)
- **Priority Queue System** - URGENT, HIGH, NORMAL, LOW, BULK priority levels
- **Type-Safe ORM** - Strongly-typed models instead of dictionaries
- **WebSocket Support** - Real-time market data and order updates
- **OHLCV Data Collection** - Native candlestick data with capability detection
- **Built-in Resilience** - Automatic rate limiting, retries, health monitoring
- **No External Dependencies** - Self-contained, no database required

### Key Philosophy: **Library Handles Complexity, Users Focus on Business Logic**

```python
# ❌ What you DON'T need to handle:
# - Rate limiting per exchange
# - Connection pooling and health monitoring  
# - Authentication token management
# - Network error recovery and retries
# - Exchange-specific API differences
# - WebSocket reconnection logic

# ✅ What you focus on:
# - Trading strategy logic
# - Priority level selection
# - Response handling
# - Business-specific error handling
```

## ⚠️ Important Update (September 2025)

**New Credential Management Pattern**: All examples now use the `fullon_credentials` service with proper ORM `Exchange` models.

**Key Changes:**
- ✅ Use `fullon_credentials` service (kraken=1, bitmex=2, hyperliquid=3)
- ✅ Proper `fullon_orm.models.Exchange` models (no more `SimpleExchange`)
- ✅ Direct `credential_provider(exchange_obj: Exchange)` functions
- ❌ No more environment variable credentials in examples
- ❌ No more `SimpleExchange` classes

## 🚀 Quick Start for LLMs

### 1. Basic Pattern - Always Follow This Structure

```python
import asyncio
from fullon_exchange.queue import ExchangeQueue
from fullon_credentials import fullon_credentials
from fullon_orm.models import CatExchange, Exchange

# Exchange ID mapping for fullon_credentials
EXCHANGE_ID_MAPPING = {
    "kraken": 1,
    "bitmex": 2,
    "hyperliquid": 3,
}

def create_example_exchange(exchange_name: str, ex_id: int) -> Exchange:
    """Create an Exchange model instance for examples."""
    # Create a mock CatExchange (in production this would be from DB)
    cat_exchange = CatExchange()
    cat_exchange.name = exchange_name
    cat_exchange.id = 1  # Mock ID

    # Create Exchange instance with ORM structure
    exchange = Exchange()
    exchange.ex_id = ex_id  # This is what fullon_credentials uses
    exchange.uid = "example_user"
    exchange.test = False
    exchange.cat_exchange = cat_exchange

    return exchange

# NOTE: credential_provider function NO LONGER NEEDED!
# ExchangeQueue.get_*_handler() now uses fullon_credentials internally
# (Shown here for reference only - you don't need to define this)

async def main():
    # Step 1: Initialize factory (ALWAYS required)
    await ExchangeQueue.initialize_factory()

    try:
        # Step 2: Get exchange ID from mapping
        exchange_name = "kraken"
        ex_id = EXCHANGE_ID_MAPPING.get(exchange_name)
        if ex_id is None:
            print(f"❌ Unsupported exchange: {exchange_name}")
            return

        # Step 3: Create Exchange model instance
        exchange_obj = create_example_exchange(exchange_name, ex_id)

        # Step 4: Get unified handler - automatically connected by ExchangeQueue
        handler = await ExchangeQueue.get_rest_handler(exchange_obj)

        # Step 5: Use the handler
        balance = await handler.get_balance()
        print(f"Balance: {balance}")

        # Get OHLCV data
        ohlcv = await handler.get_ohlcv("BTC/USD", timeframe="1h", limit=100)
        print(f"Retrieved {len(ohlcv)} candles")

    finally:
        # Step 6: Cleanup (ALWAYS required)
        await ExchangeQueue.shutdown_factory()

if __name__ == "__main__":
    asyncio.run(main())
```

### 2. Priority System (Critical for LLMs to Understand)

```python
from fullon_exchange.queue.priority import Priority, PriorityLevel

# 🔥 URGENT (1) - Emergency operations only
urgent = Priority(level=PriorityLevel.URGENT, bypass_rate_limit=True)
await handler.cancel_order(order_id, priority=urgent)

# ⚡ HIGH (2) - Important trading operations  
high = Priority(level=PriorityLevel.HIGH, timeout=15.0)
order = await handler.place_order(order_request, priority=high)

# 📊 NORMAL (5) - Regular operations (DEFAULT)
normal = Priority(level=PriorityLevel.NORMAL)
balance = await handler.get_balance(priority=normal)

# 🐌 LOW (8) - Background operations
low = Priority(level=PriorityLevel.LOW, timeout=60.0)
history = await handler.get_trade_history(priority=low)

# 📦 BULK (10) - Large batch operations
bulk = Priority(level=PriorityLevel.BULK, timeout=300.0)
export_data = await handler.export_all_data(priority=bulk)
```

### 3. ORM-Based API (Type-Safe)

```python
from fullon_exchange.core.orm_utils import OrderRequest, CancelRequest
from fullon_exchange.core.types import OrderType, OrderSide

# ✅ Type-safe order placement
order_request = OrderRequest(
    symbol="BTC/USD",
    order_type=OrderType.LIMIT,    # Enum with IDE autocomplete
    side=OrderSide.BUY,           # Enum with IDE autocomplete  
    amount=0.001,
    price=50000.0
)
order = await handler.place_order(order_request)

# ✅ Type-safe order cancellation
cancel_request = CancelRequest(
    order_id=order.id,
    symbol="BTC/USD"
)
canceled = await handler.cancel_order(cancel_request)

# ✅ Business methods on responses
if order.is_active():
    print(f"Order {order.id} is still active")
if order.is_filled():
    print(f"Order filled at {order.average_price}")
```

## 📖 Example Discovery System

**The library includes a comprehensive examples system for learning:**

```python
from fullon_exchange import examples

# 📋 List all available examples
examples.list_examples()

# 🔍 Search for specific topics
websocket_examples = examples.search_examples('websocket')
trading_examples = examples.search_examples('trading')

# 📚 Get examples by category
basic_examples = examples.get_examples_by_category('basic')
streaming_examples = examples.get_examples_by_category('streaming')
advanced_examples = examples.get_examples_by_category('advanced')

# 🎯 Get examples by difficulty
beginner = examples.get_examples_by_difficulty('beginner')
intermediate = examples.get_examples_by_difficulty('intermediate')

# 🏃 Run an example programmatically
result = await examples.run_example('basic_usage')

# 📄 Get example source code
source = examples.get_example_source('basic_usage')
print(source)
```

### Current Examples Available

| Example | Category | Difficulty | Description |
|---------|----------|------------|-------------|
| `basic_multiexchange_handler.py` | basic | beginner | Multi-exchange basic operations |
| `rest_example.py` | basic | intermediate | Comprehensive REST API usage with OHLCV |
| `websocket_example.py` | streaming | intermediate | WebSocket streaming patterns |
| `simple_price_monitor.py` | streaming | beginner | Real-time price monitoring |
| `rest_ordering_example.py` | advanced | advanced | Advanced order management |
| `sandbox_example.py` | basic | beginner | Testing with sandbox/testnet |
| `ohlcv_collection_example.py` | data | intermediate | OHLCV data collection patterns |

## 🏗️ Architecture Overview

```
┌─────────────┐    ┌──────────────┐    ┌─────────────┐
│   Your App  │────│ ExchangeQueue │────│  Exchanges  │
│             │    │   (Factory)   │    │ (Kraken,    │
│             │    │               │    │  BitMEX,    │
│             │    │               │    │  etc.)      │
└─────────────┘    └──────────────┘    └─────────────┘
                           │
                    ┌──────────────┐
                    │ Queue System │
                    │ - Priorities │ 
                    │ - Rate Limit │
                    │ - Resilience │
                    └──────────────┘
```

### Key Components:

1. **ExchangeQueue** - Factory for getting handlers
2. **UnifiedHandler** - Single interface for all exchanges
3. **Priority System** - Smart request ordering
4. **ORM Models** - Type-safe request/response objects
5. **WebSocket Manager** - Real-time data streaming
6. **Rate Limiter** - Automatic API compliance

## 📊 OHLCV Data Collection (Candlestick Data)

The library provides comprehensive OHLCV (Open, High, Low, Close, Volume) data collection with intelligent capability detection for optimal performance across exchanges.

### Core OHLCV Method

```python
# Basic OHLCV retrieval
ohlcv = await handler.get_ohlcv("BTC/USD")  # Default: 1-minute timeframe

# With specific parameters
ohlcv = await handler.get_ohlcv(
    symbol="BTC/USD",
    timeframe="1h",     # 1m, 5m, 15m, 30m, 1h, 4h, 1d, 1w
    since=1609459200000,  # Timestamp in milliseconds
    limit=100           # Maximum number of candles
)

# Data format: [timestamp, open, high, low, close, volume]
for candle in ohlcv:
    timestamp, open_price, high, low, close, volume = candle
    print(f"Time: {timestamp}, Close: ${close:.2f}, Volume: {volume}")
```

### OHLCV Capability Detection

The library automatically detects exchange capabilities to optimize data collection:

```python
# Check exchange capabilities
supports_ohlcv = handler.supports_ohlcv()          # Native OHLCV support
supports_1m = handler.supports_1m_ohlcv()          # 1-minute timeframe
needs_trades = handler.needs_trades_for_ohlcv()    # Requires trade collection
timeframes = handler.get_supported_timeframes()    # Available timeframes

print(f"Native OHLCV: {'✅' if supports_ohlcv else '❌'}")
print(f"1-minute support: {'✅' if supports_1m else '❌'}")
print(f"Needs trades: {'✅' if needs_trades else '❌'}")
print(f"Timeframes: {timeframes}")

# Smart collection strategy
if supports_1m and not needs_trades:
    # Optimal: Use native OHLCV
    ohlcv = await handler.get_ohlcv("BTC/USD", timeframe="1m")
elif needs_trades:
    # Use trade collection for better accuracy (e.g., Kraken)
    trades = await handler.get_public_trades("BTC/USD", limit=1000)
    # Library can construct OHLCV from trades
else:
    # Fallback to available timeframes
    available = handler.get_supported_timeframes()
    best_timeframe = "5m" if "5m" in available else available[0]
    ohlcv = await handler.get_ohlcv("BTC/USD", timeframe=best_timeframe)
```

### Exchange-Specific OHLCV Behavior

| Exchange | Native OHLCV | 1-minute | Needs Trades | Recommendation |
|----------|--------------|----------|--------------|----------------|
| **Kraken** | ✅ Yes | ✅ Yes | ✅ Yes | Use trade collection for accuracy |
| **Binance** | ✅ Yes | ✅ Yes | ❌ No | Use native OHLCV |
| **BitMEX** | ✅ Yes | ✅ Yes | ❌ No | Use native OHLCV |
| **HyperLiquid** | ✅ Yes | ✅ Yes | ❌ No | Use native OHLCV |

### Common OHLCV Patterns for LLMs

#### Pattern 1: Multi-Timeframe Analysis
```python
from fullon_credentials import fullon_credentials
from fullon_orm.models import CatExchange, Exchange

# Exchange ID mapping for fullon_credentials
EXCHANGE_ID_MAPPING = {
    "kraken": 1,
    "bitmex": 2,
    "hyperliquid": 3,
}

def create_example_exchange(exchange_name: str, ex_id: int) -> Exchange:
    """Create an Exchange model instance for examples."""
    cat_exchange = CatExchange()
    cat_exchange.name = exchange_name
    cat_exchange.id = 1

    exchange = Exchange()
    exchange.ex_id = ex_id
    exchange.uid = "analysis_user"
    exchange.test = False
    exchange.cat_exchange = cat_exchange
    return exchange

def credential_provider(exchange_obj: Exchange) -> tuple[str, str]:
    """Public data credential provider."""
    return "", ""  # Empty credentials for public data

async def analyze_multiple_timeframes():
    await ExchangeQueue.initialize_factory()
    try:
        # Create Exchange model instance
        exchange_name = "kraken"
        ex_id = EXCHANGE_ID_MAPPING.get(exchange_name)
        exchange_obj = create_example_exchange(exchange_name, ex_id)

        # Handler is automatically connected by ExchangeQueue
        handler = await ExchangeQueue.get_rest_handler(exchange_obj)

        # Get multiple timeframes
        timeframes = ["1m", "1h", "1d"]
        symbol = "BTC/USD"

        ohlcv_data = {}
        for tf in timeframes:
            try:
                ohlcv = await handler.get_ohlcv(symbol, timeframe=tf, limit=100)
                ohlcv_data[tf] = ohlcv
                print(f"{tf}: {len(ohlcv)} candles")
            except Exception as e:
                print(f"{tf}: Error - {e}")

        # Analyze trends across timeframes
        for tf, data in ohlcv_data.items():
            if data:
                latest = data[-1]
                close_price = latest[4]
                print(f"{tf} latest close: ${close_price:.2f}")

    finally:
        await ExchangeQueue.shutdown_factory()
```

#### Pattern 2: Price Monitoring with OHLCV
```python
async def monitor_price_action():
    await ExchangeQueue.initialize_factory()
    try:
        # Use standard functions (see top of document)
        exchange_name = "binance"
        ex_id = EXCHANGE_ID_MAPPING.get(exchange_name)
        exchange_obj = create_example_exchange(exchange_name, ex_id)

        # Public data credential provider
        def public_credential_provider(exchange_obj):
            return "", ""

        # Handler is automatically connected by ExchangeQueue
        handler = await ExchangeQueue.get_rest_handler(exchange_obj)

        # Check capabilities first
        if handler.supports_1m_ohlcv() and not handler.needs_trades_for_ohlcv():
            # Get latest 1-minute candles
            ohlcv = await handler.get_ohlcv("BTC/USD", timeframe="1m", limit=50)

            # Analyze recent price action
            recent_candles = ohlcv[-10:]  # Last 10 minutes
            for candle in recent_candles:
                timestamp, open_p, high, low, close, volume = candle
                range_pct = ((high - low) / low) * 100
                print(f"Range: {range_pct:.2f}%, Close: ${close:.2f}, Volume: {volume:.2f}")
        else:
            print("Using alternative data collection method")

    finally:
        await ExchangeQueue.shutdown_factory()
```

#### Pattern 3: Historical Data Collection
```python
async def collect_historical_data():
    await ExchangeQueue.initialize_factory()
    try:
        # Setup for historical data collection (use standard functions)
        exchange_name = "kraken"
        ex_id = EXCHANGE_ID_MAPPING.get(exchange_name)
        exchange_obj = create_example_exchange(exchange_name, ex_id)

        # Public data credential provider
        def public_credential_provider(exchange_obj):
            return "", ""

        # Handler is automatically connected by ExchangeQueue
        handler = await ExchangeQueue.get_rest_handler(exchange_obj)

        # Collect data with proper priority for bulk operations
        from fullon_exchange.queue.priority import Priority, PriorityLevel

        bulk_priority = Priority(level=PriorityLevel.BULK, timeout=300.0)

        # Get historical daily data
        from datetime import datetime, timedelta

        # Start from 30 days ago
        since = int((datetime.now() - timedelta(days=30)).timestamp() * 1000)

        daily_ohlcv = await handler.get_ohlcv(
            "BTC/USD",
            timeframe="1d",
            since=since,
            limit=30,
            priority=bulk_priority
        )

        print(f"Collected {len(daily_ohlcv)} daily candles")

        # Process historical data
        for candle in daily_ohlcv:
            timestamp, open_p, high, low, close, volume = candle
            date = datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d')
            print(f"{date}: O:{open_p:.2f} H:{high:.2f} L:{low:.2f} C:{close:.2f}")

    finally:
        await ExchangeQueue.shutdown_factory()
```

### Common Timeframes Reference

| Timeframe | Description | Use Case | Typical Limit |
|-----------|-------------|----------|---------------|
| `1m` | 1 minute | Scalping, HFT | 1000-5000 |
| `5m` | 5 minutes | Short-term analysis | 500-2000 |
| `15m` | 15 minutes | Intraday trading | 200-1000 |
| `1h` | 1 hour | Swing trading | 100-500 |
| `4h` | 4 hours | Position analysis | 50-200 |
| `1d` | 1 day | Daily analysis | 30-365 |
| `1w` | 1 week | Weekly trends | 12-52 |

### OHLCV Error Handling

```python
from fullon_exchange.core.exceptions import FullonExchangeError

try:
    ohlcv = await handler.get_ohlcv("BTC/USD", timeframe="1m", limit=100)

    if not ohlcv:
        print("No OHLCV data available for this symbol/timeframe")
    else:
        print(f"Successfully retrieved {len(ohlcv)} candles")

except FullonExchangeError as e:
    print(f"OHLCV collection failed: {e}")
    # Fallback to alternative data source
    try:
        trades = await handler.get_public_trades("BTC/USD", limit=100)
        print(f"Fallback: collected {len(trades)} trades")
    except Exception as fallback_error:
        print(f"Fallback also failed: {fallback_error}")
```

## 🎯 Common LLM Usage Patterns

**Note**: All patterns below use the new ORM-based approach. For the standard functions (`create_example_exchange`, `credential_provider`, `EXCHANGE_ID_MAPPING`), see the examples at the top of this document.

### Pattern 1: Simple Data Retrieval
```python
async def get_market_data():
    await ExchangeQueue.initialize_factory()
    try:
        # Use standard pattern from above
        exchange_name = "kraken"
        ex_id = EXCHANGE_ID_MAPPING.get(exchange_name)
        exchange_obj = create_example_exchange(exchange_name, ex_id)

        # Public data credential provider (empty credentials)
        def public_credential_provider(exchange_obj: Exchange) -> tuple[str, str]:
            return "", ""  # No credentials needed for public data

        # Handler is automatically connected by ExchangeQueue
        handler = await ExchangeQueue.get_websocket_handler(exchange_obj)

        # Get ticker data (no credentials needed)
        ticker = await handler.get_ticker("BTC/USD")
        print(f"BTC/USD Price: {ticker['last']}")

        # Get OHLCV data (public endpoint)
        ohlcv = await handler.get_ohlcv("BTC/USD", timeframe="1h", limit=24)
        print(f"Retrieved {len(ohlcv)} hourly candles")

    finally:
        await ExchangeQueue.shutdown_factory()
```

### Pattern 2: Multi-Exchange Comparison
```python
async def compare_prices():
    await ExchangeQueue.initialize_factory()
    try:
        # Create Exchange model instances for different exchanges (use standard pattern)
        kraken_ex_id = EXCHANGE_ID_MAPPING.get("kraken")
        bitmex_ex_id = EXCHANGE_ID_MAPPING.get("bitmex")

        kraken_obj = create_example_exchange("kraken", kraken_ex_id)
        bitmex_obj = create_example_exchange("bitmex", bitmex_ex_id)

        # Public data credential providers for each exchange
        def kraken_creds(exchange_obj):
            return "", ""  # Public data

        def bitmex_creds(exchange_obj):
            return "", ""  # Public data

        kraken = await ExchangeQueue.get_rest_handler(kraken_obj, kraken_creds)
        bitmex = await ExchangeQueue.get_rest_handler(bitmex_obj, bitmex_creds)
        
        await asyncio.gather(
            kraken.connect(),
            bitmex.connect()
        )
        
        # Get prices concurrently
        kraken_ticker, bitmex_ticker = await asyncio.gather(
            kraken.get_ticker("BTC/USD"),
            bitmex.get_ticker("BTC/USD")
        )
        
        print(f"Kraken: {kraken_ticker['last']}")
        print(f"BitMEX: {bitmex_ticker['last']}")
        
    finally:
        await ExchangeQueue.shutdown_factory()
```

### Pattern 3: WebSocket Streaming
```python
import asyncio
from fullon_credentials import fullon_credentials
from fullon_orm.models import CatExchange, Exchange
from fullon_exchange.queue import ExchangeQueue

# Exchange ID mapping for fullon_credentials
EXCHANGE_ID_MAPPING = {
    "kraken": 1,
    "bitmex": 2,
    "hyperliquid": 3,
}

def create_example_exchange(exchange_name: str, ex_id: int) -> Exchange:
    """Create an Exchange model instance for examples."""
    # Create a mock CatExchange (in production this would be from DB)
    cat_exchange = CatExchange()
    cat_exchange.name = exchange_name
    cat_exchange.id = 1  # Mock ID

    # Create Exchange instance with ORM structure
    exchange = Exchange()
    exchange.ex_id = ex_id  # This is what fullon_credentials uses
    exchange.uid = "example_user"
    exchange.test = False
    exchange.cat_exchange = cat_exchange

    return exchange

def credential_provider(exchange_obj: Exchange) -> tuple[str, str]:
    """Credential provider function for the queue system."""
    try:
        secret, api_key = fullon_credentials(ex_id=exchange_obj.ex_id)
        return api_key, secret
    except ValueError as e:
        raise ValueError(f"Failed to resolve credentials for exchange ID {exchange_obj.ex_id}: {e}")

async def stream_data():
    await ExchangeQueue.initialize_factory()
    try:
        # Get exchange ID from mapping
        exchange_name = "kraken"
        ex_id = EXCHANGE_ID_MAPPING.get(exchange_name)

        # Create Exchange model instance for queue system
        exchange_obj = create_example_exchange(exchange_name, ex_id)

        # Handler is automatically connected by ExchangeQueue
        handler = await ExchangeQueue.get_websocket_handler(exchange_obj)

        # Define callback
        async def handle_ticker(data):
            print(f"Price update: {data['symbol']} = ${data['last']}")

        # Subscribe to real-time updates
        sub_id = await handler.subscribe_ticker("BTC/USD", handle_ticker)

        # Stream for 60 seconds
        await asyncio.sleep(60)

        # Unsubscribe
        await handler.unsubscribe(sub_id)

    finally:
        await ExchangeQueue.shutdown_factory()
```

### Pattern 4: Error Handling
```python
from fullon_credentials import fullon_credentials
from fullon_orm.models import CatExchange, Exchange
from fullon_exchange.queue import ExchangeQueue
from fullon_exchange.core.exceptions import (
    QueueError, TimeoutError, AuthenticationError
)

# Exchange ID mapping for fullon_credentials
EXCHANGE_ID_MAPPING = {
    "kraken": 1,
    "bitmex": 2,
    "hyperliquid": 3,
}

def create_example_exchange(exchange_name: str, ex_id: int) -> Exchange:
    """Create an Exchange model instance for examples."""
    # Create a mock CatExchange (in production this would be from DB)
    cat_exchange = CatExchange()
    cat_exchange.name = exchange_name
    cat_exchange.id = 1  # Mock ID

    # Create Exchange instance with ORM structure
    exchange = Exchange()
    exchange.ex_id = ex_id  # This is what fullon_credentials uses
    exchange.uid = "example_user"
    exchange.test = False
    exchange.cat_exchange = cat_exchange

    return exchange

def credential_provider(exchange_obj: Exchange) -> tuple[str, str]:
    """Credential provider function for the queue system."""
    try:
        secret, api_key = fullon_credentials(ex_id=exchange_obj.ex_id)
        return api_key, secret
    except ValueError as e:
        raise ValueError(f"Failed to resolve credentials for exchange ID {exchange_obj.ex_id}: {e}")

async def robust_trading():
    await ExchangeQueue.initialize_factory()
    try:
        # Get exchange ID from mapping
        exchange_name = "kraken"
        ex_id = EXCHANGE_ID_MAPPING.get(exchange_name)

        # Create Exchange model instance for queue system
        exchange_obj = create_example_exchange(exchange_name, ex_id)

        # Handler is automatically connected by ExchangeQueue
        handler = await ExchangeQueue.get_rest_handler(exchange_obj)

        # Configure with retries
        priority = Priority(level=PriorityLevel.HIGH, timeout=30.0)

        try:
            order = await handler.place_order(order_request, priority=priority)
            print(f"Order placed: {order.id}")

        except QueueError:
            print("Queue is full, try again later")
        except TimeoutError:
            print("Operation timed out")
        except AuthenticationError:
            print("Check your API credentials")

    finally:
        await ExchangeQueue.shutdown_factory()
```

## 📊 Supported Exchanges

| Exchange | REST API | WebSocket | Derivatives | Status |
|----------|----------|-----------|-------------|---------|
| **Kraken** | ✅ | ✅ | ❌ | Production |
| **BitMEX** | ✅ | ✅ | ✅ | Production |
| **HyperLiquid** | ✅ | ✅ | ✅ | Beta |

### Exchange-Specific Features

#### Kraken
- Spot trading only
- Advanced order types
- Historical data access
- Real-time market data

#### BitMEX  
- Derivatives trading (futures, perpetuals)
- Advanced margin management
- Position tracking
- Funding rate data

#### HyperLiquid
- On-chain derivatives
- Cross-collateral margin
- Liquidity incentives
- Advanced order types

## 🔧 Configuration Options

### Environment Variables
```bash
# Logging configuration
export LOG_LEVEL=INFO
export LOG_CONSOLE=true
export LOG_FILE_PATH=""  # Empty = no file logging

# New fullon_credentials pattern - Exchange ID based
# For Kraken (ex_id=1)
export EX_ID_1_KEY="your_kraken_api_key"
export EX_ID_1_SECRET="your_kraken_secret"

# For BitMEX (ex_id=2)
export EX_ID_2_KEY="your_bitmex_api_key"
export EX_ID_2_SECRET="your_bitmex_secret"

# For Hyperliquid (ex_id=3)
export EX_ID_3_KEY="your_hyperliquid_api_key"
export EX_ID_3_SECRET="your_hyperliquid_secret"

# Testing
export USE_SANDBOX=true  # Use testnet/sandbox
```

### Programmatic Configuration
```python
from fullon_credentials import fullon_credentials
from fullon_orm.models import CatExchange, Exchange
from fullon_exchange.queue import ExchangeQueue
from fullon_exchange.core.config import ExchangeConfig

# Exchange ID mapping for fullon_credentials
EXCHANGE_ID_MAPPING = {
    "kraken": 1,
    "bitmex": 2,
    "hyperliquid": 3,
}

def create_example_exchange(exchange_name: str, ex_id: int) -> Exchange:
    """Create an Exchange model instance for examples."""
    # Create a mock CatExchange (in production this would be from DB)
    cat_exchange = CatExchange()
    cat_exchange.name = exchange_name
    cat_exchange.id = 1  # Mock ID

    # Create Exchange instance with ORM structure
    exchange = Exchange()
    exchange.ex_id = ex_id  # This is what fullon_credentials uses
    exchange.uid = "example_user"
    exchange.test = False
    exchange.cat_exchange = cat_exchange

    return exchange

def credential_provider(exchange_obj: Exchange) -> tuple[str, str]:
    """Credential provider function for the queue system."""
    try:
        secret, api_key = fullon_credentials(ex_id=exchange_obj.ex_id)
        return api_key, secret
    except ValueError as e:
        raise ValueError(f"Failed to resolve credentials for exchange ID {exchange_obj.ex_id}: {e}")

# Configure exchange with custom settings
config = ExchangeConfig(
    use_sandbox=True,  # Use testnet
    account_id="my_trading_account",
    timeout=30.0,
    rate_limit_buffer=0.1
)

# Get exchange ID from mapping
exchange_name = "kraken"
ex_id = EXCHANGE_ID_MAPPING.get(exchange_name)

# Create Exchange model instance
exchange_obj = create_example_exchange(exchange_name, ex_id)

# Handler is automatically connected by ExchangeQueue
handler = await ExchangeQueue.get_rest_handler(exchange_obj)
# Apply custom config settings to handler if needed
handler.config = config
```

## ⚠️ Important Notes for LLMs

### 1. Always Use Modern ORM Pattern
```python
# ✅ CORRECT - Use new ORM-based pattern
await ExchangeQueue.initialize_factory()
try:
    # Use standard functions from examples above
    exchange_name = "kraken"
    ex_id = EXCHANGE_ID_MAPPING.get(exchange_name)
    exchange_obj = create_example_exchange(exchange_name, ex_id)

    # Use get_rest_handler for REST operations or get_websocket_handler for streaming
    # Handler is automatically connected by ExchangeQueue
    handler = await ExchangeQueue.get_rest_handler(exchange_obj)
    # ... use handler
finally:
    await ExchangeQueue.shutdown_factory()

# ❌ INCORRECT - Don't use old SimpleExchange pattern
# NEVER create SimpleExchange classes - they are forbidden

# ❌ INCORRECT - Never create adapters directly
adapter = KrakenAdapter(config)  # DON'T DO THIS
```

### 2. Priority Levels Matter
```python
# ✅ CORRECT - Use appropriate priorities
urgent_cancel = Priority(level=PriorityLevel.URGENT)  # Cancel orders
high_trade = Priority(level=PriorityLevel.HIGH)       # Place orders  
normal_data = Priority(level=PriorityLevel.NORMAL)    # Get data
low_history = Priority(level=PriorityLevel.LOW)       # Get history

# ❌ INCORRECT - Don't abuse URGENT priority
urgent = Priority(level=PriorityLevel.URGENT)
await handler.get_ticker("BTC/USD", priority=urgent)  # NOT urgent!
```

### 3. Always Handle Cleanup
```python
# ✅ Use try/finally or context managers
try:
    # Your trading logic
    pass
finally:
    await ExchangeQueue.shutdown_factory()

# ✅ Or use context managers
async with ExchangeQueue.get_context_manager("kraken", "account") as handler:
    # Automatic cleanup
    pass
```

### 4. Use fullon_credentials Service
```python
# ✅ CORRECT - Use fullon_credentials with exchange IDs
from fullon_credentials import fullon_credentials

def credential_provider(exchange_obj: Exchange) -> tuple[str, str]:
    try:
        secret, api_key = fullon_credentials(ex_id=exchange_obj.ex_id)
        return api_key, secret
    except ValueError as e:
        raise ValueError(f"Failed to resolve credentials for exchange ID {exchange_obj.ex_id}: {e}")

# ❌ INCORRECT - Don't use environment variables directly
api_key = os.getenv("KRAKEN_API_KEY")  # DON'T DO THIS

# ❌ INCORRECT - Don't use factory pattern
def create_credential_provider(api_key, secret):  # DON'T DO THIS
    def credential_provider(exchange_obj):
        return api_key, secret
    return credential_provider
```

### 5. ORM Models Are Required
```python
# ✅ CORRECT - Use ORM models
order_request = OrderRequest(
    symbol="BTC/USD",
    order_type=OrderType.LIMIT,
    side=OrderSide.BUY,
    amount=0.001,
    price=50000.0
)
order = await handler.place_order(order_request)

# ❌ INCORRECT - Don't use raw parameters
order = await handler.place_order(
    "BTC/USD", "limit", "buy", 0.001, 50000.0
)  # This won't work!
```

## 📚 Additional Documentation

For comprehensive information, see these additional guides:

- **[Quick Start Guide](QUICK_START.md)** - Detailed getting started
- **[Architecture Guide](ARCHITECTURE.md)** - System design details  
- **[API Reference](API_REFERENCE.md)** - Complete API documentation
- **[ORM API Guide](ORM_API_GUIDE.md)** - Type-safe API patterns
- **[Queue System Guide](queue_system_guide.md)** - Priority and queue management
- **[Examples Guide](EXAMPLES.md)** - All examples with explanations

## 🤝 LLM Integration Best Practices

### 1. Start with Examples
Always begin by exploring the examples system to understand patterns:

```python
from fullon_exchange import examples
examples.list_examples()
beginner_examples = examples.get_examples_by_difficulty('beginner')
```

### 2. Use Progressive Complexity
- Start with `basic_multiexchange_handler.py`
- Move to `websocket_example.py` for real-time data
- Advance to `rest_ordering_example.py` for trading

### 3. Follow the Factory Pattern
Never deviate from the ExchangeQueue factory pattern - it handles all complexity.

### 4. Understand Priority Levels
Use appropriate priority levels to ensure proper queue management and rate limiting.

### 5. Leverage Type Safety
Use the ORM models for all operations - they prevent errors and provide IDE support.

---

## 🎯 Quick Reference Card

### Essential Imports
```python
from fullon_exchange.queue import ExchangeQueue
from fullon_exchange.queue.priority import Priority, PriorityLevel
from fullon_credentials import fullon_credentials
from fullon_orm.models import CatExchange, Exchange
from fullon_exchange.core.orm_utils import OrderRequest, CancelRequest
from fullon_exchange.core.types import OrderType, OrderSide
from fullon_exchange.core.exceptions import FullonExchangeError
```

### Basic Workflow
```
1. await ExchangeQueue.initialize_factory()
2. Get exchange ID: ex_id = EXCHANGE_ID_MAPPING.get(exchange_name)
3. Create exchange object: exchange_obj = create_example_exchange(exchange_name, ex_id)
4. Get handler: handler = await ExchangeQueue.get_rest_handler(exchange_obj)
   (Handler is automatically connected and credentials are handled via fullon_credentials)
5. Use handler with proper priorities
6. await ExchangeQueue.shutdown_factory()
```

### Priority Levels
- `URGENT(1)` - Emergency operations only
- `HIGH(2)` - Important trading operations
- `NORMAL(5)` - Default for most operations
- `LOW(8)` - Background operations
- `BULK(10)` - Large batch operations

### Key OHLCV Methods
```python
# Basic OHLCV retrieval
ohlcv = await handler.get_ohlcv("BTC/USD", timeframe="1h", limit=100)

# Capability detection
supports_ohlcv = handler.supports_ohlcv()
supports_1m = handler.supports_1m_ohlcv()
needs_trades = handler.needs_trades_for_ohlcv()
timeframes = handler.get_supported_timeframes()

# Common timeframes: 1m, 5m, 15m, 30m, 1h, 4h, 1d, 1w
# Data format: [timestamp, open, high, low, close, volume]
```

### OHLCV Decision Logic
```python
if handler.supports_1m_ohlcv() and not handler.needs_trades_for_ohlcv():
    # Use native OHLCV (optimal)
    ohlcv = await handler.get_ohlcv(symbol, timeframe="1m")
elif handler.needs_trades_for_ohlcv():
    # Use trade collection (Kraken-style)
    trades = await handler.get_public_trades(symbol, limit=1000)
else:
    # Use available timeframes
    timeframes = handler.get_supported_timeframes()
    ohlcv = await handler.get_ohlcv(symbol, timeframe=timeframes[0])
```

This guide provides everything an LLM needs to understand and effectively use the Fullon Exchange library. The combination of type safety, unified interfaces, comprehensive OHLCV data collection, and comprehensive examples makes it ideal for AI-assisted trading application development.