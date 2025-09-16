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

## 🚀 Quick Start for LLMs

### 1. Basic Pattern - Always Follow This Structure

```python
import asyncio
from fullon_exchange.queue import ExchangeQueue
from fullon_exchange.core.config import ExchangeCredentials

async def main():
    # Step 1: Initialize factory (ALWAYS required)
    await ExchangeQueue.initialize_factory()

    try:
        # Step 2: Create exchange object and credential provider
        class SimpleExchange:
            def __init__(self, exchange_name: str, account_id: str):
                self.ex_id = f"{exchange_name}_{account_id}"
                self.uid = account_id
                self.test = False
                self.cat_exchange = type('CatExchange', (), {'name': exchange_name})()

        exchange_obj = SimpleExchange("kraken", "my_account")

        # Step 3: Create credential provider
        def credential_provider(exchange_obj):
            return "your_api_key", "your_secret"

        # Step 4: Get unified handler
        handler = await ExchangeQueue.get_rest_handler(exchange_obj, credential_provider)

        # Step 5: Connect
        await handler.connect()
        
        # Step 5: Use the handler
        balance = await handler.get_balance()
        print(f"Balance: {balance}")
        
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
| `rest_example.py` | basic | intermediate | Comprehensive REST API usage |
| `websocket_example.py` | streaming | intermediate | WebSocket streaming patterns |
| `simple_price_monitor.py` | streaming | beginner | Real-time price monitoring |
| `rest_ordering_example.py` | advanced | advanced | Advanced order management |
| `sandbox_example.py` | basic | beginner | Testing with sandbox/testnet |

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

## 🎯 Common LLM Usage Patterns

### Pattern 1: Simple Data Retrieval
```python
async def get_market_data():
    await ExchangeQueue.initialize_factory()
    try:
        # Create exchange object
        class SimpleExchange:
            def __init__(self, exchange_name: str, account_id: str):
                self.ex_id = f"{exchange_name}_{account_id}"
                self.uid = account_id
                self.test = False
                self.cat_exchange = type('CatExchange', (), {'name': exchange_name})()

        exchange_obj = SimpleExchange("kraken", "data_account")

        # Public data doesn't need credentials
        def credential_provider(exchange_obj):
            return "", ""

        handler = await ExchangeQueue.get_websocket_handler(exchange_obj, credential_provider)
        await handler.connect()
        
        # Get ticker data (no credentials needed)
        ticker = await handler.get_ticker("BTC/USD")
        print(f"BTC/USD Price: {ticker['last']}")
        
    finally:
        await ExchangeQueue.shutdown_factory()
```

### Pattern 2: Multi-Exchange Comparison
```python
async def compare_prices():
    await ExchangeQueue.initialize_factory()
    try:
        # Create exchange objects
        class SimpleExchange:
            def __init__(self, exchange_name: str, account_id: str):
                self.ex_id = f"{exchange_name}_{account_id}"
                self.uid = account_id
                self.test = False
                self.cat_exchange = type('CatExchange', (), {'name': exchange_name})()

        kraken_obj = SimpleExchange("kraken", "account1")
        bitmex_obj = SimpleExchange("bitmex", "account1")

        # Credential providers (can be different for each exchange)
        def kraken_creds(exchange_obj):
            return "", ""  # Or actual credentials

        def bitmex_creds(exchange_obj):
            return "", ""  # Or actual credentials

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
async def stream_data():
    await ExchangeQueue.initialize_factory()
    try:
        # Create exchange object for WebSocket streaming
        class SimpleExchange:
            def __init__(self, exchange_name: str, account_id: str):
                self.ex_id = f"{exchange_name}_{account_id}"
                self.uid = account_id
                self.test = False
                self.cat_exchange = type('CatExchange', (), {'name': exchange_name})()

        exchange_obj = SimpleExchange("kraken", "stream_account")

        def credential_provider(exchange_obj):
            return "", ""  # Public WebSocket streams don't need credentials

        handler = await ExchangeQueue.get_websocket_handler(exchange_obj, credential_provider)
        await handler.connect()
        
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
from fullon_exchange.core.exceptions import (
    QueueError, TimeoutError, AuthenticationError
)

async def robust_trading():
    await ExchangeQueue.initialize_factory()
    try:
        # Create exchange object for trading
        class SimpleExchange:
            def __init__(self, exchange_name: str, account_id: str):
                self.ex_id = f"{exchange_name}_{account_id}"
                self.uid = account_id
                self.test = False
                self.cat_exchange = type('CatExchange', (), {'name': exchange_name})()

        exchange_obj = SimpleExchange("kraken", "trade_account")

        def credential_provider(exchange_obj):
            # Trading requires real credentials
            return "api_key", "secret"

        handler = await ExchangeQueue.get_rest_handler(exchange_obj, credential_provider)
        
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

# Exchange credentials
export KRAKEN_API_KEY="your_api_key"
export KRAKEN_SECRET="your_secret"
export BITMEX_API_KEY="your_api_key"
export BITMEX_SECRET="your_secret"

# Testing
export USE_SANDBOX=true  # Use testnet/sandbox
```

### Programmatic Configuration
```python
from fullon_exchange.core.config import ExchangeConfig, ExchangeCredentials

# Configure exchange
config = ExchangeConfig(
    credentials=ExchangeCredentials(
        api_key="your_api_key",
        secret="your_secret",
        passphrase="your_passphrase"  # If required
    ),
    use_sandbox=True,  # Use testnet
    account_id="my_trading_account",
    timeout=30.0,
    rate_limit_buffer=0.1
)

# Create exchange object with custom config
class SimpleExchange:
    def __init__(self, exchange_name: str, account_id: str):
        self.ex_id = f"{exchange_name}_custom"
        self.uid = "custom_account"
        self.test = False
        self.cat_exchange = type('CatExchange', (), {'name': exchange_name})()

exchange_obj = SimpleExchange("kraken", "custom")

def credential_provider(exchange_obj):
    return config.credentials.api_key, config.credentials.secret

handler = await ExchangeQueue.get_rest_handler(exchange_obj, credential_provider)
# Apply custom config settings to handler if needed
handler.config = config
```

## ⚠️ Important Notes for LLMs

### 1. Always Use Factory Pattern
```python
# ✅ CORRECT - Always initialize factory
await ExchangeQueue.initialize_factory()
try:
    # Create exchange object and credential provider
    class SimpleExchange:
        def __init__(self, exchange_name: str, account_id: str):
            self.ex_id = f"{exchange_name}_{account_id}"
            self.uid = account_id
            self.test = False
            self.cat_exchange = type('CatExchange', (), {'name': exchange_name})()

    exchange_obj = SimpleExchange("kraken", "account")

    def credential_provider(exchange_obj):
        return "api_key", "secret"  # Or empty strings for public data

    # Use get_rest_handler for REST operations or get_websocket_handler for streaming
    handler = await ExchangeQueue.get_rest_handler(exchange_obj, credential_provider)
    # ... use handler
finally:
    await ExchangeQueue.shutdown_factory()

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

### 4. ORM Models Are Required
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
from fullon_exchange.core.config import ExchangeCredentials
from fullon_exchange.core.orm_utils import OrderRequest, CancelRequest
from fullon_exchange.core.types import OrderType, OrderSide
```

### Basic Workflow
```
1. await ExchangeQueue.initialize_factory()
2. Create exchange object: exchange_obj = SimpleExchange(exchange_name, account_id)
3. Create credential provider: credential_provider = lambda obj: (api_key, secret)
4. Get handler: handler = await ExchangeQueue.get_rest_handler(exchange_obj, credential_provider)
5. await handler.connect()
6. Use handler with proper priorities
6. await ExchangeQueue.shutdown_factory()
```

### Priority Levels
- `URGENT(1)` - Emergency operations only
- `HIGH(2)` - Important trading operations
- `NORMAL(5)` - Default for most operations
- `LOW(8)` - Background operations
- `BULK(10)` - Large batch operations

This guide provides everything an LLM needs to understand and effectively use the Fullon Exchange library. The combination of type safety, unified interfaces, and comprehensive examples makes it ideal for AI-assisted trading application development.