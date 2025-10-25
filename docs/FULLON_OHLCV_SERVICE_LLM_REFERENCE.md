# fullon_ohlcv_service - Complete LLM Development Reference

**Version**: 1.1.7
**Last Updated**: 2025-10-20
**Purpose**: Comprehensive reference for LLM-assisted development of fullon_ohlcv_service

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Core Architecture](#core-architecture)
3. [Ecosystem Dependencies](#ecosystem-dependencies)
4. [Implementation Patterns](#implementation-patterns)
5. [Examples Reference](#examples-reference)
6. [Development Workflow](#development-workflow)
7. [Best Practices](#best-practices)
8. [Common Pitfalls](#common-pitfalls)

---

## Quick Start

### Mission Statement

**fullon_ohlcv_service** is a **simple async daemon** for OHLCV/trade data collection that serves as a **lightweight integration layer** over the fullon ecosystem libraries.

### LRRS Architecture Principles

- **Little**: Lightweight integration layer, NOT a data collection framework (~1,600 lines total)
- **Responsible**: Coordinate collectors, leverage fullon_exchange/fullon_ohlcv for heavy lifting
- **Reusable**: Standard fullon ecosystem patterns, database-driven configuration
- **Separate**: Clean integration layer, zero reimplementation of existing fullon functionality

### What This Service Does

1. **Discovers** exchanges/symbols from `fullon_orm` database
2. **Collects** OHLCV/trade data via `fullon_exchange` WebSocket/REST APIs
3. **Stores** data using `fullon_ohlcv` repositories
4. **Monitors** health via `fullon_cache` ProcessCache
5. **Logs** operations with `fullon_log` structured logging

### What This Service Does NOT Do

- ❌ Implement WebSocket connections (use `fullon_exchange.queue.ExchangeQueue`)
- ❌ Manage database schemas (use `fullon_ohlcv` repositories)
- ❌ Handle reconnection logic (use `fullon_exchange` auto-reconnection)
- ❌ Parse exchange responses (use `fullon_exchange` handlers)
- ❌ Hardcode exchange/symbol lists (use `fullon_orm` database)

---

## Core Architecture

### Directory Structure

```
fullon_ohlcv_service/
├── src/fullon_ohlcv_service/
│   ├── ohlcv/
│   │   ├── live_collector.py      # LiveOHLCVCollector - WebSocket OHLCV streaming
│   │   └── historic_collector.py  # HistoricOHLCVCollector - REST-based historic OHLCV
│   ├── trade/
│   │   ├── live_collector.py      # LiveTradeCollector - WebSocket trade streaming
│   │   ├── historic_collector.py  # HistoricTradeCollector - REST-based historic trades
│   │   └── batcher.py             # GlobalTradeBatcher - Redis batch processing
│   ├── config/
│   │   ├── settings.py            # Configuration management
│   │   └── database_config.py     # Database-driven configuration
│   ├── daemon.py                  # Main service daemon coordinator
│   └── utils/
│       └── add_symbols.py         # Symbol initialization utilities
├── examples/
│   ├── ohlcv_collection_example.py    # OHLCV collection demo
│   ├── trade_collection_example.py    # Trade collection demo
│   ├── run_example_pipeline.py        # Full pipeline demo
│   ├── monitor_processes.py           # ProcessCache monitoring demo
│   └── demo_data.py                   # Test database setup
└── tests/
    └── (pytest-based test suite)
```

### Component Overview

| Component | Lines | Purpose |
|-----------|-------|---------|
| **LiveOHLCVCollector** | ~150 | WebSocket OHLCV streaming using fullon_exchange |
| **HistoricOHLCVCollector** | ~200 | REST-based historic OHLCV backfill |
| **LiveTradeCollector** | ~150 | WebSocket trade streaming |
| **HistoricTradeCollector** | ~150 | REST-based historic trade backfill |
| **GlobalTradeBatcher** | ~200 | Redis-based trade batching for OHLCV conversion |
| **DatabaseConfig** | ~100 | Database-driven symbol/exchange discovery |
| **OhlcvServiceDaemon** | ~400 | Main service orchestration |
| **Settings** | ~50 | Environment configuration |

**Total**: ~1,600 lines (Foundation complete, ready for optimization)

### Two-Phase Collection Pattern

All collectors follow a two-phase pattern inspired by legacy fullon systems:

```
Phase 1: Historical Catch-Up
├── REST API calls with pagination
├── Backfill from database last_timestamp to now
└── Transition to Phase 2 when caught up

Phase 2: Real-Time Streaming
├── WebSocket connections for live data
├── Continuous streaming until shutdown
└── Auto-reconnection via fullon_exchange
```

---

## Ecosystem Dependencies

### 1. fullon_orm - Database Operations

**Purpose**: Database-driven configuration and ORM models

```python
from fullon_orm.database_context import DatabaseContext
from fullon_orm.models import User, Exchange, Symbol

# Get exchanges/symbols from database (NO hardcoded lists)
async with DatabaseContext() as db:
    exchanges = await db.exchanges.get_user_exchanges(user_id=1)
    symbols = await db.symbols.get_by_exchange_id(cat_ex_id)
```

**Key Repositories**:
- `db.exchanges.*` - Exchange operations
- `db.symbols.*` - Symbol operations
- `db.users.*` - User operations
- `db.bots.*` - Bot operations

**Critical Rule**: ALL configuration comes from the database. Never hardcode exchange names or symbol lists.

### 2. fullon_exchange - Exchange Integration

**Purpose**: WebSocket/REST data collection with auto-reconnection

```python
from fullon_exchange.queue import ExchangeQueue

# Initialize factory (once at startup)
await ExchangeQueue.initialize_factory()

# Create exchange object
class SimpleExchange:
    def __init__(self, exchange_name: str, account_id: str):
        self.ex_id = f"{exchange_name}_{account_id}"
        self.uid = account_id
        self.test = False
        self.cat_exchange = type('CatExchange', (), {'name': exchange_name})()

exchange_obj = SimpleExchange("kraken", "ohlcv_account")

# Create credential provider (empty for public data)
def credential_provider(exchange_obj):
    return "", ""  # Empty for public data

# Get REST handler
handler = await ExchangeQueue.get_rest_handler(exchange_obj, credential_provider)
await handler.connect()
candles = await handler.get_ohlcv("BTC/USD", "1m", limit=100)

# Get WebSocket handler
ws_handler = await ExchangeQueue.get_websocket_handler(exchange_obj, credential_provider)
await ws_handler.connect()
await ws_handler.subscribe_ohlcv("BTC/USD", "1m", callback)

# Cleanup
await ExchangeQueue.shutdown_factory()
```

**Key Features**:
- Auto-reconnection with exponential backoff
- Unified API across all exchanges (Kraken, Binance, BitMEX, Hyperliquid, etc.)
- Automatic rate limiting
- Error handling for network/exchange issues

**Critical Rule**: NEVER implement your own WebSocket connections. Always use ExchangeQueue.

### 3. fullon_ohlcv - Data Storage

**Purpose**: Database storage for OHLCV/trade data with TimescaleDB

```python
from fullon_ohlcv.repositories.ohlcv import CandleRepository, TradeRepository, TimeseriesRepository
from fullon_ohlcv.models import Candle, Trade

# Store candles
async with CandleRepository("kraken", "BTC/USD", test=False) as repo:
    success = await repo.save_candles(candles)

# Store trades
async with TradeRepository("kraken", "BTC/USD", test=False) as repo:
    success = await repo.save_trades(trades)

# Fetch OHLCV data
async with TimeseriesRepository("kraken", "BTC/USD", test=False) as repo:
    ohlcv = await repo.fetch_ohlcv(
        compression=1,
        period="minutes",
        fromdate=arrow.get(start_time),
        todate=arrow.get(end_time)
    )
```

**Key Repositories**:
- `CandleRepository` - OHLCV candle storage
- `TradeRepository` - Individual trade storage
- `TimeseriesRepository` - TimescaleDB-based time-series queries

**Critical Rule**: Never implement your own database queries. Always use fullon_ohlcv repositories.

### 4. fullon_cache - Redis Operations

**Purpose**: Real-time cache updates and process health monitoring

```python
from fullon_cache import ProcessCache, OHLCVCache

# Health monitoring
async with ProcessCache() as cache:
    await cache.update_process("ohlcv_service", "daemon", "running")
    await cache.update_process("ohlcv_service", "kraken:BTC/USD", "collecting",
                             message="100 candles collected")

# OHLCV cache operations
async with OHLCVCache() as cache:
    await cache.set_ohlcv("kraken", "BTC/USD", "1m", ohlcv_data)
    cached = await cache.get_ohlcv("kraken", "BTC/USD", "1m")
```

**Key Features**:
- Process health monitoring
- Real-time OHLCV caching
- Trade batching coordination
- Component status tracking

**Update Frequency**: Rate-limit ProcessCache updates to ~30 seconds to avoid Redis overhead.

### 5. fullon_log - Structured Logging

**Purpose**: Component-based structured logging

```python
from fullon_log import get_component_logger

logger = get_component_logger("fullon.ohlcv.collector.kraken.BTCUSD")
logger.info("OHLCV collected", symbol="BTC/USD", count=100, exchange="kraken")
logger.error("Collection failed", symbol="BTC/USD", error=str(e))
```

**Logging Hierarchy**:
```
fullon
└── ohlcv
    ├── daemon
    ├── collector
    │   ├── kraken
    │   │   └── BTCUSD
    │   └── binance
    │       └── BTCUSDT
    └── batcher
```

**Critical Rule**: Always use structured logging with key-value pairs, not string interpolation.

---

## Implementation Patterns

### Pattern 1: Database-Driven Configuration

```python
from fullon_orm.database_context import DatabaseContext

async def get_collection_targets():
    """Get what to collect from fullon database - like ticker service"""
    async with DatabaseContext() as db:
        # Get user's active exchanges
        exchanges = await db.exchanges.get_user_exchanges(user_id=1)

        targets = {}
        for exchange in exchanges:
            if exchange['active']:
                # Get active symbols for this exchange
                symbols = await db.symbols.get_by_exchange_id(exchange['cat_ex_id'])
                targets[exchange['name']] = [s.symbol for s in symbols if s.active]

        return targets  # {"kraken": ["BTC/USD"], "binance": ["BTC/USDT"]}
```

**Why**: Configuration in database allows dynamic addition of exchanges/symbols without code changes.

### Pattern 2: Basic OHLCV Collector

```python
from fullon_log import get_component_logger
from fullon_exchange.queue import ExchangeQueue
from fullon_ohlcv.repositories.ohlcv import CandleRepository

class OhlcvCollector:
    def __init__(self, symbol: str, exchange: str):
        self.logger = get_component_logger(f"fullon.ohlcv.{exchange}.{symbol}")
        self.symbol = symbol
        self.exchange = exchange

    async def collect_data(self):
        """Collect OHLCV using fullon_exchange, store with fullon_ohlcv"""
        await ExchangeQueue.initialize_factory()
        try:
            # Create exchange object
            class SimpleExchange:
                def __init__(self, exchange_name: str, account_id: str):
                    self.ex_id = f"{exchange_name}_{account_id}"
                    self.uid = account_id
                    self.test = False
                    self.cat_exchange = type('CatExchange', (), {'name': exchange_name})()

            exchange_obj = SimpleExchange(self.exchange, "ohlcv_account")

            # Create credential provider
            def credential_provider(exchange_obj):
                return "", ""  # Empty for public data

            # Use fullon_exchange for data collection
            handler = await ExchangeQueue.get_rest_handler(exchange_obj, credential_provider)
            await handler.connect()
            candles = await handler.get_ohlcv(self.symbol, "1m", limit=100)

            # Use fullon_ohlcv for database storage
            async with CandleRepository(self.exchange, self.symbol, test=False) as repo:
                success = await repo.save_candles(candles)
                self.logger.info("OHLCV collection completed",
                               symbol=self.symbol, count=len(candles), success=success)

        finally:
            await ExchangeQueue.shutdown_factory()
```

**Why**: This pattern keeps collector code under 100 lines by leveraging ecosystem libraries.

### Pattern 3: Health Monitoring

```python
from fullon_cache import ProcessCache

class OhlcvServiceDaemon:
    async def update_health_status(self, status: str):
        """Update daemon health using fullon_cache"""
        async with ProcessCache() as cache:
            await cache.update_process("ohlcv_service", "daemon", status)

    async def update_collector_status(self, exchange: str, symbol: str,
                                     status: str, message: str = ""):
        """Update individual collector status"""
        component = f"{exchange}:{symbol}"
        async with ProcessCache() as cache:
            await cache.update_process("ohlcv_service", component, status,
                                     message=message)
```

**Why**: Centralized health monitoring allows external tools to track service status.

### Pattern 4: Basic Daemon Coordination

```python
from fullon_orm.database_context import DatabaseContext
from fullon_log import get_component_logger

class OhlcvServiceDaemon:
    def __init__(self):
        self.logger = get_component_logger("fullon.ohlcv.daemon")

    async def start(self):
        """Start daemon using fullon_orm for configuration"""
        # Get what to collect from database
        targets = await get_collection_targets()

        # Start collectors for each exchange/symbol
        for exchange, symbols in targets.items():
            for symbol in symbols:
                collector = OhlcvCollector(symbol, exchange)
                await collector.start_collection()
```

**Why**: Simple coordination logic keeps daemon under 100 lines.

---

## Examples Reference

### Example 1: OHLCV Collection (ohlcv_collection_example.py)

**Purpose**: Demonstrates single-symbol OHLCV collection with two-phase pattern

**Key Features**:
- Test database setup with dual databases (ORM + OHLCV)
- Automatic collector selection based on exchange capabilities
- Two-phase collection (historic REST + live WebSocket)
- OHLCV data verification

**Usage**:
```bash
python examples/ohlcv_collection_example.py
```

**What It Does**:
1. Creates isolated test databases (`fullon_ohlcv_test_<random>` + `_ohlcv`)
2. Installs demo data (exchanges: kraken, bitmex, hyperliquid)
3. Collects BTC/USDC:USDC on hyperliquid
4. Verifies candles in database
5. Cleans up test databases

**Expected Output**:
```
🧪 OHLCV SINGLE-SYMBOL COLLECTION EXAMPLE
Testing daemon process_symbol() method for BTC/USDC:USDC on hyperliquid

Pattern:
  Phase 1: Historical catch-up (REST with pagination)
  Phase 2: Real-time streaming (WebSocket)

📊 Starting OHLCV collection for BTC/USDC:USDC on hyperliquid...
⏱️  Running collection until end of minute plus 1 second (45 seconds)...
✅ Collection completed

📊 Checking OHLCV candle data for BTC/USDC:USDC on hyperliquid...
🔍 Checking symbol: hyperliquid:BTC/USDC:USDC
   🕐 Last 10 1-minute candles:
   2025-10-20 14:55:00 | O:67345.50 H:67350.00 L:67340.00 C:67348.00 V:1.2345
   ...
   ✅ Found 15 1-minute candles (showing last 10)
```

### Example 2: Trade Collection (trade_collection_example.py)

**Purpose**: Demonstrates single-symbol trade collection with two-phase pattern

**Key Features**:
- Trade streaming via WebSocket
- Automatic fallback to OHLCV if trades not supported
- Trade→OHLCV conversion via GlobalTradeBatcher
- Database verification

**Usage**:
```bash
python examples/trade_collection_example.py
```

**What It Does**:
1. Creates test databases
2. Collects BTC/USDC on kraken
3. Streams trades and converts to OHLCV
4. Verifies candles derived from trades
5. Cleanup

**Note**: Kraken supports trades, so this demonstrates the trade→OHLCV conversion path.

### Example 3: Full Pipeline (run_example_pipeline.py)

**Purpose**: Demonstrates complete daemon-based collection for all symbols

**Key Features**:
- Multi-symbol concurrent collection
- Daemon lifecycle management
- Timing control (runs until end of next minute + 1ms)
- Results verification across multiple symbols

**Usage**:
```bash
python examples/run_example_pipeline.py
```

**What It Does**:
1. Creates test databases
2. Starts daemon with all symbols
3. Runs historic collection concurrently
4. Transitions to live collection
5. Runs until specific time (end of next minute + 1ms)
6. Verifies data for multiple symbols
7. Cleanup

**Expected Output**:
```
🧪 FULL PIPELINE WITH DAEMON EXAMPLE
Testing complete OHLCV/trade collection using daemon

Pattern:
  Phase 1: Historical catch-up (via daemon)
  Phase 2: Live streaming until end of next minute + 1ms

📚 Phase 1: Running concurrent historic collection (OHLCV + Trades)...
✅ Historic collection completed
📊 Phase 2: Running live collection until end of next minute + 1ms...
Running live collection until 14:56:00.001 UTC (45.234 seconds)...
⏹️  Requesting daemon shutdown...
✅ Live collection completed

📊 Checking pipeline results...
Found 3 symbols in database
✅ kraken:BTC/USDC - 15 candles
✅ bitmex:BTC/USD:BTC - 12 candles
✅ hyperliquid:BTC/USDC:USDC - 18 candles
```

### Example 4: Process Monitoring (monitor_processes.py)

**Purpose**: Demonstrates ProcessCache monitoring integration

**Key Features**:
- Query active processes
- Check component status
- System health monitoring

**Usage**:
```bash
python examples/monitor_processes.py
```

**What It Shows**:
- How to monitor OHLCV collection processes
- How to check specific symbol status
- How to get system health overview

### Example 5: Demo Data Setup (demo_data.py)

**Purpose**: Test database creation and demo data installation

**Key Features**:
- Dual database creation (ORM + OHLCV)
- Demo data installation (users, exchanges, symbols, bots)
- Database lifecycle management
- Worker-aware test database naming (for pytest-xdist)

**Usage**:
```bash
# Setup test environment
python examples/demo_data.py --setup

# Cleanup specific database
python examples/demo_data.py --cleanup fullon_ohlcv_test_abc123

# Full workflow (setup → run examples → cleanup)
python examples/demo_data.py --run-all
```

**Demo Data Installed**:
- **User**: admin@fullon (admin role)
- **Exchanges**:
  - kraken (BTC/USDC)
  - bitmex (BTC/USD:BTC)
  - hyperliquid (BTC/USDC:USDC)
- **Bots** (optional): RSI strategies, LLM trader

---

## Development Workflow

### 1. Setting Up Development Environment

```bash
# Clone repository
git clone <repo-url>
cd fullon_ohlcv_service

# Install dependencies (poetry)
poetry install

# Configure environment
cp .env.example .env
# Edit .env with your database/Redis credentials

# Run tests
poetry run pytest

# Run examples
poetry run python examples/ohlcv_collection_example.py
```

### 2. Running the Daemon

```bash
# Production mode
poetry run python -m fullon_ohlcv_service.daemon

# Development mode with debug logging
LOG_LEVEL=DEBUG poetry run python -m fullon_ohlcv_service.daemon
```

### 3. Adding a New Collector

**Step 1**: Create collector class following the pattern:

```python
from fullon_log import get_component_logger
from fullon_exchange.queue import ExchangeQueue

class MyCollector:
    def __init__(self, symbol: str, exchange: str):
        self.logger = get_component_logger(f"fullon.ohlcv.{exchange}.{symbol}")
        self.symbol = symbol
        self.exchange = exchange

    async def collect(self):
        # Use fullon_exchange for data collection
        # Use fullon_ohlcv for storage
        pass
```

**Step 2**: Register in daemon:

```python
# In daemon.py
from .collectors.my_collector import MyCollector

async def process_symbol(self, symbol):
    collector = MyCollector(symbol.symbol, symbol.cat_exchange.name)
    await collector.collect()
```

**Step 3**: Add tests:

```python
# In tests/test_my_collector.py
import pytest

@pytest.mark.asyncio
async def test_my_collector():
    collector = MyCollector("BTC/USD", "kraken")
    await collector.collect()
    # Assert expected behavior
```

### 4. Database Migrations

**fullon_ohlcv_service does NOT handle migrations**. Database schemas are managed by:
- `fullon_orm` - ORM database schema
- `fullon_ohlcv` - OHLCV database schema (TimescaleDB)

If you need schema changes, update the respective library.

### 5. Testing Strategy

```bash
# Unit tests (fast, no external dependencies)
poetry run pytest tests/unit

# Integration tests (requires database/Redis)
poetry run pytest tests/integration

# Full test suite
poetry run pytest

# With coverage
poetry run pytest --cov=fullon_ohlcv_service --cov-report=html
```

---

## Best Practices

### 1. Keep Collectors Under 100 Lines

**Good**:
```python
class SimpleOHLCVCollector:
    """Collect OHLCV using fullon_exchange, store with fullon_ohlcv."""

    def __init__(self, symbol, exchange):
        self.symbol = symbol
        self.exchange = exchange

    async def collect(self):
        # Use ecosystem libraries
        handler = await ExchangeQueue.get_rest_handler(...)
        candles = await handler.get_ohlcv(...)
        await repo.save_candles(candles)
```

**Bad**:
```python
class ComplexOHLCVCollector:
    """300-line collector with custom WebSocket implementation."""

    async def collect(self):
        # Reimplementing fullon_exchange functionality
        ws = await websocket.connect(...)  # ❌ Don't do this
        # Custom reconnection logic  # ❌ Don't do this
        # Custom exchange parsing  # ❌ Don't do this
```

### 2. Use Structured Logging

**Good**:
```python
logger.info("OHLCV collected",
           symbol=symbol,
           exchange=exchange,
           count=len(candles),
           timestamp=arrow.utcnow().isoformat())
```

**Bad**:
```python
logger.info(f"Collected {len(candles)} candles for {symbol} on {exchange}")
```

### 3. Database-Driven Configuration

**Good**:
```python
async with DatabaseContext() as db:
    exchanges = await db.exchanges.get_user_exchanges(user_id=1)
```

**Bad**:
```python
EXCHANGES = ["kraken", "binance", "bitmex"]  # ❌ Hardcoded
```

### 4. Rate-Limit ProcessCache Updates

**Good**:
```python
if (datetime.now() - self.last_cache_update).seconds >= 30:
    await cache.update_process(...)
    self.last_cache_update = datetime.now()
```

**Bad**:
```python
# Update on every candle (thousands per hour)
await cache.update_process(...)  # ❌ Too frequent
```

### 5. Handle Errors Gracefully

**Good**:
```python
try:
    candles = await handler.get_ohlcv(...)
except Exception as e:
    logger.error("OHLCV collection failed",
                symbol=symbol,
                error=str(e),
                exc_info=True)
    # Continue with next symbol
```

**Bad**:
```python
candles = await handler.get_ohlcv(...)  # ❌ No error handling
```

### 6. Clean Up Resources

**Good**:
```python
try:
    await ExchangeQueue.initialize_factory()
    # ... collection logic ...
finally:
    await ExchangeQueue.shutdown_factory()
```

**Bad**:
```python
await ExchangeQueue.initialize_factory()
# ... collection logic ...
# ❌ No cleanup, resources leak
```

---

## Common Pitfalls

### Pitfall 1: Reimplementing Exchange Connections

**Problem**:
```python
import websocket

async def connect_to_exchange():
    ws = await websocket.connect("wss://kraken.com/...")  # ❌ Don't do this
```

**Solution**: Use `fullon_exchange.queue.ExchangeQueue`

```python
from fullon_exchange.queue import ExchangeQueue

handler = await ExchangeQueue.get_websocket_handler(exchange_obj, credential_provider)
await handler.subscribe_ohlcv(symbol, timeframe, callback)
```

### Pitfall 2: Hardcoding Exchange/Symbol Lists

**Problem**:
```python
EXCHANGES = ["kraken", "binance"]  # ❌ Hardcoded
SYMBOLS = ["BTC/USD", "ETH/USD"]  # ❌ Hardcoded
```

**Solution**: Read from database

```python
async with DatabaseContext() as db:
    exchanges = await db.exchanges.get_user_exchanges(user_id=1)
    symbols = await db.symbols.get_by_exchange_id(cat_ex_id)
```

### Pitfall 3: Custom Database Queries

**Problem**:
```python
async with db.session.execute("SELECT * FROM ohlcv WHERE ...") as result:
    # ❌ Don't write raw SQL
```

**Solution**: Use `fullon_ohlcv` repositories

```python
async with TimeseriesRepository(exchange, symbol, test=False) as repo:
    ohlcv = await repo.fetch_ohlcv(compression=1, period="minutes", ...)
```

### Pitfall 4: Over-Engineering

**Problem**:
```python
class AbstractCollectorFactory:  # ❌ Unnecessary abstraction
    def create_collector(self, collector_type):
        # 200 lines of factory logic
```

**Solution**: Keep it simple

```python
# Just instantiate directly
collector = LiveOHLCVCollector(symbol, exchange)
await collector.collect()
```

### Pitfall 5: Ignoring Error Recovery

**Problem**:
```python
async def collect():
    candles = await handler.get_ohlcv(...)
    # ❌ No error handling, crashes on network issues
```

**Solution**: Implement retry logic

```python
async def collect(self):
    for attempt in range(3):
        try:
            candles = await handler.get_ohlcv(...)
            return candles
        except Exception as e:
            logger.warning("Collection attempt failed",
                         attempt=attempt+1, error=str(e))
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
            else:
                logger.error("All collection attempts failed")
                raise
```

### Pitfall 6: Test Database Pollution

**Problem**:
```python
# Tests use production database
async def test_collection():
    # ❌ Writing to production database
    await collector.collect()
```

**Solution**: Use isolated test databases

```python
from examples.demo_data import create_dual_test_databases, drop_dual_test_databases

async def test_collection():
    test_db = "fullon_ohlcv_test_" + random_string()
    orm_db, ohlcv_db = await create_dual_test_databases(test_db)
    try:
        # Test with isolated database
        await collector.collect()
    finally:
        await drop_dual_test_databases(orm_db, ohlcv_db)
```

---

## Additional Resources

### Documentation
- **CLAUDE.md** - Core architectural principles and development guidelines
- **README.md** - Project overview and quick start
- **examples/** - Working code examples

### Related Libraries
- [fullon_orm](https://github.com/ingmarAvocado/fullon_orm) - ORM and database operations
- [fullon_exchange](https://github.com/ingmarAvocado/fullon_exchange) - Exchange integration
- [fullon_ohlcv](https://github.com/ingmarAvocado/fullon_ohlcv) - OHLCV data storage
- [fullon_cache](https://github.com/ingmarAvocado/fullon_cache) - Redis caching
- [fullon_log](https://github.com/ingmarAvocado/fullon_log) - Structured logging

### Support
- GitHub Issues: Report bugs and feature requests
- Examples: See `examples/` directory for working code

---

## Appendix: Quick Reference Commands

### Development Commands

```bash
# Install dependencies
poetry install

# Run tests
poetry run pytest

# Run daemon
poetry run python -m fullon_ohlcv_service.daemon

# Run examples
poetry run python examples/ohlcv_collection_example.py
poetry run python examples/trade_collection_example.py
poetry run python examples/run_example_pipeline.py

# Setup demo database
poetry run python examples/demo_data.py --setup

# Cleanup demo database
poetry run python examples/demo_data.py --cleanup <db_name>
```

### Testing Commands

```bash
# Unit tests only
poetry run pytest tests/unit

# Integration tests
poetry run pytest tests/integration

# With coverage
poetry run pytest --cov=fullon_ohlcv_service

# Specific test file
poetry run pytest tests/test_daemon.py

# Specific test
poetry run pytest tests/test_daemon.py::test_daemon_startup
```

### Database Commands

```bash
# Create test databases
poetry run python examples/demo_data.py --setup

# Drop test databases
poetry run python examples/demo_data.py --cleanup fullon_ohlcv_test_abc123

# Full workflow (setup → test → cleanup)
poetry run python examples/demo_data.py --run-all
```

---

**Document Version**: 1.0.0
**Last Updated**: 2025-10-20
**Maintained By**: fullon_ohlcv_service development team
