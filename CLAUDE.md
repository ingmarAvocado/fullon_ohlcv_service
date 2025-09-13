# `fullon_ohlcv_service` - LLM Development Guide

## 1. Core Mission & Principles

- **Mission**: Simple async daemon for OHLCV/trade collection using fullon ecosystem integration.
- **Architecture (LRRS)**:
    - **Little**: ~300-500 lines of integration code, NOT a data collection framework
    - **Responsible**: Coordinate collectors, leverage fullon_exchange/fullon_ohlcv for heavy lifting
    - **Reusable**: Standard fullon ecosystem patterns, database-driven configuration
    - **Separate**: Clean integration layer, zero reimplementation of existing fullon functionality

## 2. Critical fullon Ecosystem Dependencies

**THESE LIBRARIES DO THE HEAVY LIFTING - USE THEM, DON'T REINVENT:**

### **fullon_orm**: Database Operations & Configuration
```python
from fullon_orm.database_context import DatabaseContext
from fullon_orm.models import User, Exchange, Symbol

# Get exchanges/symbols from database (NO hardcoded lists)
async with DatabaseContext() as db:
    exchanges = await db.exchanges.get_user_exchanges(user_id=1)
    symbols = await db.symbols.get_by_exchange_id(cat_ex_id)
```

### **fullon_exchange**: WebSocket Data Collection & Exchange APIs
```python
from fullon_exchange.queue import ExchangeQueue

# Handles ALL websocket connections, reconnection, error recovery
await ExchangeQueue.initialize_factory()
handler = await ExchangeQueue.get_handler("kraken", "ohlcv_account")
candles = await handler.get_ohlcv("BTC/USD", "1m", limit=100)
```

### **fullon_ohlcv**: Database Storage for OHLCV/Trade Data
```python
from fullon_ohlcv.repositories.ohlcv import CandleRepository, TradeRepository
from fullon_ohlcv.models import Candle, Trade

# Database storage with proper models
async with CandleRepository("kraken", "BTC/USD", test=False) as repo:
    success = await repo.save_candles(candles)
```

### **fullon_cache**: Redis Operations & Process Health
```python
from fullon_cache import ProcessCache, OHLCVCache

# Health monitoring and real-time cache updates
async with ProcessCache() as cache:
    await cache.update_process("ohlcv_service", "daemon", "running")
```

### **fullon_log**: Structured Component Logging
```python
from fullon_log import get_component_logger

logger = get_component_logger("fullon.ohlcv.collector.kraken.BTCUSD")
logger.info("OHLCV collected", symbol="BTC/USD", count=100)
```

## 3. Simplified Architecture (Integration Layer Only)

```
fullon_ohlcv_service/
├── ohlcv/
│   ├── collector.py    # Simple fullon_exchange + fullon_ohlcv integration
│   └── manager.py      # Coordinate multiple collectors
├── trade/  
│   ├── collector.py    # Simple fullon_exchange + fullon_ohlcv integration
│   └── manager.py      # Coordinate multiple collectors  
├── daemon.py           # Read from fullon_orm, start collectors
└── config.py           # Basic environment settings
```

**Total Expected Code: ~300-500 lines of integration glue**

## 4. Simple Integration Patterns (Using fullon Libraries)

### A. Database-Driven Configuration (fullon_orm)

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

### B. Basic OHLCV Collector (fullon_exchange + fullon_ohlcv)

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
            # Use fullon_exchange for data collection
            handler = await ExchangeQueue.get_handler(self.exchange, "ohlcv_account")
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

### C. Health Monitoring (fullon_cache)

```python
from fullon_cache import ProcessCache

class OhlcvServiceDaemon:
    async def update_health_status(self, status: str):
        """Update daemon health using fullon_cache"""
        async with ProcessCache() as cache:
            await cache.update_process("ohlcv_service", "daemon", status)
```

### D. Basic Daemon Coordination

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
                # Start collection...
```

## 5. Simple Service Architecture

### A. Basic Daemon (Like ticker service)

```python
from fullon_orm.database_context import DatabaseContext
from fullon_ohlcv_service.ohlcv.collector import OhlcvCollector

class OhlcvDaemon:
    async def start(self):
        # Get targets from database (like ticker service)
        targets = await get_collection_targets()
        
        # Start collectors for each exchange/symbol
        for exchange, symbols in targets.items():
            for symbol in symbols:
                collector = OhlcvCollector(symbol, exchange)
                await collector.start_collection()

# Usage: python -m fullon_ohlcv_service.daemon
```

### B. Simple Configuration

```python
# .env file - basic settings only
LOG_LEVEL=INFO

# Database/cache connection handled by fullon ecosystem
# Exchange/symbol configuration read from fullon_orm database
```

## 6. Foundation Issues Needed (#1-10)

**The service needs these basic issues before advanced features:**

1. **Issue #1**: Basic OhlcvCollector (✅ DONE - see implementation)
2. **Issue #2**: Basic TradeCollector
3. **Issue #3**: Simple Manager coordination
4. **Issue #4**: Basic daemon with database-driven configuration  
5. **Issue #5**: Health monitoring via ProcessCache
6. **Issue #6**: Configuration management
7. **Issue #7**: Basic error handling
8. **Issue #8**: Integration testing
9. **Issue #9**: Examples creation
10. **Issue #10**: Documentation cleanup

**Current Issues #11-20 should be CLOSED until foundation is complete.**

## 7. What You DON'T Need To Build

**fullon_exchange already provides:**
- WebSocket connections and management
- Auto-reconnection with exponential backoff  
- Exchange API abstraction
- Error handling for network/exchange issues

**fullon_ohlcv already provides:**
- CandleRepository, TradeRepository
- Database models (Candle, Trade)
- Database connection management

**fullon_orm already provides:**
- Exchange and symbol configuration
- Database-driven symbol discovery

**Your job: ~300-500 lines of simple integration code**

## 8. Development Approach

### A. Follow ticker service patterns:
- Database-driven symbol discovery
- Simple integration classes  
- Examples-driven development
- Basic health monitoring

### B. Implementation order:
1. Create Issues #1-10 (foundation)
2. Implement basic collectors
3. Add simple daemon coordination
4. Create working examples
5. THEN consider advanced features

### C. Keep it simple:
- ~50-100 lines per collector class
- ~100 lines for daemon coordination
- Focus on integration, not innovation

## 9. Key Rules

### A. Use fullon ecosystem - don't reinvent:
```python
# ✅ CORRECT - Use fullon_exchange
handler = await ExchangeQueue.get_handler("kraken", "ohlcv_account") 

# ❌ INCORRECT - Don't build your own exchange connections
# websocket.connect("wss://kraken.com/...")
```

### B. Database-driven configuration:
```python
# ✅ CORRECT - Read from fullon_orm database
async with DatabaseContext() as db:
    exchanges = await db.exchanges.get_user_exchanges(user_id=1)

# ❌ INCORRECT - Hardcoded lists  
# exchanges = ["kraken", "binance"]
```

## 10. Bottom Line

**This service should be simple integration code, not a framework.**

- Follow fullon_ticker_service patterns
- Use fullon ecosystem libraries for heavy lifting
- Keep collectors under 100 lines each
- Database-driven configuration only
- Create foundation Issues #1-10 first
- Close premature Issues #11-20

**Stop over-engineering. Start integrating.**