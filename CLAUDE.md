# `fullon_ohlcv_service` - LLM Development Guide

## 1. Core Mission & Principles

- **Mission**: Async daemon service for collecting OHLCV candles and individual trades from cryptocurrency exchanges, storing them in fullon_ohlcv database.
- **Architecture (LRRS)**:
    - **Little**: Two focused modules: OHLCV candle collection and trade data collection
    - **Responsible**: Reliable data collection with proper error handling and recovery
    - **Reusable**: Can be integrated into larger trading systems or run standalone
    - **Separate**: Clean separation between candle and trade collection logic

## 2. Key Dependencies

This service integrates with the fullon ecosystem:

- **fullon_log**: Structured logging with component isolation
- **fullon_exchange**: Unified exchange API with WebSocket support
- **fullon_ohlcv**: Database storage for OHLCV and trade data
- **fullon_cache**: Redis caching for real-time data and coordination
- **fullon_orm**: Database models and connection management

## 3. Architecture Overview

```
fullon_ohlcv_service/
├── ohlcv/              # OHLCV 1-minute candle collection
│   ├── manager.py      # Multi-symbol OHLCV coordination
│   └── collector.py    # Single symbol OHLCV collection
├── trade/              # Individual trade data collection  
│   ├── manager.py      # Multi-symbol trade coordination
│   └── collector.py    # Single symbol trade collection
├── daemon.py           # Main service launcher
└── config.py           # Environment configuration
```

## 4. Core Development Patterns

### A. Async/Await First

All operations use async/await patterns, no threading:

```python
from fullon_log import get_component_logger
from fullon_exchange.queue import ExchangeQueue
import asyncio

class OhlcvCollector:
    def __init__(self, symbol: str, exchange: str):
        self.logger = get_component_logger(f"fullon.ohlcv.collector.{exchange}.{symbol}")
        self.symbol = symbol
        self.exchange = exchange
        
    async def start_collection(self):
        """Start async OHLCV collection for this symbol"""
        await ExchangeQueue.initialize_factory()
        try:
            handler = await ExchangeQueue.get_handler(self.exchange, "ohlcv_account")
            await handler.connect()
            
            # Collection logic here
            await self._collect_historical()
            await self._start_realtime()
            
        finally:
            await ExchangeQueue.shutdown_factory()
```

### B. Database Integration

Use fullon_ohlcv for storage:

```python
from fullon_ohlcv import OHLCVDatabase

async def store_candles(self, candles: list):
    """Store OHLCV candles to database"""
    async with OHLCVDatabase(exchange=self.exchange, symbol=self.symbol) as db:
        await db.store_candles(candles)
        self.logger.info("Stored candles", count=len(candles))
```

### C. Cache Integration

Use fullon_cache for coordination:

```python
from fullon_cache import OHLCVCache, ProcessCache

async def update_status(self, status: str):
    """Update collection status in cache"""
    async with ProcessCache() as cache:
        key = f"{self.exchange}:{self.symbol}"
        await cache.update_process(type="ohlcv", key=key, status=status)
```

## 5. Service Architecture

### A. Daemon Launcher

The main daemon can launch either service independently:

```python
# Launch OHLCV service only
python -m fullon_ohlcv_service.daemon --service ohlcv --exchanges kraken,binance

# Launch trade service only  
python -m fullon_ohlcv_service.daemon --service trade --symbols BTC/USD,ETH/USD

# Launch both services
python -m fullon_ohlcv_service.daemon --service both
```

### B. Configuration Management

Environment-based configuration:

```python
# .env file
OHLCV_EXCHANGES=kraken,binance,bitmex
OHLCV_SYMBOLS=BTC/USD,ETH/USD,BTC/EUR
TRADE_COLLECTION_ENABLED=true
LOG_LEVEL=INFO
REDIS_HOST=localhost
POSTGRES_HOST=localhost
```

## 6. Development Workflow

### A. Setup
```bash
cd /home/ingmar/code/fullon_ohlcv_service
poetry install
cp .env.example .env  # Configure your environment
```

### B. Development Commands
```bash
# Run tests
poetry run pytest

# Code formatting
poetry run black .
poetry run ruff check .

# Type checking
poetry run mypy src/

# Run OHLCV service
poetry run python -m fullon_ohlcv_service.daemon --service ohlcv

# Run trade service
poetry run python -m fullon_ohlcv_service.daemon --service trade
```

## 7. Testing Strategy

### A. Integration with Real Services
Tests use real Redis and PostgreSQL databases with proper cleanup:

```python
import pytest
from fullon_ohlcv_service.ohlcv.collector import OhlcvCollector

@pytest.mark.asyncio
async def test_ohlcv_collection(db_context, redis_cache):
    """Test OHLCV collection with real database"""
    collector = OhlcvCollector("BTC/USD", "kraken")
    
    # Test historical data collection
    await collector.collect_historical(days=1)
    
    # Verify data was stored
    async with db_context.ohlcv as db:
        candles = await db.get_recent_candles("BTC/USD", limit=100)
        assert len(candles) > 0
```

### B. Mock External APIs
Use exchange sandbox/testnet for testing:

```python
@pytest.fixture
async def exchange_handler():
    """Provide sandbox exchange handler for testing"""
    await ExchangeQueue.initialize_factory()
    handler = await ExchangeQueue.get_handler("kraken", "test_account")
    handler.config.use_sandbox = True
    yield handler
    await ExchangeQueue.shutdown_factory()
```

## 8. Monitoring and Observability

### A. Structured Logging
```python
self.logger.info(
    "OHLCV collection completed",
    exchange=self.exchange,
    symbol=self.symbol,
    candles_collected=count,
    duration_seconds=elapsed,
    errors=error_count
)
```

### B. Health Checks
```python
async def health_check(self) -> dict:
    """Return service health status"""
    return {
        "service": "ohlcv_collector",
        "status": "healthy" if self._is_collecting else "stopped",
        "last_collection": self._last_collection.isoformat(),
        "symbols_active": len(self._active_symbols)
    }
```

## 9. Error Handling and Recovery

### A. Graceful Degradation
```python
async def _handle_collection_error(self, error: Exception):
    """Handle collection errors with exponential backoff"""
    self.logger.error("Collection error", error=str(error), symbol=self.symbol)
    
    # Exponential backoff
    wait_time = min(300, 2 ** self._error_count)
    await asyncio.sleep(wait_time)
    
    # Retry collection
    await self._retry_collection()
```

### B. Service Recovery
```python
async def _ensure_service_health(self):
    """Monitor and restart failed collectors"""
    for collector in self._collectors.values():
        if not collector.is_healthy():
            self.logger.warning("Restarting unhealthy collector", symbol=collector.symbol)
            await collector.restart()
```

## 10. Key Implementation Notes

### A. Exchange Abstraction
Use fullon_exchange unified interface - never call exchange APIs directly:

```python
# ✅ CORRECT - Use fullon_exchange
from fullon_exchange.queue import ExchangeQueue
handler = await ExchangeQueue.get_handler("kraken", "ohlcv")
candles = await handler.get_ohlcv("BTC/USD", since=timestamp)

# ❌ INCORRECT - Never call exchange APIs directly  
# import kraken_api; kraken_api.get_ohlcv(...)
```

### B. Database Operations
Use fullon_ohlcv database abstraction:

```python
# ✅ CORRECT - Use fullon_ohlcv
from fullon_ohlcv import OHLCVDatabase
async with OHLCVDatabase(exchange="kraken", symbol="BTC/USD") as db:
    await db.store_candles(candles)

# ❌ INCORRECT - Never use direct SQL
# "INSERT INTO ohlcv_data VALUES ..."
```

### C. Configuration
Always use environment variables via config.py:

```python
# ✅ CORRECT
from fullon_ohlcv_service.config import Config
config = Config()
exchanges = config.OHLCV_EXCHANGES

# ❌ INCORRECT - No hardcoded values
# exchanges = ["kraken", "binance"]  # DON'T DO THIS
```

## 11. Production Deployment

### A. Service Management
```bash
# Systemd service
sudo systemctl start fullon-ohlcv-service
sudo systemctl enable fullon-ohlcv-service

# Docker deployment
docker run -d --name fullon-ohlcv fullon-ohlcv-service:latest
```

### B. Monitoring
- Health check endpoints for service monitoring
- Prometheus metrics export
- Log aggregation with structured JSON logs
- Database connection pooling optimization

## 12. Development Checklist

When implementing new features:

- [ ] Use async/await patterns exclusively
- [ ] Implement proper error handling with exponential backoff  
- [ ] Add structured logging with relevant context
- [ ] Write integration tests with real databases
- [ ] Update configuration management
- [ ] Document API changes in this file
- [ ] Verify exchange abstraction usage
- [ ] Test graceful shutdown behavior

This service is designed to be a reliable, high-performance daemon for cryptocurrency data collection, following modern Python async patterns and integrating seamlessly with the fullon ecosystem.