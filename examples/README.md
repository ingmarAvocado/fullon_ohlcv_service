# fullon_ohlcv_service Examples

This directory contains practical examples demonstrating the fullon_ohlcv_service functionality.

## Available Examples

1. **run_example_pipeline.py** - Complete demonstration pipeline showing:
   - Service initialization
   - OHLCV collection setup
   - Trade streaming configuration
   - Data retrieval and validation
   - Health monitoring integration

2. **demo_data.py** - Demo data generation and testing utilities for development

## Examples

### 1. Simple Daemon Control (`simple_daemon_control.py`)

Shows how to control the OHLCV daemon with 3 simple calls:

```python
from fullon_ohlcv_service.ohlcv.manager import OhlcvManager

# Create daemon
daemon = OhlcvManager()

# 1. START - daemon queries fullon_orm and starts collectors  
await daemon.start()

# 2. STATUS - check what's running
status = await daemon.status()

# 3. STOP - clean shutdown
await daemon.stop()
```

**Key Features:**
- Self-contained daemon that queries fullon_orm internally
- Suitable for master daemon integration
- Clean start/stop lifecycle management

### 2. Data Retrieval (`data_retrieval.py`)

Shows how to retrieve OHLCV and trade data using fullon_ohlcv:

```python
from fullon_ohlcv import OHLCVDatabase

# Get latest 100 candles
async with OHLCVDatabase(exchange="kraken", symbol="BTC/USD") as db:
    latest_bars = await db.get_recent_candles(limit=100)

# Get historical data by time range
historical_bars = await db.get_candles_by_timerange(
    start_time=start_time,
    end_time=end_time
)

# Get trade data
latest_trades = await db.get_recent_trades(limit=1000)
```

**Examples Include:**
- Latest bars/candles
- Historical time range queries
- Individual trade data
- Multi-symbol data fetching
- Data quality checks
- Real-time data freshness

### 3. WebSocket Callbacks (`websocket_callbacks.py`)

Shows WebSocket subscriptions with callbacks for real-time data:

```python
from fullon_exchange.queue import ExchangeQueue

# Initialize exchange
await ExchangeQueue.initialize_factory()

# Create exchange object following modern fullon_exchange pattern
def create_example_exchange(exchange_name: str, exchange_id: int = 1):
    from fullon_orm.models import CatExchange, Exchange

    cat_exchange = CatExchange()
    cat_exchange.name = exchange_name
    cat_exchange.id = 1

    exchange = Exchange()
    exchange.ex_id = exchange_id
    exchange.uid = "example_account"
    exchange.test = False
    exchange.cat_exchange = cat_exchange

    return exchange

exchange_obj = create_example_exchange("kraken", exchange_id=1)

# Create credential provider following modern pattern
def credential_provider(exchange_obj):
    try:
        from fullon_credentials import fullon_credentials
        secret, api_key = fullon_credentials(ex_id=exchange_obj.ex_id)
        return api_key, secret
    except ValueError:
        return "", ""  # Empty for public data

handler = await ExchangeQueue.get_websocket_handler(exchange_obj, credential_provider)

# Define callback
async def on_ohlcv_data(ohlcv_data):
    # Save to database
    async with OHLCVDatabase(exchange=exchange, symbol=symbol) as db:
        await db.store_candles([candle_data])

# Subscribe
await handler.connect()
await handler.subscribe_ohlcv(symbol=symbol, callback=on_ohlcv_data, interval="1m")
```

**Examples Include:**
- Direct OHLCV candle callbacks (for exchanges that support it)
- Trade-to-bar conversion (for exchanges that only provide trades)
- WebSocket reconnection logic
- Multiple symbol management
- Trade data caching

**Trade-to-Bar Conversion:**
- Accumulates individual trades into 1-minute OHLCV bars
- Based on legacy code patterns
- Saves both individual trades and converted bars

### 4. Historic OHLCV REST (`historic_ohlcv_rest.py`)

Shows how to fetch historical data using REST APIs:

```python
# Fetch historical candles
candles = await handler.get_candles(symbol=symbol, since=since, limit=1000)

# Convert and save
formatted_candles = []
for candle in candles:
    formatted_candles.append({
        'timestamp': arrow.get(candle[0]).isoformat(),
        'open': float(candle[1]),
        'high': float(candle[2]),
        'low': float(candle[3]),  
        'close': float(candle[4]),
        'volume': float(candle[5]),
    })

await db.store_candles(formatted_candles)
```

**Examples Include:**
- Initial historical data collection
- Historical trade data fetching
- Backfilling missing data gaps
- Bulk fetch for multiple symbols
- Smart catch-up (REST for old data, WebSocket for real-time)

## Architecture Patterns

### Daemon Design Philosophy

The examples follow the **"Smart Service, Dumb Master"** pattern:

- **Master Daemon**: Simple lifecycle management (start/stop/status only)
- **OHLCV Service**: Self-contained, queries fullon_orm internally, manages all collectors

### Data Flow

1. **Historic Collection** (REST API):
   ```
   fullon_orm (symbols) → REST API → fullon_ohlcv (database)
   ```

2. **Real-time Collection** (WebSocket):
   ```
   WebSocket → Callback → fullon_cache (trades) → fullon_ohlcv (bars)
   ```

3. **Trade-to-Bar Conversion**:
   ```
   Individual Trades → 1-minute Accumulation → OHLCV Bars → Database
   ```

### Key Components Integration

- **fullon_orm**: Query active symbols and exchanges
- **fullon_exchange**: Unified API for all exchanges (REST + WebSocket)
- **fullon_ohlcv**: Database storage for candles and trades
- **fullon_cache**: Redis caching for real-time coordination
- **fullon_log**: Structured logging with component isolation

## Running the Examples

### Prerequisites

```bash
# Install dependencies
poetry install

# Set up environment
cp .env.example .env
# Configure your database and Redis settings
```

### Environment Variables

```env
# Database
DB_USER=postgres_user
DB_PASSWORD=postgres_password
DB_NAME=fullon2

# Cache (Redis)  
CACHE_HOST=localhost
CACHE_PORT=6379
CACHE_DB=0

# Exchange APIs (optional for REST examples)
KRAKEN_API_KEY=your_key
KRAKEN_SECRET=your_secret
```

### Run Examples

```bash
# Run the complete example pipeline
poetry run python examples/run_example_pipeline.py

# Generate demo data for testing
poetry run python examples/demo_data.py
```

### Run All Examples Sequentially

```bash
# Run all examples in sequence
for script in examples/*.py; do
    echo "=== Running $script ==="
    python $script
    echo
done
```

## Common Usage Patterns

### Production Daemon Setup

```python
class MasterDaemon:
    async def start_ohlcv(self):
        self.ohlcv_service = OhlcvManager()
        await self.ohlcv_service.start()
        
    async def stop_ohlcv(self):
        await self.ohlcv_service.stop()
        
    async def ohlcv_status(self):
        return await self.ohlcv_service.status()
```

### Data Collection Pipeline

```python
# 1. Initial historic collection
await fetch_historic_candles()

# 2. Start real-time collection  
await start_websocket_collectors()

# 3. Monitor and retrieve data
latest_data = await get_latest_bars()
```

### Error Handling

All examples include:
- Exponential backoff for reconnections
- Graceful error handling and recovery
- Proper resource cleanup
- Structured logging for debugging

## Testing

The examples can be run in test mode:

```python
# Most examples support test mode for faster execution
await daemon.start(test=True)  # Runs briefly then exits
```

## Integration with Master Daemon

These examples show how the OHLCV service integrates with a larger trading system:

1. Master daemon queries available services
2. Starts/stops OHLCV service as needed
3. OHLCV service handles all domain logic internally
4. Clean separation of concerns

The OHLCV service is designed to be:
- **Self-contained**: Knows what data it needs
- **Observable**: Provides status and health metrics  
- **Reliable**: Handles errors and recovers gracefully
- **Efficient**: Uses async patterns throughout

## Next Steps

After running these examples:

1. Adapt the patterns to your specific exchanges and symbols
2. Configure environment variables for your setup
3. Set up monitoring and alerting for production use
4. Consider scaling patterns for high-frequency trading

See `CLAUDE.md` for detailed development patterns and best practices.