# 🔧 Method Reference

Quick reference of available methods based on examples.

## TradeRepository Methods

```python
from fullon_ohlcv.repositories.ohlcv import TradeRepository
```

### Initialization
- `TradeRepository(exchange, symbol, test=True)` - Create repository
- `async with TradeRepository(...) as repo:` - Context manager (recommended)
- `await repo.initialize()` - Initialize connection, schema, and TimescaleDB extension
- `await repo.init_symbol(main="view")` - Create all database tables/views for symbol
- `await repo.close()` - Manual cleanup

**init_symbol() parameters**:
- `main="view"` (default) - OHLCV alias points to continuous aggregate (fastest)
- `main="candles"` - OHLCV alias points to candles table
- `main="trades"` - OHLCV alias points to trades table

**What init_symbol() creates**:
1. `{symbol}_trades` - Raw trade data table (TimescaleDB hypertable)
2. `{symbol}_candles1m` - 1-minute candles table (TimescaleDB hypertable)
3. `{symbol}_candles1m_view` - Continuous aggregate materialized view
4. `{symbol}_ohlcv` - Alias view pointing to main source

### Save Data
- `await repo.save_trades(trades: List[Trade]) -> bool` - Save trade list

### Query Data
- `await repo.get_recent_trades(limit=100) -> List[Trade]` - Get recent trades
- `await repo.get_trades_in_range(start, end, limit=10) -> List[Trade]` - Get trades by time range

### Timestamps
- `await repo.get_oldest_timestamp() -> Optional[datetime]` - Get oldest trade timestamp
- `await repo.get_latest_timestamp() -> Optional[datetime]` - Get latest trade timestamp

## CandleRepository Methods

```python
from fullon_ohlcv.repositories.ohlcv import CandleRepository
```

### Initialization
- `CandleRepository(exchange, symbol, test=True)` - Create repository
- `async with CandleRepository(...) as repo:` - Context manager (recommended)
- `await repo.initialize()` - Initialize connection, schema, and TimescaleDB extension
- `await repo.init_symbol(main="candles")` - Create all database tables/views for symbol (see TradeRepository for details)
- `await repo.close()` - Manual cleanup

### Save Data  
- `await repo.save_candles(candles: List[Candle]) -> bool` - Save candle list

### Timestamps
- `await repo.get_oldest_timestamp() -> Optional[arrow.Arrow]` - Get oldest candle timestamp
- `await repo.get_latest_timestamp() -> Optional[arrow.Arrow]` - Get latest candle timestamp

## TimeseriesRepository Methods

```python
from fullon_ohlcv.repositories.ohlcv import TimeseriesRepository
import arrow
```

### Initialization
- `TimeseriesRepository(exchange, symbol, test=True)` - Create repository
- `async with TimeseriesRepository(...) as repo:` - Context manager (recommended)
- `await repo.initialize()` - Initialize connection, schema, and TimescaleDB extension
- `await repo.init_symbol(main="view")` - Create all database tables/views for symbol (see TradeRepository for details)
- `await repo.close()` - Manual cleanup

### OHLCV Data Generation
- `await repo.fetch_ohlcv(compression: int, period: str, fromdate: arrow.Arrow, todate: arrow.Arrow) -> List[tuple]` - Generate OHLCV candles from existing trade data

### Timestamps
- `await repo.get_oldest_timestamp() -> Optional[arrow.Arrow]` - Get oldest data timestamp
- `await repo.get_latest_timestamp() -> Optional[arrow.Arrow]` - Get latest data timestamp

## Utilities

```python
from fullon_ohlcv.utils import install_uvloop
```

### Performance
- `install_uvloop()` - Install uvloop for better async performance (call before asyncio.run())

## Models

```python
from fullon_ohlcv.models import Trade, Candle
```

### Trade Model
```python
Trade(
    timestamp=datetime.now(timezone.utc),
    price=50000.0,
    volume=0.1,
    side="BUY",      # "BUY" or "SELL"
    type="MARKET"    # "MARKET" or "LIMIT"
)
```

### Candle Model
```python
Candle(
    timestamp=datetime.now(timezone.utc),
    open=3000.0,
    high=3010.0,
    low=2995.0,
    close=3005.0,
    vol=150.5
)
```

## Common Patterns

### Context Manager (Recommended)
```python
async with TradeRepository("binance", "BTC/USDT", test=True) as repo:
    # Initialize symbol tables first
    await repo.init_symbol(main="view")

    # Then save/query data
    success = await repo.save_trades(trades)
```

### Manual Management
```python
repo = CandleRepository("binance", "ETH/USDT", test=True)
await repo.initialize()
try:
    # Initialize symbol tables
    await repo.init_symbol(main="candles")

    # Then save/query data
    success = await repo.save_candles(candles)
finally:
    await repo.close()
```

### Performance Setup
```python
from fullon_ohlcv.utils import install_uvloop
install_uvloop()  # Call before asyncio.run() for better performance
asyncio.run(main())
```

### Bulk Operations
```python
# Save multiple trades/candles at once
trades = [Trade(...), Trade(...), Trade(...)]
success = await repo.save_trades(trades)
```

### TimeseriesRepository OHLCV Generation
```python
import arrow
from datetime import datetime, timezone, timedelta

# Generate 1-minute OHLCV candles from trade data
end_time = datetime.now(timezone.utc)
start_time = end_time - timedelta(hours=24)

async with TimeseriesRepository("binance", "BTC/USDT", test=True) as repo:
    # Initialize symbol tables
    await repo.init_symbol(main="view")

    # Fetch OHLCV data
    ohlcv_data = await repo.fetch_ohlcv(
        compression=1,
        period="minutes",
        fromdate=arrow.get(start_time),
        todate=arrow.get(end_time)
    )
    # Returns list of tuples: (timestamp, open, high, low, close, volume)
    for ts, open_price, high_price, low_price, close_price, volume in ohlcv_data:
        print(f"Candle: {ts} O:{open_price} H:{high_price} L:{low_price} C:{close_price} V:{volume}")
```

That's all the methods shown in examples! See example files for complete usage.