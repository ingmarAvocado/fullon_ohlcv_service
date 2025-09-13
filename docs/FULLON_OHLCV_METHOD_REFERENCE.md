# 🔧 Method Reference

Quick reference of available methods based on examples.

## TradeRepository Methods

```python
from fullon_ohlcv.repositories.ohlcv import TradeRepository
```

### Initialization
- `TradeRepository(exchange, symbol, test=True)` - Create repository
- `async with TradeRepository(...) as repo:` - Context manager (recommended)
- `await repo.initialize()` - Manual initialization
- `await repo.close()` - Manual cleanup

### Save Data
- `await repo.save_trades(trades: List[Trade]) -> bool` - Save trade list

### Query Data
- `await repo.get_recent_trades(limit=100) -> List[Trade]` - Get recent trades
- `await repo.get_trades_in_range(start, end, limit=10) -> List[Trade]` - Get trades by time range

### Timestamps
- `await repo.get_oldest_timestamp() -> datetime` - Get oldest trade timestamp
- `await repo.get_latest_timestamp() -> datetime` - Get latest trade timestamp

## CandleRepository Methods

```python
from fullon_ohlcv.repositories.ohlcv import CandleRepository
```

### Initialization
- `CandleRepository(exchange, symbol, test=True)` - Create repository
- `async with CandleRepository(...) as repo:` - Context manager (recommended)
- `await repo.initialize()` - Manual initialization
- `await repo.close()` - Manual cleanup

### Save Data  
- `await repo.save_candles(candles: List[Candle]) -> bool` - Save candle list

### Timestamps
- `await repo.get_oldest_timestamp() -> datetime` - Get oldest candle timestamp
- `await repo.get_latest_timestamp() -> datetime` - Get latest candle timestamp

## TimeseriesRepository Methods

```python
from fullon_ohlcv.repositories.ohlcv import TimeseriesRepository
```

### Initialization
- `TimeseriesRepository(exchange, symbol, test=True)` - Create repository
- `async with TimeseriesRepository(...) as repo:` - Context manager (recommended)
- `await repo.initialize()` - Manual initialization
- `await repo.close()` - Manual cleanup

### OHLCV Data Generation
- `await repo.fetch_ohlcv(start_time, end_time, timeframe="1m", limit=None) -> List[Candle]` - Generate OHLCV candles from existing trade data

### Timestamps
- `await repo.get_oldest_timestamp() -> datetime` - Get oldest data timestamp
- `await repo.get_latest_timestamp() -> datetime` - Get latest data timestamp

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
    success = await repo.save_trades(trades)
```

### Manual Management
```python
repo = CandleRepository("binance", "ETH/USDT", test=True)
await repo.initialize()
try:
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

That's all the methods shown in examples! See example files for complete usage.