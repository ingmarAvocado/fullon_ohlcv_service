# 🤖 LLM Guide: Fullon OHLCV

Quick guide for LLMs to use this PostgreSQL/TimescaleDB library for trading data.

## 📦 Installation

```bash
poetry add git+ssh://git@github.com/ingmarAvocado/fullon_ohlcv.git
```

## 🎯 Usage Examples

### Save Trade Data

```python
import asyncio
from datetime import datetime, timezone, timedelta
from fullon_ohlcv.repositories.ohlcv import TradeRepository
from fullon_ohlcv.models import Trade

async def main():
    # Initialize with test=True for testing
    async with TradeRepository("binance", "BTC/USDT", test=True) as repo:
        # Create trades
        trades = [
            Trade(
                timestamp=datetime.now(timezone.utc),
                price=50000.0,
                volume=0.1,
                side="BUY",
                type="MARKET"
            ),
            Trade(
                timestamp=datetime.now(timezone.utc) + timedelta(seconds=1),
                price=50050.0,
                volume=0.2,
                side="SELL",
                type="LIMIT"
            )
        ]
        
        # Save trades
        success = await repo.save_trades(trades)
        
        # Query recent trades
        recent = await repo.get_recent_trades(limit=10)
        print(f"Found {len(recent)} trades")

if __name__ == "__main__":
    # Setup uvloop for performance
    from fullon_ohlcv.utils import install_uvloop
    install_uvloop()
    
    asyncio.run(main())
```

### Save Candle Data

```python
import asyncio
from datetime import datetime, timezone, timedelta
from fullon_ohlcv.repositories.ohlcv import CandleRepository
from fullon_ohlcv.models import Candle

async def main():
    # Use context manager (recommended)
    async with CandleRepository("binance", "ETH/USDT", test=True) as repo:
        # Create candles
        base_time = datetime.now(timezone.utc)
        candles = [
            Candle(
                timestamp=base_time,
                open=3000.0,
                high=3010.0,
                low=2995.0,
                close=3005.0,
                vol=150.5
            ),
            Candle(
                timestamp=base_time + timedelta(minutes=1),
                open=3005.0,
                high=3020.0,
                low=3000.0,
                close=3015.0,
                vol=200.0
            )
        ]
        
        # Save candles
        success = await repo.save_candles(candles)
        
        # Get timestamp info
        oldest = await repo.get_oldest_timestamp()
        latest = await repo.get_latest_timestamp()
        print(f"Time range: {oldest} to {latest}")

if __name__ == "__main__":
    # Setup uvloop for performance
    from fullon_ohlcv.utils import install_uvloop
    install_uvloop()
    
    asyncio.run(main())
```

### TimeseriesRepository (OHLCV Aggregation)

```python
import asyncio
from datetime import datetime, timezone, timedelta
from fullon_ohlcv.repositories.ohlcv import TimeseriesRepository

async def main():
    async with TimeseriesRepository("binance", "BTC/USDT", test=True) as repo:
        # Generate OHLCV candles from existing trade data
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=1)
        
        ohlcv_data = await repo.fetch_ohlcv(
            start_time=start_time,
            end_time=end_time,
            timeframe="1m",
            limit=10
        )
        
        print(f"Generated {len(ohlcv_data)} OHLCV candles")

if __name__ == "__main__":
    from fullon_ohlcv.utils import install_uvloop
    install_uvloop()
    asyncio.run(main())
```

## 🔑 Key Points

1. **Always async** - Use `async`/`await` for all operations
2. **Use context managers** - `async with Repository(...) as repo:` (recommended) or manual `await repo.initialize()` + `await repo.close()`
3. **UTC timestamps** - Always use `datetime.now(timezone.utc)`
4. **Test mode** - Use `test=True` for testing to avoid production data
5. **Performance** - Call `install_uvloop()` before `asyncio.run()` for better performance

## 📊 What Gets Created

For `TradeRepository("binance", "BTC/USDT")`:
- Schema: `binance` 
- Table: `binance.BTC_USDT_trades`
- TimescaleDB hypertable for performance

## 🔗 Quick References

- **Method Reference**: `docs/METHOD_REFERENCE.md` - All available methods
- **Examples**: 
  - `src/fullon_ohlcv/examples/trade_repository_example.py`
  - `src/fullon_ohlcv/examples/candle_repository_example.py`  
  - `src/fullon_ohlcv/examples/timeseries_repository_example.py`
  - `src/fullon_ohlcv/examples/run_all.py` - Run all examples at once

That's it! Check METHOD_REFERENCE.md for all methods, use examples as templates.