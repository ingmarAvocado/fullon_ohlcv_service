# OHLCV Library - Complete Usage Example

This example demonstrates the complete workflow for initializing a symbol and saving both trades and OHLCV (candle) data.

## Overview

The typical workflow is:
1. Create a repository instance (TradeRepository or CandleRepository)
2. Initialize the symbol with `init_symbol()` - creates database tables and views
3. Save trades using `save_trades()`
4. Save candles using `save_candles()`

## Complete Example Code

```python
import asyncio
from datetime import datetime, timezone, timedelta
from fullon_ohlcv.repositories.ohlcv import TradeRepository, CandleRepository
from fullon_ohlcv.models import Trade, Candle

async def main():
    # Step 1: Initialize a TradeRepository for a symbol
    # Parameters: exchange name, symbol (with / separator), test=True for test database
    trade_repo = TradeRepository("binance", "BTC/USDT", test=True)
    await trade_repo.initialize()  # Must call initialize() before using repository

    # Step 2: Initialize the symbol (creates tables, hypertables, views)
    # This is idempotent - safe to call multiple times
    success = await trade_repo.init_symbol()
    print(f"Symbol initialized: {success}")

    # Step 3: Create and save trades
    base_time = datetime.now(timezone.utc)
    trades = [
        Trade(
            timestamp=base_time,
            price=50000.0,
            volume=0.1,
            side="BUY",
            type="MARKET"
        ),
        Trade(
            timestamp=base_time + timedelta(seconds=1),
            price=50050.0,
            volume=0.2,
            side="SELL",
            type="LIMIT"
        ),
        Trade(
            timestamp=base_time + timedelta(seconds=2),
            price=50025.0,
            volume=0.15,
            side="BUY",
            type="MARKET"
        )
    ]

    # Save trades to database
    success = await trade_repo.save_trades(trades)
    print(f"Trades saved: {success}")

    # Close the trade repository
    await trade_repo.close()

    # Step 4: Initialize a CandleRepository for the same symbol
    candle_repo = CandleRepository("binance", "BTC/USDT", test=True)
    await candle_repo.initialize()

    # Initialize symbol for candles (creates candles table)
    success = await candle_repo.init_symbol()
    print(f"Candle symbol initialized: {success}")

    # Step 5: Create and save OHLCV candles
    candle_time = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    candles = [
        Candle(
            timestamp=candle_time,
            open=50000.0,
            high=50100.0,
            low=49900.0,
            close=50050.0,
            vol=10.5  # Volume
        ),
        Candle(
            timestamp=candle_time + timedelta(minutes=1),
            open=50050.0,
            high=50150.0,
            low=50000.0,
            close=50100.0,
            vol=12.3
        ),
        Candle(
            timestamp=candle_time + timedelta(minutes=2),
            open=50100.0,
            high=50200.0,
            low=50050.0,
            close=50150.0,
            vol=15.7
        )
    ]

    # Save candles to database
    success = await candle_repo.save_candles(candles)
    print(f"Candles saved: {success}")

    # Close the candle repository
    await candle_repo.close()

# Run the async function
if __name__ == "__main__":
    asyncio.run(main())
```

## Using Context Managers (Recommended)

The recommended approach is to use async context managers for automatic resource cleanup:

```python
import asyncio
from datetime import datetime, timezone, timedelta
from fullon_ohlcv.repositories.ohlcv import TradeRepository, CandleRepository
from fullon_ohlcv.models import Trade, Candle

async def main():
    # Using context manager - automatically initializes and closes
    async with TradeRepository("binance", "BTC/USDT", test=True) as trade_repo:
        # Initialize symbol
        await trade_repo.init_symbol()

        # Create and save trades
        base_time = datetime.now(timezone.utc)
        trades = [
            Trade(
                timestamp=base_time,
                price=50000.0,
                volume=0.1,
                side="BUY",
                type="MARKET"
            )
        ]
        await trade_repo.save_trades(trades)

    # Using context manager for candles
    async with CandleRepository("binance", "BTC/USDT", test=True) as candle_repo:
        # Initialize symbol
        await candle_repo.init_symbol()

        # Create and save candles
        candle_time = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        candles = [
            Candle(
                timestamp=candle_time,
                open=50000.0,
                high=50100.0,
                low=49900.0,
                close=50050.0,
                vol=10.5
            )
        ]
        await candle_repo.save_candles(candles)

if __name__ == "__main__":
    asyncio.run(main())
```

## Important Notes

### Symbol Initialization (`init_symbol()`)
- **Always call `init_symbol()` before saving data** - This creates the necessary database tables and views
- **Idempotent** - Safe to call multiple times, won't cause errors
- Creates:
  - The symbol's table (e.g., `binance.BTC_USDT_trades` or `binance.BTC_USDT_candles`)
  - TimescaleDB hypertable for time-series optimization
  - OHLCV materialized view (for trades)
  - Necessary indexes

### Trade Data Model
- `timestamp`: datetime object with timezone (must be timezone-aware)
- `price`: float - trade price
- `volume`: float - trade volume/amount
- `side`: string - "BUY" or "SELL"
- `type`: string - "MARKET", "LIMIT", etc.

### Candle (OHLCV) Data Model
- `timestamp`: datetime object with timezone (must be timezone-aware)
- `open`: float - opening price
- `high`: float - highest price in period
- `low`: float - lowest price in period
- `close`: float - closing price
- `vol`: float - total volume in period

### Database Configuration
- `test=True` - Uses test database (configured in environment)
- `test=False` - Uses production database
- Exchange name becomes the PostgreSQL schema (e.g., "binance" schema)
- Symbol becomes the table prefix (e.g., "BTC_USDT_trades")

### Performance Tips
- Use bulk operations - pass lists of trades/candles to `save_trades()` and `save_candles()`
- TimescaleDB automatically optimizes time-series queries
- Use context managers to ensure proper cleanup

## Querying Data

After saving data, you can query it:

```python
# Query trades
recent_trades = await trade_repo.get_recent_trades(limit=10)
oldest = await trade_repo.get_oldest_timestamp()
latest = await trade_repo.get_latest_timestamp()

# Query trades in time range
start_time = datetime.now(timezone.utc) - timedelta(hours=1)
end_time = datetime.now(timezone.utc)
range_trades = await trade_repo.get_trades_in_range(start_time, end_time, limit=100)

# Query candle timestamps
oldest_candle = await candle_repo.get_oldest_timestamp()
latest_candle = await candle_repo.get_latest_timestamp()
```

## TimeseriesRepository for OHLCV Views

For querying aggregated OHLCV data from trades, use `TimeseriesRepository`:

```python
from fullon_ohlcv.repositories.ohlcv import TimeseriesRepository
import arrow

async with TimeseriesRepository("binance", "BTC/USDT", test=True) as ts_repo:
    # Fetch OHLCV data with specific compression and period
    fromdate = arrow.get(datetime.now(timezone.utc) - timedelta(hours=1))
    todate = arrow.get(datetime.now(timezone.utc))

    # Get 1-minute candles
    data = await ts_repo.fetch_ohlcv(
        compression=1,
        period="minute",
        fromdate=fromdate,
        todate=todate
    )

    # data is a list of tuples: (timestamp, open, high, low, close, volume)
    for ts, o, h, l, c, v in data:
        print(f"{ts}: O={o} H={h} L={l} C={c} V={v}")
```
