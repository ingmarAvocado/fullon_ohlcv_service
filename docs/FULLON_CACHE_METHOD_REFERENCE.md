# Fullon Cache - Method Reference

**Methods demonstrated in working examples, organized by cache type.**

## BaseCache (All caches inherit from)

```python
async def ping() -> str                    # Test Redis connection
async def test() -> bool                   # Connection health check  
async def prepare_cache() -> None          # Clear all Redis data
```

## TickCache - Real-time Market Data

```python
# Model Creation (as used in examples)
from fullon_orm.models import Symbol, Tick

# Create Symbol objects
symbol = Symbol(symbol="BTC/USDT", cat_ex_id=1, base="BTC", quote="USDT")

# Create Tick objects  
tick = Tick(
    symbol="BTC/USDT",
    exchange="binance", 
    price=50000.0,
    volume=100.0,
    time=time.time(),
    bid=49999.0,
    ask=50001.0,
    last=50000.0
)

# Ticker Operations (fullon_orm models as input/output)
async def set_ticker(tick: Tick) -> bool                              # Set ticker using Tick object only
async def get_ticker(symbol: Symbol, exchange: str) -> Tick | None    # Get ticker for symbol/exchange
async def update_ticker(exchange: str, symbol: str, tick_data: dict) -> bool
async def get_price(symbol: str, exchange: str = None) -> float
```

## OrdersCache - FIFO Order Queue Management

```python
# Queue Operations
async def push_open_order(exchange: str, order: Order) -> bool
async def pop_open_order(exchange: str, timeout: int = 0) -> Order | None

# Status Operations
async def set_order_status(order_id: str, status: str) -> bool
async def get_order_status(order_id: str) -> str | None
async def delete_order_status(order_id: str) -> bool

# Batch Operations
async def get_queue_length(exchange: str) -> int
```

## TradesCache - Trade Data Management

```python
# Model Creation (as used in examples)
from fullon_orm.models import Trade
from datetime import datetime, timezone

# Create Trade objects
trade = Trade(
    trade_id=10000,
    ex_trade_id="TRADE_BINANCE_001234",
    symbol="BTC/USDT",
    side="buy",  # or "sell"
    volume=0.05,
    price=50000.0,
    cost=2500.0,
    fee=2.5,
    time=datetime.now(timezone.utc)
)

# Trade Operations (fullon_orm models as input/output)
async def push_trade(exchange: str, trade: Trade) -> bool
async def get_trades(symbol: str, exchange: str) -> list[Trade]

# Status Operations  
async def update_trade_status(trade_key: str) -> bool
async def get_trade_status(trade_key: str) -> datetime | None

# Analytics Operations
async def get_trade_analytics(symbol: str, exchange: str) -> dict
```

## AccountCache - Balances & Positions

```python
# Model Creation (as used in examples)
from fullon_orm.models import Position
from datetime import datetime, timezone

# Create Position objects
position = Position(
    symbol="BTC/USDT",
    cost=22500.0,     # Total cost basis
    volume=0.5,       # Position size
    fee=22.5,         # Total fees
    price=45000.0,    # Average entry price
    timestamp=datetime.now(timezone.utc).timestamp(),
    ex_id="2000"      # Exchange ID as string
)

# Balance Operations (using exchange IDs and dict structures)
async def set_user_balances(exchange_id: int, balance_data: dict) -> bool
async def get_user_balance(exchange_id: int, currency: str) -> dict | None
async def get_user_balances(exchange_id: int) -> dict

# Position Operations (fullon_orm models as input/output)
async def set_position(exchange_id: int, position: Position) -> bool  
async def get_position(exchange_id: int, symbol: str) -> Position | None
async def get_positions(exchange_id: int) -> dict[str, Position]
async def delete_position(exchange_id: int, symbol: str) -> bool

# Portfolio Operations
async def get_portfolio_summary(exchange_id: int) -> dict
```

## BotCache - Bot Coordination & Blocking

```python
# Exchange Blocking
async def block_exchange(ex_id: str, symbol: str, bot_id: str | int) -> bool
async def unblock_exchange(ex_id: str, symbol: str) -> bool
async def is_blocked(ex_id: str, symbol: str) -> str | None
async def get_blocks() -> list[dict[str, str]]

# Position Opening States
async def mark_opening_position(ex_id: str, symbol: str, bot_id: str | int) -> bool
async def unmark_opening_position(ex_id: str, symbol: str) -> bool  
async def is_opening_position(ex_id: str, symbol: str) -> bool

# Bot Management
async def update_bot(bot_id: str, bot_data: dict[str, str | int | float]) -> bool
async def get_bots() -> dict[str, dict[str, Any]]
async def del_bot(bot_id: str) -> bool
```

## OHLCVCache - Candlestick Data

```python
# OHLCV Operations
async def update_ohlcv_bars(symbol: str, timeframe: str, bars: list[list[float]]) -> None
async def get_latest_ohlcv_bars(symbol: str, timeframe: str, count: int) -> list[list[float]]

# Supported timeframes: "1m", "5m", "15m", "30m", "1h", "4h", "1d"
# Bar format: [timestamp, open, high, low, close, volume]
```

## ProcessCache - System Monitoring

```python
# Process Registration
async def register_process(process_type: ProcessType, component: str, 
                          params: dict = None, message: str = None) -> str

# Process Monitoring  
async def get_active_processes(process_type: ProcessType = None, 
                              component: str = None, 
                              since_minutes: int = 5) -> list[dict[str, Any]]

# Health Monitoring
async def get_system_health() -> dict[str, Any]
async def get_component_status(component: str) -> dict[str, Any] | None
```

## Common Patterns Used in Examples

### Context Manager Usage (Always Required)
```python
async with TickCache() as cache:
    # All operations must be inside context manager
    await cache.ping()
    # ... other operations
```

### Error Handling Pattern
```python
try:
    async with Cache() as cache:
        result = await cache.some_operation()
        if result:
            print("✅ Success")
        else:
            print("❌ Failed") 
except Exception as e:
    print(f"❌ Error: {e}")
```

### Model Imports Used
```python
from fullon_orm.models import (
    Tick,        # Ticker data
    Order,       # Order objects
    Trade,       # Trade data  
    Position,    # Position data
    Symbol       # Symbol objects
)
```

### Return Types
- `bool` - Success/failure operations
- `None` - Void operations (like update_ohlcv_bars)  
- `dict` - Complex data structures
- `list` - Collections of objects
- `str | None` - Optional string values
- `int` - Counts and IDs
- `float` - Prices and numeric values
- `datetime` - Timestamps

### Key Notes
1. **All methods are async** - Use `await` for every cache operation
2. **Context managers required** - Always use `async with Cache() as cache:`
3. **Redis keys are auto-generated** - No manual key management needed
4. **UTC timestamps** - All time-based operations use UTC
5. **JSON serialization** - Complex objects auto-serialized/deserialized
6. **Connection pooling** - Automatic Redis connection management