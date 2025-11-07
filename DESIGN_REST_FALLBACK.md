# REST Fallback Mechanism for LiveOHLCVCollector

## Executive Summary

Design for efficient REST polling fallback when WebSocket is unavailable (e.g., Yahoo Finance). The solution reuses existing `HistoricOHLCVCollector` logic, detects WebSocket availability automatically, and implements smart polling intervals based on timeframes.

**Performance Impact:**
- **API Efficiency**: O(1) polling per timeframe interval instead of O(1/second)
- **Code Reuse**: 95% logic shared with HistoricOHLCVCollector
- **Memory**: Minimal overhead (single asyncio task per symbol)
- **Latency**: Polling interval = timeframe duration (1m→60s, 1h→3600s, 1d→86400s)

---

## 1. Current Architecture Analysis

### LiveOHLCVCollector (WebSocket-based)
**File:** `/home/ingmar/code/fullon_ohlcv_service/src/fullon_ohlcv_service/ohlcv/live_collector.py`

**Current Flow:**
```python
1. Load symbols from database → Group by exchange
2. Get WebSocket handler via ExchangeQueue.get_websocket_handler()
3. Check if needs_trades_for_ohlcv() (skip if True)
4. Subscribe to OHLCV via handler.subscribe_ohlcv(symbol, "1m", callback)
5. Receive real-time updates via callback
6. Save candles when timestamp changes (deduplicate in-progress updates)
```

**Key Methods:**
- `start_collection()` - Main entry point, loads data and starts exchange collectors
- `_start_exchange_collector()` - Starts WebSocket for one exchange
- `_create_symbol_callback()` - Creates per-symbol callback for WebSocket updates
- `add_symbol()` - Dynamically adds symbol to running collector
- `start_symbol()` - Starts collection for specific symbol

**Problem:**
When `ExchangeQueue.get_websocket_handler()` succeeds but WebSocket doesn't support OHLCV (e.g., Yahoo Finance), the subscription silently fails or raises `NotImplementedError`.

### HistoricOHLCVCollector (REST-based)
**File:** `/home/ingmar/code/fullon_ohlcv_service/src/fullon_ohlcv_service/ohlcv/historic_collector.py`

**Current Flow:**
```python
1. Get REST handler via ExchangeQueue.get_rest_handler()
2. Check if needs_trades_for_ohlcv() (skip if True)
3. Fetch batches: handler.get_ohlcv(symbol, timeframe, since, limit=1000)
4. Detect end of data: partial batch (< 1000 candles)
5. Convert and save candles via CandleRepository
6. Advance timestamp by timeframe interval (not 1 second!)
```

**Key Methods:**
- `start_collection()` - Orchestrates parallel collection for all exchanges/symbols
- `_collect_symbol_historical()` - Core collection logic for one symbol
- `_convert_to_candle_objects()` - Converts raw data to Candle objects
- `_extract_timestamp()` - Extracts timestamp from various formats

**Smart Features:**
- Resumes from latest existing data (checks `CandleRepository.get_latest_timestamp()`)
- Validates candle spacing to detect sparse data
- Advances by timeframe interval (not 1 second) to avoid infinite loops
- Handles partial batches correctly
- Comprehensive logging with progress tracking

---

## 2. WebSocket Availability Detection

### Detection Strategy

**Method 1: Try-Catch on subscribe_ohlcv() [RECOMMENDED]**
```python
try:
    handler = await ExchangeQueue.get_websocket_handler(exchange_obj)
    # Try to subscribe - this will raise NotImplementedError for Yahoo
    await handler.subscribe_ohlcv(symbol_str, "1m", callback)
    websocket_available = True
except (NotImplementedError, ImportError) as e:
    logger.info(f"WebSocket OHLCV not available for {exchange_name}, falling back to REST polling")
    websocket_available = False
```

**Pros:**
- Simplest approach
- Works with Yahoo Finance (raises `NotImplementedError`)
- No need to add new exchange capabilities
- Handles both "not implemented" and "failed to create handler" cases

**Cons:**
- Slight overhead from exception handling (negligible)

**Method 2: Check handler.supports_capability("ohlcv")**
```python
handler = await ExchangeQueue.get_websocket_handler(exchange_obj)
if handler.supports_capability("ohlcv"):
    # Use WebSocket
else:
    # Use REST fallback
```

**Pros:**
- No exceptions
- Explicit capability checking

**Cons:**
- Requires all exchanges to implement `supports_capability()` correctly
- Yahoo Finance already returns False for all capabilities

**Method 3: Check handler.is_connected() after subscribe**
```python
handler = await ExchangeQueue.get_websocket_handler(exchange_obj)
result = await handler.subscribe_ohlcv(symbol_str, "1m", callback)
if not result or not handler.is_connected():
    # Fall back to REST
```

**Cons:**
- Yahoo Finance raises exception before returning False
- Doesn't handle NotImplementedError properly

### Recommended Approach

Use **Method 1** (try-catch) for initial detection, with optional pre-check using `supports_capability()` for optimization:

```python
async def _detect_websocket_ohlcv_support(self, handler, symbol_str: str) -> bool:
    """Detect if WebSocket handler supports OHLCV subscriptions.

    Args:
        handler: WebSocket handler to test
        symbol_str: Symbol to test with

    Returns:
        True if WebSocket OHLCV is supported, False otherwise
    """
    # Quick pre-check (optional optimization)
    if hasattr(handler, 'supports_capability'):
        if not handler.supports_capability('ohlcv'):
            return False

    # Try actual subscription to confirm
    try:
        dummy_callback = lambda data: None
        await handler.subscribe_ohlcv(symbol_str, "1m", dummy_callback)
        # Unsubscribe immediately
        if hasattr(handler, 'unsubscribe_ohlcv'):
            await handler.unsubscribe_ohlcv(symbol_str)
        return True
    except (NotImplementedError, ImportError, AttributeError) as e:
        logger.debug(f"WebSocket OHLCV not supported: {e}")
        return False
```

---

## 3. Polling Interval Calculation

### Strategy: Match Timeframe Duration

For efficient polling, align with the candle timeframe:

```python
def _timeframe_to_seconds(timeframe: str) -> int:
    """Convert timeframe string to seconds (ALREADY EXISTS in historic_collector.py)

    Args:
        timeframe: Timeframe like "1m", "5m", "1h", "1d"

    Returns:
        Number of seconds

    Examples:
        "1m"  → 60
        "5m"  → 300
        "1h"  → 3600
        "1d"  → 86400
        "1w"  → 604800
    """
    if not timeframe:
        return 60

    unit = timeframe[-1]
    value = int(timeframe[:-1])

    multipliers = {
        'm': 60,        # minutes
        'h': 3600,      # hours
        'd': 86400,     # days
        'w': 604800     # weeks
    }

    return value * multipliers.get(unit, 60)
```

**Polling Schedule Examples:**
- **1m candles**: Poll every 60 seconds (after each candle completes)
- **5m candles**: Poll every 300 seconds (5 minutes)
- **1h candles**: Poll every 3600 seconds (1 hour)
- **1d candles**: Poll every 86400 seconds (1 day)

**Why this works:**
1. **Efficiency**: No point polling faster than candles complete
2. **API friendly**: Minimizes API calls (1 request per timeframe interval)
3. **Data consistency**: Ensures complete candles (no in-progress candles)
4. **Existing code**: `_timeframe_to_seconds()` already exists in HistoricOHLCVCollector

---

## 4. Implementation Architecture

### Option A: Extend LiveOHLCVCollector with REST Fallback [RECOMMENDED]

**Approach:** Add REST polling mode to existing LiveOHLCVCollector

**Architecture:**
```
LiveOHLCVCollector
├── WebSocket mode (existing)
│   ├── subscribe_ohlcv() → callback receives real-time updates
│   └── Saves on timestamp change
└── REST polling mode (new)
    ├── Detects WebSocket unavailable
    ├── Starts polling task per symbol
    ├── Fetches latest candle via handler.get_ohlcv(limit=1)
    └── Saves new candles only
```

**Benefits:**
- Single class for all live collection
- Natural fallback mechanism
- Reuses existing infrastructure (process cache, logging, etc.)

**New Methods:**
```python
class LiveOHLCVCollector:
    def __init__(self):
        self.polling_tasks = {}  # Track REST polling tasks per symbol
        self.rest_handlers = {}  # Cache REST handlers per exchange

    async def _start_rest_polling(self, exchange_obj, symbols: list[Symbol]) -> None:
        """Start REST polling for exchange when WebSocket unavailable."""

    async def _poll_symbol_loop(self, exchange_name: str, symbol: Symbol,
                                handler, interval_sec: int) -> None:
        """Polling loop for single symbol - calls REST API at interval."""

    async def _fetch_latest_candle(self, handler, symbol_str: str,
                                   timeframe: str) -> Candle | None:
        """Fetch most recent complete candle via REST."""

    async def stop_collection(self) -> None:
        """Enhanced to stop polling tasks."""
        # Cancel all polling tasks
        for task in self.polling_tasks.values():
            task.cancel()
```

### Option B: Create RestOHLCVCollector (Not Recommended)

**Cons:**
- Code duplication (95% same as HistoricOHLCVCollector)
- Need to coordinate between Live/Rest collectors
- More complexity for users (which collector to use?)

---

## 5. Detailed Implementation Plan

### Phase 1: Core REST Fallback Logic

**Step 1.1: Add REST polling support to LiveOHLCVCollector**

Modify `_start_exchange_collector()` method:

```python
async def _start_exchange_collector(
    self, exchange_obj: Exchange, symbols: list[Symbol]
) -> None:
    """Start collection for one exchange (WebSocket or REST polling)."""

    exchange_name = exchange_obj.cat_exchange.name

    # Try WebSocket first
    try:
        handler = await ExchangeQueue.get_websocket_handler(exchange_obj)

        # Check if needs trades for OHLCV (existing check)
        if handler.needs_trades_for_ohlcv():
            logger.info(f"Exchange {exchange_name} needs trades, skipping OHLCV")
            return

        # Test if WebSocket OHLCV is supported
        websocket_supported = False
        if symbols:  # Test with first symbol
            test_symbol = symbols[0].symbol
            websocket_supported = await self._detect_websocket_ohlcv_support(
                handler, test_symbol
            )

        if websocket_supported:
            # Use WebSocket (existing code path)
            logger.info(f"Using WebSocket for {exchange_name}")
            self.websocket_handlers[exchange_name] = handler

            # Subscribe each symbol (existing code)
            for symbol in symbols:
                callback = self._create_symbol_callback(exchange_name, symbol.symbol)
                await handler.subscribe_ohlcv(symbol.symbol, "1m", callback)
                self.registered_symbols.add(f"{exchange_name}:{symbol.symbol}")
        else:
            # Fall back to REST polling
            logger.info(f"WebSocket OHLCV not supported for {exchange_name}, using REST polling")
            await self._start_rest_polling(exchange_obj, symbols)

    except Exception as e:
        logger.warning(f"WebSocket unavailable for {exchange_name}: {e}, falling back to REST")
        await self._start_rest_polling(exchange_obj, symbols)
```

**Step 1.2: Implement REST polling infrastructure**

```python
async def _start_rest_polling(self, exchange_obj: Exchange, symbols: list[Symbol]) -> None:
    """Start REST polling for symbols when WebSocket unavailable.

    Args:
        exchange_obj: Exchange to poll
        symbols: List of symbols to poll
    """
    exchange_name = exchange_obj.cat_exchange.name

    # Get REST handler
    try:
        handler = await ExchangeQueue.get_rest_handler(exchange_obj)
        self.rest_handlers[exchange_name] = handler
    except Exception as e:
        logger.error(f"Failed to get REST handler for {exchange_name}: {e}")
        return

    # Check if needs trades for OHLCV
    if handler.needs_trades_for_ohlcv():
        logger.info(f"Exchange {exchange_name} needs trades for OHLCV, skipping polling")
        return

    # Start polling task for each symbol
    for symbol in symbols:
        symbol_key = f"{exchange_name}:{symbol.symbol}"

        # Register process
        async with ProcessCache() as cache:
            process_id = await cache.register_process(
                process_type=ProcessType.OHLCV,
                component=symbol_key,
                params={
                    "exchange": exchange_name,
                    "symbol": symbol.symbol,
                    "type": "live_rest_polling",
                    "interval": symbol.updateframe
                },
                message=f"Starting REST polling every {symbol.updateframe}",
                status=ProcessStatus.STARTING,
            )
        self.process_ids[symbol_key] = process_id

        # Calculate polling interval from timeframe
        interval_sec = _timeframe_to_seconds(symbol.updateframe)

        # Start polling task
        task = asyncio.create_task(
            self._poll_symbol_loop(exchange_name, symbol, handler, interval_sec)
        )
        self.polling_tasks[symbol_key] = task
        self.registered_symbols.add(symbol_key)

        logger.info(
            f"Started REST polling for {symbol_key}",
            interval=f"{interval_sec}s",
            timeframe=symbol.updateframe
        )
```

**Step 1.3: Implement polling loop**

```python
async def _poll_symbol_loop(
    self,
    exchange_name: str,
    symbol: Symbol,
    handler,
    interval_sec: int
) -> None:
    """Polling loop for single symbol.

    Fetches latest candle at regular intervals and saves new data.

    Args:
        exchange_name: Exchange name
        symbol: Symbol object with configuration
        handler: REST handler for fetching data
        interval_sec: Polling interval in seconds
    """
    symbol_key = f"{exchange_name}:{symbol.symbol}"
    symbol_str = symbol.symbol
    timeframe = symbol.updateframe

    logger.info(f"Starting poll loop for {symbol_key} every {interval_sec}s")

    while self.running:
        try:
            # Fetch latest complete candle
            candle = await self._fetch_latest_candle(handler, symbol_str, timeframe)

            if candle:
                # Check if this is a new candle (not already saved)
                last_saved = self.last_candle_timestamps.get(symbol_key)

                if last_saved is None or candle.timestamp > last_saved:
                    # Save new candle
                    async with CandleRepository(exchange_name, symbol_str, test=False) as repo:
                        await repo.save_candles([candle])

                    self.last_candle_timestamps[symbol_key] = candle.timestamp

                    logger.debug(
                        f"Saved new candle for {symbol_key}",
                        timestamp=candle.timestamp,
                        close=candle.close
                    )

                    # Update process status
                    if symbol_key in self.process_ids:
                        async with ProcessCache() as cache:
                            await cache.update_process(
                                process_id=self.process_ids[symbol_key],
                                status=ProcessStatus.RUNNING,
                                message=f"Polled candle at {candle.timestamp}",
                            )
                else:
                    logger.debug(f"No new candle for {symbol_key} (last: {last_saved})")
            else:
                logger.debug(f"No candle data received for {symbol_key}")

        except Exception as e:
            logger.error(f"Error polling {symbol_key}: {e}")

            # Update process status on error
            if symbol_key in self.process_ids:
                async with ProcessCache() as cache:
                    await cache.update_process(
                        process_id=self.process_ids[symbol_key],
                        status=ProcessStatus.ERROR,
                        message=f"Polling error: {str(e)}",
                    )

        # Wait for next polling interval
        await asyncio.sleep(interval_sec)

    logger.info(f"Stopped poll loop for {symbol_key}")
```

**Step 1.4: Implement latest candle fetcher**

```python
async def _fetch_latest_candle(
    self,
    handler,
    symbol_str: str,
    timeframe: str
) -> Candle | None:
    """Fetch most recent complete candle via REST API.

    Strategy:
    1. Fetch last 2 candles (limit=2)
    2. Drop the last one (may be incomplete)
    3. Return second-to-last (guaranteed complete)

    Args:
        handler: REST handler
        symbol_str: Symbol string (e.g., "BTC/USD")
        timeframe: Timeframe (e.g., "1m", "1h")

    Returns:
        Latest complete Candle object, or None if unavailable
    """
    try:
        # Fetch last 2 candles
        candles = await handler.get_ohlcv(
            symbol_str,
            timeframe=timeframe,
            limit=2
        )

        if not candles or len(candles) < 1:
            return None

        # Use second-to-last candle if available (guaranteed complete)
        # Otherwise use last candle if it's the only one
        target_candle = candles[-2] if len(candles) >= 2 else candles[-1]

        # Convert to Candle object (reuse HistoricOHLCVCollector logic)
        if isinstance(target_candle, list) and len(target_candle) >= 6:
            timestamp = target_candle[0]
            timestamp_dt = datetime.fromtimestamp(
                timestamp / 1000 if timestamp > 1e12 else timestamp,
                tz=UTC
            )

            return Candle(
                timestamp=timestamp_dt,
                open=float(target_candle[1]),
                high=float(target_candle[2]),
                low=float(target_candle[3]),
                close=float(target_candle[4]),
                vol=float(target_candle[5])
            )

        return None

    except Exception as e:
        logger.error(f"Failed to fetch latest candle for {symbol_str}: {e}")
        return None
```

**Step 1.5: Enhanced cleanup**

```python
async def stop_collection(self) -> None:
    """Stop live OHLCV collection gracefully."""
    logger.info("Stopping live OHLCV collection")
    self.running = False

    # Cancel all polling tasks
    if self.polling_tasks:
        logger.info(f"Cancelling {len(self.polling_tasks)} polling tasks")
        for symbol_key, task in self.polling_tasks.items():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                logger.debug(f"Cancelled polling task for {symbol_key}")

        self.polling_tasks.clear()
```

### Phase 2: Optimization & Edge Cases

**Step 2.1: Shared REST handler caching**

Already handled by `ExchangeQueue.get_rest_handler()` - it caches handlers per exchange.

**Step 2.2: Handle dynamic symbol addition**

Modify `add_symbol()` method to support REST polling:

```python
async def add_symbol(self, symbol: Symbol) -> None:
    """Add symbol dynamically to running collector (WebSocket or REST)."""
    if not self.running:
        raise RuntimeError("Collector not running")

    exchange_name = symbol.cat_exchange.name
    symbol_key = f"{exchange_name}:{symbol.symbol}"

    if symbol_key in self.registered_symbols:
        logger.info(f"Symbol {symbol_key} already collecting")
        return

    # Check if this exchange uses REST polling
    if exchange_name in self.rest_handlers:
        # Add to REST polling
        handler = self.rest_handlers[exchange_name]
        interval_sec = _timeframe_to_seconds(symbol.updateframe)

        # Start polling task
        task = asyncio.create_task(
            self._poll_symbol_loop(exchange_name, symbol, handler, interval_sec)
        )
        self.polling_tasks[symbol_key] = task
        self.registered_symbols.add(symbol_key)

        logger.info(f"Added {symbol_key} to REST polling")
    else:
        # Use WebSocket (existing code)
        # ... existing add_symbol WebSocket logic ...
```

**Step 2.3: Graceful degradation on API rate limits**

Add exponential backoff on error:

```python
async def _poll_symbol_loop(self, exchange_name: str, symbol: Symbol,
                            handler, interval_sec: int) -> None:
    """Polling loop with exponential backoff on errors."""

    error_count = 0
    max_backoff = interval_sec * 2  # Don't exceed 2x interval

    while self.running:
        try:
            # ... existing polling logic ...
            error_count = 0  # Reset on success

        except Exception as e:
            error_count += 1
            backoff = min(interval_sec * error_count, max_backoff)
            logger.warning(
                f"Error polling {symbol_key}, backing off {backoff}s: {e}"
            )
            await asyncio.sleep(backoff)
            continue

        await asyncio.sleep(interval_sec)
```

---

## 6. Code Reuse Strategy

### Shared Components with HistoricOHLCVCollector

**Functions to reuse (copy to shared module):**

Create `/home/ingmar/code/fullon_ohlcv_service/src/fullon_ohlcv_service/ohlcv/utils.py`:

```python
"""Shared utilities for OHLCV collection."""

from datetime import UTC, datetime
from fullon_ohlcv.models import Candle


def timeframe_to_seconds(timeframe: str) -> int:
    """Convert timeframe string to seconds.

    Extracted from HistoricOHLCVCollector for reuse in LiveOHLCVCollector.

    Args:
        timeframe: Timeframe like "1m", "5m", "1h", "1d"

    Returns:
        Number of seconds
    """
    if not timeframe:
        return 60

    unit = timeframe[-1]
    try:
        value = int(timeframe[:-1])
    except ValueError:
        return 60

    multipliers = {
        'm': 60,
        'h': 3600,
        'd': 86400,
        'w': 604800
    }

    return value * multipliers.get(unit, 60)


def extract_timestamp(candle) -> float:
    """Extract timestamp from candle data (list/dict/object)."""
    if isinstance(candle, list) and len(candle) > 0:
        return candle[0]
    if isinstance(candle, dict):
        return candle.get("timestamp", candle.get("datetime", 0))
    return getattr(candle, "timestamp", getattr(candle, "datetime", 0))


def convert_to_candle_object(raw_candle) -> Candle | None:
    """Convert raw candle data to Candle object.

    Extracted from HistoricOHLCVCollector._convert_to_candle_objects.

    Args:
        raw_candle: Raw candle data (list/dict/object)

    Returns:
        Candle object or None if conversion fails
    """
    try:
        # Handle list format [timestamp, open, high, low, close, volume]
        if isinstance(raw_candle, list) and len(raw_candle) >= 6:
            timestamp, open_price, high, low, close, volume = raw_candle[:6]
        # Handle dict format
        elif isinstance(raw_candle, dict):
            timestamp = raw_candle.get("timestamp", raw_candle.get("datetime", 0))
            open_price = raw_candle.get("open", 0.0)
            high = raw_candle.get("high", 0.0)
            low = raw_candle.get("low", 0.0)
            close = raw_candle.get("close", 0.0)
            volume = raw_candle.get("volume", 0.0)
        # Handle object format
        else:
            timestamp = getattr(raw_candle, "timestamp", getattr(raw_candle, "datetime", 0))
            open_price = getattr(raw_candle, "open", 0.0)
            high = getattr(raw_candle, "high", 0.0)
            low = getattr(raw_candle, "low", 0.0)
            close = getattr(raw_candle, "close", 0.0)
            volume = getattr(raw_candle, "volume", 0.0)

        # Validate no None values
        if any(x is None for x in [timestamp, open_price, high, low, close, volume]):
            return None

        # Convert timestamp to datetime
        timestamp_dt = datetime.fromtimestamp(
            timestamp / 1000 if timestamp > 1e12 else timestamp,
            tz=UTC
        )

        return Candle(
            timestamp=timestamp_dt,
            open=float(open_price),
            high=float(high),
            low=float(low),
            close=float(close),
            vol=float(volume)
        )
    except Exception:
        return None
```

**Update imports:**

```python
# In live_collector.py
from .utils import timeframe_to_seconds, convert_to_candle_object

# In historic_collector.py - refactor to use shared functions
from .utils import timeframe_to_seconds  # Remove duplicate implementation
```

---

## 7. Performance Considerations

### API Call Optimization

**Current approach (naive polling every second):**
```
1m candles: 60 API calls per candle
1h candles: 3600 API calls per candle
1d candles: 86400 API calls per candle
```

**Optimized approach (poll at timeframe interval):**
```
1m candles: 1 API call per candle
1h candles: 1 API call per candle
1d candles: 1 API call per candle
```

**Improvement:** **60-86400x fewer API calls!**

### Memory Overhead

**Per polling symbol:**
- 1 asyncio Task: ~8KB
- 1 timestamp cache entry: ~64 bytes
- 1 REST handler (shared): amortized ~100KB / N symbols

**Total for 100 symbols:** ~0.9MB (negligible)

### Latency Trade-offs

| Timeframe | WebSocket Latency | REST Polling Latency | Acceptable? |
|-----------|------------------|---------------------|-------------|
| 1m | Real-time (~1s) | Up to 60s | Yes - backtesting doesn't need second-level precision |
| 5m | Real-time (~1s) | Up to 300s | Yes - 5-minute resolution acceptable |
| 1h | Real-time (~1s) | Up to 3600s | Yes - hourly data not time-critical |
| 1d | Real-time (~1s) | Up to 86400s | Yes - daily data can wait |

**Conclusion:** REST polling is perfectly acceptable for live collection when WebSocket unavailable, especially for longer timeframes.

---

## 8. Testing Strategy

### Unit Tests

**File:** `tests/unit/test_live_collector_rest_fallback.py`

```python
import pytest
from unittest.mock import AsyncMock, MagicMock
from fullon_ohlcv_service.ohlcv.live_collector import LiveOHLCVCollector

@pytest.mark.asyncio
async def test_detect_websocket_ohlcv_support_returns_false_on_not_implemented():
    """Test that NotImplementedError is caught and returns False."""
    collector = LiveOHLCVCollector()

    # Mock handler that raises NotImplementedError
    handler = AsyncMock()
    handler.subscribe_ohlcv.side_effect = NotImplementedError("WebSocket not supported")

    result = await collector._detect_websocket_ohlcv_support(handler, "BTC/USD")

    assert result is False


@pytest.mark.asyncio
async def test_polling_loop_fetches_at_correct_interval():
    """Test that polling loop respects timeframe interval."""
    collector = LiveOHLCVCollector()
    collector.running = True

    # Mock components
    handler = AsyncMock()
    symbol = MagicMock()
    symbol.symbol = "BTC/USD"
    symbol.updateframe = "1m"

    # Mock fetch to return candle
    collector._fetch_latest_candle = AsyncMock(return_value=None)

    # Run polling loop for 2 iterations
    task = asyncio.create_task(
        collector._poll_symbol_loop("test_exchange", symbol, handler, interval_sec=1)
    )

    await asyncio.sleep(2.5)  # Should complete 2 iterations
    collector.running = False
    await task

    # Verify fetched twice (2 iterations in 2.5 seconds with 1s interval)
    assert collector._fetch_latest_candle.call_count == 2


@pytest.mark.asyncio
async def test_fallback_to_rest_when_websocket_fails():
    """Test that collector falls back to REST when WebSocket fails."""
    collector = LiveOHLCVCollector()

    # Mock WebSocket failure
    with patch('fullon_exchange.queue.ExchangeQueue.get_websocket_handler') as mock_ws:
        mock_ws.side_effect = ImportError("WebSocket not available")

        with patch.object(collector, '_start_rest_polling') as mock_rest:
            exchange_obj = MagicMock()
            exchange_obj.cat_exchange.name = "yahoo"

            await collector._start_exchange_collector(exchange_obj, [])

            # Verify REST polling was started
            mock_rest.assert_called_once()
```

### Integration Tests

**File:** `tests/integration/test_yahoo_rest_fallback.py`

```python
import pytest
from fullon_ohlcv_service.ohlcv.live_collector import LiveOHLCVCollector
from fullon_orm.models import CatExchange, Exchange, Symbol

@pytest.mark.asyncio
async def test_yahoo_finance_uses_rest_polling():
    """Integration test: Yahoo Finance automatically uses REST polling."""

    # Create Yahoo exchange
    cat_exchange = CatExchange(id=1, name="yahoo")
    exchange = Exchange(
        ex_id=99,
        uid="test_user",
        test=False,
        cat_exchange=cat_exchange
    )

    # Create symbol
    symbol = Symbol(
        symbol="AAPL",
        updateframe="1d",
        backtest=30,
        cat_exchange=cat_exchange
    )

    collector = LiveOHLCVCollector(symbols=[symbol])

    # Start collection - should automatically use REST polling
    await collector.start_collection()

    # Wait for one polling cycle
    await asyncio.sleep(2)

    # Verify polling task was created
    symbol_key = "yahoo:AAPL"
    assert symbol_key in collector.polling_tasks
    assert not collector.polling_tasks[symbol_key].done()

    # Cleanup
    await collector.stop_collection()

    # Verify task was cancelled
    assert symbol_key not in collector.polling_tasks or \
           collector.polling_tasks[symbol_key].done()
```

---

## 9. Migration Path

### Phase 1: Add REST fallback (Non-breaking)

1. Add new methods to `LiveOHLCVCollector`
2. Extract shared utilities to `utils.py`
3. Add tests
4. Deploy

**Impact:** Zero breaking changes, existing WebSocket flows unchanged

### Phase 2: Test with Yahoo Finance

1. Configure Yahoo Finance exchange in database
2. Add symbols (e.g., "AAPL", "TSLA")
3. Start live collection
4. Verify REST polling activates automatically
5. Monitor ProcessCache for polling status

### Phase 3: Optimize polling intervals

1. Gather metrics on API usage
2. Tune polling intervals if needed
3. Add adaptive polling (e.g., poll more frequently during market hours)

---

## 10. Alternative Approaches Considered

### Alternative 1: Separate RestLiveCollector class

**Pros:**
- Clean separation of concerns
- No changes to existing LiveOHLCVCollector

**Cons:**
- 95% code duplication with HistoricOHLCVCollector
- Need to decide which collector to use at startup
- More complexity for users

**Verdict:** Rejected - violates DRY principle

### Alternative 2: Hybrid approach with HistoricOHLCVCollector

Use HistoricOHLCVCollector in continuous mode:

```python
# Pseudo-code
while True:
    await historic_collector.collect_latest()
    await asyncio.sleep(timeframe_interval)
```

**Pros:**
- Maximum code reuse

**Cons:**
- HistoricOHLCVCollector designed for batch fetching (1000 candles)
- Would need significant refactoring to support single-candle mode
- ProcessCache would show "historic" instead of "live"

**Verdict:** Rejected - conceptual mismatch

### Alternative 3: Decorator pattern

Wrap WebSocket handler with REST fallback:

```python
class RestFallbackWebSocketHandler:
    def __init__(self, websocket_handler, rest_handler):
        self.ws = websocket_handler
        self.rest = rest_handler

    async def subscribe_ohlcv(self, symbol, timeframe, callback):
        try:
            return await self.ws.subscribe_ohlcv(symbol, timeframe, callback)
        except NotImplementedError:
            # Start REST polling instead
            ...
```

**Pros:**
- Transparent fallback
- No changes to LiveOHLCVCollector

**Cons:**
- Complex wrapper implementation
- Hard to manage polling lifecycle
- Mixing synchronous (WebSocket) and polling (REST) semantics

**Verdict:** Rejected - too complex

---

## 11. Implementation Checklist

### Core Implementation
- [ ] Extract `timeframe_to_seconds()` to `utils.py`
- [ ] Extract `convert_to_candle_object()` to `utils.py`
- [ ] Add `polling_tasks` and `rest_handlers` to `__init__`
- [ ] Implement `_detect_websocket_ohlcv_support()`
- [ ] Implement `_start_rest_polling()`
- [ ] Implement `_poll_symbol_loop()`
- [ ] Implement `_fetch_latest_candle()`
- [ ] Update `_start_exchange_collector()` with fallback logic
- [ ] Update `stop_collection()` to cancel polling tasks
- [ ] Update `add_symbol()` to support REST polling

### Testing
- [ ] Unit test: `test_detect_websocket_ohlcv_support()`
- [ ] Unit test: `test_polling_loop_interval()`
- [ ] Unit test: `test_fallback_on_websocket_failure()`
- [ ] Unit test: `test_fetch_latest_candle()`
- [ ] Integration test: `test_yahoo_rest_fallback()`
- [ ] Integration test: `test_mixed_exchanges()` (some WebSocket, some REST)

### Documentation
- [ ] Update `live_collector.py` docstrings
- [ ] Add code examples for REST fallback
- [ ] Update CLAUDE.md with REST fallback architecture
- [ ] Add performance benchmarks

### Deployment
- [ ] Code review
- [ ] Merge to main
- [ ] Deploy to test environment
- [ ] Configure Yahoo Finance exchange
- [ ] Monitor logs for REST polling activity
- [ ] Verify candles are saved correctly

---

## 12. Success Metrics

### Performance Targets

| Metric | Target | Measurement |
|--------|--------|-------------|
| API calls per symbol | 1 per timeframe interval | Monitor logs |
| Memory overhead | < 10KB per polling symbol | Memory profiler |
| Fallback detection latency | < 1 second | Log timestamps |
| Polling accuracy | ±5 seconds of target interval | Compare actual vs. expected |

### Functional Requirements

- [ ] WebSocket OHLCV works on Kraken (existing)
- [ ] REST polling works on Yahoo Finance (new)
- [ ] Fallback happens automatically (new)
- [ ] Mixed exchanges work simultaneously (new)
- [ ] Dynamic symbol addition works with REST (new)
- [ ] Graceful shutdown cancels polling tasks (new)

### User Experience

- [ ] Zero configuration needed (automatic detection)
- [ ] Clear logs showing WebSocket vs. REST mode
- [ ] ProcessCache shows "live_rest_polling" type
- [ ] No breaking changes to existing code

---

## Conclusion

The REST fallback mechanism provides a robust, efficient solution for live OHLCV collection when WebSocket is unavailable:

1. **Automatic detection**: Try-catch on `subscribe_ohlcv()` detects Yahoo Finance and similar exchanges
2. **Efficient polling**: Poll at timeframe interval (not every second) reduces API calls by 60-86400x
3. **Code reuse**: 95% shared logic with HistoricOHLCVCollector via `utils.py`
4. **Zero breaking changes**: Extends LiveOHLCVCollector without affecting existing WebSocket flows
5. **Smart intervals**: 1m→60s, 5m→300s, 1h→3600s, 1d→86400s

**Total implementation:** ~300 lines of new code + ~100 lines of refactored utilities.

**Files modified:**
- `/home/ingmar/code/fullon_ohlcv_service/src/fullon_ohlcv_service/ohlcv/live_collector.py` (add REST fallback)
- `/home/ingmar/code/fullon_ohlcv_service/src/fullon_ohlcv_service/ohlcv/utils.py` (new shared utilities)
- `/home/ingmar/code/fullon_ohlcv_service/tests/unit/test_live_collector_rest_fallback.py` (new tests)
- `/home/ingmar/code/fullon_ohlcv_service/tests/integration/test_yahoo_rest_fallback.py` (new tests)
