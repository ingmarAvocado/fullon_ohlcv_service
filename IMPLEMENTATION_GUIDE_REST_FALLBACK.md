# Quick Implementation Guide: REST Fallback for LiveOHLCVCollector

## Overview

This guide provides step-by-step instructions to implement REST polling fallback in LiveOHLCVCollector when WebSocket is unavailable.

**Estimated time:** 4-6 hours
**Files to modify:** 4
**Lines of code:** ~400

---

## Step 1: Create Shared Utilities (30 minutes)

**File:** `/home/ingmar/code/fullon_ohlcv_service/src/fullon_ohlcv_service/ohlcv/utils.py`

Create new file with shared functions extracted from HistoricOHLCVCollector:

```python
"""Shared utilities for OHLCV collection."""

from datetime import UTC, datetime
from fullon_ohlcv.models import Candle


def timeframe_to_seconds(timeframe: str) -> int:
    """Convert timeframe string to seconds.

    Examples:
        "1m"  → 60
        "5m"  → 300
        "1h"  → 3600
        "1d"  → 86400
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


def convert_to_candle_object(raw_candle) -> Candle | None:
    """Convert raw candle data to Candle object."""
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
        else:
            return None

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

---

## Step 2: Update LiveOHLCVCollector Init (15 minutes)

**File:** `/home/ingmar/code/fullon_ohlcv_service/src/fullon_ohlcv_service/ohlcv/live_collector.py`

Add imports:
```python
import asyncio
from datetime import UTC, datetime
from .utils import timeframe_to_seconds, convert_to_candle_object
```

Update `__init__`:
```python
def __init__(self, symbols: list | None = None):
    self.symbols = symbols or []
    self.running = False
    self.websocket_handlers = {}
    self.registered_symbols = set()
    self.last_candle_timestamps = {}
    self.process_ids = {}

    # NEW: REST polling support
    self.rest_handlers = {}  # Cache REST handlers per exchange
    self.polling_tasks = {}  # Track polling tasks per symbol
```

---

## Step 3: Implement WebSocket Detection (30 minutes)

Add this method to LiveOHLCVCollector:

```python
async def _detect_websocket_ohlcv_support(
    self, handler, symbol_str: str
) -> bool:
    """Detect if WebSocket handler supports OHLCV subscriptions.

    Strategy:
    1. Quick check via supports_capability() if available
    2. Try actual subscription to confirm
    3. Catch NotImplementedError for exchanges like Yahoo Finance

    Args:
        handler: WebSocket handler to test
        symbol_str: Symbol to test with

    Returns:
        True if WebSocket OHLCV is supported, False otherwise
    """
    # Quick pre-check (optional optimization)
    if hasattr(handler, 'supports_capability'):
        try:
            if not handler.supports_capability('ohlcv'):
                logger.debug(
                    "WebSocket OHLCV not supported (capability check)",
                    symbol=symbol_str
                )
                return False
        except Exception as e:
            logger.debug(f"Could not check capability: {e}")

    # Try actual subscription to confirm
    try:
        dummy_callback = lambda data: None
        await handler.subscribe_ohlcv(symbol_str, "1m", dummy_callback)

        # Unsubscribe immediately to clean up
        if hasattr(handler, 'unsubscribe_ohlcv'):
            try:
                await handler.unsubscribe_ohlcv(symbol_str)
            except Exception:
                pass  # Ignore unsubscribe errors

        logger.debug(
            "WebSocket OHLCV supported (subscription test passed)",
            symbol=symbol_str
        )
        return True

    except (NotImplementedError, ImportError, AttributeError) as e:
        logger.debug(
            "WebSocket OHLCV not supported (subscription test failed)",
            symbol=symbol_str,
            error=str(e)
        )
        return False
```

---

## Step 4: Implement REST Polling Infrastructure (60 minutes)

Add these three methods to LiveOHLCVCollector:

### 4.1: Start REST Polling

```python
async def _start_rest_polling(
    self, exchange_obj: Exchange, symbols: list[Symbol]
) -> None:
    """Start REST polling for symbols when WebSocket unavailable.

    Args:
        exchange_obj: Exchange to poll
        symbols: List of symbols to poll
    """
    exchange_name = exchange_obj.cat_exchange.name

    logger.info(
        "Starting REST polling for exchange",
        exchange=exchange_name,
        symbol_count=len(symbols)
    )

    # Get REST handler (cached by ExchangeQueue)
    try:
        handler = await ExchangeQueue.get_rest_handler(exchange_obj)
        self.rest_handlers[exchange_name] = handler
    except Exception as e:
        logger.error(
            "Failed to get REST handler",
            exchange=exchange_name,
            error=str(e)
        )
        return

    # Check if needs trades for OHLCV
    try:
        if handler.needs_trades_for_ohlcv():
            logger.info(
                "Exchange needs trades for OHLCV, skipping REST polling",
                exchange=exchange_name
            )
            return
    except AttributeError:
        pass  # Handler doesn't have this method

    # Start polling task for each symbol
    for symbol in symbols:
        symbol_key = f"{exchange_name}:{symbol.symbol}"

        # Register process in cache
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
        interval_sec = timeframe_to_seconds(symbol.updateframe)

        # Start polling task
        task = asyncio.create_task(
            self._poll_symbol_loop(exchange_name, symbol, handler, interval_sec)
        )
        self.polling_tasks[symbol_key] = task
        self.registered_symbols.add(symbol_key)

        logger.info(
            "Started REST polling",
            symbol=symbol_key,
            interval_sec=interval_sec,
            timeframe=symbol.updateframe
        )
```

### 4.2: Polling Loop

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

    logger.info(
        "Starting poll loop",
        symbol=symbol_key,
        interval_sec=interval_sec
    )

    error_count = 0
    max_backoff = interval_sec * 2

    while self.running:
        try:
            # Fetch latest complete candle
            candle = await self._fetch_latest_candle(handler, symbol_str, timeframe)

            if candle:
                # Check if this is a new candle (not already saved)
                last_saved = self.last_candle_timestamps.get(symbol_key)

                if last_saved is None or candle.timestamp > last_saved:
                    # Save new candle
                    async with CandleRepository(
                        exchange_name, symbol_str, test=False
                    ) as repo:
                        await repo.save_candles([candle])

                    self.last_candle_timestamps[symbol_key] = candle.timestamp

                    logger.debug(
                        "Saved new candle",
                        symbol=symbol_key,
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

                    # Reset error count on success
                    error_count = 0
                else:
                    logger.debug(
                        "No new candle",
                        symbol=symbol_key,
                        last_saved=last_saved
                    )
            else:
                logger.debug(
                    "No candle data received",
                    symbol=symbol_key
                )

        except Exception as e:
            error_count += 1
            logger.error(
                "Error polling symbol",
                symbol=symbol_key,
                error=str(e),
                error_count=error_count
            )

            # Update process status on error
            if symbol_key in self.process_ids:
                async with ProcessCache() as cache:
                    await cache.update_process(
                        process_id=self.process_ids[symbol_key],
                        status=ProcessStatus.ERROR,
                        message=f"Polling error: {str(e)}",
                    )

            # Exponential backoff on repeated errors
            if error_count > 1:
                backoff = min(interval_sec * error_count, max_backoff)
                logger.warning(
                    "Backing off after errors",
                    symbol=symbol_key,
                    backoff_sec=backoff
                )
                await asyncio.sleep(backoff)
                continue

        # Wait for next polling interval
        await asyncio.sleep(interval_sec)

    logger.info("Stopped poll loop", symbol=symbol_key)
```

### 4.3: Fetch Latest Candle

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
    2. Use second-to-last candle (guaranteed complete)
    3. If only 1 candle available, use that

    Args:
        handler: REST handler
        symbol_str: Symbol string (e.g., "BTC/USD")
        timeframe: Timeframe (e.g., "1m", "1h")

    Returns:
        Latest complete Candle object, or None if unavailable
    """
    try:
        # Fetch last 2 candles to ensure we get a complete one
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

        # Convert to Candle object using shared utility
        return convert_to_candle_object(target_candle)

    except Exception as e:
        logger.error(
            "Failed to fetch latest candle",
            symbol=symbol_str,
            error=str(e)
        )
        return None
```

---

## Step 5: Update Exchange Collector (30 minutes)

Modify `_start_exchange_collector()` to add fallback logic:

```python
async def _start_exchange_collector(
    self, exchange_obj: Exchange, symbols: list[Symbol]
) -> None:
    """Start WebSocket collection for one exchange with symbol list.

    Falls back to REST polling if WebSocket OHLCV is not available.
    """
    exchange_name = exchange_obj.cat_exchange.name

    logger.info(
        "Starting collector for exchange",
        exchange=exchange_name,
        symbol_count=len(symbols)
    )

    # Try WebSocket first
    try:
        handler = await ExchangeQueue.get_websocket_handler(exchange_obj)
        self.websocket_handlers[exchange_name] = handler

        logger.debug("WebSocket handler obtained", exchange=exchange_name)

        # Check if this exchange needs trade collection instead of OHLCV
        try:
            if handler.needs_trades_for_ohlcv():
                logger.info(
                    "Exchange requires trade collection instead of OHLCV - skipping",
                    exchange=exchange_name,
                    symbol_count=len(symbols),
                )
                return
        except AttributeError:
            pass  # Handler doesn't have this method

        # Test if WebSocket OHLCV is supported (using first symbol)
        websocket_supported = False
        if symbols:
            test_symbol = symbols[0].symbol
            websocket_supported = await self._detect_websocket_ohlcv_support(
                handler, test_symbol
            )

        if websocket_supported:
            logger.info(
                "Exchange supports WebSocket OHLCV - using WebSocket mode",
                exchange=exchange_name,
                symbol_count=len(symbols),
            )

            # Subscribe each symbol with its own callback (EXISTING CODE)
            try:
                for symbol in symbols:
                    try:
                        symbol_str = symbol.symbol
                        symbol_key = f"{exchange_name}:{symbol_str}"

                        # Register process for this symbol
                        async with ProcessCache() as cache:
                            process_id = await cache.register_process(
                                process_type=ProcessType.OHLCV,
                                component=symbol_key,
                                params={
                                    "exchange": exchange_name,
                                    "symbol": symbol_str,
                                    "type": "live_websocket",
                                },
                                message="Starting live OHLCV collection",
                                status=ProcessStatus.STARTING,
                            )
                        self.process_ids[symbol_key] = process_id

                        logger.debug(
                            "Subscribing to OHLCV",
                            exchange=exchange_name,
                            symbol=symbol_str
                        )

                        # Create per-symbol callback
                        symbol_callback = self._create_symbol_callback(
                            exchange_name, symbol_str
                        )
                        result = await handler.subscribe_ohlcv(
                            symbol_str, "1m", symbol_callback
                        )

                        logger.info(
                            "Subscription result",
                            exchange=exchange_name,
                            symbol=symbol_str,
                            success=result,
                        )

                        # Register symbol
                        self.registered_symbols.add(symbol_key)

                    except Exception as e:
                        logger.warning(
                            "Failed to subscribe to OHLCV",
                            exchange=exchange_name,
                            symbol=symbol.symbol if hasattr(symbol, "symbol") else str(symbol),
                            error=str(e),
                        )
            finally:
                logger.info(
                    "Finished subscribing to OHLCV",
                    exchange=exchange_name,
                    symbol_count=len(symbols),
                )
        else:
            # WebSocket OHLCV not supported - fall back to REST polling
            logger.info(
                "WebSocket OHLCV not supported - falling back to REST polling",
                exchange=exchange_name,
                symbol_count=len(symbols),
            )
            await self._start_rest_polling(exchange_obj, symbols)

    except Exception as e:
        # WebSocket handler creation failed - fall back to REST polling
        logger.warning(
            "WebSocket unavailable for exchange - falling back to REST polling",
            exchange=exchange_name,
            error=str(e)
        )
        await self._start_rest_polling(exchange_obj, symbols)
```

---

## Step 6: Update Cleanup (15 minutes)

Modify `stop_collection()` to cancel polling tasks:

```python
async def stop_collection(self) -> None:
    """Stop live OHLCV collection gracefully."""
    logger.info("Stopping live OHLCV collection")
    self.running = False

    # Cancel all REST polling tasks
    if self.polling_tasks:
        logger.info(
            "Cancelling polling tasks",
            count=len(self.polling_tasks)
        )

        for symbol_key, task in self.polling_tasks.items():
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    logger.debug(
                        "Cancelled polling task",
                        symbol=symbol_key
                    )
                except Exception as e:
                    logger.warning(
                        "Error cancelling polling task",
                        symbol=symbol_key,
                        error=str(e)
                    )

        self.polling_tasks.clear()
```

---

## Step 7: Update add_symbol() for REST Support (30 minutes)

Modify `add_symbol()` to support REST polling:

```python
async def add_symbol(self, symbol: Symbol) -> None:
    """Add symbol dynamically to running collector.

    Supports both WebSocket and REST polling modes.
    """
    if not self.running:
        raise RuntimeError("Collector not running - call start_collection() first")

    exchange_name = symbol.cat_exchange.name
    symbol_key = f"{exchange_name}:{symbol.symbol}"

    # Check if already collecting
    if symbol_key in self.registered_symbols:
        logger.info("Symbol already collecting", symbol_key=symbol_key)
        return

    # Check if this exchange uses REST polling
    if exchange_name in self.rest_handlers:
        # Add to REST polling
        logger.info(
            "Adding symbol to REST polling",
            exchange=exchange_name,
            symbol=symbol.symbol
        )

        handler = self.rest_handlers[exchange_name]
        interval_sec = timeframe_to_seconds(symbol.updateframe)

        # Register process
        async with ProcessCache() as cache:
            process_id = await cache.register_process(
                process_type=ProcessType.OHLCV,
                component=symbol_key,
                params={
                    "exchange": exchange_name,
                    "symbol": symbol.symbol,
                    "type": "live_rest_polling",
                },
                message="Starting REST polling",
                status=ProcessStatus.STARTING,
            )
        self.process_ids[symbol_key] = process_id

        # Start polling task
        task = asyncio.create_task(
            self._poll_symbol_loop(exchange_name, symbol, handler, interval_sec)
        )
        self.polling_tasks[symbol_key] = task
        self.registered_symbols.add(symbol_key)

        logger.info(
            "Added symbol to REST polling",
            symbol=symbol_key,
            interval_sec=interval_sec
        )
        return

    # Otherwise use WebSocket (EXISTING CODE)
    # Get admin exchanges
    _, admin_exchanges = await get_admin_exchanges()

    # Find admin exchange for this symbol
    admin_exchange = None
    for exchange in admin_exchanges:
        if exchange.cat_exchange.name == exchange_name:
            admin_exchange = exchange
            break

    if not admin_exchange:
        raise ValueError(f"Admin exchange {exchange_name} not found")

    # Check if handler exists for this exchange
    if exchange_name in self.websocket_handlers:
        # Reuse existing handler (EXISTING CODE)
        logger.info("Reusing existing handler", exchange=exchange_name, symbol=symbol.symbol)
        handler = self.websocket_handlers[exchange_name]

        # ... rest of existing WebSocket logic ...
    else:
        # No handler yet, create new one for this exchange
        logger.info("Creating new handler", exchange=exchange_name, symbol=symbol.symbol)
        await self._start_exchange_collector(admin_exchange, [symbol])
```

---

## Step 8: Testing (90 minutes)

### 8.1: Unit Tests

Create file: `tests/unit/test_live_collector_rest_fallback.py`

```python
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from fullon_ohlcv_service.ohlcv.live_collector import LiveOHLCVCollector
from fullon_ohlcv.models import Candle
from datetime import datetime, UTC


@pytest.mark.asyncio
async def test_detect_websocket_ohlcv_support_returns_true_on_success():
    """Test successful WebSocket detection."""
    collector = LiveOHLCVCollector()

    handler = AsyncMock()
    handler.subscribe_ohlcv = AsyncMock(return_value=True)
    handler.unsubscribe_ohlcv = AsyncMock()

    result = await collector._detect_websocket_ohlcv_support(handler, "BTC/USD")

    assert result is True
    handler.subscribe_ohlcv.assert_called_once()


@pytest.mark.asyncio
async def test_detect_websocket_ohlcv_support_returns_false_on_not_implemented():
    """Test that NotImplementedError triggers REST fallback."""
    collector = LiveOHLCVCollector()

    handler = AsyncMock()
    handler.subscribe_ohlcv.side_effect = NotImplementedError("Not supported")

    result = await collector._detect_websocket_ohlcv_support(handler, "BTC/USD")

    assert result is False


@pytest.mark.asyncio
async def test_fetch_latest_candle_returns_candle():
    """Test fetching latest candle via REST."""
    collector = LiveOHLCVCollector()

    handler = AsyncMock()
    handler.get_ohlcv = AsyncMock(return_value=[
        [1609459200000, 29000, 30000, 28000, 29500, 100],  # Older candle
        [1609462800000, 29500, 31000, 29000, 30500, 150],  # Latest complete
    ])

    candle = await collector._fetch_latest_candle(handler, "BTC/USD", "1h")

    assert candle is not None
    assert candle.close == 29500  # Should use second-to-last
    handler.get_ohlcv.assert_called_once_with("BTC/USD", timeframe="1h", limit=2)


@pytest.mark.asyncio
async def test_polling_loop_stops_when_not_running():
    """Test polling loop stops gracefully."""
    collector = LiveOHLCVCollector()
    collector.running = False

    handler = AsyncMock()
    symbol = MagicMock()
    symbol.symbol = "BTC/USD"
    symbol.updateframe = "1m"

    # Should exit immediately since running=False
    await collector._poll_symbol_loop("test", symbol, handler, 60)

    # No fetches should occur
    assert not hasattr(collector, '_fetch_latest_candle') or \
           collector._fetch_latest_candle.call_count == 0
```

### 8.2: Integration Test

Create file: `tests/integration/test_yahoo_rest_fallback.py`

```python
import pytest
import asyncio
from fullon_ohlcv_service.ohlcv.live_collector import LiveOHLCVCollector
from fullon_orm import DatabaseContext


@pytest.mark.asyncio
async def test_yahoo_finance_automatic_rest_fallback():
    """Integration test: Yahoo Finance uses REST polling automatically."""

    # Load Yahoo exchange and symbols from database
    async with DatabaseContext() as db:
        # Get Yahoo exchange (assumes it exists in database)
        exchanges = await db.cat_exchanges.get_all()
        yahoo_exchange = None
        for ex in exchanges:
            if ex.name.lower() == "yahoo":
                yahoo_exchange = ex
                break

        if not yahoo_exchange:
            pytest.skip("Yahoo Finance not configured in database")

        # Get Yahoo symbols
        symbols = await db.symbols.get_by_exchange(yahoo_exchange.id)

        if not symbols:
            pytest.skip("No Yahoo symbols configured")

    # Create collector
    collector = LiveOHLCVCollector(symbols=symbols)

    try:
        # Start collection
        await collector.start_collection()

        # Wait for one polling cycle
        await asyncio.sleep(2)

        # Verify REST polling is active
        for symbol in symbols:
            symbol_key = f"yahoo:{symbol.symbol}"
            assert symbol_key in collector.polling_tasks, \
                f"Polling task not created for {symbol_key}"
            assert not collector.polling_tasks[symbol_key].done(), \
                f"Polling task died for {symbol_key}"

        # Verify no WebSocket handlers
        assert "yahoo" not in collector.websocket_handlers, \
            "Yahoo should not use WebSocket"

    finally:
        # Cleanup
        await collector.stop_collection()

        # Verify tasks were cancelled
        for symbol in symbols:
            symbol_key = f"yahoo:{symbol.symbol}"
            if symbol_key in collector.polling_tasks:
                task = collector.polling_tasks[symbol_key]
                assert task.done() or task.cancelled(), \
                    f"Task not stopped for {symbol_key}"
```

---

## Step 9: Documentation (30 minutes)

### Update CLAUDE.md

Add section describing REST fallback:

```markdown
## REST Fallback for LiveOHLCVCollector

When WebSocket OHLCV is not available (e.g., Yahoo Finance), LiveOHLCVCollector
automatically falls back to REST polling.

### Architecture

- **WebSocket Mode**: Real-time subscription via subscribe_ohlcv()
- **REST Polling Mode**: Periodic fetching via get_ohlcv() at timeframe intervals

### Polling Intervals

Polling frequency matches timeframe to minimize API calls:
- 1m candles → poll every 60 seconds
- 5m candles → poll every 300 seconds
- 1h candles → poll every 3600 seconds
- 1d candles → poll every 86400 seconds

### Detection Logic

1. Try to create WebSocket handler
2. Test subscribe_ohlcv() with first symbol
3. If NotImplementedError or ImportError → use REST polling
4. If successful → use WebSocket mode

### Code Reuse

Shared utilities in `ohlcv/utils.py`:
- `timeframe_to_seconds()` - Convert timeframe to seconds
- `convert_to_candle_object()` - Convert raw data to Candle

### Example

```python
from fullon_ohlcv_service.ohlcv.live_collector import LiveOHLCVCollector

# Create collector
collector = LiveOHLCVCollector()

# Start collection (automatic detection)
await collector.start_collection()

# Kraken symbols → WebSocket mode
# Yahoo symbols → REST polling mode
```
```

---

## Testing Checklist

After implementation, verify:

- [ ] Kraken symbols still use WebSocket (existing behavior)
- [ ] Yahoo Finance symbols use REST polling (new behavior)
- [ ] Mixed exchanges work (some WebSocket, some REST)
- [ ] Polling intervals match timeframes (check logs)
- [ ] ProcessCache shows "live_rest_polling" for REST symbols
- [ ] ProcessCache shows "live_websocket" for WebSocket symbols
- [ ] Dynamic symbol addition works with REST
- [ ] Graceful shutdown cancels all polling tasks
- [ ] No memory leaks from uncancelled tasks
- [ ] Errors trigger exponential backoff

---

## Troubleshooting

### Issue: Yahoo still tries WebSocket

**Cause:** Detection logic not catching NotImplementedError

**Fix:** Check that try-catch includes NotImplementedError

### Issue: Polling too slow

**Cause:** Interval calculation wrong

**Fix:** Verify `timeframe_to_seconds()` works for all timeframes

### Issue: Duplicate candles saved

**Cause:** Not checking `last_candle_timestamps`

**Fix:** Verify timestamp comparison in polling loop

### Issue: Tasks not cancelled on shutdown

**Cause:** `running` flag not checked in loop

**Fix:** Add `while self.running:` check in polling loop

---

## Performance Validation

Monitor these metrics:

1. **API calls per minute** - Should be ~1/timeframe_seconds
2. **Memory usage** - Should be constant (no leaks)
3. **Latency** - Should be ≤ timeframe interval
4. **Error rate** - Should be low (<1%)

Use ProcessCache dashboard to view real-time status.

---

## Next Steps

After basic implementation:

1. Add adaptive polling (faster during market hours)
2. Add configurable backoff strategies
3. Add metrics export (Prometheus/Grafana)
4. Add alerting for prolonged polling failures
