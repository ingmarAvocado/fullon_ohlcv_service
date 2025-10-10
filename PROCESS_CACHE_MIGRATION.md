# ProcessCache API Migration Summary

## Overview
Successfully migrated from the old ProcessCache API (`new_process`/`delete_process`) to the new API (`register_process`/`stop_process`).

## Changes Made

### 1. `/home/ingmar/code/fullon_ohlcv_service/src/fullon_ohlcv_service/daemon.py`

#### Added Imports (lines 11-12)
```python
from fullon_cache import ProcessCache
from fullon_cache.process_cache import ProcessType, ProcessStatus
```

#### Added process_id Attribute (line 32)
```python
self.process_id: str | None = None
```

#### Updated _register_daemon Method (lines 34-43)
**Before:**
```python
async def _register_daemon(self) -> None:
    """Register daemon in ProcessCache for health monitoring"""
    async with ProcessCache() as cache:
        await cache.new_process(
            tipe="ohlcv_service",
            key="ohlcv_daemon",
            pid=os.getpid(),
            params=["ohlcv_daemon"],
            message="Started",
        )
```

**After:**
```python
async def _register_daemon(self) -> None:
    """Register daemon in ProcessCache for health monitoring"""
    async with ProcessCache() as cache:
        self.process_id = await cache.register_process(
            process_type=ProcessType.OHLCV,
            component="ohlcv_daemon",
            params={"pid": os.getpid(), "args": ["ohlcv_daemon"]},
            message="Started",
            status=ProcessStatus.STARTING
        )
```

#### Updated cleanup Method (lines 109-115)
**Before:**
```python
# Clean up ProcessCache registration
try:
    async with ProcessCache() as cache:
        await cache.delete_process(tipe="ohlcv_service", key="ohlcv_daemon")
except Exception as e:
    self.logger.warning(f"Could not clean up ProcessCache: {e}")
```

**After:**
```python
# Clean up ProcessCache registration
try:
    async with ProcessCache() as cache:
        if hasattr(self, 'process_id') and self.process_id:
            await cache.stop_process(self.process_id, "Daemon shutdown")
except Exception as e:
    self.logger.warning(f"Could not clean up ProcessCache: {e}")
```

### 2. `/home/ingmar/code/fullon_ohlcv_service/tests/unit/test_health_monitoring.py`

Updated all test mocks to use the new API:

#### test_register_process (lines 28-42)
- Added mock return value: `mock_cache.register_process.return_value = "mock_process_id"`
- Changed assertion from `new_process` to `register_process`
- Updated parameters to match new API signature

#### test_cleanup_process (lines 79-89)
- Added `self.manager.process_id = "mock_process_id"`
- Changed assertion from `delete_process` to `stop_process`
- Updated parameters to match new API signature

#### test_cleanup_process_with_error (lines 93-108)
- Changed mock side effect from `delete_process` to `stop_process`
- Added `self.manager.process_id = "mock_process_id"`
- Updated assertion to use `stop_process`

#### test_start_registers_process (lines 163-173)
- Added mock return value: `mock_cache.register_process.return_value = "mock_process_id"`
- Changed assertion from `new_process` to `register_process`

#### test_stop_cleans_up_process (lines 188-198)
- Added mock return value: `mock_cache.register_process.return_value = "mock_process_id"`
- Changed assertion from `delete_process` to `stop_process`
- Updated parameters to match new API signature

## API Differences

### Old API
```python
# Register
await cache.new_process(
    tipe="ohlcv_service",
    key="ohlcv_daemon",
    pid=os.getpid(),
    params=["ohlcv_daemon"],
    message="Started"
)

# Delete
await cache.delete_process(tipe="ohlcv_service", key="ohlcv_daemon")
```

### New API
```python
# Register - returns process_id
process_id = await cache.register_process(
    process_type=ProcessType.OHLCV,
    component="ohlcv_daemon",
    params={"pid": os.getpid(), "args": ["ohlcv_daemon"]},
    message="Started",
    status=ProcessStatus.STARTING
)

# Stop - uses process_id
await cache.stop_process(process_id, "Shutdown reason")
```

## Key Changes
1. **Returns process_id**: `register_process()` now returns a process ID that must be stored
2. **Type-safe enums**: Uses `ProcessType.OHLCV` and `ProcessStatus.STARTING` instead of strings
3. **Dictionary params**: `params` is now a dictionary instead of a list
4. **ID-based cleanup**: `stop_process()` uses the returned process_id instead of tipe/key lookup
5. **Shutdown reason**: `stop_process()` requires a reason message

## Verification

To verify the migration is complete:
```bash
cd /home/ingmar/code/fullon_ohlcv_service
python examples/run_example_pipeline.py
```

Expected behavior:
- No `AttributeError: 'ProcessCache' object has no attribute 'new_process'`
- No `AttributeError: 'ProcessCache' object has no attribute 'delete_process'`
- Daemon registers successfully with ProcessCache
- Daemon cleans up properly on shutdown

## Status
✅ **COMPLETE** - All code changes implemented and ready for testing with proper environment setup.
