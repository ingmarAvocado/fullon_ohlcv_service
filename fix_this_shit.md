# Fix This Shit: Issue #39 + fullon_orm Compatibility

## Current Situation Analysis

**Branch Status**: `feature/issue-39-websocket-trade-collection` with significant work in progress
**Problem**: Outdated fullon_orm API usage preventing Issue #39 implementation from working
**Goal**: Fix fullon_orm integration AND complete Issue #39 implementation

## Two-Phase Solution Strategy

### Phase 1: Critical fullon_orm Compatibility Fix (2-3 hours)

**Problem**: Code uses old dictionary-based fullon_orm API that now requires ORM model instances

**Critical Files to Fix:**
1. `src/fullon_ohlcv_service/config/database_config.py` - Replace `.get()` calls with direct attribute access
2. `examples/test_two_phase_collection.py` - Update ORM model usage patterns
3. `src/fullon_ohlcv_service/trade/live_collector.py` - Fix Symbol object access
4. `src/fullon_ohlcv_service/trade/historic_collector.py` - Update Trade model usage

**Key Changes Required:**
- Replace `exchange.get('id')` → `exchange.id`
- Replace `symbol.get('symbol')` → `symbol.symbol`
- Add required imports: `from fullon_orm.models import Trade, Symbol, Exchange`
- Use `volume` field instead of `amount` in Trade objects

### Phase 2: Complete Issue #39 Implementation (4-5 hours)

**Already Implemented (Good Foundation):**
- ✅ `LiveTradeCollector` class created
- ✅ `GlobalTradeBatcher` class created
- ✅ `HistoricTradeCollector` modifications made
- ✅ Redis integration with TradesCache
- ✅ WebSocket connection handling framework

**Remaining Work:**
1. **Fix ORM integration** in all collector classes
2. **Update test_two_phase_collection.py** to work like `fullon_ticker_service` pattern
3. **Validate the workflow**: WebSocket → Redis cache → batch save to fullon_orm
4. **Test end-to-end functionality** with real data

### Phase 3: Integration Testing & Validation (1-2 hours)

**Success Criteria from Issue #39:**
- [ ] Historical collection completes successfully
- [ ] WebSocket streaming starts automatically after historical phase
- [ ] Fresh trade data appears (< 2 minutes old) in monitoring
- [ ] No data loss during historical→live transition
- [ ] Batch processing works every minute

## Implementation Order

1. **Fix fullon_orm compatibility** (blocks everything else)
2. **Test basic database operations** (ensure ORM integration works)
3. **Validate test_two_phase_collection.py** (main validation script)
4. **End-to-end testing** (complete Issue #39 workflow)

## Expected Outcome

After completion:
- Issue #39 fully implemented and working
- Compatible with current fullon_orm model-based API
- `test_two_phase_collection.py` demonstrates working real-time pipeline
- Ready for production deployment

**Total Effort**: 7-10 hours over 2-3 days
**Risk**: Medium (foundation exists, mainly integration fixes needed)

---

## Detailed Breaking Changes from fullon_orm

### OLD (Dictionary-based API) - BROKEN:
```python
# This will FAIL with current fullon_orm
await db.users.add_user("test@example.com", "John")
await db.trades.save_trades([{"symbol": "BTC/USD"}])

# Dictionary access patterns
exchange_id = exchange.get('id')
symbol_name = symbol.get('symbol')
```

### NEW (ORM Model-based API) - REQUIRED:
```python
# Required imports first
from fullon_orm.models import User, Trade, Symbol, Exchange

# Create ORM objects, then pass to repository
user = User(mail="test@example.com", name="John")
await db.users.add_user(user)

trades = [Trade(symbol="BTC/USD", side="buy", volume=1.0)]
await db.trades.save_trades(trades)

# Direct attribute access (no .get())
exchange_id = exchange.id
symbol_name = symbol.symbol
```

### Critical Field Changes:
- **Order model**: Use `volume` field, NEVER `amount`
- **All models**: Use `obj.field` syntax, NEVER `obj['field']`

---

## Files That Need Immediate Fixing

### 1. `src/fullon_ohlcv_service/config/database_config.py`
**Problem**: Uses `.get()` dictionary methods on ORM objects
**Fix**: Replace all `.get('field')` with direct `.field` access

### 2. `examples/test_two_phase_collection.py`
**Problem**: May use old ORM patterns
**Fix**: Update to use proper ORM model instances

### 3. All Collector Classes
**Problem**: Symbol/Exchange object access patterns
**Fix**: Use direct attribute access instead of dictionary methods

### 4. Test Files
**Problem**: Mock objects return dictionaries instead of ORM objects
**Fix**: Update mocks to return proper ORM model instances

---

## Quick Fix Checklist

- [ ] Fix database_config.py ORM object access
- [ ] Update all collector Symbol/Exchange access patterns
- [ ] Add required fullon_orm model imports
- [ ] Change 'amount' to 'volume' in Trade objects
- [ ] Update test mocks to return ORM objects
- [ ] Test basic database operations work
- [ ] Run test_two_phase_collection.py successfully
- [ ] Validate end-to-end Issue #39 workflow

**Priority**: Fix database_config.py FIRST - it blocks everything else