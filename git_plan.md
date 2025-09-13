# 🎯 fullon_ohlcv_service - CORRECTED Development Plan

## **CRITICAL CORRECTION NEEDED**

**Current Issues #11-20 should be CLOSED immediately.** They are over-engineered and skip foundation.

**Read `fix_this_fuck.md` for detailed explanation of what went wrong.**

## **Corrected Project Overview** 

**Mission**: Simple async daemon for OHLCV/trade collection using fullon ecosystem integration.

**Scope**: ~300-500 lines of integration code, NOT a data collection framework.

**Architecture - fullon Ecosystem Integration**: 
- **fullon_orm**: Database operations, exchange/symbol configuration from DB (NO hardcoded lists)
- **fullon_exchange**: WebSocket data collection, handles ALL connection/reconnection logic
- **fullon_ohlcv**: Database storage via CandleRepository/TradeRepository, proper models
- **fullon_cache**: ProcessCache for health monitoring, OHLCVCache for real-time updates  
- **fullon_log**: Structured component logging with proper context
- **This service**: ~300-500 lines of simple coordination/integration code ONLY

## **Foundation Issues Needed (#1-10)**

The service needs these basic issues implemented BEFORE any advanced features:

**📋 FOUNDATION ISSUES WITH fullon LIBRARY INTEGRATION:**

- **Issue #1**: Basic OhlcvCollector 
  - Use `fullon_exchange.queue.ExchangeQueue` for data collection
  - Use `fullon_ohlcv.repositories.ohlcv.CandleRepository` for storage
  - Use `fullon_log.get_component_logger` for logging
  
- **Issue #2**: Basic TradeCollector
  - Use `fullon_exchange.queue.ExchangeQueue` for trade stream subscription  
  - Use `fullon_ohlcv.repositories.trade.TradeRepository` for storage
  - Follow same patterns as OhlcvCollector
  
- **Issue #3**: Database-Driven Configuration
  - Use `fullon_orm.database_context.DatabaseContext` for DB access
  - Use `db.exchanges.get_user_exchanges()` and `db.symbols.get_by_exchange_id()`
  - NO hardcoded exchange/symbol lists - all from database
  
- **Issue #4**: Simple Daemon Coordination  
  - Use `fullon_log.get_component_logger("fullon.ohlcv.daemon")` 
  - Coordinate collectors using database configuration
  
- **Issue #5**: Health Monitoring Integration
  - Use `fullon_cache.ProcessCache` for daemon status updates
  - Follow ticker service patterns for health reporting

## **Action Required**

1. **CLOSE Issues #11-20** - They skip foundation and are over-scoped
2. **CREATE Issues #2, #3, #5, #7-10** - Foundation implementation
3. **Implement remaining foundation issues** in order
4. **THEN** consider recreating advanced features with proper scope

## **Key Rules: fullon Ecosystem Integration**

### **MANDATORY fullon Library Usage:**

1. **fullon_orm**: ALL configuration from database
   ```python
   # ✅ CORRECT - Database-driven
   async with DatabaseContext() as db:
       exchanges = await db.exchanges.get_user_exchanges(user_id=1)
   
   # ❌ WRONG - Hardcoded
   # exchanges = ["kraken", "binance"]
   ```

2. **fullon_exchange**: ALL data collection 
   ```python
   # ✅ CORRECT - Use ExchangeQueue
   handler = await ExchangeQueue.get_handler("kraken", "ohlcv_account")
   candles = await handler.get_ohlcv("BTC/USD", "1m")
   
   # ❌ WRONG - Direct exchange APIs
   # import kraken; kraken.get_ohlcv(...)
   ```

3. **fullon_ohlcv**: ALL database storage
   ```python
   # ✅ CORRECT - Use repositories 
   async with CandleRepository("kraken", "BTC/USD", test=False) as repo:
       success = await repo.save_candles(candles)
   
   # ❌ WRONG - Direct SQL
   # cursor.execute("INSERT INTO ohlcv...")
   ```

4. **fullon_cache**: Health monitoring & real-time updates
5. **fullon_log**: Component-specific structured logging

## **Current Status: Foundation Implementation Needed**

**After rollback, all core files are back to comment stubs:**
- `src/fullon_ohlcv_service/daemon.py`: "# Main daemon service launcher"  
- `src/fullon_ohlcv_service/ohlcv/collector.py`: "# OHLCV Collector - Single symbol OHLCV candle collection"
- `src/fullon_ohlcv_service/config.py`: "# Configuration management"

**Next Steps:**
1. Create foundation Issues #1-5 with proper fullon library integration requirements
2. Implement using patterns shown in CLAUDE.md 
3. Focus on fullon_orm, fullon_exchange, fullon_ohlcv, fullon_cache, fullon_log integration
4. Keep each component under 100 lines - simple integration code only

## **Remember: Follow fullon_ticker_service Success Pattern**

The ticker service got the planning right by:
- Understanding fullon ecosystem capabilities
- Only creating Issues #6-15 (skipped 5 foundation issues, not 10)
- Focusing on integration rather than framework building
- Leveraging existing fullon libraries properly

**This service should follow the same approach with proper fullon library integration.**

---

**✅ BOTTOM LINE:** Create Issues #1-5 with mandatory fullon_orm, fullon_exchange, fullon_ohlcv, fullon_cache, and fullon_log integration. Keep it simple. Follow ticker service success patterns.
