# LLM_METHOD_REFERENCE.md

**🚨 MANDATORY FOR ALL LLMs: REPOSITORY METHODS ONLY ACCEPT ORM OBJECTS! 🚨**

## Complete Method Reference for Repository Pattern Usage

**CRITICAL WARNING**: Repository methods MUST receive ORM model instances, NEVER dictionaries or individual parameters!

### ⚠️ BEFORE READING THIS DOCUMENT:
1. **ALWAYS** import required ORM models first
2. **ALWAYS** create ORM objects before calling repository methods
3. **NEVER** pass dictionaries `{"field": "value"}` to repository methods
4. **NEVER** pass individual parameters `(field1, field2, field3)` to repository methods

### 🔧 Required Imports:
```python
from fullon_orm.models import (
    User, Bot, Order, Trade, DryTrade, Exchange, Symbol, Strategy
)
from fullon_orm.models.exchange import CatExchange, CatExchangeParam
from fullon_orm.models.strategy import CatStrategy, CatStrategyParam, StrategyParam
from fullon_orm.models.bot import BotLog
from fullon_orm.models.simulation import Simulation
```

## Base Repository Methods

All repositories inherit from `BaseRepository[T]` with these common methods:

### CRUD Operations
```python
async def get_by_id(self, id: int) -> Optional[T]
async def get_all(self, **filters) -> List[T]
async def delete(self, instance: T) -> bool
async def commit(self) -> None
async def rollback(self) -> None
async def flush(self) -> None
```

## UserRepository Methods

### User Creation & Management
```python
# Add user - USES MODEL INSTANCE ONLY
async def add_user(self, user: User) -> User
"""Adds user instance to database, handles role conversion"""

# Get user by email
async def get_by_email(self, email: str) -> Optional[User]

# Get user ID by email
async def get_user_id(self, mail: str) -> Optional[int]
"""Returns user ID or None if not found"""
```

### User Queries & Search
```python
# Get user list with pagination
async def get_user_list(self, page: int = 1, page_size: int = 10, all: bool = False) -> List[Dict]
"""Returns list of user dictionaries, supports pagination"""

# Get active users
async def get_active_users(self, page: int = 1, page_size: int = 10) -> List[User]

# Search users
async def search(self, query: str, page: int = 1, page_size: int = 10) -> List[User]
"""Searches by name, lastname, or email with ilike pattern matching"""
```

### User Updates & Management
```python
# ✅ CORRECT: Modify user - USES USER OBJECT
async def modify_user(self, user: User) -> Optional[User]
"""Updates user fields from User object - get existing user first, modify fields, then pass User object"""

# ✅ CORRECT Usage Pattern:
# existing_user = await db.users.get_by_id(user_id)
# existing_user.name = "New Name"
# updated_user = await db.users.modify_user(existing_user)

# Update password
async def update_password(self, uid: int, new_password: str) -> Optional[User]

# Toggle active status
async def toggle_active(self, uid: int) -> Optional[User]

# Remove user
async def remove_user(self, user_id: Optional[int] = None, email: Optional[str] = None) -> bool
"""Requires either user_id or email, returns True if removed"""
```

## BotRepository Methods

### Bot Creation & Management
```python
# ✅ CORRECT: Add bot - USES BOT OBJECT ONLY
async def add_bot(self, bot: Bot) -> Optional[Bot]
"""Adds bot instance, applies defaults: active=True, test=False, dry_run=True"""

# ✅ CORRECT: Edit bot - USES BOT OBJECT ONLY
async def edit_bot(self, bot: Bot) -> bool
"""Updates bot fields from Bot object - get existing bot first, modify fields, then pass Bot object"""

# ✅ CORRECT Usage Pattern:
# existing_bot = await db.bots.get_bot_params(bot_id)
# existing_bot.name = "Updated Bot Name"
# success = await db.bots.edit_bot(existing_bot)

# ✅ CORRECT: Save bot logs - USES LIST OF BOTLOG OBJECTS
async def save_bot_log(self, logs: List[BotLog]) -> bool
"""Saves bot log entries - pass list of BotLog objects"""

# ✅ CORRECT: Save simulation - USES SIMULATION OBJECT
async def save_simulation(self, simulation: Simulation) -> Optional[int]
"""Saves simulation - pass Simulation object"""
```

### Bot Queries
```python
# Get user's bots
async def get_user_bots(self, user_id: int, active_only: bool = True) -> List[Bot]

# Get bot with full details
async def get_bot_with_details(self, bot_id: int) -> Optional[Dict[str, Any]]
"""Returns bot with exchanges, strategies, feeds as comprehensive dictionary"""

# Get bot feeds
async def get_bot_feeds(self, bot_id: int) -> List[Dict]
"""Returns list of feeds with symbol and exchange information"""

# Get bot exchanges  
async def get_bot_exchanges(self, bot_id: int) -> List[Dict]
"""Returns exchanges associated with bot"""

# Get bot strategies
async def get_bot_strategies(self, bot_id: int) -> List[Dict]
"""Returns strategies with category information"""
```

### Bot Activity & Logs
```python
# Get last bot log timestamp
async def get_last_bot_log(self, bot_id: int) -> Optional[datetime]

# Get last N actions
async def get_last_actions(self, bot_id: int, limit: int = 10) -> List[BotLog]

# Get bot logs with filtering
async def get_logs(
    self, 
    bot_id: int,
    log_level: Optional[str] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    limit: int = 100
) -> List[BotLog]
"""Supports filtering by level (ERROR, WARNING, INFO) and time range"""
```

### Bot Statistics & Analysis
```python
# Get bot statistics
async def get_bot_stats(self, bot_id: int) -> Dict[str, Any]
"""Returns comprehensive stats: trades, profit/loss, uptime, etc."""

# Get active bots summary
async def get_active_bots_summary(self, user_id: int) -> List[Dict]
"""Summary of all active bots with key metrics"""
```

## ExchangeRepository Methods

### Exchange Setup & Management
```python
# ✅ CORRECT: Install exchange - USES CATEXCHANGE AND CATEXCHANGEPARAM OBJECTS
async def install_exchange(self, cat_exchange: CatExchange, params: Optional[List[CatExchangeParam]] = None) -> Optional[CatExchange]
"""Creates exchange catalog entry with optional parameters"""

# ✅ CORRECT Usage Pattern:
# cat_exchange = CatExchange(name="binance", ohlcv_view="binance_data")
# params = [CatExchangeParam(name="api_key", value="test_key")]
# result = await db.exchanges.install_exchange(cat_exchange, params)

# ✅ CORRECT: Add user exchange - USES EXCHANGE OBJECT
async def add_user_exchange(self, exchange: Exchange) -> Optional[Exchange]
"""Adds user's exchange configuration, applies default active=True"""

# ✅ CORRECT Usage Pattern:
# exchange = Exchange(uid=user_id, cat_ex_id=cat_ex_id, name="my_binance", active=True)
# result = await db.exchanges.add_user_exchange(exchange)

# Remove user exchange
async def remove_user_exchange(self, ex_id: int) -> bool
```

### Exchange Queries (Cached)
```python
# Get user exchanges (CACHED)
async def get_user_exchanges(self, user_id: int) -> List[Dict]
"""Returns user's configured exchanges with full details"""

# Get exchange catalog (CACHED)
async def get_cat_exchanges(
    self, 
    exchange: Optional[str] = None, 
    all: bool = False,
    page: int = 1, 
    page_size: int = 10
) -> List[CatExchange]
"""Get available exchanges, supports filtering and pagination"""

# Get exchange parameters (CACHED)
async def get_exchanges_params(self, cat_ex_id: int) -> List[Dict]
"""Returns configuration parameters for exchange"""

# Get exchange ID by name (CACHED)  
async def get_exchange_id(self, name: str, user_id: int) -> Optional[int]

# Get exchange category ID (CACHED)
async def get_exchange_cat_id(self, name: Optional[str] = None, ex_id: Optional[int] = None) -> Optional[int]
```

### Exchange Symbols (Cached)
```python
# Get exchange symbols (CACHED)
async def get_exchange_symbols(self, cat_ex_id: int) -> List[Symbol]
"""Returns all symbols available on exchange"""

# Get exchange details
async def get_exchange(
    self,
    ex_id: Optional[int] = None,
    user_id: Optional[int] = None, 
    exchange_name: Optional[str] = None
) -> List[Dict]
"""Flexible exchange lookup with multiple filter options"""
```

## SymbolRepository Methods

### Symbol Queries (Cached)
```python
# Get symbol by name (CACHED)
async def get_by_symbol(self, symbol: str, cat_ex_id: Optional[int] = None) -> Optional[Symbol]
"""Primary symbol lookup method with exchange filtering"""

# Get symbols by exchange (CACHED)
async def get_by_exchange_id(self, cat_ex_id: int) -> List[Symbol]
"""All symbols for specific exchange"""

# Get all symbols with filtering
async def get_all(
    self,
    exchange_name: Optional[str] = None,
    active_only: bool = False,
    limit: Optional[int] = None
) -> List[Symbol]
"""Supports exchange filtering and active symbol filtering"""
```

### Symbol Information
```python
# Get symbol decimals
async def get_symbol_decimals(self, symbol: str, cat_ex_id: Optional[int] = None) -> Optional[int]

# Search symbols by pattern
async def search_symbols(self, pattern: str, exchange_id: Optional[int] = None) -> List[Symbol]
"""Search with SQL LIKE pattern matching"""
```

## StrategyRepository Methods

### Strategy Management
```python
# ✅ CORRECT: Add bot strategy - USES STRATEGY OBJECT
async def add_bot_strategy(self, strategy: Strategy) -> Optional[Strategy]
"""Adds strategy instance, fills defaults from template"""

# ✅ CORRECT: Install strategy - USES CATSTRATEGY AND CATSTRATEGYPARAMS OBJECTS
async def install_strategy(self, cat_strategy: CatStrategy, params: Optional[List[CatStrategyParam]] = None) -> Optional[int]
"""Creates or updates strategy template with parameters"""

# ✅ CORRECT Usage Pattern:
# cat_strategy = CatStrategy(name="ma_crossover", description="MA strategy")
# params = [CatStrategyParam(name="fast_period", value="12")]
# result = await db.strategies.install_strategy(cat_strategy, params)

# ✅ CORRECT: Edit base strategy params - USES STRATEGY OBJECT
async def edit_base_strat_params(self, strategy: Strategy) -> bool
"""Updates strategy base parameters from Strategy object"""

# ✅ CORRECT: Edit strategy params - USES LIST OF STRATEGYPARAM OBJECTS
async def edit_strat_params(self, params: List[StrategyParam]) -> bool
"""Updates custom strategy parameters from StrategyParam objects"""

# Delete bot strategy
async def del_bot_strategy(self, bot_id: int) -> bool
```

### Strategy Queries
```python
# Get user strategies
async def get_user_strategies(self, user_id: int) -> List[Dict]
"""Returns strategies with bot and category information"""

# Get strategy catalog
async def get_cat_strategies(self) -> List[CatStrategy]
"""Available strategy templates"""

# Get strategy parameters
async def get_cat_strategies_params(self, cat_str_id: int) -> List[Dict]
"""Configuration parameters for strategy"""

# Get category strategy ID
async def get_cat_str_id(self, name: str) -> Optional[int]

# Get strategy details
async def get_cat_strategy(self, cat_str_id: int) -> Optional[CatStrategy]
```

### Strategy Management
```python
# Delete strategy template
async def del_cat_strategy(self, cat_str_id: Optional[int] = None, name: Optional[str] = None) -> bool
```

## TradeRepository Methods

### Trade Recording
```python
# ✅ CORRECT: Save dry trade - USES DRYTRADE OBJECT
async def save_dry_trade(self, dry_trade: DryTrade) -> bool
"""Saves simulated trade, requires DryTrade model instance"""

# ✅ CORRECT: Save live trades - USES LIST OF TRADE OBJECTS
async def save_trades(self, trades: List[Trade]) -> bool
"""Saves live trades, requires list of Trade model instances"""

# ✅ CORRECT Usage Pattern:
# trades = [Trade(symbol="BTC/USD", side="buy", volume=1.0, price=45000.0)]
# success = await db.trades.save_trades(trades)

# ✅ CORRECT: Update trade - USES TRADE OBJECT
async def update_trade(self, trade: Trade) -> None
"""Updates trade fields from Trade object"""

# ✅ CORRECT Usage Pattern:
# trade_update = Trade(trade_id=trade_id)
# trade_update.roi = 150.50
# trade_update.roi_pct = 3.35
# await db.trades.update_trade(trade_update)
```

### Trade Queries
```python
# Get trades by bot
async def get_bot_trades(
    self,
    bot_id: int,
    trade_type: str = "dry",  # "dry" or "live"
    limit: int = 100
) -> List[Union[DryTrade, Trade]]

# Get trades by symbol
async def get_symbol_trades(self, symbol: str, bot_id: Optional[int] = None) -> List[Dict]

# Get trade statistics
async def get_trade_stats(self, bot_id: int, days: int = 30) -> Dict[str, Any]
"""Returns P&L, win rate, trade count, etc."""
```

## OrderRepository Methods

### Order Management
```python
# ✅ CORRECT: Save order - USES ORDER OBJECT (field 'volume', NOT 'amount')
async def save_order(self, order: Order) -> Optional[int]
"""Creates order from Order model instance. CRITICAL: Use 'volume' field, NOT 'amount'"""

# ✅ CORRECT Usage Pattern:
# order = Order(
#     bot_id=bot_id,
#     ex_id=ex_id,
#     symbol="BTC/USD",
#     side="buy",
#     volume=1.0,  # ✅ CORRECT: Use 'volume', NOT 'amount'
#     price=45000.0,
#     order_type="market",
#     status="New"
# )
# order_id = await db.orders.save_order(order)

# Update order status
async def update_order_status(self, order_id: int, status: str) -> bool

# Get orders by bot
async def get_bot_orders(
    self,
    bot_id: int,
    status: Optional[str] = None,
    limit: int = 100
) -> List[Order]
"""Supports status filtering: PENDING, FILLED, CANCELLED, REJECTED"""
```

### Order Queries
```python
# Get open orders
async def get_open_orders(self, bot_id: Optional[int] = None) -> List[Order]

# Get order history
async def get_order_history(
    self,
    bot_id: int,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
) -> List[Order]
```

## Error Handling Patterns

### Repository Error Handling
```python
# All repositories follow consistent error handling:
try:
    result = await repo.some_method()
    await session.commit()  # Always commit after modifications
except SQLAlchemyError as e:
    await session.rollback()  # Rollback on database errors
    logger.warning(f"Database error: {e}")
    # Methods return safe defaults: None, False, or []
except Exception as e:
    await session.rollback()
    logger.error(f"Unexpected error: {e}")
    raise  # Re-raise unexpected errors
```

### Safe Defaults
- **None**: For single object lookups that fail
- **False**: For boolean operations that fail  
- **[]**: For list operations that fail
- **Exceptions**: Re-raised for data integrity violations

## Model Instance Usage (Important!)

### Correct Usage - Model Instances
```python
# ✅ CORRECT: Use model instances
user = User(mail="test@example.com", password="hash", f2a="", name="John", lastname="Doe", phone="", id_num="")
result = await user_repo.add_user(user)

bot = Bot(uid=user.uid, name="MyBot", active=True, test=False, dry_run=True)
result = await bot_repo.add_bot(bot)

strategy = Strategy(bot_id=bot.bot_id, cat_str_id=1, take_profit=0.02, stop_loss=0.01)
result = await strategy_repo.add_bot_strategy(strategy)

dry_trade = DryTrade(bot_id=bot.bot_id, symbol="BTC/USD", side="buy", volume=0.001, price=50000.0)
result = await trade_repo.save_dry_trade(dry_trade)

exchange = Exchange(uid=user.uid, cat_ex_id=1, name="my_binance", active=True)
result = await exchange_repo.add_user_exchange(exchange)
```

### Incorrect Usage - Dictionaries (Will Fail!)
```python
# ❌ WRONG: Don't use dictionaries for these methods
await user_repo.add_user({"mail": "test@example.com"})  # TypeError!
await bot_repo.add_bot({"name": "MyBot"})  # TypeError!
await strategy_repo.add_bot_strategy({"bot_id": 1})  # TypeError!
```

## Session Management Pattern

```python
from fullon_orm.database_context import DatabaseContext

async def business_logic():
    async with DatabaseContext() as db:
        # Direct repository access
        user = User(mail="test@example.com", name="John")
        user = await db.users.add_user(user)
        bot = Bot(uid=user.uid, name="MyBot")
        bot = await db.bots.add_bot(bot)
        
        await db.commit()
        return {"user_id": user.uid, "bot_id": bot.bot_id}
```

## Cache Invalidation

Repositories with caching automatically invalidate on modifications:

### Cached Repositories & Methods
- **SymbolRepository**: `get_by_symbol()`, `get_by_exchange_id()`
- **ExchangeRepository**: `get_user_exchanges()`, `get_cat_exchanges()`, `get_exchange_cat_id()`, `get_exchanges_params()`, `get_exchange_symbols()`, `get_exchange()`, `get_exchange_id()`

### Cache Keys Pattern
```
symbols:get:{symbol}:{cat_ex_id}
symbols:by_id:{symbol_id}  
symbols:by_ex_id:{cat_ex_id}
exchanges:user_exchanges:{user_id}
exchanges:cat_exchanges:{exchange}:{page}:{page_size}:{all}
exchanges:id:{name}:{user_id}
```

## Timezone Handling

All datetime operations use UTC:
```python
from datetime import datetime, timezone

# ✅ CORRECT: Use UTC timezone
created_at = datetime.now(timezone.utc)

# For PostgreSQL storage, convert to timezone-naive
stored_time = created_at.replace(tzinfo=None)
```

## Common Patterns Summary

1. **Always use model instances** for add/create operations
2. **Use async/await** for all database operations  
3. **Handle exceptions** with rollback and safe defaults
4. **Share sessions** across related operations
5. **Commit once** at the end of business logic
6. **Use repositories** instead of direct model access
7. **Leverage caching** for read-heavy operations
8. **Follow UTC timezone** conventions