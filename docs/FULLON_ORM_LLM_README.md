# LLM_README.md

**🚨 CRITICAL FOR ALL LLMs: Repository methods ONLY accept ORM objects - NEVER dictionaries! 🚨**

## Project Overview
Async SQLAlchemy ORM library for Fullon trading system. Provides Python ORM models for PostgreSQL database with async/await patterns and connection pooling.

### ⚠️ MANDATORY RULE: ORM Objects Only
**ALL repository methods expect ORM model instances, NOT dictionaries or individual parameters!**

## Core Architecture
- **Async-First**: SQLAlchemy 2.0+ with uvloop optimization (2x performance boost)
- **Repository Pattern**: Clean abstraction over database operations
- **Model-Based API**: All repository methods use ORM model instances (not dictionaries)
- **Redis Caching**: 24-hour TTL with automatic invalidation
- **Connection Pooling**: 20 connections, 10 overflow, hourly recycling

## Quick Start (Model-Based API)

### 🔧 Required Imports First:
```python
from fullon_orm import DatabaseContext
from fullon_orm.models import User, Bot, Order, Trade, DryTrade, Exchange, Strategy
from fullon_orm.models.exchange import CatExchange, CatExchangeParam
from fullon_orm.models.strategy import CatStrategy, CatStrategyParam, StrategyParam
from fullon_orm.models.bot import BotLog
from fullon_orm.models.simulation import Simulation
```

### ✅ CORRECT Usage Pattern:
```python
async def example():
    async with DatabaseContext() as db:
        # ✅ CORRECT: Create ORM objects first, then pass to repository methods
        user = User(mail="test@example.com", name="John", password="hashed", f2a="", lastname="", phone="", id_num="")
        user = await db.users.add_user(user)

        # ✅ CORRECT: Create bot with ORM object
        bot = Bot(uid=user.uid, name="MyBot", active=True, dry_run=True)
        bot = await db.bots.add_bot(bot)

        # ✅ CORRECT: Create order with 'volume' field
        order = Order(bot_id=bot.bot_id, ex_id=1, symbol="BTC/USD", side="buy", volume=1.0, order_type="market", status="New")
        order_id = await db.orders.save_order(order)

        await db.commit()

### ❌ WRONG Usage Pattern:
```python
async def wrong_example():
    async with DatabaseContext() as db:
        # ❌ WRONG: NEVER pass individual parameters
        user = await db.users.add_user("test@example.com", "John")  # ERROR!

        # ❌ WRONG: NEVER pass dictionaries
        bot = await db.bots.add_bot({"name": "MyBot"})  # ERROR!

        # ❌ WRONG: NEVER use 'amount' field (use 'volume')
        order = Order(amount=1.0)  # ERROR! Use 'volume'
```

## Project Structure
```
fullon_orm/
├── src/fullon_orm/
│   ├── models/          # ORM models (user, bot, exchange, order, trade, etc.)
│   ├── repositories/    # Repository pattern implementations
│   ├── database.py      # Connection management with pooling
│   ├── cache.py         # Redis caching with dogpile.cache
│   └── base.py          # SQLAlchemy declarative base
├── tests/               # 100% coverage test suite
└── alembic/             # Database migration management
```

## Key Components

### Models
- **User**: Authentication, roles, relationships to bots/exchanges
- **Bot**: Trading bots with strategies, feeds, logs, simulations
- **Exchange**: User exchange configs with API credentials
- **Order/Trade**: Order management and trade execution tracking
- **Symbol**: Trading pairs with exchange associations
- **Strategy/Feed**: Strategy definitions and data feeds

### Repositories (Model-Based API) - ALL METHODS REQUIRE ORM OBJECTS

- **BaseRepository**: Common CRUD (get_by_id, get_all, delete, commit, rollback)
- **BotRepository**:
  - ✅ `add_bot(bot: Bot)` - Complex bot queries with feeds, strategies, logs
  - ✅ `edit_bot(bot: Bot)` - Updates bot using Bot object
  - ✅ `save_bot_log(logs: List[BotLog])` - Logs using BotLog objects
  - ✅ `save_simulation(simulation: Simulation)` - Simulation using Simulation object
- **ExchangeRepository**:
  - ✅ `add_user_exchange(exchange: Exchange)` - Exchange management with caching
  - ✅ `install_exchange(cat_exchange: CatExchange, params: List[CatExchangeParam])` - Uses ORM objects
- **SymbolRepository**: Symbol operations with caching (mostly query methods)
- **UserRepository**:
  - ✅ `add_user(user: User)` - User management with search and auth
  - ✅ `modify_user(user: User)` - Updates using User object
- **OrderRepository**:
  - ✅ `save_order(order: Order)` - CRITICAL: Use 'volume' field, NOT 'amount'
- **TradeRepository**:
  - ✅ `save_dry_trade(dry_trade: DryTrade)` - Live and dry trade operations
  - ✅ `save_trades(trades: List[Trade])` - Uses list of Trade objects
  - ✅ `update_trade(trade: Trade)` - Updates using Trade object
- **StrategyRepository**:
  - ✅ `add_bot_strategy(strategy: Strategy)` - Strategy management
  - ✅ `install_strategy(cat_strategy: CatStrategy, params: List[CatStrategyParam])` - Uses ORM objects
  - ✅ `edit_base_strat_params(strategy: Strategy)` - Uses Strategy object
  - ✅ `edit_strat_params(params: List[StrategyParam])` - Uses StrategyParam objects

**📚 For complete method documentation, see [LLM_METHOD_REFERENCE.md](LLM_METHOD_REFERENCE.md)**

### Caching (Redis)
- **Symbol Repository**: get_by_symbol, get_by_exchange_id
- **Exchange Repository**: user exchanges, exchange params, catalog
- **Automatic invalidation** on create/update/delete operations
- **Graceful fallback** to database when Redis unavailable

## Environment Setup
```env
# Database
DB_USER=postgres_user
DB_PASSWORD=postgres_password  
DB_NAME=fullon2

# Cache (Redis)
CACHE_HOST=localhost
CACHE_PORT=6379
CACHE_DB=0  # Production: 0, Testing: 1

# Performance
UVLOOP_ENABLED=true  # 2x async performance boost
```

## Model-Based API (Important!)

### ✅ Correct Usage - Model Instances
```python
from fullon_orm.models import User, Bot, Strategy, DryTrade, Exchange

# Use model instances for repository methods
user = User(mail="test@example.com", name="John", f2a="", lastname="", phone="", id_num="")
await user_repo.add_user(user)

bot = Bot(uid=user.uid, name="MyBot", active=True, dry_run=True)  
await bot_repo.add_bot(bot)

strategy = Strategy(bot_id=bot.bot_id, cat_str_id=1, take_profit=0.02)
await strategy_repo.add_bot_strategy(strategy)
```

### ❌ Incorrect Usage - Dictionaries (Will Fail!)
```python
# These will cause TypeError - don't use dictionaries!
await bot_repo.add_bot({"name": "MyBot"})  # ❌ TypeError!
await strategy_repo.add_bot_strategy({"bot_id": 1})  # ❌ TypeError!
```

### Dictionary Conversion (For API Integration)
```python
# Models support conversion for API responses
user = User.from_dict({"mail": "test@example.com", "name": "John"})
user_dict = user.to_dict()  # JSON-serializable dictionary
```

## Self-Documenting Package
```python
# Complete understanding via imports
from fullon_orm import docs, examples
print(docs.QUICK_START)          # Installation and usage
print(docs.REPOSITORY_USAGE)     # Repository patterns
print(examples.REPOSITORY_PATTERN) # Working examples

# Model/repository documentation
help(User)                       # Complete model docs
help(BotRepository)             # Repository method docs
```

## Common Commands
```bash
# Install
poetry add  git+ssh://github.com/ingmarAvocado/fullon_orm.git

# Test
poetry run pytest --cov=fullon_orm

# Format/Lint
poetry run black . && poetry run ruff check .

# Database setup
poetry run alembic upgrade head
```

## Key Features
- 100% async/await support with uvloop optimization
- Repository pattern with consistent error handling
- Redis caching with automatic invalidation
- Connection pooling optimized for high concurrency
- Complete type hints and comprehensive test coverage
- Self-contained documentation accessible via Python imports
- Dictionary conversion methods for all models
- PostgreSQL-specific features (UUID, custom aggregates)

## Testing Strategy
- Each test runs in isolated PostgreSQL database
- Real database operations (no mocks except error paths)
- Parallel execution support with pytest-xdist (robust event loop handling)
- Cache testing with mock Redis
- 100% coverage on all repository modules
- TDD-based approach with comprehensive test isolation

## Recent Standardization (2024) - CRITICAL CHANGES

🚨 **BREAKING CHANGE**: All repository methods now REQUIRE ORM model instances instead of dictionaries

### What Changed:
- **Model-Based API**: ALL repository methods now use ORM model instances ONLY
- **NO MORE DICTIONARIES**: Repository methods will FAIL if passed dictionaries or individual parameters
- **NO MORE INDIVIDUAL PARAMETERS**: Methods like `add_user(email, name)` no longer exist
- **Type Safety**: Enhanced type hints prevent dictionary usage at compile-time
- **Field Name Changes**: Order model uses `volume` field, NOT `amount`

### Migration Required:
```python
# OLD (BROKEN):
await db.users.add_user("test@example.com", "John")
await db.trades.save_trades([{"symbol": "BTC/USD"}])

# NEW (REQUIRED):
user = User(mail="test@example.com", name="John")
await db.users.add_user(user)

trades = [Trade(symbol="BTC/USD", side="buy", volume=1.0)]
await db.trades.save_trades(trades)
```

### Technical Metrics:
- **Parallel Testing**: Robust uvloop integration for pytest-xdist
- **Test Coverage**: 99.68% coverage with 634+ passing tests
- **100% Repository Compliance**: All examples and tests use ORM objects
