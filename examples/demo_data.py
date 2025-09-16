#!/usr/bin/env python3
"""
Demo Data Setup for fullon_ticker_service Examples

Creates isolated test database with minimal data needed for ticker service examples:
- Test exchanges (binance, kraken, hyperliquid)  
- Test symbols (BTC/USDT, ETH/USDT, etc.)
- Test user for authentication

Usage:
    python examples/demo_data.py --setup      # Create test DB and install data
    python examples/demo_data.py --cleanup    # Drop test DB
    python examples/demo_data.py --run-all    # Setup, run examples, cleanup
"""

import asyncio
import argparse
import os
import sys
import random
import string
from typing import Optional, Tuple
from decimal import Decimal
from contextlib import asynccontextmanager
from pathlib import Path

# Load environment variables from .env file
project_root = Path(__file__).parent.parent
try:
    from dotenv import load_dotenv
    load_dotenv(project_root / ".env")
except ImportError:
    print("⚠️  python-dotenv not available, make sure .env variables are set manually")
except Exception as e:
    print(f"⚠️  Could not load .env file: {e}")

from fullon_orm import init_db, DatabaseContext
from fullon_orm.models import User, Exchange, CatExchange, Symbol, Bot, Strategy, Feed
from fullon_orm.models.user import RoleEnum
from fullon_log import get_component_logger
import redis

# Create fullon logger alongside color output
fullon_logger = get_component_logger("fullon.ticker.example.demo_data")


class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    END = '\033[0m'
    BOLD = '\033[1m'


def print_success(msg: str):
    print(f"{Colors.GREEN}✓ {msg}{Colors.END}")


def print_error(msg: str):
    print(f"{Colors.RED}✗ {msg}{Colors.END}")


def print_warning(msg: str):
    print(f"{Colors.YELLOW}⚠ {msg}{Colors.END}")


def print_info(msg: str):
    print(f"{Colors.CYAN}→ {msg}{Colors.END}")


def print_header(msg: str):
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'=' * 60}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{msg:^60}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'=' * 60}{Colors.END}\n")


def generate_test_db_name() -> str:
    """Generate unique test database name"""
    base_name = os.getenv('DB_TEST_NAME', 'fullon_ticker_test')
    random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
    return f"{base_name}_{random_suffix}"


async def create_test_database(db_name: str) -> bool:
    """Create isolated test database using fullon_orm database utilities where possible"""
    print_info(f"Creating test database: {db_name}")
    fullon_logger.info(f"Creating isolated test database: {db_name}")

    try:
        # Try to use fullon_orm database utilities first
        try:
            from fullon_orm.database import DatabaseManager
            db_manager = DatabaseManager()

            # Use fullon_orm database creation if available
            await db_manager.create_database(db_name)
            print_success(f"Test database created via fullon_orm: {db_name}")
            fullon_logger.info(f"Test database created successfully via fullon_orm: {db_name}")
            return True

        except (ImportError, AttributeError):
            # Fallback to direct asyncpg for database creation (administrative operation)
            print_info("Using direct database creation (fullon_orm utilities not available)")

            import asyncpg

            host = os.getenv("DB_HOST", "localhost")
            port = int(os.getenv("DB_PORT", "5432"))
            user = os.getenv("DB_USER", "postgres")
            password = os.getenv("DB_PASSWORD", "")

            conn = await asyncpg.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database="postgres"
            )

            try:
                # Database creation requires direct SQL (administrative operation)
                await conn.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
                await conn.execute(f'CREATE DATABASE "{db_name}"')

                print_success(f"Test database created: {db_name}")
                fullon_logger.info(f"Test database created successfully: {db_name}")
                return True

            finally:
                await conn.close()

    except Exception as e:
        print_error(f"Failed to create test database: {e}")
        fullon_logger.error(f"Failed to create test database {db_name}: {e}")
        return False


async def drop_test_database(db_name: str) -> bool:
    """Drop test database using fullon_orm database utilities where possible"""
    print_info(f"Dropping test database: {db_name}")

    try:
        # Try to use fullon_orm database utilities first
        try:
            from fullon_orm.database import DatabaseManager
            db_manager = DatabaseManager()

            # Use fullon_orm database dropping if available
            await db_manager.drop_database(db_name)
            print_success(f"Test database dropped via fullon_orm: {db_name}")
            return True

        except (ImportError, AttributeError):
            # Fallback to direct asyncpg for database operations (administrative)
            print_info("Using direct database operations (fullon_orm utilities not available)")

            import asyncpg

            host = os.getenv("DB_HOST", "localhost")
            port = int(os.getenv("DB_PORT", "5432"))
            user = os.getenv("DB_USER", "postgres")
            password = os.getenv("DB_PASSWORD", "")

            conn = await asyncpg.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database="postgres"
            )

            try:
                # Connection termination and database dropping require direct SQL (administrative)
                await conn.execute("""
                    SELECT pg_terminate_backend(pg_stat_activity.pid)
                    FROM pg_stat_activity
                    WHERE pg_stat_activity.datname = $1
                    AND pid <> pg_backend_pid()
                """, db_name)

                await conn.execute(f'DROP DATABASE IF EXISTS "{db_name}"')

                print_success(f"Test database dropped: {db_name}")
                return True

            finally:
                await conn.close()

    except Exception as e:
        print_error(f"Failed to drop test database: {e}")
        return False


@asynccontextmanager
async def database_context_for_test(db_name: str):
    """Context manager for test database lifecycle (following fullon_orm_api pattern)"""
    # Set environment variable for this test database
    original_db_name = os.getenv('DATABASE_URL', '')
    
    # Update DATABASE_URL to point to test database
    host = os.getenv("DB_HOST", "localhost")
    port = int(os.getenv("DB_PORT", "5432"))
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "")
    
    test_db_url = f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{db_name}"
    os.environ['DATABASE_URL'] = test_db_url
    
    try:
        # Create test database
        if not await create_test_database(db_name):
            raise Exception("Failed to create test database")

        # Clear Redis cache to avoid stale data
        try:
            redis_host = os.getenv("REDIS_HOST", "localhost")
            redis_port = int(os.getenv("REDIS_PORT", "6379"))
            redis_db = int(os.getenv("REDIS_DB", "0"))
            r = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
            r.flushdb()
            print_info("Cleared Redis cache to avoid stale data")
        except Exception as e:
            print_warning(f"Could not clear Redis cache: {e}")

        # Initialize schema
        print_info("Initializing database schema...")
        await init_db()
        print_success("Database schema initialized")

        yield db_name
        
    finally:
        # Restore original DATABASE_URL
        if original_db_name:
            os.environ['DATABASE_URL'] = original_db_name
        else:
            os.environ.pop('DATABASE_URL', None)
        
        # Drop test database
        await drop_test_database(db_name)


async def install_demo_data():
    """Install demo data matching fullon_orm demo_install.py exactly"""
    print_header("INSTALLING DEMO DATA")
    fullon_logger.info("Starting demo data installation (matching fullon_orm)")

    try:
        async with DatabaseContext() as db:
            uid = await install_admin_user_internal(db)
            if not uid:
                print_error("Could not install admin user")
                return False
            ex_id, cat_ex_id = await install_exchanges_internal(db, uid=uid)
            if not ex_id or not cat_ex_id:
                print_error("Could not install admin exchanges")
                return False

            # Install symbols for all created exchanges
            await install_symbols_for_all_exchanges_internal(db, uid=uid)
            await install_bots_internal(db, uid=uid, ex_id=ex_id, cat_ex_id=cat_ex_id)
            await db.commit()  # Final commit for everything

            print_success("Demo data installation complete!")

            # Print summary matching fullon_orm format
            print_info("\nDemo credentials:")
            admin_email = os.getenv("ADMIN_MAIL", "admin@fullon")
            print(f"  Email: {Colors.BOLD}{admin_email}{Colors.END}")
            print(f"  Password: {Colors.BOLD}password{Colors.END}")
            print(f"  Role: {Colors.BOLD}admin{Colors.END}")

            print_info("\nNext steps:")
            print("  1. Update .env file with your database credentials")
            print("  2. Add exchange API keys if needed")
            print("  3. Start using the ticker service with examples")

            fullon_logger.info("Demo data installation completed successfully")
            return True

    except Exception as e:
        print_error(f"Failed to install demo data: {e}")
        fullon_logger.error(f"Demo data installation failed: {e}")
        raise


async def install_admin_user_internal(db: DatabaseContext) -> Optional[int]:
    """Install admin user using provided DatabaseContext and ORM models."""
    print_info("Installing admin user...")

    # Get admin email from environment variable
    admin_email = os.getenv("ADMIN_MAIL", "admin@fullon")
    print_info(f"Using admin email: {admin_email}")

    # Check if user exists
    existing_uid = await db.users.get_user_id(admin_email)
    if existing_uid:
        print_warning("Admin user already exists")
        return existing_uid

    # Create User ORM model
    user = User(
        mail=admin_email,
        password="password",  # In production this would be hashed
        f2a="---",
        role=RoleEnum.ADMIN,
        name="robert",
        lastname="plant",
        phone="666666666",
        id_num="3242"
    )

    try:
        created_user = await db.users.add_user(user)
        print_success(f"Admin user created: {created_user.name} {created_user.lastname}")
        return created_user.uid
    except Exception as e:
        print_error(f"Failed to create admin user: {e}")
        return None


async def install_exchanges_internal(db: DatabaseContext, uid: int) -> Tuple[Optional[int], Optional[int]]:
    """Install exchanges using provided DatabaseContext and ORM models."""
    print_info("Installing exchanges...")

    # Clear ALL cache to ensure fresh data after database drop
    from fullon_orm.cache import cache_manager
    cache_manager.region.invalidate()  # Clear entire cache
    cache_manager.invalidate_exchange_caches()

    # Define the exchanges to create (matching .env credentials)
    exchanges_to_create = [
        {"name": "kraken", "user_name": "kraken1"},
        {"name": "bitmex", "user_name": "bitmex1"},
        {"name": "hyperliquid", "user_name": "hyperliquid1"}
    ]

    # Use repository method to get existing cat_exchanges
    cat_exchanges = await db.exchanges.get_cat_exchanges(all=True)
    print_info(f"  Found {len(cat_exchanges)} existing category exchanges in database")
    for ce in cat_exchanges:
        print_info(f"    - {ce.name} (ID: {ce.cat_ex_id})")

    created_exchanges = []

    for exchange_config in exchanges_to_create:
        exchange_name = exchange_config["name"]
        user_exchange_name = exchange_config["user_name"]

        # Check if category exchange exists
        cat_ex_id = None
        for ce in cat_exchanges:
            if ce.name == exchange_name:
                cat_ex_id = ce.cat_ex_id
                print_info(f"  Category exchange '{exchange_name}' already exists with ID: {cat_ex_id}")
                break

        # Create category exchange if it doesn't exist
        if not cat_ex_id:
            cat_exchange = await db.exchanges.create_cat_exchange(exchange_name, "")
            cat_ex_id = cat_exchange.cat_ex_id
            print_info(f"  Created category exchange: {exchange_name}")

        # Check if user already has this exchange
        user_exchanges = await db.exchanges.get_user_exchanges(uid)
        existing_exchange = None
        for ue in user_exchanges:
            if ue.get('ex_named') == user_exchange_name and ue.get('cat_ex_id') == cat_ex_id:
                existing_exchange = ue
                break

        if existing_exchange:
            ex_id = existing_exchange.get('ex_id')
            print_info(f"  User exchange '{user_exchange_name}' already exists")
            created_exchanges.append((ex_id, cat_ex_id))
        else:
            # Create user exchange
            exchange = Exchange(
                uid=uid,
                cat_ex_id=cat_ex_id,
                name=user_exchange_name,
                test=False,
                active=True
            )

            created_exchange = await db.exchanges.add_user_exchange(exchange)
            ex_id = created_exchange.ex_id
            print_success(f"Exchange created: {user_exchange_name}")
            created_exchanges.append((ex_id, cat_ex_id))

    # Return the first exchange (for backward compatibility with existing code)
    if created_exchanges:
        return created_exchanges[0]
    else:
        return (None, None)


async def install_symbols_for_all_exchanges_internal(db: DatabaseContext, uid: int):
    """Install symbols for all exchanges belonging to the admin user."""
    print_info("Installing symbols for all exchanges...")

    # Get all exchanges for the admin user
    user_exchanges = await db.exchanges.get_user_exchanges(uid)

    for ue_dict in user_exchanges:
        cat_ex_id = ue_dict.get('cat_ex_id')
        ex_name = ue_dict.get('ex_named', 'unknown')

        if cat_ex_id:
            print_info(f"  Installing symbols for {ex_name} (cat_ex_id: {cat_ex_id})")
            await install_symbols_internal(db, cat_ex_id)


async def install_symbols_internal(db: DatabaseContext, cat_ex_id: int):
    """Install symbols using provided DatabaseContext and ORM models."""
    print_info("Installing symbols...")

    # Use the provided cat_ex_id directly
    cat_exchange_id = cat_ex_id

    # Define symbol data
    symbols_data = [
        {
            "symbol": "BTC/USD",
            "cat_ex_id": cat_exchange_id,
            "updateframe": "1h",
            "backtest": 2700,
            "decimals": 6,
            "base": "BTC",
            "quote": "USD",
            "futures": True,
        },
        {
            "symbol": "ETH/USD",
            "cat_ex_id": cat_exchange_id,
            "updateframe": "1h",
            "backtest": 300,
            "decimals": 6,
            "base": "ETH",
            "quote": "USD",
            "futures": True,
        },
        {
            "symbol": "ETH/BTC",
            "cat_ex_id": cat_exchange_id,
            "updateframe": "1h",
            "backtest": 365,
            "decimals": 6,
            "base": "ETH",
            "quote": "BTC",
            "futures": True,
        },
        {
            "symbol": "BTC/USDC",
            "cat_ex_id": cat_exchange_id,
            "updateframe": "1h",
            "backtest": 7,
            "decimals": 6,
            "base": "BTC",
            "quote": "USDC",
            "futures": True,
        },
        {
            "symbol": "SOL/USD",
            "cat_ex_id": cat_exchange_id,
            "updateframe": "1h",
            "backtest": 365,
            "decimals": 6,
            "base": "SOL",
            "quote": "USD",
            "futures": True,
        }
    ]

    symbols_created = 0
    for symbol_data in symbols_data:
        try:
            # Create Symbol model instance with explicit field assignment
            symbol = Symbol(
                symbol=symbol_data["symbol"],
                cat_ex_id=symbol_data["cat_ex_id"],
                updateframe=symbol_data["updateframe"],
                backtest=symbol_data["backtest"],
                decimals=symbol_data["decimals"],
                base=symbol_data["base"],
                quote=symbol_data["quote"],
                futures=symbol_data["futures"]
            )

            # Use repository method if available, otherwise direct ORM
            try:
                # Try using repository method first
                created_symbol = await db.symbols.add_symbol(symbol)
                symbols_created += 1
                print_info(f"  Added symbol via repository: {symbol_data['symbol']}")
            except AttributeError:
                # Fallback to direct ORM if repository method doesn't exist
                db.session.add(symbol)
                await db.session.flush()
                symbols_created += 1
                print_info(f"  Added symbol via ORM: {symbol_data['symbol']}")

        except Exception as e:
            if "duplicate key" in str(e).lower() or "unique constraint" in str(e).lower():
                print_warning(f"  Symbol {symbol_data['symbol']} already exists")
                # Rollback the session to clean up after constraint violation
                await db.session.rollback()
            else:
                print_error(f"  Failed to create symbol {symbol_data['symbol']}: {e}")
                await db.session.rollback()

    if symbols_created > 0:
        print_success(f"Symbols installed successfully ({symbols_created} new)")
    else:
        print_info("All symbols already existed")


async def install_bots_internal(db: DatabaseContext, uid: int, ex_id: int, cat_ex_id: int):
    """Install bots using provided DatabaseContext and ORM models."""
    print_info("Installing bots, strategies and feeds...")

    try:
        # First ensure we have the required cat_strategies using repository
        cat_strategy_names = ['rsi_hayden_long', 'rsi_hayden_short', 'llm_trader']
        cat_strategies = {}

        for name in cat_strategy_names:
            # Check if strategy exists using repository method
            try:
                cat_str_id = await db.strategies.get_cat_str_id(name)
                if cat_str_id:
                    cat_strategies[name] = cat_str_id
                    continue
            except:
                pass  # Method might not exist, continue with install

            # Install strategy using repository method
            try:
                base_params = {
                    "take_profit": Decimal("2.0"),
                    "stop_loss": Decimal("1.0"),
                    "pre_load_bars": 200,
                    "feeds": 2 if 'rsi' in name else 4
                }
                cat_str_id = await db.strategies.install_strategy(name, base_params)
                if cat_str_id:
                    cat_strategies[name] = cat_str_id
                    print_info(f"  Created strategy category: {name}")
                else:
                    print_error(f"  Failed to create strategy category: {name}")
                    raise Exception(f"Strategy installation failed for {name}")
            except Exception as e:
                if "already exists" in str(e).lower() or "duplicate" in str(e).lower():
                    print_warning(f"  Strategy category {name} already exists")
                    # Try alternative approach to get existing strategy
                    cat_str_id = await db.strategies.get_cat_str_id(name)
                    if cat_str_id:
                        cat_strategies[name] = cat_str_id
                    else:
                        raise Exception(f"Could not create or find strategy {name}")
                else:
                    raise e

        # Get symbol using repository method with cat_ex_id since symbols were just created
        btc_symbol = await db.symbols.get_by_symbol("BTC/USD", cat_ex_id=cat_ex_id)
        if not btc_symbol:
            raise Exception("BTC/USD symbol not found for kraken exchange")
        symbol_id = btc_symbol.symbol_id

        # Create bots with strategies and feeds using ORM models
        bot_data = [
            {
                'bot': Bot(uid=uid, name='HAYDEN RSI LONG BTC/USD', dry_run=True, active=True),
                'strategy_data': {
                    'cat_str_id': cat_strategies['rsi_hayden_long'],
                    'leverage': 2, 'size': None, 'size_pct': Decimal("20"),
                    'size_currency': "USD", 'take_profit': Decimal("3"),
                    'trailing_stop': Decimal("3"), 'timeout': None
                },
                'feeds_data': [
                    {'period': 'Ticks', 'compression': 1, 'order': 1},
                    {'period': 'Minutes', 'compression': 60, 'order': 2}
                ]
            },
            {
                'bot': Bot(uid=uid, name='HAYDEN RSI SHORT BTC/USD', dry_run=True, active=True),
                'strategy_data': {
                    'cat_str_id': cat_strategies['rsi_hayden_short'],
                    'leverage': 2, 'size': None, 'size_pct': Decimal("20"),
                    'size_currency': "USD", 'take_profit': Decimal("2"),
                    'trailing_stop': Decimal("1"), 'timeout': None
                },
                'feeds_data': [
                    {'period': 'Ticks', 'compression': 1, 'order': 1},
                    {'period': 'Minutes', 'compression': 60, 'order': 2}
                ]
            },
            {
                'bot': Bot(uid=uid, name='HTEST LLM trader', dry_run=True, active=True),
                'strategy_data': {
                    'cat_str_id': cat_strategies['llm_trader'],
                    'leverage': 2, 'size': None, 'size_pct': Decimal("20"),
                    'size_currency': "USD", 'take_profit': Decimal("2"), 'timeout': None
                },
                'feeds_data': [
                    {'period': 'Ticks', 'compression': 1, 'order': 1},
                    {'period': 'Minutes', 'compression': 60, 'order': 2},
                    {'period': 'Minutes', 'compression': 240, 'order': 3},
                    {'period': 'Days', 'compression': 1, 'order': 4}
                ]
            }
        ]

        # Create each bot using repository methods
        for bot_config in bot_data:
            bot_model = bot_config['bot']

            # Check if bot already exists using repository (if method exists)
            try:
                existing_bots = await db.bots.get_bots_by_user(bot_model.uid)
                bot_exists = any(b.name == bot_model.name for b in existing_bots if hasattr(b, 'name'))

                if bot_exists:
                    print_warning(f"  Bot '{bot_model.name}' already exists")
                    continue
            except:
                pass  # Method might not exist or fail, continue with creation

            try:
                # Create bot using repository method with model instance
                created_bot = await db.bots.add_bot(bot_model)
                bot_id = created_bot.bot_id
                print_info(f"  Created bot: {bot_model.name}")

                # Add exchange to bot using repository method
                await db.bots.add_exchange_to_bot(bot_id, ex_id)

                # Add strategy using repository method - create Strategy model instance
                strategy_data = bot_config['strategy_data'].copy()
                strategy_data['bot_id'] = bot_id

                # Create Strategy model instance with explicit field assignment
                strategy = Strategy(
                    bot_id=strategy_data['bot_id'],
                    cat_str_id=strategy_data['cat_str_id'],
                    leverage=strategy_data.get('leverage'),
                    size=strategy_data.get('size'),
                    size_pct=strategy_data.get('size_pct'),
                    size_currency=strategy_data.get('size_currency'),
                    take_profit=strategy_data.get('take_profit'),
                    trailing_stop=strategy_data.get('trailing_stop'),
                    timeout=strategy_data.get('timeout')
                )
                created_strategy = await db.strategies.add_bot_strategy(strategy)
                str_id = created_strategy.str_id if created_strategy else None

                if str_id:
                    # Add feeds - no FeedRepository available, so use direct ORM creation
                    # This is acceptable as Feed is a simple entity with no complex business logic
                    for feed_data in bot_config['feeds_data']:
                        # Create Feed model instance with explicit field assignment
                        feed = Feed(
                            symbol_id=symbol_id,
                            str_id=str_id,
                            period=feed_data['period'],
                            compression=feed_data['compression'],
                            order=feed_data['order']
                        )
                        db.session.add(feed)

            except Exception as e:
                if "already exists" in str(e).lower() or "duplicate" in str(e).lower():
                    print_warning(f"  Bot '{bot_model.name}' already exists")
                    continue
                else:
                    raise e

        print_success("All bots, strategies and feeds installed successfully")

    except Exception as e:
        print_error(f"Failed to create bots: {e}")
        raise


async def run_examples():
    """Run all ticker service examples against demo data"""
    print_header("RUNNING EXAMPLES")
    
    examples_dir = os.path.dirname(__file__)
    examples = [
        'daemon_control.py',
        'ticker_retrieval.py', 
        'callback_override.py'
    ]
    
    success_count = 0
    total_count = len(examples)
    
    for example in examples:
        example_path = os.path.join(examples_dir, example)
        if not os.path.exists(example_path):
            print_warning(f"Example not found: {example}")
            continue
            
        print_info(f"Running example: {example}")
        
        try:
            # Import and run the example
            # Note: In a real implementation, you'd want to run these as subprocesses
            # or import them dynamically to avoid side effects
            proc = await asyncio.create_subprocess_exec(
                sys.executable, example_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=examples_dir
            )
            
            stdout, stderr = await proc.communicate()
            
            if proc.returncode == 0:
                print_success(f"Example {example} passed")
                success_count += 1
            else:
                print_error(f"Example {example} failed")
                if stderr:
                    print(f"Error: {stderr.decode()}")
                    
        except Exception as e:
            print_error(f"Failed to run example {example}: {e}")
    
    print_info(f"\nExamples completed: {success_count}/{total_count} passed")
    return success_count == total_count


async def setup_demo_environment():
    """Setup demo environment with test database and data"""
    test_db_name = generate_test_db_name()
    
    async with database_context_for_test(test_db_name):
        await install_demo_data()
        print_success(f"\nDemo environment ready!")
        print_info(f"Test database: {test_db_name}")
        print_info("Use --cleanup when done to remove test database")
        
        return test_db_name


async def run_full_demo():
    """Setup, run examples, and cleanup"""
    test_db_name = generate_test_db_name()
    
    async with database_context_for_test(test_db_name):
        await install_demo_data()
        success = await run_examples()
        
        if success:
            print_success("\n🎉 All examples passed!")
        else:
            print_warning("\n⚠️ Some examples failed")
        
        return success


async def main():
    """Main CLI interface"""
    parser = argparse.ArgumentParser(
        description="Demo Data Setup for fullon_ticker_service Examples",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--setup', action='store_true',
                        help='Create test database and install demo data')
    parser.add_argument('--cleanup', metavar='DB_NAME',
                        help='Drop specific test database')
    parser.add_argument('--run-all', action='store_true',
                        help='Setup, run all examples, then cleanup')
    parser.add_argument('--examples-only', action='store_true',
                        help='Run examples against existing database')
    
    args = parser.parse_args()
    
    if args.setup:
        db_name = await setup_demo_environment()
        print_info(f"\nTo cleanup later, run: python {sys.argv[0]} --cleanup {db_name}")
        
    elif args.cleanup:
        success = await drop_test_database(args.cleanup)
        sys.exit(0 if success else 1)
        
    elif args.run_all:
        success = await run_full_demo()
        sys.exit(0 if success else 1)
        
    elif args.examples_only:
        success = await run_examples()
        sys.exit(0 if success else 1)
        
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print_warning("\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print_error(f"\nUnexpected error: {e}")
        sys.exit(1)