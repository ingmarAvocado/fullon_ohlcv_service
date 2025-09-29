"""Database-driven configuration loading using fullon_orm.

This module provides configuration loading from the fullon_orm database,
following the ticker service pattern. NO hardcoded exchange/symbol lists.
"""

from fullon_log import get_component_logger
from fullon_orm.database_context import DatabaseContext


async def get_collection_targets(user_id: int = 1, db_context=None) -> dict[str, dict]:
    """Get exchanges/symbols from database - like legacy Database().get_symbols().

    Args:
        user_id: The user ID to get exchanges for (default: 1)
        db_context: Optional database context to use (for testing)

    Returns:
        Dictionary mapping exchange names to exchange info with symbols and ex_id
        Example: {
            "kraken": {
                "symbols": ["BTC/USD", "ETH/USD"],
                "ex_id": 1
            },
            "binance": {
                "symbols": ["BTC/USDT"],
                "ex_id": 2
            }
        }
    """
    logger = get_component_logger("fullon.ohlcv.config")

    # Helper function to process the database
    async def process_database(db):
        # Get user's active exchanges
        exchanges = await db.exchanges.get_user_exchanges(uid=user_id)

        targets = {}
        for exchange in exchanges:
            # Exchanges from get_user_exchanges are dictionaries per fullon_orm docs
            exchange_name = exchange.get('ex_named', exchange.get('name', 'unknown'))
            cat_ex_id = exchange.get('cat_ex_id')
            ex_id = exchange.get('ex_id')  # Get the exchange ID for fullon_credentials

            if cat_ex_id:  # Basic validity check
                # Get exchange name from cat_ex_id for symbol lookup
                cat_exchanges = await db.exchanges.get_cat_exchanges(all=True)
                exchange_category_name = None
                for cat_ex in cat_exchanges:
                    # CatExchange objects have attributes, not dictionary keys
                    if cat_ex.cat_ex_id == cat_ex_id:
                        exchange_category_name = cat_ex.name
                        break

                if not exchange_category_name:
                    logger.warning(f"Category exchange not found for cat_ex_id {cat_ex_id}")
                    continue

                # Get all symbols for this exchange (avoids bot filtering in get_by_exchange_id)
                symbols = await db.symbols.get_all(exchange_name=exchange_category_name)

                # Handle case where symbols don't have .active attribute (simpler test scenarios)
                active_symbols = [s.symbol for s in symbols if getattr(s, 'active', True)]
                if active_symbols:
                    targets[exchange_name] = {
                        "symbols": active_symbols,
                        "ex_id": ex_id,
                        "exchange_category_name": exchange_category_name  # Add category name for fullon_exchange
                    }

        logger.info("Loaded configuration",
                   exchanges=len(targets),
                   total_symbols=sum(len(info["symbols"]) for info in targets.values()))

        return targets

    if db_context:
        return await process_database(db_context)
    else:
        async with DatabaseContext() as db:
            return await process_database(db)


async def should_collect_ohlcv(exchange: str, symbol: str) -> bool:
    """Check if symbol needs OHLCV collection (like legacy only_ticker check).

    Args:
        exchange: Exchange name (unused but kept for API consistency)
        symbol: Symbol to check

    Returns:
        True if OHLCV should be collected, False if ticker-only
    """
    async with DatabaseContext() as db:
        symbol_obj = await db.symbols.get_by_symbol(symbol)
        return symbol_obj and not symbol_obj.only_ticker if symbol_obj else False
