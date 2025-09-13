"""Database-driven configuration loading using fullon_orm.

This module provides configuration loading from the fullon_orm database,
following the ticker service pattern. NO hardcoded exchange/symbol lists.
"""

from fullon_orm.database_context import DatabaseContext
from fullon_log import get_component_logger


async def get_collection_targets(user_id: int = 1) -> dict[str, list[str]]:
    """Get exchanges/symbols from database - like legacy Database().get_symbols().

    Args:
        user_id: The user ID to get exchanges for (default: 1)

    Returns:
        Dictionary mapping exchange names to list of active symbols
        Example: {"kraken": ["BTC/USD", "ETH/USD"], "binance": ["BTC/USDT"]}
    """
    logger = get_component_logger("fullon.ohlcv.config")

    async with DatabaseContext() as db:
        # Get user's active exchanges
        exchanges = await db.exchanges.get_user_exchanges(user_id=user_id)

        targets = {}
        for exchange in exchanges:
            if exchange['active']:
                # Get active symbols for this exchange
                symbols = await db.symbols.get_by_exchange_id(exchange['cat_ex_id'])
                active_symbols = [s.symbol for s in symbols if s.active]
                if active_symbols:
                    targets[exchange['name']] = active_symbols

        logger.info("Loaded configuration",
                   exchanges=len(targets),
                   total_symbols=sum(len(s) for s in targets.values()))

        return targets


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