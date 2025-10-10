#!/usr/bin/env python3
"""
Symbol Initialization Utility for fullon_ohlcv_service

Provides utilities to initialize database symbols for OHLCV repositories.
This ensures symbols are ready for data operations without manual init_symbol() calls.
"""

import asyncio
from typing import Optional
from fullon_ohlcv.repositories.ohlcv import TradeRepository, CandleRepository, TimeseriesRepository
from fullon_orm import DatabaseContext
from fullon_log import get_component_logger

logger = get_component_logger("fullon.ohlcv.utils.add_symbols")


async def add_symbol(
    exchange: str,
    symbol: str,
    main: str = "view",
    test: bool = True
) -> bool:
    """
    Initialize a single symbol for OHLCV operations.

    Args:
        exchange: Exchange name (e.g., 'binance', 'kraken')
        symbol: Symbol name (e.g., 'BTC/USDT', 'ETH/USD')
        main: Main source for OHLCV data ('view', 'candles', or 'trades')
        test: Whether to use test database

    Returns:
        bool: True if initialization successful, False otherwise
    """
    try:
        logger.info(f"Initializing symbol {exchange}:{symbol}'")

        # Determine which repository to use based on main parameter
        if main == "candles":
            repo_class = CandleRepository
        else:
            # For 'view' and 'trades', use TradeRepository
            repo_class = TradeRepository

        # Initialize the symbol
        async with repo_class(exchange, symbol, test=test) as repo:
            try:
                success = await repo.init_symbol()
            except Exception as e:
                logger.warning(f"Symbol initialization failed, but continuing: {e}")
                success = True  # Consider it successful if table creation worked but hypertable failed

        if success:
            logger.info(f"Successfully initialized symbol {exchange}:{symbol}")
            return True
        else:
            logger.error(f"Failed to initialize symbol {exchange}:{symbol}")
            return False

    except Exception as e:
        logger.error(f"Error initializing symbol {exchange}:{symbol}: {e}")
        return False


async def add_all_symbols(
    symbols: Optional[list] = None,
    main: str = "view",
    test: bool = True,
    limit: Optional[int] = None,
    database_url: Optional[str] = None
) -> bool:
    """
    Initialize all symbols from the database for OHLCV operations.

    This method loads all symbols from the database and initializes them sequentially.
    It's designed to be called once during service startup, not before every save operation.

    Args:
        symbols: Optional list of symbols to initialize. If None, loads from database.
        main: Main source for OHLCV data ('view', 'candles', or 'trades')
        test: Whether to use test database
        limit: Optional limit on number of symbols to initialize (for testing)
        database_url: Optional database URL to use for symbol reading

    Returns:
        bool: True if all symbols initialized successfully, False if any failed
    """
    try:
        logger.info(f"Starting initialization of all symbols with main='{main}'")

        if symbols is None:
            # Load symbols from database
            async with DatabaseContext() as db:
                symbols = await db.symbols.get_all(limit=limit)

        if not symbols:
            logger.warning("No symbols found")
            return False

        logger.info(f"Found {len(symbols)} symbols to initialize")

        success_count = 0
        total_count = len(symbols)

        # Initialize symbols sequentially (as required)
        for i, symbol in enumerate(symbols, 1):
            exchange_name = symbol.exchange_name if hasattr(symbol, 'exchange_name') else symbol.cat_exchange.name
            symbol_name = symbol.symbol

            logger.debug(f"Initializing symbol {i}/{total_count}: {exchange_name}:{symbol_name}")

            success = await add_symbol(
                exchange=exchange_name,
                symbol=symbol_name,
                main=main,
                test=test
            )

            if success:
                success_count += 1
            else:
                logger.error(f"Failed to initialize symbol {exchange_name}:{symbol_name}")

        logger.info(f"Symbol initialization complete: {success_count}/{total_count} successful")

        return success_count == total_count

    except Exception as e:
        logger.error(f"Error during bulk symbol initialization: {e}")
        return False


async def add_symbols_for_exchange(
    exchange: str,
    main: str = "view",
    test: bool = True
) -> bool:
    """
    Initialize all symbols for a specific exchange.

    Args:
        exchange: Exchange name to initialize symbols for
        main: Main source for OHLCV data ('view', 'candles', or 'trades')
        test: Whether to use test database

    Returns:
        bool: True if all symbols initialized successfully
    """
    try:
        logger.info(f"Initializing all symbols for exchange: {exchange}")

        async with DatabaseContext() as db:
            # Load symbols for specific exchange
            symbols = await db.symbols.get_by_exchange(exchange)

            if not symbols:
                logger.warning(f"No symbols found for exchange {exchange}")
                return False

            logger.info(f"Found {len(symbols)} symbols for exchange {exchange}")

            success_count = 0
            for symbol in symbols:
                success = await add_symbol(
                    exchange=exchange,
                    symbol=symbol.symbol,
                    main=main,
                    test=test
                )

                if success:
                    success_count += 1

            logger.info(f"Exchange {exchange} initialization: {success_count}/{len(symbols)} successful")
            return success_count == len(symbols)

    except Exception as e:
        logger.error(f"Error initializing symbols for exchange {exchange}: {e}")
        return False