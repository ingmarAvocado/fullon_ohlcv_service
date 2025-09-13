#!/usr/bin/env python3
"""
Example: OHLCV Data Retrieval

Shows how to retrieve OHLCV data using fullon_ohlcv repositories.
Demonstrates database access patterns for historical candle data.
"""

import asyncio
from fullon_log import get_component_logger
from fullon_ohlcv import CandleRepository
from fullon_cache import OHLCVCache


async def get_bars_from_cache_example():
    """Get OHLCV bars from cache (fastest method)"""
    
    logger = get_component_logger("fullon.examples.cache_bars")
    symbol = "BTCUSD"
    timeframe = "1m"
    
    logger.info("Fetching OHLCV bars from cache", symbol=symbol, timeframe=timeframe)
    
    try:
        # Get latest 100 1-minute bars from cache using real OHLCVCache
        async with OHLCVCache() as cache:
            bars = await cache.get_latest_ohlcv_bars(symbol=symbol, timeframe=timeframe, count=100)
            
        if bars:
            logger.info("Retrieved bars from cache", symbol=symbol, count=len(bars))
            
            # Show last 3 bars [timestamp, open, high, low, close, volume]
            for i, bar in enumerate(bars[-3:]):
                logger.info(
                    f"Bar {len(bars)-3+i+1}",
                    timestamp=bar[0],
                    open=bar[1],
                    high=bar[2], 
                    low=bar[3],
                    close=bar[4],
                    volume=bar[5]
                )
        else:
            logger.warning("No bars found in cache", symbol=symbol)
            
    except Exception as e:
        logger.error("Error fetching bars from cache", symbol=symbol, error=str(e))


async def get_latest_timestamp_example():
    """Get latest timestamp from database (like legacy Database_Ohlcv)"""
    
    logger = get_component_logger("fullon.examples.database_ts")
    exchange = "kraken"
    symbol = "BTC/USD"
    
    logger.info("Getting latest timestamp from database", exchange=exchange, symbol=symbol)
    
    try:
        # Use proper fullon_ohlcv CandleRepository
        async with CandleRepository(exchange=exchange, symbol=symbol, test=True) as repo:
            # Main method used in legacy code
            latest_ts = await repo.get_latest_timestamp()
            
        logger.info("Retrieved latest timestamp", exchange=exchange, symbol=symbol, timestamp=latest_ts)
        
        # Check oldest timestamp as well
        async with CandleRepository(exchange=exchange, symbol=symbol, test=True) as repo:
            oldest_ts = await repo.get_oldest_timestamp()
            
        logger.info("Retrieved oldest timestamp", exchange=exchange, symbol=symbol, timestamp=oldest_ts)
        
    except Exception as e:
        logger.error("Error getting timestamps", exchange=exchange, symbol=symbol, error=str(e))


async def multiple_symbols_latest_prices():
    """Get latest prices for multiple symbols from database"""
    
    logger = get_component_logger("fullon.examples.multi_prices")
    
    exchange_symbols = [
        ("kraken", "BTC/USD"),
        ("binance", "ETH/USDT"), 
        ("kraken", "ADA/USD"),
    ]
    
    logger.info("Getting latest prices for multiple symbols", symbols=exchange_symbols)
    
    for exchange, symbol in exchange_symbols:
        try:
            async with CandleRepository(exchange=exchange, symbol=symbol, test=True) as repo:
                latest_candle = await repo.get_latest_candle()
                
            if latest_candle:
                logger.info(
                    "Latest price retrieved",
                    exchange=exchange,
                    symbol=symbol,
                    price=latest_candle.close,
                    timestamp=latest_candle.timestamp
                )
            else:
                logger.warning("No data found", exchange=exchange, symbol=symbol)
                
        except Exception as e:
            logger.error("Error getting price", exchange=exchange, symbol=symbol, error=str(e))


async def check_data_freshness():
    """Check how fresh the database data is"""
    
    logger = get_component_logger("fullon.examples.data_freshness")
    
    symbols = [("kraken", "BTCUSD"), ("binance", "ETHUSDT")]
    
    logger.info("Checking data freshness", symbols=symbols)
    
    for exchange, symbol in symbols:
        try:
            async with CandleRepository(exchange=exchange, symbol=symbol, test=True) as repo:
                latest_ts = await repo.get_latest_timestamp()
                
            if latest_ts:
                logger.info("Data freshness check", exchange=exchange, symbol=symbol, latest_timestamp=latest_ts)
            else:
                logger.warning("No data found", exchange=exchange, symbol=symbol)
                
        except Exception as e:
            logger.error("Error checking freshness", exchange=exchange, symbol=symbol, error=str(e))


async def save_sample_candles_example():
    """Example of saving OHLCV candles (like what daemon would do)"""
    
    logger = get_component_logger("fullon.examples.save_candles")
    
    exchange = "kraken"
    symbol = "BTC/USD"
    
    logger.info("Saving sample candles to database", exchange=exchange, symbol=symbol)
    
    try:
        # Sample candle data - this would come from fullon_exchange in real daemon
        from fullon_ohlcv.models import Candle
        import time
        
        current_time = int(time.time())
        sample_candles = [
            Candle(
                timestamp=current_time - 120,
                open=50000.0,
                high=51000.0, 
                low=49500.0,
                close=50800.0,
                volume=1.5
            ),
            Candle(
                timestamp=current_time - 60,
                open=50800.0,
                high=52000.0,
                low=50200.0, 
                close=51500.0,
                volume=2.1
            ),
        ]
        
        async with CandleRepository(exchange=exchange, symbol=symbol, test=True) as repo:
            success = await repo.save_candles(sample_candles)
            
        logger.info("Successfully saved candles", exchange=exchange, symbol=symbol, success=success, count=len(sample_candles))
        
    except Exception as e:
        logger.error("Error saving candles", exchange=exchange, symbol=symbol, error=str(e))


if __name__ == "__main__":
    logger = get_component_logger("fullon.examples.data_retrieval_main")
    
    logger.info("Starting OHLCV data retrieval examples")
    
    logger.info("=== Running: Get Bars from Cache ===")
    asyncio.run(get_bars_from_cache_example())
    
    logger.info("=== Running: Get Latest Timestamps ===") 
    asyncio.run(get_latest_timestamp_example())
    
    logger.info("=== Running: Multiple Symbols Latest Prices ===")
    asyncio.run(multiple_symbols_latest_prices())
    
    logger.info("=== Running: Data Freshness Check ===") 
    asyncio.run(check_data_freshness())
    
    logger.info("=== Running: Save Sample Candles Example ===")
    asyncio.run(save_sample_candles_example())
    
    logger.info("All OHLCV data retrieval examples completed")