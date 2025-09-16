#!/usr/bin/env python3
"""
Example: WebSocket Callback Examples

Shows how to use fullon_exchange for WebSocket subscriptions with callbacks.
Includes trade data conversion to 1-minute bars and database storage.
"""

import asyncio
import arrow
from datetime import datetime, timedelta
from typing import Dict, List
from fullon_log import get_component_logger
from fullon_exchange.queue import ExchangeQueue
from fullon_ohlcv import CandleRepository, Candle
from fullon_cache import OHLCVCache


class TradeToBarConverter:
    """
    Converts individual trades to 1-minute OHLCV bars.
    Based on legacy code patterns.
    """
    
    def __init__(self, exchange: str, symbol: str):
        self.logger = get_component_logger(f"fullon.trade_converter.{exchange}.{symbol}")
        self.exchange = exchange
        self.symbol = symbol
        self.current_bar = None
        self.current_minute = None
        
    async def process_trade(self, trade_data: Dict):
        """Process a single trade and update current bar"""
        
        try:
            timestamp = trade_data['timestamp']
            price = float(trade_data['price'])
            amount = float(trade_data['amount'])
            
            # Get minute boundary
            trade_time = arrow.get(timestamp)
            minute_start = trade_time.floor('minute')
            
            # If new minute, save previous bar and start new one
            if self.current_minute != minute_start:
                if self.current_bar:
                    await self._save_completed_bar()
                    
                self.current_minute = minute_start
                self.current_bar = {
                    'timestamp': minute_start.isoformat(),
                    'open': price,
                    'high': price,
                    'low': price,
                    'close': price,
                    'volume': amount,
                    'trade_count': 1
                }
            else:
                # Update existing bar
                if self.current_bar:
                    self.current_bar['high'] = max(self.current_bar['high'], price)
                    self.current_bar['low'] = min(self.current_bar['low'], price)
                    self.current_bar['close'] = price
                    self.current_bar['volume'] += amount
                    self.current_bar['trade_count'] += 1
                    
            self.logger.debug(
                "Trade processed",
                price=price,
                amount=amount,
                current_bar_volume=self.current_bar['volume'] if self.current_bar else 0
            )
            
        except Exception as e:
            self.logger.error("Error processing trade", error=str(e), trade_data=trade_data)
            
    async def _save_completed_bar(self):
        """Save completed 1-minute bar to cache"""
        if not self.current_bar:
            return
            
        try:
            # Convert to bar format [timestamp, open, high, low, close, volume]
            bar_data = [
                self.current_bar['timestamp'],
                self.current_bar['open'],
                self.current_bar['high'],
                self.current_bar['low'],
                self.current_bar['close'],
                self.current_bar['volume']
            ]
            
            # Save to database using CandleRepository
            candle = Candle(
                timestamp=self.current_bar['timestamp'],
                open=self.current_bar['open'],
                high=self.current_bar['high'],
                low=self.current_bar['low'],
                close=self.current_bar['close'],
                volume=self.current_bar['volume']
            )
            
            async with CandleRepository(exchange=self.exchange, symbol=self.symbol, test=True) as repo:
                success = await repo.save_candles([candle])
                
            self.logger.info(
                "Saved 1-minute bar to database",
                symbol=self.symbol,
                timestamp=self.current_bar['timestamp'],
                ohlc=f"{self.current_bar['open']}/{self.current_bar['high']}/{self.current_bar['low']}/{self.current_bar['close']}",
                volume=self.current_bar['volume'],
                trades=self.current_bar['trade_count'],
                success=success
            )
            
        except Exception as e:
            self.logger.error("Error saving bar", error=str(e), bar=self.current_bar)
            
    async def force_save_current_bar(self):
        """Force save current bar (called on shutdown)"""
        if self.current_bar:
            await self._save_completed_bar()


async def websocket_candle_callback_example():
    """
    Example: Direct OHLCV candle subscription (for exchanges that support it)
    This is simpler than trade-to-bar conversion.
    """
    
    exchange = "kraken"
    symbol = "BTC/USD"
    logger = get_component_logger(f"fullon.ohlcv_callback.{exchange}.{symbol}")
    
    # Initialize exchange factory
    await ExchangeQueue.initialize_factory()
    
    try:
        # Create exchange object for new API
        class SimpleExchange:
            def __init__(self, exchange_name: str, account_id: str):
                self.ex_id = f"{exchange_name}_{account_id}"
                self.uid = account_id
                self.test = False
                self.cat_exchange = type('CatExchange', (), {'name': exchange_name})()

        exchange_obj = SimpleExchange(exchange, "ohlcv_account")

        # Create credential provider for public data
        def credential_provider(exchange_obj):
            return "", ""  # Empty for public data

        # Get exchange handler
        handler = await ExchangeQueue.get_websocket_handler(exchange_obj, credential_provider)
        await handler.connect()
        
        # Define callback for OHLCV data
        async def on_ohlcv_data(ohlcv_data):
            """Callback for 1-minute OHLCV candles"""
            try:
                # Convert to bar format and save to cache
                bar_data = [
                    ohlcv_data['timestamp'],
                    float(ohlcv_data['open']),
                    float(ohlcv_data['high']),
                    float(ohlcv_data['low']),
                    float(ohlcv_data['close']),
                    float(ohlcv_data['volume']),
                ]
                
                # Save to database using CandleRepository
                candle = Candle(
                    timestamp=bar_data[0],
                    open=bar_data[1],
                    high=bar_data[2],
                    low=bar_data[3],
                    close=bar_data[4],
                    volume=bar_data[5]
                )
                
                async with CandleRepository(exchange=exchange, symbol=symbol, test=True) as repo:
                    success = await repo.save_candles([candle])
                    
                logger.info(
                    "Saved candle to database",
                    exchange=exchange,
                    symbol=symbol,
                    timestamp=bar_data[0],
                    close=bar_data[4],
                    volume=bar_data[5],
                    success=success
                )
                    
            except Exception as e:
                logger.error("Error in OHLCV callback", error=str(e))
        
        # Connect and subscribe
        await handler.connect()
        logger.info("Connected to WebSocket")
        
        # Subscribe to 1-minute OHLCV
        await handler.subscribe_ohlcv(
            symbol=symbol,
            callback=on_ohlcv_data,
            interval="1m"
        )
        
        logger.info("Subscribed to OHLCV data")
        
        # Keep running for demo (in production this runs indefinitely)
        print(f"Listening for OHLCV data on {exchange}:{symbol}...")
        await asyncio.sleep(60)  # Run for 1 minute
        
    finally:
        await ExchangeQueue.shutdown_factory()


async def websocket_trade_to_bar_callback_example():
    """
    Example: Trade subscription with conversion to 1-minute bars.
    This is for exchanges that only provide trade data (like legacy code).
    """
    
    exchange = "kraken"  
    symbol = "BTC/USD"
    logger = get_component_logger(f"fullon.trade_callback.{exchange}.{symbol}")
    
    # Initialize trade-to-bar converter
    converter = TradeToBarConverter(exchange=exchange, symbol=symbol)
    
    # Initialize exchange factory
    await ExchangeQueue.initialize_factory()
    
    try:
        # Create exchange object for new API
        class SimpleExchange:
            def __init__(self, exchange_name: str, account_id: str):
                self.ex_id = f"{exchange_name}_{account_id}"
                self.uid = account_id
                self.test = False
                self.cat_exchange = type('CatExchange', (), {'name': exchange_name})()

        exchange_obj = SimpleExchange(exchange, "trade_account")

        # Create credential provider for public data
        def credential_provider(exchange_obj):
            return "", ""  # Empty for public data

        # Get exchange handler
        handler = await ExchangeQueue.get_websocket_handler(exchange_obj, credential_provider)
        await handler.connect()
        
        # Define callback for trade data
        async def on_trade_data(trade_data):
            """
            Callback for individual trades - converts to bars like legacy code
            """
            try:
                # Save individual trade to cache first
                async with TradesCache() as cache:
                    await cache.store_trade(
                        exchange=exchange,
                        symbol=symbol,
                        trade_data=trade_data
                    )
                
                # Convert trade to bar data
                await converter.process_trade(trade_data)
                
                logger.debug(
                    "Trade processed",
                    price=trade_data['price'],
                    amount=trade_data['amount'],
                    side=trade_data.get('side', 'unknown')
                )
                
            except Exception as e:
                logger.error("Error in trade callback", error=str(e))
        
        # Connect and subscribe
        await handler.connect()
        logger.info("Connected to WebSocket")
        
        # Subscribe to trades
        await handler.subscribe_trades(
            symbol=symbol,
            callback=on_trade_data
        )
        
        logger.info("Subscribed to trade data")
        
        # Keep running and convert trades to bars
        print(f"Listening for trade data on {exchange}:{symbol}...")
        print("Converting trades to 1-minute bars...")
        
        # Run for demo period
        await asyncio.sleep(120)  # Run for 2 minutes
        
        # Force save any incomplete bar on shutdown
        await converter.force_save_current_bar()
        
    finally:
        await ExchangeQueue.shutdown_factory()


async def websocket_with_reconnection_example():
    """
    Example: WebSocket with reconnection logic (production pattern)
    """
    
    exchange = "binance"
    symbol = "ETH/USD"
    logger = get_component_logger(f"fullon.reconnect_example.{exchange}.{symbol}")
    
    # Initialize exchange factory
    await ExchangeQueue.initialize_factory()
    
    reconnect_count = 0
    max_reconnects = 5
    
    while reconnect_count < max_reconnects:
        try:
            # Get fresh handler for each connection attempt
            # Create exchange object for new API
            class SimpleExchange:
                def __init__(self, exchange_name: str, account_id: str):
                    self.ex_id = f"{exchange_name}_{account_id}"
                    self.uid = account_id
                    self.test = False
                    self.cat_exchange = type('CatExchange', (), {'name': exchange_name})()

            exchange_obj = SimpleExchange(exchange, "ohlcv_account")

            # Create credential provider for public data
            def credential_provider(exchange_obj):
                return "", ""  # Empty for public data

            handler = await ExchangeQueue.get_websocket_handler(exchange_obj, credential_provider)
            await handler.connect()
            
            # Define callback
            async def on_ohlcv_data(ohlcv_data):
                # Save to database
                async with CandleRepository(exchange=exchange, symbol=symbol, test=True) as repo:
                    candle = {
                        'timestamp': ohlcv_data['timestamp'],
                        'open': float(ohlcv_data['open']),
                        'high': float(ohlcv_data['high']),
                        'low': float(ohlcv_data['low']),
                        'close': float(ohlcv_data['close']),
                        'volume': float(ohlcv_data['volume']),
                    }
                    await db.store_candles([candle])
                    
                # Update process status in cache
                async with ProcessCache() as cache:
                    key = f"{exchange}:{symbol}"
                    await cache.update_process(
                        type="ohlcv",
                        key=key,
                        status="active",
                        last_update=datetime.now().isoformat()
                    )
            
            # Connect and subscribe
            await handler.connect()
            logger.info("Connected to WebSocket", attempt=reconnect_count + 1)
            
            await handler.subscribe_ohlcv(
                symbol=symbol,
                callback=on_ohlcv_data,
                interval="1m"
            )
            
            # Keep connection alive and monitor health
            while handler.is_connected():
                await asyncio.sleep(5)
                
                # Health check
                if not handler.is_connected():
                    logger.warning("Connection lost, will reconnect")
                    break
                    
            # Connection lost - prepare for reconnect
            reconnect_count += 1
            
            if reconnect_count < max_reconnects:
                wait_time = min(60, 2 ** reconnect_count)  # Exponential backoff
                logger.info(f"Reconnecting in {wait_time} seconds...", attempt=reconnect_count)
                await asyncio.sleep(wait_time)
            
        except Exception as e:
            logger.error("WebSocket error", error=str(e), attempt=reconnect_count + 1)
            reconnect_count += 1
            
            if reconnect_count < max_reconnects:
                wait_time = min(60, 2 ** reconnect_count)
                await asyncio.sleep(wait_time)
    
    logger.error("Max reconnection attempts reached", max_attempts=max_reconnects)
    await ExchangeQueue.shutdown_factory()


async def multiple_symbol_websockets_example():
    """
    Example: Managing multiple WebSocket subscriptions
    """
    
    symbols = [
        ("kraken", "BTC/USD"),
        ("binance", "ETH/USD"),
        ("kraken", "ETH/USD"),
    ]
    
    logger = get_component_logger("fullon.multi_websocket_example")
    
    # Initialize exchange factory
    await ExchangeQueue.initialize_factory()
    
    tasks = []
    
    try:
        for exchange, symbol in symbols:
            # Create task for each symbol
            task = asyncio.create_task(
                _run_single_websocket(exchange, symbol)
            )
            tasks.append(task)
            
        logger.info(f"Started {len(tasks)} WebSocket connections")
        
        # Run all connections
        await asyncio.sleep(60)  # Demo duration
        
        # Cancel all tasks
        for task in tasks:
            task.cancel()
            
        # Wait for cleanup
        await asyncio.gather(*tasks, return_exceptions=True)
        
    finally:
        await ExchangeQueue.shutdown_factory()


async def _run_single_websocket(exchange: str, symbol: str):
    """Helper function to run single WebSocket connection"""
    logger = get_component_logger(f"fullon.websocket.{exchange}.{symbol}")
    
    try:
        # Create exchange object for new API
        class SimpleExchange:
            def __init__(self, exchange_name: str, account_id: str):
                self.ex_id = f"{exchange_name}_{account_id}"
                self.uid = account_id
                self.test = False
                self.cat_exchange = type('CatExchange', (), {'name': exchange_name})()

        exchange_obj = SimpleExchange(exchange, "ohlcv_account")

        # Create credential provider for public data
        def credential_provider(exchange_obj):
            return "", ""  # Empty for public data

        handler = await ExchangeQueue.get_websocket_handler(exchange_obj, credential_provider)
        await handler.connect()
        
        async def callback(ohlcv_data):
            # Simple callback to save data
            async with OHLCVDatabase(exchange=exchange, symbol=symbol) as db:
                candle = {
                    'timestamp': ohlcv_data['timestamp'],
                    'open': float(ohlcv_data['open']),
                    'high': float(ohlcv_data['high']),
                    'low': float(ohlcv_data['low']),
                    'close': float(ohlcv_data['close']),
                    'volume': float(ohlcv_data['volume']),
                }
                await db.store_candles([candle])
                
            logger.debug("Saved candle", close=candle['close'])
        
        await handler.connect()
        await handler.subscribe_ohlcv(symbol=symbol, callback=callback, interval="1m")
        
        # Keep running until cancelled
        while True:
            await asyncio.sleep(1)
            
    except asyncio.CancelledError:
        logger.info("WebSocket cancelled")
    except Exception as e:
        logger.error("WebSocket error", error=str(e))


if __name__ == "__main__":
    logger = get_component_logger("fullon.examples.websocket_main")
    
    logger.info("Starting WebSocket callback examples")
    
    logger.info("=== Running: WebSocket OHLCV Callback ===")
    asyncio.run(websocket_candle_callback_example())
    
    logger.info("=== Running: WebSocket Trade-to-Bar Callback ===")
    asyncio.run(websocket_trade_to_bar_callback_example())
    
    logger.info("=== Running: WebSocket with Reconnection ===")
    asyncio.run(websocket_with_reconnection_example())
    
    logger.info("=== Running: Multiple Symbol WebSockets ===")
    asyncio.run(multiple_symbol_websockets_example())
    
    logger.info("All WebSocket callback examples completed")