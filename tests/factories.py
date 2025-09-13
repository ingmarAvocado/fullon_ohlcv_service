"""Test data factories for fullon_ohlcv_service.

This module provides factory functions for creating test data objects
with sensible defaults that can be easily overridden.
Adapted from fullon_ohlcv factories for service-level testing.
"""

import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional, List, Dict, Any
import factory
from factory import fuzzy


# ============================================================================
# BASE FACTORY CLASSES
# ============================================================================


class BaseOHLCVFactory(factory.Factory):
    """Base factory for OHLCV service testing."""
    
    class Meta:
        abstract = True


# ============================================================================
# OHLCV CANDLE FACTORIES
# ============================================================================


class CandleDataFactory(BaseOHLCVFactory):
    """Factory for creating OHLCV candle data dictionaries."""
    
    class Meta:
        model = dict
    
    timestamp = factory.LazyFunction(
        lambda: datetime.now(timezone.utc).replace(second=0, microsecond=0)
    )
    open = factory.LazyFunction(lambda: round(random.uniform(30000, 50000), 2))
    high = factory.LazyAttribute(lambda obj: obj.open + random.uniform(0, 1000))
    low = factory.LazyAttribute(lambda obj: obj.open - random.uniform(0, 1000))
    close = factory.LazyAttribute(
        lambda obj: obj.low + random.uniform(0, obj.high - obj.low)
    )
    volume = factory.LazyFunction(lambda: round(random.uniform(100, 10000), 4))
    symbol = "BTC/USDT"
    exchange = "binance"


class TradeDataFactory(BaseOHLCVFactory):
    """Factory for creating trade data dictionaries."""
    
    class Meta:
        model = dict
    
    timestamp = factory.LazyFunction(lambda: datetime.now(timezone.utc))
    price = factory.LazyFunction(lambda: round(random.uniform(30000, 50000), 2))
    volume = factory.LazyFunction(lambda: round(random.uniform(0.01, 10.0), 8))
    side = fuzzy.FuzzyChoice(['buy', 'sell'])
    type = fuzzy.FuzzyChoice(['market', 'limit'])
    symbol = "BTC/USDT"
    exchange = "binance"
    order_id = factory.LazyFunction(
        lambda: f"order_{random.randint(1000, 9999)}" if random.random() > 0.5 else None
    )


# ============================================================================
# SERVICE CONFIGURATION FACTORIES
# ============================================================================


class OHLCVServiceConfigFactory(BaseOHLCVFactory):
    """Factory for OHLCV service configuration."""
    
    class Meta:
        model = dict
    
    exchanges = ["binance", "kraken"]
    symbols = ["BTC/USDT", "ETH/USDT"]
    collection_interval = 60
    batch_size = 100
    enabled = True
    historical_days = 7
    websocket_timeout = 30
    retry_attempts = 3


class TradeServiceConfigFactory(BaseOHLCVFactory):
    """Factory for trade service configuration."""
    
    class Meta:
        model = dict
    
    exchanges = ["binance", "kraken"]
    symbols = ["BTC/USDT", "ETH/USDT"]
    collection_enabled = True
    batch_size = 50
    store_raw_trades = True
    aggregate_to_candles = True
    websocket_timeout = 30


class CollectorConfigFactory(BaseOHLCVFactory):
    """Factory for individual collector configuration."""
    
    class Meta:
        model = dict
    
    symbol = "BTC/USDT"
    exchange = "binance"
    collection_type = fuzzy.FuzzyChoice(["ohlcv", "trade"])
    enabled = True
    retry_count = 0
    last_error = None
    status = "running"


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================


def create_candle_list(
    count: int = 24,
    base_time: Optional[datetime] = None,
    timeframe: str = "1h",
    symbol: str = "BTC/USDT",
    exchange: str = "binance",
    **kwargs
) -> List[Dict[str, Any]]:
    """Create a list of candles with proper time spacing.
    
    Args:
        count: Number of candles to create
        base_time: Starting timestamp (defaults to now)
        timeframe: Candle timeframe (1m, 5m, 15m, 1h, 4h, 1d)
        symbol: Trading symbol
        exchange: Exchange name
        **kwargs: Override any candle attributes
    
    Returns:
        List of candle data dictionaries
    """
    # Map timeframe to timedelta
    timeframe_map = {
        "1m": timedelta(minutes=1),
        "5m": timedelta(minutes=5),
        "15m": timedelta(minutes=15),
        "1h": timedelta(hours=1),
        "4h": timedelta(hours=4),
        "1d": timedelta(days=1),
    }
    
    interval = timeframe_map.get(timeframe, timedelta(hours=1))
    
    if base_time is None:
        base_time = datetime.now(timezone.utc) - (interval * count)
        # Align to timeframe boundary
        if timeframe == "1h":
            base_time = base_time.replace(minute=0, second=0, microsecond=0)
        elif timeframe == "1d":
            base_time = base_time.replace(hour=0, minute=0, second=0, microsecond=0)
    
    candles = []
    for i in range(count):
        timestamp = base_time + (interval * i)
        candle = CandleDataFactory(
            timestamp=timestamp,
            symbol=symbol,
            exchange=exchange,
            **kwargs
        )
        candles.append(candle)
    
    return candles


def create_trade_list(
    count: int = 10,
    base_time: Optional[datetime] = None,
    time_interval: timedelta = timedelta(seconds=1),
    symbol: str = "BTC/USDT",
    exchange: str = "binance",
    **kwargs
) -> List[Dict[str, Any]]:
    """Create a list of trades with incrementing timestamps.
    
    Args:
        count: Number of trades to create
        base_time: Starting timestamp (defaults to now)
        time_interval: Time between trades
        symbol: Trading symbol
        exchange: Exchange name
        **kwargs: Override any trade attributes
    
    Returns:
        List of trade data dictionaries
    """
    if base_time is None:
        base_time = datetime.now(timezone.utc) - timedelta(seconds=count)
    
    trades = []
    for i in range(count):
        timestamp = base_time + (time_interval * i)
        trade = TradeDataFactory(
            timestamp=timestamp,
            symbol=symbol,
            exchange=exchange,
            **kwargs
        )
        trades.append(trade)
    
    return trades


def create_price_series(
    start_price: float = 45000.0,
    count: int = 100,
    volatility: float = 0.01,
    trend: float = 0.0
) -> List[float]:
    """Create a realistic price series with random walk.
    
    Args:
        start_price: Initial price
        count: Number of prices to generate
        volatility: Price volatility (0.01 = 1%)
        trend: Trend bias (-1 to 1)
    
    Returns:
        List of prices
    """
    prices = [start_price]
    
    for _ in range(count - 1):
        change = random.gauss(trend * volatility, volatility)
        new_price = prices[-1] * (1 + change)
        prices.append(max(new_price, 0.01))  # Ensure positive
    
    return prices


def create_volume_series(
    base_volume: float = 100.0,
    count: int = 100,
    volatility: float = 0.5
) -> List[float]:
    """Create realistic volume series.
    
    Args:
        base_volume: Average volume
        count: Number of volumes to generate
        volatility: Volume volatility
    
    Returns:
        List of volumes
    """
    return [
        max(random.gauss(base_volume, base_volume * volatility), 0.01)
        for _ in range(count)
    ]


def create_realistic_trades(
    count: int = 100,
    exchange: str = "binance",
    symbol: str = "BTC/USDT",
    start_time: Optional[datetime] = None,
    price_volatility: float = 0.001,
    volume_range: tuple = (0.01, 2.0)
) -> List[Dict[str, Any]]:
    """Create realistic trade data with correlated prices.
    
    Args:
        count: Number of trades
        exchange: Exchange name
        symbol: Trading symbol
        start_time: Starting timestamp
        price_volatility: Price movement volatility
        volume_range: Min/max volume range
    
    Returns:
        List of realistic trade data dictionaries
    """
    if start_time is None:
        start_time = datetime.now(timezone.utc) - timedelta(hours=1)
    
    # Generate price series
    prices = create_price_series(
        start_price=45000.0,
        count=count,
        volatility=price_volatility
    )
    
    trades = []
    for i in range(count):
        # Random time intervals (0.1 to 10 seconds)
        time_delta = timedelta(seconds=random.uniform(0.1, 10))
        timestamp = start_time + time_delta * i
        
        # Determine side based on price movement
        if i > 0 and prices[i] > prices[i-1]:
            side = "buy" if random.random() > 0.3 else "sell"
        else:
            side = "sell" if random.random() > 0.3 else "buy"
        
        trade = TradeDataFactory(
            timestamp=timestamp,
            price=prices[i],
            volume=random.uniform(*volume_range),
            side=side,
            type="market" if random.random() > 0.2 else "limit",
            symbol=symbol,
            exchange=exchange
        )
        trades.append(trade)
    
    return trades


def create_realistic_candles(
    count: int = 24,
    timeframe: str = "1h",
    start_time: Optional[datetime] = None,
    base_price: float = 45000.0,
    volatility: float = 0.02,
    symbol: str = "BTC/USDT",
    exchange: str = "binance"
) -> List[Dict[str, Any]]:
    """Create realistic OHLCV candles with proper relationships.
    
    Args:
        count: Number of candles
        timeframe: Candle timeframe
        start_time: Starting timestamp
        base_price: Starting price
        volatility: Price volatility
        symbol: Trading symbol
        exchange: Exchange name
    
    Returns:
        List of realistic candle data dictionaries
    """
    candles = []
    current_price = base_price
    
    # Get time interval
    timeframe_map = {
        "1m": timedelta(minutes=1),
        "5m": timedelta(minutes=5),
        "15m": timedelta(minutes=15),
        "1h": timedelta(hours=1),
        "4h": timedelta(hours=4),
        "1d": timedelta(days=1),
    }
    interval = timeframe_map.get(timeframe, timedelta(hours=1))
    
    if start_time is None:
        start_time = datetime.now(timezone.utc) - (interval * count)
        if timeframe in ["1h", "4h"]:
            start_time = start_time.replace(minute=0, second=0, microsecond=0)
        elif timeframe == "1d":
            start_time = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
    
    for i in range(count):
        timestamp = start_time + (interval * i)
        
        # Generate OHLC with realistic relationships
        open_price = current_price
        
        # Generate high and low
        max_move = current_price * volatility
        high = open_price + random.uniform(0, max_move)
        low = open_price - random.uniform(0, max_move)
        
        # Close should be between high and low
        close = random.uniform(low, high)
        
        # Volume with some correlation to price movement
        price_change = abs(close - open_price) / open_price
        base_volume = 1000.0
        volume = base_volume * (1 + price_change * 10) * random.uniform(0.5, 1.5)
        
        candle = {
            "timestamp": timestamp,
            "open": round(open_price, 2),
            "high": round(high, 2),
            "low": round(low, 2),
            "close": round(close, 2),
            "volume": round(volume, 4),
            "symbol": symbol,
            "exchange": exchange
        }
        candles.append(candle)
        
        # Next candle opens at previous close
        current_price = close
    
    return candles


# ============================================================================
# MOCK DATA FOR SPECIFIC EXCHANGES
# ============================================================================


def create_binance_candle_data(**kwargs) -> Dict[str, Any]:
    """Create Binance-specific candle data."""
    defaults = {
        "exchange": "binance",
        "symbol": "BTCUSDT"  # Binance format
    }
    defaults.update(kwargs)
    return CandleDataFactory(**defaults)


def create_kraken_candle_data(**kwargs) -> Dict[str, Any]:
    """Create Kraken-specific candle data."""
    defaults = {
        "exchange": "kraken",
        "symbol": "BTC/USD"  # Kraken format
    }
    defaults.update(kwargs)
    return CandleDataFactory(**defaults)


def create_binance_trade_data(**kwargs) -> Dict[str, Any]:
    """Create Binance-specific trade data."""
    defaults = {
        "exchange": "binance",
        "symbol": "BTCUSDT"
    }
    defaults.update(kwargs)
    return TradeDataFactory(**defaults)


def create_kraken_trade_data(**kwargs) -> Dict[str, Any]:
    """Create Kraken-specific trade data."""
    defaults = {
        "exchange": "kraken",
        "symbol": "BTC/USD"
    }
    defaults.update(kwargs)
    return TradeDataFactory(**defaults)


# ============================================================================
# SERVICE STATUS AND HEALTH DATA
# ============================================================================


class ServiceStatusFactory(BaseOHLCVFactory):
    """Factory for service status data."""
    
    class Meta:
        model = dict
    
    service_type = fuzzy.FuzzyChoice(["ohlcv", "trade"])
    status = fuzzy.FuzzyChoice(["running", "stopped", "error", "starting"])
    collectors_active = factory.LazyFunction(lambda: random.randint(0, 10))
    symbols_monitored = factory.LazyFunction(lambda: random.randint(1, 50))
    last_collection = factory.LazyFunction(lambda: datetime.now(timezone.utc))
    errors_count = factory.LazyFunction(lambda: random.randint(0, 5))
    uptime_seconds = factory.LazyFunction(lambda: random.randint(0, 86400))


class CollectorStatusFactory(BaseOHLCVFactory):
    """Factory for individual collector status."""
    
    class Meta:
        model = dict
    
    symbol = "BTC/USDT"
    exchange = "binance"
    status = fuzzy.FuzzyChoice(["collecting", "stopped", "error", "reconnecting"])
    last_update = factory.LazyFunction(lambda: datetime.now(timezone.utc))
    error_count = factory.LazyFunction(lambda: random.randint(0, 3))
    data_points_collected = factory.LazyFunction(lambda: random.randint(0, 1000))
    connection_healthy = True


# ============================================================================ 
# REPOSITORY MODEL FACTORIES
# ============================================================================


def create_candle_models(
    count: int = 10,
    exchange: str = "binance", 
    symbol: str = "BTC/USDT",
    **kwargs
) -> List:
    """Create Candle model objects for Repository testing.
    
    Args:
        count: Number of candle models to create
        exchange: Exchange name
        symbol: Trading symbol
        **kwargs: Additional candle attributes
    
    Returns:
        List of Candle model objects
    """
    from fullon_ohlcv.models import Candle
    
    candle_dicts = create_candle_list(count=count, symbol=symbol, exchange=exchange, **kwargs)
    candles = []
    
    for candle_data in candle_dicts:
        candle = Candle(
            timestamp=candle_data["timestamp"],
            open=candle_data["open"],
            high=candle_data["high"], 
            low=candle_data["low"],
            close=candle_data["close"],
            vol=candle_data["volume"]
        )
        candles.append(candle)
    
    return candles


def create_trade_models(
    count: int = 10,
    exchange: str = "binance",
    symbol: str = "BTC/USDT", 
    **kwargs
) -> List:
    """Create Trade model objects for Repository testing.
    
    Args:
        count: Number of trade models to create
        exchange: Exchange name
        symbol: Trading symbol
        **kwargs: Additional trade attributes
    
    Returns:
        List of Trade model objects
    """
    from fullon_ohlcv.models import Trade
    
    trade_dicts = create_trade_list(count=count, symbol=symbol, exchange=exchange, **kwargs)
    trades = []
    
    for trade_data in trade_dicts:
        trade = Trade(
            timestamp=trade_data["timestamp"],
            price=trade_data["price"],
            volume=trade_data["volume"],
            side=trade_data["side"].upper(),  # Trade model expects uppercase
            type=trade_data["type"].upper()   # Trade model expects uppercase
        )
        trades.append(trade)
    
    return trades


# ============================================================================
# EXPORTED FUNCTIONS
# ============================================================================


__all__ = [
    # Factories
    "CandleDataFactory",
    "TradeDataFactory", 
    "OHLCVServiceConfigFactory",
    "TradeServiceConfigFactory",
    "CollectorConfigFactory",
    "ServiceStatusFactory",
    "CollectorStatusFactory",
    
    # Helper functions
    "create_candle_list",
    "create_trade_list",
    "create_price_series",
    "create_volume_series",
    "create_realistic_trades",
    "create_realistic_candles",
    
    # Exchange-specific
    "create_binance_candle_data",
    "create_kraken_candle_data",
    "create_binance_trade_data", 
    "create_kraken_trade_data",
    
    # Repository model factories
    "create_candle_models",
    "create_trade_models",
]