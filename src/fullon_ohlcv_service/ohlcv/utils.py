"""Shared utilities for OHLCV collectors."""

from typing import List, Any
from fullon_log import get_component_logger
from fullon_ohlcv.models import Candle

logger = get_component_logger("fullon.ohlcv.utils")


def timeframe_to_seconds(timeframe: str) -> int:
    """Convert timeframe string to seconds.

    Args:
        timeframe: Timeframe string (e.g., "1m", "5m", "1h", "1d")

    Returns:
        Number of seconds in the timeframe
    """
    if not timeframe:
        return 60  # Default to 1 minute

    # Extract number and unit
    if timeframe[-1] == 'm':
        minutes = int(timeframe[:-1])
        return minutes * 60
    elif timeframe[-1] == 'h':
        hours = int(timeframe[:-1])
        return hours * 3600
    elif timeframe[-1] == 'd':
        days = int(timeframe[:-1])
        return days * 86400
    elif timeframe[-1] == 'w':
        weeks = int(timeframe[:-1])
        return weeks * 604800
    else:
        logger.warning(f"Unknown timeframe format: {timeframe}, defaulting to 1m")
        return 60


def convert_to_candle_objects(batch_candles: List[Any]) -> List[Candle]:
    """Convert raw OHLCV data to Candle objects.

    Args:
        batch_candles: List of OHLCV data (list, dict, or object format)

    Returns:
        List of Candle objects
    """
    from datetime import UTC, datetime

    candle_objects = []

    for raw_candle in batch_candles:
        try:
            # Handle list format [timestamp, open, high, low, close, volume]
            if isinstance(raw_candle, list) and len(raw_candle) >= 6:
                timestamp = raw_candle[0]
                open_price = raw_candle[1]
                high = raw_candle[2]
                low = raw_candle[3]
                close = raw_candle[4]
                volume = raw_candle[5]
            # Handle dict format
            elif isinstance(raw_candle, dict):
                timestamp = raw_candle.get("timestamp", raw_candle.get("datetime", 0))
                open_price = raw_candle.get("open", 0.0)
                high = raw_candle.get("high", 0.0)
                low = raw_candle.get("low", 0.0)
                close = raw_candle.get("close", 0.0)
                volume = raw_candle.get("volume", 0.0)
            # Handle object format
            else:
                timestamp = getattr(raw_candle, "timestamp", getattr(raw_candle, "datetime", 0))
                open_price = getattr(raw_candle, "open", 0.0)
                high = getattr(raw_candle, "high", 0.0)
                low = getattr(raw_candle, "low", 0.0)
                close = getattr(raw_candle, "close", 0.0)
                volume = getattr(raw_candle, "volume", 0.0)

            # Skip candles with None values
            if any(x is None for x in [timestamp, open_price, high, low, close, volume]):
                logger.warning("Skipping candle with None values", raw_candle=raw_candle)
                continue

            # Convert timestamp to datetime
            timestamp_dt = datetime.fromtimestamp(
                timestamp / 1000 if timestamp > 1e12 else timestamp, tz=UTC
            )

            candle_objects.append(
                Candle(
                    timestamp=timestamp_dt,
                    open=float(open_price),
                    high=float(high),
                    low=float(low),
                    close=float(close),
                    vol=float(volume),
                )
            )
        except Exception as e:
            logger.warning("Failed to convert candle", error=str(e))
            continue

    return candle_objects
