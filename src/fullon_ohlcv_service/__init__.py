"""
fullon_ohlcv_service - Async daemon service for OHLCV and trade data collection

This package provides async services for collecting:
- OHLCV 1-minute candles from cryptocurrency exchanges
- Individual trade data from cryptocurrency exchanges

Data is stored using fullon_ohlcv database and coordinated via fullon_cache.
"""

__version__ = "0.1.0"
__author__ = "ingmar"
__email__ = "ingmar@avocadoblock.com"

from fullon_ohlcv_service.config import Config

__all__ = ["Config"]