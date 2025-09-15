"""Simple OHLCV service for fullon ecosystem"""

__version__ = "0.1.0"

from .config.settings import config
from .ohlcv.collector import OhlcvCollector
from .ohlcv.manager import OhlcvManager

__all__ = ['config', 'OhlcvCollector', 'OhlcvManager']
