"""Configuration module for fullon_ohlcv_service.

This module provides database-driven configuration loading
using fullon_orm, following the ticker service pattern.
"""

from .database_config import get_collection_targets, should_collect_ohlcv
from .settings import OhlcvServiceConfig, config

__all__ = ['get_collection_targets', 'should_collect_ohlcv', 'OhlcvServiceConfig', 'config']
