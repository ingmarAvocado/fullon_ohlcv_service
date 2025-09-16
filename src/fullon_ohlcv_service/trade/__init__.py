"""
Trade module - Individual trade data collection from exchanges

This module handles:
- Historical trade data backfill
- Real-time trade collection via WebSocket
- User trade synchronization
- Multi-symbol coordination and management
- Database storage via fullon_ohlcv
"""

from fullon_ohlcv_service.trade.collector import TradeCollector
from fullon_ohlcv_service.trade.manager import TradeManager

__all__ = ["TradeCollector", "TradeManager"]
