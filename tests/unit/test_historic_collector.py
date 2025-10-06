"""
Unit tests for HistoricTradeCollector capability validation.

Tests the critical capability validation requirements from Issue #41:
- handler.supports_ohlcv() checking
- handler.needs_trades_for_ohlcv() checking
- Graceful handling of unsupported exchanges
- AttributeError handling for handlers without capability methods
"""

import pytest
from unittest.mock import AsyncMock, Mock, patch
from datetime import datetime, timezone

from fullon_ohlcv_service.trade.historic_collector import HistoricTradeCollector
from fullon_orm.models import Symbol, Exchange, CatExchange


@pytest.fixture
def mock_symbol():
    """Create a mock Symbol object."""
    symbol = Mock(spec=Symbol)
    symbol.symbol = "BTC/USD"
    symbol.backtest = 30
    symbol.cat_ex_id = 1

    cat_exchange = Mock(spec=CatExchange)
    cat_exchange.name = "kraken"
    symbol.cat_exchange = cat_exchange

    return symbol


@pytest.fixture
def mock_exchange():
    """Create a mock Exchange object."""
    exchange = Mock(spec=Exchange)
    exchange.ex_id = 1
    exchange.cat_ex_id = 1

    cat_exchange = Mock(spec=CatExchange)
    cat_exchange.name = "kraken"
    exchange.cat_exchange = cat_exchange

    return exchange


@pytest.fixture
def collector():
    """Create HistoricTradeCollector instance."""
    return HistoricTradeCollector()


class TestCapabilityValidation:
    """Test capability validation in _start_exchange_historic_collector."""

    @pytest.mark.asyncio
    async def test_skips_exchange_with_native_ohlcv(
        self, collector, mock_exchange, mock_symbol
    ):
        """Test that exchanges with native OHLCV (not needing trades) are skipped."""
        # Mock handler that doesn't need trades (has native OHLCV)
        mock_handler = AsyncMock()
        mock_handler.needs_trades_for_ohlcv = Mock(return_value=False)

        with patch(
            'fullon_ohlcv_service.trade.historic_collector.ExchangeQueue.get_rest_handler',
            return_value=mock_handler
        ), patch(
            'fullon_ohlcv_service.trade.historic_collector.ExchangeQueue.initialize_factory',
            new_callable=AsyncMock
        ):
            # Should return empty results and log info
            results = await collector._start_exchange_historic_collector(
                mock_exchange, [mock_symbol]
            )

            assert results == {}
            assert mock_handler.needs_trades_for_ohlcv.called


    @pytest.mark.asyncio
    async def test_proceeds_when_exchange_needs_trades(
        self, collector, mock_exchange, mock_symbol
    ):
        """Test that collection proceeds when exchange needs trades for OHLCV."""
        # Mock handler that needs trades
        mock_handler = AsyncMock()
        mock_handler.needs_trades_for_ohlcv = Mock(return_value=True)
        mock_handler.get_public_trades = AsyncMock(return_value=[])

        with patch(
            'fullon_ohlcv_service.trade.historic_collector.ExchangeQueue.get_rest_handler',
            return_value=mock_handler
        ):
            # Should proceed with collection
            results = await collector._start_exchange_historic_collector(
                mock_exchange, [mock_symbol]
            )

            # Should have attempted collection
            assert mock_handler.needs_trades_for_ohlcv.called
            assert "kraken:BTC/USD" in results

    @pytest.mark.asyncio
    async def test_handles_handler_without_capability_methods(
        self, collector, mock_exchange, mock_symbol
    ):
        """Test graceful handling when handler lacks capability methods."""
        # Mock handler without capability methods (raises AttributeError)
        mock_handler = AsyncMock()

        # Make needs_trades_for_ohlcv raise AttributeError
        mock_handler.needs_trades_for_ohlcv = Mock(side_effect=AttributeError("Method not found"))
        mock_handler.get_public_trades = AsyncMock(return_value=[])

        with patch(
            'fullon_ohlcv_service.trade.historic_collector.ExchangeQueue.get_rest_handler',
            return_value=mock_handler
        ):
            # Should catch AttributeError and proceed with collection
            results = await collector._start_exchange_historic_collector(
                mock_exchange, [mock_symbol]
            )

            # Should have proceeded despite missing capability methods
            assert "kraken:BTC/USD" in results

    @pytest.mark.asyncio
    async def test_handles_handler_creation_failure(
        self, collector, mock_exchange, mock_symbol
    ):
        """Test graceful handling when handler creation fails."""
        with patch(
            'fullon_ohlcv_service.trade.historic_collector.ExchangeQueue.get_rest_handler',
            side_effect=Exception("Handler creation failed")
        ):
            # Should return empty results and log error
            results = await collector._start_exchange_historic_collector(
                mock_exchange, [mock_symbol]
            )

            assert results == {}


class TestCapabilityMethodsExist:
    """Test that capability validation methods are actually called."""

    @pytest.mark.asyncio
    async def test_needs_trades_for_ohlcv_is_called(
        self, collector, mock_exchange, mock_symbol
    ):
        """Verify needs_trades_for_ohlcv() method is actually invoked."""
        mock_handler = AsyncMock()
        mock_handler.needs_trades_for_ohlcv = Mock(return_value=False)

        with patch(
            'fullon_ohlcv_service.trade.historic_collector.ExchangeQueue.get_rest_handler',
            return_value=mock_handler
        ):
            await collector._start_exchange_historic_collector(
                mock_exchange, [mock_symbol]
            )

            # Verify the method was actually called
            mock_handler.needs_trades_for_ohlcv.assert_called_once()
