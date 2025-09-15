"""Integration tests for configuration usage across the system"""

import os
import pytest
from unittest.mock import patch, AsyncMock, MagicMock

from fullon_ohlcv_service.config.settings import OhlcvServiceConfig
from fullon_ohlcv_service.ohlcv.collector import OhlcvCollector
from fullon_ohlcv_service.ohlcv.manager import OhlcvManager


class TestConfigIntegration:
    """Test that configuration is properly used by collectors and managers"""

    @pytest.mark.asyncio
    async def test_collector_uses_config(self):
        """Test that OhlcvCollector respects configuration settings"""
        # Create custom config
        config = OhlcvServiceConfig(
            user_id=42,
            collection_interval=120,
            historical_limit=500,
            enable_streaming=False,
            enable_historical=True,
            test_mode=True
        )

        # Create collector with custom config
        collector = OhlcvCollector(
            symbol="BTC/USD",
            exchange="kraken",
            config=config
        )

        # Verify collector uses config values
        assert collector.config.user_id == 42
        assert collector.config.collection_interval == 120
        assert collector.config.historical_limit == 500
        assert collector.config.enable_streaming is False
        assert collector.config.enable_historical is True
        assert collector.config.test_mode is True

    @pytest.mark.asyncio
    async def test_manager_uses_config(self):
        """Test that OhlcvManager respects configuration settings"""
        # Create custom config
        config = OhlcvServiceConfig(
            user_id=99,
            collection_interval=30,
            health_update_interval=60,
            test_mode=True
        )

        # Create manager with custom config
        manager = OhlcvManager(config=config)

        # Verify manager uses config values
        assert manager.config.user_id == 99
        assert manager.config.collection_interval == 30
        assert manager.config.health_update_interval == 60
        assert manager.config.test_mode is True

    @pytest.mark.asyncio
    async def test_config_affects_collector_behavior(self):
        """Test that configuration actually affects collector behavior"""
        # Test with streaming disabled
        config_no_stream = OhlcvServiceConfig(
            enable_streaming=False,
            enable_historical=True
        )

        collector_no_stream = OhlcvCollector(
            symbol="BTC/USD",
            exchange="kraken",
            config=config_no_stream
        )

        # Verify streaming is disabled
        assert not collector_no_stream.config.enable_streaming

        # Test with historical disabled
        config_no_historical = OhlcvServiceConfig(
            enable_streaming=True,
            enable_historical=False
        )

        collector_no_historical = OhlcvCollector(
            symbol="ETH/USD",
            exchange="binance",
            config=config_no_historical
        )

        # Verify historical is disabled
        assert not collector_no_historical.config.enable_historical

    @pytest.mark.asyncio
    async def test_global_config_import(self):
        """Test that global config can be imported and used"""
        from fullon_ohlcv_service import config

        # Verify it's the right type
        assert isinstance(config, OhlcvServiceConfig)

        # Create collector using global config
        collector = OhlcvCollector(
            symbol="BTC/USD",
            exchange="kraken",
            config=config
        )

        assert collector.config == config

    @pytest.mark.asyncio
    async def test_config_from_env_in_integration(self):
        """Test that environment variables are properly loaded in integration"""
        env_vars = {
            'OHLCV_USER_ID': '123',
            'OHLCV_COLLECTION_INTERVAL': '90',
            'OHLCV_TEST_MODE': 'true'
        }

        with patch.dict(os.environ, env_vars, clear=False):
            # Create new config from environment
            config = OhlcvServiceConfig.from_env()

            # Create collector with env-based config
            collector = OhlcvCollector(
                symbol="BTC/USD",
                exchange="kraken",
                config=config
            )

            # Verify environment values are used
            assert collector.config.user_id == 123
            assert collector.config.collection_interval == 90
            assert collector.config.test_mode is True