"""Tests for configuration management"""

import os
import pytest
from unittest.mock import patch
from dataclasses import is_dataclass

from fullon_ohlcv_service.config.settings import OhlcvServiceConfig


class TestOhlcvServiceConfig:
    """Test configuration management"""

    def test_config_is_dataclass(self):
        """Test that OhlcvServiceConfig is a dataclass"""
        assert is_dataclass(OhlcvServiceConfig)

    def test_default_values(self):
        """Test configuration default values"""
        config = OhlcvServiceConfig()

        assert config.user_id == 1
        assert config.collection_interval == 60
        assert config.health_update_interval == 30
        assert config.historical_limit == 1000
        assert config.enable_streaming is True
        assert config.enable_historical is True
        assert config.test_mode is False

    def test_from_env_with_defaults(self):
        """Test loading configuration from environment with defaults"""
        with patch.dict(os.environ, {}, clear=True):
            config = OhlcvServiceConfig.from_env()

            assert config.user_id == 1
            assert config.collection_interval == 60
            assert config.health_update_interval == 30
            assert config.historical_limit == 1000
            assert config.enable_streaming is True
            assert config.enable_historical is True
            assert config.test_mode is False

    def test_from_env_with_custom_values(self):
        """Test loading configuration from environment with custom values"""
        env_vars = {
            'OHLCV_USER_ID': '42',
            'OHLCV_COLLECTION_INTERVAL': '120',
            'OHLCV_HEALTH_INTERVAL': '60',
            'OHLCV_HISTORICAL_LIMIT': '500',
            'OHLCV_ENABLE_STREAMING': 'false',
            'OHLCV_ENABLE_HISTORICAL': 'false',
            'OHLCV_TEST_MODE': 'true'
        }

        with patch.dict(os.environ, env_vars, clear=True):
            config = OhlcvServiceConfig.from_env()

            assert config.user_id == 42
            assert config.collection_interval == 120
            assert config.health_update_interval == 60
            assert config.historical_limit == 500
            assert config.enable_streaming is False
            assert config.enable_historical is False
            assert config.test_mode is True

    def test_from_env_boolean_parsing(self):
        """Test boolean parsing from environment variables"""
        test_cases = [
            ('true', True),
            ('True', True),
            ('TRUE', True),
            ('false', False),
            ('False', False),
            ('FALSE', False),
            ('anything_else', False),
            ('1', False),
            ('0', False),
        ]

        for value, expected in test_cases:
            with patch.dict(os.environ, {'OHLCV_ENABLE_STREAMING': value}, clear=True):
                config = OhlcvServiceConfig.from_env()
                assert config.enable_streaming == expected, f"Failed for value: {value}"

    def test_from_env_integer_parsing(self):
        """Test integer parsing from environment variables"""
        with patch.dict(os.environ, {'OHLCV_USER_ID': '123'}, clear=True):
            config = OhlcvServiceConfig.from_env()
            assert config.user_id == 123
            assert isinstance(config.user_id, int)

    def test_from_env_invalid_integer(self):
        """Test that invalid integer values raise appropriate errors"""
        with patch.dict(os.environ, {'OHLCV_USER_ID': 'not_a_number'}, clear=True):
            with pytest.raises(ValueError):
                OhlcvServiceConfig.from_env()

    def test_config_immutability(self):
        """Test that config fields can be modified (dataclass not frozen)"""
        config = OhlcvServiceConfig()
        config.user_id = 999
        assert config.user_id == 999

    def test_config_representation(self):
        """Test string representation of config"""
        config = OhlcvServiceConfig(user_id=5, test_mode=True)
        repr_str = repr(config)

        assert 'OhlcvServiceConfig' in repr_str
        assert 'user_id=5' in repr_str
        assert 'test_mode=True' in repr_str


class TestGlobalConfig:
    """Test global config instance"""

    def test_global_config_exists(self):
        """Test that global config can be imported"""
        from fullon_ohlcv_service.config.settings import config

        assert isinstance(config, OhlcvServiceConfig)

    def test_global_config_from_env(self):
        """Test that global config is loaded from environment"""
        # Test that we can create a new config with different env vars
        with patch.dict(os.environ, {'OHLCV_USER_ID': '88'}, clear=False):
            # Create a fresh config instance
            new_config = OhlcvServiceConfig.from_env()

            # Verify the environment variable was read
            assert new_config.user_id == 88
            assert isinstance(new_config, OhlcvServiceConfig)