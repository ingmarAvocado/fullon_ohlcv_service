"""Unit tests for env_config.py"""

import os
from unittest.mock import patch

import pytest

from fullon_ohlcv_service.env_config import Config


class TestConfig:
    """Test cases for the Config class."""

    @patch.dict(os.environ, {}, clear=True)
    def test_init_default_log_level(self):
        """Test Config initialization with default log level."""
        config = Config()
        assert config.log_level == "INFO"

    @patch.dict(os.environ, {"LOG_LEVEL": "DEBUG"})
    def test_init_custom_log_level(self):
        """Test Config initialization with custom log level from environment."""
        config = Config()
        assert config.log_level == "DEBUG"

    @patch.dict(os.environ, {"LOG_LEVEL": "WARNING"})
    def test_init_different_log_level(self):
        """Test Config initialization with different log level."""
        config = Config()
        assert config.log_level == "WARNING"

    def test_get_instance_creates_singleton(self):
        """Test that get_instance returns the same instance."""
        instance1 = Config.get_instance()
        instance2 = Config.get_instance()
        assert instance1 is instance2
        assert isinstance(instance1, Config)

    def test_get_instance_has_correct_type(self):
        """Test that get_instance returns a Config instance."""
        instance = Config.get_instance()
        assert isinstance(instance, Config)
        assert hasattr(instance, "log_level")

    @patch.dict(os.environ, {"LOG_LEVEL": "ERROR"})
    def test_singleton_respects_environment(self):
        """Test that singleton instance respects environment variables."""
        # Clear any existing instance
        if hasattr(Config, "_instance"):
            delattr(Config, "_instance")

        instance = Config.get_instance()
        assert instance.log_level == "ERROR"
