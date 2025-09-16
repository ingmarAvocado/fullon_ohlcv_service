"""Configuration management for fullon_ohlcv_service."""

import os

from dotenv import load_dotenv

load_dotenv()


class Config:
    """Simple configuration class for the OHLCV service."""

    def __init__(self):
        self.log_level = os.getenv("LOG_LEVEL", "INFO")

    @classmethod
    def get_instance(cls):
        """Get singleton instance of config."""
        if not hasattr(cls, "_instance"):
            cls._instance = cls()
        return cls._instance
