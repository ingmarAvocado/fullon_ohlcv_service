"""Simple configuration for OHLCV service"""

import os
from dataclasses import dataclass


@dataclass
class OhlcvServiceConfig:
    """Simple configuration for OHLCV service"""

    user_id: int = 1
    collection_interval: int = 60
    health_update_interval: int = 30
    historical_limit: int = 1000
    enable_streaming: bool = True
    enable_historical: bool = True
    test_mode: bool = False

    @classmethod
    def from_env(cls) -> 'OhlcvServiceConfig':
        """Load configuration from environment variables"""
        return cls(
            user_id=int(os.getenv('OHLCV_USER_ID', '1')),
            collection_interval=int(os.getenv('OHLCV_COLLECTION_INTERVAL', '60')),
            health_update_interval=int(os.getenv('OHLCV_HEALTH_INTERVAL', '30')),
            historical_limit=int(os.getenv('OHLCV_HISTORICAL_LIMIT', '1000')),
            enable_streaming=os.getenv('OHLCV_ENABLE_STREAMING', 'true').lower() == 'true',
            enable_historical=os.getenv('OHLCV_ENABLE_HISTORICAL', 'true').lower() == 'true',
            test_mode=os.getenv('OHLCV_TEST_MODE', 'false').lower() == 'true'
        )


# Global config instance
config = OhlcvServiceConfig.from_env()
