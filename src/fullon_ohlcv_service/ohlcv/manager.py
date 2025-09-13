"""OHLCV Manager - Multi-symbol OHLCV collection coordination."""

from fullon_log import get_component_logger


class OhlcvManager:
    """Simple OHLCV manager for coordinating multiple collectors."""

    def __init__(self):
        self.logger = get_component_logger("fullon.ohlcv.manager")
        self.collectors = {}
        self.running = False

    async def start(self):
        """Start the OHLCV manager."""
        self.running = True
        self.logger.info("OhlcvManager started")

    async def stop(self):
        """Stop the OHLCV manager."""
        self.running = False
        self.logger.info("OhlcvManager stopped")

    async def status(self):
        """Get the current status of the OHLCV manager."""
        return {
            "running": self.running,
            "collectors_count": len(self.collectors),
            "collectors": list(self.collectors.keys())
        }