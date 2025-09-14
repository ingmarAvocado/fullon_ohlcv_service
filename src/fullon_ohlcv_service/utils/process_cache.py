"""Mock ProcessCache implementation for development.

This is a placeholder until fullon_cache is available.
In production, replace with: from fullon_cache import ProcessCache
"""


class ProcessCache:
    """Mock ProcessCache for health monitoring."""

    async def __aenter__(self):
        """Enter async context."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit async context."""
        pass

    async def new_process(self, tipe: str, key: str, pid: str, params: list, message: str):
        """Register a new process in cache."""
        # In production, this would register with Redis
        pass

    async def update_process(self, tipe: str, key: str, message: str):
        """Update process status in cache."""
        # In production, this would update Redis
        pass

    async def delete_process(self, tipe: str, key: str):
        """Delete process from cache."""
        # In production, this would remove from Redis
        pass