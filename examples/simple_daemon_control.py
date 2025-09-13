#!/usr/bin/env python3
"""
Example: Simple Daemon Control

Shows how to start/stop/status the OHLCV daemon from master_daemon.
Includes process registration in fullon_cache for monitoring.
"""

import asyncio
from fullon_log import get_component_logger
from fullon_ohlcv_service.ohlcv.manager import OhlcvManager
from fullon_cache import ProcessCache


async def main():
    """Simple daemon control example with process registration"""
    
    logger = get_component_logger("fullon.examples.daemon_control")
    
    logger.info("Starting OHLCV daemon control example")
    
    # Create daemon instance
    ohlcv_daemon = OhlcvManager()
    
    try:
        # Start the daemon
        logger.info("Starting OHLCV daemon")
        await ohlcv_daemon.start()
        
        # Register process in cache for monitoring
        async with ProcessCache() as cache:
            await cache.new_process(
                tipe="ohlcv_service",
                key="ohlcv_daemon", 
                pid=f"async:{id(ohlcv_daemon)}",
                params=["ohlcv_daemon"],
                message="Started"
            )
        
        logger.info("Process registered in cache", process_id=id(ohlcv_daemon))
        
        # Check status
        status = await ohlcv_daemon.status()
        logger.info("Daemon status retrieved", status=status)
        
        # Update process status
        async with ProcessCache() as cache:
            await cache.update_process(
                tipe="ohlcv_service",
                key="ohlcv_daemon",
                message="Running"
            )
        
        logger.info("Process status updated to Running")
        
        # Let it run for a bit
        logger.info("Letting daemon run for 5 seconds")
        await asyncio.sleep(5)
        
    except Exception as e:
        logger.error("Error during daemon operation", error=str(e))
        raise
    finally:
        # Stop the daemon
        logger.info("Stopping OHLCV daemon")
        await ohlcv_daemon.stop()
        
        # Clean up process from cache
        async with ProcessCache() as cache:
            await cache.delete_from_top(component="ohlcv_service:ohlcv_daemon")
            
        logger.info("Daemon stopped and process cleaned up")


if __name__ == "__main__":
    asyncio.run(main())