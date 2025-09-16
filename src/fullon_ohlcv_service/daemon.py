#!/usr/bin/env python3
"""
OHLCV Service Daemon

Main entry point for the fullon_ohlcv_service CLI daemon.
Coordinates OHLCV and Trade collection services using the fullon ecosystem.
"""

import asyncio
import argparse
import signal
import sys
import os
import json
from pathlib import Path
from typing import Optional

from fullon_log import get_component_logger
from fullon_cache import ProcessCache
from fullon_ohlcv_service.ohlcv.manager import OhlcvManager
from fullon_ohlcv_service.trade.manager import TradeManager
from fullon_ohlcv_service.config.settings import OhlcvServiceConfig


class OhlcvServiceDaemon:
    """Main daemon coordinator for OHLCV and Trade services"""

    def __init__(self):
        self.logger = get_component_logger("fullon.ohlcv.daemon")
        self.config = OhlcvServiceConfig.from_env()
        self.ohlcv_manager: Optional[OhlcvManager] = None
        self.trade_manager: Optional[TradeManager] = None
        self.running = False
        self.pid_file = Path("/tmp/fullon_ohlcv_service.pid")

        # Setup signal handlers
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        self.logger.info(f"Received signal {signum}, initiating shutdown...")
        if self.running:
            asyncio.create_task(self.stop())

    async def start(self) -> bool:
        """Start the OHLCV service daemon"""
        if self.running:
            self.logger.warning("Daemon already running")
            return False

        try:
            self.logger.info("Starting OHLCV Service Daemon")

            # Create PID file
            self._create_pid_file()

            # Initialize managers
            self.ohlcv_manager = OhlcvManager(config=self.config)
            self.trade_manager = TradeManager()

            # Register main daemon process
            # await self._register_daemon()  # Temporarily disabled until ProcessCache API is clarified

            # Start services
            self.logger.info("Starting OHLCV Manager...")
            await self.ohlcv_manager.start()

            self.logger.info("Starting Trade Manager...")
            await self.trade_manager.start()

            self.running = True

            # Get status for startup summary
            ohlcv_status = await self.ohlcv_manager.status()
            trade_status = await self.trade_manager.get_status()

            self.logger.info("OHLCV Service Daemon started successfully",
                           ohlcv_collectors=ohlcv_status.get('collectors', 0),
                           trade_collectors=trade_status.get('collectors', 0))

            return True

        except Exception as e:
            self.logger.error(f"Failed to start daemon: {e}")
            await self.cleanup()
            return False

    async def stop(self) -> bool:
        """Stop the OHLCV service daemon"""
        if not self.running:
            self.logger.warning("Daemon not running")
            return False

        try:
            self.logger.info("Stopping OHLCV Service Daemon")
            self.running = False

            # Stop services
            if self.trade_manager:
                self.logger.info("Stopping Trade Manager...")
                await self.trade_manager.stop()

            if self.ohlcv_manager:
                self.logger.info("Stopping OHLCV Manager...")
                await self.ohlcv_manager.stop()

            # Cleanup
            await self.cleanup()

            self.logger.info("OHLCV Service Daemon stopped successfully")
            return True

        except Exception as e:
            self.logger.error(f"Error during daemon shutdown: {e}")
            return False

    async def status(self) -> dict:
        """Get daemon status"""
        status = {
            "daemon_running": self.running,
            "pid": os.getpid(),
            "config": {
                "user_id": self.config.user_id,
                "collection_interval": self.config.collection_interval,
                "health_update_interval": self.config.health_update_interval,
                "enable_streaming": self.config.enable_streaming,
                "enable_historical": self.config.enable_historical
            },
            "ohlcv_service": {},
            "trade_service": {}
        }

        if self.ohlcv_manager:
            status["ohlcv_service"] = await self.ohlcv_manager.status()

        if self.trade_manager:
            status["trade_service"] = await self.trade_manager.get_status()

        return status

    async def restart(self) -> bool:
        """Restart the daemon"""
        self.logger.info("Restarting OHLCV Service Daemon")
        await self.stop()
        await asyncio.sleep(2)  # Brief pause
        return await self.start()

    def _create_pid_file(self):
        """Create PID file"""
        try:
            with open(self.pid_file, 'w') as f:
                f.write(str(os.getpid()))
            self.logger.debug(f"Created PID file: {self.pid_file}")
        except Exception as e:
            self.logger.warning(f"Could not create PID file: {e}")

    def _remove_pid_file(self):
        """Remove PID file"""
        try:
            if self.pid_file.exists():
                self.pid_file.unlink()
                self.logger.debug(f"Removed PID file: {self.pid_file}")
        except Exception as e:
            self.logger.warning(f"Could not remove PID file: {e}")

    async def _register_daemon(self):
        """Register daemon in ProcessCache"""
        try:
            async with ProcessCache() as cache:
                admin_mail = os.getenv('ADMIN_MAIL', 'admin@fullon')

                # Try to register process - use available API
                await cache.register_process(
                    process_type="ohlcv_daemon",
                    component="fullon_ohlcv_service",
                    params={
                        "admin_mail": admin_mail,
                        "user_id": self.config.user_id,
                        "collection_interval": self.config.collection_interval,
                        "version": "1.0.0",
                        "pid": os.getpid()
                    },
                    message="Starting"
                )

                # Update to running status
                await cache.update_process("ohlcv_daemon", "fullon_ohlcv_service", "running")

        except Exception as e:
            self.logger.warning(f"Could not register daemon in ProcessCache: {e}")

    async def cleanup(self):
        """Cleanup daemon resources"""
        # Remove PID file
        self._remove_pid_file()

        # Clean up ProcessCache registration
        try:
            async with ProcessCache() as cache:
                await cache.stop_process("ohlcv_daemon", "fullon_ohlcv_service")
        except Exception as e:
            self.logger.warning(f"Could not clean up ProcessCache: {e}")


def create_parser() -> argparse.ArgumentParser:
    """Create CLI argument parser"""
    parser = argparse.ArgumentParser(
        description="fullon_ohlcv_service - OHLCV and Trade Data Collection Daemon",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  fullon-ohlcv-service start           # Start the daemon
  fullon-ohlcv-service stop            # Stop the daemon
  fullon-ohlcv-service status          # Show status
  fullon-ohlcv-service restart         # Restart the daemon
  fullon-ohlcv-service status --json   # Status in JSON format
        """
    )

    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # Start command
    start_parser = subparsers.add_parser('start', help='Start the daemon')
    start_parser.add_argument('--foreground', '-f', action='store_true',
                             help='Run in foreground (don\'t daemonize)')

    # Stop command
    subparsers.add_parser('stop', help='Stop the daemon')

    # Status command
    status_parser = subparsers.add_parser('status', help='Show daemon status')
    status_parser.add_argument('--json', action='store_true',
                              help='Output status in JSON format')

    # Restart command
    restart_parser = subparsers.add_parser('restart', help='Restart the daemon')
    restart_parser.add_argument('--foreground', '-f', action='store_true',
                               help='Run in foreground after restart')

    return parser


async def run_daemon_forever(daemon: OhlcvServiceDaemon):
    """Run daemon until shutdown signal"""
    try:
        while daemon.running:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        pass
    finally:
        await daemon.stop()


async def main_async():
    """Main async entry point"""
    parser = create_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    daemon = OhlcvServiceDaemon()

    try:
        if args.command == 'start':
            success = await daemon.start()
            if not success:
                return 1

            if getattr(args, 'foreground', False):
                print("Running in foreground... Press Ctrl+C to stop")
                await run_daemon_forever(daemon)
            else:
                print("Daemon started successfully")

        elif args.command == 'stop':
            success = await daemon.stop()
            return 0 if success else 1

        elif args.command == 'status':
            status = await daemon.status()

            if getattr(args, 'json', False):
                print(json.dumps(status, indent=2))
            else:
                print(f"Daemon Running: {status['daemon_running']}")
                print(f"PID: {status['pid']}")
                print(f"User ID: {status['config']['user_id']}")
                print(f"OHLCV Collectors: {status['ohlcv_service'].get('active_collectors', 0)}")
                print(f"Trade Collectors: {status['trade_service'].get('active_collectors', 0)}")

        elif args.command == 'restart':
            success = await daemon.restart()
            if not success:
                return 1

            if getattr(args, 'foreground', False):
                print("Running in foreground... Press Ctrl+C to stop")
                await run_daemon_forever(daemon)
            else:
                print("Daemon restarted successfully")

        return 0

    except KeyboardInterrupt:
        print("\nShutdown requested...")
        await daemon.stop()
        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 1


def main():
    """Main CLI entry point"""
    try:
        return asyncio.run(main_async())
    except KeyboardInterrupt:
        print("\nInterrupted")
        return 130
    except Exception as e:
        print(f"Fatal error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())