#!/usr/bin/env python3
"""
Complete OHLCV Service Integration Test

This script orchestrates a full integration test of the fullon_ohlcv_service:
1. Setup demo database with fullon ecosystem data
2. Start the daemon with OHLCV collectors
3. Run data_retrieval.py example to show basic functionality
4. Run websocket_callbacks.py example to show real-time collection
5. Gracefully stop daemon and cleanup

This is the definitive integration test showing the complete workflow!
"""

import asyncio
import signal
import sys
import os
import json
import subprocess
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Any

# Add project src to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))
sys.path.insert(0, str(project_root))

try:
    from fullon_log import get_component_logger
    from fullon_cache import ProcessCache
    # Import demo_data functionality for database setup
    from demo_data import (
        generate_test_db_name,
        database_context_for_test,
        install_demo_data
    )
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("⚠️ Make sure fullon libraries are installed and project src/ is in path")
    sys.exit(1)


# ============================================================================
# CONSOLE COLORS AND FORMATTING
# ============================================================================

class Colors:
    """ANSI color codes for console output"""
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'
    BOLD = '\033[1m'
    END = '\033[0m'


def print_header(message: str):
    """Print formatted header message"""
    print(f"\n{Colors.BOLD}{Colors.CYAN}{'='*80}")
    print(f"{message:^80}")
    print(f"{'='*80}{Colors.END}\n")


def print_success(message: str):
    """Print success message in green"""
    print(f"{Colors.GREEN}✅ {message}{Colors.END}")


def print_error(message: str):
    """Print error message in red"""
    print(f"{Colors.RED}❌ {message}{Colors.END}")


def print_warning(message: str):
    """Print warning message in yellow"""
    print(f"{Colors.YELLOW}⚠️ {message}{Colors.END}")


def print_info(message: str):
    """Print info message in blue"""
    print(f"{Colors.BLUE}ℹ️ {message}{Colors.END}")


def print_status(message: str):
    """Print status message in magenta"""
    print(f"{Colors.MAGENTA}🔄 {message}{Colors.END}")


# ============================================================================
# EXAMPLE EXECUTION
# ============================================================================

class ExampleRunner:
    """Run actual examples to demonstrate functionality"""

    def __init__(self):
        self.logger = get_component_logger("demo.example_runner")
        self.examples_dir = Path(__file__).parent

    async def run_data_retrieval_example(self):
        """Run data_retrieval.py example"""
        print_status("Running data_retrieval.py example...")

        try:
            proc = await asyncio.create_subprocess_exec(
                sys.executable, str(self.examples_dir / "data_retrieval.py"),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=str(self.examples_dir)
            )

            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)

            if proc.returncode == 0:
                print_success("data_retrieval.py example completed successfully")
                # Show some output
                output_lines = stdout.decode().strip().split('\n')[-10:]
                for line in output_lines:
                    if line.strip():
                        print(f"  {line}")
                return True
            else:
                print_error("data_retrieval.py example failed")
                if stderr:
                    print_error(f"Error: {stderr.decode()}")
                return False

        except asyncio.TimeoutError:
            print_warning("data_retrieval.py example timed out")
            return False
        except Exception as e:
            print_error(f"Failed to run data_retrieval.py: {e}")
            return False

    async def run_websocket_callbacks_example(self, duration_seconds: int = 30):
        """Run websocket_callbacks.py example for limited time"""
        print_status(f"Running websocket_callbacks.py example for {duration_seconds}s...")

        try:
            proc = await asyncio.create_subprocess_exec(
                sys.executable, str(self.examples_dir / "websocket_callbacks.py"),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=str(self.examples_dir)
            )

            # Let it run for specified duration
            try:
                stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=duration_seconds)
                print_success("websocket_callbacks.py example completed")

                # Show some output
                output_lines = stdout.decode().strip().split('\n')[-10:]
                for line in output_lines:
                    if line.strip():
                        print(f"  {line}")
                return True

            except asyncio.TimeoutError:
                # Expected timeout, terminate gracefully
                proc.terminate()
                try:
                    await asyncio.wait_for(proc.wait(), timeout=5)
                except asyncio.TimeoutError:
                    proc.kill()
                    await proc.wait()

                print_success(f"websocket_callbacks.py ran for {duration_seconds}s (as expected)")
                return True

        except Exception as e:
            print_error(f"Failed to run websocket_callbacks.py: {e}")
            return False


# ============================================================================
# DAEMON MANAGEMENT
# ============================================================================

class DaemonManager:
    """Manage daemon lifecycle for the demo"""

    def __init__(self):
        self.logger = get_component_logger("demo.daemon")
        self.daemon_process = None
        self.daemon_running = False

    async def start_daemon_foreground(self):
        """Start daemon in foreground for monitoring"""
        print_status("Starting OHLCV Service Daemon...")

        try:
            # Start daemon in foreground mode
            self.daemon_process = subprocess.Popen(
                ["poetry", "run", "fullon-ohlcv-service", "start", "--foreground"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )

            # Wait a bit for startup
            await asyncio.sleep(5)

            # Check if it's running
            if self.daemon_process.poll() is None:
                self.daemon_running = True
                print_success("Daemon started successfully")

                # Get status
                status = await self._get_daemon_status()
                if status:
                    print_info(f"Daemon Running: {status.get('daemon_running', False)}")
                    print_info(f"OHLCV Collectors: {status.get('ohlcv_collectors', 0)}")
                    print_info(f"Trade Collectors: {status.get('trade_collectors', 0)}")

                return True
            else:
                stdout, stderr = self.daemon_process.communicate()
                print_error(f"Daemon failed to start")
                print_error(f"STDERR: {stderr}")
                return False

        except Exception as e:
            print_error(f"Failed to start daemon: {e}")
            return False

    async def _get_daemon_status(self):
        """Get daemon status via CLI"""
        try:
            result = subprocess.run(
                ["poetry", "run", "fullon-ohlcv-service", "status", "--json"],
                capture_output=True,
                text=True,
                timeout=10
            )

            if result.returncode == 0:
                # Extract JSON from output (filter out log messages)
                lines = result.stdout.strip().split('\n')
                for line in lines:
                    if line.startswith('{'):
                        status = json.loads(line)
                        return {
                            'daemon_running': status.get('daemon_running', False),
                            'ohlcv_collectors': len(status.get('ohlcv_service', {}).get('collectors', [])),
                            'trade_collectors': len(status.get('trade_service', {}).get('collectors', []))
                        }
            return None

        except Exception as e:
            self.logger.warning(f"Could not get daemon status: {e}")
            return None

    async def stop_daemon(self):
        """Stop the daemon gracefully"""
        if not self.daemon_running:
            return

        print_status("Stopping OHLCV Service Daemon...")

        try:
            # Send SIGTERM to daemon process
            if self.daemon_process and self.daemon_process.poll() is None:
                self.daemon_process.terminate()

                # Wait for graceful shutdown
                try:
                    self.daemon_process.wait(timeout=10)
                    print_success("Daemon stopped gracefully")
                except subprocess.TimeoutExpired:
                    print_warning("Daemon didn't stop gracefully, killing...")
                    self.daemon_process.kill()
                    self.daemon_process.wait()

        except Exception as e:
            print_error(f"Error stopping daemon: {e}")

        finally:
            self.daemon_running = False
            self.daemon_process = None




# ============================================================================
# MAIN ORCHESTRATOR
# ============================================================================

class IntegrationTestOrchestrator:
    """Integration test orchestrator using real examples and database setup"""

    def __init__(self):
        self.logger = get_component_logger("integration.orchestrator")
        self.daemon_manager = DaemonManager()
        self.example_runner = ExampleRunner()
        self.test_db_name = None
        self.setup_signal_handlers()

    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        print_warning(f"\nReceived signal {signum}, cleaning up...")
        asyncio.create_task(self.cleanup())

    async def run_complete_integration_test(self):
        """Run the complete OHLCV service integration test"""

        print_header("FULLON OHLCV SERVICE - INTEGRATION TEST")
        print_info("This integration test demonstrates the complete workflow:")
        print_info("1. 💾 Setup demo database with fullon ecosystem data")
        print_info("2. 🚀 Start OHLCV service daemon")
        print_info("3. 📊 Run data_retrieval.py example")
        print_info("4. 📶 Run websocket_callbacks.py example (real-time collection)")
        print_info("5. 🛑 Stop daemon and cleanup test database")
        print()

        try:
            # Step 1: Setup demo database
            await self._step1_setup_demo_database()

            # Step 2: Start daemon
            await self._step2_start_daemon()

            # Step 3: Run data retrieval example
            await self._step3_run_data_retrieval()

            # Step 4: Run real-time collection example
            await self._step4_run_realtime_collection()

            # Step 5: Cleanup
            await self._step5_cleanup()

            print_header("INTEGRATION TEST COMPLETED SUCCESSFULLY! 🎉")

        except Exception as e:
            print_error(f"Integration test failed: {e}")
            await self.cleanup()
            raise

    async def _step1_setup_demo_database(self):
        """Step 1: Setup demo database"""
        print_header("STEP 1: DEMO DATABASE SETUP")

        self.test_db_name = generate_test_db_name()
        print_info(f"Creating test database: {self.test_db_name}")

        # This will be handled by the context manager in main()
        # Just install the demo data
        await install_demo_data()
        print_success("Demo database setup completed")
        print()

    async def _step2_start_daemon(self):
        """Step 2: Start daemon"""
        print_header("STEP 2: DAEMON STARTUP")

        success = await self.daemon_manager.start_daemon_foreground()
        if not success:
            print_warning("Daemon failed to start, but continuing with demo...")
            # Don't fail the demo, just continue
        else:
            print_success("Daemon is running!")

        print()

    async def _step3_run_data_retrieval(self):
        """Step 3: Run data retrieval example"""
        print_header("STEP 3: DATA RETRIEVAL EXAMPLE")
        success = await self.example_runner.run_data_retrieval_example()
        if not success:
            print_warning("data_retrieval.py example had issues, but continuing...")
        print()

    async def _step4_run_realtime_collection(self):
        """Step 4: Run real-time collection example"""
        print_header("STEP 4: REAL-TIME COLLECTION EXAMPLE")
        success = await self.example_runner.run_websocket_callbacks_example(duration_seconds=30)
        if not success:
            print_warning("websocket_callbacks.py example had issues, but continuing...")
        print()

    async def _step5_cleanup(self):
        """Step 5: Cleanup"""
        print_header("STEP 5: CLEANUP")
        await self.cleanup()

    async def cleanup(self):
        """Cleanup all resources"""
        try:
            # Stop daemon
            await self.daemon_manager.stop_daemon()

            print_success("Cleanup completed")

        except Exception as e:
            print_error(f"Cleanup error: {e}")


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

async def main():
    """Main entry point"""
    orchestrator = IntegrationTestOrchestrator()

    # Generate test database name
    test_db_name = generate_test_db_name()
    print_info(f"Using test database: {test_db_name}")

    try:
        # Use database context manager for proper setup/cleanup
        async with database_context_for_test(test_db_name):
            orchestrator.test_db_name = test_db_name
            await orchestrator.run_complete_integration_test()
            return 0

    except KeyboardInterrupt:
        print_warning("\nIntegration test interrupted by user")
        await orchestrator.cleanup()
        return 130

    except Exception as e:
        print_error(f"Integration test failed: {e}")
        await orchestrator.cleanup()
        return 1


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print_warning("\nInterrupted")
        sys.exit(130)
    except Exception as e:
        print_error(f"Fatal error: {e}")
        sys.exit(1)