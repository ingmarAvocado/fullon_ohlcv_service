#!/usr/bin/env python3
"""
Run All Examples Script for fullon_ohlcv_service

This is the main validation script that:
1. Creates isolated TimescaleDB test database with demo trade data
2. Runs all examples (or specific example) against real trade data
3. Examples demonstrate trade-to-candle conversion (the core OHLCV service functionality)
4. Cleans up test database and reports success/failure

This script is the PRIMARY completion criteria for GitHub issues.
When this script passes, the feature is complete and ready for production.

Usage:
    ./run_all_examples.py                                    # Run all examples with auto cleanup
    ./run_all_examples.py --example simple_daemon_control.py # Run specific example only
    ./run_all_examples.py --list                             # List available examples
    ./run_all_examples.py --keep-db                          # Keep test database after run
    ./run_all_examples.py --verbose                          # Show detailed output
    ./run_all_examples.py -e data_retrieval.py -v           # Run specific example with verbose output
"""

import asyncio
import argparse
import sys
import os
from pathlib import Path

# Add examples directory to path so we can import demo_data
examples_dir = Path(__file__).parent / "examples"
sys.path.insert(0, str(examples_dir))

from demo_data import (
    test_database_context, generate_test_db_name, install_demo_data,
    print_header, print_success, print_error, print_info, print_warning,
    Colors
)


async def run_example(example_path: Path, verbose: bool = False) -> bool:
    """Run a single example script and return success status"""
    if not example_path.exists():
        print_warning(f"Example not found: {example_path.name}")
        return False
    
    print_info(f"Running: {example_path.name}")
    
    try:
        proc = await asyncio.create_subprocess_exec(
            sys.executable, str(example_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=str(example_path.parent),
            env=dict(os.environ)  # Pass current environment including test database
        )
        
        stdout, stderr = await proc.communicate()
        
        if proc.returncode == 0:
            print_success(f"✓ {example_path.name} passed")
            if verbose and stdout:
                print(f"Output:\n{stdout.decode()}")
            return True
        else:
            print_error(f"✗ {example_path.name} failed (exit code: {proc.returncode})")
            if stderr:
                print(f"Error output:\n{stderr.decode()}")
            if verbose and stdout:
                print(f"Stdout:\n{stdout.decode()}")
            return False
            
    except Exception as e:
        print_error(f"✗ {example_path.name} failed with exception: {e}")
        return False


async def run_all_examples(verbose: bool = False, specific_example: str = None) -> tuple[int, int]:
    """Run all examples (or specific example) and return (passed, total) counts"""
    if specific_example:
        print_header(f"RUNNING SPECIFIC EXAMPLE: {specific_example}")
    else:
        print_header("RUNNING ALL EXAMPLES")
    
    # Define all available examples in order
    all_examples = [
        "simple_daemon_control.py",
        "data_retrieval.py", 
        "websocket_callbacks.py"
    ]
    
    # Filter to specific example if requested
    if specific_example:
        if specific_example not in all_examples:
            print_error(f"Example '{specific_example}' not found!")
            print_info(f"Available examples: {', '.join(all_examples)}")
            return 0, 1
        examples_to_run = [specific_example]
    else:
        examples_to_run = all_examples
    
    results = []
    examples_dir_path = Path(__file__).parent / "examples"
    
    for example_name in examples_to_run:
        example_path = examples_dir_path / example_name
        success = await run_example(example_path, verbose)
        results.append((example_name, success))
    
    # Summary
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    print_info(f"\nResults Summary:")
    for example_name, success in results:
        status = f"{Colors.GREEN}✓{Colors.END}" if success else f"{Colors.RED}✗{Colors.END}"
        print(f"  {status} {example_name}")
    
    return passed, total


async def main():
    """Main function that sets up test environment and runs examples"""
    parser = argparse.ArgumentParser(
        description="Run all fullon_ohlcv_service examples against TimescaleDB test database",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--keep-db', action='store_true',
                        help='Keep test database after running (for debugging)')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Show detailed output from examples')
    parser.add_argument('--db-name', 
                        help='Use specific test database name instead of generating random one')
    parser.add_argument('--example', '-e',
                        help='Run only specific example (e.g., simple_daemon_control.py)')
    parser.add_argument('--list', '-l', action='store_true',
                        help='List available examples')
    
    args = parser.parse_args()
    
    # Handle --list option
    if args.list:
        print_header("AVAILABLE EXAMPLES")
        examples = [
            "simple_daemon_control.py",
            "data_retrieval.py", 
            "websocket_callbacks.py"
        ]
        for i, example in enumerate(examples, 1):
            print_info(f"  {i}. {example}")
            
        print_info(f"\nUsage examples:")
        print_info(f"  python {sys.argv[0]} --example simple_daemon_control.py")
        print_info(f"  python {sys.argv[0]} -e data_retrieval.py -v")
        print_info(f"  python {sys.argv[0]} --keep-db --verbose")
        return 0
    
    # Generate or use provided database name
    test_db_name = args.db_name or generate_test_db_name()
    
    print_header("FULLON OHLCV SERVICE - EXAMPLES VALIDATION")
    print_info(f"Test database: {test_db_name}")
    if args.example:
        print_info(f"Running example: {args.example}")
    else:
        print_info("Running: ALL examples")
    print_info(f"Keep database: {'Yes' if args.keep_db else 'No'}")
    print_info(f"Verbose output: {'Yes' if args.verbose else 'No'}")
    
    try:
        if args.keep_db and args.db_name:
            # Use existing database, don't create/drop
            print_info("Using existing test database...")
            await install_demo_data()
            passed, total = await run_all_examples(args.verbose, args.example)
        else:
            # Use context manager for automatic cleanup
            async with test_database_context(test_db_name):
                await install_demo_data()
                passed, total = await run_all_examples(args.verbose, args.example)
                
                if args.keep_db:
                    print_warning(f"Test database preserved: {test_db_name}")
                    print_info(f"To cleanup later: python examples/demo_data.py --cleanup {test_db_name}")
        
        # Final results
        print_header("FINAL RESULTS")
        
        if passed == total:
            print_success(f"🎉 ALL EXAMPLES PASSED! ({passed}/{total})")
            print_info("✅ fullon_ohlcv_service examples are working correctly")
            print_info("✅ Integration with fullon ecosystem validated")
            print_info("✅ TimescaleDB trade data collection working")
            print_info("✅ Trade-to-candle conversion validated")
            print_info("✅ WebSocket callbacks and real-time processing validated")
            print_info("✅ Ready for production use")
            return 0
        else:
            print_error(f"❌ SOME EXAMPLES FAILED ({passed}/{total})")
            print_warning("⚠️ Issues need to be fixed before production")
            failed = total - passed
            print_info(f"📋 {failed} example(s) need attention")
            
            # Show specific failures
            print_info("\n🔍 Debugging tips:")
            print_info("  • Run with --verbose to see detailed output")
            print_info("  • Run specific failing example: --example <name>")
            print_info("  • Keep test database for inspection: --keep-db")
            print_info("  • Check TimescaleDB and Redis are running")
            print_info("  • Verify fullon ecosystem libraries are installed")
            
            return 1
            
    except Exception as e:
        print_error(f"❌ EXAMPLES RUN FAILED: {e}")
        print_warning("⚠️ Check environment setup and dependencies")
        
        # Show common troubleshooting tips
        print_info("\n🔧 Environment checklist:")
        print_info("  • TimescaleDB running: sudo systemctl status postgresql")
        print_info("  • Redis running: sudo systemctl status redis")
        print_info("  • Environment variables set (check .env)")
        print_info("  • fullon libraries installed: poetry install")
        print_info("  • Database permissions correct")
        
        return 1


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print_warning("\n⚠️ Examples run interrupted by user")
        sys.exit(1)
    except Exception as e:
        print_error(f"\n❌ Unexpected error: {e}")
        sys.exit(1)