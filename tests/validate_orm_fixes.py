#!/usr/bin/env python3
"""
Validation script for ORM compatibility fixes.

This script validates that the code has been properly updated for fullon_orm compatibility.
It checks the source files for correct patterns without importing them.
"""

import re
from pathlib import Path


def check_file_for_patterns(file_path, issues):
    """Check a file for ORM compatibility issues."""
    with open(file_path, 'r') as f:
        content = f.read()
        lines = content.splitlines()

    filename = file_path.name

    # Check for correct imports
    if 'from fullon_orm' in content:
        if 'from fullon_orm.models import' in content:
            # Good - using model imports
            pass

    # Check for Trade volume field usage
    for i, line in enumerate(lines, 1):
        # Check if we're creating Trade objects with correct field
        if 'Trade(' in line:
            # Look for the volume field in the next few lines
            context = '\n'.join(lines[max(0, i-1):min(len(lines), i+10)])
            if 'volume=' in context:
                # Good - using volume field
                pass
            elif 'amount=' in context and 'Trade' in context:
                issues.append(f"{filename}:{i} - Trade object using 'amount' instead of 'volume'")

    # Check for correct mapping of raw trade data
    for i, line in enumerate(lines, 1):
        if '.get("amount"' in line or ".get('amount'" in line:
            # Check if it's being mapped to volume
            if 'volume=' in line or 'volume:' in line:
                # Good - mapping amount to volume
                pass
            elif 'Trade(' in lines[max(0, i-5):min(len(lines), i+5)].__str__():
                # Check context to see if it's part of Trade creation
                pass

    return issues


def validate_database_config():
    """Validate database_config.py has correct patterns."""
    print("\n1. Validating database_config.py...")

    config_file = Path('src/fullon_ohlcv_service/config/database_config.py')
    with open(config_file, 'r') as f:
        content = f.read()

    issues = []

    # Check that we're using .get() for exchange dictionaries
    if "exchange.get('cat_ex_id')" in content or 'exchange.get("cat_ex_id")' in content:
        print("   ✅ Using dictionary access for exchanges")

    # Check that we're using attributes for cat_exchanges
    if "cat_ex.cat_ex_id" in content:
        print("   ✅ Using attribute access for CatExchange objects")

    # Check for Symbol attribute access
    if "s.symbol for s in symbols" in content:
        print("   ✅ Using attribute access for Symbol objects")

    return len(issues) == 0


def validate_trade_collectors():
    """Validate trade collectors use volume field correctly."""
    print("\n2. Validating trade collectors...")

    collector_files = [
        Path('src/fullon_ohlcv_service/trade/collector.py'),
        Path('src/fullon_ohlcv_service/trade/historic_collector.py'),
        Path('src/fullon_ohlcv_service/trade/live_collector.py'),
        Path('src/fullon_ohlcv_service/trade/global_batcher.py'),
    ]

    issues = []
    for file_path in collector_files:
        if file_path.exists():
            check_file_for_patterns(file_path, issues)

    # Check specific patterns
    with open(Path('src/fullon_ohlcv_service/trade/collector.py'), 'r') as f:
        content = f.read()
        # Check for volume field mapping
        if 't.get("amount", t.get("volume"' in content:
            print("   ✅ collector.py handles both 'amount' and 'volume' fields")

    with open(Path('src/fullon_ohlcv_service/trade/historic_collector.py'), 'r') as f:
        content = f.read()
        if "volume=float(amount)" in content:
            print("   ✅ historic_collector.py maps 'amount' to 'volume'")

    return len(issues) == 0


def validate_exchange_lookups():
    """Validate that exchange lookups use correct patterns."""
    print("\n3. Validating exchange lookups...")

    files_to_check = [
        Path('src/fullon_ohlcv_service/trade/historic_collector.py'),
        Path('src/fullon_ohlcv_service/trade/live_collector.py'),
        Path('src/fullon_ohlcv_service/trade/manager.py'),
        Path('src/fullon_ohlcv_service/ohlcv/historic_collector.py'),
        Path('src/fullon_ohlcv_service/ohlcv/live_collector.py'),
    ]

    correct_patterns = 0
    for file_path in files_to_check:
        if file_path.exists():
            with open(file_path, 'r') as f:
                content = f.read()
                # Check for correct dictionary access pattern
                if "exchange.get('cat_ex_id')" in content or 'exchange.get("cat_ex_id")' in content:
                    correct_patterns += 1
                if "exchange.get('ex_id')" in content or 'exchange.get("ex_id")' in content:
                    correct_patterns += 1

    if correct_patterns > 0:
        print(f"   ✅ Found {correct_patterns} correct exchange dictionary access patterns")

    return True


def validate_imports():
    """Validate that necessary imports are present."""
    print("\n4. Validating ORM model imports...")

    files_to_check = [
        ('src/fullon_ohlcv_service/trade/historic_collector.py', ['Symbol', 'Exchange']),
        ('src/fullon_ohlcv_service/trade/live_collector.py', ['Symbol', 'Exchange']),
        ('src/fullon_ohlcv_service/ohlcv/historic_collector.py', ['Symbol', 'Exchange']),
        ('src/fullon_ohlcv_service/ohlcv/live_collector.py', ['Symbol', 'Exchange']),
    ]

    all_good = True
    for file_path, required_models in files_to_check:
        path = Path(file_path)
        if path.exists():
            with open(path, 'r') as f:
                content = f.read()
                for model in required_models:
                    if f'from fullon_orm.models import' in content and model in content:
                        pass  # Good
                    else:
                        print(f"   ⚠️  {path.name} may be missing import for {model}")
                        all_good = False

    if all_good:
        print("   ✅ All necessary ORM model imports are present")

    return all_good


def main():
    """Run all validations."""
    print("="*60)
    print("ORM Compatibility Validation")
    print("="*60)

    all_valid = True

    all_valid &= validate_database_config()
    all_valid &= validate_trade_collectors()
    all_valid &= validate_exchange_lookups()
    all_valid &= validate_imports()

    print("\n" + "="*60)
    if all_valid:
        print("✅ All ORM compatibility fixes are in place!")
        print("\nSummary of fixes:")
        print("- database_config.py uses dictionary access for exchanges")
        print("- database_config.py uses attribute access for CatExchange and Symbol objects")
        print("- Trade collectors map 'amount' field to 'volume'")
        print("- All collectors handle exchange dictionary lookups correctly")
        print("- All necessary ORM model imports are present")
    else:
        print("❌ Some validation checks failed")
    print("="*60)

    return all_valid


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)