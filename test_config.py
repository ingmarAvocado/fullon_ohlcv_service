#!/usr/bin/env python3
"""Test the fixed database configuration loading."""

import asyncio
import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

async def test_with_demo_data():
    """Test configuration using demo data setup."""
    # Import after path setup
    from fullon_ohlcv_service.config.database_config import get_collection_targets
    from examples.demo_data import database_context_for_test, install_demo_data, generate_test_db_name

    # Set admin email
    os.environ['ADMIN_MAIL'] = 'admin@fullon'

    # Create test database with demo data
    test_db_name = generate_test_db_name()
    print(f"🔧 Creating test database: {test_db_name}")

    async with database_context_for_test(test_db_name):
        # Install demo data
        await install_demo_data()
        print("✅ Demo data installed")

        # Test the fixed configuration loading
        print("🔍 Testing configuration loading...")
        try:
            targets = await get_collection_targets(user_id=1)
            print(f"\n📊 Configuration Results:")
            print(f"   Found {len(targets)} exchanges")

            for exchange, info in targets.items():
                print(f"\n🏪 {exchange}:")
                print(f"   📈 Symbols ({len(info['symbols'])}): {info['symbols']}")
                print(f"   🆔 Exchange ID: {info['ex_id']}")

            if len(targets) == 3:
                expected_symbols = 5
                actual_symbols = [len(info['symbols']) for info in targets.values()]
                if all(count == expected_symbols for count in actual_symbols):
                    print(f"\n✅ SUCCESS: All 3 exchanges found with {expected_symbols} symbols each!")
                    return True
                else:
                    print(f"\n❌ FAILED: Expected {expected_symbols} symbols per exchange, got: {actual_symbols}")
            else:
                print(f"\n❌ FAILED: Expected 3 exchanges, got {len(targets)}")

        except Exception as e:
            print(f"\n❌ ERROR: {e}")
            import traceback
            traceback.print_exc()

    return False

if __name__ == "__main__":
    success = asyncio.run(test_with_demo_data())
    sys.exit(0 if success else 1)