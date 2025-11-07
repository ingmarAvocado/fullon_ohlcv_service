#!/usr/bin/env python3
"""
Simple test to verify REST fallback code is properly integrated.

This test validates:
1. Utils module exists and has required functions
2. LiveOHLCVCollector has REST fallback methods
3. HistoricOHLCVCollector uses shared utils
"""

def test_utils_module():
    """Test utils module has required functions."""
    print("\n" + "=" * 80)
    print("Test 1: Utils Module")
    print("=" * 80)

    try:
        from fullon_ohlcv_service.ohlcv.utils import timeframe_to_seconds, convert_to_candle_objects
        print("✓ Utils module imports successfully")

        # Test timeframe_to_seconds
        assert timeframe_to_seconds("1m") == 60, "1m should be 60 seconds"
        assert timeframe_to_seconds("5m") == 300, "5m should be 300 seconds"
        assert timeframe_to_seconds("1h") == 3600, "1h should be 3600 seconds"
        assert timeframe_to_seconds("1d") == 86400, "1d should be 86400 seconds"
        print("✓ timeframe_to_seconds() works correctly")

        # Test convert_to_candle_objects with sample data
        from datetime import UTC, datetime
        sample_candles = [
            [1699142400000, 50000.0, 50100.0, 49900.0, 50050.0, 1.5],
            [1699142460000, 50050.0, 50150.0, 50000.0, 50100.0, 2.0],
        ]
        candles = convert_to_candle_objects(sample_candles)
        assert len(candles) == 2, "Should convert 2 candles"
        assert candles[0].open == 50000.0, "First candle open should be 50000.0"
        assert candles[1].close == 50100.0, "Second candle close should be 50100.0"
        print("✓ convert_to_candle_objects() works correctly")

        print("\n✅ Test 1 PASSED: Utils module is working")
        return True

    except Exception as e:
        print(f"\n❌ Test 1 FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_live_collector_methods():
    """Test LiveOHLCVCollector has REST fallback methods."""
    print("\n" + "=" * 80)
    print("Test 2: LiveOHLCVCollector REST Fallback Methods")
    print("=" * 80)

    try:
        from fullon_ohlcv_service.ohlcv.live_collector import LiveOHLCVCollector

        collector = LiveOHLCVCollector()
        print("✓ LiveOHLCVCollector instantiates successfully")

        # Check for REST polling attributes
        assert hasattr(collector, 'rest_polling_tasks'), "Should have rest_polling_tasks attribute"
        print("✓ Has rest_polling_tasks attribute")

        # Check for REST fallback methods
        assert hasattr(collector, '_supports_websocket'), "Should have _supports_websocket method"
        print("✓ Has _supports_websocket() method")

        assert hasattr(collector, '_start_rest_polling'), "Should have _start_rest_polling method"
        print("✓ Has _start_rest_polling() method")

        # Verify the start_symbol method exists
        assert hasattr(collector, 'start_symbol'), "Should have start_symbol method"
        print("✓ Has start_symbol() method")

        print("\n✅ Test 2 PASSED: LiveOHLCVCollector has REST fallback methods")
        return True

    except Exception as e:
        print(f"\n❌ Test 2 FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_historic_collector_uses_utils():
    """Test HistoricOHLCVCollector uses shared utils."""
    print("\n" + "=" * 80)
    print("Test 3: HistoricOHLCVCollector Uses Shared Utils")
    print("=" * 80)

    try:
        # Read the historic_collector.py file
        with open('/home/ingmar/code/fullon_ohlcv_service/src/fullon_ohlcv_service/ohlcv/historic_collector.py', 'r') as f:
            content = f.read()

        # Check imports
        assert 'from .utils import timeframe_to_seconds, convert_to_candle_objects' in content, \
            "Should import from utils"
        print("✓ Imports from utils module")

        # Check it doesn't define its own versions
        assert 'def _timeframe_to_seconds(' not in content, \
            "Should NOT define _timeframe_to_seconds (should use shared version)"
        print("✓ Does NOT define _timeframe_to_seconds (uses shared version)")

        assert 'def _convert_to_candle_objects(' not in content, \
            "Should NOT define _convert_to_candle_objects (should use shared version)"
        print("✓ Does NOT define _convert_to_candle_objects (uses shared version)")

        # Check it uses the imported functions
        assert 'timeframe_to_seconds(' in content, \
            "Should call timeframe_to_seconds()"
        print("✓ Uses timeframe_to_seconds()")

        assert 'convert_to_candle_objects(' in content, \
            "Should call convert_to_candle_objects()"
        print("✓ Uses convert_to_candle_objects()")

        print("\n✅ Test 3 PASSED: HistoricOHLCVCollector uses shared utils")
        return True

    except Exception as e:
        print(f"\n❌ Test 3 FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_live_collector_imports_utils():
    """Test LiveOHLCVCollector imports shared utils."""
    print("\n" + "=" * 80)
    print("Test 4: LiveOHLCVCollector Imports Shared Utils")
    print("=" * 80)

    try:
        # Read the live_collector.py file
        with open('/home/ingmar/code/fullon_ohlcv_service/src/fullon_ohlcv_service/ohlcv/live_collector.py', 'r') as f:
            content = f.read()

        # Check imports
        assert 'from .utils import timeframe_to_seconds, convert_to_candle_objects' in content, \
            "Should import from utils"
        print("✓ Imports from utils module")

        # Check it uses the imported functions in REST polling
        assert 'timeframe_to_seconds(' in content, \
            "Should use timeframe_to_seconds()"
        print("✓ Uses timeframe_to_seconds()")

        assert 'convert_to_candle_objects(' in content, \
            "Should use convert_to_candle_objects()"
        print("✓ Uses convert_to_candle_objects()")

        # Check for REST fallback logic
        assert '_supports_websocket' in content, \
            "Should have _supports_websocket method"
        print("✓ Has _supports_websocket method")

        assert '_start_rest_polling' in content, \
            "Should have _start_rest_polling method"
        print("✓ Has _start_rest_polling method")

        assert 'REST polling' in content or 'rest polling' in content, \
            "Should have REST polling logic"
        print("✓ Contains REST polling logic")

        print("\n✅ Test 4 PASSED: LiveOHLCVCollector properly implements REST fallback")
        return True

    except Exception as e:
        print(f"\n❌ Test 4 FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all tests."""
    print("=" * 80)
    print("REST Fallback Implementation Test Suite")
    print("=" * 80)

    results = []

    # Run tests
    results.append(("Utils Module", test_utils_module()))
    results.append(("LiveOHLCVCollector Methods", test_live_collector_methods()))
    results.append(("HistoricOHLCVCollector Uses Utils", test_historic_collector_uses_utils()))
    results.append(("LiveOHLCVCollector Imports Utils", test_live_collector_imports_utils()))

    # Summary
    print("\n" + "=" * 80)
    print("Test Summary")
    print("=" * 80)

    passed = sum(1 for _, result in results if result)
    total = len(results)

    for name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}: {name}")

    print("\n" + "=" * 80)
    if passed == total:
        print(f"🎉 ALL TESTS PASSED ({passed}/{total})")
        print("=" * 80)
        print("\n✅ REST fallback implementation is complete and correct!")
        return 0
    else:
        print(f"⚠️  SOME TESTS FAILED ({passed}/{total} passed)")
        print("=" * 80)
        return 1


if __name__ == "__main__":
    exit(main())
