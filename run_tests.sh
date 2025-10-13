#!/bin/bash
# Simple wrapper to run tests with timeout protection
# This prevents hanging if pytest still encounters cleanup issues

echo "Running pytest with 3-minute timeout protection..."
timeout 180 poetry run pytest tests/unit/ -n auto "$@"

exit_code=$?

if [ $exit_code == 124 ]; then
    echo ""
    echo "ERROR: Tests timed out after 3 minutes"
    echo "This suggests pytest is still hanging during cleanup"
    exit 1
elif [ $exit_code == 0 ]; then
    echo ""
    echo "SUCCESS: All tests completed without hanging!"
    exit 0
else
    echo ""
    echo "Tests completed with exit code: $exit_code"
    exit $exit_code
fi
