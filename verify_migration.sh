#!/bin/bash
# Verification script for ProcessCache API migration

echo "=========================================="
echo "ProcessCache API Migration Verification"
echo "=========================================="
echo ""

# Check for old API calls in source code
echo "1. Checking for old API calls in source code..."
OLD_API_IN_SRC=$(grep -r "new_process\|delete_process" --include="*.py" src/ 2>/dev/null)
if [ -z "$OLD_API_IN_SRC" ]; then
    echo "   ✅ No old API calls found in source code"
else
    echo "   ❌ Found old API calls in source code:"
    echo "$OLD_API_IN_SRC"
    exit 1
fi

# Check for new API usage
echo ""
echo "2. Checking for new API usage..."
NEW_API_REGISTER=$(grep -r "register_process" --include="*.py" src/ 2>/dev/null)
NEW_API_STOP=$(grep -r "stop_process" --include="*.py" src/ 2>/dev/null)

if [ -n "$NEW_API_REGISTER" ]; then
    echo "   ✅ Found register_process() calls"
else
    echo "   ❌ No register_process() calls found"
    exit 1
fi

if [ -n "$NEW_API_STOP" ]; then
    echo "   ✅ Found stop_process() calls"
else
    echo "   ❌ No stop_process() calls found"
    exit 1
fi

# Check for ProcessType and ProcessStatus imports
echo ""
echo "3. Checking for required imports..."
IMPORTS=$(grep -r "from fullon_cache.process_cache import ProcessType, ProcessStatus" --include="*.py" src/ 2>/dev/null)
if [ -n "$IMPORTS" ]; then
    echo "   ✅ Found ProcessType and ProcessStatus imports"
else
    echo "   ❌ Missing ProcessType and ProcessStatus imports"
    exit 1
fi

# Check for process_id attribute
echo ""
echo "4. Checking for process_id attribute..."
PROCESS_ID=$(grep -r "self.process_id" --include="*.py" src/ 2>/dev/null)
if [ -n "$PROCESS_ID" ]; then
    echo "   ✅ Found process_id attribute usage"
else
    echo "   ❌ Missing process_id attribute"
    exit 1
fi

echo ""
echo "=========================================="
echo "✅ ALL CHECKS PASSED"
echo "=========================================="
echo ""
echo "Migration is complete! Next steps:"
echo "1. Set up your environment with required dependencies"
echo "2. Run: python examples/run_example_pipeline.py"
echo "3. Verify no AttributeError exceptions occur"
echo ""
