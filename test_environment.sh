#!/bin/bash
# Test environment capabilities

echo "🔍 Testing Environment Capabilities"
echo "=================================="

echo "1. User: $(whoami)"
echo "2. Display: $DISPLAY"
echo "3. GUI available: $([ -n "$DISPLAY" ] && echo "Yes" || echo "No")"

echo ""
echo "4. Firefox available:"
which firefox 2>/dev/null && echo "   Yes: $(which firefox)" || echo "   No"

echo ""
echo "5. Python versions:"
for py in python python3 python3.12 python3.11 python3.10 python3.9 python3.8; do
    if command -v $py >/dev/null 2>&1; then
        echo "   $py: $($py --version 2>&1)"
    fi
done

echo ""
echo "6. Can run GUI apps? (testing with xeyes/xclock):"
if command -v xeyes >/dev/null 2>&1; then
    timeout 2 xeyes 2>&1 | head -5 && echo "   ✅ Can run GUI apps" || echo "   ❌ Cannot run GUI apps"
else
    echo "   xeyes not installed"
fi

echo ""
echo "7. Virtual display possible?"
if command -v Xvfb >/dev/null 2>&1; then
    echo "   ✅ Xvfb available: $(which Xvfb)"
else
    echo "   ❌ Xvfb not available"
fi

echo ""
echo "📋 Summary:"
if [ -n "$DISPLAY" ] || command -v Xvfb >/dev/null 2>&1; then
    echo "✅ Possible to run browser (with Xvfb)"
else
    echo "❌ Cannot run browser - no display available"
fi