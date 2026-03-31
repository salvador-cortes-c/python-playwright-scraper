#!/bin/bash
# Test if cookies work locally in headless mode

echo "🔍 Testing Cookies Locally (Headless Mode)"
echo "=========================================="

# Start display if needed
if ! pgrep -x Xvfb > /dev/null; then
    echo "Starting virtual display..."
    ./start_display.sh &
    DISPLAY_PID=$!
    sleep 3
fi

echo "Testing with current storage_state.json..."
echo "File info:"
ls -la storage_state.json
echo "Size: $(wc -c < storage_state.json) bytes"

echo ""
echo "🚀 Running scraper in headless mode (like GitHub Actions)..."
./run_scraper.sh \
    --url "https://www.newworld.co.nz/shop/category/fruit-and-vegetables?pg=1" \
    --headless-only \
    --storage-state storage_state.json \
    --limit 2 \
    --output test_result.json \
    --delay-seconds 2

if [ -f "test_result.json" ]; then
    PRODUCT_COUNT=$(jq '. | length' test_result.json 2>/dev/null || echo "0")
    echo ""
    echo "✅ SUCCESS! Cookies work in headless mode"
    echo "📦 Products scraped: $PRODUCT_COUNT"
    echo ""
    echo "📋 Next: The issue is with GitHub Actions environment"
    echo "   Need to check:"
    echo "   1. File paths in workflow"
    echo "   2. Permissions"
    echo "   3. Environment differences"
else
    echo ""
    echo "❌ FAILED! Cookies don't work in headless mode"
    echo ""
    echo "📋 Issue: Cloudflare detects headless vs headed browsers"
    echo "   Solutions:"
    echo "   1. Use different browser (Chromium instead of Firefox)"
    echo "   2. Try different user agent"
    echo "   3. Use scraping service (ScrapingBee free tier)"
    echo "   4. Generate cookies in headless mode (if possible)"
fi

# Clean up
rm -f test_result.json 2>/dev/null
if [ -n "$DISPLAY_PID" ]; then
    kill $DISPLAY_PID 2>/dev/null
fi