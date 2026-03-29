#!/bin/bash
# Generate Cloudflare cookies using virtual display

echo "🍪 Cloudflare Cookie Generation with Virtual Display"
echo "==================================================="

# Check if display is already running
if ! pgrep -x Xvfb > /dev/null; then
    echo "🖥️  Starting virtual display..."
    ./start_display.sh &
    DISPLAY_PID=$!
    sleep 3
    echo "✅ Virtual display started"
else
    echo "✅ Virtual display already running"
fi

echo ""
echo "📋 noVNC URL to view browser:"
echo "   http://localhost:6080/vnc.html"
echo ""
echo "👀 IMPORTANT:"
echo "   1. Open the URL above in your browser"
echo "   2. Click 'Connect'"
echo "   3. You'll see Firefox when the scraper runs"
echo "   4. Complete the Cloudflare challenge there"
echo ""

read -p "Press Enter when ready to generate cookies..."

echo ""
echo "🚀 Generating cookies..."
./run_scraper.sh \
    --url "https://www.newworld.co.nz/shop/category/fruit-and-vegetables?pg=1" \
    --headed \
    --manual-wait-seconds 120 \
    --storage-state storage_state.json \
    --limit 2

if [ -f "storage_state.json" ]; then
    echo ""
    echo "✅ SUCCESS! Cookies saved to storage_state.json"
    echo ""
    echo "📤 Upload to GitHub:"
    echo "   git add storage_state.json"
    echo "   git commit -m 'Add Cloudflare cookies'"
    echo "   git push origin main"
else
    echo ""
    echo "❌ Failed to generate cookies"
    echo "💡 Make sure you:"
    echo "   1. Opened the noVNC URL"
    echo "   2. Clicked 'Connect'"
    echo "   3. Completed the Cloudflare challenge"
fi

# Clean up if we started display
if [ -n "$DISPLAY_PID" ]; then
    echo ""
    echo "🧹 Stopping virtual display..."
    kill $DISPLAY_PID 2>/dev/null
fi