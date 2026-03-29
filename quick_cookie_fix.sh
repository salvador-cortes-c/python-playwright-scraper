#!/bin/bash
# Quick fix for cookie generation - uses system Python

echo "🚀 Quick Cookie Generation Fix"
echo "=============================="

# Deactivate any venv
deactivate 2>/dev/null || true

# Use system Python (outside venv)
# Force use of /usr/bin/python3 to avoid venv
if [ -f "/usr/bin/python3" ]; then
    SYSTEM_PYTHON="/usr/bin/python3"
elif [ -f "/usr/local/bin/python3" ]; then
    SYSTEM_PYTHON="/usr/local/bin/python3"
else
    SYSTEM_PYTHON=$(which python3)
fi

echo "Using: $SYSTEM_PYTHON"
$SYSTEM_PYTHON --version

# Install Playwright system-wide if needed
if ! $SYSTEM_PYTHON -c "import playwright" 2>/dev/null; then
    echo "Installing Playwright system-wide..."
    $SYSTEM_PYTHON -m pip install playwright playwright-stealth
    $SYSTEM_PYTHON -m playwright install firefox
fi

echo ""
echo "🔑 Generating Cloudflare cookies..."
echo "💡 A Firefox window will open - complete the 'I'm not a robot' challenge"
echo "⏳ This will take about 2 minutes..."

# Run the scraper directly with system Python
$SYSTEM_PYTHON scraper.py \
    --url "https://www.newworld.co.nz/shop/category/fruit-and-vegetables?pg=1" \
    --limit 3 \
    --headed \
    --manual-wait-seconds 120 \
    --output cookie_test.json \
    --storage-state storage_state.json \
    --delay-seconds 2

if [ -f "storage_state.json" ]; then
    echo ""
    echo "✅ SUCCESS! Cookies generated: storage_state.json"
    echo ""
    echo "📤 Next steps:"
    echo "   git add storage_state.json"
    echo "   git commit -m 'Cloudflare cookies'"
    echo "   git push origin main"
    echo ""
    echo "🎉 GitHub Actions will now work automatically!"
else
    echo ""
    echo "❌ Failed to generate cookies"
    echo "💡 Try running with: python3 scraper.py ..."
fi