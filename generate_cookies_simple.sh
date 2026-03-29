#!/bin/bash
# Simple cookie generation using system Python

echo "🍪 Simple Cookie Generation"
echo "==========================="

# Check if we can use system python3
if command -v python3 >/dev/null 2>&1; then
    PYTHON_CMD="python3"
elif command -v python >/dev/null 2>&1; then
    PYTHON_CMD="python"
else
    echo "❌ No Python found"
    exit 1
fi

echo "Python command: $PYTHON_CMD"
echo "Python version:"
$PYTHON_CMD --version

# Check Python version
VERSION=$($PYTHON_CMD -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
echo "Version: $VERSION"

# Check if Playwright is installed
if ! $PYTHON_CMD -c "import playwright" 2>/dev/null; then
    echo "Installing Playwright..."
    
    # Try different installation methods based on Python version
    if [[ $VERSION =~ ^3\.(8|9|10|11|12) ]]; then
        echo "Python $VERSION is compatible with Playwright"
        $PYTHON_CMD -m pip install playwright playwright-stealth
        $PYTHON_CMD -m playwright install firefox
    else
        echo "⚠️  Python $VERSION may not be fully compatible"
        echo "Trying to install anyway..."
        $PYTHON_CMD -m pip install playwright playwright-stealth || {
            echo "❌ Could not install Playwright"
            echo "Please install Python 3.8-3.12"
            exit 1
        }
        $PYTHON_CMD -m playwright install firefox
    fi
fi

echo ""
echo "🚀 Starting cookie generation..."
echo "📝 Complete the Cloudflare challenge in Firefox when it opens"
echo "⏰ Will wait 2 minutes for manual verification..."

$PYTHON_CMD scraper.py \
    --url "https://www.newworld.co.nz/shop/category/fruit-and-vegetables?pg=1" \
    --limit 3 \
    --headed \
    --manual-wait-seconds 120 \
    --output test_cookies.json \
    --storage-state storage_state.json \
    --delay-seconds 2

if [ -f "storage_state.json" ]; then
    echo ""
    echo "✅ SUCCESS! Cookies saved to storage_state.json"
    echo ""
    echo "📤 Upload to GitHub:"
    echo "   git add storage_state.json"
    echo "   git commit -m 'Add Cloudflare cookies'"
    echo "   git push origin main"
    echo ""
    echo "🎉 GitHub Actions will now run automatically!"
else
    echo ""
    echo "❌ Failed to generate cookies"
    echo ""
    echo "💡 Troubleshooting:"
    echo "   1. Make sure Firefox is installed"
    echo "   2. Complete the Cloudflare challenge"
    echo "   3. Wait for the full 2 minutes"
fi