#!/bin/bash
# Fix Python 3.13 issue and generate cookies

set -e

echo "🔧 Fixing Python 3.13 Compatibility Issue"
echo "========================================="

# Step 1: Install Python 3.12
echo "1. Installing Python 3.12..."
apt update
apt install -y python3.12 python3.12-venv python3.12-dev

# Verify
if ! command -v python3.12 >/dev/null 2>&1; then
    echo "❌ Failed to install Python 3.12"
    exit 1
fi
echo "✅ Python 3.12 installed: $(python3.12 --version)"

# Step 2: Create new virtual environment
echo ""
echo "2. Creating new virtual environment..."
if [ -d ".venv" ]; then
    echo "Removing old .venv..."
    rm -rf .venv
fi

python3.12 -m venv .venv
source .venv/bin/activate
echo "✅ New venv created with Python $(python --version)"

# Step 3: Install dependencies
echo ""
echo "3. Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt
pip install playwright-stealth

# Step 4: Install Playwright browsers
echo ""
echo "4. Installing Playwright browsers..."
python -m playwright install firefox

# Step 5: Generate cookies
echo ""
echo "5. Generating Cloudflare cookies..."
echo "=================================="
echo "💡 A Firefox window will open"
echo "📝 Complete the 'I'm not a robot' challenge"
echo "⏰ Will wait 2 minutes..."

python scraper.py \
    --url "https://www.newworld.co.nz/shop/category/fruit-and-vegetables?pg=1" \
    --limit 3 \
    --headed \
    --manual-wait-seconds 120 \
    --output cookie_test.json \
    --storage-state storage_state.json \
    --delay-seconds 2

if [ -f "storage_state.json" ]; then
    echo ""
    echo "🎉 SUCCESS! Cookies generated!"
    echo ""
    echo "📁 Files created:"
    echo "   - storage_state.json (Cloudflare cookies)"
    echo "   - cookie_test.json (test results)"
    echo ""
    echo "📤 Upload to GitHub:"
    echo "   git add storage_state.json"
    echo "   git commit -m 'Add Cloudflare cookies'"
    echo "   git push origin main"
    echo ""
    echo "✅ GitHub Actions will now run automatically!"
else
    echo ""
    echo "❌ Cookie generation failed"
    echo "💡 Check if Firefox opened and you completed the challenge"
fi