#!/bin/bash
# Script to refresh Cloudflare cookies for New World scraper
# Run this locally every 1-2 weeks to maintain session

set -e  # Exit on error

echo "🔄 New World Scraper - Cookie Refresh Tool"
echo "=========================================="
echo ""

# Check if virtual environment exists
if [ ! -f ".venv/bin/activate" ]; then
    echo "❌ Virtual environment not found. Creating..."
    python -m venv .venv
fi

# Activate virtual environment
echo "🐍 Activating virtual environment..."
source .venv/bin/activate

# Install/update dependencies
echo "📦 Installing/updating dependencies..."
pip install --upgrade pip
pip install -r requirements.txt
pip install playwright-stealth

# Install Playwright browsers if needed
echo "🌐 Checking Playwright installation..."
if ! python -m playwright --version 2>/dev/null; then
    echo "Installing Playwright browsers..."
    python -m playwright install firefox
fi

echo ""
echo "🚀 Starting New World scraper with manual Cloudflare verification..."
echo "📝 You will need to complete the Cloudflare challenge in the browser window"
echo "⏰ This may take 1-2 minutes for the challenge to appear"
echo ""

# Run scraper with manual verification
python scraper.py \
    --url "https://www.newworld.co.nz/shop/category/fruit-and-vegetables?pg=1" \
    --limit 5 \
    --headed \
    --manual-wait-seconds 120 \
    --output test_refresh.json \
    --storage-state storage_state.json \
    --delay-seconds 3

echo ""
echo "✅ Cookie refresh complete!"
echo ""
echo "📁 Updated files:"
echo "   - storage_state.json (Cloudflare session cookies)"
echo "   - test_refresh.json (test scrape results)"
echo ""
echo "📤 Next steps:"
echo "   1. Review test_refresh.json to ensure scraping worked"
echo "   2. Commit and push storage_state.json to GitHub:"
echo "      git add storage_state.json"
echo "      git commit -m 'Update Cloudflare session cookies'"
echo "      git push origin main"
echo "   3. GitHub Actions will use the updated cookies automatically"
echo ""
echo "⏰ Recommended refresh schedule: Every 1-2 weeks"
echo "   (Cloudflare sessions typically expire after 7-14 days)"