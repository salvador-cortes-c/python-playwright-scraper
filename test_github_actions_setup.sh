#!/bin/bash
# Test script to verify GitHub Actions setup locally

if [ -n "$PYTHON_BIN" ]; then
  PYTHON_CMD="$PYTHON_BIN"
elif [ -x ".venv/bin/python" ]; then
  PYTHON_CMD=".venv/bin/python"
elif [ -x "/workspaces/.venv/bin/python" ]; then
  PYTHON_CMD="/workspaces/.venv/bin/python"
else
  PYTHON_CMD="python3"
fi

echo "Testing GitHub Actions setup for Playwright Scraper"
echo "=================================================="

# Check if required files exist
echo "1. Checking required files..."
REQUIRED_FILES=(
  ".github/workflows/scrape_scrapingbee.yml"
  ".github/workflows/scrape-test.yml"
  "scraper_scrapingbee_newworld.py"
  "requirements.txt"
)

for file in "${REQUIRED_FILES[@]}"; do
  if [ -f "$file" ]; then
    echo "  ✅ $file"
  else
    echo "  ❌ $file (MISSING)"
  fi
done

echo ""
echo "2. Checking Python dependencies..."
if command -v "$PYTHON_CMD" &> /dev/null; then
  echo "  ✅ Python is installed: $PYTHON_CMD"
  "$PYTHON_CMD" --version
else
  echo "  ❌ Python is not installed"
fi

echo ""
echo "3. Testing workflow syntax..."
if command -v yamllint &> /dev/null; then
  yamllint .github/workflows/scrape_scrapingbee.yml .github/workflows/scrape-test.yml
else
  echo "  ⚠️  yamllint not installed, skipping syntax check"
fi

echo ""
echo "4. Checking for common issues..."
echo "  - ScrapingBee secret: SCRAPINGBEE_API_KEY must be configured"
echo "  - ScrapingBee workflow timeout: 30 minutes"
echo "  - Playwright smoke test requires Firefox system packages"
echo "  - Artifact upload: products.json, price_snapshots.json"

echo ""
echo "5. Quick test of scraper help..."
if [ -f "scraper_scrapingbee_newworld.py" ]; then
  "$PYTHON_CMD" scraper_scrapingbee_newworld.py --help | head -20
  echo "..."
else
  echo "  ❌ scraper_scrapingbee_newworld.py not found"
fi

echo ""
echo "=================================================="
echo "Setup Summary:"
echo ""
echo "To use GitHub Actions:"
echo "1. Push this repository to GitHub"
echo "2. Go to Actions tab in your repository"
echo "3. The workflows will be automatically detected"
echo "4. Manual trigger: Click 'Run workflow'"
echo "5. Scheduled runs: Daily at 2 AM UTC"
echo ""
echo "For testing:"
echo "- Use 'scrape_scrapingbee.yml' for ScrapingBee validation"
echo "- Use 'scrape-test.yml' for Playwright smoke testing"
echo "- Check workflow logs for any errors"
echo "- Download artifacts to verify output"
echo ""
echo "Note: GitHub Actions has 6-hour timeout limit"
echo "      Adjust scraping limits if hitting timeout"