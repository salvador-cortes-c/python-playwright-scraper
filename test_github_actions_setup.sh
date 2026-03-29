#!/bin/bash
# Test script to verify GitHub Actions setup locally

echo "Testing GitHub Actions setup for Playwright Scraper"
echo "=================================================="

# Check if required files exist
echo "1. Checking required files..."
REQUIRED_FILES=(
  ".github/workflows/scrape.yml"
  ".github/workflows/scrape-test.yml"
  "scraper.py"
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
if command -v python3 &> /dev/null; then
  echo "  ✅ Python3 is installed"
  python3 --version
else
  echo "  ❌ Python3 is not installed"
fi

echo ""
echo "3. Testing workflow syntax..."
if command -v yamllint &> /dev/null; then
  yamllint .github/workflows/scrape.yml .github/workflows/scrape-test.yml
else
  echo "  ⚠️  yamllint not installed, skipping syntax check"
fi

echo ""
echo "4. Checking for common issues..."
echo "  - Virtual display setup: Xvfb configuration in scrape.yml"
echo "  - Browser dependencies: Firefox system packages"
echo "  - Timeout settings: 360 minutes (6 hours) max"
echo "  - Artifact upload: products.json, price_snapshots.json"

echo ""
echo "5. Quick test of scraper help..."
if [ -f "scraper.py" ]; then
  python3 scraper.py --help | head -20
  echo "..."
else
  echo "  ❌ scraper.py not found"
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
echo "- Use 'scrape-test.yml' for basic validation"
echo "- Check workflow logs for any errors"
echo "- Download artifacts to verify output"
echo ""
echo "Note: GitHub Actions has 6-hour timeout limit"
echo "      Adjust scraping limits if hitting timeout"