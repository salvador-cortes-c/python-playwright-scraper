#!/bin/bash
# Manual Playwright installation for Python 3.13

set -e

echo "🛠️  Manual Playwright installation for Python 3.13"
echo "=================================================="

# Check if we're in venv
if [ -z "$VIRTUAL_ENV" ]; then
    echo "❌ Not in a virtual environment"
    echo "Run: source .venv/bin/activate"
    exit 1
fi

echo "Python version: $(python --version)"

# Clone Playwright Python repository
echo "Cloning Playwright Python repository..."
if [ ! -d "/tmp/playwright-python" ]; then
    git clone https://github.com/microsoft/playwright-python.git /tmp/playwright-python
fi

cd /tmp/playwright-python
git pull

# Install from source
echo "Installing Playwright from source..."
pip install -e .

# Install browsers
echo "Installing browsers..."
python -m playwright install firefox

cd -

echo ""
echo "✅ Playwright installed from source!"
echo ""
echo "Now try running: ./refresh_cookies.sh"