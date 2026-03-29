#!/bin/bash
# Setup compatible Python virtual environment for Playwright

set -e

echo "🔧 Setting up compatible Python environment for Playwright"
echo "=========================================================="

# Check available Python versions
echo "Checking available Python versions..."

PYTHON_VERSIONS=("python3.12" "python3.11" "python3.10" "python3.9" "python3.8" "python3")

for py_cmd in "${PYTHON_VERSIONS[@]}"; do
    if command -v $py_cmd &> /dev/null; then
        version=$($py_cmd --version 2>&1 | awk '{print $2}')
        echo "Found: $py_cmd -> $version"
        
        # Check if version is compatible with Playwright
        if [[ $version =~ ^3\.(8|9|10|11|12) ]]; then
            COMPATIBLE_PYTHON=$py_cmd
            echo "✅ $py_cmd ($version) is compatible with Playwright"
            break
        else
            echo "⚠️  $py_cmd ($version) may not be fully compatible"
        fi
    fi
done

if [ -z "$COMPATIBLE_PYTHON" ]; then
    echo "❌ No compatible Python version found"
    echo "Installing Python 3.12..."
    
    # Try to install Python 3.12
    if command -v apt &> /dev/null; then
        # Ubuntu/Debian
        sudo apt update
        sudo apt install -y python3.12 python3.12-venv
        COMPATIBLE_PYTHON="python3.12"
    elif command -v yum &> /dev/null; then
        # RHEL/CentOS
        sudo yum install -y python3.12
        COMPATIBLE_PYTHON="python3.12"
    elif command -v brew &> /dev/null; then
        # macOS
        brew install python@3.12
        COMPATIBLE_PYTHON="python3.12"
    else
        echo "❌ Could not install Python 3.12 automatically"
        echo "Please install Python 3.8-3.12 manually"
        exit 1
    fi
fi

echo ""
echo "Using: $COMPATIBLE_PYTHON"

# Remove existing venv if it exists
if [ -d ".venv" ]; then
    echo "Removing existing virtual environment..."
    rm -rf .venv
fi

# Create new virtual environment
echo "Creating new virtual environment with $COMPATIBLE_PYTHON..."
$COMPATIBLE_PYTHON -m venv .venv

# Activate and install dependencies
echo "Installing dependencies..."
source .venv/bin/activate
pip install --upgrade pip

# Install Playwright with compatible version
if [[ $($COMPATIBLE_PYTHON --version 2>&1) =~ 3\.1[0-2] ]]; then
    # Python 3.10-3.12
    pip install "playwright>=1.48.0"
elif [[ $($COMPATIBLE_PYTHON --version 2>&1) =~ 3\.[89] ]]; then
    # Python 3.8-3.9
    pip install "playwright>=1.48.0"
else
    # Fallback
    pip install playwright
fi

pip install playwright-stealth

# Install browsers
echo "Installing Playwright browsers..."
python -m playwright install firefox

echo ""
echo "✅ Setup complete!"
echo "Virtual environment: .venv/"
echo "Python: $($COMPATIBLE_PYTHON --version)"
echo ""
echo "To activate: source .venv/bin/activate"
echo "To generate cookies: ./refresh_cookies.sh"