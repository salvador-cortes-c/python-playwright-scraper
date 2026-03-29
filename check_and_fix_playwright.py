#!/usr/bin/env python3
"""
Check Python version and install compatible Playwright version.
Python 3.13 requires Playwright 1.49.0 or later.
"""

import sys
import subprocess
import platform

def get_python_version():
    """Get Python version as tuple (major, minor, micro)."""
    return sys.version_info[:3]

def check_playwright_installed():
    """Check if Playwright is already installed."""
    try:
        import playwright
        print(f"✅ Playwright {playwright.__version__} is installed")
        return True
    except ImportError:
        print("❌ Playwright is not installed")
        return False

def install_playwright_compatible():
    """Install Playwright version compatible with current Python."""
    version = get_python_version()
    print(f"Python version: {version[0]}.{version[1]}.{version[2]}")
    
    # Determine compatible Playwright version
    if version[0] == 3 and version[1] >= 13:
        # Python 3.13+ needs Playwright 1.49.0+
        playwright_version = "playwright>=1.49.0,<2.0.0"
        print(f"Python 3.13+ detected, installing: {playwright_version}")
    elif version[0] == 3 and version[1] >= 8:
        # Python 3.8-3.12 works with most versions
        playwright_version = "playwright>=1.48.0,<2.0.0"
        print(f"Python 3.{version[1]} detected, installing: {playwright_version}")
    else:
        print(f"⚠️  Python {version[0]}.{version[1]} may not be fully supported")
        playwright_version = "playwright"
    
    # Try to install
    print(f"Installing: {playwright_version}")
    try:
        subprocess.run([sys.executable, "-m", "pip", "install", playwright_version], check=True)
        print("✅ Playwright installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install Playwright: {e}")
        
        # Try alternative: install from GitHub
        print("Trying to install from GitHub...")
        try:
            subprocess.run([
                sys.executable, "-m", "pip", "install",
                "git+https://github.com/microsoft/playwright-python.git"
            ], check=True)
            print("✅ Playwright installed from GitHub")
            return True
        except subprocess.CalledProcessError:
            print("❌ Failed to install from GitHub")
            return False

def main():
    print("🔧 Playwright Compatibility Check")
    print("=" * 40)
    
    # Check current installation
    if check_playwright_installed():
        print("\n✅ Playwright is already installed correctly")
        return 0
    
    print("\n🔄 Installing compatible Playwright version...")
    if install_playwright_compatible():
        print("\n✅ Installation successful!")
        
        # Verify installation
        try:
            import playwright
            print(f"✅ Verified: Playwright {playwright.__version__} installed")
            
            # Install browsers
            print("\n🌐 Installing Playwright browsers...")
            subprocess.run([sys.executable, "-m", "playwright", "install", "firefox"], check=True)
            print("✅ Browsers installed")
            
            return 0
        except ImportError:
            print("❌ Installation verification failed")
            return 1
    else:
        print("\n❌ Failed to install Playwright")
        print("\n💡 Alternative solutions:")
        print("1. Use Python 3.8-3.12 (more compatible)")
        print("2. Install Playwright system-wide: sudo pip3 install playwright")
        print("3. Use Docker container with compatible Python")
        return 1

if __name__ == "__main__":
    sys.exit(main())