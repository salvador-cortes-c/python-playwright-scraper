#!/usr/bin/env python3
"""
Quick cookie refresh script for New World scraper.
Run this locally every 1-2 weeks to maintain Cloudflare session.
"""

import subprocess
import sys
import os

def main():
    print("🔑 New World Scraper - Quick Cookie Refresh")
    print("=" * 50)
    
    # Check if we're in the right directory
    if not os.path.exists("scraper.py"):
        print("❌ Error: Run this script from the project root directory")
        sys.exit(1)
    
    # Check virtual environment
    venv_python = ".venv/bin/python"
    if not os.path.exists(venv_python):
        print("❌ Virtual environment not found.")
        print("   Create one with: python -m venv .venv")
        print("   Then run: source .venv/bin/activate")
        print("   And: pip install -r requirements.txt")
        sys.exit(1)
    
    print("\n🚀 Starting scraper with manual Cloudflare verification...")
    print("💡 A Firefox window will open - complete the 'I'm not a robot' challenge")
    print("⏳ Waiting for challenge to appear (may take 30-60 seconds)...")
    
    # Run the scraper
    cmd = [
        venv_python, "scraper.py",
        "--url", "https://www.newworld.co.nz/shop/category/fruit-and-vegetables?pg=1",
        "--limit", "3",
        "--headed",
        "--manual-wait-seconds", "90",
        "--output", "cookie_test.json",
        "--storage-state", "storage_state.json",
        "--delay-seconds", "2"
    ]
    
    try:
        result = subprocess.run(cmd, check=True)
        print("\n✅ Cookie refresh successful!")
        print("\n📁 Updated: storage_state.json")
        print("📊 Test results: cookie_test.json")
        
        # Show quick stats if test file exists
        if os.path.exists("cookie_test.json"):
            import json
            with open("cookie_test.json", "r") as f:
                data = json.load(f)
                print(f"📦 Products scraped: {len(data)}")
        
        print("\n📤 Next: Commit storage_state.json to GitHub")
        print("   git add storage_state.json")
        print("   git commit -m 'Update Cloudflare cookies'")
        print("   git push origin main")
        
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Error during cookie refresh: {e}")
        print("\n💡 Tips:")
        print("   - Make sure Firefox is installed")
        print("   - Complete the Cloudflare challenge when it appears")
        print("   - Wait for the manual-wait-seconds (90 seconds)")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n⏹️  Cookie refresh cancelled by user")
        sys.exit(0)

if __name__ == "__main__":
    main()