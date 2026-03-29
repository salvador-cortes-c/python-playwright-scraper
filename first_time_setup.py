#!/usr/bin/env python3
"""
First-time setup script for New World scraper.
Run this on YOUR LOCAL MACHINE (with GUI) to generate initial cookies.
"""

import subprocess
import sys
import os

def print_step(step, description):
    print(f"\n{'='*60}")
    print(f"STEP {step}: {description}")
    print('='*60)

def main():
    print("🎬 NEW WORLD SCRAPER - FIRST TIME SETUP")
    print("This script will help you generate Cloudflare cookies.")
    print("You need to run this on YOUR LOCAL COMPUTER with a GUI.")
    print()
    
    # Step 1: Check environment
    print_step(1, "Checking Python environment")
    try:
        import playwright
        print("✅ Playwright is installed")
    except ImportError:
        print("❌ Playwright not installed")
        print("   Run: pip install playwright")
        print("   Then: python -m playwright install firefox")
        return
    
    # Step 2: Check Firefox
    print_step(2, "Checking Firefox installation")
    firefox_paths = [
        "/usr/bin/firefox",
        "/usr/local/bin/firefox",
        "C:\\Program Files\\Mozilla Firefox\\firefox.exe",
        "C:\\Program Files (x86)\\Mozilla Firefox\\firefox.exe"
    ]
    
    firefox_found = False
    for path in firefox_paths:
        if os.path.exists(path):
            print(f"✅ Firefox found at: {path}")
            firefox_found = True
            break
    
    if not firefox_found:
        print("⚠️  Firefox not found in common locations")
        print("   Make sure Firefox is installed on your system")
    
    # Step 3: Instructions
    print_step(3, "READY TO GENERATE COOKIES")
    print()
    print("📋 MANUAL STEPS REQUIRED:")
    print("1. A Firefox window will open")
    print("2. You'll see a Cloudflare 'I'm not a robot' challenge")
    print("3. Complete the challenge (click checkboxes, etc.)")
    print("4. Wait 2 minutes for the script to continue")
    print("5. Cookies will be saved to 'storage_state.json'")
    print()
    
    response = input("Ready to proceed? (yes/no): ").strip().lower()
    if response not in ['yes', 'y']:
        print("Setup cancelled.")
        return
    
    # Step 4: Run scraper
    print_step(4, "Running scraper with manual Cloudflare verification")
    print("⏳ Opening Firefox...")
    print("💡 Complete the Cloudflare challenge when it appears")
    print("⏰ This will take about 2 minutes...")
    
    cmd = [
        sys.executable, "scraper.py",
        "--url", "https://www.newworld.co.nz/shop/category/fruit-and-vegetables?pg=1",
        "--limit", "3",
        "--headed",
        "--manual-wait-seconds", "120",
        "--output", "first_run_test.json",
        "--storage-state", "storage_state.json",
        "--delay-seconds", "2"
    ]
    
    try:
        print("\n🚀 Starting scraper...")
        result = subprocess.run(cmd, check=True)
        
        print_step(5, "SUCCESS!")
        print("✅ Cookies generated successfully!")
        print("📁 File created: storage_state.json")
        
        if os.path.exists("first_run_test.json"):
            import json
            with open("first_run_test.json", "r") as f:
                data = json.load(f)
                print(f"📦 Test products scraped: {len(data)}")
        
        print("\n📤 NEXT STEPS:")
        print("1. Upload cookies to GitHub:")
        print("   git add storage_state.json")
        print("   git commit -m 'Initial Cloudflare cookies'")
        print("   git push origin main")
        print()
        print("2. GitHub Actions will run automatically:")
        print("   - Daily at 2 AM UTC")
        print("   - Using your saved cookies")
        print("   - Results saved as artifacts")
        
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Error: {e}")
        print("\n💡 TROUBLESHOOTING:")
        print("   - Make sure Firefox opens")
        print("   - Complete the Cloudflare challenge")
        print("   - Wait for the full 2 minutes")
    except KeyboardInterrupt:
        print("\n⏹️  Setup cancelled by user")

if __name__ == "__main__":
    main()