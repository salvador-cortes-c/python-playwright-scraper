#!/usr/bin/env python3
"""
Test different approaches for GitHub Actions Cloudflare bypass.
"""

import asyncio
from playwright.async_api import async_playwright
import json

async def test_with_different_settings():
    """Test different browser settings to bypass Cloudflare on GitHub Actions."""
    
    print("Testing different browser configurations...")
    
    # Try different user agents
    user_agents = [
        # Standard Firefox
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0",
        # macOS Firefox
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/119.0",
        # Linux Firefox
        "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0",
    ]
    
    url = "https://www.newworld.co.nz/shop/category/fruit-and-vegetables?pg=1"
    
    for i, user_agent in enumerate(user_agents):
        print(f"\nTest {i+1}: User Agent: {user_agent[:50]}...")
        
        async with async_playwright() as p:
            # Try with different launch options
            browser = await p.firefox.launch(
                headless=True,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-features=IsolateOrigins,site-per-process',
                    '--disable-web-security',
                    '--disable-features=site-per-process',
                ]
            )
            
            context = await browser.new_context(
                user_agent=user_agent,
                viewport={'width': 1920, 'height': 1080},
                locale='en-US',
                timezone_id='America/New_York',
                permissions=['geolocation'],
                extra_http_headers={
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'none',
                    'Sec-Fetch-User': '?1',
                    'Cache-Control': 'max-age=0',
                }
            )
            
            page = await context.new_page()
            
            try:
                # Try to navigate
                response = await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                print(f"  Status: {response.status if response else 'No response'}")
                
                # Check if we got a Cloudflare challenge
                content = await page.content()
                if "cloudflare" in content.lower() or "challenge" in content.lower():
                    print("  ❌ Cloudflare challenge detected")
                else:
                    print("  ✅ No Cloudflare challenge!")
                    # Try to get page title
                    title = await page.title()
                    print(f"  Title: {title[:50]}...")
                    break
                    
            except Exception as e:
                print(f"  ❌ Error: {e}")
            finally:
                await browser.close()
    
    print("\n" + "="*50)
    print("Summary: Need to find settings that work on GitHub Actions")

if __name__ == "__main__":
    asyncio.run(test_with_different_settings())