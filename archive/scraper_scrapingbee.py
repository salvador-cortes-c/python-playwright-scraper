#!/usr/bin/env python3
"""
New World scraper using ScrapingBee API to bypass Cloudflare.
"""

import asyncio
import aiohttp
import json
import sys
import os
from urllib.parse import urlparse, parse_qs, urlencode
from typing import List, Dict, Any, Optional, Tuple
import argparse
from datetime import datetime
import time

# ScrapingBee API configuration
SCRAPINGBEE_API_URL = "https://app.scrapingbee.com/api/v1/"
# Your API key will be loaded from environment variable
SCRAPINGBEE_API_KEY_ENV = "SCRAPINGBEE_API_KEY"

async def fetch_with_scrapingbee(session: aiohttp.ClientSession, url: str, api_key: str) -> Tuple[Optional[str], Optional[Dict]]:
    """
    Fetch a URL using ScrapingBee API.
    Returns (html_content, error_dict)
    """
    params = {
        'api_key': api_key,
        'url': url,
        'render_js': 'true',  # Render JavaScript (important for SPAs)
        'premium_proxy': 'true',  # Use premium proxies (better success rate)
        'country_code': 'nz',  # New Zealand proxies
        'wait': '2000',  # Wait 2 seconds for JS to render
        'block_ads': 'true',  # Block ads for faster loading
        'block_resources': 'false',  # Allow images/CSS
        'return_page_source': 'true',  # Return full HTML
    }
    
    try:
        print(f"  Fetching with ScrapingBee: {url[:80]}...")
        
        async with session.get(SCRAPINGBEE_API_URL, params=params, timeout=30) as response:
            if response.status == 200:
                html = await response.text()
                # Check if we got a valid HTML response
                if "<!DOCTYPE" in html or "<html" in html:
                    return html, None
                else:
                    # Might be JSON error response
                    try:
                        error_data = await response.json()
                        return None, error_data
                    except:
                        return None, {"error": "Invalid response (not HTML)", "response_preview": html[:200]}
            else:
                error_text = await response.text()
                try:
                    error_data = await response.json()
                    return None, error_data
                except:
                    return None, {"error": f"HTTP {response.status}", "response": error_text[:200]}
                
    except asyncio.TimeoutError:
        return None, {"error": "Timeout after 30 seconds"}
    except Exception as e:
        return None, {"error": str(e)}

def extract_products_from_html(html: str, url: str) -> List[Dict[str, Any]]:
    """
    Extract products from HTML using the same selectors as the original scraper.
    This is a simplified version - you might need to adjust selectors.
    """
    from bs4 import BeautifulSoup
    
    products = []
    
    try:
        soup = BeautifulSoup(html, 'html.parser')
        
        # Try different selectors based on New World's structure
        # These are example selectors - you'll need to adjust based on actual page structure
        product_selectors = [
            'div[data-testid="product-tile"]',
            '.product-tile',
            '.product-item',
            '[class*="product"]',
            'article',
        ]
        
        for selector in product_selectors:
            product_elements = soup.select(selector)
            if product_elements and len(product_elements) > 0:
                print(f"  Found {len(product_elements)} products with selector: {selector}")
                
                for elem in product_elements[:5]:  # Limit for testing
                    product = {
                        'name': extract_text(elem, ['h3', 'h4', '.product-name', '[data-testid*="name"]']),
                        'price': extract_text(elem, ['.price', '.product-price', '[data-testid*="price"]']),
                        'image_url': extract_attr(elem, 'img', 'src', ['product-image', 'img']),
                        'product_url': extract_attr(elem, 'a', 'href', ['product-link']),
                        'scraped_at': datetime.now().isoformat(),
                        'source_url': url,
                    }
                    
                    # Clean up data
                    product = {k: v.strip() if isinstance(v, str) else v for k, v in product.items()}
                    product = {k: v for k, v in product.items() if v}  # Remove empty values
                    
                    if product.get('name') and product.get('price'):
                        products.append(product)
                
                break  # Use first working selector
        
        if not products:
            # Fallback: try to find any product-like structures
            all_text = soup.get_text()
            if any(keyword in all_text.lower() for keyword in ['$', 'nzd', 'price', 'product']):
                print("  Found price/product keywords but no structured data")
                # You might need to implement more sophisticated parsing
        
        return products
        
    except Exception as e:
        print(f"  Error parsing HTML: {e}")
        return []

def extract_text(element, selectors):
    """Extract text from element using multiple selector attempts."""
    from bs4 import BeautifulSoup
    if not isinstance(element, BeautifulSoup):
        soup = element
    else:
        soup = element
    
    for selector in selectors:
        found = soup.select_one(selector)
        if found and found.text.strip():
            return found.text.strip()
    return ""

def extract_attr(element, tag, attr, class_hints):
    """Extract attribute from element."""
    from bs4 import BeautifulSoup
    if not isinstance(element, BeautifulSoup):
        soup = element
    else:
        soup = element
    
    # Try direct tag
    found = soup.find(tag)
    if found and found.get(attr):
        return found[attr]
    
    # Try with class hints
    for hint in class_hints:
        found = soup.find(tag, class_=lambda x: x and hint in str(x).lower())
        if found and found.get(attr):
            return found[attr]
    
    return ""

async def scrape_url(session: aiohttp.ClientSession, url: str, api_key: str, delay: float = 1.0) -> List[Dict[str, Any]]:
    """Scrape a single URL."""
    print(f"Scraping: {url}")
    
    # Add delay to be polite
    if delay > 0:
        await asyncio.sleep(delay)
    
    html, error = await fetch_with_scrapingbee(session, url, api_key)
    
    if error:
        print(f"  ❌ Error: {error}")
        return []
    
    if not html:
        print("  ❌ No HTML returned")
        return []
    
    products = extract_products_from_html(html, url)
    print(f"  ✅ Found {len(products)} products")
    
    return products

async def main():
    parser = argparse.ArgumentParser(description='New World scraper using ScrapingBee')
    parser.add_argument('--url', required=True, help='URL to scrape')
    parser.add_argument('--limit', type=int, default=20, help='Max products to scrape')
    parser.add_argument('--output', default='products.json', help='Output JSON file')
    parser.add_argument('--delay-seconds', type=float, default=2.0, help='Delay between requests')
    parser.add_argument('--api-key', help='ScrapingBee API key (or use SCRAPINGBEE_API_KEY env var)')
    
    args = parser.parse_args()
    
    # Get API key
    api_key = args.api_key or os.getenv(SCRAPINGBEE_API_KEY_ENV)
    if not api_key:
        print("❌ Error: ScrapingBee API key required")
        print("   Set SCRAPINGBEE_API_KEY environment variable or use --api-key")
        sys.exit(1)
    
    print(f"Using ScrapingBee API (key: {api_key[:8]}...)")
    
    all_products = []
    
    async with aiohttp.ClientSession() as session:
        products = await scrape_url(session, args.url, api_key, args.delay_seconds)
        all_products.extend(products[:args.limit])
    
    # Save results
    if all_products:
        with open(args.output, 'w') as f:
            json.dump(all_products, f, indent=2)
        print(f"\n✅ Saved {len(all_products)} products to {args.output}")
    else:
        print("\n❌ No products scraped")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())