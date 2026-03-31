#!/usr/bin/env python3
"""
New World scraper using ScrapingBee API with New World-specific selectors.
"""

import asyncio
import aiohttp
import json
import sys
import os
from typing import List, Dict, Any, Optional, Tuple
import argparse
from datetime import datetime
import time

# ScrapingBee API configuration
SCRAPINGBEE_API_URL = "https://app.scrapingbee.com/api/v1/"
SCRAPINGBEE_API_KEY_ENV = "SCRAPINGBEE_API_KEY"

async def fetch_with_scrapingbee(session: aiohttp.ClientSession, url: str, api_key: str) -> Tuple[Optional[str], Optional[Dict]]:
    """Fetch a URL using ScrapingBee API."""
    params = {
        'api_key': api_key,
        'url': url,
        'render_js': 'true',
        'premium_proxy': 'true',
        'country_code': 'nz',
        'wait': '3000',  # Wait 3 seconds for JS to render
        'block_ads': 'true',
        'block_resources': 'false',
        'return_page_source': 'true',
    }
    
    try:
        print(f"  Fetching: {url[:80]}...")
        
        async with session.get(SCRAPINGBEE_API_URL, params=params, timeout=45) as response:
            if response.status == 200:
                html = await response.text()
                if "<!DOCTYPE" in html or "<html" in html:
                    return html, None
                else:
                    try:
                        error_data = await response.json()
                        return None, error_data
                    except:
                        return None, {"error": "Invalid response (not HTML)"}
            else:
                error_text = await response.text()
                try:
                    error_data = await response.json()
                    return None, error_data
                except:
                    return None, {"error": f"HTTP {response.status}"}
                
    except asyncio.TimeoutError:
        return None, {"error": "Timeout after 45 seconds"}
    except Exception as e:
        return None, {"error": str(e)}

def extract_products_from_html_newworld(html: str, url: str) -> List[Dict[str, Any]]:
    """
    Extract products from New World HTML.
    Based on New World's specific page structure.
    """
    from bs4 import BeautifulSoup
    
    products = []
    
    try:
        soup = BeautifulSoup(html, 'html.parser')
        
        # New World specific selectors
        product_containers = []
        
        # Based on what we found: data-testid attributes are key
        selectors_to_try = [
            '[data-testid*="product"]',  # Primary selector that worked
            '[data-testid*="Product"]',
            '[data-testid*="item"]',
            '[data-testid*="tile"]',
            'div[data-testid]',  # Any div with data-testid
            'article[data-testid]',  # Any article with data-testid
            '[class*="product"]',
            '.product-tile', '.product-item',
        ]
        
        for selector in selectors_to_try:
            containers = soup.select(selector)
            if containers:
                print(f"  Found {len(containers)} elements with selector: {selector}")
                # Filter to likely product containers
                # Look for containers that have price or name elements
                filtered_containers = []
                for container in containers:
                    # Check if container has price or name indicators
                    text = container.get_text().lower()
                    if any(word in text for word in ['$', 'each', 'kg', 'price', 'product']):
                        filtered_containers.append(container)
                
                if filtered_containers:
                    product_containers = filtered_containers
                    print(f"  Filtered to {len(product_containers)} likely product containers")
                    break
                elif len(containers) > 5:  # If many containers, assume they're products
                    product_containers = containers
                    break
        
        # If no containers found, try a different approach
        if not product_containers:
            # Look for any elements that might contain product info
            all_elements = soup.find_all(True)  # All elements
            for elem in all_elements[:100]:  # Check first 100
                text = elem.get_text().lower()
                if '$' in elem.get_text() and any(word in text for word in ['each', 'kg', 'price']):
                    product_containers.append(elem)
        
        # Extract product info from containers
        for container in product_containers[:20]:  # Limit to 20 for testing
            product = extract_product_info(container)
            if product.get('name') and product.get('price'):
                product['scraped_at'] = datetime.now().isoformat()
                product['source_url'] = url
                products.append(product)
        
        # Debug: Show what we found
        if products:
            print(f"  Extracted {len(products)} products")
            for i, p in enumerate(products[:3]):
                print(f"    Product {i+1}: {p.get('name', 'No name')[:30]}... - {p.get('price', 'No price')}")
        else:
            print("  No products extracted")
            # Save HTML for debugging
            with open('debug_newworld.html', 'w', encoding='utf-8') as f:
                f.write(html[:10000])
            print("  Saved first 10k chars to debug_newworld.html for inspection")
        
        return products
        
    except Exception as e:
        print(f"  Error parsing HTML: {e}")
        import traceback
        traceback.print_exc()
        return []

def extract_product_info(container) -> Dict[str, Any]:
    """Extract product info from a container element."""
    from bs4 import BeautifulSoup
    
    product = {}
    
    # Try to find name
    name_selectors = [
        'h3', 'h4',  # Common heading tags
        '[data-testid*="name"]', '[data-testid*="title"]',
        '.product-name', '.product-title', '.name', '.title',
        '[class*="name"]', '[class*="title"]',
    ]
    
    for selector in name_selectors:
        elem = container.select_one(selector)
        if elem and elem.get_text().strip():
            product['name'] = elem.get_text().strip()
            break
    
    # Try to find price - New World specific
    price_selectors = [
        '[data-testid*="price"]',  # Primary: data-testid attributes
        '[data-testid*="Price"]',
        '.price', '.product-price', '.current-price',
        '[class*="price"]', '[class*="Price"]',
        'span', 'div', 'p'
    ]
    
    for selector in price_selectors:
        elem = container.select_one(selector)
        if elem:
            text = elem.get_text().strip()
            # Clean the price text
            import re
            
            # Remove extra whitespace, newlines
            text = ' '.join(text.split())
            
            # New World prices might be like "329" meaning "$3.29"
            # Or might have $ sign: "$3.29"
            # Or might be "3.29"
            
            # Try to find price pattern
            price_patterns = [
                r'[\$\£\€]?\s*(\d+\.\d{2})',  # $3.29 or 3.29
                r'[\$\£\€]?\s*(\d+)',  # $329 or 329 (needs decimal)
                r'(\d+)c',  # 329c
                r'each\s*[\$\£\€]?\s*(\d+\.?\d*)',  # each $3.29
                r'kg\s*[\$\£\€]?\s*(\d+\.?\d*)',  # kg $3.29
            ]
            
            for pattern in price_patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    price_num = match.group(1)
                    # If it's a whole number like "329", add decimal
                    if '.' not in price_num and len(price_num) > 2:
                        # Assume last 2 digits are cents
                        dollars = price_num[:-2]
                        cents = price_num[-2:]
                        if dollars:  # Avoid empty dollars
                            product['price'] = f"${dollars}.{cents}"
                        else:
                            product['price'] = f"$0.{cents}"
                    elif '.' in price_num:
                        # Already has decimal
                        if not price_num.startswith('$'):
                            product['price'] = f"${price_num}"
                        else:
                            product['price'] = price_num
                    else:
                        # Simple number
                        product['price'] = f"${price_num}"
                    
                    # Also store raw text for debugging
                    product['price_raw'] = text
                    break
            
            if 'price' in product:
                break
    
    # Try to find image
    img_selectors = ['img', 'picture', '[data-testid*="image"]']
    for selector in img_selectors:
        elem = container.select_one(selector)
        if elem and elem.get('src'):
            product['image_url'] = elem['src']
            # Make absolute URL if relative
            if product['image_url'].startswith('/'):
                product['image_url'] = f"https://www.newworld.co.nz{product['image_url']}"
            break
    
    # Try to find product URL
    link_selectors = ['a', '[href*="product"]', '[data-testid*="link"]']
    for selector in link_selectors:
        elem = container.select_one(selector)
        if elem and elem.get('href'):
            product['product_url'] = elem['href']
            if product['product_url'].startswith('/'):
                product['product_url'] = f"https://www.newworld.co.nz{product['product_url']}"
            break
    
    return product

async def scrape_url(session: aiohttp.ClientSession, url: str, api_key: str, delay: float = 2.0) -> List[Dict[str, Any]]:
    """Scrape a single URL."""
    print(f"Scraping: {url}")
    
    if delay > 0:
        await asyncio.sleep(delay)
    
    html, error = await fetch_with_scrapingbee(session, url, api_key)
    
    if error:
        print(f"  ❌ Error: {error}")
        return []
    
    if not html:
        print("  ❌ No HTML returned")
        return []
    
    products = extract_products_from_html_newworld(html, url)
    print(f"  ✅ Found {len(products)} products")
    
    return products

async def main():
    parser = argparse.ArgumentParser(description='New World scraper using ScrapingBee')
    parser.add_argument('--url', required=True, help='URL to scrape')
    parser.add_argument('--limit', type=int, default=20, help='Max products to scrape')
    parser.add_argument('--output', default='products.json', help='Output JSON file')
    parser.add_argument('--delay-seconds', type=float, default=3.0, help='Delay between requests')
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
        
        # Show summary
        print("\n📊 Summary:")
        for i, p in enumerate(all_products[:5]):
            print(f"  {i+1}. {p.get('name', 'No name')[:40]}... - {p.get('price', 'No price')}")
        if len(all_products) > 5:
            print(f"  ... and {len(all_products) - 5} more")
    else:
        print("\n❌ No products scraped")
        print("💡 Check debug_newworld.html to see page structure")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())