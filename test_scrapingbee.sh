#!/bin/bash
# Test ScrapingBee API with New World

echo "🧪 Testing ScrapingBee API"
echo "=========================="

# Check if API key is set
if [ -z "$SCRAPINGBEE_API_KEY" ]; then
    echo "❌ SCRAPINGBEE_API_KEY environment variable not set"
    echo ""
    echo "💡 Set it with:"
    echo "   export SCRAPINGBEE_API_KEY='your_api_key_here'"
    echo "   Or add to ~/.bashrc or ~/.zshrc"
    exit 1
fi

echo "API Key: ${SCRAPINGBEE_API_KEY:0:8}..."
echo ""

# Install dependencies first
echo "Installing dependencies..."
pip install aiohttp beautifulsoup4 requests

# Test with a simple request first
echo "1. Testing basic ScrapingBee request..."
python3 -c "
import os
import requests

api_key = os.getenv('SCRAPINGBEE_API_KEY')
url = 'https://www.newworld.co.nz/shop/category/fruit-and-vegetables?pg=1'

params = {
    'api_key': api_key,
    'url': url,
    'render_js': 'true',
    'premium_proxy': 'true',
    'country_code': 'nz',
    'wait': '2000',
}

print(f'Testing URL: {url}')
response = requests.get('https://app.scrapingbee.com/api/v1/', params=params, timeout=30)

print(f'Status Code: {response.status_code}')
print(f'Content Length: {len(response.text)} chars')

if response.status_code == 200:
    if '<!DOCTYPE' in response.text or '<html' in response.text:
        print('✅ SUCCESS: Got HTML response')
        # Check for Cloudflare
        if 'cloudflare' in response.text.lower() or 'challenge' in response.text.lower():
            print('⚠️  Warning: Cloudflare might still be present')
        else:
            print('✅ No Cloudflare detected')
        
        # Save sample
        with open('scrapingbee_test.html', 'w') as f:
            f.write(response.text[:5000])
        print('📁 Sample saved to scrapingbee_test.html')
    else:
        print('❌ Response is not HTML')
        print(f'Preview: {response.text[:200]}')
else:
    print(f'❌ Request failed: {response.status_code}')
    print(f'Error: {response.text[:200]}')
"

echo ""
echo "2. Testing with full scraper..."
python3 scraper_scrapingbee.py \
    --url "https://www.newworld.co.nz/shop/category/fruit-and-vegetables?pg=1" \
    --limit 5 \
    --output scrapingbee_products.json \
    --delay-seconds 2

if [ -f "scrapingbee_products.json" ]; then
    echo ""
    echo "✅ ScrapingBee test successful!"
    echo "📦 Products found: $(jq '. | length' scrapingbee_products.json)"
    echo ""
    echo "Sample product:"
    jq '.[0]' scrapingbee_products.json
else
    echo ""
    echo "❌ ScrapingBee test failed"
    echo "💡 Check:"
    echo "   1. API key is correct"
    echo "   2. You have credits available"
    echo "   3. URL is accessible"
fi