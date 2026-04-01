#!/bin/bash
# Test New World-specific ScrapingBee scraper

echo "🧪 Testing New World ScrapingBee Scraper"
echo "========================================"

# Check API key
if [ -z "$SCRAPINGBEE_API_KEY" ]; then
    echo "❌ SCRAPINGBEE_API_KEY not set"
    echo "   export SCRAPINGBEE_API_KEY='your_key'"
    exit 1
fi

echo "API Key: ${SCRAPINGBEE_API_KEY:0:8}..."
echo ""

# First, let's examine the HTML structure
echo "1. Examining New World page structure..."
if [ -f "scrapingbee_test.html" ]; then
    echo "Looking for product patterns in saved HTML..."
    
    # Search for common patterns
    echo "Searching for product-related text..."
    grep -i "product\|price\|each\|kg\|nzd" scrapingbee_test.html | head -20
    
    echo ""
    echo "Searching for data-testid attributes..."
    grep -i "data-testid" scrapingbee_test.html | head -10
    
    echo ""
    echo "Searching for class names with product..."
    grep -i "class.*product" scrapingbee_test.html | head -10
else
    echo "No saved HTML found. Running quick fetch..."
    python3 -c "
import requests
api_key = '$SCRAPINGBEE_API_KEY'
url = 'https://www.newworld.co.nz/shop/category/fruit-and-vegetables?pg=1'

params = {
    'api_key': api_key,
    'url': url,
    'render_js': 'true',
    'premium_proxy': 'true',
    'country_code': 'nz',
    'wait': '2000',
}

response = requests.get('https://app.scrapingbee.com/api/v1/', params=params, timeout=30)
if response.status_code == 200:
    with open('scrapingbee_test.html', 'w') as f:
        f.write(response.text[:5000])
    print('Saved sample HTML')
    # Look for patterns
    html = response.text[:5000]
    import re
    # Find all data-testid attributes
    testids = re.findall(r'data-testid=\"([^\"]+)\"', html)
    print(f'Found data-testid values: {set(testids)}')
else:
    print(f'Failed: {response.status_code}')
"
fi

echo ""
echo "2. Testing New World-specific scraper..."
python3 scraper.py \
    --url "https://www.newworld.co.nz/shop/category/fruit-and-vegetables?pg=1" \
    --limit 10 \
    --output newworld_products.json \
    --delay-seconds 3

if [ -f "newworld_products.json" ]; then
    echo ""
    echo "✅ New World scraper test complete!"
    echo "📦 Products found: $(jq '. | length' newworld_products.json)"
    echo ""
    echo "Sample products:"
    jq '.[] | {name: .name, price: .price}' newworld_products.json | head -5
else
    echo ""
    echo "❌ No products file created"
    echo ""
    echo "💡 Next steps:"
    echo "   1. Check debug_newworld.html for page structure"
    echo "   2. Update selectors in scraper.py"
    echo "   3. Test with different URL or parameters"
fi