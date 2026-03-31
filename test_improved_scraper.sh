#!/bin/bash
# Test improved New World scraper

echo "🧪 Testing Improved New World Scraper"
echo "====================================="

if [ -z "$SCRAPINGBEE_API_KEY" ]; then
    echo "❌ SCRAPINGBEE_API_KEY not set"
    exit 1
fi

echo "API Key: ${SCRAPINGBEE_API_KEY:0:8}..."
echo ""

# Test with improved scraper
echo "Testing improved scraper with better price parsing..."
python3 scraper_scrapingbee_newworld.py \
    --url "https://www.newworld.co.nz/shop/category/fruit-and-vegetables?pg=1" \
    --limit 20 \
    --output improved_products.json \
    --delay-seconds 3

if [ -f "improved_products.json" ]; then
    echo ""
    echo "✅ Improved scraper test complete!"
    PRODUCT_COUNT=$(jq '. | length' improved_products.json 2>/dev/null || echo "0")
    echo "📦 Products found: $PRODUCT_COUNT"
    
    if [ "$PRODUCT_COUNT" -gt 0 ]; then
        echo ""
        echo "📊 Product details:"
        jq '.[] | {name: .name, price: .price, price_raw: .price_raw}' improved_products.json | head -10
        
        echo ""
        echo "💰 Price analysis:"
        jq '.[].price' improved_products.json | sort | uniq -c | sort -rn
        
        # Check if we have proper price formatting
        echo ""
        echo "🔍 Price formatting check:"
        jq '.[] | select(.price | test("^\\$[0-9]+\\.[0-9]{2}$")) | .price' improved_products.json | head -5
        WELL_FORMATTED=$(jq '[.[] | select(.price | test("^\\$[0-9]+\\.[0-9]{2}$"))] | length' improved_products.json)
        echo "Well-formatted prices: $WELL_FORMATTED/$PRODUCT_COUNT"
    fi
else
    echo ""
    echo "❌ No products file created"
    echo "Check debug_newworld.html for page structure"
fi