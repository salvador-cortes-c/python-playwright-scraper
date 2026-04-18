#!/bin/bash
# Setup script for product deduplication system
set -e

echo "🚀 Product Deduplication System Setup"
echo "===================================="

# Check if DATABASE_URL is set
if [ -z "$DATABASE_URL" ]; then
    echo "⚠️  DATABASE_URL not set. Please provide it:"
    echo "   export DATABASE_URL='postgresql://user:password@host:port/dbname'"
    echo ""
    echo "Or set it in .env file and source it:"
    echo "   source .env"
    exit 1
fi

echo "✓ DATABASE_URL configured"

# Step 1: Create consolidation logging table
echo ""
echo "📋 Step 1: Creating consolidation_log table..."
psql "$DATABASE_URL" < db/010_create_consolidation_log.sql
echo "✓ consolidation_log table created"

# Step 2: Install Python dependencies
echo ""
echo "📦 Step 2: Installing Python dependencies..."
pip install -q sentence-transformers numpy 2>/dev/null || pip install sentence-transformers numpy
echo "✓ Dependencies installed"

# Step 3: Test database connection
echo ""
echo "🔗 Step 3: Testing database connection..."
python -c "
import os
import psycopg
db_url = os.getenv('DATABASE_URL')
try:
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT COUNT(*) as product_count FROM products')
            count = cur.fetchone()[0]
            print(f'✓ Connected! Found {count} products in database')
except Exception as e:
    print(f'✗ Connection failed: {e}')
    exit(1)
"

# Step 4: Run deduplication analysis
echo ""
echo "🔍 Step 4: Analyzing for duplicate products..."
python similarity_deduplication.py --category wine --threshold 0.85 | head -20
echo ""

# Step 5: Extract patterns
echo ""
echo "📊 Step 5: Extracting consolidation patterns..."
patterns=$(python similarity_deduplication.py --extract-patterns 2>/dev/null)
if [ -z "$patterns" ]; then
    echo "ℹ️  No consolidations logged yet (first run)"
else
    echo "$patterns" | head -10
fi

echo ""
echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo "1. Review duplicate suggestions:"
echo "   python similarity_deduplication.py --category wine --threshold 0.85"
echo ""
echo "2. Export SQL migrations:"
echo "   python similarity_deduplication.py --export-migration > consolidations.sql"
echo ""
echo "3. Integrate into scraper:"
echo "   See DEDUPLICATION_PATTERNS.md for integration approaches"
