# Semantic Deduplication - Quick Start Guide

## Prerequisites

1. **Set DATABASE_URL:**
   ```bash
   export DATABASE_URL="postgresql://user:password@host:port/dbname"
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

## One-Time Setup

### 1. Create consolidation logging table

```bash
# Option A: Run migration directly (requires psql CLI)
psql "$DATABASE_URL" < db/010_create_consolidation_log.sql

# Option B: Run from Python
python -c "
import psycopg
import os
with open('db/010_create_consolidation_log.sql') as f:
    sql = f.read()
with psycopg.connect(os.getenv('DATABASE_URL')) as conn:
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
print('✓ consolidation_log table created')
"
```

### 2. Test the setup

```bash
python test_deduplication.py
```

## Usage Examples

### Quick Test: Find duplicates

```bash
python similarity_deduplication.py --threshold 0.85
```

### Find duplicates in specific category

```bash
python similarity_deduplication.py --category wine --threshold 0.87
```

### Export for manual review

```bash
python similarity_deduplication.py --export-migration > consolidations.sql
```

### View SQL consolidation suggestions

```bash
cat consolidations.sql | head -50
```

### Apply consolidations to database

```bash
# Manually review first!
psql "$DATABASE_URL" < consolidations.sql
```

### Extract learned patterns

```bash
python similarity_deduplication.py --extract-patterns
```

## Integration with Scraper

### Run scraper with deduplication

```bash
python scraper.py \
    --url "https://example.com/products" \
    --retailers woolworths \
    --persist-db
```

Deduplication runs automatically after scraping (unless you add `--skip-deduplication`).

### Skip deduplication for this run

```bash
python scraper.py \
    --url "https://example.com/products" \
    --skip-deduplication
```

### Adjust deduplication thresholds

```bash
python scraper.py \
    --url "https://example.com/products" \
    --dedup-auto-threshold 0.92 \
    --dedup-review-threshold 0.82
```

## Available Files

| File | Purpose |
|------|---------|
| `similarity_deduplication.py` | Core deduplication engine |
| `scraper_deduplication_integration.py` | Scraper integration hook |
| `deduplication_config.yaml` | Configuration (thresholds, behavior) |
| `consolidations_log.json` | Log of all consolidations (auto-created) |
| `db/010_create_consolidation_log.sql` | Database migration |
| `test_deduplication.py` | Comprehensive test suite |

## How It Works

### Layer 1: Pre-Scraping (Normalization)
- Strips common modifiers before product_key generation
- Fast, happens during scrape

### Layer 2: Post-Scraping (Semantic Matching)
- Uses embeddings to find similar products
- Runs after scraper completes
- Exports suggestions for review

### Layer 3: Learning
- Analyzes past consolidations
- Extracts patterns for future improvement

## Monitoring & Analytics

### Check consolidation history

```bash
# How many consolidations?
jq 'length' consolidations_log.json

# Breakdown by method
jq 'group_by(.method) | map({method: .[0].method, count: length})' consolidations_log.json

# Average similarity
jq '[.[].similarity_score] | add / length' consolidations_log.json
```

### View database consolidation logs

```sql
-- All consolidations
SELECT * FROM consolidation_log ORDER BY created_at DESC LIMIT 20;

-- Statistics
SELECT * FROM consolidation_stats;

-- Active consolidations
SELECT * FROM active_consolidations;
```

## Troubleshooting

### "DATABASE_URL not set"
```bash
export DATABASE_URL="postgresql://user:password@host:port/dbname"
```

### "consolidation_log table not found"
```bash
psql "$DATABASE_URL" < db/010_create_consolidation_log.sql
```

### "sentence_transformers not installed"
```bash
pip install sentence-transformers numpy
```

### Slow first run?
First run downloads the embedding model (~100MB). Subsequent runs are fast due to caching.

### Want to reset cache?
```bash
rm -rf .embeddings_cache/
```

## Next Steps

1. ✅ Set DATABASE_URL
2. ✅ Run `python test_deduplication.py`
3. ✅ Try `python similarity_deduplication.py --category wine`
4. ✅ Review suggestions in exported SQL file
5. ✅ Apply consolidations: `psql $DATABASE_URL < consolidations.sql`
6. ✅ Run `python similarity_deduplication.py --extract-patterns` to learn
7. ✅ Integrate into scraper runs

## Support

See these detailed guides:
- **Architecture**: `DEDUPLICATION_STRATEGY.md`
- **Integration**: `DEDUPLICATION_PATTERNS.md`
- **Configuration**: `deduplication_config.yaml`
