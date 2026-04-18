# Product Deduplication Usage Guide

## Overview

Three-layer system to mitigate duplicates generically:
1. **Pre-scraping:** Normalize product names (existing `_normalize_product_name()`)
2. **Post-scraping:** Find semantic duplicates across categories (new `similarity_deduplication.py`)
3. **Learning:** Extract patterns from consolidations for future improvements

## Quick Start

### 1. Find similar products

```bash
# Find all similar products (threshold 0.85)
python similarity_deduplication.py

# Find similar *wine* products only
python similarity_deduplication.py --category wine

# Use stricter threshold (0.90)
python similarity_deduplication.py --category wine --threshold 0.90

# Show SQL migrations (don't execute yet)
python similarity_deduplication.py --export-migration
```

### 2. Learn from past consolidations

```bash
# Extract patterns from consolidation history
python similarity_deduplication.py --extract-patterns
```

Output example:
```json
{
  "high_confidence_modifiers": {
    "low alcohol screw cap": 5,
    "twist cap": 3,
    "cork embossed": 2
  },
  "category_patterns": {
    "wine": 8,
    "beverages": 2
  }
}
```

### 3. Integrate with scraper workflow

**In `scraper.py` (main loop, after scraping):**

```python
from similarity_deduplication import ProductDeduplicator

async def main():
    # ... existing scraper code ...
    
    # After inserting products into DB
    if args.deduplicate:
        print("\n[Deduplication] Finding similar products...")
        dedup = ProductDeduplicator(
            db_url=os.getenv("DATABASE_URL"),
            threshold=0.85
        )
        
        groups = dedup.find_similar_products(
            category=args.category  # Optional: filter by category
        )
        
        if groups:
            print(f"Found {len(groups)} similar product groups")
            
            # Option A: Auto-consolidate high-confidence matches (>0.95)
            for group in groups:
                if group.similarity > 0.95:
                    migration = dedup.generate_consolidation_migration(
                        source_key=group.product_b,
                        canonical_key=group.product_a,
                        similarity=group.similarity,
                        method="semantic"
                    )
                    print(f"Auto-consolidating: {group.name_b} → {group.name_a}")
                    # execute_migration(migration)
            
            # Option B: Export for manual review
            with open("consolidation_suggestions.sql", "w") as f:
                for group in groups:
                    if group.similarity >= 0.85:
                        migration = dedup.generate_consolidation_migration(
                            source_key=group.product_b,
                            canonical_key=group.product_a,
                            similarity=group.similarity,
                            method="semantic"
                        )
                        f.write(migration + "\n")
            
            print("Consolidation suggestions exported to consolidation_suggestions.sql")
```

**CLI usage:**

```bash
# Scrape and deduplicate
python scraper.py \
    --retailers woolworths new_world \
    --store-name "Karori" \
    --deduplicate
```

## Database Schema

You'll need a consolidation logging table:

```sql
CREATE TABLE IF NOT EXISTS consolidation_log (
    id SERIAL PRIMARY KEY,
    source_product_key VARCHAR(255) NOT NULL,
    canonical_product_key VARCHAR(255) NOT NULL,
    method VARCHAR(50) NOT NULL,  -- 'semantic', 'manual', 'pattern'
    similarity_score FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    executed_at TIMESTAMP,
    status VARCHAR(50) DEFAULT 'pending'  -- 'pending', 'executed', 'rejected'
);
```

## Workflow Example: Wine Category

### Scenario
Scraper run finds these products:
- `montana affinity sauvignon blanc_750ml` (New World)
- `montana affinity sauvignon blanc low alcohol screw cap_750ml` (Woolworths)
- `affinity sauvignon blanc 750ml` (PAK'nSAVE, partial name)

### Layer 1: Pre-scraping normalization
- WW product normalized: `"Montana Affinity Sauvignon Blanc Low Alcohol Screw Cap 750ml"` → product_key: `montana affinity sauvignon blanc_750ml`
- Creates consolidation during scrape

### Layer 2: Semantic deduplication (if Layer 1 missed anything)
```bash
python similarity_deduplication.py --category wine --threshold 0.87
```

Output:
```
1. Similarity: 0.92
   A: montana affinity sauvignon blanc_750ml → Montana Affinity Sauvignon Blanc 750ml
   B: affinity sauvignon blanc_750ml → Affinity Sauvignon Blanc 750ml
   Why: High token overlap: affinity, sauvignon, blanc, 750ml
```

Human reviews → Approve consolidation

### Layer 3: Pattern learning
```bash
python similarity_deduplication.py --extract-patterns
```

Output shows:
- "low alcohol screw cap" was removed 8 times → update normalization regex if needed
- Woolworths consistently adds "[alcohol level] [closure]" → document pattern

Next scraper run automatically normalizes these patterns (if added to Layer 1).

---

## Advanced: Custom Thresholds Per Category

Edit `deduplication_config.yaml`:

```yaml
category_thresholds:
  wine:
    threshold: 0.87  # Finer similarity = more aggressive consolidation
    min_snapshots: 5
  
  beverages:
    threshold: 0.82  # Broader similarity tolerance
    min_snapshots: 3
```

Then load in code:

```python
import yaml

with open("deduplication_config.yaml") as f:
    config = yaml.safe_load(f)

threshold = config['category_thresholds'].get(category, {}).get('threshold', 0.85)
dedup = ProductDeduplicator(db_url, threshold=threshold)
```

---

## Performance Notes

- **First run:** ~5-10 seconds (model download + embedding computation)
- **Subsequent runs:** <1 second (embeddings cached locally)
- **Memory:** ~200MB for model + embeddings cache
- **CPU:** Single-threaded, suitable for edge/low-resource environments

To speed up on large catalogs, filter by category:
```bash
python similarity_deduplication.py --category wine  # Faster than all products
```

---

## Safety & Review Gates

1. **Auto-consolidate only high confidence** (>0.95 similarity)
2. **Export medium confidence** (0.85-0.95) for manual review
3. **Reject low confidence** (<0.85)

Example:
```python
for group in groups:
    if group.similarity > 0.95:
        consolidate(group)  # Auto
    elif group.similarity > 0.85:
        export_for_review(group)  # Manual gate
    else:
        skip(group)  # Ignore
```

---

## Monitoring & Analytics

View consolidation history:
```bash
# How many consolidations done?
jq 'length' consolidations_log.json

# Success rate (method breakdown)
jq 'group_by(.method) | map({method: .[0].method, count: length})' consolidations_log.json

# Average similarity score
jq '[.[].similarity_score] | add / length' consolidations_log.json
```

Export for dashboard:
```bash
python similarity_deduplication.py --extract-patterns > dedup_metrics.json
```
