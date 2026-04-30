# Deduplication Flags Documentation

## Overview
The scraper now includes three new command-line flags for controlling the post-scrape semantic deduplication pipeline. These flags allow you to:
- Enable/disable deduplication entirely
- Adjust auto-consolidation thresholds
- Fine-tune which products are exported for manual review

---

## Flags

### `--skip-deduplication`
**Type:** `action="store_true"` (boolean flag)  
**Default:** `False` (deduplication is **enabled** by default)  
**When to use:** When you want to skip the post-scrape deduplication step entirely

**Effect:**
- Post-scrape semantic deduplication will **not run** after products are persisted to the database
- Useful for quick test runs or when you want to handle deduplication separately
- Does NOT affect Layer 1 (key normalization during scrape time)

**Example:**
```bash
python scraper.py \
  --url "https://www.newworld.co.nz/shop/category/beverages?pg=1" \
  --skip-deduplication \
  --output products.json
```

---

### `--dedup-auto-threshold`
**Type:** `float` (0.0 to 1.0)  
**Default:** `0.95`  
**When to use:** When you want to automatically consolidate products above a specific similarity score

**Effect:**
- Products with cosine similarity **≥ this threshold** are automatically merged
- Higher value (e.g., 0.99) = stricter, fewer auto-merges (safer)
- Lower value (e.g., 0.80) = looser, more auto-merges (riskier)
- Typical range: **0.90–0.99**

**Example — strict auto-consolidation:**
```bash
python scraper.py \
  --url "https://www.woolworths.co.nz/shop/browse/wine" \
  --dedup-auto-threshold 0.98 \
  --output products.json
```

**Example — aggressive auto-consolidation:**
```bash
python scraper.py \
  --url "https://www.newworld.co.nz/shop/category/beverages?pg=1" \
  --dedup-auto-threshold 0.85 \
  --output products.json
```

---

### `--dedup-review-threshold`
**Type:** `float` (0.0 to 1.0)  
**Default:** `0.85`  
**When to use:** When you want to export potential duplicates for manual review

**Effect:**
- Products with similarity between `review_threshold` and `auto_threshold` are exported for review
- Results are written to:
  - `dedup_suggestions_YYYYMMDD_HHMMSS.json` (exported suggestions)
  - `dedup_patterns_YYYYMMDD_HHMMSS.json` (learned patterns)
- These can be reviewed manually before deciding whether to merge

**Relationship to `--dedup-auto-threshold`:**
- Must be **less than** auto_threshold: `review_threshold < auto_threshold`
- Typical values:
  - `review_threshold=0.85` + `auto_threshold=0.95`
  - `review_threshold=0.80` + `auto_threshold=0.90`

**Example:**
```bash
python scraper.py \
  --url "https://www.paknsave.co.nz/shop/category/fresh-foods-and-bakery/fruit-vegetables?pg=1" \
  --dedup-review-threshold 0.82 \
  --dedup-auto-threshold 0.95 \
  --output products.json
```

---

## Workflow Examples

### 1. **Production Run (Safe Defaults)**
```bash
python scraper.py \
  --url "https://www.newworld.co.nz/shop/category/wine?pg=1" \
  --dedup-auto-threshold 0.95 \
  --dedup-review-threshold 0.85 \
  --persist-db \
  --database-url "$DATABASE_URL" \
  --output wine_products.json
```
- **Auto-merges:** Similarity ≥ 0.95 (very confident matches only)
- **Export for review:** Similarity 0.85–0.95 (candidates for manual review)
- Products reviewed externally before approval

---

### 2. **Quick Test (No Dedup)**
```bash
python scraper.py \
  --url "https://www.woolworths.co.nz/shop/browse/fruit-veg?page=1" \
  --skip-deduplication \
  --limit 20 \
  --output test_products.json
```
- Skips post-scrape deduplication entirely
- Faster for testing or validation runs

---

### 3. **Aggressive Dedup (Full Merge)**
```bash
python scraper.py \
  --url "https://www.newworld.co.nz/" \
  --discover-category-urls \
  --crawl-category-pages \
  --dedup-auto-threshold 0.90 \
  --dedup-review-threshold 0.75 \
  --persist-db \
  --output all_products.json
```
- **Auto-merges:** Similarity ≥ 0.90 (broader consolidation)
- **Export for review:** Similarity 0.75–0.90 (more candidates for human review)
- Useful for full-site scrapes where dedup accuracy is important

---

## Configuration Reference

| Flag | Default | Min | Max | Typical |
|---|---|---|---|---|
| `--skip-deduplication` | N/A (false) | — | — | `--skip-deduplication` |
| `--dedup-auto-threshold` | 0.95 | 0.0 | 1.0 | 0.90–0.99 |
| `--dedup-review-threshold` | 0.85 | 0.0 | 1.0 | 0.75–0.95 |

**Constraint:** `review_threshold < auto_threshold`  
(The scraper will warn if this is violated.)

---

## Output Files

After deduplication runs, you'll find these files in the current directory:

### `dedup_suggestions_YYYYMMDD_HHMMSS.json`
JSON array of consolidation suggestions (mid-confidence matches):
```json
[
  {
    "group_id": 1,
    "product_a": "heineken lager_330ml",
    "product_b": "heineken lager bottle_330ml",
    "name_a": "Heineken Lager",
    "name_b": "Heineken Lager Bottle",
    "similarity": 0.92,
    "explanation": "Near-identical product names with same packaging"
  }
]
```

### `dedup_patterns_YYYYMMDD_HHMMSS.json`
Learned patterns from recent consolidations:
```json
{
  "patterns": [
    {
      "id": "low_alcohol_modifier",
      "regex": "(?i)\\s+low\\s+alcohol",
      "category": "wine",
      "confidence": 0.95,
      "action": "remove"
    }
  ],
  "retailer_conventions": {
    "woolworths": {
      "category": "wine",
      "adds_attributes": ["alcohol_level"]
    }
  }
}
```

---

## Troubleshooting

### **"Similarity threshold warning: review > auto"**
**Problem:** You set `--dedup-review-threshold 0.95 --dedup-auto-threshold 0.90`  
**Solution:** Swap them: `--dedup-review-threshold 0.85 --dedup-auto-threshold 0.95`

### **"Too many auto-consolidations, products are missing"**
**Problem:** `--dedup-auto-threshold` is too low (e.g., 0.70)  
**Solution:** Increase it: `--dedup-auto-threshold 0.92`

### **"Dedup files not being created"**
**Problem:** No mid-confidence matches exist (all matches are either very high or very low similarity)  
**Solution:** Adjust thresholds or run `--skip-deduplication` to check if the issue is elsewhere

### **"I want to disable Layer 1 normalization too"**
**Problem:** You want to skip all deduplication  
**Solution:** That's not directly possible; Layer 1 (normalization during scrape) always runs. Use `--skip-deduplication` to skip Layer 2 only.

---

## See Also

- **Main README:** [Product deduplication section](./README.md#product-deduplication)
- **Architecture:** [DEDUPLICATION_STRATEGY.md](./DEDUPLICATION_STRATEGY.md)
- **CLI Integration:** [DEDUPLICATION_PATTERNS.md](./DEDUPLICATION_PATTERNS.md)
- **Setup & Deployment:** [DEDUPLICATION_DEPLOYMENT.md](./DEDUPLICATION_DEPLOYMENT.md)
