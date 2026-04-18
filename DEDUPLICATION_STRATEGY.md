# Generic Product Deduplication Strategy

## Problem
Current hardcoded approach (`_normalize_product_name()`) only strips specific modifiers. This:
- Requires manual updates for each new modifier pattern discovered
- Doesn't scale across different product categories
- Doesn't handle variant grouping (750ml vs 1L of same wine)
- Fails when retailers use unknown attribute patterns

## Solution: 3-Layer Intelligent Deduplication

### Layer 1: Pre-Scraping Normalization (Current - Lightweight)
**Purpose:** Handle obvious cases quickly before DB operations
**Approach:**
- Extract packaging format (750ml, 6-pack, etc.) using regex patterns
- Remove known modifiers (Low Alcohol, Screw Cap, etc.)
- Keep as is—fast, database-friendly

**Cost:** Low (string operations only)
**Handles:** ~60-70% of common duplicates

### Layer 2: Post-Scraping Semantic Deduplication (NEW - Intelligent)
**Purpose:** Group similar product names that survived Layer 1
**Approach:**
- Use **sentence embeddings** (e.g., SentenceTransformers) to vectorize product names
- Calculate similarity between products with `norm_name` + same category
- Group products with similarity > threshold (e.g., 0.85)
- Create consolidation suggestions for review

**Cost:** Medium (embedding inference on new products)
**Handles:** Remaining 25-30% of duplicates (variants, retailer-specific descriptions)
**Tools:**
- `sentence-transformers` (lightweight, no GPU needed)
- Local SQLite cache for embeddings (avoid recomputing)

### Layer 3: Canonical Product Registry (NEW - Long-term Learning)
**Purpose:** Build knowledge base from past consolidations
**Approach:**
- Track all manual consolidations (which products were merged, why)
- Extract patterns automatically (e.g., "Low Alcohol Screw Cap" → attribute pattern)
- Learn retailer-specific naming conventions per category
- Auto-suggest consolidations for new products matching known patterns

**Cost:** One-time per consolidation, amortized
**Handles:** Future duplicates without human review

---

## Implementation Roadmap

### Phase 1: Semantic Matching (this sprint)
1. Add `sentence-transformers` to `requirements.txt`
2. Create `similarity_deduplication.py` with:
   - `ProductEmbeddingCache`: Store/retrieve product embeddings
   - `find_similar_products()`: Query for duplicates above threshold
   - `consolidate_similar_products()`: Create migrations
3. Add CLI: `python scraper.py --deduplicate` (post-scrape)
4. Store embedding results in DB for audit trail

### Phase 2: Pattern Learning (next sprint)
1. Create `consolidation_patterns.json`:
   ```json
   {
     "patterns": [
       {
         "id": "low_alcohol_modifier",
         "regex": "(?i)\\s+low\\s+alcohol",
         "category": "wine",
         "confidence": 0.95,
         "discovered_from": ["consolidation_697040e"],
         "action": "remove"
       }
     ],
     "retailer_conventions": {
       "woolworths": {
         "category": "wine",
         "adds_attributes": ["alcohol_level", "closure_type"]
       }
     }
   }
   ```
2. Auto-extract patterns from consolidation data
3. Generate new normalization rules dynamically

### Phase 3: Admin Interface (future)
- Dashboard to review pending consolidations
- Manual override/rejection mechanism
- Analytics: consolidation success rate, false positives

---

## Architecture

```
scraper.py
    ↓
[Extract product data]
    ↓
Layer 1: _normalize_product_name() ← Current
    ↓
_product_key() → DB INSERT
    ↓
[Periodic batch job or on-demand]
    ↓
Layer 2: find_similar_products() ← NEW
    ├─ Load embeddings cache
    ├─ Vectorize norm_name per category
    ├─ Calculate pairwise similarity
    ├─ Group by threshold (0.85+)
    └─ Output consolidation plan
    ↓
[Admin review / auto-consolidate]
    ↓
Layer 3: Extract patterns from consolidations ← NEW
    ├─ Analyze what was merged
    ├─ Identify common modifiers
    ├─ Update consolidation_patterns.json
    └─ Feed back to Layer 1 & 2
```

---

## Code Examples

### Layer 2: Semantic Deduplication
```python
from sentence_transformers import SentenceTransformer
import numpy as np

class ProductDeduplicator:
    def __init__(self):
        self.model = SentenceTransformer('all-MiniLM-L6-v2')  # Fast, lightweight
        self.embedding_cache = {}  # product_key → embedding
    
    def find_similar_products(self, category: str, threshold: float = 0.85):
        """Find product groups with semantic similarity > threshold"""
        products = db.query(f"""
            SELECT product_key, product_name, normalized_name
            FROM products
            WHERE category = ? AND active = true
        """, (category,))
        
        embeddings = [
            self.model.encode(p['normalized_name'], convert_to_numpy=True)
            for p in products
        ]
        
        groups = []
        for i in range(len(embeddings)):
            for j in range(i + 1, len(embeddings)):
                sim = np.dot(embeddings[i], embeddings[j])
                if sim > threshold:
                    groups.append({
                        'similarity': float(sim),
                        'product_a': products[i]['product_key'],
                        'product_b': products[j]['product_key'],
                        'name_a': products[i]['product_name'],
                        'name_b': products[j]['product_name'],
                    })
        
        return sorted(groups, key=lambda x: x['similarity'], reverse=True)
```

### Layer 3: Pattern Extraction
```python
def extract_consolidation_patterns(consolidations: list[dict]):
    """Learn from past consolidations to improve future normalization"""
    patterns = {
        'modifiers': {},
        'retailer_conventions': {},
    }
    
    for consol in consolidations:
        removed = consol['source_name'] - consol['canonical_name']
        if removed:
            # Extract what was removed to identify modifier patterns
            pattern = extract_pattern(removed)
            patterns['modifiers'][pattern] = patterns['modifiers'].get(pattern, 0) + 1
    
    # Return high-confidence patterns (appeared 3+ times)
    return {k: v for k, v in patterns['modifiers'].items() if v >= 3}
```

---

## Configuration

**`deduplication_config.yaml`:**
```yaml
embedding_model: "all-MiniLM-L6-v2"
similarity_threshold: 0.85
cache_embeddings: true
cache_dir: ".embeddings_cache"

patterns_config: "consolidation_patterns.json"
consolidation_history: "consolidations_log.json"

# Per-category settings (if needed)
categories:
  wine:
    threshold: 0.87  # Higher threshold for wine (subtle variants)
    min_confidence_to_apply_pattern: 0.90
  beverages:
    threshold: 0.85
```

---

## Benefits

✅ **Flexible:** Works for any product category (wine, groceries, etc.)
✅ **Generic:** No hardcoded lists—learns from data
✅ **Scalable:** Batch process doesn't block scraper
✅ **Auditable:** All consolidations logged with confidence scores
✅ **Safe:** Starts with suggestions, manual review gate
✅ **Self-improving:** Patterns extracted from consolidations
✅ **Maintainable:** Non-technical admin can review/override

---

## Next Steps

1. Review this strategy with team
2. Choose implementation order (Phase 1 → Phase 2 → Phase 3)
3. Set up test environment with sample duplicates
4. Implement Layer 2 semantic matching first (highest ROI)
