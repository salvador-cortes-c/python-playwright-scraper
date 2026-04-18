"""
Integration Patterns: How to hook semantic deduplication into scraper.py

Choose the pattern that best fits your workflow.
"""

# ============================================================================
# PATTERN 1: Post-Scrape Batch (Recommended for safety)
# ============================================================================

# Scraper runs, saves all products to DB
# Then run deduplication as separate step

# In your main runner script or GitHub Actions:
"""
#!/bin/bash

# Step 1: Scrape
python scraper.py --retailers woolworths new_world --store-name Karori

# Step 2: Deduplicate (separate process, can review first)
python similarity_deduplication.py \
    --category wine \
    --export-migration > consolidation_suggestions.sql

# Step 3: Review suggestions
# git diff consolidation_suggestions.sql

# Step 4: Apply consolidations
# psql $DATABASE_URL < consolidation_suggestions.sql
"""


# ============================================================================
# PATTERN 2: Inline Integration (During scrape)
# ============================================================================

# Add to scraper.py after product insertion:

example_inline = """
import asyncio
from similarity_deduplication import ProductDeduplicator

async def deduplicate_products(db_url: str, category: str = None):
    \"\"\"Run deduplication after scraper completes.\"\"\"
    dedup = ProductDeduplicator(db_url, threshold=0.85)
    
    groups = dedup.find_similar_products(category=category)
    
    if not groups:
        print("✓ No duplicates detected")
        return
    
    print(f"Found {len(groups)} potential duplicates")
    
    # Auto-consolidate high-confidence matches
    auto_count = 0
    for group in groups:
        if group.similarity > 0.95:
            migration = dedup.generate_consolidation_migration(
                source_key=group.product_b,
                canonical_key=group.product_a,
                similarity=group.similarity,
                method="semantic"
            )
            # Execute migration
            exec_migration(migration)
            dedup.log_consolidation(
                source_key=group.product_b,
                canonical_key=group.product_a,
                source_name=group.name_b,
                canonical_name=group.name_a,
                similarity=group.similarity,
                method="semantic"
            )
            auto_count += 1
    
    print(f"✓ Auto-consolidated {auto_count} high-confidence duplicates")
    
    # Export mid-confidence for review
    review_groups = [g for g in groups if 0.85 <= g.similarity <= 0.95]
    if review_groups:
        print(f"⚠ Review {len(review_groups)} medium-confidence duplicates:")
        for group in review_groups[:5]:  # Show first 5
            print(f"  - {group.name_a} ↔ {group.name_b} ({group.similarity:.2f})")

# In main():
# if args.deduplicate:
#     await deduplicate_products(db_url, category=args.category)
"""


# ============================================================================
# PATTERN 3: Incremental with Feedback Loop
# ============================================================================

# For learning from consolidations over time

example_learning = """
from similarity_deduplication import ProductDeduplicator
import subprocess
import os

async def smart_deduplicate(db_url: str, auto_threshold: float = 0.95):
    \"\"\"
    Adaptive deduplication:
    1. Auto-consolidate high-confidence matches
    2. Learn patterns from consolidations
    3. Apply learned patterns to future runs
    \"\"\"
    dedup = ProductDeduplicator(db_url)
    
    # Extract patterns from previous consolidations
    patterns = dedup.extract_patterns()
    
    high_confidence_modifiers = patterns.get('high_confidence_modifiers', {})
    
    if high_confidence_modifiers:
        print(f"Learned {len(high_confidence_modifiers)} modifier patterns:")
        for modifier, count in sorted(
            high_confidence_modifiers.items(),
            key=lambda x: x[1],
            reverse=True
        )[:5]:
            print(f"  - '{modifier}' (seen {count} times)")
    
    # Find duplicates
    groups = dedup.find_similar_products()
    
    if not groups:
        return
    
    print(f"\\nFound {len(groups)} potential duplicates")
    
    # Categorize by confidence
    auto = [g for g in groups if g.similarity >= auto_threshold]
    review = [g for g in groups if 0.85 <= g.similarity < auto_threshold]
    
    # Auto-consolidate
    for group in auto:
        exec_consolidation(
            source=group.product_b,
            canonical=group.product_a,
            method="semantic_auto"
        )
        dedup.log_consolidation(
            source_key=group.product_b,
            canonical_key=group.product_a,
            source_name=group.name_b,
            canonical_name=group.name_a,
            similarity=group.similarity,
            method="semantic"
        )
    
    print(f"✓ Auto-consolidated {len(auto)} duplicates")
    
    # Export review list
    if review:
        export_review_list(review)
        print(f"⚠ Exported {len(review)} items for manual review")

# Usage:
# await smart_deduplicate(os.getenv("DATABASE_URL"), auto_threshold=0.95)
"""


# ============================================================================
# PATTERN 4: Category-Aware with Store Scope
# ============================================================================

# Run deduplication scoped to specific category + retailer

example_scoped = """
async def deduplicate_by_category_and_store(
    db_url: str,
    category: str,
    store_name: str = None
) -> dict:
    \"\"\"Deduplicate products in specific scope.\"\"\"
    dedup = ProductDeduplicator(db_url)
    
    # Scope to category (and optionally store)
    query = f"SELECT * FROM products WHERE category = '{category}'"
    if store_name:
        query += f" AND store_name ILIKE '%{store_name}%'"
    
    groups = dedup.find_similar_products(category=category)
    
    results = {
        'category': category,
        'store': store_name,
        'total_checked': len(groups),
        'auto_consolidated': 0,
        'pending_review': len([g for g in groups if g.similarity < 0.95]),
    }
    
    for group in groups:
        if group.similarity > 0.95:
            exec_consolidation(group.product_b, group.product_a)
            results['auto_consolidated'] += 1
    
    return results

# Usage per scraper run:
# for category in ['wine', 'beverages', 'spirits']:
#     results = await deduplicate_by_category_and_store(
#         db_url, category, store_name='Karori'
#     )
#     print(f"{results['category']}: {results['auto_consolidated']} consolidated")
"""


# ============================================================================
# PATTERN 5: Admin Dashboard Integration (Future)
# ============================================================================

# For web-based review and consolidation approvals

example_dashboard = """
# From a FastAPI/Flask endpoint:

from fastapi import FastAPI, HTTPException
from similarity_deduplication import ProductDeduplicator

app = FastAPI()

@app.get("/api/dedup/suggestions")
async def get_dedup_suggestions(category: str = None, threshold: float = 0.85):
    \"\"\"List pending consolidation suggestions.\"\"\"
    dedup = ProductDeduplicator(os.getenv("DATABASE_URL"))
    
    groups = dedup.find_similar_products(category=category, threshold=threshold)
    
    # Convert to JSON-serializable format
    suggestions = [
        {
            'product_a': g.product_a,
            'product_b': g.product_b,
            'name_a': g.name_a,
            'name_b': g.name_b,
            'similarity': g.similarity,
            'explanation': g.explanation,
        }
        for g in groups
    ]
    
    return {'count': len(suggestions), 'suggestions': suggestions}

@app.post("/api/dedup/approve")
async def approve_consolidation(source: str, canonical: str):
    \"\"\"Admin approves a consolidation.\"\"\"
    dedup = ProductDeduplicator(os.getenv("DATABASE_URL"))
    
    # Find the original group for similarity score
    migration = dedup.generate_consolidation_migration(
        source_key=source,
        canonical_key=canonical,
        similarity=0.90,  # Retrieve from DB if needed
        method="manual"
    )
    
    # Execute and log
    exec_migration(migration)
    
    return {'status': 'consolidated'}

# Usage:
# GET /api/dedup/suggestions?category=wine
# POST /api/dedup/approve?source=key_a&canonical=key_b
"""


# ============================================================================
# CLI COMMANDS For Easy Use
# ============================================================================

cli_commands = """
# After installation (pip install -r requirements.txt), use:

# 1. Find duplicates across all products
python similarity_deduplication.py

# 2. Find duplicates in wine category
python similarity_deduplication.py --category wine

# 3. Use stricter threshold (less false positives)
python similarity_deduplication.py --category wine --threshold 0.90

# 4. Export as SQL migrations for review
python similarity_deduplication.py --threshold 0.85 --export-migration > consolidations.sql

# 5. Learn patterns from past consolidations
python similarity_deduplication.py --extract-patterns

# 6. Integrated with scraper (if added to main)
python scraper.py --retailers woolworths --deduplicate
"""


# ============================================================================
# Summary: Choose Your Pattern
# ============================================================================

"""
PATTERN 1: Post-Scrape Batch (RECOMMENDED for first-time use)
  - Run scraper, then deduplicate separately
  - Allows manual review of suggestions
  - Safest for data integrity
  - Command: python similarity_deduplication.py --export-migration

PATTERN 2: Inline Integration
  - Deduplicate during each scraper run
  - Good for production pipelines
  - Requires confidence-based automation gates

PATTERN 3: Learning Loop
  - Analyzes past consolidations
  - Adapts to your data patterns
  - Good for long-term optimization

PATTERN 4: Scoped Deduplication
  - Run per-category or per-store
  - Useful for parallel scraping
  - Scales to multiple retailers

PATTERN 5: Dashboard (Future)
  - Web UI for consolidation review
  - Approval workflows
  - Audit trail

Start with Pattern 1 + Pattern 3 (export suggestions + learn patterns).
Move to Pattern 2 as confidence grows.
"""
