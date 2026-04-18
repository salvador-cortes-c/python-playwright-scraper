"""
Product Semantic Deduplication

Layer 2: Post-scraping deduplication using embeddings
- Groups similar product names using sentence embeddings
- Works across retailers and categories
- Learns from past consolidations
"""

import json
import hashlib
from pathlib import Path
from typing import Optional
import numpy as np
from sentence_transformers import SentenceTransformer
import psycopg
from dataclasses import dataclass, asdict


EMBEDDING_CACHE_DIR = Path(".embeddings_cache")
CONSOLIDATION_LOG = Path("consolidations_log.json")
PATTERNS_FILE = Path("consolidation_patterns.json")

# Lightweight model suitable for CPU-only environments
EMBEDDING_MODEL = "all-MiniLM-L6-v2"
DEFAULT_SIMILARITY_THRESHOLD = 0.85


@dataclass
class SimilarProductGroup:
    """Group of similar products found by embedding comparison"""
    similarity: float
    product_a: str  # product_key
    product_b: str  # product_key
    name_a: str
    name_b: str
    category: Optional[str] = None
    explanation: Optional[str] = None  # Why they're similar


@dataclass
class ConsolidationLog:
    """Record of a product consolidation"""
    source_product_key: str
    canonical_product_key: str
    source_name: str
    canonical_name: str
    similarity_score: float
    method: str  # "semantic", "manual", "pattern"
    timestamp: str
    snapshots_migrated: int = 0


class ProductEmbeddingCache:
    """Local cache for product embeddings to avoid recomputing"""

    def __init__(self, cache_dir: Path = EMBEDDING_CACHE_DIR):
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(exist_ok=True)
        self.memory_cache = {}

    def _get_cache_path(self, product_key: str) -> Path:
        """Hash product_key to filename"""
        hash_val = hashlib.md5(product_key.encode()).hexdigest()
        return self.cache_dir / f"{hash_val}.npy"

    def get(self, product_key: str) -> Optional[np.ndarray]:
        """Load embedding from cache"""
        if product_key in self.memory_cache:
            return self.memory_cache[product_key]

        cache_path = self._get_cache_path(product_key)
        if cache_path.exists():
            embedding = np.load(cache_path)
            self.memory_cache[product_key] = embedding
            return embedding
        return None

    def set(self, product_key: str, embedding: np.ndarray) -> None:
        """Save embedding to cache"""
        self.memory_cache[product_key] = embedding
        cache_path = self._get_cache_path(product_key)
        np.save(cache_path, embedding)


class ProductDeduplicator:
    """Semantic similarity-based product deduplication"""

    def __init__(
        self,
        db_url: str,
        threshold: float = DEFAULT_SIMILARITY_THRESHOLD,
        model_name: str = EMBEDDING_MODEL,
    ):
        self.db_url = db_url
        self.threshold = threshold
        self.embedding_cache = ProductEmbeddingCache()
        print(f"Loading embedding model: {model_name}")
        self.model = SentenceTransformer(model_name)
        self.consolidations = self._load_consolidation_log()

    def _load_consolidation_log(self) -> list[dict]:
        """Load history of past consolidations"""
        if CONSOLIDATION_LOG.exists():
            with open(CONSOLIDATION_LOG) as f:
                return json.load(f)
        return []

    def _save_consolidation_log(self) -> None:
        """Persist consolidation history"""
        with open(CONSOLIDATION_LOG, "w") as f:
            json.dump(self.consolidations, f, indent=2)

    def _get_embedding(self, text: str, product_key: str) -> np.ndarray:
        """Get embedding from cache or compute"""
        cached = self.embedding_cache.get(product_key)
        if cached is not None:
            return cached

        embedding = self.model.encode(text, convert_to_numpy=True)
        self.embedding_cache.set(product_key, embedding)
        return embedding

    def find_similar_products(
        self,
        category: Optional[str] = None,
        retailer: Optional[str] = None,
        threshold: Optional[float] = None,
    ) -> list[SimilarProductGroup]:
        """
        Find products with semantic similarity above threshold.

        Args:
            category: Filter by product category (e.g., "wine", "beverages")
            retailer: Filter by retailer scope
            threshold: Override default similarity threshold (0.0-1.0)

        Returns:
            List of similar product groups, sorted by similarity descending
        """
        if threshold is None:
            threshold = self.threshold

        try:
            with psycopg.connect(self.db_url) as conn:
                with conn.cursor(row_factory=dict) as cur:
                    # Query for products in scope (using actual schema columns)
                    query = """
                        SELECT DISTINCT
                            p.product_key,
                            p.name,
                            p.packaging_format
                        FROM products p
                    """
                    params = []

                    # Optional: filter by category if needed
                    if category:
                        query += """
                            INNER JOIN product_categories pc ON p.product_key = pc.product_key
                            INNER JOIN categories c ON pc.category_id = c.id
                            WHERE c.name ILIKE %s
                        """
                        params.append(f"%{category}%")

                    query += " ORDER BY p.product_key"

                    cur.execute(query, params)
                    products = cur.fetchall()

                    if len(products) < 2:
                        return []

                    # Compute embeddings
                    embeddings = []
                    for p in products:
                        # Use product name for embedding
                        text = p["name"]
                        embedding = self._get_embedding(text, p["product_key"])
                        embeddings.append(embedding)

                    # Find pairs above threshold
                    groups: list[SimilarProductGroup] = []
                    for i in range(len(embeddings)):
                        for j in range(i + 1, len(embeddings)):
                            # Cosine similarity
                            sim = float(
                                np.dot(embeddings[i], embeddings[j])
                                / (
                                    np.linalg.norm(embeddings[i])
                                    * np.linalg.norm(embeddings[j])
                                )
                            )

                            if sim >= threshold:
                                # Skip already-consolidated pairs
                                if self._is_already_consolidated(
                                    products[i]["product_key"],
                                    products[j]["product_key"],
                                ):
                                    continue

                                groups.append(
                                    SimilarProductGroup(
                                        similarity=sim,
                                        product_a=products[i]["product_key"],
                                        product_b=products[j]["product_key"],
                                        name_a=products[i]["name"],
                                        name_b=products[j]["name"],
                                        explanation=self._explain_similarity(
                                            products[i]["name"],
                                            products[j]["name"],
                                        ),
                                    )
                                )

                    return sorted(groups, key=lambda x: x.similarity, reverse=True)

        except Exception as e:
            print(f"Error finding similar products: {e}")
            raise

    def _is_already_consolidated(self, key_a: str, key_b: str) -> bool:
        """Check if these products were already consolidated"""
        for consol in self.consolidations:
            if (
                consol["source_product_key"] == key_a
                and consol["canonical_product_key"] == key_b
            ) or (
                consol["source_product_key"] == key_b
                and consol["canonical_product_key"] == key_a
            ):
                return True
        return False

    def _explain_similarity(self, name_a: str, name_b: str) -> str:
        """Generate human-readable explanation of why products are similar"""
        # Simple heuristic: find common tokens
        tokens_a = set(name_a.lower().split())
        tokens_b = set(name_b.lower().split())
        common = tokens_a & tokens_b

        if len(common) / max(len(tokens_a), len(tokens_b)) > 0.7:
            return f"High token overlap: {', '.join(sorted(common)[:5])}"

        return "Semantic embedding similarity"

    def generate_consolidation_migration(
        self,
        source_key: str,
        canonical_key: str,
        similarity: float,
        method: str = "semantic",
    ) -> str:
        """
        Generate SQL migration to consolidate products.

        Args:
            source_key: Product to consolidate FROM
            canonical_key: Product to consolidate TO
            similarity: Confidence score (0.0-1.0)
            method: Consolidation method ("semantic", "manual", "pattern")

        Returns:
            SQL migration script as string
        """
        return f"""
-- Consolidate similar products
-- Similarity: {similarity:.3f} (method: {method})
-- Source: {source_key}
-- Canonical: {canonical_key}

BEGIN;

-- Log the consolidation
INSERT INTO consolidation_log (
    source_product_key, canonical_product_key, method, similarity_score
) VALUES (
    '{source_key}', '{canonical_key}', '{method}', {similarity}
);

-- Redirect snapshots
UPDATE price_snapshots
SET product_key = '{canonical_key}'
WHERE product_key = '{source_key}';

-- Redirect categories
UPDATE product_categories
SET product_key = '{canonical_key}'
WHERE product_key = '{source_key}';

-- Delete orphaned product
DELETE FROM products WHERE product_key = '{source_key}';

COMMIT;
"""

    def log_consolidation(
        self,
        source_key: str,
        canonical_key: str,
        source_name: str,
        canonical_name: str,
        similarity: float,
        method: str,
        snapshots_migrated: int = 0,
    ) -> None:
        """Log a consolidation for future pattern learning"""
        from datetime import datetime

        entry = {
            "source_product_key": source_key,
            "canonical_product_key": canonical_key,
            "source_name": source_name,
            "canonical_name": canonical_name,
            "similarity_score": similarity,
            "method": method,
            "timestamp": datetime.utcnow().isoformat(),
            "snapshots_migrated": snapshots_migrated,
        }
        self.consolidations.append(entry)
        self._save_consolidation_log()

    def extract_patterns(self) -> dict:
        """
        Learn patterns from consolidations to improve future deduplication.

        Returns:
            Dictionary of discovered patterns (for analysis/updating normalization)
        """
        patterns = {
            "common_removed_modifiers": {},
            "retailer_conventions": {},
            "category_patterns": {},
        }

        for consol in self.consolidations:
            if consol["method"] != "manual":
                continue  # Only learn from manually verified consolidations

            source = consol["source_name"].lower()
            canonical = consol["canonical_name"].lower()

            # Find what was removed between source and canonical
            source_tokens = set(source.split())
            canonical_tokens = set(canonical.split())
            removed = source_tokens - canonical_tokens

            if removed:
                removed_str = " ".join(sorted(removed))
                patterns["common_removed_modifiers"][removed_str] = (
                    patterns["common_removed_modifiers"].get(removed_str, 0) + 1
                )

            category = consol.get("category", "unknown")
            if category not in patterns["category_patterns"]:
                patterns["category_patterns"][category] = 0
            patterns["category_patterns"][category] += 1

        # Filter for high-confidence patterns (appeared 3+ times)
        patterns["high_confidence_modifiers"] = {
            k: v
            for k, v in patterns["common_removed_modifiers"].items()
            if v >= 3
        }

        return patterns


def main():
    """CLI: Find and display similar products"""
    import argparse
    import os

    parser = argparse.ArgumentParser(description="Find similar products")
    parser.add_argument(
        "--db-url", default=os.getenv("DATABASE_URL"), help="PostgreSQL URL"
    )
    parser.add_argument(
        "--category", help="Filter by category (e.g., 'wine')"
    )
    parser.add_argument(
        "--threshold", type=float, default=DEFAULT_SIMILARITY_THRESHOLD,
        help="Similarity threshold (0.0-1.0)"
    )
    parser.add_argument(
        "--export-migration",
        action="store_true",
        help="Export consolidation suggestions as SQL migrations"
    )
    parser.add_argument(
        "--extract-patterns",
        action="store_true",
        help="Analyze past consolidations to extract patterns"
    )

    args = parser.parse_args()

    if not args.db_url:
        print("ERROR: DATABASE_URL not set")
        return 1

    dedup = ProductDeduplicator(args.db_url, threshold=args.threshold)

    if args.extract_patterns:
        patterns = dedup.extract_patterns()
        print("\n=== Learned Patterns ===")
        print(json.dumps(patterns, indent=2))
        return 0

    # Find similar products
    groups = dedup.find_similar_products(category=args.category)

    if not groups:
        print("No similar products found above threshold.")
        return 0

    print(f"\n=== Found {len(groups)} Similar Product Groups ===\n")

    for idx, group in enumerate(groups, 1):
        print(f"{idx}. Similarity: {group.similarity:.3f}")
        print(f"   A: {group.product_a} → {group.name_a}")
        print(f"   B: {group.product_b} → {group.name_b}")
        print(f"   Why: {group.explanation}")

        if args.export_migration:
            migration = dedup.generate_consolidation_migration(
                source_key=group.product_b,
                canonical_key=group.product_a,
                similarity=group.similarity,
                method="semantic",
            )
            print(f"\n   SQL Migration:\n{migration}\n")
        print()

    return 0


if __name__ == "__main__":
    exit(main())
