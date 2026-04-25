"""
Integration hook for semantic deduplication into scraper.py

This module provides ready-to-use functions that can be called from scraper.py
after products are scraped and inserted into the database.
"""

import os
import asyncio
from datetime import datetime
from pathlib import Path
from typing import Optional
import logging

from similarity_deduplication import ProductDeduplicator

logger = logging.getLogger(__name__)


class DeduplicationIntegration:
    """Wrapper for scraper integration with sensible defaults."""

    def __init__(
        self,
        db_url: Optional[str] = None,
        auto_consolidate_threshold: float = 0.95,
        review_threshold: float = 0.85,
        export_dir: Path = Path("."),
    ):
        """
        Initialize deduplication integration.

        Args:
            db_url: Database URL (defaults to DATABASE_URL env var)
            auto_consolidate_threshold: Auto-consolidate above this similarity
            review_threshold: Export for review above this threshold
            export_dir: Directory to export suggestion files
        """
        self.db_url = db_url or os.getenv("DATABASE_URL")
        if not self.db_url:
            raise ValueError(
                "DATABASE_URL not set. Set via env var or pass db_url parameter."
            )

        self.auto_consolidate_threshold = auto_consolidate_threshold
        self.review_threshold = review_threshold
        self.export_dir = Path(export_dir)
        self.dedup = ProductDeduplicator(self.db_url)

    async def run_post_scrape(
        self,
        category: Optional[str] = None,
        auto_consolidate: bool = True,
        export_suggestions: bool = True,
        export_patterns: bool = True,
    ) -> dict:
        """
        Run deduplication after scraper completes.

        Args:
            category: Deduplicate specific category only (e.g., "wine")
            auto_consolidate: Auto-consolidate high-confidence matches
            export_suggestions: Export mid-confidence matches for review
            export_patterns: Extract and save learned patterns

        Returns:
            Dictionary with results:
            {
                'total_groups': int,
                'auto_consolidated': int,
                'pending_review': int,
                'exported_files': list[str],
            }
        """
        logger.info(
            f"[Dedup] Starting post-scrape deduplication (category={category})"
        )

        results = {
            "total_groups": 0,
            "auto_consolidated": 0,
            "pending_review": 0,
            "exported_files": [],
            "errors": [],
        }

        try:
            # Find similar products
            groups = self.dedup.find_similar_products(
                category=category, threshold=self.review_threshold
            )

            if not groups:
                logger.info("[Dedup] No duplicates detected")
                return results

            results["total_groups"] = len(groups)
            logger.info(f"[Dedup] Found {len(groups)} potential duplicates")

            # Categorize by confidence
            auto = [g for g in groups if g.similarity >= self.auto_consolidate_threshold]
            review = [
                g
                for g in groups
                if self.review_threshold <= g.similarity < self.auto_consolidate_threshold
            ]

            # Auto-consolidate high-confidence
            if auto_consolidate and auto:
                logger.info(f"[Dedup] Auto-consolidating {len(auto)} high-confidence matches")
                for idx, group in enumerate(auto, 1):
                    try:
                        logger.debug(
                            f"[Dedup]   {idx}/{len(auto)}: "
                            f"{group.product_b} → {group.product_a} ({group.similarity:.3f})"
                        )

                        # Execute consolidation directly against the database
                        result = self.dedup.execute_consolidation(
                            source_key=group.product_b,
                            canonical_key=group.product_a,
                            similarity=group.similarity,
                            method="semantic_auto",
                        )

                        logger.info(
                            f"[Dedup]   Consolidated {group.product_b} → {group.product_a}: "
                            f"{result['snapshots_migrated']} snapshots, "
                            f"{result['categories_migrated']} categories migrated"
                        )

                        # Also update local JSON log
                        self.dedup.log_consolidation(
                            source_key=group.product_b,
                            canonical_key=group.product_a,
                            source_name=group.name_b,
                            canonical_name=group.name_a,
                            similarity=group.similarity,
                            method="semantic",
                            snapshots_migrated=result["snapshots_migrated"],
                        )

                        results["auto_consolidated"] += 1

                    except Exception as e:
                        msg = f"Failed to consolidate {group.product_b}: {e}"
                        logger.error(f"[Dedup] {msg}")
                        results["errors"].append(msg)

            # Export mid-confidence for review
            if export_suggestions and review:
                export_file = self._export_review_suggestions(review, category)
                logger.info(
                    f"[Dedup] Exported {len(review)} suggestions for review to {export_file}"
                )
                results["exported_files"].append(str(export_file))
                results["pending_review"] = len(review)

            # Extract patterns from consolidations
            if export_patterns:
                patterns_file = self._extract_and_save_patterns()
                logger.info(f"[Dedup] Extracted patterns to {patterns_file}")
                results["exported_files"].append(str(patterns_file))

        except Exception as e:
            logger.error(f"[Dedup] Error during deduplication: {e}")
            results["errors"].append(str(e))

        return results

    def _export_review_suggestions(self, groups: list, category: Optional[str]) -> Path:
        """Export mid-confidence suggestions as SQL migrations."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        category_suffix = f"_{category}" if category else ""
        filename = f"consolidation_suggestions_{timestamp}{category_suffix}.sql"
        filepath = self.export_dir / filename

        logger.debug(f"[Dedup] Exporting {len(groups)} suggestions to {filepath}")

        with open(filepath, "w") as f:
            f.write(f"-- Consolidation suggestions generated {datetime.now().isoformat()}\n")
            f.write(f"-- Total suggestions: {len(groups)}\n")
            f.write(f"-- Category: {category or 'all'}\n\n")

            for idx, group in enumerate(groups, 1):
                f.write(
                    f"-- #{idx} Similarity: {group.similarity:.3f}\n"
                    f"-- {group.name_a} ↔ {group.name_b}\n"
                )
                migration = self.dedup.generate_consolidation_migration(
                    source_key=group.product_b,
                    canonical_key=group.product_a,
                    similarity=group.similarity,
                    method="semantic_review",
                )
                f.write(migration + "\n\n")

        logger.info(f"[Dedup] Exported suggestions to {filepath}")
        return filepath

    def _extract_and_save_patterns(self) -> Path:
        """Extract patterns from consolidations and save to JSON."""
        import json

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"dedup_patterns_{timestamp}.json"
        filepath = self.export_dir / filename

        patterns = self.dedup.extract_patterns()

        with open(filepath, "w") as f:
            json.dump(patterns, f, indent=2)

        logger.info(f"[Dedup] Saved patterns to {filepath}")
        return filepath


# ============================================================================
# Usage Examples
# ============================================================================

async def example_post_scrape_integration():
    """
    Example: How to integrate deduplication into scraper.py main()

    In scraper.py, after inserting products to DB:

    ```python
    from scraper_deduplication_integration import DeduplicationIntegration

    async def main():
        # ... existing scraper code ...
        # After: await insert_products_to_db(products)

        if not args.skip_deduplication:
            dedup = DeduplicationIntegration()
            results = await dedup.run_post_scrape(
                category=args.category or None,
                auto_consolidate=True,
                export_suggestions=True,
                export_patterns=True,
            )

            print(f"Deduplication results:")
            print(f"  Found: {results['total_groups']} similar product groups")
            print(f"  Auto-consolidated: {results['auto_consolidated']}")
            print(f"  Pending review: {results['pending_review']}")
            if results['exported_files']:
                print(f"  Files: {', '.join(results['exported_files'])}")
    ```
    """
    print("See code comments for integration example")


# ============================================================================
# CLI: Standalone usage
# ============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Semantic deduplication integration for scraper"
    )
    parser.add_argument(
        "--category", help="Deduplicate specific category (e.g., wine)"
    )
    parser.add_argument(
        "--auto-threshold",
        type=float,
        default=0.95,
        help="Similarity threshold for auto-consolidation (default: 0.95)",
    )
    parser.add_argument(
        "--review-threshold",
        type=float,
        default=0.85,
        help="Similarity threshold for review exports (default: 0.85)",
    )
    parser.add_argument(
        "--no-auto", action="store_true", help="Disable auto-consolidation"
    )
    parser.add_argument(
        "--no-export", action="store_true", help="Disable export of suggestions"
    )

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Run integration
    integration = DeduplicationIntegration(
        auto_consolidate_threshold=args.auto_threshold,
        review_threshold=args.review_threshold,
    )

    results = asyncio.run(
        integration.run_post_scrape(
            category=args.category,
            auto_consolidate=not args.no_auto,
            export_suggestions=not args.no_export,
            export_patterns=True,
        )
    )

    print("\n=== Deduplication Results ===")
    print(f"Total groups found: {results['total_groups']}")
    print(f"Auto-consolidated: {results['auto_consolidated']}")
    print(f"Pending review: {results['pending_review']}")
    if results["exported_files"]:
        print(f"\nExported files:")
        for file in results["exported_files"]:
            print(f"  - {file}")
    if results["errors"]:
        print(f"\nErrors:")
        for error in results["errors"]:
            print(f"  - {error}")
