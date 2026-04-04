import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from database import (
    _build_category_lookup,
    _collect_category_rows,
    _find_category_id_for_source_url,
    dedupe_price_snapshots,
)


class DatabasePersistenceTests(unittest.TestCase):
    def test_dedupe_price_snapshots_collapses_duplicates_from_same_category_run(self):
        class Snapshot:
            def __init__(self, product_key, supermarket_name, source_url, scraped_at, price='4.99'):
                self.product_key = product_key
                self.supermarket_name = supermarket_name
                self.source_url = source_url
                self.scraped_at = scraped_at
                self.price = price
                self.unit_price = '$4.99/kg'
                self.promo_price = ''
                self.promo_unit_price = ''

        snapshots = [
            Snapshot(
                'abc__1kg',
                'New World Karori',
                'https://www.newworld.co.nz/shop/category/pantry?pg=1',
                '2026-04-04T06:00:00+00:00',
            ),
            Snapshot(
                'abc__1kg',
                'New World Karori',
                'https://www.newworld.co.nz/shop/category/pantry?pg=4',
                '2026-04-04T06:00:00.200000+00:00',
            ),
        ]

        deduped = dedupe_price_snapshots(snapshots)

        self.assertEqual(len(deduped), 1)

    def test_dedupe_price_snapshots_keeps_distinct_store_or_price_rows(self):
        class Snapshot:
            def __init__(self, product_key, supermarket_name, price):
                self.product_key = product_key
                self.supermarket_name = supermarket_name
                self.source_url = 'https://www.newworld.co.nz/shop/category/pantry?pg=1'
                self.scraped_at = '2026-04-04T06:00:00+00:00'
                self.price = price
                self.unit_price = '$4.99/kg'
                self.promo_price = ''
                self.promo_unit_price = ''

        snapshots = [
            Snapshot('abc__1kg', 'New World Karori', '4.99'),
            Snapshot('abc__1kg', 'New World Metro', '4.99'),
            Snapshot('abc__1kg', 'New World Karori', '5.49'),
        ]

        deduped = dedupe_price_snapshots(snapshots)

        self.assertEqual(len(deduped), 3)

    def test_collect_category_rows_can_infer_category_from_snapshot_url(self):
        class Snapshot:
            source_url = "https://www.newworld.co.nz/shop/category/snacks-treats-and-easy-meals?pg=7"

        rows = _collect_category_rows([], [Snapshot()])

        self.assertEqual(
            rows,
            [
                (
                    "Snacks Treats And Easy Meals",
                    "https://www.newworld.co.nz/shop/category/snacks-treats-and-easy-meals?pg=1",
                    "https://www.newworld.co.nz/shop/category/snacks-treats-and-easy-meals?pg=7",
                )
            ],
        )

    def test_find_category_id_matches_paginated_category_pages(self):
        lookup = _build_category_lookup([
            (11, "https://www.newworld.co.nz/shop/category/pantry?pg=1"),
        ])

        category_id = _find_category_id_for_source_url(
            lookup,
            "https://www.newworld.co.nz/shop/category/pantry?pg=4",
        )

        self.assertEqual(category_id, 11)

    def test_find_category_id_matches_even_when_category_url_has_no_pg_parameter(self):
        lookup = _build_category_lookup([
            (22, "https://www.newworld.co.nz/shop/category/bakery"),
        ])

        category_id = _find_category_id_for_source_url(
            lookup,
            "https://www.newworld.co.nz/shop/category/bakery?pg=3",
        )

        self.assertEqual(category_id, 22)

    def test_find_category_id_ignores_non_category_urls(self):
        lookup = _build_category_lookup([
            (33, "https://www.newworld.co.nz/shop/category/frozen?pg=1"),
        ])

        category_id = _find_category_id_for_source_url(
            lookup,
            "https://www.newworld.co.nz/shop/product/12345_ea_000nw?name=frozen-peas",
        )

        self.assertIsNone(category_id)


if __name__ == "__main__":
    unittest.main()
