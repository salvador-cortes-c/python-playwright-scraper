import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from database import _build_category_lookup, _collect_category_rows, _find_category_id_for_source_url


class DatabasePersistenceTests(unittest.TestCase):
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
