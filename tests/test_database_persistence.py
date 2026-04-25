import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from database import (
    _build_category_lookup,
    _collect_category_rows,
    _find_category_id_for_source_url,
    _infer_supermarket_name,
    _normalize_name_for_key,
    _normalize_packaging,
    _normalize_product_record,
    _parse_price_to_cents,
    _snapshot_store_name,
    dedupe_price_snapshots,
)


class DatabasePersistenceTests(unittest.TestCase):
    def test_normalize_product_record_infers_packaging_and_repairs_legacy_key(self):
        normalized = _normalize_product_record(
            "asahi beer super dry lager bottle 12x330ml__",
            "Asahi Beer Super Dry Lager Bottle 12x330mL",
            "",
        )

        self.assertEqual(
            normalized,
            (
                "asahi beer super dry lager bottle 12x330ml_12x330ml",
                "Asahi Beer Super Dry Lager Bottle 12x330mL",
                "12x330mL",
            ),
        )

    def test_normalize_product_record_rejects_price_only_names(self):
        self.assertIsNone(_normalize_product_record("$ 5 00", "$ 5 00", ""))

    def test_normalize_name_for_key_lowercases(self):
        self.assertEqual(_normalize_name_for_key("Heineken Lager"), "heineken lager")

    def test_normalize_name_for_key_replaces_word_connecting_hyphens_with_spaces(self):
        self.assertEqual(
            _normalize_name_for_key("Boundary Road Brewery Laid-Back Lager"),
            "boundary road brewery laid back lager",
        )

    def test_normalize_name_for_key_corrects_larger_misspelling(self):
        self.assertEqual(
            _normalize_name_for_key("Boundary Craft Beer Laid Back Larger"),
            "boundary craft beer laid back lager",
        )

    def test_normalize_name_for_key_handles_hyphen_and_misspelling_together(self):
        self.assertEqual(
            _normalize_name_for_key("Pale-Ale Larger"),
            "pale ale lager",
        )

    def test_normalize_product_record_uses_normalized_key_but_preserves_display_name(self):
        """Word-connecting hyphens are removed from the key; the display name is unchanged."""
        normalized = _normalize_product_record(
            "",
            "Boundary Road Brewery Laid-Back Lager Cans",
            "6x330ml",
        )
        self.assertEqual(
            normalized,
            (
                "boundary road brewery laid back lager cans_6x330ml",
                "Boundary Road Brewery Laid-Back Lager Cans",
                "6x330ml",
            ),
        )

    def test_normalize_product_record_corrects_misspelling_in_key_preserves_display_name(self):
        """Known misspellings are corrected in the key; the display name is unchanged."""
        normalized = _normalize_product_record(
            "",
            "Boundary Craft Beer Laid Back Larger",
            "",
        )
        self.assertIsNotNone(normalized)
        key, name, _ = normalized
        self.assertIn("lager", key)
        self.assertNotIn("larger", key)
        self.assertEqual(name, "Boundary Craft Beer Laid Back Larger")

    def test_cross_supermarket_same_product_name_produces_same_key(self):
        """PAK'nSAVE and New World list the same product under identical names.
        After normalization both should produce the exact same product_key so
        the database stores a single record, not two separate ones."""
        paknsave = _normalize_product_record(
            "",
            "Boundary Road Brewery Laid-Back Lager Cans 6 x 330ml",
            "6 x 330ml",
        )
        new_world = _normalize_product_record(
            "",
            "Boundary Road Brewery Laid-Back Lager Cans 6 x 330ml",
            "6 x 330ml",
        )

        self.assertIsNotNone(paknsave)
        self.assertIsNotNone(new_world)
        self.assertEqual(paknsave[0], new_world[0], "Same product should produce identical keys across supermarkets")

    def test_normalize_name_for_key_woolworths_larger_typo_produces_shared_base(self):
        """Woolworths uses the typo 'Larger' instead of 'Lager'.  After
        normalization the corrected key should share its 'lager' token with
        the correctly-spelled PAK'nSAVE / New World records so that a semantic
        deduplication pass can recognise them as the same product."""
        woolworths_key = _normalize_name_for_key("Boundary Craft Beer Laid Back Larger")
        paknsave_key   = _normalize_name_for_key("Boundary Road Brewery Laid-Back Lager Cans")

        # Both keys must contain 'lager' (not 'larger')
        self.assertIn("lager", woolworths_key)
        self.assertNotIn("larger", woolworths_key)
        self.assertIn("lager", paknsave_key)
        self.assertNotIn("larger", paknsave_key)
        # Word-connecting hyphen removed from Pak'nSave key
        self.assertNotIn("laid-back", paknsave_key)
        self.assertIn("laid back", paknsave_key)

    def test_normalize_packaging_removes_spaces_around_x(self):
        """Explicit packaging '6 x 330ml' and inferred '6x330ml' are equivalent."""
        self.assertEqual(_normalize_packaging("6 x 330ml"), "6x330ml")
        self.assertEqual(_normalize_packaging("12 x 330ml"), "12x330ml")
        self.assertEqual(_normalize_packaging("6x330ml"), "6x330ml")

    def test_normalize_packaging_removes_spaces_before_units(self):
        """Packaging '750 ml' normalizes to '750ml'."""
        self.assertEqual(_normalize_packaging("750 ml"), "750ml")
        self.assertEqual(_normalize_packaging("1.5 l"), "1.5l")
        self.assertEqual(_normalize_packaging("500g"), "500g")

    def test_cross_supermarket_explicit_vs_inferred_packaging_produces_same_key(self):
        """When one supermarket provides explicit packaging and another has the packaging
        only in the product name, both should produce the same product_key so that
        the database stores a single canonical record."""
        # PAKn'SAVE: packaging provided explicitly as '6 x 330ml' (with spaces)
        paknsave = _normalize_product_record(
            "",
            "Boundary Road Brewery Laid-Back Lager Cans 6 x 330ml",
            "6 x 330ml",
        )
        # New World: packaging not provided separately (inferred from name)
        new_world_inferred = _normalize_product_record(
            "",
            "Boundary Road Brewery Laid-Back Lager Cans 6 x 330ml",
            "",
        )

        self.assertIsNotNone(paknsave)
        self.assertIsNotNone(new_world_inferred)
        self.assertEqual(
            paknsave[0],
            new_world_inferred[0],
            "Explicit and inferred packaging should produce the same product_key",
        )

    def test_boundary_lager_three_supermarkets_deduplication_state(self):
        """Comprehensive deduplication check for the three cross-supermarket Boundary
        Road Lager products.

        - PAKn'SAVE and New World share the same full product name and packaging so
          they are deduplicated by normalization (same product_key).
        - Woolworths uses a different brand description and has no packaging info, so
          it produces a distinct key; it is a candidate for semantic deduplication
          (similarity_deduplication.py) rather than key-based deduplication.
        """
        paknsave = _normalize_product_record(
            "",
            "Boundary Road Brewery Laid-Back Lager Cans 6 x 330ml",
            "6 x 330ml",
        )
        new_world = _normalize_product_record(
            "",
            "Boundary Road Brewery Laid-Back Lager Cans 6 x 330ml",
            "6 x 330ml",
        )
        woolworths = _normalize_product_record(
            "",
            "Boundary Craft Beer Laid Back Larger",
            "",
        )

        self.assertIsNotNone(paknsave)
        self.assertIsNotNone(new_world)
        self.assertIsNotNone(woolworths)

        paknsave_key, _, _ = paknsave
        new_world_key, _, _ = new_world
        woolworths_key, woolworths_name, _ = woolworths

        # PAKn'SAVE and New World: same canonical product → identical key
        self.assertEqual(
            paknsave_key,
            new_world_key,
            "PAKn'SAVE and New World should share a product_key for the same product",
        )

        # Woolworths: different brand description → distinct key
        self.assertNotEqual(
            woolworths_key,
            paknsave_key,
            "Woolworths product has a different brand name and needs semantic deduplication",
        )

        # Woolworths key must have 'lager' corrected (not 'larger')
        self.assertIn("lager", woolworths_key)
        self.assertNotIn("larger", woolworths_key)

        # Woolworths display name is preserved unchanged (typo kept for display)
        self.assertEqual(woolworths_name, "Boundary Craft Beer Laid Back Larger")

    def test_infer_supermarket_name_from_store_or_url(self):
        self.assertEqual(_infer_supermarket_name(store_name="New World Karori"), "New World")
        self.assertEqual(_infer_supermarket_name(store_name="Pak'nSave Albany"), "Pak'nSave")
        self.assertEqual(_infer_supermarket_name(store_name="PAKn'SAVE Kilbirnie"), "Pak'nSave")
        self.assertEqual(
            _infer_supermarket_name(source_url="https://www.woolworths.co.nz/shop/browse/fruit-veg?page=1"),
            "Woolworths",
        )

    def test_snapshot_store_name_uses_store_name_when_present(self):
        class Snapshot:
            store_name = "Karori Woolworths"
            supermarket_name = "Woolworths"
            source_url = ""

        self.assertEqual(_snapshot_store_name(Snapshot()), "Karori Woolworths")

    def test_snapshot_store_name_falls_back_to_supermarket_name(self):
        class Snapshot:
            store_name = ""
            supermarket_name = "Pak'nSave"
            source_url = ""

        self.assertEqual(_snapshot_store_name(Snapshot()), "Pak'nSave")

    def test_snapshot_store_name_falls_back_to_inferred_supermarket_from_url(self):
        class Snapshot:
            store_name = ""
            supermarket_name = ""
            source_url = "https://www.newworld.co.nz/shop/category/pantry?pg=1"

        self.assertEqual(_snapshot_store_name(Snapshot()), "New World")

    def test_extract_unit_price_text_only_returns_unit_price(self):
        from scraper import _extract_unit_price_text

        self.assertEqual(_extract_unit_price_text("Non-member $23.00"), "")
        self.assertEqual(_extract_unit_price_text("$6.92 / 1L"), "$6.92 / 1L")
        self.assertEqual(_extract_unit_price_text("Unit price $6.92 / 1L each"), "$6.92 / 1L")
        self.assertEqual(_extract_unit_price_text("$6.92 / 1L per"), "$6.92 / 1L")

    def test_scraper_infers_paknsave_branding_variant_from_store_name(self):
        from scraper import _infer_supermarket_name_from_store_name

        self.assertEqual(_infer_supermarket_name_from_store_name("PAKn'SAVE Kilbirnie"), "Pak'nSave")

    def test_scrape_products_uses_product_subtitle_for_packaging(self):
        from scraper import scrape_products_from_html

        html = """
        <div data-testid="product-5000560-EA-000">
          <p data-testid="product-title">Heineken Lager Beer Bottles</p>
          <p data-testid="product-subtitle">12 x 330ml</p>
          <img data-testid="product-image" src="/img1.png" />
          <p data-testid="price-dollars">29</p>
          <p data-testid="price-cents">99</p>
          <p data-testid="non-promo-unit-price"></p>
        </div>
        <div data-testid="product-5000795-EA-000">
          <p data-testid="product-title">Heineken Lager Beer Bottles</p>
          <p data-testid="product-subtitle">24 x 330ml</p>
          <img data-testid="product-image" src="/img2.png" />
          <p data-testid="price-dollars">47</p>
          <p data-testid="price-cents">99</p>
          <p data-testid="non-promo-unit-price"></p>
        </div>
        """

        products, snapshots = scrape_products_from_html(
            html=html,
            url="https://www.paknsave.co.nz/shop/category/beer-wine-and-cider?pg=1",
            product_selector="div[data-testid^='product-'][data-testid$='-000']",
            name_selector="[data-testid='product-title']",
            price_selector="[data-testid='price-dollars']",
            price_cents_selector="[data-testid='price-cents']",
            unit_price_selector="[data-testid='non-promo-unit-price']",
            promo_price_dollars_selector="",
            promo_price_cents_selector="",
            promo_unit_price_selector="",
            image_selector="[data-testid='product-image']",
            limit=10,
            query=None,
            supermarket_name="Pak'nSave",
            store_name="PAKn'SAVE Kilbirnie",
        )

        self.assertEqual([product.packaging_format for product in products], ["12x330ml", "24x330ml"])
        self.assertEqual(
            [product.product_key for product in products],
            [
                "heineken lager beer bottles_12x330ml",
                "heineken lager beer bottles_24x330ml",
            ],
        )
        self.assertEqual(
            [snapshot.product_key for snapshot in snapshots],
            [
                "heineken lager beer bottles_12x330ml",
                "heineken lager beer bottles_24x330ml",
            ],
        )

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
                'abc_1kg',
                'New World Karori',
                'https://www.newworld.co.nz/shop/category/pantry?pg=1',
                '2026-04-04T06:00:00+00:00',
            ),
            Snapshot(
                'abc_1kg',
                'New World Karori',
                'https://www.newworld.co.nz/shop/category/pantry?pg=4',
                '2026-04-04T06:00:00.200000+00:00',
            ),
        ]

        deduped = dedupe_price_snapshots(snapshots)

        self.assertEqual(len(deduped), 2)

    def test_dedupe_price_snapshots_collapses_exact_duplicate_source_urls(self):
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
                'abc_1kg',
                'New World Karori',
                'https://www.newworld.co.nz/shop/category/pantry?pg=4',
                '2026-04-04T06:00:00+00:00',
            ),
            Snapshot(
                'abc_1kg',
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
            Snapshot('abc_1kg', 'New World Karori', '4.99'),
            Snapshot('abc_1kg', 'New World Metro', '4.99'),
            Snapshot('abc_1kg', 'New World Karori', '5.49'),
        ]

        deduped = dedupe_price_snapshots(snapshots)

        self.assertEqual(len(deduped), 3)

    def test_dedupe_price_snapshots_keeps_distinct_store_names(self):
        class Snapshot:
            def __init__(self, product_key, store_name):
                self.product_key = product_key
                self.supermarket_name = "New World"
                self.store_name = store_name
                self.source_url = 'https://www.newworld.co.nz/shop/category/pantry?pg=1'
                self.scraped_at = '2026-04-04T06:00:00+00:00'
                self.price = '4.99'
                self.unit_price = '$4.99/kg'
                self.promo_price = ''
                self.promo_unit_price = ''

        snapshots = [
            Snapshot('abc_1kg', 'New World Karori'),
            Snapshot('abc_1kg', 'New World Metro'),
        ]

        deduped = dedupe_price_snapshots(snapshots)

        self.assertEqual(len(deduped), 2)

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

    def test_find_category_id_matches_woolworths_browse_page_urls(self):
        lookup = _build_category_lookup([
            (44, "https://www.woolworths.co.nz/shop/browse/fruit-veg?page=1&size=48"),
        ])

        category_id = _find_category_id_for_source_url(
            lookup,
            "https://www.woolworths.co.nz/shop/browse/fruit-veg?search=&page=3&size=48",
        )

        self.assertEqual(category_id, 44)

    def test_find_category_id_ignores_non_category_urls(self):
        lookup = _build_category_lookup([
            (33, "https://www.newworld.co.nz/shop/category/frozen?pg=1"),
        ])

        category_id = _find_category_id_for_source_url(
            lookup,
            "https://www.newworld.co.nz/shop/product/12345_ea_000nw?name=frozen-peas",
        )

        self.assertIsNone(category_id)

    def test_parse_price_to_cents_converts_valid_price(self):
        self.assertEqual(_parse_price_to_cents("12.99"), 1299)
        self.assertEqual(_parse_price_to_cents("$5.00"), 500)
        self.assertEqual(_parse_price_to_cents("0.99"), 99)

    def test_parse_price_to_cents_returns_none_for_invalid_input(self):
        self.assertIsNone(_parse_price_to_cents(None))
        self.assertIsNone(_parse_price_to_cents(""))
        self.assertIsNone(_parse_price_to_cents("not-a-price"))

    def test_scrape_products_clears_promo_price_when_equal_to_price(self):
        from scraper import scrape_products_from_html

        html = """
        <div data-testid="product-1-EA-000">
          <p data-testid="product-title">Test Beer 330ml</p>
          <img data-testid="product-image" src="/img.png" />
          <p data-testid="price-dollars">10</p>
          <p data-testid="price-cents">00</p>
          <p data-testid="promo-dollars">10</p>
          <p data-testid="promo-cents">00</p>
          <p data-testid="non-promo-unit-price"></p>
        </div>
        """

        _, snapshots = scrape_products_from_html(
            html=html,
            url="https://www.paknsave.co.nz/shop/category/beer?pg=1",
            product_selector="div[data-testid^='product-'][data-testid$='-000']",
            name_selector="[data-testid='product-title']",
            price_selector="[data-testid='price-dollars']",
            price_cents_selector="[data-testid='price-cents']",
            unit_price_selector="[data-testid='non-promo-unit-price']",
            promo_price_dollars_selector="[data-testid='promo-dollars']",
            promo_price_cents_selector="[data-testid='promo-cents']",
            promo_unit_price_selector="",
            image_selector="[data-testid='product-image']",
            limit=10,
            query=None,
            supermarket_name="Pak'nSave",
            store_name="PAKn'SAVE Kilbirnie",
        )

        self.assertEqual(len(snapshots), 1)
        self.assertEqual(snapshots[0].price, "10.00")
        self.assertEqual(snapshots[0].promo_price, "")

    def test_scrape_products_clears_promo_price_when_greater_than_price(self):
        from scraper import scrape_products_from_html

        html = """
        <div data-testid="product-2-EA-000">
          <p data-testid="product-title">Test Wine 750ml</p>
          <img data-testid="product-image" src="/img.png" />
          <p data-testid="price-dollars">15</p>
          <p data-testid="price-cents">00</p>
          <p data-testid="promo-dollars">20</p>
          <p data-testid="promo-cents">00</p>
          <p data-testid="non-promo-unit-price"></p>
        </div>
        """

        _, snapshots = scrape_products_from_html(
            html=html,
            url="https://www.paknsave.co.nz/shop/category/wine?pg=1",
            product_selector="div[data-testid^='product-'][data-testid$='-000']",
            name_selector="[data-testid='product-title']",
            price_selector="[data-testid='price-dollars']",
            price_cents_selector="[data-testid='price-cents']",
            unit_price_selector="[data-testid='non-promo-unit-price']",
            promo_price_dollars_selector="[data-testid='promo-dollars']",
            promo_price_cents_selector="[data-testid='promo-cents']",
            promo_unit_price_selector="",
            image_selector="[data-testid='product-image']",
            limit=10,
            query=None,
            supermarket_name="Pak'nSave",
            store_name="PAKn'SAVE Kilbirnie",
        )

        self.assertEqual(len(snapshots), 1)
        self.assertEqual(snapshots[0].price, "15.00")
        self.assertEqual(snapshots[0].promo_price, "")

    def test_scrape_products_keeps_valid_promo_price_lower_than_price(self):
        from scraper import scrape_products_from_html

        html = """
        <div data-testid="product-3-EA-000">
          <p data-testid="product-title">Test Cider 330ml</p>
          <img data-testid="product-image" src="/img.png" />
          <p data-testid="price-dollars">12</p>
          <p data-testid="price-cents">99</p>
          <p data-testid="promo-dollars">9</p>
          <p data-testid="promo-cents">99</p>
          <p data-testid="non-promo-unit-price"></p>
        </div>
        """

        _, snapshots = scrape_products_from_html(
            html=html,
            url="https://www.paknsave.co.nz/shop/category/cider?pg=1",
            product_selector="div[data-testid^='product-'][data-testid$='-000']",
            name_selector="[data-testid='product-title']",
            price_selector="[data-testid='price-dollars']",
            price_cents_selector="[data-testid='price-cents']",
            unit_price_selector="[data-testid='non-promo-unit-price']",
            promo_price_dollars_selector="[data-testid='promo-dollars']",
            promo_price_cents_selector="[data-testid='promo-cents']",
            promo_unit_price_selector="",
            image_selector="[data-testid='product-image']",
            limit=10,
            query=None,
            supermarket_name="Pak'nSave",
            store_name="PAKn'SAVE Kilbirnie",
        )

        self.assertEqual(len(snapshots), 1)
        self.assertEqual(snapshots[0].price, "12.99")
        self.assertEqual(snapshots[0].promo_price, "9.99")


if __name__ == "__main__":
    unittest.main()
