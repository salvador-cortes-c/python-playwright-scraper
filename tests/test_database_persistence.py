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
    _strip_packaging_suffix_for_key,
    dedupe_price_snapshots,
)


class DatabasePersistenceTests(unittest.TestCase):
    def test_normalize_product_record_infers_packaging_and_repairs_legacy_key(self):
        normalized = _normalize_product_record(
            "asahi beer super dry lager bottle 12x330ml__",
            "Asahi Beer Super Dry Lager Bottle 12x330mL",
            "",
        )

        # "12x330mL" is stripped from the name-portion of the key (already
        # captured in the packaging suffix), preventing key duplication.
        self.assertEqual(
            normalized,
            (
                "asahi beer super dry lager bottle_12x330ml",
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

    def test_extract_packaging_from_name_combines_pack_count_and_size(self):
        """'N pack NNNml' subtitle should yield 'NxNNNml', not just 'NNNml'.

        Woolworths NZ renders multi-pack variants as subtitles like '10 pack 330mL'.
        Without combining the pack count, every variant collapses to '330mL' and
        the database stores only one row instead of a distinct row per pack size.
        """
        from database import _extract_packaging_from_name
        self.assertEqual(_extract_packaging_from_name("10 pack 330mL"), "10x330mL")
        self.assertEqual(_extract_packaging_from_name("6 pack 330mL"), "6x330mL")
        self.assertEqual(_extract_packaging_from_name("15 pack 330mL"), "15x330mL")
        # Existing "NxNNNml" format must be unchanged
        self.assertEqual(_extract_packaging_from_name("6x330mL"), "6x330mL")
        self.assertEqual(_extract_packaging_from_name("10x330mL"), "10x330mL")
        # Plain size (no pack count) must still work
        self.assertEqual(_extract_packaging_from_name("330mL"), "330mL")
        # "12 x 330mL" (explicit multiplier notation) must still work
        self.assertEqual(_extract_packaging_from_name("12 x 330mL"), "12x330mL")

    def test_woolworths_laid_back_lager_variants_get_distinct_keys(self):
        """Woolworths Karori lists multiple 'Laid Back Lager' pack sizes under the
        same product name with the pack count only in the subtitle.  Each variant
        must get a distinct product_key so all variants are stored in the DB and
        shown in the UI — not collapsed into a single row."""
        six_pack = _normalize_product_record(
            "",
            "Boundary Road Brewery Laid Back Lager",
            "6 pack 330mL",
        )
        ten_pack = _normalize_product_record(
            "",
            "Boundary Road Brewery Laid Back Lager",
            "10 pack 330mL",
        )
        fifteen_pack = _normalize_product_record(
            "",
            "Boundary Road Brewery Laid Back Lager",
            "15 pack 330mL",
        )

        self.assertIsNotNone(six_pack)
        self.assertIsNotNone(ten_pack)
        self.assertIsNotNone(fifteen_pack)

        six_key, _, six_fmt = six_pack
        ten_key, _, ten_fmt = ten_pack
        fifteen_key, _, fifteen_fmt = fifteen_pack

        # All three must be distinct keys
        self.assertNotEqual(six_key, ten_key, "6-pack and 10-pack must have distinct product_keys")
        self.assertNotEqual(ten_key, fifteen_key, "10-pack and 15-pack must have distinct product_keys")
        self.assertNotEqual(six_key, fifteen_key, "6-pack and 15-pack must have distinct product_keys")

        # Packaging format must reflect the full pack count, not just per-can size
        self.assertEqual(six_fmt, "6x330mL")
        self.assertEqual(ten_fmt, "10x330mL")
        self.assertEqual(fifteen_fmt, "15x330mL")

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

    # ------------------------------------------------------------------
    # Normalization edge cases: whitespace, case, special characters
    # ------------------------------------------------------------------

    def test_normalize_name_for_key_lowercases_and_preserves_whitespace(self):
        """_normalize_name_for_key lowercases but does not strip surrounding whitespace;
        stripping happens only in _normalize_product_record via the clean_name step."""
        self.assertEqual(
            _normalize_name_for_key("  Heineken Lager  "),
            "  heineken lager  ",
        )

    def test_normalize_product_record_collapses_internal_whitespace_in_name(self):
        """Multiple consecutive spaces in a name are collapsed to single spaces."""
        normalized = _normalize_product_record("", "Heineken  Lager  Beer", "")
        self.assertIsNotNone(normalized)
        key, name, _ = normalized
        self.assertEqual(name, "Heineken Lager Beer")
        self.assertNotIn("  ", key)

    def test_normalize_name_for_key_does_not_alter_apostrophes(self):
        """Apostrophes in brand names (e.g. Wattie's) are preserved in the key."""
        key = _normalize_name_for_key("Wattie's Baked Beans 420g")
        self.assertIn("wattie's", key)

    def test_normalize_name_for_key_only_replaces_word_connecting_hyphens(self):
        """A hyphen that connects two word characters is replaced with a space;
        a hyphen that does not connect two word characters (e.g. a separator dash
        preceded by a space) is left unchanged."""
        # Word-connecting hyphen should be replaced with a space
        self.assertEqual(
            _normalize_name_for_key("Laid-Back Lager"),
            "laid back lager",
        )
        # Hyphen used as a separator (space on left) should not be changed
        key_with_dash = _normalize_name_for_key("Green & Black's - Dark Chocolate")
        self.assertIn("black's - dark", key_with_dash)

    # ------------------------------------------------------------------
    # Pack-size mismatch edge cases
    # ------------------------------------------------------------------

    def test_normalize_packaging_handles_large_pack_count(self):
        """24 x 330ml and 24x330ml are the same pack size."""
        self.assertEqual(_normalize_packaging("24 x 330ml"), "24x330ml")
        self.assertEqual(_normalize_packaging("24x330ml"), "24x330ml")

    def test_normalize_packaging_handles_fractional_sizes(self):
        """Fractional sizes like 1.5 l and 1.5l normalize identically."""
        self.assertEqual(_normalize_packaging("1.5 l"), "1.5l")
        self.assertEqual(_normalize_packaging("1.5l"), "1.5l")

    def test_woolworths_1_pack_variant_gets_distinct_key_from_plain_size(self):
        """A single-unit pack ('1 pack 330mL') should produce '1x330mL', not '330mL',
        so it is distinguishable from a loose single-can listing."""
        one_pack = _normalize_product_record(
            "",
            "Boundary Road Brewery Laid Back Lager",
            "1 pack 330mL",
        )
        plain = _normalize_product_record(
            "",
            "Boundary Road Brewery Laid Back Lager",
            "330mL",
        )

        self.assertIsNotNone(one_pack)
        self.assertIsNotNone(plain)
        self.assertNotEqual(one_pack[0], plain[0], "1-pack and plain 330mL should be distinct keys")
        self.assertEqual(one_pack[2], "1x330mL")

    def test_different_pack_counts_with_same_can_size_get_distinct_keys(self):
        """6-pack and 12-pack of the same product must produce different product_keys."""
        six = _normalize_product_record("", "Heineken Lager Beer Bottles", "6 x 330ml")
        twelve = _normalize_product_record("", "Heineken Lager Beer Bottles", "12 x 330ml")

        self.assertIsNotNone(six)
        self.assertIsNotNone(twelve)
        self.assertNotEqual(six[0], twelve[0])
        self.assertEqual(six[2], "6x330ml")
        self.assertEqual(twelve[2], "12x330ml")

    def test_pack_size_with_uppercase_units_normalizes_consistently(self):
        """'330ML' (uppercase) and '330ml' (lowercase) should produce the same packaging token."""
        upper = _normalize_product_record("", "Test Beer", "6 x 330ML")
        lower = _normalize_product_record("", "Test Beer", "6 x 330ml")

        self.assertIsNotNone(upper)
        self.assertIsNotNone(lower)
        # The key comparison must be case-insensitive in the packaging portion
        self.assertEqual(upper[0].lower(), lower[0].lower())

    # ------------------------------------------------------------------
    # Price-only name rejection edge cases
    # ------------------------------------------------------------------

    def test_normalize_product_record_rejects_price_with_unit(self):
        self.assertIsNone(_normalize_product_record("", "3 kg", ""))
        self.assertIsNone(_normalize_product_record("", "500ml", ""))
        self.assertIsNone(_normalize_product_record("", "$1.99 ea", ""))

    def test_normalize_product_record_accepts_real_product_names_with_prices_embedded(self):
        """A name like '6 x 330ml Lager' contains a size but is not a price-only name."""
        result = _normalize_product_record("", "Boundary Road Brewery Laid-Back Lager Cans 6 x 330ml", "")
        self.assertIsNotNone(result)

    # ------------------------------------------------------------------
    # Snapshot deduplication: richer field coverage
    # ------------------------------------------------------------------

    def test_dedupe_price_snapshots_exact_duplicate_is_collapsed_to_one(self):
        """Two snapshots with the same product, store, source URL and all price fields
        identical are treated as exact duplicates and collapsed to one entry."""
        class Snapshot:
            def __init__(self):
                self.product_key = "abc_1kg"
                self.supermarket_name = "New World Karori"
                self.source_url = "https://www.newworld.co.nz/shop/category/pantry?pg=1"
                self.scraped_at = "2026-04-04T06:00:00+00:00"
                self.price = "4.99"
                self.unit_price = "$4.99/kg"
                self.promo_price = ""
                self.promo_unit_price = ""

        deduped = dedupe_price_snapshots([Snapshot(), Snapshot()])

        self.assertEqual(len(deduped), 1)

    def test_dedupe_price_snapshots_same_product_different_stores_are_kept(self):
        """The same product at PAK'nSAVE and New World should produce two snapshots."""
        class Snapshot:
            def __init__(self, store_name, supermarket_name):
                self.product_key = "heineken lager beer bottles_12x330ml"
                self.supermarket_name = supermarket_name
                self.store_name = store_name
                self.source_url = "https://example.com/shop/category/beer?pg=1"
                self.scraped_at = "2026-04-04T06:00:00+00:00"
                self.price = "29.99"
                self.unit_price = ""
                self.promo_price = ""
                self.promo_unit_price = ""

        snapshots = [
            Snapshot("PAK'nSAVE Kilbirnie", "Pak'nSave"),
            Snapshot("New World Karori", "New World"),
        ]

        deduped = dedupe_price_snapshots(snapshots)

        self.assertEqual(len(deduped), 2)

    # ------------------------------------------------------------------
    # _strip_packaging_suffix_for_key
    # ------------------------------------------------------------------

    def test_strip_packaging_suffix_strips_trailing_size(self):
        """Plain trailing size token is removed."""
        self.assertEqual(_strip_packaging_suffix_for_key("Squealing Pig Rose 750mL"), "Squealing Pig Rose")
        self.assertEqual(_strip_packaging_suffix_for_key("Heineken Lager 330mL"), "Heineken Lager")

    def test_strip_packaging_suffix_strips_multipack_size(self):
        """Multi-pack trailing token is removed."""
        self.assertEqual(
            _strip_packaging_suffix_for_key("Asahi Super Dry Lager Bottle 12x330mL"),
            "Asahi Super Dry Lager Bottle",
        )

    def test_strip_packaging_suffix_strips_size_with_spaces(self):
        """'6 x 330ml' (space-separated multiplier) is stripped."""
        self.assertEqual(
            _strip_packaging_suffix_for_key("Boundary Road Lager Cans 6 x 330ml"),
            "Boundary Road Lager Cans",
        )

    def test_strip_packaging_suffix_strips_multiple_trailing_tokens(self):
        """Consecutive trailing tokens (e.g. '6 pack 330mL') are both removed."""
        self.assertEqual(
            _strip_packaging_suffix_for_key("Boundary Road Lager 6 pack 330mL"),
            "Boundary Road Lager",
        )

    def test_strip_packaging_suffix_unchanged_when_no_trailing_token(self):
        """Name with no trailing size token is returned unchanged."""
        self.assertEqual(
            _strip_packaging_suffix_for_key("Boundary Road Lager Cans"),
            "Boundary Road Lager Cans",
        )

    def test_strip_packaging_suffix_does_not_strip_entire_name(self):
        """If stripping would leave an empty string, the original name is returned."""
        self.assertEqual(_strip_packaging_suffix_for_key("750mL"), "750mL")
        self.assertEqual(_strip_packaging_suffix_for_key("12x330mL"), "12x330mL")

    # ------------------------------------------------------------------
    # Cross-store deduplication via packaging-in-name stripping
    # ------------------------------------------------------------------

    def test_size_in_name_and_explicit_packaging_produce_same_key(self):
        """A store that includes the size in the product title and one that
        doesn't must produce the same product_key when packaging_format is
        identical, so they collapse to a single DB record."""
        with_size = _normalize_product_record("", "Squealing Pig Rose 750mL", "750mL")
        without_size = _normalize_product_record("", "Squealing Pig Rose", "750mL")

        self.assertIsNotNone(with_size)
        self.assertIsNotNone(without_size)
        self.assertEqual(
            with_size[0],
            without_size[0],
            "Packaging size embedded in name vs. absent should yield the same product_key",
        )

    def test_packaging_size_not_duplicated_in_key(self):
        """When the name ends with the packaging size, that size should appear
        exactly once in the product_key (in the packaging suffix only)."""
        result = _normalize_product_record("", "Heineken Lager 330mL", "330mL")
        self.assertIsNotNone(result)
        key, _, _ = result
        self.assertEqual(
            key.count("330ml"),
            1,
            "Packaging size must not appear twice in the product_key",
        )

    def test_multipack_size_in_name_and_explicit_packaging_produce_same_key(self):
        """Multi-pack size embedded in name deduplicates against explicit packaging."""
        with_size = _normalize_product_record(
            "", "Speight's Summit Lager Bottle 24x330mL", "24x330mL"
        )
        without_size = _normalize_product_record(
            "", "Speight's Summit Lager Bottle", "24x330mL"
        )

        self.assertIsNotNone(with_size)
        self.assertIsNotNone(without_size)
        self.assertEqual(
            with_size[0],
            without_size[0],
            "Multi-pack size in name should not create a separate product_key",
        )


if __name__ == "__main__":
    unittest.main()
