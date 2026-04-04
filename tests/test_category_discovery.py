import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scraper import discover_category_urls_from_html


class CategoryDiscoveryTests(unittest.TestCase):
    def test_discover_category_urls_prefers_view_all_links_for_top_level_categories(self):
        html = '''
        <div class="menu">
          <button class="_7zlpdc">Fruit & Vegetables</button>
          <div>
            <a class="_7zlpdd _7zlpdc" href="/shop/category/fruit-and-vegetables?pg=1">View all Fruit & Vegetables</a>
            <a class="_7zlpdd _7zlpdc" href="/shop/category/apples-and-pears?pg=1">Apples & Pears</a>
          </div>

          <button class="_7zlpdc">Dairy, Eggs & Fridge</button>
          <div>
            <a class="_7zlpdd _7zlpdc" href="/shop/category/dairy-eggs-and-fridge?pg=1">View all Dairy, Eggs & Fridge</a>
            <a class="_7zlpdd _7zlpdc" href="/shop/category/milk?pg=1">Milk</a>
          </div>
        </div>
        '''

        categories = discover_category_urls_from_html(
            start_url="https://www.newworld.co.nz/",
            html=html,
            category_link_selector="a._7zlpdd._7zlpdc",
            category_name_selector="button._7zlpdc",
        )

        self.assertEqual(
            [category.url for category in categories],
            [
                "https://www.newworld.co.nz/shop/category/fruit-and-vegetables?pg=1",
                "https://www.newworld.co.nz/shop/category/dairy-eggs-and-fridge?pg=1",
            ],
        )
        self.assertEqual(
            [category.name for category in categories],
            ["Fruit & Vegetables", "Dairy, Eggs & Fridge"],
        )

    def test_discover_category_urls_filters_large_mixed_menu_to_likely_top_level_categories(self):
        top_level = [
            ("Fruit & Vegetables", "fruit-and-vegetables"),
            ("Bakery", "bakery"),
            ("Frozen", "frozen"),
            ("Pet Care", "pet-care"),
        ]
        extras = [(f"Promo {idx}", f"promo-{idx}") for idx in range(1, 23)]
        sections = []
        for label, slug in top_level + extras:
            sections.append(
                f'''<button class="_7zlpdc">{label}</button>
                <div><a class="_7zlpdd _7zlpdc" href="/shop/category/{slug}?pg=1">View all {label}</a></div>'''
            )

        html = "<section><h2>Groceries</h2>" + "".join(sections) + "</section>"

        categories = discover_category_urls_from_html(
            start_url="https://www.newworld.co.nz/",
            html=html,
            category_link_selector="a._7zlpdd._7zlpdc",
            category_name_selector="button._7zlpdc",
        )

        self.assertEqual([category.name for category in categories], [label for label, _ in top_level])

    def test_discover_category_urls_can_match_when_links_are_in_separate_container(self):
        html = '''
        <section>
          <h2>Groceries</h2>
          <div class="buttons">
            <button class="_7zlpdc">Fruit & Vegetables</button>
            <button class="_7zlpdc">Bakery</button>
            <button class="_7zlpdc">Frozen</button>
          </div>
          <div></div><div></div><div></div><div></div><div></div>
          <div class="links">
            <a class="_7zlpdd _7zlpdc" href="/shop/category/frozen?pg=1">View all Frozen</a>
            <a class="_7zlpdd _7zlpdc" href="/shop/category/fruit-and-vegetables?pg=1">View all Fruit & Vegetables</a>
            <a class="_7zlpdd _7zlpdc" href="/shop/category/bakery?pg=1">View all Bakery</a>
          </div>
        </section>
        '''

        categories = discover_category_urls_from_html(
            start_url="https://www.newworld.co.nz/",
            html=html,
            category_link_selector="a._7zlpdd._7zlpdc",
            category_name_selector="button._7zlpdc",
        )

        self.assertEqual(
            [category.url for category in categories],
            [
                "https://www.newworld.co.nz/shop/category/fruit-and-vegetables?pg=1",
                "https://www.newworld.co.nz/shop/category/bakery?pg=1",
                "https://www.newworld.co.nz/shop/category/frozen?pg=1",
            ],
        )


if __name__ == "__main__":
    unittest.main()
