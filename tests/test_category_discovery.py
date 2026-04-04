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


if __name__ == "__main__":
    unittest.main()
