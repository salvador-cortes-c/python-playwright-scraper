import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scraper import (
    _detect_site_profile,
    discover_category_page_urls_from_html,
    discover_category_urls_from_html,
    scrape_products_from_html,
)


class CategoryDiscoveryTests(unittest.TestCase):
    def test_detect_site_profile_recognizes_supported_supermarkets(self):
        self.assertEqual(_detect_site_profile(["https://www.woolworths.co.nz/shop/browse/fruit-veg"]), "woolworths")
        self.assertEqual(
            _detect_site_profile(["https://www.paknsave.co.nz/shop/category/fresh-foods-and-bakery/fruit-vegetables?pg=1"]),
            "paknsave",
        )
        self.assertEqual(_detect_site_profile(["https://www.newworld.co.nz/shop/category/frozen?pg=1"]), "newworld")

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

    def test_discover_category_urls_can_use_next_data_groceries_tree(self):
        html = '''
        <html><body>
          <script id="__NEXT_DATA__" type="application/json">
          {
            "props": {
              "pageProps": {
                "navigation": [
                  {
                    "name": "Groceries",
                    "children": [
                      {"name": "Easter Deals", "url": "/shop/category/easter-deals"},
                      {"name": "Clubcard be in to Win", "url": "/shop/category/clubcard-be-in-to-win"},
                      {"name": "Easter", "url": "/shop/category/easter"},
                      {"name": "Fruit & Vegetables", "url": "/shop/category/fruit-and-vegetables"},
                      {"name": "Meat, Poultry & Seafood", "url": "/shop/category/meat-poultry-and-seafood"},
                      {"name": "Fridge, Deli & Eggs", "url": "/shop/category/dairy-eggs-and-fridge"},
                      {"name": "Bakery", "href": "/shop/category/bakery?pg=1"},
                      {"name": "Frozen", "children": [{"label": "View all Frozen", "href": "/shop/category/frozen?pg=1"}]},
                      {"name": "Pantry", "url": "/shop/category/pantry"},
                      {"name": "Hot & Cold Drinks", "url": "/shop/category/hot-and-cold-drinks"},
                      {"name": "Beer, Wine & Cider", "url": "/shop/category/beer-wine-and-cider"},
                      {"name": "Health & Body", "url": "/shop/category/health-and-body"},
                      {"name": "Baby & Toddler", "url": "/shop/category/baby-and-toddler"},
                      {"name": "Pets", "url": "/shop/category/pets"},
                      {"name": "Household & Cleaning", "url": "/shop/category/household-and-cleaning"},
                      {"name": "Snacks, Treats & Easy Meals", "url": "/shop/category/snacks-treats-and-easy-meals"}
                    ]
                  }
                ]
              }
            }
          }
          </script>
        </body></html>
        '''

        categories = discover_category_urls_from_html(
            start_url="https://www.newworld.co.nz/",
            html=html,
            category_link_selector="a._7zlpdd._7zlpdc",
            category_name_selector="button._7zlpdc",
        )

        self.assertEqual(
            [category.name for category in categories],
            [
                "Easter Deals",
                "Clubcard be in to Win",
                "Easter",
                "Fruit & Vegetables",
                "Meat, Poultry & Seafood",
                "Fridge, Deli & Eggs",
                "Bakery",
                "Frozen",
                "Pantry",
                "Hot & Cold Drinks",
                "Beer, Wine & Cider",
                "Health & Body",
                "Baby & Toddler",
                "Pets",
                "Household & Cleaning",
                "Snacks, Treats & Easy Meals",
            ],
        )

    def test_discover_category_page_urls_uses_showing_product_count_text(self):
        html = '''
        <div class="pagination">
          <span>Showing 1 - 50 of 212 products</span>
        </div>
        '''

        pages = discover_category_page_urls_from_html(
            start_url="https://www.newworld.co.nz/shop/category/fruit-and-vegetables?pg=1",
            html=html,
        )

        self.assertEqual(len(pages), 5)
        self.assertEqual(
            pages[-1],
            "https://www.newworld.co.nz/shop/category/fruit-and-vegetables?pg=5",
        )

    def test_discover_category_urls_prefers_direct_groceries_children_over_nested_lists(self):
        html = '''
        <html><body>
          <script id="__NEXT_DATA__" type="application/json">
          {
            "props": {
              "pageProps": {
                "navigation": [
                  {
                    "name": "Groceries",
                    "children": [
                      {"name": "Fruit & Vegetables", "url": "/shop/category/fruit-and-vegetables"},
                      {"name": "Bakery", "url": "/shop/category/bakery"},
                      {"name": "Frozen", "url": "/shop/category/frozen"}
                    ],
                    "items": [
                      {"name": "Fruit & Vegetables", "url": "/shop/category/fruit-and-vegetables"},
                      {"name": "Bakery", "url": "/shop/category/bakery"},
                      {"name": "Frozen", "url": "/shop/category/frozen"},
                      {"name": "Beer", "url": "/shop/category/beer-wine-and-cider/beer"},
                      {"name": "In-Store Bakery", "url": "/shop/category/bakery/in-store-bakery"}
                    ]
                  }
                ]
              }
            }
          }
          </script>
        </body></html>
        '''

        categories = discover_category_urls_from_html(
            start_url="https://www.newworld.co.nz/",
            html=html,
            category_link_selector="a._7zlpdd._7zlpdc",
            category_name_selector="button._7zlpdc",
        )

        self.assertEqual(
            [category.name for category in categories],
            ["Fruit & Vegetables", "Bakery", "Frozen"],
        )

    def test_discover_category_page_urls_uses_next_data_pagination_metadata(self):
        html = '''
        <html><body>
          <script id="__NEXT_DATA__" type="application/json">
          {
            "props": {
              "pageProps": {
                "search": {
                  "pagination": {
                    "pageSize": "50",
                    "totalItems": "212",
                    "currentPage": "1"
                  }
                }
              }
            }
          }
          </script>
        </body></html>
        '''

        pages = discover_category_page_urls_from_html(
            start_url="https://www.newworld.co.nz/shop/category/fruit-and-vegetables?pg=1",
            html=html,
        )

        self.assertEqual(len(pages), 5)
        self.assertEqual(
            pages[-1],
            "https://www.newworld.co.nz/shop/category/fruit-and-vegetables?pg=5",
        )

    def test_discover_category_page_urls_uses_pagination_button_numbers(self):
        html = '''
        <nav aria-label="Pagination">
          <button aria-label="Page 1">1</button>
          <button aria-label="Page 2">2</button>
          <button aria-label="Page 3">3</button>
          <span>…</span>
          <button aria-label="Page 5">5</button>
        </nav>
        '''

        pages = discover_category_page_urls_from_html(
            start_url="https://www.newworld.co.nz/shop/category/fruit-and-vegetables?pg=1",
            html=html,
        )

        self.assertEqual(len(pages), 5)
        self.assertEqual(
            pages[-1],
            "https://www.newworld.co.nz/shop/category/fruit-and-vegetables?pg=5",
        )

    def test_discover_category_page_urls_uses_raw_script_pagination_hints(self):
        html = '''
        <html><body>
          <script>
            self.__next_f.push([1, 'pagination:{"currentPage":1,"pageSize":50,"totalItems":212}']);
          </script>
        </body></html>
        '''

        pages = discover_category_page_urls_from_html(
            start_url="https://www.newworld.co.nz/shop/category/fruit-and-vegetables?pg=1",
            html=html,
        )

        self.assertEqual(len(pages), 5)
        self.assertEqual(
            pages[-1],
            "https://www.newworld.co.nz/shop/category/fruit-and-vegetables?pg=5",
        )

    def test_discover_category_urls_supports_woolworths_browse_routes(self):
        html = '''
        <section>
          <a href="/shop/browse/fruit-veg">Fruit &amp; Veg</a>
          <a href="/shop/browse/meat-seafood">Meat, Seafood &amp; Deli</a>
          <a href="/shop/browse/bakery">Bakery</a>
        </section>
        '''

        categories = discover_category_urls_from_html(
            start_url="https://www.woolworths.co.nz/",
            html=html,
            category_link_selector="a[href*='/shop/browse/']",
            category_name_selector="a[href*='/shop/browse/']",
        )

        self.assertEqual(
            [category.url for category in categories],
            [
                "https://www.woolworths.co.nz/shop/browse/fruit-veg?page=1",
                "https://www.woolworths.co.nz/shop/browse/meat-seafood?page=1",
                "https://www.woolworths.co.nz/shop/browse/bakery?page=1",
            ],
        )

    def test_discover_category_page_urls_supports_woolworths_page_and_size_params(self):
        html = '''
        <div id="totalItemsCount">370 items</div>
        '''

        pages = discover_category_page_urls_from_html(
            start_url="https://www.woolworths.co.nz/shop/browse/fruit-veg?search=&page=1&size=48&sort=BrowseRelevance",
            html=html,
        )

        self.assertEqual(len(pages), 8)
        self.assertEqual(
            pages[-1],
            "https://www.woolworths.co.nz/shop/browse/fruit-veg?search=&page=8&size=48&sort=BrowseRelevance",
        )

    def test_scrape_products_from_html_supports_woolworths_markup(self):
        html = '''
        <cdx-card>
          <product-stamp-grid>
            <div class="product-entry product-cup">
              <a class="productImage-container" href="/shop/productdetails?stockcode=133211&amp;name=fresh-fruit-bananas-yellow-loose">
                <figure>
                  <img src="https://assets.woolworths.com.au/images/2010/133211.jpg" alt="fresh fruit bananas yellow loose">
                </figure>
              </a>
              <a href="/shop/productdetails?stockcode=133211&amp;name=fresh-fruit-bananas-yellow-loose">
                <h3 id="product-133211-title"> fresh fruit bananas yellow loose </h3>
              </a>
              <div class="product-meta">
                <div class="priceMeta cupPriceAdjustment" id="product-133211-unitPrice">
                  <div class="cupPriceContainer">
                    <span class="cupPrice">$3.30 / 1kg</span>
                  </div>
                </div>
              </div>
              <product-price>
                <div class="priceCupAdjustmentDev">
                  <h3 class="heading--2 presentPrice priceCupAdjustment" id="product-133211-price" aria-label="$3.30 per kg.">
                    $ <em>3</em><span> 30 <br>kg</span>
                  </h3>
                </div>
              </product-price>
            </div>
          </product-stamp-grid>
        </cdx-card>
        '''

        products, snapshots = scrape_products_from_html(
            html=html,
            url="https://www.woolworths.co.nz/shop/browse/fruit-veg?search=&page=1&size=48&sort=BrowseRelevance",
            product_selector="product-stamp-grid, div.product-entry",
            name_selector="h3[id$='-title'], div.product-entry h3",
            price_selector="h3[id$='-price'] em, product-price h3 em",
            price_cents_selector="h3[id$='-price'] span, product-price h3 span",
            unit_price_selector="span.cupPrice",
            promo_price_dollars_selector="",
            promo_price_cents_selector="",
            promo_unit_price_selector="",
            image_selector="a.productImage-container img, figure img",
            limit=5,
            query=None,
        )

        self.assertEqual(len(products), 1)
        self.assertEqual(products[0].name, "fresh fruit bananas yellow loose")
        self.assertEqual(len(snapshots), 1)
        self.assertEqual(snapshots[0].price, "3.30")
        self.assertEqual(snapshots[0].unit_price, "$3.30 / 1kg")


if __name__ == "__main__":
    unittest.main()
