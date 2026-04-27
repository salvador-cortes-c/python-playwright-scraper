"""Tests for WoolworthsApiProvider — direct JSON API provider for Woolworths NZ."""

import json
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scraper import (
    TransientError,
    WoolworthsApiProvider,
    build_provider,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_storage_state(cookies: list[dict]) -> str:
    """Return a Playwright storage_state JSON string."""
    return json.dumps({"cookies": cookies, "origins": []})


def _woolworths_cookie(name: str, value: str) -> dict:
    return {
        "name": name,
        "value": value,
        "domain": ".woolworths.co.nz",
        "path": "/",
        "expires": -1,
        "httpOnly": False,
        "secure": True,
        "sameSite": "Lax",
    }


_MINIMAL_STATE = _make_storage_state([
    _woolworths_cookie("XSRF-TOKEN", "test-xsrf"),
    _woolworths_cookie("session", "abc123"),
])

_API_RESPONSE = {
    "products": {
        "items": [
            {
                "type": "Product",
                "name": "Anchor Blue Top Milk 2L",
                "brand": "Anchor",
                "sku": "12345",
                "price": {
                    "originalPrice": 4.49,
                    "salePrice": None,
                    "isSpecial": False,
                    "purchasingUnitPrice": 4.49,
                },
                "size": {
                    "volumeSize": "2L",
                    "packageType": "Bottle",
                    "cupPrice": 2.245,
                    "cupMeasure": "1L",
                },
                "availabilityStatus": "In Stock",
                "departments": [{"name": "Fridge & Deli"}],
            },
            {
                "type": "Product",
                "name": "Lewis Road Creamery Whole Milk 2L",
                "brand": "Lewis Road Creamery",
                "sku": "67890",
                "price": {
                    "originalPrice": 6.99,
                    "salePrice": 5.99,
                    "isSpecial": True,
                    "purchasingUnitPrice": 5.99,
                },
                "size": {
                    "volumeSize": "2L",
                    "packageType": "Bottle",
                    "cupPrice": 3.495,
                    "cupMeasure": "1L",
                },
                "availabilityStatus": "In Stock",
                "departments": [{"name": "Fridge & Deli"}],
            },
            {
                # Non-product type — should be skipped
                "type": "Banner",
                "name": "Promotion",
            },
        ]
    }
}


class _FakeResponse:
    def __init__(self, status: int, body: str) -> None:
        self.status = status
        self._body = body

    async def text(self) -> str:
        return self._body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False


class _FakeSession:
    def __init__(self, status: int = 200, body: str = "") -> None:
        self.calls: list[dict] = []
        self._status = status
        self._body = body

    def get(self, url: str, **kwargs):
        self.calls.append({"url": url, "kwargs": kwargs})
        return _FakeResponse(self._status, self._body)


# ---------------------------------------------------------------------------
# build_provider integration
# ---------------------------------------------------------------------------

class BuildProviderWoolworthsApiTests(unittest.TestCase):
    def test_build_provider_returns_woolworths_api_provider(self):
        provider = build_provider(
            provider_name="woolworths-api",
            api_key=None,
            render_wait_ms=0,
            country_code="nz",
            premium_proxy=False,
        )
        self.assertIsInstance(provider, WoolworthsApiProvider)
        self.assertEqual(provider.name, "woolworths-api")

    def test_build_provider_uses_api_key_as_cookies_file(self):
        provider = build_provider(
            provider_name="woolworths-api",
            api_key="/custom/state.json",
            render_wait_ms=0,
            country_code="nz",
            premium_proxy=False,
        )
        self.assertIsInstance(provider, WoolworthsApiProvider)
        self.assertEqual(str(provider.cookies_file), "/custom/state.json")

    def test_build_provider_defaults_cookies_file_when_no_key(self):
        provider = build_provider(
            provider_name="woolworths-api",
            api_key=None,
            render_wait_ms=0,
            country_code="nz",
            premium_proxy=False,
        )
        self.assertIsInstance(provider, WoolworthsApiProvider)
        self.assertEqual(str(provider.cookies_file), "storage_state.json")


# ---------------------------------------------------------------------------
# Cookie loading
# ---------------------------------------------------------------------------

class CookieLoadingTests(unittest.TestCase):
    def test_load_cookies_reads_playwright_storage_state(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            state_file = Path(tmp) / "state.json"
            state_file.write_text(_MINIMAL_STATE, encoding="utf-8")
            provider = WoolworthsApiProvider(cookies_file=str(state_file))
            cookies = provider._load_cookies()
        self.assertEqual(cookies["XSRF-TOKEN"], "test-xsrf")
        self.assertEqual(cookies["session"], "abc123")

    def test_load_cookies_filters_non_woolworths_domains(self):
        import tempfile
        state = json.dumps({"cookies": [
            _woolworths_cookie("ww_session", "ww_val"),
            {"name": "other", "value": "val", "domain": ".example.com"},
        ]})
        with tempfile.TemporaryDirectory() as tmp:
            state_file = Path(tmp) / "state.json"
            state_file.write_text(state, encoding="utf-8")
            provider = WoolworthsApiProvider(cookies_file=str(state_file))
            cookies = provider._load_cookies()
        self.assertIn("ww_session", cookies)
        self.assertNotIn("other", cookies)

    def test_load_cookies_accepts_plain_list_format(self):
        """woolies-nz-cli stores cookies as a plain list rather than a dict."""
        import tempfile
        raw_list = json.dumps([_woolworths_cookie("token", "xyz")])
        with tempfile.TemporaryDirectory() as tmp:
            state_file = Path(tmp) / "cookies.json"
            state_file.write_text(raw_list, encoding="utf-8")
            provider = WoolworthsApiProvider(cookies_file=str(state_file))
            cookies = provider._load_cookies()
        self.assertEqual(cookies["token"], "xyz")

    def test_load_cookies_raises_when_file_missing(self):
        provider = WoolworthsApiProvider(cookies_file="/nonexistent/state.json")
        with self.assertRaises(RuntimeError) as ctx:
            provider._load_cookies()
        self.assertIn("not found", str(ctx.exception))

    def test_load_cookies_raises_when_no_woolworths_cookies(self):
        import tempfile
        state = json.dumps({"cookies": [
            {"name": "x", "value": "y", "domain": ".example.com"},
        ]})
        with tempfile.TemporaryDirectory() as tmp:
            state_file = Path(tmp) / "state.json"
            state_file.write_text(state, encoding="utf-8")
            provider = WoolworthsApiProvider(cookies_file=str(state_file))
            with self.assertRaises(RuntimeError) as ctx:
                provider._load_cookies()
        self.assertIn("No Woolworths cookies", str(ctx.exception))


# ---------------------------------------------------------------------------
# URL → API params
# ---------------------------------------------------------------------------

class BuildParamsTests(unittest.TestCase):
    def setUp(self):
        self.provider = WoolworthsApiProvider()

    def test_browse_url_maps_to_browse_target(self):
        params = self.provider._build_params(
            "https://www.woolworths.co.nz/shop/browse/fresh-foods-and-bakery?page=2&size=48"
        )
        self.assertEqual(params["target"], "browse")
        self.assertEqual(params["category"], "fresh-foods-and-bakery")
        self.assertEqual(params["page"], "2")
        self.assertEqual(params["size"], "48")

    def test_category_url_maps_to_browse_target(self):
        params = self.provider._build_params(
            "https://www.woolworths.co.nz/shop/category/bakery"
        )
        self.assertEqual(params["target"], "browse")
        self.assertEqual(params["category"], "bakery")

    def test_search_url_maps_to_search_target(self):
        params = self.provider._build_params(
            "https://www.woolworths.co.nz/shop/searchproducts?search=milk"
        )
        self.assertEqual(params["target"], "search")
        self.assertEqual(params["search"], "milk")

    def test_unknown_url_falls_back_to_empty_search(self):
        params = self.provider._build_params("https://www.woolworths.co.nz/")
        self.assertEqual(params["target"], "search")
        self.assertEqual(params["search"], "")

    def test_defaults_page_1_and_size_48_when_absent(self):
        params = self.provider._build_params(
            "https://www.woolworths.co.nz/shop/browse/bakery"
        )
        self.assertEqual(params["page"], "1")
        self.assertEqual(params["size"], "48")


# ---------------------------------------------------------------------------
# fetch_products
# ---------------------------------------------------------------------------

class FetchProductsTests(unittest.IsolatedAsyncioTestCase):
    def _make_provider(self, tmp_dir: str) -> WoolworthsApiProvider:
        state_file = Path(tmp_dir) / "state.json"
        state_file.write_text(_MINIMAL_STATE, encoding="utf-8")
        return WoolworthsApiProvider(cookies_file=str(state_file))

    async def test_returns_products_and_snapshots_from_api_response(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            provider = self._make_provider(tmp)
            session = _FakeSession(200, json.dumps(_API_RESPONSE))
            products, snapshots = await provider.fetch_products(
                session=session,
                url="https://www.woolworths.co.nz/shop/browse/dairy?page=1&size=48",
                query=None,
                limit=100,
                supermarket_name=None,
                store_name=None,
            )

        self.assertEqual(len(products), 2)
        self.assertEqual(len(snapshots), 2)

    async def test_product_fields_are_mapped_correctly(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            provider = self._make_provider(tmp)
            session = _FakeSession(200, json.dumps(_API_RESPONSE))
            products, snapshots = await provider.fetch_products(
                session=session,
                url="https://www.woolworths.co.nz/shop/browse/dairy",
                query=None,
                limit=100,
                supermarket_name=None,
                store_name=None,
            )

        anchor = next(p for p in products if "Anchor" in p.name)
        self.assertEqual(anchor.name, "Anchor Blue Top Milk 2L")
        # packaging_format should include the size
        self.assertIn("2", anchor.packaging_format)
        self.assertEqual(anchor.image, "")

        anchor_snap = next(s for s in snapshots if "Anchor" in s.product_key or "anchor" in s.product_key)
        self.assertEqual(anchor_snap.price, "4.49")
        self.assertEqual(anchor_snap.promo_price, "")
        self.assertEqual(anchor_snap.unit_price, "$2.25/1L")
        self.assertEqual(anchor_snap.supermarket_name, "Woolworths")

    async def test_sale_price_lower_than_original_becomes_promo_price(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            provider = self._make_provider(tmp)
            session = _FakeSession(200, json.dumps(_API_RESPONSE))
            products, snapshots = await provider.fetch_products(
                session=session,
                url="https://www.woolworths.co.nz/shop/browse/dairy",
                query=None,
                limit=100,
                supermarket_name=None,
                store_name=None,
            )

        lewis = next(s for s in snapshots if "lewis" in s.product_key)
        self.assertEqual(lewis.price, "6.99")
        self.assertEqual(lewis.promo_price, "5.99")

    async def test_limit_caps_returned_products(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            provider = self._make_provider(tmp)
            session = _FakeSession(200, json.dumps(_API_RESPONSE))
            products, snapshots = await provider.fetch_products(
                session=session,
                url="https://www.woolworths.co.nz/shop/browse/dairy",
                query=None,
                limit=1,
                supermarket_name=None,
                store_name=None,
            )

        self.assertEqual(len(products), 1)
        self.assertEqual(len(snapshots), 1)

    async def test_query_filter_excludes_non_matching_products(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            provider = self._make_provider(tmp)
            session = _FakeSession(200, json.dumps(_API_RESPONSE))
            products, snapshots = await provider.fetch_products(
                session=session,
                url="https://www.woolworths.co.nz/shop/browse/dairy",
                query="anchor",
                limit=100,
                supermarket_name=None,
                store_name=None,
            )

        self.assertEqual(len(products), 1)
        self.assertIn("Anchor", products[0].name)

    async def test_banner_items_are_skipped(self):
        """Items with type != 'Product' should be silently skipped."""
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            provider = self._make_provider(tmp)
            only_banner = {"products": {"items": [{"type": "Banner", "name": "Promo"}]}}
            session = _FakeSession(200, json.dumps(only_banner))
            products, snapshots = await provider.fetch_products(
                session=session,
                url="https://www.woolworths.co.nz/shop/browse/dairy",
                query=None,
                limit=100,
                supermarket_name=None,
                store_name=None,
            )

        self.assertEqual(products, [])
        self.assertEqual(snapshots, [])

    async def test_auth_failure_raises_runtime_error(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            provider = self._make_provider(tmp)
            session = _FakeSession(401, "Unauthorized")
            with self.assertRaises(RuntimeError) as ctx:
                await provider.fetch_products(
                    session=session,
                    url="https://www.woolworths.co.nz/shop/browse/dairy",
                    query=None,
                    limit=100,
                    supermarket_name=None,
                    store_name=None,
                )
        self.assertIn("authentication failed", str(ctx.exception).lower())

    async def test_transient_network_error_raises_transient_error(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            provider = self._make_provider(tmp)
            import asyncio

            class _TimeoutSession:
                def get(self, *args, **kwargs):
                    return _TimeoutResponse()

            class _TimeoutResponse:
                async def __aenter__(self):
                    raise asyncio.TimeoutError()

                async def __aexit__(self, *_):
                    return False

            with self.assertRaises(TransientError):
                await provider.fetch_products(
                    session=_TimeoutSession(),
                    url="https://www.woolworths.co.nz/shop/browse/dairy",
                    query=None,
                    limit=100,
                    supermarket_name=None,
                    store_name=None,
                )

    async def test_request_includes_woolworths_required_headers(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            provider = self._make_provider(tmp)
            session = _FakeSession(200, json.dumps(_API_RESPONSE))
            await provider.fetch_products(
                session=session,
                url="https://www.woolworths.co.nz/shop/browse/dairy",
                query=None,
                limit=100,
                supermarket_name=None,
                store_name=None,
            )

        self.assertEqual(len(session.calls), 1)
        headers = session.calls[0]["kwargs"]["headers"]
        self.assertEqual(headers["x-requested-with"], "OnlineShopping.WebApp")
        self.assertIn("x-ui-ver", headers)
        self.assertEqual(headers["x-xsrf-token"], "test-xsrf")

    async def test_request_hits_woolworths_api_endpoint(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            provider = self._make_provider(tmp)
            session = _FakeSession(200, json.dumps(_API_RESPONSE))
            await provider.fetch_products(
                session=session,
                url="https://www.woolworths.co.nz/shop/browse/dairy",
                query=None,
                limit=100,
                supermarket_name=None,
                store_name=None,
            )

        self.assertEqual(session.calls[0]["url"], "https://www.woolworths.co.nz/api/v1/products")

    async def test_supermarket_and_store_name_propagated_to_snapshots(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            provider = self._make_provider(tmp)
            session = _FakeSession(200, json.dumps(_API_RESPONSE))
            _, snapshots = await provider.fetch_products(
                session=session,
                url="https://www.woolworths.co.nz/shop/browse/dairy",
                query=None,
                limit=100,
                supermarket_name="Woolworths",
                store_name="Woolworths Karori",
            )

        for snap in snapshots:
            self.assertEqual(snap.supermarket_name, "Woolworths")
            self.assertEqual(snap.store_name, "Woolworths Karori")


if __name__ == "__main__":
    unittest.main()
