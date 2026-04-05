import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scraper import build_provider


class _FakeResponse:
    def __init__(self, status: int, text: str) -> None:
        self.status = status
        self._text = text

    async def text(self) -> str:
        return self._text

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeSession:
    def __init__(self, response_text: str = '{"html": "<html><body>ok</body></html>"}') -> None:
        self.calls: list[tuple[str, str, dict]] = []
        self.response_text = response_text

    def post(self, url: str, **kwargs):
        self.calls.append(("post", url, kwargs))
        return _FakeResponse(200, self.response_text)

    def get(self, url: str, **kwargs):
        self.calls.append(("get", url, kwargs))
        return _FakeResponse(200, self.response_text)


class ProviderSupportTests(unittest.IsolatedAsyncioTestCase):
    def test_build_provider_supports_floppydata(self):
        provider = build_provider(
            provider_name="floppydata",
            api_key="test-key",
            render_wait_ms=3000,
            country_code="nz",
            premium_proxy=True,
        )

        self.assertEqual(provider.name, "floppydata")

    def test_build_provider_supports_oxylabs(self):
        provider = build_provider(
            provider_name="oxylabs",
            api_key="user:pass",
            render_wait_ms=3000,
            country_code="nz",
            premium_proxy=True,
        )

        self.assertEqual(provider.name, "oxylabs")

    def test_build_provider_uses_floppydata_env_hint_when_missing_key(self):
        with self.assertRaisesRegex(ValueError, "FLOPPYDATA_API_KEY"):
            build_provider(
                provider_name="floppydata",
                api_key=None,
                render_wait_ms=3000,
                country_code="nz",
                premium_proxy=True,
            )

    def test_build_provider_uses_oxylabs_env_hint_when_missing_key(self):
        with self.assertRaisesRegex(ValueError, "OXYLABS_API_KEY"):
            build_provider(
                provider_name="oxylabs",
                api_key=None,
                render_wait_ms=3000,
                country_code="nz",
                premium_proxy=True,
            )

    async def test_floppydata_provider_posts_json_payload_and_extracts_html(self):
        provider = build_provider(
            provider_name="floppydata",
            api_key="test-key",
            render_wait_ms=3000,
            country_code="nz",
            premium_proxy=True,
        )
        session = _FakeSession()

        html, error, status = await provider.fetch(session, "https://example.com/products")

        self.assertEqual(html, "<html><body>ok</body></html>")
        self.assertIsNone(error)
        self.assertEqual(status, 200)
        self.assertEqual(len(session.calls), 1)

        method, url, kwargs = session.calls[0]
        self.assertEqual(method, "post")
        self.assertEqual(url, "https://client-api.floppy.host/v1/webUnlocker")
        self.assertEqual(kwargs["headers"]["X-Api-Key"], "test-key")
        self.assertEqual(kwargs["json"]["url"], "https://example.com/products")
        self.assertEqual(kwargs["json"]["country"], "NZ")
        self.assertEqual(kwargs["json"]["difficulty"], "medium")
        self.assertEqual(kwargs["json"]["expiration"], 0)

    async def test_oxylabs_provider_uses_web_unblocker_proxy_and_extracts_html(self):
        provider = build_provider(
            provider_name="oxylabs",
            api_key="user:pass",
            render_wait_ms=3000,
            country_code="nz",
            premium_proxy=True,
        )
        session = _FakeSession('<html><body>ok</body></html>')

        html, error, status = await provider.fetch(session, "https://example.com/products")

        self.assertEqual(html, "<html><body>ok</body></html>")
        self.assertIsNone(error)
        self.assertEqual(status, 200)
        self.assertEqual(len(session.calls), 1)

        method, url, kwargs = session.calls[0]
        self.assertEqual(method, "get")
        self.assertEqual(url, "https://example.com/products")
        self.assertEqual(kwargs["proxy"], "https://unblock.oxylabs.io:60000")
        self.assertEqual(kwargs["proxy_auth"].login, "user")
        self.assertEqual(kwargs["proxy_auth"].password, "pass")
        self.assertEqual(kwargs["headers"]["X-Oxylabs-Geo-Location"], "New Zealand")
        self.assertEqual(kwargs["headers"]["X-Oxylabs-Render"], "html")
        self.assertEqual(kwargs["headers"]["X-Oxylabs-Force-Headers"], "1")
        self.assertFalse(kwargs["ssl"])


if __name__ == "__main__":
    unittest.main()
