import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scraper import (
    RateLimitError,
    TransientError,
    _compute_backoff,
    build_provider,
)


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

    async def test_oxylabs_provider_posts_basic_auth_payload_and_extracts_html(self):
        provider = build_provider(
            provider_name="oxylabs",
            api_key="user:pass",
            render_wait_ms=3000,
            country_code="nz",
            premium_proxy=True,
        )
        session = _FakeSession('{"results": [{"content": "<html><body>ok</body></html>"}]}')

        html, error, status = await provider.fetch(session, "https://example.com/products")

        self.assertEqual(html, "<html><body>ok</body></html>")
        self.assertIsNone(error)
        self.assertEqual(status, 200)
        self.assertEqual(len(session.calls), 1)

        method, url, kwargs = session.calls[0]
        self.assertEqual(method, "post")
        self.assertEqual(url, "https://realtime.oxylabs.io/v1/queries")
        self.assertEqual(kwargs["json"]["source"], "universal")
        self.assertEqual(kwargs["json"]["url"], "https://example.com/products")
        self.assertEqual(kwargs["json"]["geo_location"], "New Zealand")
        self.assertEqual(kwargs["json"]["render"], "html")
        self.assertEqual(kwargs["json"]["user_agent_type"], "desktop")
        self.assertEqual(kwargs["auth"].login, "user")
        self.assertEqual(kwargs["auth"].password, "pass")

    def test_build_provider_supports_direct_without_api_key(self):
        provider = build_provider(
            provider_name="direct",
            api_key=None,
            render_wait_ms=3000,
            country_code="nz",
            premium_proxy=False,
        )

        self.assertEqual(provider.name, "direct")

    def test_build_provider_raises_for_unknown_provider(self):
        with self.assertRaisesRegex(ValueError, "Unknown provider"):
            build_provider(
                provider_name="unknown_provider",
                api_key="key",
                render_wait_ms=3000,
                country_code="nz",
                premium_proxy=False,
            )


class ErrorClassTests(unittest.TestCase):
    def test_rate_limit_error_is_runtime_error_subclass(self):
        err = RateLimitError("HTTP 429")
        self.assertIsInstance(err, RuntimeError)
        self.assertEqual(str(err), "HTTP 429")

    def test_transient_error_is_runtime_error_subclass(self):
        err = TransientError("connection reset")
        self.assertIsInstance(err, RuntimeError)
        self.assertEqual(str(err), "connection reset")

    def test_rate_limit_error_and_transient_error_are_distinct(self):
        self.assertFalse(issubclass(RateLimitError, TransientError))
        self.assertFalse(issubclass(TransientError, RateLimitError))


class BackoffTests(unittest.TestCase):
    def test_compute_backoff_first_attempt_is_close_to_base(self):
        """Attempt 1: delay should be between base * 0.75 and base * 2 * 1.25 (one doubling + full jitter)."""
        for _ in range(20):
            result = _compute_backoff(attempt=1, base_seconds=5.0, max_seconds=120.0)
            self.assertGreaterEqual(result, 5.0 * 0.75)
            self.assertLessEqual(result, 5.0 * 2 * 1.25)

    def test_compute_backoff_caps_at_max_seconds(self):
        """Very high attempt count should not exceed max_seconds * 1.25 (jitter ceiling)."""
        for _ in range(20):
            result = _compute_backoff(attempt=100, base_seconds=5.0, max_seconds=60.0)
            self.assertLessEqual(result, 60.0 * 1.25)

    def test_compute_backoff_returns_positive_value(self):
        for attempt in range(5):
            result = _compute_backoff(attempt=attempt, base_seconds=1.0, max_seconds=30.0)
            self.assertGreater(result, 0)


if __name__ == "__main__":
    unittest.main()
