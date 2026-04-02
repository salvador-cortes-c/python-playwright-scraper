#!/usr/bin/env python3
"""
New World scraper using ScrapingBee with feature parity to the archive Playwright scraper
where feasible in an API-based scraping workflow.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import re
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

import aiohttp
from bs4 import BeautifulSoup, Tag

from database import persist_scrape_results, resolve_database_url

try:
    from playwright.async_api import async_playwright
    from playwright_stealth import Stealth
except ImportError:
    async_playwright = None
    Stealth = None

SCRAPINGBEE_API_URL = "https://app.scrapingbee.com/api/v1/"
SCRAPING_PROVIDER_API_KEY_ENV = "SCRAPING_PROVIDER_API_KEY"

_PROVIDER_ENDPOINTS: dict[str, str] = {
    "scrapingbee": "https://app.scrapingbee.com/api/v1/",
    "scraperapi":  "http://api.scraperapi.com/",
    "crawlbase":   "https://api.crawlbase.com/",
    "zenrows":     "https://api.zenrows.com/v1/",
}
_PROVIDER_KEY_ENVVARS: dict[str, str] = {
    "playwright":  "",
    "scrapingbee": "SCRAPING_PROVIDER_API_KEY",
    "scraperapi":  "SCRAPERAPI_KEY",
    "crawlbase":   "CRAWLBASE_TOKEN",
    "zenrows":     "ZENROWS_API_KEY",
    "direct":      "",
}


class RateLimitError(RuntimeError):
    pass


class TransientError(RuntimeError):
    """Retriable network or server error (timeout, connection reset, empty response)."""


def _compute_backoff(attempt: int, base_seconds: float, max_seconds: float) -> float:
    """Exponential backoff with full jitter.

    Returns a delay between base_seconds and min(base_seconds * 2^attempt, max_seconds),
    randomised with ±25% jitter so concurrent workers don't retry in sync.
    """
    exp = min(base_seconds * (2 ** attempt), max_seconds)
    return exp * (0.75 + 0.5 * random.random())


# --- Scraper providers --------------------------------------------------------

FetchResult = tuple[Optional[str], Optional[dict[str, Any]], int | None]


class _BaseProvider:
    """Shared fetch mechanics for proxy-API-based scraping providers."""

    def __init__(self, api_key: str, render_wait_ms: int, country_code: str, premium_proxy: bool) -> None:
        self.api_key = api_key
        self.render_wait_ms = render_wait_ms
        self.country_code = country_code
        self.premium_proxy = premium_proxy

    @property
    def name(self) -> str:  # pragma: no cover
        raise NotImplementedError

    def _endpoint(self) -> str:  # pragma: no cover
        raise NotImplementedError

    def _build_params(self, url: str) -> dict[str, str]:  # pragma: no cover
        raise NotImplementedError

    async def fetch(self, session: aiohttp.ClientSession, url: str) -> FetchResult:
        """Fetch a URL via the provider API and return (html, error, status)."""
        params = self._build_params(url)
        try:
            async with session.get(self._endpoint(), params=params, timeout=60) as response:
                status = response.status
                text = await response.text()
                if status != 200:
                    try:
                        payload = json.loads(text)
                    except Exception:
                        payload = {"error": f"HTTP {status}", "response": text[:300]}
                    return None, payload, status
                if "<html" in text.lower() or "<!doctype" in text.lower():
                    return text, None, status
                try:
                    payload = json.loads(text)
                    return None, payload, status
                except Exception:
                    return None, {"error": "Invalid non-HTML response", "response": text[:300]}, status
        except asyncio.TimeoutError:
            return None, {"error": "Timeout after 60 seconds"}, None
        except Exception as exc:
            return None, {"error": str(exc)}, None


class ScrapingBeeProvider(_BaseProvider):
    @property
    def name(self) -> str:
        return "scrapingbee"

    def _endpoint(self) -> str:
        return _PROVIDER_ENDPOINTS["scrapingbee"]

    def _build_params(self, url: str) -> dict[str, str]:
        return {
            "api_key": self.api_key,
            "url": url,
            "render_js": "true",
            "premium_proxy": "true" if self.premium_proxy else "false",
            "country_code": self.country_code,
            "wait": str(max(0, self.render_wait_ms)),
            "block_ads": "true",
            "block_resources": "false",
            "return_page_source": "true",
        }


class ScraperAPIProvider(_BaseProvider):
    @property
    def name(self) -> str:
        return "scraperapi"

    def _endpoint(self) -> str:
        return _PROVIDER_ENDPOINTS["scraperapi"]

    def _build_params(self, url: str) -> dict[str, str]:
        params: dict[str, str] = {
            "api_key": self.api_key,
            "url": url,
            "render": "true",
            "country_code": self.country_code,
            "keep_headers": "true",
        }
        if self.premium_proxy:
            params["premium"] = "true"
        if self.render_wait_ms > 0:
            params["wait_for_css"] = "body"
        return params


class CrawlbaseProvider(_BaseProvider):
    @property
    def name(self) -> str:
        return "crawlbase"

    def _endpoint(self) -> str:
        return _PROVIDER_ENDPOINTS["crawlbase"]

    def _build_params(self, url: str) -> dict[str, str]:
        return {
            "token": self.api_key,
            "url": url,
            "headless": "true",
            "page_wait": str(max(0, self.render_wait_ms)),
            "country": self.country_code.upper(),
        }


class ZenrowsProvider(_BaseProvider):
    @property
    def name(self) -> str:
        return "zenrows"

    def _endpoint(self) -> str:
        return _PROVIDER_ENDPOINTS["zenrows"]

    def _build_params(self, url: str) -> dict[str, str]:
        params: dict[str, str] = {
            "apikey": self.api_key,
            "url": url,
            "js_render": "true",
            "premium_proxy": "true" if self.premium_proxy else "false",
            "country_code": self.country_code,
        }
        if self.render_wait_ms > 0:
            params["wait"] = str(self.render_wait_ms)
        return params


class DirectProvider:
    """Fetch URLs directly without a proxy (for testing or non-protected sites)."""

    @property
    def name(self) -> str:
        return "direct"

    async def fetch(self, session: aiohttp.ClientSession, url: str) -> FetchResult:
        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-NZ,en;q=0.5",
        }
        try:
            async with session.get(url, headers=headers, timeout=30) as response:
                status = response.status
                text = await response.text()
                if status not in (200, 201):
                    return None, {"error": f"HTTP {status}", "response": text[:300]}, status
                if "<html" in text.lower() or "<!doctype" in text.lower():
                    return text, None, status
                return None, {"error": "Non-HTML response", "response": text[:300]}, status
        except asyncio.TimeoutError:
            return None, {"error": "Timeout after 30 seconds"}, None
        except Exception as exc:
            return None, {"error": str(exc)}, None


AnyProvider = ScrapingBeeProvider | ScraperAPIProvider | CrawlbaseProvider | ZenrowsProvider | DirectProvider


def build_provider(
    provider_name: str,
    api_key: str | None,
    render_wait_ms: int,
    country_code: str,
    premium_proxy: bool,
) -> AnyProvider:
    if provider_name == "direct":
        return DirectProvider()
    if not api_key:
        env_hint = _PROVIDER_KEY_ENVVARS.get(provider_name, "")
        hint = f"Set {env_hint} or pass --api-key." if env_hint else "Pass --api-key."
        raise ValueError(f"Provider '{provider_name}' requires an API key. {hint}")
    cls_map: dict[str, type[_BaseProvider]] = {
        "scrapingbee": ScrapingBeeProvider,
        "scraperapi":  ScraperAPIProvider,
        "crawlbase":   CrawlbaseProvider,
        "zenrows":     ZenrowsProvider,
    }
    cls = cls_map.get(provider_name)
    if cls is None:
        raise ValueError(
            f"Unknown provider '{provider_name}'. "
            f"Choose from: {', '.join(list(cls_map) + ['direct'])}"
        )
    return cls(
        api_key=api_key,
        render_wait_ms=render_wait_ms,
        country_code=country_code,
        premium_proxy=premium_proxy,
    )


# ------------------------------------------------------------------------------


@dataclass
class ProgressState:
    completed_urls: list[str]


@dataclass
class Product:
    product_key: str
    name: str
    packaging_format: str
    image: str


@dataclass
class ProductPriceSnapshot:
    product_key: str
    supermarket_name: str
    price: str
    unit_price: str
    source_url: str
    scraped_at: str
    promo_price: str = ""
    promo_unit_price: str = ""


@dataclass
class CategoryLink:
    name: str
    url: str
    source_url: str


def load_progress(path: Path) -> ProgressState:
    if not path.exists():
        return ProgressState(completed_urls=[])

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return ProgressState(completed_urls=[])

    completed = data.get("completed_urls", []) if isinstance(data, dict) else []
    if not isinstance(completed, list):
        completed = []
    return ProgressState(completed_urls=[str(item) for item in completed])


def save_progress(path: Path, state: ProgressState) -> None:
    path.write_text(json.dumps({"completed_urls": state.completed_urls}, indent=2), encoding="utf-8")


def write_products(output_path: Path, products: list[Product]) -> None:
    output_path.write_text(json.dumps([asdict(product) for product in products], indent=2), encoding="utf-8")


def write_price_snapshots(output_path: Path, snapshots: list[ProductPriceSnapshot]) -> None:
    output_path.write_text(json.dumps([asdict(snapshot) for snapshot in snapshots], indent=2), encoding="utf-8")


def write_category_links(output_path: Path, categories: list[CategoryLink]) -> None:
    unique_categories: list[CategoryLink] = []
    seen_category_urls: set[str] = set()
    for category in categories:
        if category.url in seen_category_urls:
            continue
        seen_category_urls.add(category.url)
        unique_categories.append(category)
    output_path.write_text(json.dumps([asdict(category) for category in unique_categories], indent=2), encoding="utf-8")


def maybe_persist_to_database(
    args: argparse.Namespace,
    *,
    provider: str,
    mode: str,
    started_at: datetime,
    products: list[Product],
    snapshots: list[ProductPriceSnapshot],
    categories: list[CategoryLink],
) -> None:
    if not args.persist_db:
        return

    database_url = resolve_database_url(args.database_url)
    if not database_url:
        raise SystemExit("--persist-db requires --database-url or DATABASE_URL environment variable.")

    stats = persist_scrape_results(
        database_url=database_url,
        provider=provider,
        mode=mode,
        started_at=started_at,
        products=products,
        snapshots=snapshots,
        categories=categories,
    )
    print(
        "Persisted to DB: "
        f"products={stats.products_upserted}, "
        f"stores={stats.stores_upserted}, "
        f"categories={stats.categories_upserted}, "
        f"snapshots={stats.snapshots_inserted}, "
        f"product_category_links={stats.product_category_links_upserted}"
    )


def load_price_snapshots(path: Path) -> list[ProductPriceSnapshot]:
    if not path.exists():
        return []

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []

    if not isinstance(raw, list):
        return []

    snapshots: list[ProductPriceSnapshot] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        try:
            snapshots.append(ProductPriceSnapshot(**item))
        except Exception:
            continue
    return snapshots


def merge_snapshots(existing: list[ProductPriceSnapshot], new: list[ProductPriceSnapshot]) -> list[ProductPriceSnapshot]:
    merged: list[ProductPriceSnapshot] = []
    seen: set[tuple[str, str, str, str, str, str, str, str]] = set()

    def key(snapshot: ProductPriceSnapshot) -> tuple[str, str, str, str, str, str, str, str]:
        return (
            snapshot.product_key,
            snapshot.scraped_at,
            snapshot.source_url,
            snapshot.price,
            snapshot.unit_price,
            snapshot.promo_price,
            snapshot.promo_unit_price,
            snapshot.supermarket_name,
        )

    for snapshot in existing + new:
        current_key = key(snapshot)
        if current_key in seen:
            continue
        seen.add(current_key)
        merged.append(snapshot)

    return merged


def _product_key(name: str, packaging_format: str) -> str:
    return f"{(name or '').strip()}__{(packaging_format or '').strip()}".lower()


def _replace_last_nth_child(selector: str, nth: int) -> str:
    matches = list(re.finditer(r"nth-child\((\d+)\)", selector))
    if not matches:
        return selector

    last = matches[-1]
    return selector[: last.start(1)] + str(int(nth)) + selector[last.end(1) :]


def _clean_text(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.split()).strip()


def _element_nth_in_parent(card: Tag) -> int:
    if not card.parent:
        return 1
    position = 0
    for child in card.parent.children:
        if isinstance(child, Tag):
            position += 1
            if child is card:
                return max(1, position)
    return 1


def _safe_select_one(root: BeautifulSoup | Tag, selector: str) -> Tag | None:
    if not selector:
        return None
    try:
        return root.select_one(selector)
    except Exception:
        return None


def _text_for_card_selector(soup: BeautifulSoup, card: Tag, selector: str, nth_in_parent: int) -> str:
    if not selector:
        return ""

    local = _safe_select_one(card, selector)
    if local:
        text = _clean_text(local.get_text(" ", strip=True))
        if text:
            return text

    if "nth-child" in selector:
        adjusted = _replace_last_nth_child(selector, nth_in_parent)
        global_el = _safe_select_one(soup, adjusted)
        if global_el:
            text = _clean_text(global_el.get_text(" ", strip=True))
            if text:
                return text

    global_fallback = _safe_select_one(soup, selector)
    if global_fallback:
        return _clean_text(global_fallback.get_text(" ", strip=True))

    return ""


def _get_supermarket_name(soup: BeautifulSoup) -> str:
    marker = _safe_select_one(soup, '[data-testid="choose-store"]')
    if not marker:
        return ""

    text = _clean_text(marker.get_text(" ", strip=True))
    lowered = text.lower()
    if lowered in {"choose store", "choose a store", "select store", "select a store"}:
        return ""
    return text


def _is_bot_challenge(response_status: int | None, title: str, body_preview: str) -> bool:
    return (
        (response_status is not None and response_status == 403)
        or "just a moment" in title
        or "security verification" in body_preview
        or "cloudflare" in body_preview
    )


def _is_rate_limited(response_status: int | None, title: str, body_preview: str) -> bool:
    return (
        response_status == 429
        or "error 1015" in title
        or "rate limited" in title
        or "error 1015" in body_preview
        or "you are being rate limited" in body_preview
    )


def _extract_packaging_format(unit_price: str) -> str:
    value = (unit_price or "").strip()
    if not value:
        return ""

    if "/" in value:
        return value.split("/", 1)[1].strip()

    lower = value.lower()
    if "per " in lower:
        return value[lower.rfind("per ") + 4 :].strip()

    return ""


def _with_page_number(url: str, page_number: int) -> str:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    query["pg"] = [str(page_number)]
    updated_query = urlencode(query, doseq=True)
    return urlunparse(parsed._replace(query=updated_query))


def discover_category_page_urls_from_html(start_url: str, html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    href_elements = soup.select("a[href*='pg=']")

    start_parsed = urlparse(start_url)
    page_numbers: set[int] = set()

    current_pg = parse_qs(start_parsed.query).get("pg", ["1"])[0]
    if str(current_pg).isdigit():
        page_numbers.add(int(current_pg))

    for anchor in href_elements:
        href = anchor.get("href")
        if not href:
            continue

        absolute = urljoin(start_url, str(href))
        parsed = urlparse(absolute)
        if parsed.path != start_parsed.path:
            continue

        pg_values = parse_qs(parsed.query).get("pg", [])
        for value in pg_values:
            if str(value).isdigit():
                page_numbers.add(int(value))

    if not page_numbers:
        return [start_url]

    max_page = max(page_numbers)
    return [_with_page_number(start_url, page_num) for page_num in range(1, max_page + 1)]


def discover_category_urls_from_html(
    start_url: str,
    html: str,
    category_link_selector: str,
    category_name_selector: str,
) -> list[CategoryLink]:
    soup = BeautifulSoup(html, "html.parser")

    category_names = [_clean_text(node.get_text(" ", strip=True)) for node in soup.select(category_name_selector)]
    category_names = [name for name in category_names if name]
    if category_names:
        print(f"Detected category names ({len(category_names)}): {', '.join(category_names[:8])}")

    links = soup.select(category_link_selector)
    if not links:
        links = soup.select("a[href]")

    categories: list[CategoryLink] = []
    seen: set[str] = set()

    for link in links:
        href = link.get("href")
        if not href:
            continue

        absolute = urljoin(start_url, str(href))
        parsed = urlparse(absolute)
        if "/shop/" not in parsed.path:
            continue
        if absolute in seen:
            continue

        seen.add(absolute)
        name = _clean_text(link.get_text(" ", strip=True)) or absolute.rsplit("/", 1)[-1]
        categories.append(CategoryLink(name=name, url=absolute, source_url=start_url))

    return categories


def discover_store_names_from_html(html: str) -> list[str]:
    """Best-effort store name extraction from rendered HTML."""
    soup = BeautifulSoup(html, "html.parser")

    candidates: set[str] = set()

    for node in soup.select("[role='option']"):
        text = _clean_text(node.get_text(" ", strip=True))
        if text:
            candidates.add(text.removesuffix(" Store details").strip())

    for node in soup.find_all(string=re.compile(r"Store details", re.IGNORECASE)):
        text = _clean_text(str(node))
        if not text:
            continue
        cleaned = re.sub(r"\s*Store details\s*$", "", text, flags=re.IGNORECASE).strip()
        if cleaned:
            candidates.add(cleaned)

    filtered = sorted(
        {
            name
            for name in candidates
            if len(name) >= 4
            and "choose" not in name.lower()
            and "search" not in name.lower()
            and "current" not in name.lower()
        }
    )
    return filtered


def _playwright_is_ready() -> bool:
    return async_playwright is not None and Stealth is not None


def _ensure_playwright_available() -> None:
    if _playwright_is_ready():
        return
    raise RuntimeError(
        "Playwright mode requires additional dependencies. Install them with: "
        "pip install playwright-stealth && python -m playwright install firefox"
    )


def _normalize_store_names(raw_values: list[str] | None) -> list[str]:
    normalized: list[str] = []
    if not raw_values:
        return normalized
    for entry in raw_values:
        if not entry:
            continue
        parts = [part.strip() for part in str(entry).split(",")]
        normalized.extend([part for part in parts if part])
    return normalized


async def _playwright_navigate(
    page: Any,
    url: str,
    args: argparse.Namespace,
    purpose: str,
) -> None:
    response = await page.goto(url, wait_until="domcontentloaded", timeout=60000)
    title = _clean_text(await page.title()).lower()
    body_preview = _clean_text((await page.locator("body").inner_text())[:1200]).lower()
    response_status = response.status if response else None

    if _is_rate_limited(response_status, title, body_preview):
        raise RateLimitError(f"Cloudflare rate limit detected while {purpose} (Error 1015/429).")

    challenge_detected = _is_bot_challenge(response_status, title, body_preview)
    if challenge_detected and args.manual_wait_seconds > 0:
        print(
            f"Bot challenge detected while {purpose}. Waiting {args.manual_wait_seconds}s for manual verification..."
        )
        await page.wait_for_timeout(max(0, int(args.manual_wait_seconds)) * 1000)

    if challenge_detected and not args.headed:
        raise RuntimeError(
            "Target page returned a bot challenge (Cloudflare). "
            "Re-run with --provider playwright --headed --manual-wait-seconds 60."
        )


async def choose_store_playwright(
    page: Any,
    start_url: str,
    args: argparse.Namespace,
    store_name: str | None,
    store_index: int,
) -> str:
    await _playwright_navigate(page, start_url, args, "choosing store")

    await page.click(args.store_ribbon_button_selector, timeout=30000)
    try:
        await page.locator(args.store_change_link_selector).click(timeout=15000, force=True)
    except Exception:
        try:
            await page.locator('[data-testid="tooltip-choose-store"]').first.click(timeout=15000)
        except Exception:
            href = await page.locator(args.store_change_link_selector).get_attribute("href")
            if not href:
                raise
            await page.goto(urljoin(page.url, href), wait_until="domcontentloaded", timeout=60000)

    await page.wait_for_load_state("domcontentloaded")
    await page.click(args.store_bar_selector, timeout=30000)
    await page.wait_for_timeout(300)

    if store_name:
        normalized_target = " ".join(store_name.split()).strip().removesuffix(" Store details").strip()
        fallback_target = normalized_target.split(",")[0].strip() if "," in normalized_target else normalized_target
        options = page.locator("[role='option']")
        for _ in range(80):
            filtered = options.filter(has_text=normalized_target)
            if await filtered.count() > 0:
                await filtered.first.click(timeout=30000)
                break

            if fallback_target and fallback_target != normalized_target:
                filtered_fallback = options.filter(has_text=fallback_target)
                if await filtered_fallback.count() > 0:
                    await filtered_fallback.first.click(timeout=30000)
                    break

            await page.keyboard.press("PageDown")
            await page.wait_for_timeout(150)
        else:
            raise RuntimeError(
                f"Could not find store option matching '{normalized_target}'. "
                "Try adjusting --store-name or the store selectors."
            )
    else:
        options = page.locator("[role='option']")
        option_count = await options.count()
        if option_count <= 0:
            raise RuntimeError("Could not find any store options after opening the store list.")
        index = max(0, min(int(store_index), option_count - 1))
        await options.nth(index).click(timeout=30000)

    await page.wait_for_timeout(1000)
    selected = await _get_supermarket_name(BeautifulSoup(await page.content(), "html.parser"))
    if selected:
        print(f"Selected store: {selected}")
    return selected


async def discover_store_names_playwright(page: Any, start_url: str, args: argparse.Namespace) -> list[str]:
    await _playwright_navigate(page, start_url, args, "discovering stores")

    await page.click(args.store_ribbon_button_selector, timeout=30000)
    try:
        await page.locator(args.store_change_link_selector).click(timeout=15000, force=True)
    except Exception:
        try:
            await page.locator('[data-testid="tooltip-choose-store"]').first.click(timeout=15000)
        except Exception:
            href = await page.locator(args.store_change_link_selector).get_attribute("href")
            if not href:
                raise
            await page.goto(urljoin(page.url, href), wait_until="domcontentloaded", timeout=60000)

    await page.wait_for_load_state("domcontentloaded")
    await page.click(args.store_bar_selector, timeout=30000)
    await page.wait_for_timeout(400)

    options = page.locator("[role='option']")
    seen: set[str] = set()
    stagnation = 0
    last_count = 0
    last_seen_size = 0
    for _ in range(60):
        count = await options.count()
        for index in range(count):
            text = _clean_text(await options.nth(index).inner_text())
            if text:
                seen.add(text)
        await page.keyboard.press("PageDown")
        await page.wait_for_timeout(250)
        if count == last_count and len(seen) == last_seen_size:
            stagnation += 1
        else:
            stagnation = 0
        last_count = count
        last_seen_size = len(seen)
        if stagnation >= 6:
            break
    return sorted(seen)


async def count_stores_playwright(page: Any, start_url: str, args: argparse.Namespace) -> int:
    """Count the number of stores available on the fulfillment page by clicking the search bar and counting options."""
    await _playwright_navigate(page, start_url, args, "counting stores")

    await page.click(args.store_ribbon_button_selector, timeout=30000)
    try:
        await page.locator(args.store_change_link_selector).click(timeout=15000, force=True)
    except Exception:
        try:
            await page.locator('[data-testid="tooltip-choose-store"]').first.click(timeout=15000)
        except Exception:
            href = await page.locator(args.store_change_link_selector).get_attribute("href")
            if not href:
                raise
            await page.goto(urljoin(page.url, href), wait_until="domcontentloaded", timeout=60000)

    await page.wait_for_load_state("domcontentloaded")
    await page.click(args.store_bar_selector, timeout=30000)
    await page.wait_for_timeout(400)

    options = page.locator("[role='option']")
    seen: set[str] = set()
    stagnation = 0
    last_count = 0
    last_seen_size = 0
    for _ in range(60):
        count = await options.count()
        for index in range(count):
            text = _clean_text(await options.nth(index).inner_text())
            if text:
                seen.add(text)
        await page.keyboard.press("PageDown")
        await page.wait_for_timeout(250)
        if count == last_count and len(seen) == last_seen_size:
            stagnation += 1
        else:
            stagnation = 0
        last_count = count
        last_seen_size = len(seen)
        if stagnation >= 6:
            break
    return len(seen)


async def discover_category_urls_playwright(
    page: Any,
    start_url: str,
    category_link_selector: str,
    category_name_selector: str,
    args: argparse.Namespace,
) -> list[CategoryLink]:
    await _playwright_navigate(page, start_url, args, "discovering category URLs")
    html = await page.content()
    return discover_category_urls_from_html(
        start_url=start_url,
        html=html,
        category_link_selector=category_link_selector,
        category_name_selector=category_name_selector,
    )


async def discover_category_page_urls_playwright(
    page: Any,
    start_url: str,
    args: argparse.Namespace,
) -> list[str]:
    await _playwright_navigate(page, start_url, args, "discovering paginated category URLs")
    html = await page.content()
    return discover_category_page_urls_from_html(start_url=start_url, html=html)


async def scrape_url_playwright(
    page: Any,
    url: str,
    args: argparse.Namespace,
) -> tuple[list[Product], list[ProductPriceSnapshot]]:
    await _playwright_navigate(page, url, args, f"scraping {url}")
    if args.wait_for_selector:
        await page.wait_for_selector(args.wait_for_selector)
    html = await page.content()
    return scrape_products_from_html(
        html=html,
        url=url,
        product_selector=args.product_selector,
        name_selector=args.name_selector,
        price_selector=args.price_selector,
        price_cents_selector=args.price_cents_selector,
        unit_price_selector=args.unit_price_selector,
        promo_price_dollars_selector=args.promo_price_dollars_selector,
        promo_price_cents_selector=args.promo_price_cents_selector,
        promo_unit_price_selector=args.promo_unit_price_selector,
        image_selector=args.image_selector,
        limit=max(1, int(args.limit)),
        query=args.query,
    )


async def run_playwright_mode(args: argparse.Namespace) -> None:
    _ensure_playwright_available()
    run_started_at = datetime.now(timezone.utc)

    store_names_to_scrape = _normalize_store_names(args.store_names)
    if store_names_to_scrape and args.scrape_all_stores:
        raise SystemExit("Do not combine --store-names with --scrape-all-stores. Use one or the other.")

    if args.headless_only:
        args.headed = False

    progress_path = Path(args.progress_file)
    output_path = Path(args.output)
    price_output_path = Path(args.price_output)
    progress = load_progress(progress_path)
    all_products: list[Product] = []
    all_snapshots: list[ProductPriceSnapshot] = []
    discovered_categories_all: list[CategoryLink] = []
    rate_limit_hit = False

    if args.append_snapshots:
        all_snapshots = load_price_snapshots(price_output_path)

    if args.initial_delay_seconds > 0:
        print(f"Initial delay: waiting {args.initial_delay_seconds:.1f}s before starting...")
        await asyncio.sleep(args.initial_delay_seconds)

    storage_state_path = Path(args.storage_state)
    try:
        browser_launcher = async_playwright()
        async with browser_launcher as playwright:
            browser = await playwright.firefox.launch(headless=not args.headed)
            context = await browser.new_context(
                storage_state=str(storage_state_path) if storage_state_path.exists() else None
            )
            page = await context.new_page()
            await Stealth().apply_stealth_async(page)

            if args.count_stores:
                try:
                    store_count = await count_stores_playwright(page, args.url[0], args)
                    print(f"Total number of stores: {store_count}")
                except Exception as exc:
                    print(f"Error counting stores: {exc}")
                    raise SystemExit(1)
                finally:
                    await context.close()
                    await browser.close()
                return

            if args.scrape_all_stores:
                if args.max_stores is not None and int(args.max_stores) > 0:
                    store_indices: list[int] | None = list(range(int(args.max_stores)))
                    stores: list[str] = []
                else:
                    store_indices = None
                    stores = await discover_store_names_playwright(page, args.url[0], args)
                args.resume = False
            elif store_names_to_scrape:
                store_indices = None
                stores = store_names_to_scrape
                args.resume = False
            else:
                store_indices = None
                stores = []

            if args.choose_store and (not args.scrape_all_stores) and (not store_names_to_scrape):
                await choose_store_playwright(page, args.url[0], args, args.store_name, args.store_index)

            async def resolve_urls() -> list[str]:
                resolved_urls: list[str] = []
                seen_urls: set[str] = set()

                for input_url in args.url:
                    expanded_urls = [input_url]

                    if args.discover_category_urls:
                        try:
                            categories = await discover_category_urls_playwright(
                                page=page,
                                start_url=input_url,
                                category_link_selector=args.category_link_selector,
                                category_name_selector=args.category_name_selector,
                                args=args,
                            )
                        except RateLimitError as exc:
                            print(f"WARNING: {exc}")
                            return []
                        discovered_categories_all.extend(categories)
                        expanded_urls = [item.url for item in categories]
                        print(f"Discovered {len(expanded_urls)} category URLs from: {input_url}")
                        if not expanded_urls:
                            print(
                                "WARNING: No category URLs discovered; falling back to input URL. "
                                "Check --category-link-selector and --category-name-selector."
                            )
                            expanded_urls = [input_url]

                    if args.crawl_category_pages and not args.count_only:
                        paginated_urls: list[str] = []
                        for category_url in expanded_urls:
                            try:
                                pages = await discover_category_page_urls_playwright(page, category_url, args)
                            except RateLimitError as exc:
                                print(f"WARNING: {exc}")
                                return []
                            except Exception as exc:
                                print(f"WARNING: pagination discovery failed for {category_url}: {exc}")
                                pages = [category_url]

                            print(f"Discovered {len(pages)} paginated URLs for category: {category_url}")
                            paginated_urls.extend(pages)

                            if args.delay_seconds > 0:
                                jitter = max(0.0, float(args.delay_jitter_seconds))
                                wait_seconds = max(0.0, float(args.delay_seconds)) + random.random() * jitter
                                await page.wait_for_timeout(int(wait_seconds * 1000))

                        expanded_urls = paginated_urls

                    for expanded_url in expanded_urls:
                        if expanded_url in seen_urls:
                            continue
                        seen_urls.add(expanded_url)
                        resolved_urls.append(expanded_url)

                return resolved_urls

            resolved_urls = await resolve_urls()
            if not resolved_urls and (args.discover_category_urls or args.crawl_category_pages):
                rate_limit_hit = True

            if args.category_output:
                category_output_path = Path(args.category_output)
                write_category_links(category_output_path, discovered_categories_all)
                print(f"Saved {len({category.url for category in discovered_categories_all})} category URLs to {category_output_path}")

            if args.categories_only:
                print("Skipping product scraping because --categories-only was set")
                await context.close()
                await browser.close()
                return

            if args.count_only:
                category_count = len({category.url for category in discovered_categories_all})
                print(f"Resolved {len(resolved_urls)} page URLs to scrape")
                if args.discover_category_urls:
                    print(f"Discovered {category_count} unique category URLs")
                await context.close()
                await browser.close()
                return

            async def scrape_urls_for_current_store(to_scrape: list[str]) -> None:
                nonlocal rate_limit_hit, all_products, all_snapshots

                completed_set = set(progress.completed_urls) if args.resume else set()
                targets = [current for current in to_scrape if current not in completed_set]
                if args.max_pages is not None:
                    targets = targets[: max(0, int(args.max_pages))]
                if args.resume:
                    print(f"Resume enabled: skipping {len(to_scrape) - len(targets)} already-completed URLs")

                for current_url in targets:
                    print(f"Scraping URL: {current_url}")
                    rl_attempt = 0
                    while True:
                        try:
                            products, snapshots = await scrape_url_playwright(page=page, url=current_url, args=args)
                            break
                        except RateLimitError as exc:
                            print(f"WARNING: {exc}")
                            if rl_attempt >= max(0, int(args.max_rate_limit_retries)):
                                rate_limit_hit = True
                                products, snapshots = [], []
                                break
                            rl_attempt += 1
                            wait_seconds = max(0, int(args.rate_limit_wait_seconds))
                            print(f"Waiting {wait_seconds}s then retrying ({rl_attempt}/{args.max_rate_limit_retries})...")
                            await page.wait_for_timeout(wait_seconds * 1000)

                    if rate_limit_hit:
                        break

                    all_products.extend(products)
                    all_snapshots.extend(snapshots)

                    if args.resume:
                        progress.completed_urls.append(current_url)
                        save_progress(progress_path, progress)

                    if args.flush_every_url:
                        write_products(output_path, all_products)
                        if args.append_snapshots:
                            write_price_snapshots(
                                price_output_path,
                                merge_snapshots(load_price_snapshots(price_output_path), all_snapshots),
                            )
                        else:
                            write_price_snapshots(price_output_path, all_snapshots)

                    await context.storage_state(path=str(storage_state_path))

                    if args.delay_seconds > 0:
                        jitter = max(0.0, float(args.delay_jitter_seconds))
                        wait_seconds = max(0.0, float(args.delay_seconds)) + random.random() * jitter
                        await page.wait_for_timeout(int(wait_seconds * 1000))

            if args.scrape_all_stores:
                if store_indices is not None:
                    for index in store_indices:
                        print(f"\n=== Store index: {index} ===")
                        await choose_store_playwright(page, args.url[0], args, None, index)
                        await scrape_urls_for_current_store(resolved_urls)
                        if rate_limit_hit:
                            break
                else:
                    for store in stores:
                        print(f"\n=== Store: {store} ===")
                        await choose_store_playwright(page, args.url[0], args, store, args.store_index)
                        await scrape_urls_for_current_store(resolved_urls)
                        if rate_limit_hit:
                            break
            elif store_names_to_scrape:
                for store in stores:
                    print(f"\n=== Store: {store} ===")
                    await choose_store_playwright(page, args.url[0], args, store, args.store_index)
                    await scrape_urls_for_current_store(resolved_urls)
                    if rate_limit_hit:
                        break
            else:
                await scrape_urls_for_current_store(resolved_urls)

            await context.close()
            await browser.close()
    except Exception:
        raise

    if args.dedupe:
        merged: dict[str, Product] = {}
        merged_order: list[str] = []
        for product in all_products:
            key = product.product_key
            existing = merged.get(key)
            if existing is None:
                merged[key] = product
                merged_order.append(key)
                continue
            if (not existing.image) and product.image:
                existing.image = product.image
        all_products = [merged[key] for key in merged_order]

    write_products(output_path, all_products)
    if args.append_snapshots:
        existing = load_price_snapshots(price_output_path)
        write_price_snapshots(price_output_path, merge_snapshots(existing, all_snapshots))
    else:
        write_price_snapshots(price_output_path, all_snapshots)

    print(f"Saved {len(all_products)} products to {output_path}")
    print(f"Saved {len(all_snapshots)} price snapshots to {price_output_path}")

    maybe_persist_to_database(
        args,
        provider="playwright",
        mode="scrape",
        started_at=run_started_at,
        products=all_products,
        snapshots=all_snapshots,
        categories=discovered_categories_all,
    )

    if rate_limit_hit:
        print("Run stopped early due to rate limiting; partial results were saved.")


def scrape_products_from_html(
    html: str,
    url: str,
    product_selector: str,
    name_selector: str,
    price_selector: str,
    price_cents_selector: str,
    unit_price_selector: str,
    promo_price_dollars_selector: str,
    promo_price_cents_selector: str,
    promo_unit_price_selector: str,
    image_selector: str,
    limit: int,
    query: str | None,
) -> tuple[list[Product], list[ProductPriceSnapshot]]:
    soup = BeautifulSoup(html, "html.parser")
    supermarket_name = _get_supermarket_name(soup)

    cards = soup.select(product_selector)
    query_normalized = query.strip().lower() if query else ""

    products: list[Product] = []
    snapshots: list[ProductPriceSnapshot] = []

    for card in cards:
        if len(products) >= limit:
            break

        nth_in_parent = _element_nth_in_parent(card)

        name_el = _safe_select_one(card, name_selector)
        name = _clean_text(name_el.get_text(" ", strip=True) if name_el else "")

        dollars_el = _safe_select_one(card, price_selector)
        cents_el = _safe_select_one(card, price_cents_selector)
        unit_price_el = _safe_select_one(card, unit_price_selector)

        price_dollars = _clean_text(dollars_el.get_text(" ", strip=True) if dollars_el else "")
        price_cents = _clean_text(cents_el.get_text(" ", strip=True) if cents_el else "")
        unit_price = _clean_text(unit_price_el.get_text(" ", strip=True) if unit_price_el else "")

        promo_price_dollars = _text_for_card_selector(
            soup=soup,
            card=card,
            selector=promo_price_dollars_selector,
            nth_in_parent=nth_in_parent,
        )
        promo_price_cents = _text_for_card_selector(
            soup=soup,
            card=card,
            selector=promo_price_cents_selector,
            nth_in_parent=nth_in_parent,
        )
        promo_unit_price = _text_for_card_selector(
            soup=soup,
            card=card,
            selector=promo_unit_price_selector,
            nth_in_parent=nth_in_parent,
        )

        image = ""
        image_el = _safe_select_one(card, image_selector)
        if image_el:
            src = image_el.get("src") or image_el.get("data-src")
            if not src:
                srcset = image_el.get("srcset")
                if srcset and isinstance(srcset, str):
                    src = srcset.split(",")[0].strip().split(" ")[0].strip()
            if src and isinstance(src, str):
                image = urljoin(url, src)

        cleaned_cents = price_cents.replace(".", "").strip()
        price = f"{price_dollars}.{cleaned_cents}" if price_dollars and cleaned_cents else price_dollars

        promo_price = ""
        promo_cents_cleaned = promo_price_cents.replace(".", "").strip()
        promo_dollars_cleaned = promo_price_dollars.strip()
        if promo_dollars_cleaned and promo_cents_cleaned:
            promo_price = f"{promo_dollars_cleaned}.{promo_cents_cleaned}"
        elif promo_dollars_cleaned:
            promo_price = promo_dollars_cleaned

        if not name:
            continue

        if query_normalized and query_normalized not in name.lower():
            continue

        packaging_format = _extract_packaging_format(unit_price)
        product_key = _product_key(name, packaging_format)
        scraped_at = datetime.now(timezone.utc).isoformat()

        products.append(
            Product(
                product_key=product_key,
                name=name,
                packaging_format=packaging_format,
                image=image,
            )
        )
        snapshots.append(
            ProductPriceSnapshot(
                product_key=product_key,
                supermarket_name=supermarket_name,
                price=price,
                unit_price=unit_price,
                source_url=url,
                scraped_at=scraped_at,
                promo_price=promo_price,
                promo_unit_price=promo_unit_price or "",
            )
        )

    return products, snapshots


async def scrape_url(
    session: aiohttp.ClientSession,
    url: str,
    provider: AnyProvider,
    args: argparse.Namespace,
) -> tuple[list[Product], list[ProductPriceSnapshot]]:
    html, error, status = await provider.fetch(session, url)

    if error:
        err_title = str(error.get("title", "")).lower() if isinstance(error, dict) else ""
        err_body = str(error).lower()
        if _is_rate_limited(status, err_title, err_body):
            raise RateLimitError(f"Cloudflare rate limit detected while scraping {url} (Error 1015/429).")
        _TRANSIENT_SIGNALS = ("timeout", "timed out", "connection", "refused", "reset", "unreachable", "eof")
        if any(t in err_body for t in _TRANSIENT_SIGNALS):
            raise TransientError(f"Transient network error for {url}: {error}")
        print(f"WARNING: request failed for {url}: {error}")
        return [], []

    if not html:
        raise TransientError(f"Empty HTML response for {url}")

    soup = BeautifulSoup(html, "html.parser")
    title = _clean_text(soup.title.get_text(" ", strip=True) if soup.title else "").lower()
    body_preview = _clean_text(soup.get_text(" ", strip=True))[:1200].lower()

    if _is_rate_limited(status, title, body_preview):
        raise RateLimitError(f"Cloudflare rate limit detected while scraping {url} (Error 1015/429).")

    if _is_bot_challenge(status, title, body_preview):
        print("WARNING: bot challenge detected in response HTML; extraction may be incomplete")

    products, snapshots = scrape_products_from_html(
        html=html,
        url=url,
        product_selector=args.product_selector,
        name_selector=args.name_selector,
        price_selector=args.price_selector,
        price_cents_selector=args.price_cents_selector,
        unit_price_selector=args.unit_price_selector,
        promo_price_dollars_selector=args.promo_price_dollars_selector,
        promo_price_cents_selector=args.promo_price_cents_selector,
        promo_unit_price_selector=args.promo_unit_price_selector,
        image_selector=args.image_selector,
        limit=max(1, int(args.limit)),
        query=args.query,
    )
    return products, snapshots


async def fetch_html_or_raise(
    session: aiohttp.ClientSession,
    url: str,
    provider: AnyProvider,
) -> str:
    html, error, status = await provider.fetch(session, url)
    if error:
        title = str(error.get("title", "")).lower() if isinstance(error, dict) else ""
        body = str(error).lower()
        if _is_rate_limited(status, title, body):
            raise RateLimitError(f"Cloudflare rate limit detected while resolving {url} (Error 1015/429).")
        raise RuntimeError(f"Unable to fetch {url}: {error}")

    if not html:
        raise RuntimeError(f"No HTML returned for {url}")

    soup = BeautifulSoup(html, "html.parser")
    title = _clean_text(soup.title.get_text(" ", strip=True) if soup.title else "").lower()
    body_preview = _clean_text(soup.get_text(" ", strip=True))[:1200].lower()

    if _is_rate_limited(status, title, body_preview):
        raise RateLimitError(f"Cloudflare rate limit detected while resolving {url} (Error 1015/429).")

    return html


async def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape New World products")
    parser.add_argument(
        "--url",
        action="append",
        required=True,
        help="Target page URL. Repeat this argument to scrape multiple URLs.",
    )
    parser.add_argument(
        "--product-selector",
        default="div[data-testid^='product-'][data-testid$='-000']",
        help="CSS selector for product cards",
    )
    parser.add_argument(
        "--name-selector",
        default="[data-testid='product-title']",
        help="CSS selector for product name within card",
    )
    parser.add_argument(
        "--price-selector",
        default="[data-testid='price-dollars']",
        help="CSS selector for product price dollars within card",
    )
    parser.add_argument(
        "--price-cents-selector",
        default="[data-testid='price-cents']",
        help="CSS selector for price cents within card",
    )
    parser.add_argument(
        "--unit-price-selector",
        default="[data-testid='non-promo-unit-price']",
        help="CSS selector for unit price (e.g. '$2.99/100g') within card",
    )
    parser.add_argument(
        "--promo-price-dollars-selector",
        default="#search > div > div:nth-child(3) > div > div:nth-child(19) > div._1afq4wy2 > div > div > div > div > div > p",
        help="CSS selector for promo price dollars",
    )
    parser.add_argument(
        "--promo-price-cents-selector",
        default="#search > div > div:nth-child(3) > div > div:nth-child(19) > div._1afq4wy2 > div > div > div > div > div > div > p",
        help="CSS selector for promo price cents",
    )
    parser.add_argument(
        "--promo-unit-price-selector",
        default="#search > div > div:nth-child(3) > div > div:nth-child(19) > div._1afq4wy2 > div > div > div > div > p",
        help="CSS selector for promo unit price",
    )
    parser.add_argument(
        "--image-selector",
        default="[data-testid='product-image']",
        help="CSS selector for product image within card",
    )
    parser.add_argument(
        "--wait-for-selector",
        default=None,
        help="Optional selector to wait for before scraping (Playwright mode only).",
    )
    parser.add_argument("--query", default=None, help="Optional name filter")
    parser.add_argument("--limit", type=int, default=20, help="Maximum products to return per URL")
    parser.add_argument("--output", default="products.json", help="Output JSON file")
    parser.add_argument(
        "--price-output",
        default="price_snapshots.json",
        help="Output JSON file for price snapshots",
    )
    parser.add_argument(
        "--append-snapshots",
        action="store_true",
        help="Append new price snapshots to the existing price output file (keeps history).",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Maximum number of resolved URLs (pages) to scrape.",
    )
    parser.add_argument(
        "--count-only",
        action="store_true",
        help="Resolve category/pagination URLs and print the total without scraping products.",
    )
    parser.add_argument(
        "--count-stores",
        action="store_true",
        help="Count the number of available stores from the fulfillment page and exit.",
    )
    parser.add_argument("--choose-store", action="store_true", help="Choose a store before scraping (Playwright mode only).")
    parser.add_argument("--scrape-all-stores", action="store_true", help="Scrape the same URLs across all discovered stores (Playwright mode only).")
    parser.add_argument("--max-stores", type=int, default=None, help="Optional cap when using --scrape-all-stores in Playwright mode.")
    parser.add_argument("--store-name", default=None, help="Specific store name to select in Playwright mode.")
    parser.add_argument(
        "--store-names",
        action="append",
        default=None,
        help="Specific store name(s) to scrape in Playwright mode. May be repeated or comma-separated.",
    )
    parser.add_argument("--store-index", type=int, default=0, help="Fallback store option index in Playwright mode.")
    parser.add_argument(
        "--store-ribbon-button-selector",
        default="#ribbon > div > div._1dmgezfn9._1dmgezf4i._1dmgezf9.svd0ke7 > button",
        help="CSS selector for the ribbon store button (Playwright mode only).",
    )
    parser.add_argument(
        "--store-change-link-selector",
        default="#ribbon > div > div._1dmgezfn9._1dmgezf4i._1dmgezf9.svd0ke7 > div._1g0swly0._74qpuq0 > div > div > div._1dmgezfmi._1dmgezf9._1dmgezf19 > a:nth-child(2)",
        help="CSS selector for the change-store link (Playwright mode only).",
    )
    parser.add_argument(
        "--store-bar-selector",
        default="#_r_8_",
        help="CSS selector for the store search/selection bar (Playwright mode only).",
    )
    parser.add_argument(
        "--crawl-category-pages",
        action="store_true",
        help="For category URLs with ?pg= pagination, discover all pages and scrape the full category",
    )
    parser.add_argument(
        "--discover-category-urls",
        action="store_true",
        help="Discover category URLs from each input URL before scraping",
    )
    parser.add_argument(
        "--category-link-selector",
        default="a._7zlpdd._7zlpdc",
        help="CSS selector for category links",
    )
    parser.add_argument(
        "--category-name-selector",
        default="button._7zlpdc",
        help="CSS selector for category name buttons",
    )
    parser.add_argument(
        "--category-output",
        default=None,
        help="Optional JSON output file to store discovered category names and URLs",
    )
    parser.add_argument(
        "--categories-only",
        action="store_true",
        help="Only discover/save category URLs and exit without scraping products",
    )
    parser.add_argument(
        "--dedupe",
        action="store_true",
        help="Remove duplicate products by (name, unit_price) across all URLs",
    )
    parser.add_argument("--headed", action="store_true", help="Run Playwright browser in headed mode.")
    parser.add_argument("--headless-only", action="store_true", help="Force Playwright browser to stay headless.")
    parser.add_argument(
        "--manual-wait-seconds",
        type=int,
        default=0,
        help="Manual wait after page load for Cloudflare/store interaction in Playwright mode.",
    )
    parser.add_argument(
        "--delay-seconds",
        type=float,
        default=3.0,
        help="Delay between requests to reduce rate limiting",
    )
    parser.add_argument(
        "--delay-jitter-seconds",
        type=float,
        default=1.0,
        help="Random extra delay (0..jitter) added to each request delay",
    )
    parser.add_argument(
        "--progress-file",
        default="scrape_progress.json",
        help="Path to progress JSON used for resume",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip URLs already recorded in progress file",
    )
    parser.add_argument(
        "--flush-every-url",
        action="store_true",
        help="Write output JSON after each scraped URL",
    )
    parser.add_argument(
        "--max-rate-limit-retries",
        type=int,
        default=0,
        help="How many times to wait and retry when Cloudflare 1015/429 is hit",
    )
    parser.add_argument(
        "--rate-limit-wait-seconds",
        type=int,
        default=300,
        help="Base delay in seconds for first rate-limit retry (subsequent retries use exponential backoff)",
    )
    parser.add_argument(
        "--rate-limit-max-delay-seconds",
        type=int,
        default=600,
        help="Maximum backoff cap in seconds for rate-limit retries",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Maximum retries per URL on transient network errors (timeout, connection reset, empty response)",
    )
    parser.add_argument(
        "--retry-base-delay-seconds",
        type=float,
        default=5.0,
        help="Base delay in seconds for first transient-error retry (subsequent retries use exponential backoff)",
    )
    parser.add_argument(
        "--retry-max-delay-seconds",
        type=float,
        default=120.0,
        help="Maximum backoff cap in seconds for transient-error retries",
    )
    parser.add_argument(
        "--initial-delay-seconds",
        type=float,
        default=0.0,
        help="Wait this many seconds before the first request (useful to stagger parallel GHA runs)",
    )
    parser.add_argument(
        "--provider",
        default="scrapingbee",
        choices=["scrapingbee", "scraperapi", "crawlbase", "zenrows", "direct", "playwright"],
        help="Scraping provider/engine to use (default: scrapingbee).",
    )
    parser.add_argument(
        "--country-code",
        default="nz",
        help="Country code for proxy targeting (default: nz).",
    )
    parser.add_argument(
        "--premium-proxy",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Use premium/residential proxies when supported by the provider (default: true).",
    )
    parser.add_argument(
        "--render-wait-ms",
        type=int,
        default=None,
        dest="render_wait_ms",
        help="Milliseconds to wait for JS rendering (provider-agnostic). Overrides --scrapingbee-wait-ms when set.",
    )
    parser.add_argument(
        "--storage-state",
        default="storage_state.json",
        help="Path to Playwright storage state JSON for cookie reuse (Playwright mode only).",
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help="Scraping provider API key. Alternatively set the provider-specific env var (e.g. SCRAPING_PROVIDER_API_KEY, SCRAPERAPI_KEY, CRAWLBASE_TOKEN, ZENROWS_API_KEY).",
    )
    parser.add_argument(
        "--persist-db",
        action="store_true",
        help="Persist final products and price snapshots to PostgreSQL.",
    )
    parser.add_argument(
        "--database-url",
        default=None,
        help="PostgreSQL connection string. Falls back to DATABASE_URL env var.",
    )
    parser.add_argument(
        "--scrapingbee-wait-ms",
        type=int,
        default=3000,
        help="Deprecated: use --render-wait-ms instead. Kept for backward compatibility.",
    )

    args = parser.parse_args()

    provider_name: str = args.provider
    run_started_at = datetime.now(timezone.utc)
    if provider_name == "playwright":
        await run_playwright_mode(args)
        return

    env_var = _PROVIDER_KEY_ENVVARS.get(provider_name, "")
    api_key: str | None = args.api_key or (os.getenv(env_var) if env_var else None)
    render_wait_ms: int = (
        args.render_wait_ms if args.render_wait_ms is not None else args.scrapingbee_wait_ms
    )
    try:
        provider = build_provider(
            provider_name=provider_name,
            api_key=api_key,
            render_wait_ms=render_wait_ms,
            country_code=args.country_code,
            premium_proxy=args.premium_proxy,
        )
    except ValueError as exc:
        print(f"Error: {exc}")
        raise SystemExit(1)

    print(f"Using scraping provider: {provider_name}")

    unsupported_flags_used = any(
        [
            args.choose_store,
            args.scrape_all_stores,
            bool(args.store_name),
            bool(args.store_names),
            args.max_stores is not None,
            args.headed,
            args.headless_only,
            args.manual_wait_seconds > 0,
        ]
    )
    if unsupported_flags_used:
        print(
            "WARNING: Store interaction and browser-only flags are only supported in --provider playwright "
            "and will be ignored for API-based providers."
        )

    progress_path = Path(args.progress_file)
    output_path = Path(args.output)
    price_output_path = Path(args.price_output)

    progress = load_progress(progress_path)
    all_products: list[Product] = []
    all_snapshots: list[ProductPriceSnapshot] = []
    discovered_categories_all: list[CategoryLink] = []
    rate_limit_hit = False

    if args.append_snapshots:
        all_snapshots = load_price_snapshots(price_output_path)

    if args.initial_delay_seconds > 0:
        print(f"Initial delay: waiting {args.initial_delay_seconds:.1f}s before starting...")
        await asyncio.sleep(args.initial_delay_seconds)

    async with aiohttp.ClientSession() as session:

        if args.count_stores:
            try:
                html = await fetch_html_or_raise(
                    session=session,
                    url=args.url[0],
                    provider=provider,
                )
                store_names = discover_store_names_from_html(html)
                if not store_names:
                    raise RuntimeError(
                        "No store options were detected in provider HTML. "
                        "Try increasing --render-wait-ms (e.g. 8000) or verify provider key/URL."
                    )
                print(f"Total number of stores: {len(store_names)}")
            except Exception as exc:
                print(f"Error counting stores: {exc}")
                raise SystemExit(1)
            return

        async def resolve_urls() -> list[str]: 
            resolved_urls: list[str] = []
            seen_urls: set[str] = set()

            for input_url in args.url:
                expanded_urls = [input_url]

                if args.discover_category_urls:
                    try:
                        html = await fetch_html_or_raise(
                            session=session,
                            url=input_url,
                            provider=provider,
                        )
                        categories = discover_category_urls_from_html(
                            start_url=input_url,
                            html=html,
                            category_link_selector=args.category_link_selector,
                            category_name_selector=args.category_name_selector,
                        )
                    except RateLimitError as exc:
                        print(f"WARNING: {exc}")
                        return []
                    except Exception as exc:
                        print(f"WARNING: category discovery failed for {input_url}: {exc}")
                        categories = []

                    discovered_categories_all.extend(categories)
                    expanded_urls = [item.url for item in categories]
                    print(f"Discovered {len(expanded_urls)} category URLs from: {input_url}")
                    if not expanded_urls:
                        print(
                            "WARNING: No category URLs discovered; falling back to input URL. "
                            "Check --category-link-selector and --category-name-selector."
                        )
                        expanded_urls = [input_url]

                if args.crawl_category_pages and not args.count_only:
                    paginated_urls: list[str] = []
                    for category_url in expanded_urls:
                        try:
                            html = await fetch_html_or_raise(
                                session=session,
                                url=category_url,
                                provider=provider,
                            )
                            pages = discover_category_page_urls_from_html(start_url=category_url, html=html)
                        except RateLimitError as exc:
                            print(f"WARNING: {exc}")
                            return []
                        except Exception as exc:
                            print(f"WARNING: pagination discovery failed for {category_url}: {exc}")
                            pages = [category_url]

                        print(f"Discovered {len(pages)} paginated URLs for category: {category_url}")
                        paginated_urls.extend(pages)

                        if args.delay_seconds > 0:
                            jitter = max(0.0, float(args.delay_jitter_seconds))
                            wait_seconds = max(0.0, float(args.delay_seconds)) + random.random() * jitter
                            await asyncio.sleep(wait_seconds)

                    expanded_urls = paginated_urls

                for expanded_url in expanded_urls:
                    if expanded_url in seen_urls:
                        continue
                    seen_urls.add(expanded_url)
                    resolved_urls.append(expanded_url)

            return resolved_urls

        resolved_urls = await resolve_urls()
        if not resolved_urls and (args.discover_category_urls or args.crawl_category_pages):
            rate_limit_hit = True

        if args.categories_only:
            category_output_path = Path(args.category_output or "category_urls.json")
            write_category_links(category_output_path, discovered_categories_all)
            print(f"Saved {len({category.url for category in discovered_categories_all})} category URLs to {category_output_path}")
            print("Skipping product scraping because --categories-only was set")
            return

        if args.count_only:
            if args.category_output:
                category_output_path = Path(args.category_output)
                write_category_links(category_output_path, discovered_categories_all)
                print(f"Saved {len({category.url for category in discovered_categories_all})} category URLs to {category_output_path}")
            print(f"Resolved {len(resolved_urls)} page URLs to scrape")
            if args.discover_category_urls:
                print(f"Discovered {len({category.url for category in discovered_categories_all})} unique category URLs")
            return

        completed_set = set(progress.completed_urls) if args.resume else set()
        targets = [url for url in resolved_urls if url not in completed_set]
        if args.max_pages is not None:
            targets = targets[: max(0, int(args.max_pages))]
        if args.resume:
            skipped = len(resolved_urls) - len(targets)
            print(f"Resume enabled: skipping {skipped} already-completed URLs")

        for current_url in targets:
            print(f"Scraping URL: {current_url}")
            rl_attempt = 0
            transient_attempt = 0
            products, snapshots = [], []

            while True:
                try:
                    products, snapshots = await scrape_url(
                        session=session,
                        url=current_url,
                        provider=provider,
                        args=args,
                    )
                    break
                except RateLimitError as exc:
                    print(f"WARNING: {exc}")
                    if rl_attempt >= max(0, int(args.max_rate_limit_retries)):
                        rate_limit_hit = True
                        break
                    rl_attempt += 1
                    wait_seconds = _compute_backoff(
                        rl_attempt,
                        base_seconds=float(args.rate_limit_wait_seconds),
                        max_seconds=float(args.rate_limit_max_delay_seconds),
                    )
                    print(
                        f"Rate limit: waiting {wait_seconds:.1f}s then retrying "
                        f"({rl_attempt}/{args.max_rate_limit_retries})..."
                    )
                    await asyncio.sleep(wait_seconds)
                except TransientError as exc:
                    print(f"WARNING: {exc}")
                    if transient_attempt >= max(0, int(args.max_retries)):
                        print(f"Giving up on {current_url} after {transient_attempt} transient retries")
                        break
                    transient_attempt += 1
                    wait_seconds = _compute_backoff(
                        transient_attempt,
                        base_seconds=float(args.retry_base_delay_seconds),
                        max_seconds=float(args.retry_max_delay_seconds),
                    )
                    print(
                        f"Transient error: waiting {wait_seconds:.1f}s then retrying "
                        f"({transient_attempt}/{args.max_retries})..."
                    )
                    await asyncio.sleep(wait_seconds)

            if rate_limit_hit:
                break

            all_products.extend(products)
            all_snapshots.extend(snapshots)

            if args.resume:
                progress.completed_urls.append(current_url)
                save_progress(progress_path, progress)

            if args.flush_every_url:
                write_products(output_path, all_products)
                if args.append_snapshots:
                    write_price_snapshots(
                        price_output_path,
                        merge_snapshots(load_price_snapshots(price_output_path), all_snapshots),
                    )
                else:
                    write_price_snapshots(price_output_path, all_snapshots)

            if args.delay_seconds > 0:
                jitter = max(0.0, float(args.delay_jitter_seconds))
                wait_seconds = max(0.0, float(args.delay_seconds)) + random.random() * jitter
                await asyncio.sleep(wait_seconds)

    if args.dedupe:
        merged: dict[str, Product] = {}
        merged_order: list[str] = []

        for product in all_products:
            key = product.product_key
            existing = merged.get(key)
            if existing is None:
                merged[key] = product
                merged_order.append(key)
                continue
            if (not existing.image) and product.image:
                existing.image = product.image

        all_products = [merged[key] for key in merged_order]

    if args.category_output:
        category_output_path = Path(args.category_output)
        write_category_links(category_output_path, discovered_categories_all)
        print(f"Saved {len({category.url for category in discovered_categories_all})} category URLs to {category_output_path}")

    write_products(output_path, all_products)
    if args.append_snapshots:
        existing = load_price_snapshots(price_output_path)
        write_price_snapshots(price_output_path, merge_snapshots(existing, all_snapshots))
    else:
        write_price_snapshots(price_output_path, all_snapshots)

    print(f"Saved {len(all_products)} products to {output_path}")
    print(f"Saved {len(all_snapshots)} price snapshots to {price_output_path}")

    maybe_persist_to_database(
        args,
        provider=provider_name,
        mode="scrape",
        started_at=run_started_at,
        products=all_products,
        snapshots=all_snapshots,
        categories=discovered_categories_all,
    )

    if rate_limit_hit:
        print("Run stopped early due to rate limiting; partial results were saved.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Interrupted")
        sys.exit(130)
