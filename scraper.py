#!/usr/bin/env python3
"""
NZ supermarket scraper with provider-based fetching and Playwright support for
New World, Pak'nSave, and Woolworths-compatible category pages.
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


_CATEGORY_PATH_PREFIXES = ("/shop/category/", "/shop/browse/")
_DEFAULT_SITE_PROFILE = "newworld"
_SITE_PROFILE_DEFAULTS: dict[str, dict[str, str | None]] = {
    "newworld": {
        "product_selector": "div[data-testid^='product-'][data-testid$='-000']",
        "name_selector": "[data-testid='product-title']",
        "price_selector": "[data-testid='price-dollars']",
        "price_cents_selector": "[data-testid='price-cents']",
        "unit_price_selector": "[data-testid='non-promo-unit-price']",
        "promo_price_dollars_selector": "#search > div > div:nth-child(3) > div > div:nth-child(19) > div._1afq4wy2 > div > div > div > div > div > p",
        "promo_price_cents_selector": "#search > div > div:nth-child(3) > div > div:nth-child(19) > div._1afq4wy2 > div > div > div > div > div > div > p",
        "promo_unit_price_selector": "#search > div > div:nth-child(3) > div > div:nth-child(19) > div._1afq4wy2 > div > div > div > div > p",
        "image_selector": "[data-testid='product-image']",
        "category_link_selector": "a._7zlpdd._7zlpdc, a[href*='/shop/category/']",
        "category_name_selector": "button._7zlpdc, [data-testid='choose-store'], a[href*='/shop/category/']",
        "store_ribbon_button_selector": "[data-testid='choose-store'], #ribbon button[aria-haspopup='dialog'], button[aria-label*='store' i], button[aria-label*='location' i]",
        "store_change_link_selector": "#ribbon a[href], [data-testid='tooltip-choose-store'], a[href*='change-store' i], a[href*='/shop/fulfillment']",
        "store_bar_selector": "#_r_8_, [role='combobox'], input[placeholder*='store' i], input[aria-label*='store' i]",
    },
    "paknsave": {
        "product_selector": "div[data-testid^='product-'][data-testid$='-000']",
        "name_selector": "[data-testid='product-title']",
        "price_selector": "[data-testid='price-dollars']",
        "price_cents_selector": "[data-testid='price-cents']",
        "unit_price_selector": "[data-testid='non-promo-unit-price']",
        "promo_price_dollars_selector": "",
        "promo_price_cents_selector": "",
        "promo_unit_price_selector": "",
        "image_selector": "[data-testid='product-image']",
        "category_link_selector": "a[href*='/shop/category/']",
        "category_name_selector": "button, a[href*='/shop/category/']",
        "store_ribbon_button_selector": "[data-testid='choose-store'], button[aria-label*='store' i], button[aria-label*='location' i], #ribbon button",
        "store_change_link_selector": "[data-testid='tooltip-choose-store'], a[href*='change-store' i], a[href*='/shop/fulfillment']",
        "store_bar_selector": "#_r_8_, [role='combobox'], input[placeholder*='store' i], input[aria-label*='store' i]",
    },
    "woolworths": {
        "product_selector": "product-stamp-grid",
        "name_selector": "h3[id$='-title'], div.product-entry h3",
        "price_selector": "h3[id$='-price'] em, product-price h3 em",
        "price_cents_selector": "h3[id$='-price'] span, product-price h3 span",
        "unit_price_selector": "span.cupPrice, p.price-single-unit-text",
        "promo_price_dollars_selector": "",
        "promo_price_cents_selector": "",
        "promo_unit_price_selector": ".noMemberCupPrice, .previousPrice",
        "image_selector": "a.productImage-container img, figure img",
        "wait_for_selector": "product-stamp-grid, h3[id$='-title']",
        "category_link_selector": "a[href*='/shop/browse/']",
        "category_name_selector": "a[href*='/shop/browse/'], nav a, button span",
        "store_ribbon_button_selector": "button[aria-label*='location' i], button[aria-label*='pick up or delivery' i], [data-testid='select-store'], [data-testid='choose-store']",
        "store_change_link_selector": "a[aria-label*='Change location' i], a[href*='change-location' i], a[href*='store']",
        "store_bar_selector": "[role='combobox'], input[placeholder*='Search' i], input[aria-label*='Search' i]",
        "open_category_menu_selector": "button:has-text('Browse')",
    },
}


def _detect_site_profile(urls: list[str]) -> str:
    for url in urls:
        host = urlparse(str(url)).netloc.lower()
        if "woolworths.co.nz" in host or "countdown.co.nz" in host:
            return "woolworths"
        if "paknsave.co.nz" in host:
            return "paknsave"
        if "newworld.co.nz" in host:
            return "newworld"
    return _DEFAULT_SITE_PROFILE


def _apply_site_profile_defaults(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    requested = getattr(args, "site_profile", "auto")
    profile_name = _detect_site_profile(getattr(args, "url", []) or []) if requested == "auto" else requested
    profile_defaults = _SITE_PROFILE_DEFAULTS.get(profile_name, {})
    args.site_profile = profile_name

    for option_name, option_value in profile_defaults.items():
        if getattr(args, option_name, None) == parser.get_default(option_name):
            setattr(args, option_name, option_value)


def _is_category_like_path(path: str) -> bool:
    normalized_path = str(path or "").rstrip("/") or str(path or "")
    return any(normalized_path.startswith(prefix) for prefix in _CATEGORY_PATH_PREFIXES)


def _is_category_like_url(url: str) -> bool:
    return _is_category_like_path(urlparse(url).path)


def _page_query_name_for_url(url: str) -> str:
    parsed = urlparse(url)
    query = parse_qs(parsed.query, keep_blank_values=True)
    if "page" in query:
        return "page"
    if "pg" in query:
        return "pg"
    host = parsed.netloc.lower()
    if "woolworths.co.nz" in host or "countdown.co.nz" in host or parsed.path.startswith("/shop/browse/"):
        return "page"
    return "pg"


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
            "antibot": "true",
            "premium_proxy": "true" if self.premium_proxy else "false",
            "proxy_country": self.country_code,
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


@dataclass
class CategoryPageCount:
    name: str
    url: str
    page_count: int


def _category_name_from_url(url: str) -> str:
    parsed = urlparse(url)
    slug = parsed.path.rstrip("/").rsplit("/", 1)[-1]
    if not slug:
        return url
    return slug.replace("-", " ").replace("_", " ").strip().title() or url


def print_category_page_counts(category_page_counts: list[CategoryPageCount]) -> None:
    if not category_page_counts:
        print("No category page counts were discovered")
        return

    total_pages = sum(item.page_count for item in category_page_counts)
    print("Category page counts:")
    for item in sorted(category_page_counts, key=lambda current: current.name.lower()):
        print(f"- {item.name}: {item.page_count} page(s) ({item.url})")
    print(f"Total category pages across all categories: {total_pages}")


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


def _select_product_cards(soup: BeautifulSoup, product_selector: str) -> list[Tag]:
    if not product_selector:
        return []
    try:
        candidates = soup.select(product_selector)
    except Exception:
        return []

    candidate_ids = {id(card) for card in candidates}
    selected: list[Tag] = []
    seen_signatures: set[str] = set()
    for card in candidates:
        if any(id(parent) in candidate_ids for parent in card.parents if isinstance(parent, Tag)):
            continue
        signature = f"{card.name}|{_clean_text(card.get_text(' ', strip=True))[:160]}"
        if signature in seen_signatures:
            continue
        seen_signatures.add(signature)
        selected.append(card)
    return selected


def _extract_price_parts(dollars_text: str, cents_text: str) -> tuple[str, str]:
    dollars_match = re.search(r"\d+", str(dollars_text or ""))
    cents_match = re.search(r"\d{1,2}", str(cents_text or "").replace(".", " "))
    return (
        dollars_match.group(0) if dollars_match else "",
        cents_match.group(0) if cents_match else "",
    )


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
    if marker:
        text = _clean_text(marker.get_text(" ", strip=True))
        lowered = text.lower()
        if lowered not in {"choose store", "choose a store", "select store", "select a store"}:
            return text

    body_text = _clean_text(soup.get_text(" ", strip=True))
    for pattern in (
        r"seeing information for the\s+(.+?)\s+area",
        r"current(?:ly)?\s+shopping\s+at\s+(.+?)(?:\.|$)",
        r"selected\s+store\s*[:\-]\s*(.+?)(?:\.|$)",
    ):
        match = re.search(pattern, body_text, re.IGNORECASE)
        if match:
            return _clean_text(match.group(1))

    return ""


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
    query = parse_qs(parsed.query, keep_blank_values=True)
    page_param = _page_query_name_for_url(url)
    query[page_param] = [str(page_number)]
    updated_query = urlencode(query, doseq=True)
    return urlunparse(parsed._replace(query=updated_query))


def _normalize_category_label(value: str) -> str:
    cleaned = _clean_text(value)
    if not cleaned:
        return ""
    cleaned = re.sub(r"^\s*view all\s+", "", cleaned, flags=re.IGNORECASE)
    cleaned = cleaned.replace("&", " and ")
    cleaned = re.sub(r"[^a-z0-9]+", " ", cleaned.lower())
    return re.sub(r"\s+", " ", cleaned).strip()


def _normalize_category_url(url: str) -> str:
    parsed = urlparse(url)
    if not _is_category_like_path(parsed.path):
        return url
    query = parse_qs(parsed.query, keep_blank_values=True)
    page_param = _page_query_name_for_url(url)
    query.setdefault(page_param, ["1"])
    return urlunparse(parsed._replace(query=urlencode(query, doseq=True)))


def _category_candidate_matches(candidate_name: str, candidate_url: str, category_name: str) -> bool:
    target_label = _normalize_category_label(category_name)
    if not target_label:
        return False
    if _normalize_category_label(candidate_name) == target_label:
        return True
    slug = urlparse(candidate_url).path.rstrip("/").rsplit("/", 1)[-1].lower()
    target_slug = target_label.replace(" ", "-")
    return slug == target_slug or slug.startswith(f"{target_slug}-")


def _find_groceries_container(root: BeautifulSoup, category_name_selector: str) -> Tag | BeautifulSoup:
    best_match: tuple[int, Tag] | None = None
    for text_node in root.find_all(string=re.compile(r"^\s*Groceries\s*$", re.IGNORECASE)):
        current = text_node.parent
        while isinstance(current, Tag):
            category_nodes = [
                node
                for node in current.select(category_name_selector)
                if _clean_text(node.get_text(" ", strip=True))
            ]
            category_link_count = len(current.select("a[href*='/shop/category/'], a[href*='/shop/browse/']"))
            if 5 <= len(category_nodes) <= 40 and category_link_count >= 3:
                if best_match is None or len(category_nodes) < best_match[0]:
                    best_match = (len(category_nodes), current)
            current = current.parent
    return best_match[1] if best_match else root


def _find_category_url_for_node(node: Tag, start_url: str, category_name: str) -> str | None:
    search_roots: list[Tag] = []
    if isinstance(node.parent, Tag):
        search_roots.append(node.parent)
        search_roots.extend(sibling for sibling in node.parent.find_next_siblings(limit=4) if isinstance(sibling, Tag))
    search_roots.extend(sibling for sibling in node.find_next_siblings(limit=4) if isinstance(sibling, Tag))

    seen_roots: set[int] = set()
    for root in search_roots:
        root_id = id(root)
        if root_id in seen_roots:
            continue
        seen_roots.add(root_id)

        anchors = root.select("a[href]")
        for prefer_view_all in (True, False):
            for anchor in anchors:
                href = anchor.get("href")
                if not href:
                    continue
                absolute = _normalize_category_url(urljoin(start_url, str(href)))
                parsed = urlparse(absolute)
                if not _is_category_like_path(parsed.path):
                    continue
                anchor_name = _clean_text(anchor.get_text(" ", strip=True)) or _category_name_from_url(absolute)
                is_view_all = bool(re.match(r"^view all\b", anchor_name, flags=re.IGNORECASE))
                if prefer_view_all and not is_view_all:
                    continue
                if _category_candidate_matches(anchor_name, absolute, category_name):
                    return absolute
    return None


_CATEGORY_JSON_NAME_KEYS = ("name", "title", "label", "displayName", "text")
_CATEGORY_JSON_URL_KEYS = ("url", "href", "path")
_CATEGORY_JSON_CHILD_KEYS = (
    "children",
    "items",
    "links",
    "categories",
    "subCategories",
    "subcategories",
    "menuItems",
    "navigation",
)


def _first_category_json_text(item: dict[str, Any]) -> str:
    for key in _CATEGORY_JSON_NAME_KEYS:
        value = item.get(key)
        if isinstance(value, str):
            cleaned = _clean_text(value)
            if cleaned:
                return cleaned
    return ""


def _is_root_category_url(url: str) -> bool:
    parsed = urlparse(url)
    normalized_path = parsed.path.rstrip("/") or parsed.path

    for prefix in _CATEGORY_PATH_PREFIXES:
        if not normalized_path.startswith(prefix):
            continue
        remainder = normalized_path[len(prefix) :].strip("/")
        return bool(remainder) and "/" not in remainder

    return False


def _normalize_category_candidate_url(raw_url: Any, start_url: str) -> str:
    if not isinstance(raw_url, str) or not raw_url.strip():
        return ""
    absolute = _normalize_category_url(urljoin(start_url, raw_url.strip()))
    return absolute if _is_root_category_url(absolute) else ""


def _collect_category_links_from_json(data: Any, start_url: str, out: dict[str, CategoryLink], depth: int = 0) -> None:
    if depth > 30:
        return
    if isinstance(data, list):
        for item in data:
            _collect_category_links_from_json(item, start_url, out, depth + 1)
        return
    if not isinstance(data, dict):
        return

    name = _first_category_json_text(data)
    for key in _CATEGORY_JSON_URL_KEYS:
        absolute = _normalize_category_candidate_url(data.get(key), start_url)
        if not absolute:
            continue
        cleaned_name = _clean_text(re.sub(r"^\s*view all\s+", "", name, flags=re.IGNORECASE))
        out.setdefault(
            absolute,
            CategoryLink(
                name=cleaned_name or _category_name_from_url(absolute),
                url=absolute,
                source_url=start_url,
            ),
        )

    for value in data.values():
        _collect_category_links_from_json(value, start_url, out, depth + 1)


def _iter_category_json_child_lists(item: dict[str, Any]) -> list[list[dict[str, Any]]]:
    child_lists: list[list[dict[str, Any]]] = []
    for key in _CATEGORY_JSON_CHILD_KEYS:
        value = item.get(key)
        if isinstance(value, list):
            children = [child for child in value if isinstance(child, dict)]
            if children:
                child_lists.append(children)
    return child_lists


def _find_matching_category_url_in_json(data: Any, start_url: str, category_name: str) -> str | None:
    candidates_by_url: dict[str, CategoryLink] = {}
    _collect_category_links_from_json(data, start_url, candidates_by_url)
    if not candidates_by_url:
        return None

    candidates = list(candidates_by_url.values())
    for prefer_view_all in (True, False):
        for candidate in candidates:
            is_view_all = bool(re.match(r"^view all\b", candidate.name, flags=re.IGNORECASE))
            if prefer_view_all and not is_view_all:
                continue
            if _category_candidate_matches(candidate.name, candidate.url, category_name):
                return candidate.url

    if len(candidates) == 1:
        return candidates[0].url
    return None


def _discover_groceries_category_links_from_data(
    data: Any,
    start_url: str,
    depth: int = 0,
) -> list[CategoryLink]:
    if depth > 30:
        return []
    if isinstance(data, list):
        for item in data:
            categories = _discover_groceries_category_links_from_data(item, start_url, depth + 1)
            if categories:
                return categories
        return []
    if not isinstance(data, dict):
        return []

    if _normalize_category_label(_first_category_json_text(data)) == "groceries":
        best_score: tuple[int, int, int] | None = None
        best_categories: list[CategoryLink] = []
        for child_list in _iter_category_json_child_lists(data):
            categories: list[CategoryLink] = []
            seen_urls: set[str] = set()
            for child in child_list:
                category_name = _first_category_json_text(child)
                if not category_name or re.match(r"^view all\b", category_name, flags=re.IGNORECASE):
                    continue
                category_url = _find_matching_category_url_in_json(child, start_url, category_name)
                if not category_url or category_url in seen_urls or not _is_root_category_url(category_url):
                    continue
                seen_urls.add(category_url)
                categories.append(CategoryLink(name=category_name, url=category_url, source_url=start_url))

            if not categories:
                continue

            direct_root_count = sum(1 for category in categories if _is_root_category_url(category.url))
            score = (direct_root_count, -abs(len(categories) - 16), -len(categories))
            if best_score is None or score > best_score:
                best_score = score
                best_categories = categories

        if best_categories:
            return best_categories

    for value in data.values():
        categories = _discover_groceries_category_links_from_data(value, start_url, depth + 1)
        if categories:
            return categories
    return []


def discover_category_urls_from_json(start_url: str, html: str) -> list[CategoryLink]:
    soup = BeautifulSoup(html, "html.parser")
    categories_by_url: dict[str, CategoryLink] = {}

    for script_tag in soup.find_all("script"):
        tag_id = script_tag.get("id") or ""
        content = script_tag.string or script_tag.get_text() or ""
        if not content:
            continue
        if tag_id != "__NEXT_DATA__" and "/shop/category/" not in content and "/shop/browse/" not in content:
            continue
        json_text = re.sub(r"^\s*\w[\w.]*\s*=\s*", "", content.strip())
        try:
            data = json.loads(json_text)
        except (json.JSONDecodeError, ValueError):
            continue

        groceries_categories = _discover_groceries_category_links_from_data(data, start_url)
        if groceries_categories:
            return groceries_categories

        _collect_category_links_from_json(data, start_url, categories_by_url)

    categories = list(categories_by_url.values())
    return _maybe_filter_top_level_categories(categories)


_TOP_LEVEL_CATEGORY_LABEL_HINTS = frozenset(
    {
        "easter deals",
        "clubcard be in to win",
        "easter",
        "fruit and vegetables",
        "meat",
        "meat poultry and seafood",
        "poultry and seafood",
        "butchery and seafood",
        "fridge",
        "fridge deli and eggs",
        "deli and eggs",
        "dairy eggs and fridge",
        "bakery",
        "in store bakery",
        "frozen",
        "pantry",
        "hot and cold drinks",
        "drinks",
        "beer",
        "beer wine and cider",
        "beer wine and spirits",
        "wine and spirits",
        "health and body",
        "health and beauty",
        "health beauty and baby",
        "baby",
        "baby and toddler",
        "baby health and beauty",
        "cleaning",
        "cleaning household",
        "cleaning laundry and paper",
        "laundry and paper",
        "household and cleaning",
        "household cleaning and laundry",
        "pets",
        "pet care",
        "snacks treats and easy meals",
        "fruit veg",
        "meat seafood deli",
        "fridge dairy and eggs",
        "freezer",
        "freezer foods",
        "bakery desserts",
        "snacks confectionery",
        "beer cider and wine",
        "health and beauty",
        "cleaning and household",
        "baby and child",
        "fresh foods and bakery",
    }
)


def _is_likely_top_level_category(name: str, url: str) -> bool:
    normalized_name = _normalize_category_label(name)
    normalized_slug = _normalize_category_label(urlparse(url).path.rstrip("/").rsplit("/", 1)[-1])
    return (
        normalized_name in _TOP_LEVEL_CATEGORY_LABEL_HINTS
        or normalized_slug in _TOP_LEVEL_CATEGORY_LABEL_HINTS
    )


def _maybe_filter_top_level_categories(categories: list[CategoryLink]) -> list[CategoryLink]:
    if len(categories) <= 25:
        return categories
    filtered = [category for category in categories if _is_likely_top_level_category(category.name, category.url)]
    return filtered or categories


def _as_positive_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    if isinstance(value, str):
        digits = re.sub(r"[^0-9]", "", value)
        if digits:
            parsed = int(digits)
            return parsed if parsed > 0 else None
    return None


def _collect_page_numbers_from_json(data: Any, page_numbers: set[int], depth: int = 0) -> None:
    if depth > 30:
        return
    if isinstance(data, list):
        for item in data:
            _collect_page_numbers_from_json(item, page_numbers, depth + 1)
        return
    if not isinstance(data, dict):
        return

    normalized_keys = {str(key).lower(): value for key, value in data.items()}

    total_pages_value = _as_positive_int(normalized_keys.get("totalpages") or normalized_keys.get("pagecount"))
    if total_pages_value:
        page_numbers.add(total_pages_value)

    total_items = None
    for key in ("totalitems", "totalproducts", "totalproductcount", "totalresults", "resultcount"):
        value = _as_positive_int(normalized_keys.get(key))
        if value:
            total_items = value
            break

    page_size = None
    for key in ("pagesize", "itemsperpage", "resultsperpage", "perpage", "limit"):
        value = _as_positive_int(normalized_keys.get(key))
        if value:
            page_size = value
            break

    if total_items and page_size:
        page_numbers.add((total_items + page_size - 1) // page_size)

    for value in data.values():
        _collect_page_numbers_from_json(value, page_numbers, depth + 1)


def _collect_page_numbers_from_pagination_elements(soup: BeautifulSoup, page_numbers: set[int]) -> None:
    containers = soup.select(
        '[aria-label*="pagination" i], [data-testid*="pagination" i], nav, ul[class*="pagination" i], div[class*="pagination" i]'
    )
    seen_container_ids: set[int] = set()
    for container in containers:
        container_id = id(container)
        if container_id in seen_container_ids:
            continue
        seen_container_ids.add(container_id)

        text = container.get_text(" ", strip=True)
        label = " ".join(
            filter(
                None,
                [
                    str(container.get("aria-label") or ""),
                    str(container.get("data-testid") or ""),
                    text,
                ],
            )
        )
        if not re.search(r"pagination|page\s*\d+|\b1\b.*\b2\b", label, re.IGNORECASE):
            continue

        for attr_value in container.find_all(attrs={"aria-label": True}):
            match = re.search(r"page\s*(\d+)", str(attr_value.get("aria-label")), re.IGNORECASE)
            if match:
                page_numbers.add(int(match.group(1)))

        numeric_tokens = re.findall(r"\b\d{1,3}\b", text)
        if len(numeric_tokens) >= 2:
            for token in numeric_tokens:
                value = int(token)
                if 1 <= value <= 500:
                    page_numbers.add(value)


def _collect_page_numbers_from_raw_html(html: str, page_numbers: set[int]) -> None:
    variants = {
        html,
        html.replace(r'\/', '/').replace(r'\"', '"').replace('&quot;', '"'),
    }

    total_pages_patterns = (
        r'"?(?:totalPages|pageCount|totalPageCount|numberOfPages|lastPage|pageTotal)"?\s*[:=]\s*"?(\d{1,4})"?',
    )
    total_items_patterns = (
        r'"?(?:totalItems|totalProducts|totalProductCount|totalResults|resultCount|productCount|itemCount)"?\s*[:=]\s*"?(\d{1,6})"?',
    )
    page_size_patterns = (
        r'"?(?:pageSize|itemsPerPage|resultsPerPage|perPage|limit)"?\s*[:=]\s*"?(\d{1,4})"?',
    )

    for text in variants:
        for pattern in total_pages_patterns:
            for match in re.finditer(pattern, text, re.IGNORECASE):
                value = int(match.group(1))
                if 1 <= value <= 500:
                    page_numbers.add(value)

        total_items_values: list[int] = []
        page_size_values: list[int] = []

        for pattern in total_items_patterns:
            for match in re.finditer(pattern, text, re.IGNORECASE):
                value = int(match.group(1))
                if 1 <= value <= 500000:
                    total_items_values.append(value)

        for pattern in page_size_patterns:
            for match in re.finditer(pattern, text, re.IGNORECASE):
                value = int(match.group(1))
                if 1 <= value <= 500:
                    page_size_values.append(value)

        for total_items in total_items_values:
            for page_size in page_size_values:
                if total_items >= page_size:
                    page_numbers.add((total_items + page_size - 1) // page_size)


def discover_category_page_urls_from_html(start_url: str, html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    href_elements = soup.select("a[href*='pg='], a[href*='page=']")

    start_parsed = urlparse(start_url)
    page_numbers: set[int] = set()
    page_param = _page_query_name_for_url(start_url)

    current_page = parse_qs(start_parsed.query).get(page_param, ["1"])[0]
    if str(current_page).isdigit():
        page_numbers.add(int(current_page))

    for anchor in href_elements:
        href = anchor.get("href")
        if not href:
            continue

        absolute = urljoin(start_url, str(href))
        parsed = urlparse(absolute)
        if parsed.path != start_parsed.path:
            continue

        page_values = parse_qs(parsed.query).get(page_param, [])
        for value in page_values:
            if str(value).isdigit():
                page_numbers.add(int(value))

    page_text = soup.get_text(" ", strip=True)
    for pattern in (r"page\s+\d+\s+of\s+(\d+)", r"of\s+(\d+)\s+pages?"):
        for match in re.finditer(pattern, page_text, re.IGNORECASE):
            page_numbers.add(int(match.group(1)))

    for pattern in (
        r"showing\s+(\d+)\s*[-–]\s*(\d+)\s+of\s+(\d+)\s+products?",
        r"(\d+)\s*[-–]\s*(\d+)\s+of\s+(\d+)\s+products?",
        r"showing\s+(\d+)\s+to\s+(\d+)\s+of\s+(\d+)",
    ):
        showing_match = re.search(pattern, page_text, re.IGNORECASE)
        if not showing_match:
            continue
        start_item = int(showing_match.group(1))
        end_item = int(showing_match.group(2))
        total_items = int(showing_match.group(3))
        if end_item >= start_item and total_items >= end_item:
            page_size = max(1, (end_item - start_item) + 1)
            page_numbers.add((total_items + page_size - 1) // page_size)
            break

    query_page_size = None
    for key in ("size", "pageSize", "pagesize", "limit"):
        value = parse_qs(start_parsed.query).get(key, [""])[0]
        if str(value).isdigit():
            query_page_size = int(value)
            break

    if not query_page_size:
        detected_page_size = len(
            _select_product_cards(
                soup,
                "div[data-testid^='product-'][data-testid$='-000'], product-stamp-grid, div.product-entry",
            )
        )
        if detected_page_size > 0:
            query_page_size = detected_page_size

    if query_page_size:
        for match in re.finditer(r"\b(\d{2,6})\s+(?:items|products?)\b", page_text, re.IGNORECASE):
            total_items = int(match.group(1))
            if total_items >= query_page_size:
                page_numbers.add((total_items + query_page_size - 1) // query_page_size)

    _collect_page_numbers_from_pagination_elements(soup, page_numbers)

    for script_tag in soup.find_all("script"):
        content = script_tag.string or script_tag.get_text() or ""
        if not content:
            continue
        if "__NEXT_DATA__" not in content and "totalItems" not in content and "pageSize" not in content:
            tag_id = script_tag.get("id") or ""
            if tag_id != "__NEXT_DATA__":
                continue
        json_text = re.sub(r"^\s*\w[\w.]*\s*=\s*", "", content.strip())
        try:
            data = json.loads(json_text)
        except (json.JSONDecodeError, ValueError):
            continue
        _collect_page_numbers_from_json(data, page_numbers)

    _collect_page_numbers_from_raw_html(html, page_numbers)

    if not page_numbers:
        return [_normalize_category_url(start_url)]

    max_page = max(page_numbers)
    normalized_start_url = _normalize_category_url(start_url)
    return [_with_page_number(normalized_start_url, page_num) for page_num in range(1, max_page + 1)]


def discover_category_urls_from_html(
    start_url: str,
    html: str,
    category_link_selector: str,
    category_name_selector: str,
) -> list[CategoryLink]:
    json_categories = discover_category_urls_from_json(start_url=start_url, html=html)
    if json_categories:
        print(
            f"Discovered category URLs from __NEXT_DATA__ ({len(json_categories)}): "
            f"{', '.join(item.name for item in json_categories[:8])}"
        )
        return json_categories

    soup = BeautifulSoup(html, "html.parser")
    search_root = _find_groceries_container(soup, category_name_selector)

    category_nodes = search_root.select(category_name_selector)
    category_names = [_clean_text(node.get_text(" ", strip=True)) for node in category_nodes]
    category_names = list(dict.fromkeys(name for name in category_names if name))
    if category_names:
        if search_root is not soup:
            print(f"Using Groceries navigation container with {len(category_names)} candidate category buttons")
        print(f"Detected category names ({len(category_names)}): {', '.join(category_names[:8])}")

    links = search_root.select(category_link_selector)
    if not links:
        links = search_root.select("a[href]")

    candidates: list[tuple[str, str, bool]] = []
    for link in links:
        href = link.get("href")
        if not href:
            continue

        absolute = _normalize_category_url(urljoin(start_url, str(href)))
        parsed = urlparse(absolute)
        if not _is_category_like_path(parsed.path):
            continue

        link_name = _clean_text(link.get_text(" ", strip=True)) or _category_name_from_url(absolute)
        is_view_all = bool(re.match(r"^view all\b", link_name, flags=re.IGNORECASE))
        candidates.append((link_name, absolute, is_view_all))

    matched_categories: list[CategoryLink] = []
    seen_urls: set[str] = set()
    for node in category_nodes:
        category_name = _clean_text(node.get_text(" ", strip=True))
        if not category_name:
            continue
        category_url = _find_category_url_for_node(node, start_url, category_name)
        if not category_url or category_url in seen_urls:
            continue
        seen_urls.add(category_url)
        matched_categories.append(CategoryLink(name=category_name, url=category_url, source_url=start_url))

    fallback_threshold = min(8, max(3, len(category_names) // 5)) if category_names else 0
    if candidates and category_names and len(matched_categories) < fallback_threshold:
        global_matches: list[CategoryLink] = []
        global_seen_urls: set[str] = set()
        for category_name in category_names:
            match: tuple[str, str, bool] | None = None
            for prefer_view_all in (True, False):
                for candidate in candidates:
                    candidate_name, candidate_url, is_view_all = candidate
                    if candidate_url in global_seen_urls:
                        continue
                    if prefer_view_all and not is_view_all:
                        continue
                    if _category_candidate_matches(candidate_name, candidate_url, category_name):
                        match = candidate
                        break
                if match:
                    break
            if not match:
                continue
            _, candidate_url, _ = match
            global_seen_urls.add(candidate_url)
            global_matches.append(CategoryLink(name=category_name, url=candidate_url, source_url=start_url))

        if len(global_matches) > len(matched_categories):
            print(
                f"Fallback matched category URLs from shared links ({len(global_matches)}): "
                f"{', '.join(item.name for item in global_matches[:8])}"
            )
            matched_categories = global_matches

    if matched_categories:
        filtered_categories = _maybe_filter_top_level_categories(matched_categories)
        if len(filtered_categories) != len(matched_categories):
            print(
                f"Filtered likely top-level Groceries categories ({len(filtered_categories)}): "
                f"{', '.join(item.name for item in filtered_categories[:8])}"
            )
        print(
            f"Matched top-level category URLs ({len(filtered_categories)}): "
            f"{', '.join(item.name for item in filtered_categories[:8])}"
        )
        return filtered_categories

    categories: list[CategoryLink] = []
    seen: set[str] = set()

    for link_name, absolute, _ in candidates:
        if absolute in seen:
            continue
        seen.add(absolute)
        cleaned_name = _clean_text(re.sub(r"^\s*view all\s+", "", link_name, flags=re.IGNORECASE))
        categories.append(CategoryLink(name=cleaned_name or _category_name_from_url(absolute), url=absolute, source_url=start_url))

    return _maybe_filter_top_level_categories(categories)


def discover_store_names_from_html(html: str) -> list[str]:
    """Best-effort store name extraction from rendered HTML.

    Tries, in order:
    1. Inline JSON from ``__NEXT_DATA__`` script tag (Next.js SSR payload).
    2. Any ``<script>`` tag whose text contains ``storeName`` or ``store_name``.
    3. ``[role='option']`` elements (open React-Select dropdown).
    4. Text nodes containing "Store details" (individual option text).
    """
    soup = BeautifulSoup(html, "html.parser")
    candidates: set[str] = set()

    # ── 1 & 2: Inline JSON extraction ────────────────────────────────────────
    for script_tag in soup.find_all("script"):
        tag_id = script_tag.get("id") or ""
        content = script_tag.string or ""
        if not content:
            continue
        is_next_data = tag_id == "__NEXT_DATA__"
        has_store_hint = is_next_data or bool(
            re.search(r"storeName|store_name|fulfillmentStore", content, re.IGNORECASE)
        )
        if not has_store_hint:
            continue
        # Strip optional JS assignment wrapper: ``window.__X__ = {...}``
        json_text = re.sub(r"^\s*\w[\w.]*\s*=\s*", "", content.strip())
        try:
            data = json.loads(json_text)
            _collect_store_names_from_json(data, candidates)
        except (json.JSONDecodeError, ValueError):
            pass

    # ── 3: role=option (open dropdown) ───────────────────────────────────────
    for node in soup.select("[role='option']"):
        text = _clean_text(node.get_text(" ", strip=True))
        if text:
            candidates.add(text.removesuffix(" Store details").strip())

    # ── 4: "Store details" text nodes ────────────────────────────────────────
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


_STORE_JSON_KEYS = frozenset(
    {"stores", "storelist", "storeoptions", "availablestores", "fulfillmentstores",
     "locations", "picklocation", "pickuplocations", "storelocations"}
)
_STORE_NAME_KEYS = ("name", "storeName", "store_name", "displayName", "label", "title")
_STORE_CITY_KEYS = ("city", "town", "locality")
_STORE_SUBURB_KEYS = ("suburb", "area", "district")
_STORE_ADDRESS_KEYS = (
    "address",
    "address1",
    "addressLine1",
    "streetAddress",
    "formattedAddress",
    "line1",
)


@dataclass(frozen=True)
class StoreRecord:
    name: str
    city: str = ""
    suburb: str = ""
    address: str = ""


def _clean_store_text(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return _clean_text(value)


def _first_store_value(item: dict[str, Any], keys: tuple[str, ...]) -> str:
    for key in keys:
        value = _clean_store_text(item.get(key))
        if value:
            return value
    return ""


def _derive_city_suburb_from_address(address: str) -> tuple[str, str]:
    if not address:
        return "", ""
    parts = [part.strip() for part in address.split(",") if part.strip()]
    if len(parts) >= 3:
        return parts[-2], parts[-3]
    if len(parts) == 2:
        return parts[-2], ""
    return "", ""


def _build_store_record(item: dict[str, Any]) -> StoreRecord | None:
    name = _first_store_value(item, _STORE_NAME_KEYS)
    if not name or len(name) < 4 or re.search(r"[/:?#{}$]", name):
        return None
    address = _first_store_value(item, _STORE_ADDRESS_KEYS)
    city = _first_store_value(item, _STORE_CITY_KEYS)
    suburb = _first_store_value(item, _STORE_SUBURB_KEYS)
    if not city or not suburb:
        derived_city, derived_suburb = _derive_city_suburb_from_address(address)
        if not city:
            city = derived_city
        if not suburb:
            suburb = derived_suburb
    return StoreRecord(name=name, city=city, suburb=suburb, address=address)


def _merge_store_record(existing: StoreRecord, candidate: StoreRecord) -> StoreRecord:
    return StoreRecord(
        name=existing.name,
        city=existing.city or candidate.city,
        suburb=existing.suburb or candidate.suburb,
        address=existing.address or candidate.address,
    )


def _collect_store_records_from_json(
    data: Any,
    out: dict[str, StoreRecord],
    depth: int = 0,
) -> None:
    if depth > 30:
        return
    if isinstance(data, list):
        for item in data:
            _collect_store_records_from_json(item, out, depth + 1)
    elif isinstance(data, dict):
        for key, val in data.items():
            if key.lower() in _STORE_JSON_KEYS and isinstance(val, list):
                for item in val:
                    if not isinstance(item, dict):
                        continue
                    record = _build_store_record(item)
                    if record is not None:
                        existing = out.get(record.name)
                        out[record.name] = _merge_store_record(existing, record) if existing else record
                    _collect_store_records_from_json(item, out, depth + 1)
            else:
                _collect_store_records_from_json(val, out, depth + 1)


def discover_store_records_from_html(html: str) -> list[StoreRecord]:
    """Best-effort store record extraction from rendered HTML."""
    soup = BeautifulSoup(html, "html.parser")
    records_by_name: dict[str, StoreRecord] = {}

    for script_tag in soup.find_all("script"):
        tag_id = script_tag.get("id") or ""
        content = script_tag.string or ""
        if not content:
            continue
        is_next_data = tag_id == "__NEXT_DATA__"
        has_store_hint = is_next_data or bool(
            re.search(r"storeName|store_name|fulfillmentStore", content, re.IGNORECASE)
        )
        if not has_store_hint:
            continue
        json_text = re.sub(r"^\s*\w[\w.]*\s*=\s*", "", content.strip())
        try:
            data = json.loads(json_text)
            _collect_store_records_from_json(data, records_by_name)
        except (json.JSONDecodeError, ValueError):
            pass

    return sorted(records_by_name.values(), key=lambda item: item.name)


def _collect_store_names_from_json(data: Any, out: set[str], depth: int = 0) -> None:
    """Recursively walk a JSON structure collecting values that look like store names."""
    if depth > 30:
        return
    if isinstance(data, list):
        for item in data:
            _collect_store_names_from_json(item, out, depth + 1)
    elif isinstance(data, dict):
        for key, val in data.items():
            # If this key looks like a stores list, harvest names from its items directly.
            if key.lower() in _STORE_JSON_KEYS and isinstance(val, list):
                for item in val:
                    if isinstance(item, dict):
                        for name_key in _STORE_NAME_KEYS:
                            raw = item.get(name_key)
                            if isinstance(raw, str) and 4 <= len(raw) <= 80:
                                if not re.search(r"[/:?#{}$]", raw):
                                    out.add(raw)
                                    break
            else:
                _collect_store_names_from_json(val, out, depth + 1)


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


def _normalize_store_cities(raw_values: list[str] | None) -> list[str]:
    normalized: list[str] = []
    if not raw_values:
        return normalized
    for entry in raw_values:
        if not entry:
            continue
        parts = [part.strip() for part in str(entry).split(",")]
        normalized.extend([part for part in parts if part])
    return normalized


def _filter_store_names_by_city(store_names: list[str], store_cities: list[str]) -> list[str]:
    if not store_cities:
        return sorted(store_names)

    city_tokens = [city.lower() for city in store_cities]
    filtered = [
        store_name
        for store_name in store_names
        if any(token in store_name.lower() for token in city_tokens)
    ]
    return sorted(filtered)


def _filter_store_records_by_city(store_records: list[StoreRecord], store_cities: list[str]) -> list[StoreRecord]:
    if not store_cities:
        return sorted(store_records, key=lambda item: item.name)

    city_tokens = [city.lower() for city in store_cities]
    filtered: list[StoreRecord] = []
    for record in store_records:
        if _store_record_matches_city(record, city_tokens):
            filtered.append(record)
    return sorted(filtered, key=lambda item: item.name)


def _store_record_matches_city(record: StoreRecord, city_tokens: list[str]) -> bool:
    if not city_tokens:
        return True
    if record.city:
        city_value = record.city.lower()
        return any(token in city_value for token in city_tokens)
    if record.suburb:
        suburb_value = record.suburb.lower()
        return any(token in suburb_value for token in city_tokens)
    return False


def _format_store_record_debug(record: StoreRecord) -> str:
    parts = [f"name={record.name}"]
    if record.city:
        parts.append(f"city={record.city}")
    if record.suburb:
        parts.append(f"suburb={record.suburb}")
    if record.address:
        parts.append(f"address={record.address}")
    return ", ".join(parts)


def _print_store_record_debug(records: list[StoreRecord], store_cities: list[str]) -> None:
    if not records:
        return

    city_tokens = [city.lower() for city in store_cities]
    for record in records:
        matched = _store_record_matches_city(record, city_tokens)
        status = "matched" if matched else "skipped"
        print(f"[store:{status}] {_format_store_record_debug(record)}", flush=True)


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
        title = _clean_text(await page.title()).lower()
        body_preview = _clean_text((await page.locator("body").inner_text())[:1200]).lower()
        challenge_detected = _is_bot_challenge(response_status, title, body_preview)

    if challenge_detected:
        remediation = "Re-run with --provider playwright --headed --manual-wait-seconds 60"
        if args.headed:
            remediation += " after completing the browser verification"
        remediation += ", or use a supported API provider."
        raise RuntimeError(
            "Target page returned a bot challenge (Cloudflare). " + remediation
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
    store_cities = _normalize_store_cities(args.store_city)
    return _filter_store_names_by_city(list(seen), store_cities)


async def count_stores_playwright(page: Any, start_url: str, args: argparse.Namespace) -> int:
    """Count stores by extracting __NEXT_DATA__ from the fulfillment page HTML."""
    await _playwright_navigate(page, start_url, args, "counting stores")
    # Wait up to 30s for __NEXT_DATA__ to appear (Cloudflare JS challenge may redirect first)
    try:
        await page.wait_for_function(
            "() => !!document.getElementById('__NEXT_DATA__')",
            timeout=30000,
        )
    except Exception:
        pass
    html = await page.content()
    store_records = discover_store_records_from_html(html)
    store_names = [record.name for record in store_records] or discover_store_names_from_html(html)
    if not store_names:
        raise RuntimeError(
            "No store options were detected in Playwright HTML. "
            f"[diag] HTML length={len(html)} has_next_data={'__NEXT_DATA__' in html}"
        )
    store_cities = _normalize_store_cities(args.store_city)
    _print_store_record_debug(store_records, store_cities)
    if store_records:
        filtered = [record.name for record in _filter_store_records_by_city(store_records, store_cities)]
    else:
        filtered = _filter_store_names_by_city(store_names, store_cities)
    if store_cities:
        print(f"City filter applied ({', '.join(store_cities)}): {len(filtered)} stores matched")
    return len(filtered)


async def discover_category_urls_playwright(
    page: Any,
    start_url: str,
    category_link_selector: str,
    category_name_selector: str,
    args: argparse.Namespace,
) -> list[CategoryLink]:
    await _playwright_navigate(page, start_url, args, "discovering category URLs")
    if getattr(args, "open_category_menu_selector", None):
        try:
            await page.locator(args.open_category_menu_selector).first.click(timeout=5000)
            await page.wait_for_timeout(750)
        except Exception:
            pass
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
    store_cities = _normalize_store_cities(args.store_city)
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
    category_page_counts_all: list[CategoryPageCount] = []
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
                    if store_cities:
                        print(f"City filter applied ({', '.join(store_cities)}): {len(stores)} stores matched")
                    if not stores:
                        raise SystemExit(
                            "No stores matched --store-city filter. "
                            "Try a different city or remove --store-city."
                        )
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
                    categories_for_pagination: list[CategoryLink] = [
                        CategoryLink(
                            name=_category_name_from_url(input_url),
                            url=input_url,
                            source_url=input_url,
                        )
                    ]

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
                        categories_for_pagination = categories or categories_for_pagination
                        expanded_urls = [item.url for item in categories]
                        print(f"Discovered {len(expanded_urls)} category URLs from: {input_url}")
                        if not expanded_urls:
                            print(
                                "WARNING: No category URLs discovered; falling back to input URL. "
                                "Check --category-link-selector and --category-name-selector."
                            )
                            expanded_urls = [input_url]

                    if args.crawl_category_pages or args.count_category_pages:
                        paginated_urls: list[str] = []
                        for category in categories_for_pagination:
                            category_url = category.url
                            try:
                                pages = await discover_category_page_urls_playwright(page, category_url, args)
                            except RateLimitError as exc:
                                print(f"WARNING: {exc}")
                                return []
                            except Exception as exc:
                                print(f"WARNING: pagination discovery failed for {category_url}: {exc}")
                                pages = [category_url]

                            print(f"Discovered {len(pages)} paginated URLs for category: {category_url}")
                            if args.count_category_pages:
                                category_page_counts_all.append(
                                    CategoryPageCount(name=category.name, url=category_url, page_count=len(pages))
                                )
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
                if args.count_category_pages:
                    print_category_page_counts(category_page_counts_all)
                    print(
                        "Count summary: "
                        f"categories={category_count}, "
                        f"pages={sum(item.page_count for item in category_page_counts_all)}"
                    )
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

    cards = _select_product_cards(soup, product_selector)
    query_normalized = query.strip().lower() if query else ""

    products: list[Product] = []
    snapshots: list[ProductPriceSnapshot] = []
    page_scraped_at = datetime.now(timezone.utc).isoformat()

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

        price_dollars_clean, cleaned_cents = _extract_price_parts(price_dollars, price_cents)
        price = f"{price_dollars_clean}.{cleaned_cents.zfill(2)}" if price_dollars_clean and cleaned_cents else price_dollars_clean

        promo_price = ""
        promo_dollars_cleaned, promo_cents_cleaned = _extract_price_parts(promo_price_dollars, promo_price_cents)
        if promo_dollars_cleaned and promo_cents_cleaned:
            promo_price = f"{promo_dollars_cleaned}.{promo_cents_cleaned.zfill(2)}"
        elif promo_dollars_cleaned:
            promo_price = promo_dollars_cleaned

        if not name:
            continue

        if query_normalized and query_normalized not in name.lower():
            continue

        packaging_format = _extract_packaging_format(unit_price)
        product_key = _product_key(name, packaging_format)

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
                scraped_at=page_scraped_at,
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

    if _is_bot_challenge(status, title, body_preview):
        raise RuntimeError(f"Bot challenge detected while resolving {url}; returned HTML is not the real category page")

    return html


async def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape NZ supermarket products")
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
    parser.add_argument(
        "--open-category-menu-selector",
        default=None,
        help="Optional Playwright selector to open a browse/category menu before discovering category URLs.",
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
        "--count-category-pages",
        action="store_true",
        help="Discover and print the number of paginated pages for each category, then exit.",
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
    parser.add_argument(
        "--store-city",
        action="append",
        default=None,
        help="Filter stores by city name (case-insensitive). May be repeated or comma-separated.",
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
        "--site-profile",
        default="auto",
        choices=["auto", "newworld", "paknsave", "woolworths"],
        help="Auto-detect selector defaults from the URL, or force a supermarket-specific profile.",
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
    if args.count_category_pages:
        args.count_only = True
    _apply_site_profile_defaults(args, parser)

    provider_name: str = args.provider
    run_started_at = datetime.now(timezone.utc)
    print(f"Using retailer profile: {args.site_profile}")
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
    category_page_counts_all: list[CategoryPageCount] = []
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
                _has_role_opt = bool(re.search(r"role=[\"']option", html))
                _has_store_name = bool(re.search(r"storeName|store_name", html, re.IGNORECASE))
                _diag = (
                    f"[diag] HTML length={len(html)} "
                    f"has_next_data={'__NEXT_DATA__' in html} "
                    f"has_role_option={_has_role_opt} "
                    f"has_store_details={'Store details' in html} "
                    f"has_storeName={_has_store_name}"
                )
                print(_diag, flush=True)
                store_records = discover_store_records_from_html(html)
                store_names = [record.name for record in store_records] or discover_store_names_from_html(html)
                store_cities = _normalize_store_cities(args.store_city)
                _print_store_record_debug(store_records, store_cities)
                if not store_names:
                    raise RuntimeError(
                        "No store options were detected in provider HTML. "
                        "Try increasing --render-wait-ms or verify provider key/URL. "
                        + _diag
                    )
                if store_records:
                    store_names = [
                        record.name for record in _filter_store_records_by_city(store_records, store_cities)
                    ]
                else:
                    store_names = _filter_store_names_by_city(store_names, store_cities)
                if store_cities and not store_names:
                    raise RuntimeError(
                        "No stores matched --store-city filter. "
                        "Try a different city or remove --store-city."
                    )
                if store_cities:
                    print(f"City filter applied ({', '.join(store_cities)}): {len(store_names)} stores matched")
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
                categories_for_pagination: list[CategoryLink] = [
                    CategoryLink(
                        name=_category_name_from_url(input_url),
                        url=input_url,
                        source_url=input_url,
                    )
                ]

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
                    categories_for_pagination = categories or categories_for_pagination
                    expanded_urls = [item.url for item in categories]
                    print(f"Discovered {len(expanded_urls)} category URLs from: {input_url}")
                    if not expanded_urls:
                        print(
                            "WARNING: No category URLs discovered; falling back to input URL. "
                            "Check --category-link-selector and --category-name-selector."
                        )
                        expanded_urls = [input_url]

                if args.crawl_category_pages or args.count_category_pages:
                    paginated_urls: list[str] = []
                    for category in categories_for_pagination:
                        category_url = category.url
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
                        if args.count_category_pages:
                            category_page_counts_all.append(
                                CategoryPageCount(name=category.name, url=category_url, page_count=len(pages))
                            )
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
            category_count = len({category.url for category in discovered_categories_all})
            if args.count_category_pages:
                print_category_page_counts(category_page_counts_all)
                print(
                    "Count summary: "
                    f"categories={category_count}, "
                    f"pages={sum(item.page_count for item in category_page_counts_all)}"
                )
            print(f"Resolved {len(resolved_urls)} page URLs to scrape")
            if args.discover_category_urls:
                print(f"Discovered {category_count} unique category URLs")
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
