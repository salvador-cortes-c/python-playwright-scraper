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

SCRAPINGBEE_API_URL = "https://app.scrapingbee.com/api/v1/"
SCRAPINGBEE_API_KEY_ENV = "SCRAPINGBEE_API_KEY"


class RateLimitError(RuntimeError):
    pass


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


async def fetch_with_scrapingbee(
    session: aiohttp.ClientSession,
    url: str,
    api_key: str,
    wait_ms: int,
) -> tuple[Optional[str], Optional[dict[str, Any]], int | None]:
    params = {
        "api_key": api_key,
        "url": url,
        "render_js": "true",
        "premium_proxy": "true",
        "country_code": "nz",
        "wait": str(max(0, int(wait_ms))),
        "block_ads": "true",
        "block_resources": "false",
        "return_page_source": "true",
    }

    try:
        async with session.get(SCRAPINGBEE_API_URL, params=params, timeout=60) as response:
            status = response.status
            text = await response.text()

            if status != 200:
                try:
                    payload = await response.json()
                except Exception:
                    payload = {"error": f"HTTP {status}", "response": text[:300]}
                return None, payload, status

            if "<html" in text.lower() or "<!doctype" in text.lower():
                return text, None, status

            try:
                payload = await response.json()
                return None, payload, status
            except Exception:
                return None, {"error": "Invalid non-HTML response", "response": text[:300]}, status

    except asyncio.TimeoutError:
        return None, {"error": "Timeout after 60 seconds"}, None
    except Exception as exc:
        return None, {"error": str(exc)}, None


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
    api_key: str,
    wait_ms: int,
    args: argparse.Namespace,
) -> tuple[list[Product], list[ProductPriceSnapshot]]:
    html, error, status = await fetch_with_scrapingbee(session=session, url=url, api_key=api_key, wait_ms=wait_ms)

    if error:
        err_title = str(error.get("title", "")).lower() if isinstance(error, dict) else ""
        err_body = str(error).lower()
        if _is_rate_limited(status, err_title, err_body):
            raise RateLimitError(f"Cloudflare rate limit detected while scraping {url} (Error 1015/429).")
        print(f"WARNING: request failed for {url}: {error}")
        return [], []

    if not html:
        print(f"WARNING: empty HTML for {url}")
        return [], []

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
    api_key: str,
    wait_ms: int,
) -> str:
    html, error, status = await fetch_with_scrapingbee(session=session, url=url, api_key=api_key, wait_ms=wait_ms)
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
    parser = argparse.ArgumentParser(description="Scrape New World products using ScrapingBee")
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
    parser.add_argument("--wait-for-selector", default=None, help="Compatibility flag; not used in ScrapingBee mode")
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
    parser.add_argument("--choose-store", action="store_true", help="Compatibility flag; unsupported in ScrapingBee mode")
    parser.add_argument("--scrape-all-stores", action="store_true", help="Compatibility flag; unsupported in ScrapingBee mode")
    parser.add_argument("--max-stores", type=int, default=None, help="Compatibility flag; unsupported in ScrapingBee mode")
    parser.add_argument("--store-name", default=None, help="Compatibility flag; unsupported in ScrapingBee mode")
    parser.add_argument(
        "--store-names",
        action="append",
        default=None,
        help="Compatibility flag; unsupported in ScrapingBee mode",
    )
    parser.add_argument("--store-index", type=int, default=0, help="Compatibility flag; unsupported in ScrapingBee mode")
    parser.add_argument(
        "--store-ribbon-button-selector",
        default="#ribbon > div > div._1dmgezfn9._1dmgezf4i._1dmgezf9.svd0ke7 > button",
        help="Compatibility flag; unsupported in ScrapingBee mode",
    )
    parser.add_argument(
        "--store-change-link-selector",
        default="#ribbon > div > div._1dmgezfn9._1dmgezf4i._1dmgezf9.svd0ke7 > div._1g0swly0._74qpuq0 > div > div > div._1dmgezfmi._1dmgezf9._1dmgezf19 > a:nth-child(2)",
        help="Compatibility flag; unsupported in ScrapingBee mode",
    )
    parser.add_argument(
        "--store-bar-selector",
        default="#_r_8_",
        help="Compatibility flag; unsupported in ScrapingBee mode",
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
    parser.add_argument("--headed", action="store_true", help="Compatibility flag; unsupported in ScrapingBee mode")
    parser.add_argument("--headless-only", action="store_true", help="Compatibility flag; unsupported in ScrapingBee mode")
    parser.add_argument(
        "--manual-wait-seconds",
        type=int,
        default=0,
        help="Compatibility flag; unsupported in ScrapingBee mode",
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
        help="Seconds to wait before retrying after rate limit (if retries enabled)",
    )
    parser.add_argument(
        "--storage-state",
        default="storage_state.json",
        help="Compatibility flag; not used in ScrapingBee mode",
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help="ScrapingBee API key (or set SCRAPINGBEE_API_KEY env var)",
    )
    parser.add_argument(
        "--scrapingbee-wait-ms",
        type=int,
        default=3000,
        help="Milliseconds for ScrapingBee to wait after render_js",
    )

    args = parser.parse_args()

    api_key = args.api_key or os.getenv(SCRAPINGBEE_API_KEY_ENV)
    if not api_key:
        print("Error: ScrapingBee API key required")
        print("Set SCRAPINGBEE_API_KEY or pass --api-key")
        raise SystemExit(1)

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
            "WARNING: Store interaction and browser-only challenge handling flags are not supported in "
            "ScrapingBee mode and will be ignored."
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

    async with aiohttp.ClientSession() as session:

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
                            api_key=api_key,
                            wait_ms=args.scrapingbee_wait_ms,
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

                if args.crawl_category_pages:
                    paginated_urls: list[str] = []
                    for category_url in expanded_urls:
                        try:
                            html = await fetch_html_or_raise(
                                session=session,
                                url=category_url,
                                api_key=api_key,
                                wait_ms=args.scrapingbee_wait_ms,
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
            unique_categories: list[CategoryLink] = []
            seen_category_urls: set[str] = set()
            for category in discovered_categories_all:
                if category.url in seen_category_urls:
                    continue
                seen_category_urls.add(category.url)
                unique_categories.append(category)
            category_output_path.write_text(
                json.dumps([asdict(category) for category in unique_categories], indent=2),
                encoding="utf-8",
            )
            print(f"Saved {len(unique_categories)} category URLs to {category_output_path}")
            print("Skipping product scraping because --categories-only was set")
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
            attempt = 0

            while True:
                try:
                    products, snapshots = await scrape_url(
                        session=session,
                        url=current_url,
                        api_key=api_key,
                        wait_ms=args.scrapingbee_wait_ms,
                        args=args,
                    )
                    break
                except RateLimitError as exc:
                    print(f"WARNING: {exc}")
                    if attempt >= max(0, int(args.max_rate_limit_retries)):
                        rate_limit_hit = True
                        products, snapshots = [], []
                        break
                    attempt += 1
                    wait_seconds = max(0, int(args.rate_limit_wait_seconds))
                    print(f"Waiting {wait_seconds}s then retrying ({attempt}/{args.max_rate_limit_retries})...")
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
        unique_categories: list[CategoryLink] = []
        seen_category_urls: set[str] = set()
        for category in discovered_categories_all:
            if category.url in seen_category_urls:
                continue
            seen_category_urls.add(category.url)
            unique_categories.append(category)
        category_output_path.write_text(
            json.dumps([asdict(category) for category in unique_categories], indent=2),
            encoding="utf-8",
        )
        print(f"Saved {len(unique_categories)} category URLs to {category_output_path}")

    write_products(output_path, all_products)
    if args.append_snapshots:
        existing = load_price_snapshots(price_output_path)
        write_price_snapshots(price_output_path, merge_snapshots(existing, all_snapshots))
    else:
        write_price_snapshots(price_output_path, all_snapshots)

    print(f"Saved {len(all_products)} products to {output_path}")
    print(f"Saved {len(all_snapshots)} price snapshots to {price_output_path}")
    if rate_limit_hit:
        print("Run stopped early due to rate limiting; partial results were saved.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Interrupted")
        sys.exit(130)
