from __future__ import annotations

import argparse
import asyncio
import json
import random
from datetime import datetime, timezone
from dataclasses import asdict, dataclass
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

from playwright.async_api import async_playwright


class RateLimitError(RuntimeError):
    pass


@dataclass
class ProgressState:
    completed_urls: list[str]


def load_progress(path: Path) -> ProgressState:
    if not path.exists():
        return ProgressState(completed_urls=[])
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        completed = data.get("completed_urls", [])
        if not isinstance(completed, list):
            completed = []
        completed_urls = [str(u) for u in completed]
        return ProgressState(completed_urls=completed_urls)
    except Exception:
        return ProgressState(completed_urls=[])


def save_progress(path: Path, state: ProgressState) -> None:
    path.write_text(
        json.dumps({"completed_urls": state.completed_urls}, indent=2),
        encoding="utf-8",
    )


def write_products(output_path: Path, products: list[Product]) -> None:
    output_path.write_text(
        json.dumps([asdict(product) for product in products], indent=2),
        encoding="utf-8",
    )


def write_price_snapshots(output_path: Path, snapshots: list[ProductPriceSnapshot]) -> None:
    output_path.write_text(
        json.dumps([asdict(snapshot) for snapshot in snapshots], indent=2),
        encoding="utf-8",
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
    seen: set[tuple[str, str, str, str, str, str]] = set()

    def key(snapshot: ProductPriceSnapshot) -> tuple[str, str, str, str, str, str]:
        return (
            snapshot.product_key,
            snapshot.scraped_at,
            snapshot.source_url,
            snapshot.price,
            snapshot.unit_price,
            snapshot.supermarket_name,
        )

    for snapshot in existing + new:
        snapshot_key = key(snapshot)
        if snapshot_key in seen:
            continue
        seen.add(snapshot_key)
        merged.append(snapshot)

    return merged


def _product_key(name: str, packaging_format: str) -> str:
    return f"{(name or '').strip()}__{(packaging_format or '').strip()}".lower()


@dataclass
class ProductPriceSnapshot:
    product_key: str
    supermarket_name: str
    price: str
    unit_price: str
    source_url: str
    scraped_at: str


async def _get_supermarket_name(page) -> str:
    try:
        locator = page.locator('[data-testid="choose-store"]').first
        if await locator.count() == 0:
            return ""
        text = (await locator.inner_text(timeout=2000)).strip()
        normalized = " ".join(text.split())
        if normalized.lower() in {"choose store", "choose a store", "select store", "select a store"}:
            return ""
        return normalized
    except Exception:
        return ""


@dataclass
class Product:
    product_key: str
    name: str
    packaging_format: str
    image: str


@dataclass
class CategoryLink:
    name: str
    url: str
    source_url: str


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


async def choose_store(
    page,
    start_url: str,
    headless: bool,
    manual_wait_seconds: int,
    ribbon_button_selector: str,
    change_store_link_selector: str,
    store_bar_selector: str,
    store_name: str | None,
    store_index: int,
) -> str:
    response = await page.goto(start_url, wait_until="domcontentloaded", timeout=60000)
    title = (await page.title()).strip().lower()
    body_preview = (await page.locator("body").inner_text())[:1000].lower()
    challenge_detected = _is_bot_challenge(
        response.status if response else None,
        title,
        body_preview,
    )
    rate_limited = _is_rate_limited(
        response.status if response else None,
        title,
        body_preview,
    )

    if rate_limited:
        raise RateLimitError("Cloudflare rate limit detected while choosing store (Error 1015/429).")

    if challenge_detected and manual_wait_seconds > 0:
        print(
            f"Bot challenge detected. Waiting {manual_wait_seconds}s for manual verification in browser..."
        )
        await page.wait_for_timeout(manual_wait_seconds * 1000)

    if challenge_detected and headless:
        raise RuntimeError(
            "Target page returned a bot challenge (Cloudflare). "
            "Re-run with --headed and --manual-wait-seconds 60, then complete verification manually."
        )

    await page.click(ribbon_button_selector, timeout=30000)

    # The "Change store" anchor can be present but have pointer-events intercepted by a tooltip button.
    # Try: click link (force), then click tooltip button, then direct navigation.
    try:
        await page.locator(change_store_link_selector).click(timeout=15000, force=True)
    except Exception:
        try:
            await page.locator('[data-testid="tooltip-choose-store"]').first.click(timeout=15000)
        except Exception:
            href = await page.locator(change_store_link_selector).get_attribute("href")
            if href:
                await page.goto(urljoin(page.url, href), wait_until="domcontentloaded", timeout=60000)
            else:
                raise

    await page.wait_for_load_state("domcontentloaded")

    await page.click(store_bar_selector, timeout=30000)
    await page.wait_for_timeout(300)

    if store_name:
        await page.get_by_text(store_name, exact=True).click(timeout=30000)
    else:
        options = page.locator("[role='option']")
        option_count = await options.count()
        if option_count <= 0:
            raise RuntimeError(
                "Could not find any store options after opening the store list. "
                "Provide --store-name or update the selectors."
            )
        index = max(0, min(int(store_index), option_count - 1))
        await options.nth(index).click(timeout=30000)

    await page.wait_for_timeout(1000)
    selected = await _get_supermarket_name(page)
    if selected:
        print(f"Selected store: {selected}")
    else:
        print("Selected store (name not detected).")
    return selected


async def discover_store_names(
    page,
    start_url: str,
    headless: bool,
    manual_wait_seconds: int,
    ribbon_button_selector: str,
    change_store_link_selector: str,
    store_bar_selector: str,
) -> list[str]:
    response = await page.goto(start_url, wait_until="domcontentloaded", timeout=60000)
    title = (await page.title()).strip().lower()
    body_preview = (await page.locator("body").inner_text())[:1000].lower()
    challenge_detected = _is_bot_challenge(
        response.status if response else None,
        title,
        body_preview,
    )
    rate_limited = _is_rate_limited(
        response.status if response else None,
        title,
        body_preview,
    )

    if rate_limited:
        raise RateLimitError("Cloudflare rate limit detected while discovering stores (Error 1015/429).")

    if challenge_detected and manual_wait_seconds > 0:
        print(
            f"Bot challenge detected. Waiting {manual_wait_seconds}s for manual verification in browser..."
        )
        await page.wait_for_timeout(manual_wait_seconds * 1000)

    if challenge_detected and headless:
        raise RuntimeError(
            "Target page returned a bot challenge (Cloudflare). "
            "Re-run with --headed and --manual-wait-seconds 60, then complete verification manually."
        )

    await page.click(ribbon_button_selector, timeout=30000)

    try:
        await page.locator(change_store_link_selector).click(timeout=15000, force=True)
    except Exception:
        try:
            await page.locator('[data-testid="tooltip-choose-store"]').first.click(timeout=15000)
        except Exception:
            href = await page.locator(change_store_link_selector).get_attribute("href")
            if href:
                await page.goto(urljoin(page.url, href), wait_until="domcontentloaded", timeout=60000)
            else:
                raise

    await page.wait_for_load_state("domcontentloaded")

    await page.click(store_bar_selector, timeout=30000)
    await page.wait_for_timeout(400)

    # Store list is rendered as role=option elements (may be virtualized)
    options = page.locator("[role='option']")
    seen: set[str] = set()

    stagnation = 0
    last_count = 0
    last_seen_size = 0
    for _ in range(60):
        count = await options.count()
        for index in range(count):
            text = (await options.nth(index).inner_text()).strip()
            normalized = " ".join(text.split())
            if normalized:
                seen.add(normalized)

        # Try to scroll the list to load more options
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

    stores = sorted(seen)
    if stores:
        print(f"Discovered {len(stores)} stores")
    else:
        print("WARNING: No stores discovered (role=option not found).")
    return stores


def _with_page_number(url: str, page_number: int) -> str:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    query["pg"] = [str(page_number)]
    updated_query = urlencode(query, doseq=True)
    return urlunparse(parsed._replace(query=updated_query))


async def discover_category_page_urls(
    page,
    start_url: str,
    headless: bool,
    manual_wait_seconds: int,
) -> list[str]:
    response = await page.goto(start_url, wait_until="domcontentloaded", timeout=60000)
    title = (await page.title()).strip().lower()
    body_preview = (await page.locator("body").inner_text())[:1000].lower()
    challenge_detected = _is_bot_challenge(
        response.status if response else None,
        title,
        body_preview,
    )
    rate_limited = _is_rate_limited(
        response.status if response else None,
        title,
        body_preview,
    )

    if rate_limited:
        raise RateLimitError(
            "Cloudflare rate limit detected while discovering paginated category URLs (Error 1015)."
        )

    if challenge_detected and manual_wait_seconds > 0:
        print(
            f"Bot challenge detected. Waiting {manual_wait_seconds}s for manual verification in browser..."
        )
        await page.wait_for_timeout(manual_wait_seconds * 1000)

    if challenge_detected and headless:
        raise RuntimeError(
            "Target page returned a bot challenge (Cloudflare). "
            "Re-run with --headed and --manual-wait-seconds 60, then complete verification manually."
        )

    hrefs = await page.locator("a[href*='pg=']").evaluate_all("els => els.map(e => e.getAttribute('href'))")

    start_parsed = urlparse(start_url)
    page_numbers = set()
    current_pg = parse_qs(start_parsed.query).get("pg", ["1"])[0]
    if str(current_pg).isdigit():
        page_numbers.add(int(current_pg))

    for href in hrefs:
        if not href:
            continue
        absolute = urljoin(start_url, href)
        parsed = urlparse(absolute)
        if parsed.path != start_parsed.path:
            continue
        pg_values = parse_qs(parsed.query).get("pg", [])
        for value in pg_values:
            if value.isdigit():
                page_numbers.add(int(value))

    if not page_numbers:
        return [start_url]

    max_page = max(page_numbers)
    return [_with_page_number(start_url, page_number) for page_number in range(1, max_page + 1)]


async def discover_category_urls(
    page,
    start_url: str,
    category_link_selector: str,
    category_name_selector: str,
    headless: bool,
    manual_wait_seconds: int,
) -> list[CategoryLink]:
    response = await page.goto(start_url, wait_until="domcontentloaded", timeout=60000)
    title = (await page.title()).strip().lower()
    body_preview = (await page.locator("body").inner_text())[:1000].lower()
    challenge_detected = _is_bot_challenge(
        response.status if response else None,
        title,
        body_preview,
    )
    rate_limited = _is_rate_limited(
        response.status if response else None,
        title,
        body_preview,
    )

    if rate_limited:
        raise RateLimitError(
            "Cloudflare rate limit detected while discovering category URLs (Error 1015)."
        )

    if challenge_detected and manual_wait_seconds > 0:
        print(
            f"Bot challenge detected. Waiting {manual_wait_seconds}s for manual verification in browser..."
        )
        await page.wait_for_timeout(manual_wait_seconds * 1000)

    if challenge_detected and headless:
        raise RuntimeError(
            "Target page returned a bot challenge (Cloudflare). "
            "Re-run with --headed and --manual-wait-seconds 60, then complete verification manually."
        )

    category_names = await page.locator(category_name_selector).evaluate_all(
        "els => els.map(e => (e.textContent || '').trim()).filter(Boolean)"
    )
    if category_names:
        print(f"Detected category names ({len(category_names)}): {', '.join(category_names[:8])}")

    link_data = await page.locator(category_link_selector).evaluate_all(
        "els => els.map(e => ({ href: e.getAttribute('href'), text: (e.textContent || '').trim() }))"
    )
    if not link_data:
        # Fallback: attempt to discover shop/category links even if the provided selector doesn't match.
        link_data = await page.locator("a[href]").evaluate_all(
            "els => els.map(e => ({ href: e.getAttribute('href'), text: (e.textContent || '').trim() }))"
        )

    categories: list[CategoryLink] = []
    seen: set[str] = set()
    for item in link_data:
        href = item.get("href")
        if not href:
            continue
        absolute = urljoin(start_url, href)
        parsed = urlparse(absolute)
        if "/shop/" not in parsed.path:
            continue
        if absolute in seen:
            continue
        seen.add(absolute)
        name = (item.get("text") or "").strip() or absolute.rsplit("/", 1)[-1]
        categories.append(CategoryLink(name=name, url=absolute, source_url=start_url))

    return categories


async def scrape_products(
    page,
    url: str,
    product_selector: str,
    name_selector: str,
    price_selector: str,
    price_cents_selector: str,
    unit_price_selector: str,
    image_selector: str,
    limit: int,
    wait_for_selector: str | None,
    query: str | None,
    headless: bool,
    manual_wait_seconds: int,
) -> tuple[list[Product], list[ProductPriceSnapshot]]:
    response = await page.goto(url, wait_until="domcontentloaded", timeout=60000)

    title = (await page.title()).strip().lower()
    body_preview = (await page.locator("body").inner_text())[:1000].lower()
    challenge_detected = _is_bot_challenge(
        response.status if response else None,
        title,
        body_preview,
    )
    rate_limited = _is_rate_limited(
        response.status if response else None,
        title,
        body_preview,
    )

    if rate_limited:
        raise RateLimitError(
            f"Cloudflare rate limit detected while scraping {url} (Error 1015)."
        )

    if challenge_detected and manual_wait_seconds > 0:
        print(
            f"Bot challenge detected. Waiting {manual_wait_seconds}s for manual verification in browser..."
        )
        await page.wait_for_timeout(manual_wait_seconds * 1000)

    if challenge_detected and headless:
        raise RuntimeError(
            "Target page returned a bot challenge (Cloudflare). "
            "Re-run with --headed and --manual-wait-seconds 60, then complete verification manually."
        )

    if wait_for_selector:
        await page.wait_for_selector(wait_for_selector)

    supermarket_name = await _get_supermarket_name(page)

    cards = page.locator(product_selector)
    count = await cards.count()

    query_normalized = query.strip().lower() if query else ""
    products: list[Product] = []
    snapshots: list[ProductPriceSnapshot] = []

    for index in range(count):
        if len(products) >= limit:
            break

        card = cards.nth(index)
        name = (await card.locator(name_selector).first.inner_text()).strip() if await card.locator(name_selector).count() else ""
        price_dollars = (await card.locator(price_selector).first.inner_text()).strip() if await card.locator(price_selector).count() else ""
        price_cents = (await card.locator(price_cents_selector).first.inner_text()).strip() if await card.locator(price_cents_selector).count() else ""
        unit_price = (await card.locator(unit_price_selector).first.inner_text()).strip() if await card.locator(unit_price_selector).count() else ""
        packaging_format = _extract_packaging_format(unit_price)
        image_raw = await card.locator(image_selector).first.get_attribute("src") if await card.locator(image_selector).count() else ""
        image = urljoin(url, image_raw or "")

        cleaned_cents = price_cents.replace(".", "").strip()
        price = f"{price_dollars}.{cleaned_cents}" if price_dollars and cleaned_cents else price_dollars

        if not name:
            continue

        if query_normalized and query_normalized not in name.lower():
            continue

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
            )
        )

    return products, snapshots


async def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape supermarket products using Playwright")
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
        "--image-selector",
        default="[data-testid='product-image']",
        help="CSS selector for product image within card",
    )
    parser.add_argument("--wait-for-selector", default=None, help="Optional selector to wait for before scraping")
    parser.add_argument("--query", default=None, help="Optional name filter")
    parser.add_argument("--limit", type=int, default=20, help="Maximum products to return")
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
        help="Maximum number of resolved URLs (pages) to scrape. Useful for small test runs.",
    )
    parser.add_argument(
        "--choose-store",
        action="store_true",
        help="Choose a store before scraping (affects prices/availability).",
    )
    parser.add_argument(
        "--scrape-all-stores",
        action="store_true",
        help="Discover all stores and scrape the same URLs once per store in a single run.",
    )
    parser.add_argument(
        "--max-stores",
        type=int,
        default=None,
        help="Optional cap on how many stores to scrape in --scrape-all-stores mode.",
    )
    parser.add_argument(
        "--store-name",
        default=None,
        help="Exact store name to click after opening the store list (optional).",
    )
    parser.add_argument(
        "--store-index",
        type=int,
        default=0,
        help="If --store-name is not set, click the Nth option (0-based) from the store list.",
    )
    parser.add_argument(
        "--store-ribbon-button-selector",
        default="#ribbon > div > div._1dmgezfn9._1dmgezf4i._1dmgezf9.svd0ke7 > button",
        help="CSS selector for the ribbon store button.",
    )
    parser.add_argument(
        "--store-change-link-selector",
        default="#ribbon > div > div._1dmgezfn9._1dmgezf4i._1dmgezf9.svd0ke7 > div._1g0swly0._74qpuq0 > div > div > div._1dmgezfmi._1dmgezf9._1dmgezf19 > a:nth-child(2)",
        help="CSS selector for the 'Change store' link.",
    )
    parser.add_argument(
        "--store-bar-selector",
        default="#_r_8_",
        help="CSS selector for the store selection bar on the store page.",
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
    parser.add_argument("--headed", action="store_true", help="Run browser in headed mode")
    parser.add_argument(
        "--headless-only",
        action="store_true",
        help=(
            "Force headless mode (never show a Chromium window). "
            "If Cloudflare blocks headless, run once without --headless-only + with --headed to complete verification, "
            "then rerun headless using the saved storage_state.json."
        ),
    )
    parser.add_argument(
        "--manual-wait-seconds",
        type=int,
        default=0,
        help="Seconds to wait after load for manual challenge/cookie handling (use with --headed)",
    )
    parser.add_argument(
        "--delay-seconds",
        type=float,
        default=3.0,
        help="Delay between page requests to reduce rate limiting",
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
        help="Write output JSON after each scraped URL (safer for long runs)",
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
        help="Path to Playwright storage state JSON (cookies/localStorage) to reuse between runs",
    )
    args = parser.parse_args()

    if args.headless_only:
        args.headed = False

    all_products: list[Product] = []
    all_snapshots: list[ProductPriceSnapshot] = []
    progress_path = Path(args.progress_file)
    progress = load_progress(progress_path)
    discovered_categories_all: list[CategoryLink] = []
    rate_limit_hit = False
    output_path = Path(args.output)
    price_output_path = Path(args.price_output)

    if args.append_snapshots:
        all_snapshots = load_price_snapshots(price_output_path)

    async with async_playwright() as playwright:
        headless = not args.headed
        try:
            browser = await playwright.chromium.launch(headless=headless)
        except Exception as exc:
            raise RuntimeError(
                "Playwright browser binaries are not installed for this scraper's venv. "
                "Run: ./.venv/bin/python -m playwright install chromium"
            ) from exc
        storage_state_path = Path(args.storage_state)
        context = await browser.new_context(
            storage_state=str(storage_state_path) if storage_state_path.exists() else None
        )
        page = await context.new_page()

        if args.scrape_all_stores:
            stores = await discover_store_names(
                page=page,
                start_url=args.url[0],
                headless=not args.headed,
                manual_wait_seconds=max(0, args.manual_wait_seconds),
                ribbon_button_selector=args.store_ribbon_button_selector,
                change_store_link_selector=args.store_change_link_selector,
                store_bar_selector=args.store_bar_selector,
            )
            if args.max_stores is not None:
                stores = stores[: max(0, int(args.max_stores))]

            # In this mode, we don't want resume to skip URLs for subsequent stores.
            args.resume = False

        else:
            stores = []

        if args.choose_store and not args.scrape_all_stores:
            await choose_store(
                page=page,
                start_url=args.url[0],
                headless=not args.headed,
                manual_wait_seconds=max(0, args.manual_wait_seconds),
                ribbon_button_selector=args.store_ribbon_button_selector,
                change_store_link_selector=args.store_change_link_selector,
                store_bar_selector=args.store_bar_selector,
                store_name=args.store_name,
                store_index=args.store_index,
            )

        async def resolve_urls() -> list[str]:
            resolved_urls: list[str] = []
            seen_urls: set[str] = set()

            for input_url in args.url:
                expanded_urls = [input_url]

                if args.discover_category_urls:
                    try:
                        discovered_categories = await discover_category_urls(
                            page=page,
                            start_url=input_url,
                            category_link_selector=args.category_link_selector,
                            category_name_selector=args.category_name_selector,
                            headless=not args.headed,
                            manual_wait_seconds=max(0, args.manual_wait_seconds),
                        )
                    except RateLimitError as error:
                        print(f"WARNING: {error}")
                        return []
                    discovered_categories_all.extend(discovered_categories)
                    expanded_urls = [item.url for item in discovered_categories]
                    print(
                        f"Discovered {len(expanded_urls)} category URLs from: {input_url}"
                    )
                    if not expanded_urls:
                        print(
                            "WARNING: No category URLs were discovered. "
                            "The page layout or selectors may have changed; try adjusting --category-link-selector and --category-name-selector."
                        )
                        expanded_urls = [input_url]

                if args.crawl_category_pages:
                    paginated_urls: list[str] = []
                    for category_url in expanded_urls:
                        try:
                            discovered_pages = await discover_category_page_urls(
                                page=page,
                                start_url=category_url,
                                headless=not args.headed,
                                manual_wait_seconds=max(0, args.manual_wait_seconds),
                            )
                        except RateLimitError as error:
                            print(f"WARNING: {error}")
                            return []
                        print(
                            f"Discovered {len(discovered_pages)} paginated URLs for category: {category_url}"
                        )
                        paginated_urls.extend(discovered_pages)
                        if args.delay_seconds > 0:
                            jitter = max(0.0, float(args.delay_jitter_seconds))
                            wait = max(0.0, float(args.delay_seconds)) + random.random() * jitter
                            await page.wait_for_timeout(int(wait * 1000))
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
            await browser.close()
            return

        async def scrape_urls_for_current_store(to_scrape: list[str]) -> None:
            nonlocal rate_limit_hit

            completed_set = set(progress.completed_urls) if args.resume else set()
            targets = [u for u in to_scrape if u not in completed_set]
            if args.max_pages is not None:
                targets = targets[: max(0, int(args.max_pages))]
            if args.resume:
                print(f"Resume enabled: skipping {len(to_scrape) - len(targets)} already-completed URLs")

            for current_url in targets:
                print(f"Scraping URL: {current_url}")
                attempt = 0
                while True:
                    try:
                        products, snapshots = await scrape_products(
                            page=page,
                            url=current_url,
                            product_selector=args.product_selector,
                            name_selector=args.name_selector,
                            price_selector=args.price_selector,
                            price_cents_selector=args.price_cents_selector,
                            unit_price_selector=args.unit_price_selector,
                            image_selector=args.image_selector,
                            limit=max(1, args.limit),
                            wait_for_selector=args.wait_for_selector,
                            query=args.query,
                            headless=not args.headed,
                            manual_wait_seconds=max(0, args.manual_wait_seconds),
                        )
                        break
                    except RateLimitError as error:
                        print(f"WARNING: {error}")
                        if attempt >= max(0, int(args.max_rate_limit_retries)):
                            rate_limit_hit = True
                            products, snapshots = [], []
                            break
                        attempt += 1
                        wait_seconds = max(0, int(args.rate_limit_wait_seconds))
                        print(f"Waiting {wait_seconds}s then retrying ({attempt}/{args.max_rate_limit_retries})...")
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

                if storage_state_path:
                    await context.storage_state(path=str(storage_state_path))

                if args.delay_seconds > 0:
                    jitter = max(0.0, float(args.delay_jitter_seconds))
                    wait = max(0.0, float(args.delay_seconds)) + random.random() * jitter
                    await page.wait_for_timeout(int(wait * 1000))

        if args.scrape_all_stores and stores:
            for store in stores:
                print(f"\n=== Store: {store} ===")
                await choose_store(
                    page=page,
                    start_url=args.url[0],
                    headless=not args.headed,
                    manual_wait_seconds=max(0, args.manual_wait_seconds),
                    ribbon_button_selector=args.store_ribbon_button_selector,
                    change_store_link_selector=args.store_change_link_selector,
                    store_bar_selector=args.store_bar_selector,
                    store_name=store,
                    store_index=args.store_index,
                )
                await scrape_urls_for_current_store(resolved_urls)
                if rate_limit_hit:
                    break
        else:
            await scrape_urls_for_current_store(resolved_urls)

        await context.close()
        await browser.close()

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
    asyncio.run(main())
