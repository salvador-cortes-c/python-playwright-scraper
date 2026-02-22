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


@dataclass
class Product:
    name: str
    price: str
    unit_price: str
    packaging_format: str
    image: str
    source_url: str
    scraped_at: str


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
    categories: list[CategoryLink] = []
    seen: set[str] = set()
    for item in link_data:
        href = item.get("href")
        if not href:
            continue
        absolute = urljoin(start_url, href)
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
) -> list[Product]:
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

    cards = page.locator(product_selector)
    count = await cards.count()

    query_normalized = query.strip().lower() if query else ""
    products: list[Product] = []

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

        products.append(
            Product(
                name=name,
                price=price,
                unit_price=unit_price,
                packaging_format=packaging_format,
                image=image,
                source_url=url,
                scraped_at=datetime.now(timezone.utc).isoformat(),
            )
        )

    return products


async def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape supermarket products using Playwright")
    parser.add_argument(
        "--url",
        action="append",
        required=True,
        help="Target page URL. Repeat this argument to scrape multiple URLs.",
    )
    parser.add_argument("--product-selector", default=".product-item", help="CSS selector for product cards")
    parser.add_argument("--name-selector", default=".product-title", help="CSS selector for product name within card")
    parser.add_argument("--price-selector", default=".product-price", help="CSS selector for product price within card")
    parser.add_argument(
        "--price-cents-selector",
        default="[data-testid='price-cents']",
        help="CSS selector for price cents within card",
    )
    parser.add_argument(
        "--unit-price-selector",
        default="[data-testid='product-subtitle']",
        help="CSS selector for unit price/subtitle within card",
    )
    parser.add_argument("--image-selector", default="img", help="CSS selector for product image within card")
    parser.add_argument("--wait-for-selector", default=None, help="Optional selector to wait for before scraping")
    parser.add_argument("--query", default=None, help="Optional name filter")
    parser.add_argument("--limit", type=int, default=20, help="Maximum products to return")
    parser.add_argument("--output", default="products.json", help="Output JSON file")
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

    all_products: list[Product] = []
    progress_path = Path(args.progress_file)
    progress = load_progress(progress_path)
    discovered_categories_all: list[CategoryLink] = []
    rate_limit_hit = False
    output_path = Path(args.output)

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=not args.headed)
        storage_state_path = Path(args.storage_state)
        context = await browser.new_context(
            storage_state=str(storage_state_path) if storage_state_path.exists() else None
        )
        page = await context.new_page()

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
                    rate_limit_hit = True
                    break
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

            urls_after_category_discovery: list[str] = []
            for discovered_url in expanded_urls:
                urls_after_category_discovery.append(discovered_url)

            expanded_urls = urls_after_category_discovery

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
                        rate_limit_hit = True
                        break
                    print(
                        f"Discovered {len(discovered_pages)} paginated URLs for category: {category_url}"
                    )
                    paginated_urls.extend(discovered_pages)
                    if args.delay_seconds > 0:
                        jitter = max(0.0, float(args.delay_jitter_seconds))
                        wait = max(0.0, float(args.delay_seconds)) + random.random() * jitter
                        await page.wait_for_timeout(int(wait * 1000))
                expanded_urls = paginated_urls
            if rate_limit_hit:
                break

            for expanded_url in expanded_urls:
                if expanded_url in seen_urls:
                    continue
                seen_urls.add(expanded_url)
                resolved_urls.append(expanded_url)

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

        completed_set = set(progress.completed_urls) if args.resume else set()
        to_scrape = [u for u in resolved_urls if u not in completed_set]
        if args.resume:
            print(f"Resume enabled: skipping {len(resolved_urls) - len(to_scrape)} already-completed URLs")

        for current_url in to_scrape:
            print(f"Scraping URL: {current_url}")
            attempt = 0
            while True:
                try:
                    products = await scrape_products(
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
                        products = []
                        break
                    attempt += 1
                    wait_seconds = max(0, int(args.rate_limit_wait_seconds))
                    print(f"Waiting {wait_seconds}s then retrying ({attempt}/{args.max_rate_limit_retries})...")
                    await page.wait_for_timeout(wait_seconds * 1000)

            if rate_limit_hit:
                break
            all_products.extend(products)

            progress.completed_urls.append(current_url)
            save_progress(progress_path, progress)

            if args.flush_every_url:
                write_products(output_path, all_products)

            if storage_state_path:
                await context.storage_state(path=str(storage_state_path))

            if args.delay_seconds > 0:
                jitter = max(0.0, float(args.delay_jitter_seconds))
                wait = max(0.0, float(args.delay_seconds)) + random.random() * jitter
                await page.wait_for_timeout(int(wait * 1000))

        await context.close()
        await browser.close()

    if args.dedupe:
        unique_products: list[Product] = []
        seen: set[tuple[str, str]] = set()
        for product in all_products:
            key = (product.name.strip().lower(), product.unit_price.strip().lower())
            if key in seen:
                continue
            seen.add(key)
            unique_products.append(product)
        all_products = unique_products

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
    print(f"Saved {len(all_products)} products to {output_path}")
    if rate_limit_hit:
        print("Run stopped early due to rate limiting; partial results were saved.")


if __name__ == "__main__":
    asyncio.run(main())
