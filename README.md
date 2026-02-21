# Playwright Product Scraper

A Python Playwright scraper that extracts product name, price, and thumbnail image from supermarket pages.

## Setup

```bash
cd python-playwright-scraper
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m playwright install chromium
```

## Run

```bash
python scraper.py \
  --url "https://example.com/supermarket/search?q=milk" \
  --product-selector ".product-item" \
  --name-selector ".product-title" \
  --price-selector ".product-price" \
  --image-selector "img" \
  --wait-for-selector ".product-item" \
  --limit 20 \
  --output products.json
```

## Multiple URLs

Repeat `--url` to scrape multiple pages in one run:

```bash
python scraper.py \
  --url "https://example.com/page-1" \
  --url "https://example.com/page-2" \
  --product-selector ".product-item" \
  --name-selector ".product-title" \
  --price-selector ".product-price" \
  --image-selector "img" \
  --limit 30 \
  --dedupe \
  --output products.json
```

## Crawl full category pagination

For category pages that use `?pg=`, enable automatic page discovery:

```bash
python scraper.py \
  --url "https://www.newworld.co.nz/shop/category/fruit-and-vegetables?pg=1" \
  --crawl-category-pages \
  --product-selector "div._1afq4wy0" \
  --name-selector "[data-testid='product-title']" \
  --price-selector "[data-testid='price-dollars']" \
  --price-cents-selector "[data-testid='price-cents']" \
  --unit-price-selector "[data-testid='product-subtitle']" \
  --image-selector "[data-testid='product-image']" \
  --limit 100 \
  --headed \
  --manual-wait-seconds 60 \
  --delay-seconds 5 \
  --dedupe \
  --output products.json
```

## Discover categories automatically

If a page lists category buttons/links, discover category URLs first and then scrape all their pages:

```bash
python scraper.py \
  --url "https://www.newworld.co.nz/" \
  --discover-category-urls \
  --category-name-selector "button._7zlpdc" \
  --category-link-selector "a._7zlpdd._7zlpdc" \
  --category-output category_urls.json \
  --crawl-category-pages \
  --product-selector "div._1afq4wy0" \
  --name-selector "[data-testid='product-title']" \
  --price-selector "[data-testid='price-dollars']" \
  --price-cents-selector "[data-testid='price-cents']" \
  --unit-price-selector "[data-testid='product-subtitle']" \
  --image-selector "[data-testid='product-image']" \
  --limit 100 \
  --headed \
  --manual-wait-seconds 60 \
  --delay-seconds 8 \
  --dedupe \
  --output products.json
```

`--category-output` stores discovered category names/URLs in a local JSON file.

If you only want the category list (no product scraping):

```bash
python scraper.py \
  --url "https://www.newworld.co.nz/" \
  --discover-category-urls \
  --category-name-selector "button._7zlpdc" \
  --category-link-selector "a._7zlpdd._7zlpdc" \
  --category-output category_urls.json \
  --categories-only \
  --headed \
  --manual-wait-seconds 60
```

## Rate limiting (Cloudflare 1015)

- If you see `Error 1015: You are being rate limited`, increase `--delay-seconds` (for example `5` or `8`).
- For long full-category runs, enable `--flush-every-url` + `--resume` so you can stop/restart without losing progress.
- The scraper writes progress to `scrape_progress.json` by default.
- `storage_state.json` is updated during runs (cookies) which can reduce repeated verification prompts.
- Keep using `--headed` and complete any verification prompts manually.
- The scraper now stops gracefully on rate limit and saves partial results to the output file.

## New World (bot-protected pages)

New World category pages may return a Cloudflare challenge (`Just a moment...`) for headless browsers.

Use headed mode and allow time to complete verification manually:

```bash
python scraper.py \
  --url "https://www.newworld.co.nz/shop/category/beer-wine-and-cider/beer/new-zealand-beers?pg=1" \
  --url "https://www.newworld.co.nz/shop/category/beer-wine-and-cider/beer/new-zealand-beers?pg=2" \
  --product-selector "article" \
  --name-selector "h3, h2, [class*='name']" \
  --price-selector "[class*='price']" \
  --image-selector "img" \
  --limit 30 \
  --headed \
  --manual-wait-seconds 60 \
  --output products.json
```

If selectors need tuning after challenge verification, inspect the page and adjust `--product-selector`, `--name-selector`, `--price-selector`, and `--image-selector`.

## Query filtering

Use `--query` to keep only matching products:

```bash
python scraper.py --url "https://example.com" --query "milk"
```

## Output

The script writes JSON to the file provided in `--output` (default: `products.json`).

Example record:

```json
{
  "name": "Milk 1L",
  "price": "€1.19",
  "image": "https://example.com/images/milk.png",
  "source_url": "https://example.com/supermarket/search?q=milk"
}
```
