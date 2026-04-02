# New World Scraper

Scrapes product names, prices, unit prices, promo prices, and images from New World category pages. Supports API-based providers (ScrapingBee, ScraperAPI, Crawlbase, Zenrows, direct HTTP) and a real Playwright browser mode, with category discovery, pagination crawling, price history snapshots, resume-on-crash, and exponential-backoff retries.

## Setup

```bash
cd python-playwright-scraper
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Optional Playwright prerequisites:

```bash
python -m playwright install firefox
python -m playwright install-deps firefox
```

Export your API key for the chosen provider (default: ScrapingBee):

```bash
export SCRAPING_PROVIDER_API_KEY="your_key_here"   # ScrapingBee (default)
export SCRAPERAPI_KEY="your_key_here"        # ScraperAPI
export CRAWLBASE_TOKEN="your_key_here"       # Crawlbase
export ZENROWS_API_KEY="your_key_here"       # Zenrows
```

Or pass it directly with `--api-key`. No key is needed for `--provider direct`.

### Database persistence (PostgreSQL)

The scraper can optionally persist product data and price snapshots to PostgreSQL instead of (or in addition to) JSON files.

Set your PostgreSQL URL:

```bash
export DATABASE_URL="postgresql://user:password@host:5432/dbname"
```

Apply schema once:

```bash
psql "$DATABASE_URL" -f db/schema.sql
```

Run scraper with persistence enabled:

```bash
python scraper.py \
  --url "https://www.newworld.co.nz/shop/category/fruit-and-vegetables?pg=1" \
  --persist-db \
  --database-url "$DATABASE_URL"
```

Your data is now available via the **[Postgres Products API](https://github.com/salvador-cortes-c/postgres-products-api)** for frontend queries.

---

## Quick start

Scrape one category page:

```bash
python scraper.py \
  --url "https://www.newworld.co.nz/shop/category/fruit-and-vegetables?pg=1" \
  --limit 20 \
  --output products.json
```

Scrape all pages of a category automatically:

```bash
python scraper.py \
  --url "https://www.newworld.co.nz/shop/category/fruit-and-vegetables?pg=1" \
  --crawl-category-pages \
  --limit 50 \
  --dedupe \
  --output products.json \
  --price-output price_snapshots.json
```

Discover all categories from the homepage and crawl every page:

```bash
python scraper.py \
  --url "https://www.newworld.co.nz/" \
  --discover-category-urls \
  --crawl-category-pages \
  --limit 100 \
  --max-pages 10 \
  --dedupe \
  --resume \
  --flush-every-url \
  --output products.json \
  --price-output price_snapshots.json
```

Discover and save only the list of category URLs without scraping products:

```bash
python scraper.py \
  --url "https://www.newworld.co.nz/" \
  --discover-category-urls \
  --categories-only \
  --category-output category_urls.json
```

Filter results by keyword:

```bash
python scraper.py \
  --url "https://www.newworld.co.nz/shop/category/dairy-eggs-and-fridge?pg=1" \
  --query "milk" \
  --limit 20 \
  --output milk_products.json
```

Preflight count only for a full-category crawl (API providers):

```bash
python scraper.py \
  --url "https://www.newworld.co.nz/" \
  --discover-category-urls \
  --count-only
```

Count the number of available stores from the fulfillment page:

```bash
python scraper.py \
  --provider playwright \
  --url "https://www.newworld.co.nz/shop/fulfillment" \
  --count-stores
```

This will output: `Total number of stores: N`

Preflight count only in Playwright mode (store-specific):

```bash
python scraper.py \
  --provider playwright \
  --url "https://www.newworld.co.nz/" \
  --discover-category-urls \
  --choose-store \
  --store-name "New World Karori" \
  --count-only
```

Note: `--limit` does not affect `--count-only` because no product cards are extracted in preflight mode.

Scrape a specific store in Playwright mode:

```bash
python scraper.py \
  --provider playwright \
  --url "https://www.newworld.co.nz/" \
  --discover-category-urls \
  --crawl-category-pages \
  --choose-store \
  --store-name "New World Karori" \
  --headed \
  --manual-wait-seconds 60 \
  --output products.json \
  --price-output price_snapshots.json
```

---

## Output files

| File | Content |
|---|---|
| `products.json` | Deduplicated product catalogue (`--output`) |
| `price_snapshots.json` | Time-series price history (`--price-output`) |
| `scrape_progress.json` | Resume state — completed URLs (`--progress-file`) |
| `category_urls.json` | Discovered category list (`--category-output`) |

### `products.json` record

```json
{
  "product_key": "wattie's baked beans 420g__420g",
  "name": "Wattie's Baked Beans 420g",
  "packaging_format": "420g",
  "image": "https://a.fsimg.co.nz/product/..."
}
```

### `price_snapshots.json` record

```json
{
  "product_key": "wattie's baked beans 420g__420g",
  "supermarket_name": "New World Rototuna",
  "price": "1.79",
  "unit_price": "$4.26/kg",
  "source_url": "https://www.newworld.co.nz/shop/category/...",
  "scraped_at": "2026-04-01T02:00:00+00:00",
  "promo_price": "",
  "promo_unit_price": ""
}
```

---

## All parameters

### Required

| Flag | Description |
|---|---|
| `--url URL` | Target page URL. Repeat to scrape multiple URLs in one run. |

### Provider configuration

| Flag | Default | Description |
|---|---|---|
| `--provider NAME` | `scrapingbee` | Scraping service/engine to use. Choices: `scrapingbee`, `scraperapi`, `crawlbase`, `zenrows`, `direct`, `playwright`. |
| `--country-code CC` | `nz` | Country code for proxy targeting (e.g. `us`, `gb`, `au`). |
| `--premium-proxy` / `--no-premium-proxy` | on | Use residential/premium proxies when supported by the provider. |
| `--render-wait-ms N` | `3000` | Milliseconds to wait for JavaScript rendering (provider-agnostic). Overrides `--scrapingbee-wait-ms` when set. |
| `--api-key KEY` | env var | Provider API key. Falls back to the provider-specific env var: `SCRAPING_PROVIDER_API_KEY`, `SCRAPERAPI_KEY`, `CRAWLBASE_TOKEN`, or `ZENROWS_API_KEY`. Not required for `--provider direct` or `--provider playwright`. |

### Authentication

| Provider | Environment variable |
|---|---|
| `scrapingbee` (default) | `SCRAPING_PROVIDER_API_KEY` |
| `scraperapi` | `SCRAPERAPI_KEY` |
| `crawlbase` | `CRAWLBASE_TOKEN` |
| `zenrows` | `ZENROWS_API_KEY` |
| `direct` | *(none required)* |
| `playwright` | *(none required)* |

### Product selectors

These CSS selectors are pre-configured for New World and rarely need to change. Override if the page structure changes.

| Flag | Default | Description |
|---|---|---|
| `--product-selector` | `div[data-testid^='product-'][data-testid$='-000']` | CSS selector for product card containers. |
| `--name-selector` | `[data-testid='product-title']` | Product name element within each card. |
| `--price-selector` | `[data-testid='price-dollars']` | Dollar part of the regular price. |
| `--price-cents-selector` | `[data-testid='price-cents']` | Cents part of the regular price. |
| `--unit-price-selector` | `[data-testid='non-promo-unit-price']` | Unit price string e.g. `$2.99/100g`. |
| `--promo-price-dollars-selector` | *(long nth-child selector)* | Dollar part of the promotional price. |
| `--promo-price-cents-selector` | *(long nth-child selector)* | Cents part of the promotional price. |
| `--promo-unit-price-selector` | *(long nth-child selector)* | Unit price under a promotional price. |
| `--image-selector` | `[data-testid='product-image']` | Product image element within each card. |

### Output

| Flag | Default | Description |
|---|---|---|
| `--output FILE` | `products.json` | File to write the product catalogue. |
| `--price-output FILE` | `price_snapshots.json` | File to write price snapshots. |
| `--append-snapshots` | off | Merge new snapshots into the existing `--price-output` file instead of overwriting. Keeps full price history across runs. |
| `--category-output FILE` | none | Write discovered category names and URLs to this file. |
| `--persist-db` | off | Persist final products/snapshots/categories to PostgreSQL. |
| `--database-url URL` | `DATABASE_URL` env | PostgreSQL connection string used when `--persist-db` is enabled. |

### Filtering

| Flag | Default | Description |
|---|---|---|
| `--limit N` | `20` | Maximum products to extract per URL. |
| `--max-pages N` | unlimited | Stop after scraping this many resolved page URLs in total. |
| `--count-only` | off | Resolve category/pagination URLs and print the total without scraping product pages. Useful as a preflight check before long runs. |
| `--query TEXT` | none | Keep only products whose name contains this string (case-insensitive). |
| `--dedupe` | off | Remove duplicate products by `product_key` across all URLs. When a duplicate is found, the entry with an image is preferred. |

### Category discovery and pagination

| Flag | Default | Description |
|---|---|---|
| `--discover-category-urls` | off | Fetch each `--url` first and extract category links from it before scraping products. Uses `--category-link-selector`. |
| `--category-link-selector` | `a._7zlpdd._7zlpdc` | CSS selector for category anchor elements during discovery. |
| `--category-name-selector` | `button._7zlpdc` | CSS selector for category name buttons — used for logging discovered names. |
| `--crawl-category-pages` | off | For each category URL, fetch page 1 and auto-discover all `?pg=N` pagination pages, then scrape them all. |
| `--categories-only` | off | Exit after saving discovered categories to `--category-output`. No products are scraped. |

### Resume and progress

| Flag | Default | Description |
|---|---|---|
| `--progress-file FILE` | `scrape_progress.json` | Path to the JSON file used to persist completed URLs between runs. |
| `--resume` | off | Skip URLs already recorded in `--progress-file`. Enables crash recovery — when combined with `--flush-every-url`, a restart continues from the last completed URL. |
| `--flush-every-url` | off | Write `--output` and `--price-output` after every single URL instead of only at the end. Prevents data loss if the run is interrupted. |

### Playwright store mode

These flags are active only when `--provider playwright` is used.

| Flag | Default | Description |
|---|---|---|
| `--count-stores` | off | Count the number of available stores from the fulfillment page and exit. Useful for store enumeration preflight checks. |
| `--choose-store` | off | Select a store before scraping. |
| `--store-name NAME` | none | Choose a specific store by name, e.g. `New World Karori`. |
| `--store-names NAME` | none | Scrape only the specified store name(s). May be repeated or comma-separated. |
| `--scrape-all-stores` | off | Iterate through all discovered stores and scrape the same URLs for each one. |
| `--max-stores N` | unlimited | Optional cap when using `--scrape-all-stores`. |
| `--store-index N` | `0` | Fallback 0-based option index if no store name is supplied. |
| `--headed` | off | Launch Firefox with a visible window. Recommended for manual Cloudflare/store verification. |
| `--headless-only` | off | Force Playwright to stay headless. |
| `--manual-wait-seconds N` | `0` | Wait after page load so you can complete challenge/store interactions manually. |
| `--storage-state FILE` | `storage_state.json` | Persist and reuse cookies/local storage between runs. |
| `--wait-for-selector SEL` | none | Wait for a selector before scraping each page. |

### Request timing

| Flag | Default | Description |
|---|---|---|
| `--initial-delay-seconds N` | `0.0` | Wait this many seconds before the very first request. Useful to stagger parallel GitHub Actions runs. |
| `--delay-seconds N` | `3.0` | Fixed delay between consecutive requests. Increase to reduce Cloudflare rate-limiting risk. |
| `--delay-jitter-seconds N` | `1.0` | Random extra delay added on top of `--delay-seconds` per request (uniformly sampled from `0..N`). Prevents rhythmic request patterns. |
| `--scrapingbee-wait-ms N` | `3000` | **Deprecated** — use `--render-wait-ms` instead. Kept for backward compatibility. |

### Resilience and retries

The scraper distinguishes two error categories with independent retry budgets and exponential backoff.

**Transient errors** — network-level failures (timeout, connection reset, refused, EOF, empty HTML response). These are URL-specific; exhausting retries skips the URL and continues the run.

| Flag | Default | Description |
|---|---|---|
| `--max-retries N` | `3` | Maximum retry attempts per URL on transient errors. |
| `--retry-base-delay-seconds N` | `5.0` | Base delay for the first transient retry. Subsequent retries use exponential backoff: `min(base × 2^attempt, max) × jitter`. |
| `--retry-max-delay-seconds N` | `120.0` | Maximum backoff cap for transient retries. |

**Rate-limit errors** — Cloudflare Error 1015 / HTTP 429. These indicate the whole session is throttled; exhausting retries aborts the run and saves partial results.

| Flag | Default | Description |
|---|---|---|
| `--max-rate-limit-retries N` | `0` | Maximum retry attempts when a rate-limit response is received. `0` means abort immediately. |
| `--rate-limit-wait-seconds N` | `300` | Base delay for the first rate-limit retry. Subsequent retries use exponential backoff. |
| `--rate-limit-max-delay-seconds N` | `600` | Maximum backoff cap for rate-limit retries. |

**Backoff formula** (both error types):

```
delay = min(base × 2^attempt, max) × uniform(0.75, 1.25)
```

The ±25% jitter prevents multiple workers from retrying in sync.

---

## GitHub Actions

The workflow `.github/workflows/scrape_scrapingbee.yml` runs daily at 2 AM UTC and can be triggered manually from the Actions tab.

### Count workflow (stores + categories)

The workflow `.github/workflows/manual-test.yml` is a dedicated manual workflow for counting only, without running full product scraping.

How to run:

1. Open **Actions** in GitHub.
2. Select **Count Stores and Categories (Manual)**.
3. Click **Run workflow**.
4. Use branch `main` and confirm.

Outputs:

- Job logs include:
  - `Total number of stores: N`
  - `Discovered N unique category URLs`

### Scrape test workflow

The workflow `.github/workflows/scrape-test.yml` runs on every push, pull request, and manual trigger.

Its purpose is a fast smoke test of the scraper runtime and CLI compatibility:

- Installs dependencies from `requirements.txt`
- Verifies `python scraper.py --help` succeeds
- Runs a direct-provider scrape against `https://httpbin.org/html` with test selectors
- Confirms output files are created (`test_output.json`, `test_price_snapshots.json`)

This workflow is intentionally lightweight and does not require provider API keys.

### Required secret

Set the secret matching the provider you plan to use in **Repository Settings → Secrets and variables → Actions**.

- `SCRAPING_PROVIDER_API_KEY` (used for ScrapingBee)
- `SCRAPERAPI_KEY`
- `CRAWLBASE_TOKEN`
- `ZENROWS_API_KEY`

No secret is needed for `direct` or `playwright` mode.

### Manual trigger inputs

| Input | Default | Description |
|---|---|---|
| `url` | fruit-and-vegetables page | Category URL to scrape. |
| `provider` | `scrapingbee` | Scraping provider/engine. Use `playwright` for real browser mode. |
| `limit` | `20` | Max products per page URL. Ignored when `count_only=true` because no products are scraped. |
| `max_pages` | `3` | Max resolved page URLs to scrape in total. |
| `discover_category_urls` | `false` | Start from the given URL, discover category URLs, then crawl each category. |
| `count_only` | `false` | Only resolve/count URLs; skip scraping products. |
| `store_name` | empty | Store name for Playwright mode, e.g. `New World Karori`. |

### Artifacts uploaded per run

- `products.json`
- `price_snapshots.json`
- `scrape_progress.json`

Retention: 7 days.

### Resume across re-runs

The workflow enables `--resume` and `--flush-every-url`. If a run is interrupted or times out, re-triggering the same workflow will read `scrape_progress.json` from the previous artifact and skip already-completed URLs — **as long as you restore the progress file before the next run**. The simplest approach is to commit `scrape_progress.json` to the repository after each successful run, or upload and download it as a persistent cache artifact.

---

## Validating changes

### Review a specific commit

```bash
git show <commit-sha>
```

### Smoke test preflight in Playwright mode

```bash
python scraper.py \
  --provider playwright \
  --url "https://www.newworld.co.nz/" \
  --discover-category-urls \
  --choose-store \
  --store-name "New World Karori" \
  --count-only
```

Expected output: `Discovered N category URLs` and `Resolved N page URLs to scrape`, then exit. In count-only mode, no product scraping happens and `--limit` is not used.

---

## Minimal Repository Policy

This repository keeps only files required for runtime and CI. Root helper shell scripts were removed to avoid stale tooling and duplicated logic.

Required core files:

- `scraper.py`
- `requirements.txt`
- `.github/workflows/manual-test.yml`
- `.github/workflows/scrape_scrapingbee.yml`
- `.github/workflows/scrape-test.yml`

Local checks without helper scripts:

```bash
python -m py_compile scraper.py
python scraper.py --help
```

If you need ad-hoc local validation, run the scraper directly with the examples in **Quick start** instead of relying on wrapper scripts.

---

## API-Only Note

When using API-based providers (`scrapingbee`, `scraperapi`, `crawlbase`, `zenrows`, `direct`), the Playwright-only store/browser flags are accepted by the CLI but have no effect.

`--headed`, `--headless-only`, `--manual-wait-seconds`, `--storage-state`, `--wait-for-selector`, `--choose-store`, `--scrape-all-stores`, `--max-stores`, `--store-name`, `--store-names`, `--store-index`, `--store-ribbon-button-selector`, `--store-change-link-selector`, `--store-bar-selector`
