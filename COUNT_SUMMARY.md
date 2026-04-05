# Verified Retailer Count Summary

_Last updated: 2026-04-05_

This file records the latest verified **store**, **top-level category**, and **category-page** counts for the supported NZ supermarket retailers, using the **lowest-cost sources first**.

## Final corrected counts

| Retailer | Stores | Top-level categories | Total category pages | Stores × pages | Primary verification source |
|---|---:|---:|---:|---:|---|
| **New World** | `154` | `16` | `138` | `21252` | GitHub Actions run `23970499564` |
| **Woolworths NZ** | `190` | `14` | `448` | `85120` | public sitemap + free local Playwright verification |
| **PAK'nSAVE** | `60` | `15` | `81` | `4860` | public store-finder links + GitHub Actions run `23991076043` |

---

## Verification details

### New World
- **Workflow:** `Count Stores and Categories (Manual)`
- **Run ID:** `23970499564`
- **Status:** `success`
- **Run URL:** <https://github.com/salvador-cortes-c/python-playwright-scraper/actions/runs/23970499564>

Verified totals:
- **Stores:** `154`
- **Categories:** `16`
- **Total category pages:** `138`

Selected page counts from that verification:
- **Fruit & Vegetables:** `4`
- **Bakery:** `5`
- **Fridge, Deli & Eggs:** `16`
- **Pantry:** `20`
- **Snacks, Treats & Easy Meals:** `20`
- **Household & Cleaning:** `10`
- **Health & Body:** `17`
- **Hot & Cold Drinks:** `15`

### Woolworths NZ
**Corrected store total:** `190`
- Researched from the retailer’s own public `sitemap.xml`
- The sitemap exposes **192** `store-finder` URLs, of which **2** are generic finder pages rather than individual stores:
  - `/shop/content/store-finder`
  - `/store-finder`
- Final individual-store total used in the summary: **190**

**Corrected category count:** `14`
- Verified from free local Playwright discovery against `https://www.woolworths.co.nz/shop`

**Corrected page total:** `448`
- Verified from free local Playwright category scans with a selected store context (`Karori`)
- Page estimates were derived from the visible per-category item counts at page size `48`
- Example evidence:
  - `Easter`: `161` items → `4` pages
  - `Fruit & Veg`: `814` items → `17` pages
  - `Pantry`: `5273` items → `110` pages
  - `Health & Body`: `3707` items → `78` pages

### PAK'nSAVE
**Corrected store total:** `60`
- Researched from the retailer’s public `https://www.paknsave.co.nz/store-finder`
- Direct parsing of the public store-finder links returns **60** unique individual store pages

**Corrected category count:** `15`
- Verified from the public `ecom_sitemap_categories.xml`
- Also matched by the latest GitHub count-only run

**Current verified page total:** `81`
- **Workflow:** `Daily Product Scrape`
- **Run ID:** `23991076043`
- **Status:** `success`
- **Run URL:** <https://github.com/salvador-cortes-c/python-playwright-scraper/actions/runs/23991076043>
- Key log evidence:
  - `Discovered 15 category URLs from: https://www.paknsave.co.nz/`
  - `Total category pages across all categories: 81`
  - `Count summary: categories=15, pages=81`

---

## Notes
- **Stores** were corrected using the retailers’ own public store-finder/sitemap sources where those were more reliable than the automated store-picker flow.
- **Woolworths page counts** are now based on the live category item totals instead of the earlier one-page-per-category undercount.
- **PAK'nSAVE page counts** still depend on provider-based fetching and should be treated as the best currently verified low-credit total from the successful `count-only` run.
