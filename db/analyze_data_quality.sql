-- Quick data-quality checks for the scraper database.
--
-- Usage:
--   psql "$DATABASE_URL" -f db/analyze_data_quality.sql

-- Row counts per table
SELECT 'products' AS table_name, COUNT(*) AS row_count FROM products
UNION ALL
SELECT 'supermarkets', COUNT(*) FROM supermarkets
UNION ALL
SELECT 'stores', COUNT(*) FROM stores
UNION ALL
SELECT 'categories', COUNT(*) FROM categories
UNION ALL
SELECT 'product_categories', COUNT(*) FROM product_categories
UNION ALL
SELECT 'price_snapshots', COUNT(*) FROM price_snapshots
UNION ALL
SELECT 'crawl_runs', COUNT(*) FROM crawl_runs
ORDER BY table_name;

-- Missing supermarket links after the schema migration
SELECT
    (SELECT COUNT(*) FROM stores WHERE supermarket_id IS NULL) AS stores_missing_supermarket,
    (SELECT COUNT(*) FROM price_snapshots WHERE supermarket_id IS NULL) AS snapshots_missing_supermarket;

-- Suspicious product rows that look like scraped prices instead of names
SELECT product_key, name, packaging_format, updated_at
FROM products
WHERE btrim(name) ~ '^\$?\s*\d+(?:[ .]\d{1,2})?(?:\s*(?:ea|each|kg|g|mg|l|ml|cl))?\s*$'
ORDER BY updated_at DESC, name ASC;

-- Products still missing packaging or carrying legacy empty-suffix keys
SELECT product_key, name, packaging_format, updated_at
FROM products
WHERE COALESCE(packaging_format, '') = ''
   OR right(product_key, 2) = '__'
ORDER BY updated_at DESC, name ASC;

-- Categories with no linked products
SELECT
    c.id,
    c.name,
    c.url,
    COUNT(pc.product_key) AS linked_products
FROM categories c
LEFT JOIN product_categories pc ON pc.category_id = c.id
GROUP BY c.id, c.name, c.url
HAVING COUNT(pc.product_key) = 0
ORDER BY c.name;

-- Category-page snapshot paths that still have no matching category row
WITH snapshot_paths AS (
    SELECT
        regexp_replace(split_part(lower(source_url), '?', 1), '/+$', '') AS category_path,
        COUNT(*) AS snapshot_count,
        COUNT(DISTINCT product_key) AS product_count
    FROM price_snapshots
    WHERE split_part(lower(source_url), '?', 1) LIKE '%/shop/category/%'
       OR split_part(lower(source_url), '?', 1) LIKE '%/shop/browse/%'
    GROUP BY 1
),
category_paths AS (
    SELECT regexp_replace(split_part(lower(url), '?', 1), '/+$', '') AS category_path
    FROM categories
)
SELECT sp.category_path, sp.snapshot_count, sp.product_count
FROM snapshot_paths sp
LEFT JOIN category_paths cp ON cp.category_path = sp.category_path
WHERE cp.category_path IS NULL
ORDER BY sp.snapshot_count DESC, sp.category_path;
