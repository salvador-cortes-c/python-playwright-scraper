-- Quick data-quality checks for the scraper database.
--
-- Usage:
--   psql "$DATABASE_URL" -f db/analyze_data_quality.sql

-- Row counts per table
SELECT 'products' AS table_name, COUNT(*) AS row_count FROM products
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
