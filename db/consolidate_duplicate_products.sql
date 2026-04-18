-- Consolidate duplicate products that differ only in packaging format presence.
-- Problem: Same product scraped from different stores creates different product_keys:
--   - "product_name" (no packaging from one scraper)
--   - "product_name_format" (with packaging from another scraper)
-- Solution: Identify duplicates and merge them to a canonical product_key (preferring one with packaging).
--
-- Usage:
--   psql "$DATABASE_URL" -f db/consolidate_duplicate_products.sql

BEGIN;

-- Step 1: Identify duplicate products by name (excluding ones that are intentionally different)
WITH duplicates_by_name AS (
    SELECT
        product_key,
        name,
        packaging_format,
        image_url,
        COUNT(*) OVER (PARTITION BY name) as products_with_same_name,
        ROW_NUMBER() OVER (
            PARTITION BY name 
            ORDER BY 
                -- Prefer product with packaging format specified
                CASE WHEN packaging_format != '' THEN 0 ELSE 1 END ASC,
                -- Then prefer non-empty image URL
                CASE WHEN image_url != '' THEN 0 ELSE 1 END ASC,
                -- Finally sort by product_key for determinism
                product_key ASC
        ) as canonical_rank
    FROM products
    WHERE name NOT ILIKE '%various%'  -- Skip intentionally vague products
)
-- Show duplicates found
SELECT
    name,
    STRING_AGG(product_key, ' | ' ORDER BY canonical_rank) as product_keys,
    products_with_same_name,
    SUM(CASE WHEN canonical_rank = 1 THEN 0 ELSE 1 END) as duplicates_to_merge
FROM duplicates_by_name
WHERE products_with_same_name > 1
GROUP BY name, products_with_same_name
ORDER BY name;

-- Step 2: Build mapping of duplicate → canonical product_key
WITH duplicate_mapping AS (
    SELECT
        d1.product_key as duplicate_key,
        d1.name,
        d2.product_key as canonical_key
    FROM (
        SELECT
            product_key,
            name,
            packaging_format,
            image_url,
            COUNT(*) OVER (PARTITION BY name) as dup_count,
            ROW_NUMBER() OVER (
                PARTITION BY name 
                ORDER BY 
                    CASE WHEN packaging_format != '' THEN 0 ELSE 1 END,
                    CASE WHEN image_url != '' THEN 0 ELSE 1 END,
                    product_key
            ) as dup_rank
        FROM products
    ) d1
    JOIN (
        SELECT
            product_key,
            name,
            packaging_format,
            image_url,
            COUNT(*) OVER (PARTITION BY name) as dup_count,
            ROW_NUMBER() OVER (
                PARTITION BY name 
                ORDER BY 
                    CASE WHEN packaging_format != '' THEN 0 ELSE 1 END,
                    CASE WHEN image_url != '' THEN 0 ELSE 1 END,
                    product_key
            ) as dup_rank
        FROM products
    ) d2 ON d1.name = d2.name AND d1.dup_count > 1 AND d2.dup_rank = 1
    WHERE d1.dup_rank > 1
)
-- Step 3: Redirect price snapshots from duplicate → canonical
UPDATE price_snapshots
SET product_key = dm.canonical_key
FROM duplicate_mapping dm
WHERE price_snapshots.product_key = dm.duplicate_key;

-- Step 4: Clean up categories for orphaned products
DELETE FROM product_categories
WHERE product_key IN (
    SELECT p1.product_key
    FROM products p1
    LEFT JOIN price_snapshots ps ON ps.product_key = p1.product_key
    WHERE ps.product_key IS NULL
    AND EXISTS (
        SELECT 1 FROM products p2 
        WHERE p1.name = p2.name 
        AND p1.product_key != p2.product_key
        AND EXISTS (
            SELECT 1 FROM price_snapshots ps2 
            WHERE ps2.product_key = p2.product_key
        )
    )
);

-- Step 5: Delete orphaned duplicate products
DELETE FROM products
WHERE product_key IN (
    SELECT p1.product_key
    FROM products p1
    LEFT JOIN price_snapshots ps ON ps.product_key = p1.product_key
    WHERE ps.product_key IS NULL
    AND EXISTS (
        SELECT 1 FROM products p2 
        WHERE p1.name = p2.name 
        AND p1.product_key != p2.product_key
        AND EXISTS (
            SELECT 1 FROM price_snapshots ps2 
            WHERE ps2.product_key = p2.product_key
        )
    )
);

-- Step 6: Merge attributes from duplicate onto canonical (if image URL was missing)
WITH best_images AS (
    SELECT
        p.name,
        MAX(CASE WHEN p.image_url != '' THEN p.image_url ELSE NULL END) as best_image
    FROM products p
    GROUP BY p.name
    HAVING COUNT(*) > 0
)
UPDATE products p
SET image_url = COALESCE(p.image_url, bi.best_image)
FROM best_images bi
WHERE p.name = bi.name
AND p.image_url = ''
AND bi.best_image IS NOT NULL;

-- Step 7: Report results
SELECT 'Consolidation Report:' as section, NULL::INT as count
UNION ALL
SELECT 'Remaining products:', COUNT(DISTINCT product_key) FROM products
UNION ALL
SELECT 'Remaining unique names:', COUNT(DISTINCT name) FROM products
UNION ALL
SELECT 'Products with duplicates:', COUNT(*) 
FROM (SELECT name FROM products GROUP BY name HAVING COUNT(*) > 1) t
UNION ALL
SELECT 'Total price snapshots:', COUNT(*) FROM price_snapshots;

COMMIT;
