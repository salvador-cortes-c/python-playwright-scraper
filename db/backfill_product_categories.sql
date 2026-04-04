-- Backfill missing product-to-category links from existing category-page snapshots.
--
-- Usage:
--   psql "$DATABASE_URL" -f db/backfill_product_categories.sql

BEGIN;

WITH matched_links AS (
    SELECT DISTINCT
        ps.product_key,
        c.id AS category_id
    FROM price_snapshots ps
    JOIN categories c
      ON regexp_replace(split_part(lower(ps.source_url), '?', 1), '/+$', '') =
         regexp_replace(split_part(lower(c.url), '?', 1), '/+$', '')
    WHERE split_part(lower(ps.source_url), '?', 1) LIKE '%/shop/category/%'
),
inserted AS (
    INSERT INTO product_categories (product_key, category_id)
    SELECT product_key, category_id
    FROM matched_links
    ON CONFLICT (product_key, category_id) DO NOTHING
    RETURNING product_key, category_id
)
SELECT COUNT(*) AS inserted_product_category_links
FROM inserted;

SELECT COUNT(*) AS total_product_category_links
FROM product_categories;

COMMIT;
