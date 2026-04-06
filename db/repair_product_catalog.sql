-- Repair category freshness timestamps and normalize malformed product rows.
--
-- Usage:
--   psql "$DATABASE_URL" -f db/repair_product_catalog.sql

BEGIN;

ALTER TABLE categories
ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

-- Refresh category freshness from the latest matching snapshot activity.
WITH latest_category_activity AS (
    SELECT
        c.id,
        MAX(ps.scraped_at) AS latest_scraped_at
    FROM categories c
    JOIN price_snapshots ps
      ON regexp_replace(split_part(lower(ps.source_url), '?', 1), '/+$', '') =
         regexp_replace(split_part(lower(c.url), '?', 1), '/+$', '')
    GROUP BY c.id
)
UPDATE categories c
SET updated_at = GREATEST(COALESCE(c.updated_at, c.created_at), latest_category_activity.latest_scraped_at)
FROM latest_category_activity
WHERE c.id = latest_category_activity.id;

-- Fill missing packaging from the product name when possible.
UPDATE products
SET
    name = btrim(regexp_replace(name, '\s+', ' ', 'g')),
    packaging_format = COALESCE(
        NULLIF(btrim(packaging_format), ''),
        NULLIF(
            btrim(
                regexp_replace(
                    regexp_replace(
                        substring(
                            name from '(?ix)(\d+\s*[x×]\s*\d+(?:\.\d+)?\s*(?:kg|g|mg|l|ml|cl)|\d+(?:\.\d+)?\s*(?:kg|g|mg|l|ml|cl|ea)|\d+\s*pack)\y'
                        ),
                        '\s*[x×]\s*',
                        'x',
                        'g'
                    ),
                    '\s+(?=(?:kg|g|mg|l|ml|cl|ea)\y)',
                    '',
                    'g'
                )
            ),
            ''
        ),
        ''
    ),
    updated_at = NOW();

-- Remove obviously bogus "products" that are just scraped price text.
DELETE FROM products
WHERE btrim(name) ~ '^\$?\s*\d+(?:[ .]\d{1,2})?(?:\s*(?:ea|each|kg|g|mg|l|ml|cl))?\s*$';

DROP TABLE IF EXISTS tmp_product_repairs;
CREATE TEMP TABLE tmp_product_repairs AS
SELECT
    p.product_key AS old_key,
    CASE
        WHEN COALESCE(NULLIF(btrim(p.packaging_format), ''), '') <> '' THEN
            lower(btrim(p.name)) || '__' || lower(btrim(p.packaging_format))
        ELSE
            lower(btrim(p.name))
    END AS new_key,
    btrim(p.name) AS clean_name,
    COALESCE(NULLIF(btrim(p.packaging_format), ''), '') AS clean_packaging,
    COALESCE(p.image_url, '') AS image_url
FROM products p
WHERE btrim(COALESCE(p.name, '')) <> '';

-- Create or refresh the normalized product rows first.
INSERT INTO products (product_key, name, packaging_format, image_url, created_at, updated_at)
SELECT new_key, clean_name, clean_packaging, image_url, NOW(), NOW()
FROM tmp_product_repairs
WHERE old_key <> new_key
ON CONFLICT (product_key)
DO UPDATE SET
    name = EXCLUDED.name,
    packaging_format = CASE
        WHEN EXCLUDED.packaging_format <> '' THEN EXCLUDED.packaging_format
        ELSE products.packaging_format
    END,
    image_url = CASE
        WHEN products.image_url = '' AND EXCLUDED.image_url <> '' THEN EXCLUDED.image_url
        ELSE products.image_url
    END,
    updated_at = NOW();

-- Repoint foreign-key references to the normalized keys.
UPDATE price_snapshots ps
SET product_key = repairs.new_key
FROM tmp_product_repairs repairs
WHERE repairs.old_key <> repairs.new_key
  AND ps.product_key = repairs.old_key;

INSERT INTO product_categories (product_key, category_id)
SELECT repairs.new_key, pc.category_id
FROM product_categories pc
JOIN tmp_product_repairs repairs
  ON repairs.old_key = pc.product_key
WHERE repairs.old_key <> repairs.new_key
ON CONFLICT (product_key, category_id) DO NOTHING;

DELETE FROM product_categories pc
USING tmp_product_repairs repairs
WHERE repairs.old_key <> repairs.new_key
  AND pc.product_key = repairs.old_key;

DELETE FROM products p
USING tmp_product_repairs repairs
WHERE repairs.old_key <> repairs.new_key
  AND p.product_key = repairs.old_key;

SELECT COUNT(*) AS categories_with_fresh_updated_at
FROM categories
WHERE updated_at >= NOW() - INTERVAL '30 days';

SELECT COUNT(*) AS products_missing_packaging_or_with_legacy_key_suffix
FROM products
WHERE COALESCE(packaging_format, '') = ''
   OR right(product_key, 2) = '__';

COMMIT;
