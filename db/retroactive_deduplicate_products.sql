-- Retroactive product deduplication migration for Neon DB
--
-- Applies the same normalization logic as Python's _normalize_name_for_key():
--   1. Lowercase the product name
--   2. Replace word-connecting hyphens with spaces  (e.g. "Laid-Back" → "laid back")
--   3. Correct known misspellings               (e.g. "Larger" → "Lager")
--   4. Rebuild product_key as:
--        <normalized_name>_<packaging_format>   when packaging is present
--        <normalized_name>                      otherwise
--
-- Then merges any products that map to the same normalized key, redirecting all
-- foreign-key references (price_snapshots, product_categories) to the chosen
-- canonical record and removing the orphaned duplicates.
--
-- A full audit trail is written to the consolidation_log table.
--
-- This script is IDEMPOTENT: re-running it after a partial run is safe.
--
-- Usage:
--   psql "$DATABASE_URL" -f db/retroactive_deduplicate_products.sql

BEGIN;

-- -------------------------------------------------------------------------
-- 0. Ensure consolidation_log table exists (idempotent)
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS consolidation_log (
    id                    SERIAL PRIMARY KEY,
    source_product_key    VARCHAR(255) NOT NULL,
    canonical_product_key VARCHAR(255) NOT NULL,
    source_product_name   VARCHAR(500),
    canonical_product_name VARCHAR(500),
    method                VARCHAR(50)  NOT NULL DEFAULT 'normalization',
    similarity_score      FLOAT,
    reason                TEXT,
    created_at            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    executed_at           TIMESTAMP,
    status                VARCHAR(50)  NOT NULL DEFAULT 'executed',
    snapshots_migrated    INT          NOT NULL DEFAULT 0,
    categories_migrated   INT          NOT NULL DEFAULT 0,
    error_message         TEXT,
    CONSTRAINT unique_consolidation UNIQUE (source_product_key, canonical_product_key)
);

CREATE INDEX IF NOT EXISTS idx_consolidation_source    ON consolidation_log (source_product_key);
CREATE INDEX IF NOT EXISTS idx_consolidation_canonical ON consolidation_log (canonical_product_key);
CREATE INDEX IF NOT EXISTS idx_consolidation_status    ON consolidation_log (status);
CREATE INDEX IF NOT EXISTS idx_consolidation_method    ON consolidation_log (method);

-- -------------------------------------------------------------------------
-- 1. Compute the normalized key for every product.
--
--    The SQL mirrors Python _normalize_name_for_key():
--      a) lower(btrim(name))
--      b) regexp_replace( …, '(\w)-(\w)', '\1 \2', 'g' )   — dehyphenate
--      c) regexp_replace( …, '\mlarger\M', 'lager',  'gi' ) — fix typo
--
--    Then append packaging_format (lowercased) with '_' separator when present,
--    matching the Python _normalize_product_record() key format.
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS _tmp_norm;
CREATE TEMP TABLE _tmp_norm AS
SELECT
    p.product_key  AS old_key,
    btrim(p.name)  AS display_name,
    COALESCE(NULLIF(btrim(p.packaging_format), ''), '') AS packaging,
    p.image_url,
    -- Apply three-step normalization then build the key
    CASE
        WHEN COALESCE(NULLIF(btrim(p.packaging_format), ''), '') <> '' THEN
            regexp_replace(
                regexp_replace(
                    lower(btrim(p.name)),
                    '(\w)-(\w)', '\1 \2', 'g'
                ),
                '\mlarger\M', 'lager', 'gi'
            ) || '_' || lower(btrim(p.packaging_format))
        ELSE
            regexp_replace(
                regexp_replace(
                    lower(btrim(p.name)),
                    '(\w)-(\w)', '\1 \2', 'g'
                ),
                '\mlarger\M', 'lager', 'gi'
            )
    END AS new_key
FROM products p
WHERE btrim(COALESCE(p.name, '')) <> ''
  -- Skip price-only "product" names (e.g. "$5.00", "3 kg")
  AND NOT (btrim(p.name) ~ '^\$?\s*\d+(?:[ .]\d{1,2})?(?:\s*(?:ea|each|kg|g|mg|l|ml|cl))?\s*$');

-- -------------------------------------------------------------------------
-- 2. Choose the canonical representative for each normalized key.
--
--    Priority (highest first):
--      1. Has packaging format set
--      2. Has an image URL
--      3. Lexicographically first old_key (deterministic)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS _tmp_canonical;
CREATE TEMP TABLE _tmp_canonical AS
SELECT DISTINCT ON (new_key)
    new_key,
    old_key           AS canonical_old_key,
    display_name      AS canonical_name
FROM _tmp_norm
ORDER BY
    new_key,
    CASE WHEN packaging  <> '' THEN 0 ELSE 1 END,
    CASE WHEN image_url  <> '' THEN 0 ELSE 1 END,
    old_key;

-- -------------------------------------------------------------------------
-- 3. Full mapping: every old_key → (new_key, canonical_old_key)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS _tmp_mapping;
CREATE TEMP TABLE _tmp_mapping AS
SELECT
    n.old_key,
    n.new_key,
    c.canonical_old_key,
    n.display_name,
    n.packaging,
    n.image_url
FROM _tmp_norm     n
JOIN _tmp_canonical c ON n.new_key = c.new_key;

-- Preview: show which records will change
SELECT
    old_key,
    new_key,
    canonical_old_key,
    CASE WHEN old_key <> new_key            THEN 'key_renamed'  ELSE 'key_unchanged' END AS key_status,
    CASE WHEN old_key <> canonical_old_key  THEN 'will_merge'   ELSE 'is_canonical'  END AS merge_status
FROM _tmp_mapping
WHERE old_key <> new_key
   OR old_key <> canonical_old_key
ORDER BY new_key, old_key;

-- -------------------------------------------------------------------------
-- 4. Insert / update canonical product rows for records whose key changed.
--    Only the row that IS the canonical gets upserted (others are merged in).
-- -------------------------------------------------------------------------
INSERT INTO products (product_key, name, packaging_format, image_url, created_at, updated_at)
SELECT
    m.new_key,
    m.display_name,
    m.packaging,
    m.image_url,
    NOW(),
    NOW()
FROM _tmp_mapping m
WHERE m.old_key = m.canonical_old_key   -- only the canonical representative
  AND m.old_key <> m.new_key            -- only where the key actually changed
ON CONFLICT (product_key) DO UPDATE
    SET
        name             = CASE WHEN EXCLUDED.name             <> '' THEN EXCLUDED.name             ELSE products.name             END,
        packaging_format = CASE WHEN EXCLUDED.packaging_format <> '' THEN EXCLUDED.packaging_format ELSE products.packaging_format END,
        image_url        = CASE WHEN products.image_url = '' AND EXCLUDED.image_url <> ''
                                THEN EXCLUDED.image_url ELSE products.image_url                     END,
        updated_at       = NOW();

-- -------------------------------------------------------------------------
-- 5. Redirect price_snapshots
--
--    Phase A: old_key was renamed to new_key
--    Phase B: old_key had the same text key but isn't the canonical
--             (two products normalise to the same key → merge)
-- -------------------------------------------------------------------------

-- Phase A: key renamed
UPDATE price_snapshots ps
SET    product_key = m.new_key
FROM   _tmp_mapping m
WHERE  ps.product_key = m.old_key
  AND  m.old_key <> m.new_key;

-- Phase B: merge non-canonical into canonical (same new_key, different old_key)
UPDATE price_snapshots ps
SET    product_key = m.canonical_old_key
FROM   _tmp_mapping m
WHERE  ps.product_key = m.old_key
  AND  m.old_key  = m.new_key           -- key text didn't change …
  AND  m.old_key <> m.canonical_old_key; -- … but this record isn't canonical

-- -------------------------------------------------------------------------
-- 6. Redirect product_categories
-- -------------------------------------------------------------------------

-- Phase A: key renamed — copy categories to new key, then drop old entries
INSERT INTO product_categories (product_key, category_id)
SELECT m.new_key, pc.category_id
FROM   product_categories pc
JOIN   _tmp_mapping m ON pc.product_key = m.old_key
WHERE  m.old_key <> m.new_key
ON CONFLICT (product_key, category_id) DO NOTHING;

DELETE FROM product_categories pc
USING  _tmp_mapping m
WHERE  pc.product_key = m.old_key
  AND  m.old_key <> m.new_key;

-- Phase B: merge non-canonical categories into canonical
INSERT INTO product_categories (product_key, category_id)
SELECT m.canonical_old_key, pc.category_id
FROM   product_categories pc
JOIN   _tmp_mapping m ON pc.product_key = m.old_key
WHERE  m.old_key  = m.new_key
  AND  m.old_key <> m.canonical_old_key
ON CONFLICT (product_key, category_id) DO NOTHING;

DELETE FROM product_categories pc
USING  _tmp_mapping m
WHERE  pc.product_key = m.old_key
  AND  m.old_key  = m.new_key
  AND  m.old_key <> m.canonical_old_key;

-- -------------------------------------------------------------------------
-- 7. Log all consolidations to consolidation_log (audit trail)
-- -------------------------------------------------------------------------

-- 7a. Key-renamed records
INSERT INTO consolidation_log (
    source_product_key,
    canonical_product_key,
    source_product_name,
    canonical_product_name,
    method,
    reason,
    status,
    executed_at
)
SELECT DISTINCT
    m.old_key,
    m.new_key,
    m.display_name,
    m.display_name,
    'normalization',
    'Retroactive key normalization: word-connecting hyphen replacement and/or misspelling correction (larger→lager)',
    'executed',
    NOW()
FROM _tmp_mapping m
WHERE m.old_key <> m.new_key
ON CONFLICT (source_product_key, canonical_product_key) DO NOTHING;

-- 7b. Same-key records merged into canonical
INSERT INTO consolidation_log (
    source_product_key,
    canonical_product_key,
    source_product_name,
    canonical_product_name,
    method,
    reason,
    status,
    executed_at
)
SELECT DISTINCT
    m.old_key,
    m.canonical_old_key,
    m.display_name,
    c.canonical_name,
    'normalization',
    'Retroactive deduplication: multiple product keys normalise to the same canonical key',
    'executed',
    NOW()
FROM _tmp_mapping m
JOIN _tmp_canonical c ON c.new_key = m.new_key
WHERE m.old_key  = m.new_key
  AND m.old_key <> m.canonical_old_key
ON CONFLICT (source_product_key, canonical_product_key) DO NOTHING;

-- -------------------------------------------------------------------------
-- 8. Delete orphaned product rows
--
--    Phase A: rows whose key was renamed (now re-inserted under new_key)
--    Phase B: rows that were merged into the canonical and have no remaining
--             price_snapshots
-- -------------------------------------------------------------------------

-- Phase A: renamed rows (safe to delete — canonical row was upserted in step 4)
DELETE FROM products p
USING  _tmp_mapping m
WHERE  p.product_key = m.old_key
  AND  m.old_key <> m.new_key;

-- Phase B: merged rows — only delete when no snapshots remain on the old key
DELETE FROM products p
USING  _tmp_mapping m
WHERE  p.product_key = m.old_key
  AND  m.old_key  = m.new_key
  AND  m.old_key <> m.canonical_old_key
  AND  NOT EXISTS (
      SELECT 1 FROM price_snapshots ps WHERE ps.product_key = p.product_key
  );

-- -------------------------------------------------------------------------
-- 9. Final report
-- -------------------------------------------------------------------------
SELECT 'Retroactive Deduplication Report' AS report_section,        NULL::BIGINT AS count
UNION ALL
SELECT 'Products after migration',     COUNT(*)                                        FROM products
UNION ALL
SELECT 'Unique normalized keys',        COUNT(DISTINCT new_key)                        FROM _tmp_mapping
UNION ALL
SELECT 'Keys renamed',                  COUNT(DISTINCT old_key) FILTER (WHERE old_key <> new_key)
                                                                                       FROM _tmp_mapping
UNION ALL
SELECT 'Products merged (non-canonical)', COUNT(DISTINCT old_key) FILTER (WHERE old_key <> canonical_old_key)
                                                                                       FROM _tmp_mapping
UNION ALL
SELECT 'Consolidations logged',         COUNT(*) FILTER (WHERE method = 'normalization')
                                                                                       FROM consolidation_log
UNION ALL
SELECT 'Total price snapshots',         COUNT(*)                                       FROM price_snapshots;

COMMIT;
