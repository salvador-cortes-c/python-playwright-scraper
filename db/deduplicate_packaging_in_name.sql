-- Retroactive deduplication: strip packaging size from the name portion of product keys.
--
-- Problem:
--   Many product names embed their packaging size (e.g. "Squealing Pig Rose 750mL").
--   When the same size is also stored in packaging_format the product_key ends up
--   containing the size twice, e.g.:
--     key  = "squealing pig rose 750ml_750ml"
--     name = "Squealing Pig Rose 750mL"
--     fmt  = "750mL"
--
--   If the same product is scraped from another store without the size in the title
--     key  = "squealing pig rose_750ml"
--     name = "Squealing Pig Rose"
--     fmt  = "750mL"
--   a duplicate product record is created.
--
-- Solution:
--   Mirror the Python _strip_packaging_suffix_for_key() logic in SQL:
--   strip trailing packaging-size tokens (e.g. "750mL", "12x330mL", "6 x 330ml",
--   "6 pack") from the lowercased name before building the key.  Container-type
--   words (Bottle, Can, etc.) are intentionally preserved so that genuinely
--   different variants remain distinct.
--
--   After recomputing keys:
--     • Products whose key changed but have no collision are simply renamed.
--     • Products whose new key collides with an existing row are merged into
--       that canonical row (price_snapshots and product_categories re-pointed).
--
--   The script is IDEMPOTENT: re-running it after a partial run is safe because
--   each step uses ON CONFLICT DO NOTHING / DO UPDATE guards.
--
-- Usage:
--   psql "$DATABASE_URL" -f db/deduplicate_packaging_in_name.sql

BEGIN;

-- -------------------------------------------------------------------------
-- 0. Ensure consolidation_log exists (idempotent)
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
-- 1. Compute the "stripped" name and the new canonical key for every product.
--
--    The regex removes trailing packaging-size tokens matching:
--      • NxNNN(unit)   e.g. "12x330mL", "6 x 330ml"
--      • NNN(unit)     e.g. "750mL", "1.5L"
--      • N pack        e.g. "6 pack"
--
--    Applied twice so that "6 pack 330mL" (two consecutive tokens) is fully
--    stripped.  If stripping leaves an empty string the original name is used.
--
--    The same three-step name normalisation used by _normalize_name_for_key()
--    is then applied: lowercase, word-connecting hyphen → space, larger→lager.
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS _tmp_pkg_norm;
CREATE TEMP TABLE _tmp_pkg_norm AS
WITH pkg_re AS (
    -- Single pattern string reused three times below.
    SELECT
        '\s*(\d+\s*[x×]\s*\d+(?:\.\d+)?\s*(?:kg|g|mg|l|ml|cl)'
        || '|\d+(?:\.\d+)?\s*(?:kg|g|mg|l|ml|cl|ea)'
        || '|\d+\s*pack)\s*$'  AS re
),
normalized_pkg AS (
    SELECT
        p.product_key AS old_key,
        btrim(p.name) AS display_name,
        COALESCE(NULLIF(btrim(p.packaging_format), ''), '') AS packaging,
        p.image_url,
        -- Normalize packaging the same way as _normalize_packaging():
        --   remove spaces around x-separator, remove spaces before units.
        CASE
            WHEN COALESCE(NULLIF(btrim(p.packaging_format), ''), '') <> '' THEN
                regexp_replace(
                    regexp_replace(
                        lower(btrim(p.packaging_format)),
                        '\s*[x×]\s*', 'x', 'g'
                    ),
                    '\s+(?=(?:kg|g|mg|l|ml|cl|ea))', '', 'gi'
                )
            ELSE ''
        END AS pkg_norm
    FROM products p
    WHERE btrim(COALESCE(p.name, '')) <> ''
      AND NOT (btrim(p.name) ~ '^\$?\s*\d+(?:[ .]\d{1,2})?(?:\s*(?:ea|each|kg|g|mg|l|ml|cl))?\s*$')
)
SELECT
    np.old_key,
    np.display_name,
    np.packaging,
    np.pkg_norm,
    np.image_url,
    -- Strip trailing size token(s) from the lowercased name.
    -- Applied twice: first pass removes the outermost token, second handles
    -- the preceding pack-count token (e.g. "6 pack 330mL" → "6 pack" → "").
    CASE
        WHEN np.pkg_norm <> '' THEN
            NULLIF(
                btrim(
                    regexp_replace(
                        btrim(
                            regexp_replace(
                                lower(btrim(np.display_name)),
                                (SELECT re FROM pkg_re),
                                '',
                                'ix'
                            )
                        ),
                        (SELECT re FROM pkg_re),
                        '',
                        'ix'
                    )
                ),
                ''  -- treat empty result as NULL → fall back to full name
            )
        ELSE NULL
    END AS name_stripped,
    -- Build new_key using the stripped name (or full name as fallback):
    --   1. lowercase, 2. word-connecting hyphen → space, 3. larger→lager
    CASE
        WHEN np.pkg_norm <> '' THEN
            regexp_replace(
                regexp_replace(
                    COALESCE(
                        NULLIF(
                            btrim(
                                regexp_replace(
                                    btrim(
                                        regexp_replace(
                                            lower(btrim(np.display_name)),
                                            (SELECT re FROM pkg_re),
                                            '',
                                            'ix'
                                        )
                                    ),
                                    (SELECT re FROM pkg_re),
                                    '',
                                    'ix'
                                )
                            ),
                            ''
                        ),
                        lower(btrim(np.display_name))
                    ),
                    '(\w)-(\w)', '\1 \2', 'g'
                ),
                '\mlarger\M', 'lager', 'gi'
            ) || '_' || np.pkg_norm
        ELSE
            regexp_replace(
                regexp_replace(
                    lower(btrim(np.display_name)),
                    '(\w)-(\w)', '\1 \2', 'g'
                ),
                '\mlarger\M', 'lager', 'gi'
            )
    END AS new_key
FROM normalized_pkg np;

-- -------------------------------------------------------------------------
-- 2. Choose the canonical representative for each new_key.
--
--    Priority (highest first):
--      1. Has packaging_format set
--      2. Has an image URL
--      3. Lexicographically first old_key (deterministic)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS _tmp_pkg_canonical;
CREATE TEMP TABLE _tmp_pkg_canonical AS
SELECT DISTINCT ON (new_key)
    new_key,
    old_key           AS canonical_old_key,
    display_name      AS canonical_name
FROM _tmp_pkg_norm
ORDER BY
    new_key,
    CASE WHEN packaging  <> '' THEN 0 ELSE 1 END,
    CASE WHEN image_url  <> '' THEN 0 ELSE 1 END,
    old_key;

-- -------------------------------------------------------------------------
-- 3. Full mapping: every old_key → (new_key, canonical_old_key)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS _tmp_pkg_mapping;
CREATE TEMP TABLE _tmp_pkg_mapping AS
SELECT
    n.old_key,
    n.new_key,
    c.canonical_old_key,
    n.display_name,
    n.packaging,
    n.pkg_norm,
    n.image_url
FROM _tmp_pkg_norm     n
JOIN _tmp_pkg_canonical c ON n.new_key = c.new_key;

-- Preview: show which records will be affected
SELECT
    old_key,
    new_key,
    canonical_old_key,
    CASE WHEN old_key <> new_key           THEN 'key_renamed'  ELSE 'key_unchanged' END AS key_status,
    CASE WHEN old_key <> canonical_old_key THEN 'will_merge'   ELSE 'is_canonical'  END AS merge_status
FROM _tmp_pkg_mapping
WHERE old_key <> new_key
   OR old_key <> canonical_old_key
ORDER BY new_key, old_key;

-- -------------------------------------------------------------------------
-- 4. Upsert canonical product rows for records whose key changed.
-- -------------------------------------------------------------------------
INSERT INTO products (product_key, name, packaging_format, image_url, created_at, updated_at)
SELECT
    m.new_key,
    m.display_name,
    m.packaging,
    m.image_url,
    NOW(),
    NOW()
FROM _tmp_pkg_mapping m
WHERE m.old_key = m.canonical_old_key   -- canonical representative only
  AND m.old_key <> m.new_key            -- key actually changed
ON CONFLICT (product_key) DO UPDATE
    SET
        name             = CASE WHEN EXCLUDED.name             <> '' THEN EXCLUDED.name             ELSE products.name             END,
        packaging_format = CASE WHEN EXCLUDED.packaging_format <> '' THEN EXCLUDED.packaging_format ELSE products.packaging_format END,
        image_url        = CASE WHEN products.image_url = '' AND EXCLUDED.image_url <> ''
                                THEN EXCLUDED.image_url ELSE products.image_url                     END,
        updated_at       = NOW();

-- -------------------------------------------------------------------------
-- 5. Redirect price_snapshots
--    Phase A: old_key renamed → new_key
--    Phase B: non-canonical old_key merged into canonical (same new_key)
-- -------------------------------------------------------------------------
UPDATE price_snapshots ps
SET    product_key = m.new_key
FROM   _tmp_pkg_mapping m
WHERE  ps.product_key = m.old_key
  AND  m.old_key <> m.new_key;

UPDATE price_snapshots ps
SET    product_key = m.canonical_old_key
FROM   _tmp_pkg_mapping m
WHERE  ps.product_key = m.old_key
  AND  m.old_key  = m.new_key
  AND  m.old_key <> m.canonical_old_key;

-- -------------------------------------------------------------------------
-- 6. Redirect product_categories
-- -------------------------------------------------------------------------
INSERT INTO product_categories (product_key, category_id)
SELECT m.new_key, pc.category_id
FROM   product_categories pc
JOIN   _tmp_pkg_mapping m ON pc.product_key = m.old_key
WHERE  m.old_key <> m.new_key
ON CONFLICT (product_key, category_id) DO NOTHING;

DELETE FROM product_categories pc
USING  _tmp_pkg_mapping m
WHERE  pc.product_key = m.old_key
  AND  m.old_key <> m.new_key;

INSERT INTO product_categories (product_key, category_id)
SELECT m.canonical_old_key, pc.category_id
FROM   product_categories pc
JOIN   _tmp_pkg_mapping m ON pc.product_key = m.old_key
WHERE  m.old_key  = m.new_key
  AND  m.old_key <> m.canonical_old_key
ON CONFLICT (product_key, category_id) DO NOTHING;

DELETE FROM product_categories pc
USING  _tmp_pkg_mapping m
WHERE  pc.product_key = m.old_key
  AND  m.old_key  = m.new_key
  AND  m.old_key <> m.canonical_old_key;

-- -------------------------------------------------------------------------
-- 7. Log to consolidation_log
-- -------------------------------------------------------------------------
INSERT INTO consolidation_log (
    source_product_key, canonical_product_key,
    source_product_name, canonical_product_name,
    method, reason, status, executed_at
)
SELECT DISTINCT
    m.old_key,
    m.new_key,
    m.display_name,
    m.display_name,
    'normalization',
    'Packaging size stripped from name portion of key (size already captured in packaging_format suffix)',
    'executed',
    NOW()
FROM _tmp_pkg_mapping m
WHERE m.old_key <> m.new_key
ON CONFLICT (source_product_key, canonical_product_key) DO NOTHING;

INSERT INTO consolidation_log (
    source_product_key, canonical_product_key,
    source_product_name, canonical_product_name,
    method, reason, status, executed_at
)
SELECT DISTINCT
    m.old_key,
    m.canonical_old_key,
    m.display_name,
    c.canonical_name,
    'normalization',
    'Duplicate product keys after packaging-size stripping merged into canonical',
    'executed',
    NOW()
FROM _tmp_pkg_mapping m
JOIN _tmp_pkg_canonical c ON c.new_key = m.new_key
WHERE m.old_key  = m.new_key
  AND m.old_key <> m.canonical_old_key
ON CONFLICT (source_product_key, canonical_product_key) DO NOTHING;

-- -------------------------------------------------------------------------
-- 8. Delete orphaned product rows
-- -------------------------------------------------------------------------

-- Phase A: renamed rows (canonical already upserted in step 4)
DELETE FROM products p
USING  _tmp_pkg_mapping m
WHERE  p.product_key = m.old_key
  AND  m.old_key <> m.new_key;

-- Phase B: merged rows — only delete when no snapshots remain on the old key
DELETE FROM products p
USING  _tmp_pkg_mapping m
WHERE  p.product_key = m.old_key
  AND  m.old_key  = m.new_key
  AND  m.old_key <> m.canonical_old_key
  AND  NOT EXISTS (
      SELECT 1 FROM price_snapshots ps WHERE ps.product_key = p.product_key
  );

-- -------------------------------------------------------------------------
-- 9. Final report
-- -------------------------------------------------------------------------
SELECT 'Packaging-in-name deduplication report' AS report_section, NULL::BIGINT AS count
UNION ALL
SELECT 'Products after migration',        COUNT(*)                                          FROM products
UNION ALL
SELECT 'Keys renamed (size stripped)',     COUNT(DISTINCT old_key) FILTER (WHERE old_key <> new_key)
                                                                                            FROM _tmp_pkg_mapping
UNION ALL
SELECT 'Products merged (non-canonical)', COUNT(DISTINCT old_key) FILTER (WHERE old_key <> canonical_old_key)
                                                                                            FROM _tmp_pkg_mapping
UNION ALL
SELECT 'Consolidations logged',           COUNT(*) FILTER (WHERE method = 'normalization'
                                                            AND reason ILIKE '%packaging%')
                                                                                            FROM consolidation_log
UNION ALL
SELECT 'Total price snapshots',           COUNT(*)                                          FROM price_snapshots;

COMMIT;
