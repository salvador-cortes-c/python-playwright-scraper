-- Returns the 20 most recently scraped products that have a price snapshot
-- in all three main supermarkets (New World, Pak'nSave, Woolworths).
--
-- For each qualifying product the most recent price per supermarket is returned,
-- giving 60 rows total (20 products × 3 stores), ordered alphabetically by
-- product name and then by store name.
--
-- Usage:
--   psql "$DATABASE_URL" -f db/recent_cross_store_snapshots.sql
--
-- Note: price_cents is divided by 100 to convert to dollar values.

WITH latest_per_supermarket AS (
    -- Most-recent price snapshot for every (product, supermarket) pair.
    SELECT DISTINCT ON (ps.product_key, ps.supermarket_id)
        ps.product_key,
        ps.supermarket_id,
        ps.price_cents,
        ps.scraped_at,
        sm.name AS supermarket_name
    FROM price_snapshots ps
    JOIN supermarkets sm ON sm.id = ps.supermarket_id
    WHERE ps.supermarket_id IS NOT NULL
      AND ps.price_cents  IS NOT NULL
    ORDER BY ps.product_key, ps.supermarket_id, ps.scraped_at DESC
),
qualifying_products AS (
    -- Products present in all 3 supermarkets; ranked by most-recent snapshot.
    SELECT
        lps.product_key,
        MAX(lps.scraped_at) AS latest_snapshot_at
    FROM latest_per_supermarket lps
    GROUP BY lps.product_key
    HAVING COUNT(DISTINCT lps.supermarket_id) = 3
    ORDER BY MAX(lps.scraped_at) DESC
    LIMIT 20
)
SELECT
    p.name                                              AS product_name,
    lps.supermarket_name                                AS store,
    '$' || TRIM(TO_CHAR(lps.price_cents / 100.0, 'FM9990.00')) AS price
FROM latest_per_supermarket lps
JOIN qualifying_products qp ON qp.product_key = lps.product_key
JOIN products            p  ON p.product_key  = lps.product_key
ORDER BY p.name ASC, lps.supermarket_name ASC;
