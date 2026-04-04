INSERT INTO supermarkets (name, code)
VALUES
    ('New World', 'newworld'),
    ('Pak''nSave', 'paknsave'),
    ('Woolworths', 'woolworths')
ON CONFLICT (code) DO UPDATE SET name = EXCLUDED.name;

WITH updated_stores AS (
    UPDATE stores s
    SET supermarket_id = sm.id
    FROM supermarkets sm
    WHERE s.supermarket_id IS NULL
      AND (
            (lower(s.name) LIKE 'new world%' AND sm.code = 'newworld')
         OR ((lower(s.name) LIKE 'pak''nsave%' OR lower(s.name) LIKE 'paknsave%') AND sm.code = 'paknsave')
         OR ((lower(s.name) LIKE 'woolworths%' OR lower(s.name) LIKE 'countdown%') AND sm.code = 'woolworths')
      )
    RETURNING s.id
)
SELECT COUNT(*) AS stores_linked_to_supermarkets
FROM updated_stores;

WITH updated_snapshots AS (
    UPDATE price_snapshots ps
    SET supermarket_id = st.supermarket_id
    FROM stores st
    WHERE ps.supermarket_id IS NULL
      AND ps.store_id = st.id
      AND st.supermarket_id IS NOT NULL
    RETURNING ps.id
)
SELECT COUNT(*) AS snapshots_linked_from_stores
FROM updated_snapshots;

WITH updated_snapshots AS (
    UPDATE price_snapshots ps
    SET supermarket_id = sm.id
    FROM supermarkets sm
    WHERE ps.supermarket_id IS NULL
      AND (
            (lower(ps.source_url) LIKE '%newworld.co.nz%' AND sm.code = 'newworld')
         OR (lower(ps.source_url) LIKE '%paknsave.co.nz%' AND sm.code = 'paknsave')
         OR ((lower(ps.source_url) LIKE '%woolworths.co.nz%' OR lower(ps.source_url) LIKE '%countdown.co.nz%') AND sm.code = 'woolworths')
      )
    RETURNING ps.id
)
SELECT COUNT(*) AS snapshots_linked_from_urls
FROM updated_snapshots;

SELECT
    COUNT(*) AS supermarkets_total,
    (SELECT COUNT(*) FROM stores WHERE supermarket_id IS NOT NULL) AS stores_with_supermarket,
    (SELECT COUNT(*) FROM price_snapshots WHERE supermarket_id IS NOT NULL) AS snapshots_with_supermarket
FROM supermarkets;
