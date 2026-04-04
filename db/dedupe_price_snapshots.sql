WITH normalized AS (
    UPDATE price_snapshots
    SET source_url = CASE
        WHEN source_url LIKE '%/shop/browse/%'
            THEN regexp_replace(split_part(source_url, '?', 1), '/+$', '') || '?page=1'
        ELSE regexp_replace(split_part(source_url, '?', 1), '/+$', '') || '?pg=1'
    END
    WHERE (
            source_url LIKE '%/shop/category/%'
         OR source_url LIKE '%/shop/browse/%'
    )
      AND source_url <> CASE
        WHEN source_url LIKE '%/shop/browse/%'
            THEN regexp_replace(split_part(source_url, '?', 1), '/+$', '') || '?page=1'
        ELSE regexp_replace(split_part(source_url, '?', 1), '/+$', '') || '?pg=1'
    END
    RETURNING id
)
SELECT COUNT(*) AS normalized_category_source_urls
FROM normalized;

WITH ranked AS (
    SELECT
        id,
        ROW_NUMBER() OVER (
            PARTITION BY
                COALESCE(crawl_run_id, -1),
                product_key,
                COALESCE(store_id, -1),
                source_url,
                COALESCE(price_cents, -1),
                unit_price_text,
                COALESCE(promo_price_cents, -1),
                promo_unit_price_text
            ORDER BY scraped_at DESC, id DESC
        ) AS rn
    FROM price_snapshots
),
deleted AS (
    DELETE FROM price_snapshots ps
    USING ranked r
    WHERE ps.id = r.id
      AND r.rn > 1
    RETURNING ps.id
)
SELECT COUNT(*) AS deleted_duplicate_price_snapshots
FROM deleted;

SELECT
    COUNT(*) AS total_price_snapshots,
    COUNT(DISTINCT crawl_run_id) AS crawl_runs_with_snapshots,
    COUNT(DISTINCT product_key) AS products_with_snapshots
FROM price_snapshots;
