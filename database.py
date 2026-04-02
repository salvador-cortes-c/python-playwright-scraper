from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

import psycopg


@dataclass
class PersistStats:
    products_upserted: int = 0
    stores_upserted: int = 0
    categories_upserted: int = 0
    snapshots_inserted: int = 0
    product_category_links_upserted: int = 0


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_price_to_cents(value: str | None) -> int | None:
    if not value:
        return None
    cleaned = str(value).strip().replace("$", "")
    if not cleaned:
        return None
    try:
        return int(round(float(cleaned) * 100))
    except Exception:
        return None


def _ensure_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS products (
                product_key TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                packaging_format TEXT NOT NULL DEFAULT '',
                image_url TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS stores (
                id BIGSERIAL PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS categories (
                id BIGSERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                url TEXT NOT NULL UNIQUE,
                source_url TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS product_categories (
                product_key TEXT NOT NULL REFERENCES products(product_key) ON DELETE CASCADE,
                category_id BIGINT NOT NULL REFERENCES categories(id) ON DELETE CASCADE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (product_key, category_id)
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS crawl_runs (
                id BIGSERIAL PRIMARY KEY,
                provider TEXT NOT NULL,
                mode TEXT NOT NULL,
                started_at TIMESTAMPTZ NOT NULL,
                finished_at TIMESTAMPTZ,
                status TEXT NOT NULL,
                error_message TEXT NOT NULL DEFAULT ''
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS price_snapshots (
                id BIGSERIAL PRIMARY KEY,
                product_key TEXT NOT NULL REFERENCES products(product_key) ON DELETE CASCADE,
                store_id BIGINT REFERENCES stores(id) ON DELETE SET NULL,
                price_cents INTEGER,
                unit_price_text TEXT NOT NULL DEFAULT '',
                promo_price_cents INTEGER,
                promo_unit_price_text TEXT NOT NULL DEFAULT '',
                source_url TEXT NOT NULL,
                scraped_at TIMESTAMPTZ NOT NULL,
                provider TEXT NOT NULL,
                crawl_run_id BIGINT REFERENCES crawl_runs(id) ON DELETE SET NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_price_snapshots_product_store_time
            ON price_snapshots (product_key, store_id, scraped_at DESC);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_price_snapshots_source_url
            ON price_snapshots (source_url);
            """
        )


def _insert_crawl_run(
    conn: psycopg.Connection,
    provider: str,
    mode: str,
    started_at: datetime,
    status: str,
    error_message: str = "",
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO crawl_runs (provider, mode, started_at, finished_at, status, error_message)
            VALUES (%s, %s, %s, NOW(), %s, %s)
            RETURNING id;
            """,
            (provider, mode, started_at, status, error_message),
        )
        row = cur.fetchone()
        return int(row[0])


def persist_scrape_results(
    *,
    database_url: str,
    provider: str,
    mode: str,
    started_at: datetime,
    products: Iterable,
    snapshots: Iterable,
    categories: Iterable,
) -> PersistStats:
    stats = PersistStats()

    with psycopg.connect(database_url) as conn:
        _ensure_schema(conn)

        # Upsert products first so snapshots and links have FK targets.
        with conn.cursor() as cur:
            for product in products:
                cur.execute(
                    """
                    INSERT INTO products (product_key, name, packaging_format, image_url, updated_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    ON CONFLICT (product_key)
                    DO UPDATE SET
                        name = EXCLUDED.name,
                        packaging_format = EXCLUDED.packaging_format,
                        image_url = CASE
                            WHEN products.image_url = '' AND EXCLUDED.image_url <> '' THEN EXCLUDED.image_url
                            ELSE products.image_url
                        END,
                        updated_at = NOW();
                    """,
                    (product.product_key, product.name, product.packaging_format or "", product.image or ""),
                )
                stats.products_upserted += 1

            for category in categories:
                cur.execute(
                    """
                    INSERT INTO categories (name, url, source_url)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (url)
                    DO UPDATE SET
                        name = EXCLUDED.name,
                        source_url = EXCLUDED.source_url;
                    """,
                    (category.name, category.url, category.source_url),
                )
                stats.categories_upserted += 1

        store_name_to_id: dict[str, int] = {}
        with conn.cursor() as cur:
            for snapshot in snapshots:
                store_name = (snapshot.supermarket_name or "").strip()
                if not store_name or store_name in store_name_to_id:
                    continue
                cur.execute(
                    """
                    INSERT INTO stores (name)
                    VALUES (%s)
                    ON CONFLICT (name)
                    DO UPDATE SET name = EXCLUDED.name
                    RETURNING id;
                    """,
                    (store_name,),
                )
                row = cur.fetchone()
                if row:
                    store_name_to_id[store_name] = int(row[0])
                else:
                    cur.execute("SELECT id FROM stores WHERE name = %s", (store_name,))
                    fallback = cur.fetchone()
                    if fallback:
                        store_name_to_id[store_name] = int(fallback[0])
                stats.stores_upserted += 1

        with conn.cursor() as cur:
            category_url_to_id: dict[str, int] = {}
            cur.execute("SELECT id, url FROM categories")
            for category_id, url in cur.fetchall():
                category_url_to_id[str(url)] = int(category_id)

            run_id = _insert_crawl_run(
                conn,
                provider=provider,
                mode=mode,
                started_at=started_at,
                status="success",
            )

            for snapshot in snapshots:
                store_name = (snapshot.supermarket_name or "").strip()
                store_id = store_name_to_id.get(store_name)

                cur.execute(
                    """
                    INSERT INTO price_snapshots (
                        product_key,
                        store_id,
                        price_cents,
                        unit_price_text,
                        promo_price_cents,
                        promo_unit_price_text,
                        source_url,
                        scraped_at,
                        provider,
                        crawl_run_id
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        snapshot.product_key,
                        store_id,
                        _parse_price_to_cents(snapshot.price),
                        snapshot.unit_price or "",
                        _parse_price_to_cents(snapshot.promo_price),
                        snapshot.promo_unit_price or "",
                        snapshot.source_url,
                        snapshot.scraped_at,
                        provider,
                        run_id,
                    ),
                )
                stats.snapshots_inserted += 1

                category_id = category_url_to_id.get(snapshot.source_url)
                if category_id is not None:
                    cur.execute(
                        """
                        INSERT INTO product_categories (product_key, category_id)
                        VALUES (%s, %s)
                        ON CONFLICT (product_key, category_id)
                        DO NOTHING;
                        """,
                        (snapshot.product_key, category_id),
                    )
                    stats.product_category_links_upserted += 1

    return stats


def resolve_database_url(value: str | None) -> str | None:
    if value:
        return value
    return os.getenv("DATABASE_URL")
