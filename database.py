from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

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


def _canonical_category_url(url: str | None) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""

    parsed = urlparse(raw)
    normalized_path = parsed.path.rstrip("/") or parsed.path
    if not normalized_path.startswith("/shop/category/"):
        return urlunparse(parsed._replace(path=normalized_path, fragment=""))

    query = parse_qs(parsed.query, keep_blank_values=True)
    query["pg"] = ["1"]
    return urlunparse(
        parsed._replace(
            netloc=parsed.netloc.lower(),
            path=normalized_path,
            query=urlencode(query, doseq=True),
            fragment="",
        )
    )


def _category_lookup_keys(url: str | None) -> tuple[str, ...]:
    raw = str(url or "").strip()
    if not raw:
        return ()

    parsed = urlparse(raw)
    normalized_path = parsed.path.rstrip("/") or parsed.path
    if not normalized_path.startswith("/shop/category/"):
        return ()

    candidates: list[str] = []
    seen: set[str] = set()

    def add(value: str) -> None:
        cleaned = str(value).strip()
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            candidates.append(cleaned)

    add(urlunparse(parsed._replace(netloc=parsed.netloc.lower(), path=normalized_path, fragment="")))
    add(_canonical_category_url(raw))
    add(urlunparse(parsed._replace(netloc=parsed.netloc.lower(), path=normalized_path, query="", fragment="")))
    add(normalized_path.lower())

    return tuple(candidates)


def _build_category_lookup(rows: Iterable[tuple[int, str]]) -> dict[str, int]:
    lookup: dict[str, int] = {}
    for category_id, url in rows:
        for key in _category_lookup_keys(str(url)):
            lookup.setdefault(key, int(category_id))
    return lookup


def _find_category_id_for_source_url(category_lookup: dict[str, int], source_url: str | None) -> int | None:
    for key in _category_lookup_keys(source_url):
        category_id = category_lookup.get(key)
        if category_id is not None:
            return category_id
    return None


def _category_name_from_url(url: str | None) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    parsed = urlparse(raw)
    slug = (parsed.path.rstrip("/") or parsed.path).rsplit("/", 1)[-1]
    if not slug:
        return raw
    return slug.replace("-", " ").replace("_", " ").strip().title() or raw


def _collect_category_rows(categories: Iterable, snapshots: Iterable) -> list[tuple[str, str, str]]:
    rows: list[tuple[str, str, str]] = []
    seen_urls: set[str] = set()

    def add(name: str, url: str, source_url: str) -> None:
        canonical_url = _canonical_category_url(url)
        if not canonical_url or canonical_url in seen_urls:
            return
        if not urlparse(canonical_url).path.startswith("/shop/category/"):
            return
        seen_urls.add(canonical_url)
        rows.append((name or _category_name_from_url(canonical_url), canonical_url, source_url or canonical_url))

    for category in categories:
        add(getattr(category, "name", ""), getattr(category, "url", ""), getattr(category, "source_url", ""))

    for snapshot in snapshots:
        source_url = getattr(snapshot, "source_url", "")
        add(_category_name_from_url(source_url), source_url, source_url)

    return rows


def _canonical_snapshot_source_url(url: str | None) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    parsed = urlparse(raw)
    normalized_path = parsed.path.rstrip("/") or parsed.path
    if normalized_path.startswith("/shop/category/"):
        return _canonical_category_url(raw)
    return urlunparse(parsed._replace(netloc=parsed.netloc.lower(), path=normalized_path, fragment=""))


def dedupe_price_snapshots(snapshots: Iterable) -> list:
    deduped: list = []
    seen: dict[tuple[str, str, str, str, str, str, str], int] = {}

    def score(snapshot: object) -> int:
        values = [
            getattr(snapshot, "price", ""),
            getattr(snapshot, "unit_price", ""),
            getattr(snapshot, "promo_price", ""),
            getattr(snapshot, "promo_unit_price", ""),
        ]
        return sum(1 for value in values if str(value or "").strip())

    for snapshot in snapshots:
        current_key = (
            str(getattr(snapshot, "product_key", "")).strip().lower(),
            str(getattr(snapshot, "supermarket_name", "")).strip().lower(),
            _canonical_snapshot_source_url(getattr(snapshot, "source_url", "")),
            str(getattr(snapshot, "price", "") or "").strip(),
            str(getattr(snapshot, "unit_price", "") or "").strip(),
            str(getattr(snapshot, "promo_price", "") or "").strip(),
            str(getattr(snapshot, "promo_unit_price", "") or "").strip(),
        )

        existing_index = seen.get(current_key)
        if existing_index is None:
            seen[current_key] = len(deduped)
            deduped.append(snapshot)
            continue

        if score(snapshot) > score(deduped[existing_index]):
            deduped[existing_index] = snapshot

    return deduped


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
    products = list(products)
    snapshots = dedupe_price_snapshots(list(snapshots))
    categories = list(categories)

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

            for name, canonical_url, source_url in _collect_category_rows(categories, snapshots):
                cur.execute(
                    """
                    INSERT INTO categories (name, url, source_url)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (url)
                    DO UPDATE SET
                        name = EXCLUDED.name,
                        source_url = EXCLUDED.source_url;
                    """,
                    (name, canonical_url, source_url),
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
            cur.execute("SELECT id, url FROM categories")
            category_url_to_id = _build_category_lookup(cur.fetchall())

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
                        _canonical_snapshot_source_url(snapshot.source_url),
                        snapshot.scraped_at,
                        provider,
                        run_id,
                    ),
                )
                stats.snapshots_inserted += 1

                category_id = _find_category_id_for_source_url(category_url_to_id, snapshot.source_url)
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
