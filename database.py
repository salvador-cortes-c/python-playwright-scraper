from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import psycopg


_CATEGORY_PATH_PREFIXES = ("/shop/category/", "/shop/browse/")
_KNOWN_SUPERMARKETS: tuple[tuple[str, str], ...] = (
    ("New World", "newworld"),
    ("Pak'nSave", "paknsave"),
    ("Woolworths", "woolworths"),
)


@dataclass
class PersistStats:
    products_upserted: int = 0
    supermarkets_upserted: int = 0
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
        print(f"WARNING: could not convert price value to cents: {value!r}", flush=True)
        return None


_PRICE_ONLY_NAME_RE = re.compile(
    r"^\$?\s*\d+(?:[ .]\d{1,2})?(?:\s*(?:ea|each|kg|g|mg|l|ml|cl))?\s*$",
    re.IGNORECASE,
)

_PACKAGING_IN_NAME_RE = re.compile(
    r"""(?ix)
    (
        \d+\s*[x×]\s*\d+(?:\.\d+)?\s*(?:kg|g|mg|l|ml|cl)
        |
        \d+(?:\.\d+)?\s*(?:kg|g|mg|l|ml|cl|ea)
        |
        \d+\s*pack
    )\b
    """
)

# Matches a hyphen that connects two word characters (e.g. "Laid-Back").
_WORD_CONNECTING_HYPHEN_RE = re.compile(r"(\w)-(\w)")

# Known misspelling corrections applied to product names before key generation.
# Each entry is (compiled pattern, canonical replacement).  Patterns are matched
# case-insensitively; the replacement is always the canonical lowercase form.
_PRODUCT_NAME_CORRECTIONS: tuple[tuple[re.Pattern, str], ...] = (
    (re.compile(r"\blarger\b", re.IGNORECASE), "lager"),  # beer name typo
)


def _looks_like_price_only_name(value: str | None) -> bool:
    cleaned = " ".join(str(value or "").split()).strip()
    if not cleaned:
        return False
    return bool(_PRICE_ONLY_NAME_RE.fullmatch(cleaned))


def _extract_packaging_from_name(name: str | None) -> str:
    value = " ".join(str(name or "").split()).strip()
    if not value:
        return ""

    matches = list(_PACKAGING_IN_NAME_RE.finditer(value))
    if not matches:
        return ""

    packaging = " ".join(matches[-1].group(1).split()).strip()
    packaging = re.sub(r"\s*[x×]\s*", "x", packaging)
    packaging = re.sub(r"\s+(?=(?:kg|g|mg|l|ml|cl|ea)\b)", "", packaging, flags=re.IGNORECASE)
    return packaging.strip()


def _normalize_name_for_key(name: str) -> str:
    """Return a normalized lowercase form of *name* for product_key generation.

    Applies surface-form standardizations so that the same physical product
    scraped from different supermarkets is more likely to produce the same key:
    - Converts to lowercase.
    - Replaces word-connecting hyphens with spaces ("Laid-Back" → "laid back").
    - Applies a fixed list of known misspelling corrections (e.g. "Larger" → "lager").

    The original display name is never modified by this function.
    """
    result = name.lower()
    result = _WORD_CONNECTING_HYPHEN_RE.sub(r"\1 \2", result)
    for pattern, replacement in _PRODUCT_NAME_CORRECTIONS:
        result = pattern.sub(replacement, result)
    return result


def _normalize_product_record(
    product_key: str | None,
    name: str | None,
    packaging_format: str | None,
) -> tuple[str, str, str] | None:
    clean_name = " ".join(str(name or "").split()).strip()
    if not clean_name or _looks_like_price_only_name(clean_name):
        return None

    clean_packaging = " ".join(str(packaging_format or "").split()).strip()
    if not clean_packaging:
        clean_packaging = _extract_packaging_from_name(clean_name)

    key_name = _normalize_name_for_key(clean_name)
    clean_key = f"{key_name}_{clean_packaging.lower()}" if clean_packaging else key_name
    return clean_key, clean_name, clean_packaging


def _supermarket_code(name: str | None) -> str:
    normalized = str(name or "").strip().lower()
    if not normalized:
        return ""
    compact = re.sub(r"[^a-z0-9]+", "", normalized)
    if compact == "paknsave" or "paknsave" in compact:
        return "paknsave"
    if "woolworths" in normalized or "countdown" in normalized:
        return "woolworths"
    if "new world" in normalized or "newworld" in normalized:
        return "newworld"
    return normalized.replace("&", "and").replace("'", "").replace(" ", "-")


def _infer_supermarket_name(store_name: str | None = None, source_url: str | None = None) -> str:
    store_value = str(store_name or "").strip()
    code = _supermarket_code(store_value)
    if code == "paknsave":
        return "Pak'nSave"
    if code == "woolworths":
        return "Woolworths"
    if code == "newworld":
        return "New World"

    host = urlparse(str(source_url or "")).netloc.lower()
    if "paknsave.co.nz" in host:
        return "Pak'nSave"
    if "woolworths.co.nz" in host or "countdown.co.nz" in host:
        return "Woolworths"
    if "newworld.co.nz" in host:
        return "New World"
    return ""


def _snapshot_store_name(snapshot: object) -> str:
    store_name = str(getattr(snapshot, "store_name", "") or getattr(snapshot, "supermarket_name", "")).strip()
    if not store_name:
        store_name = _infer_supermarket_name(source_url=getattr(snapshot, "source_url", ""))
    return store_name


def _is_category_like_path(path: str | None) -> bool:
    normalized_path = str(path or "").rstrip("/") or str(path or "")
    return any(normalized_path.startswith(prefix) for prefix in _CATEGORY_PATH_PREFIXES)


def _page_query_name_for_url(url: str | None) -> str:
    parsed = urlparse(str(url or ""))
    query = parse_qs(parsed.query, keep_blank_values=True)
    if "page" in query:
        return "page"
    if "pg" in query:
        return "pg"
    if parsed.path.startswith("/shop/browse/"):
        return "page"
    return "pg"


def _canonical_category_url(url: str | None) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""

    parsed = urlparse(raw)
    normalized_path = parsed.path.rstrip("/") or parsed.path
    if not _is_category_like_path(normalized_path):
        return urlunparse(parsed._replace(path=normalized_path, fragment=""))

    query = parse_qs(parsed.query, keep_blank_values=True)
    query[_page_query_name_for_url(raw)] = ["1"]
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
    if not _is_category_like_path(normalized_path):
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
        if not _is_category_like_path(urlparse(canonical_url).path):
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
    return urlunparse(
        parsed._replace(
            netloc=parsed.netloc.lower(),
            path=normalized_path,
            fragment="",
        )
    )


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
            str(_snapshot_store_name(snapshot)).strip().lower(),
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


def _backfill_supermarket_refs(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        supermarket_name_to_id: dict[str, int] = {}
        for name, code in _KNOWN_SUPERMARKETS:
            cur.execute(
                """
                INSERT INTO supermarkets (name, code)
                VALUES (%s, %s)
                ON CONFLICT (code)
                DO UPDATE SET name = EXCLUDED.name
                RETURNING id;
                """,
                (name, code),
            )
            row = cur.fetchone()
            if row:
                supermarket_name_to_id[name] = int(row[0])

        cur.execute("SELECT id, name FROM stores WHERE supermarket_id IS NULL")
        for store_id, store_name in cur.fetchall():
            supermarket_name = _infer_supermarket_name(store_name=store_name)
            supermarket_id = supermarket_name_to_id.get(supermarket_name)
            if supermarket_id is not None:
                cur.execute(
                    "UPDATE stores SET supermarket_id = %s WHERE id = %s",
                    (supermarket_id, int(store_id)),
                )

        cur.execute(
            """
            SELECT ps.id, ps.source_url, COALESCE(st.name, ''), st.supermarket_id
            FROM price_snapshots ps
            LEFT JOIN stores st ON st.id = ps.store_id
            WHERE ps.supermarket_id IS NULL
            """
        )
        for snapshot_id, source_url, store_name, store_supermarket_id in cur.fetchall():
            supermarket_id = store_supermarket_id
            if supermarket_id is None:
                supermarket_name = _infer_supermarket_name(store_name=store_name, source_url=source_url)
                supermarket_id = supermarket_name_to_id.get(supermarket_name)
            if supermarket_id is not None:
                cur.execute(
                    "UPDATE price_snapshots SET supermarket_id = %s WHERE id = %s",
                    (int(supermarket_id), int(snapshot_id)),
                )


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
            CREATE TABLE IF NOT EXISTS supermarkets (
                id BIGSERIAL PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                code TEXT NOT NULL UNIQUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS stores (
                id BIGSERIAL PRIMARY KEY,
                supermarket_id BIGINT REFERENCES supermarkets(id) ON DELETE SET NULL,
                name TEXT NOT NULL UNIQUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            ALTER TABLE stores
            ADD COLUMN IF NOT EXISTS supermarket_id BIGINT REFERENCES supermarkets(id) ON DELETE SET NULL;
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS categories (
                id BIGSERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                url TEXT NOT NULL UNIQUE,
                source_url TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            ALTER TABLE categories
            ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
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
                supermarket_id BIGINT REFERENCES supermarkets(id) ON DELETE SET NULL,
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
            ALTER TABLE price_snapshots
            ADD COLUMN IF NOT EXISTS supermarket_id BIGINT REFERENCES supermarkets(id) ON DELETE SET NULL;
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
            CREATE INDEX IF NOT EXISTS idx_stores_supermarket_name
            ON stores (supermarket_id, name);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_price_snapshots_supermarket_store_time
            ON price_snapshots (supermarket_id, store_id, scraped_at DESC);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_price_snapshots_source_url
            ON price_snapshots (source_url);
            """
        )
        cur.execute(
            """
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
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_consolidation_source
            ON consolidation_log (source_product_key);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_consolidation_canonical
            ON consolidation_log (canonical_product_key);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_consolidation_status
            ON consolidation_log (status);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_consolidation_method
            ON consolidation_log (method);
            """
        )

    _backfill_supermarket_refs(conn)
    _backfill_store_refs(conn)
    _repair_product_catalog(conn)


def _backfill_store_refs(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO stores (name, supermarket_id)
            SELECT sm.name, sm.id
            FROM supermarkets sm
            WHERE NOT EXISTS (
                SELECT 1 FROM stores s
                WHERE s.supermarket_id = sm.id
                  AND s.name = sm.name
            );
            """
        )
        cur.execute(
            """
            UPDATE price_snapshots ps
            SET store_id = s.id
            FROM stores s, supermarkets sm
            WHERE ps.store_id IS NULL
              AND ps.supermarket_id = s.supermarket_id
              AND sm.id = ps.supermarket_id
              AND s.name = sm.name;
            """
        )


def _repair_product_catalog(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT product_key, name, packaging_format, image_url FROM products")
        rows = cur.fetchall()

    for product_key, name, packaging_format, image_url in rows:
        normalized = _normalize_product_record(product_key, name, packaging_format)

        with conn.cursor() as cur:
            if normalized is None:
                cur.execute("DELETE FROM products WHERE product_key = %s", (str(product_key),))
                continue

            normalized_key, clean_name, clean_packaging = normalized
            if normalized_key != str(product_key):
                cur.execute(
                    """
                    INSERT INTO products (product_key, name, packaging_format, image_url, updated_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    ON CONFLICT (product_key)
                    DO UPDATE SET
                        name = CASE WHEN EXCLUDED.name <> '' THEN EXCLUDED.name ELSE products.name END,
                        packaging_format = CASE
                            WHEN EXCLUDED.packaging_format <> '' THEN EXCLUDED.packaging_format
                            ELSE products.packaging_format
                        END,
                        image_url = CASE
                            WHEN products.image_url = '' AND EXCLUDED.image_url <> '' THEN EXCLUDED.image_url
                            ELSE products.image_url
                        END,
                        updated_at = NOW();
                    """,
                    (normalized_key, clean_name, clean_packaging, image_url or ""),
                )
                cur.execute(
                    "UPDATE price_snapshots SET product_key = %s WHERE product_key = %s",
                    (normalized_key, str(product_key)),
                )
                cur.execute(
                    """
                    INSERT INTO product_categories (product_key, category_id)
                    SELECT %s, category_id
                    FROM product_categories
                    WHERE product_key = %s
                    ON CONFLICT (product_key, category_id)
                    DO NOTHING;
                    """,
                    (normalized_key, str(product_key)),
                )
                cur.execute("DELETE FROM products WHERE product_key = %s", (str(product_key),))
            else:
                cur.execute(
                    """
                    UPDATE products
                    SET name = %s,
                        packaging_format = CASE
                            WHEN %s <> '' THEN %s
                            ELSE packaging_format
                        END,
                        updated_at = NOW()
                    WHERE product_key = %s;
                    """,
                    (clean_name, clean_packaging, clean_packaging, str(product_key)),
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

    normalized_products = []
    key_overrides: dict[str, str] = {}
    for product in products:
        normalized = _normalize_product_record(
            getattr(product, "product_key", ""),
            getattr(product, "name", ""),
            getattr(product, "packaging_format", ""),
        )
        original_key = str(getattr(product, "product_key", ""))
        if normalized is None:
            key_overrides[original_key] = ""
            continue

        normalized_key, clean_name, clean_packaging = normalized
        key_overrides[original_key] = normalized_key
        product.product_key = normalized_key
        product.name = clean_name
        product.packaging_format = clean_packaging
        normalized_products.append(product)
    products = normalized_products

    normalized_snapshots = []
    for snapshot in snapshots:
        original_key = str(getattr(snapshot, "product_key", ""))
        normalized_key = key_overrides.get(original_key, original_key)
        if not normalized_key:
            continue
        snapshot.product_key = normalized_key
        normalized_snapshots.append(snapshot)
    snapshots = dedupe_price_snapshots(normalized_snapshots)

    with psycopg.connect(database_url) as conn:
        _ensure_schema(conn)

        supermarket_name_to_id: dict[str, int] = {}
        with conn.cursor() as cur:
            cur.execute("SELECT id, name FROM supermarkets")
            for supermarket_id, supermarket_name in cur.fetchall():
                supermarket_name_to_id[str(supermarket_name)] = int(supermarket_id)

            for snapshot in snapshots:
                store_name = (getattr(snapshot, "store_name", "") or getattr(snapshot, "supermarket_name", "")).strip()
                supermarket_name = _infer_supermarket_name(
                    store_name=store_name,
                    source_url=getattr(snapshot, "source_url", ""),
                )
                if not supermarket_name or supermarket_name in supermarket_name_to_id:
                    continue
                cur.execute(
                    """
                    INSERT INTO supermarkets (name, code)
                    VALUES (%s, %s)
                    ON CONFLICT (code)
                    DO UPDATE SET name = EXCLUDED.name
                    RETURNING id;
                    """,
                    (supermarket_name, _supermarket_code(supermarket_name)),
                )
                row = cur.fetchone()
                if row:
                    supermarket_name_to_id[supermarket_name] = int(row[0])
                    # Only count if this is a new insert (check if we previously had this supermarket)
                    # If the supermarket already existed, the ON CONFLICT would have updated it
                    # Check if the supermarket was previously unknown to us (which means it's newly inserted)
                    cur.execute("SELECT xmin, xmax FROM supermarkets WHERE id = %s", (int(row[0]),))
                    version_info = cur.fetchone()
                    # A newly inserted row will have xmin set but xmax = 0
                    # An updated row will have xmax > 0
                    if version_info and version_info[1] == 0:
                        stats.supermarkets_upserted += 1
                else:
                    cur.execute("SELECT id FROM supermarkets WHERE code = %s", (_supermarket_code(supermarket_name),))
                    fallback = cur.fetchone()
                    if fallback:
                        supermarket_name_to_id[supermarket_name] = int(fallback[0])

        # Upsert products first so snapshots and links have FK targets.
        with conn.cursor() as cur:
            for product in products:
                cur.execute(
                    """
                    INSERT INTO products (product_key, name, packaging_format, image_url, updated_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    ON CONFLICT (product_key)
                    DO UPDATE SET
                        name = CASE
                            WHEN EXCLUDED.name <> '' THEN EXCLUDED.name
                            ELSE products.name
                        END,
                        packaging_format = CASE
                            WHEN EXCLUDED.packaging_format <> '' THEN EXCLUDED.packaging_format
                            ELSE products.packaging_format
                        END,
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
                        source_url = EXCLUDED.source_url,
                        updated_at = NOW();
                    """,
                    (name, canonical_url, source_url),
                )
                stats.categories_upserted += 1

        store_name_to_id: dict[str, int] = {}
        with conn.cursor() as cur:
            for snapshot in snapshots:
                store_name = _snapshot_store_name(snapshot)
                if not store_name or store_name in store_name_to_id:
                    continue
                supermarket_name = _infer_supermarket_name(store_name=store_name, source_url=snapshot.source_url)
                supermarket_id = supermarket_name_to_id.get(supermarket_name)
                cur.execute(
                    """
                    INSERT INTO stores (name, supermarket_id)
                    VALUES (%s, %s)
                    ON CONFLICT (name)
                    DO UPDATE SET
                        name = EXCLUDED.name,
                        supermarket_id = COALESCE(EXCLUDED.supermarket_id, stores.supermarket_id)
                    RETURNING id;
                    """,
                    (store_name, supermarket_id),
                )
                row = cur.fetchone()
                if row:
                    store_name_to_id[store_name] = int(row[0])
                    # Only count if this is a new insert (check version info using xmin/xmax)
                    cur.execute("SELECT xmin, xmax FROM stores WHERE id = %s", (int(row[0]),))
                    version_info = cur.fetchone()
                    if version_info and version_info[1] == 0:
                        stats.stores_upserted += 1
                else:
                    cur.execute("SELECT id FROM stores WHERE name = %s", (store_name,))
                    fallback = cur.fetchone()
                    if fallback:
                        store_name_to_id[store_name] = int(fallback[0])

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
                store_name = _snapshot_store_name(snapshot)
                store_id = store_name_to_id.get(store_name)
                supermarket_name = _infer_supermarket_name(store_name=store_name, source_url=snapshot.source_url)
                supermarket_id = supermarket_name_to_id.get(supermarket_name)

                price_cents_value = _parse_price_to_cents(snapshot.price)
                promo_price_cents_value = _parse_price_to_cents(snapshot.promo_price)
                if (
                    price_cents_value is not None
                    and promo_price_cents_value is not None
                    and promo_price_cents_value >= price_cents_value
                ):
                    print(
                        f"WARNING: promo_price ({snapshot.promo_price}) >= price ({snapshot.price})"
                        f" for product '{snapshot.product_key}'; discarding promo_price",
                        flush=True,
                    )
                    promo_price_cents_value = None

                cur.execute(
                    """
                    INSERT INTO price_snapshots (
                        product_key,
                        store_id,
                        supermarket_id,
                        price_cents,
                        unit_price_text,
                        promo_price_cents,
                        promo_unit_price_text,
                        source_url,
                        scraped_at,
                        provider,
                        crawl_run_id
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        snapshot.product_key,
                        store_id,
                        supermarket_id,
                        price_cents_value,
                        snapshot.unit_price or "",
                        promo_price_cents_value,
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
