-- Core relational schema for scraper + API

CREATE TABLE IF NOT EXISTS products (
  product_key TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  packaging_format TEXT NOT NULL DEFAULT '',
  image_url TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS supermarkets (
  id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  code TEXT NOT NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stores (
  id BIGSERIAL PRIMARY KEY,
  supermarket_id BIGINT REFERENCES supermarkets(id) ON DELETE SET NULL,
  name TEXT NOT NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS categories (
  id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  url TEXT NOT NULL UNIQUE,
  source_url TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS product_categories (
  product_key TEXT NOT NULL REFERENCES products(product_key) ON DELETE CASCADE,
  category_id BIGINT NOT NULL REFERENCES categories(id) ON DELETE CASCADE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (product_key, category_id)
);

CREATE TABLE IF NOT EXISTS crawl_runs (
  id BIGSERIAL PRIMARY KEY,
  provider TEXT NOT NULL,
  mode TEXT NOT NULL,
  started_at TIMESTAMPTZ NOT NULL,
  finished_at TIMESTAMPTZ,
  status TEXT NOT NULL,
  error_message TEXT NOT NULL DEFAULT ''
);

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

CREATE INDEX IF NOT EXISTS idx_stores_supermarket_name
  ON stores (supermarket_id, name);

CREATE INDEX IF NOT EXISTS idx_price_snapshots_product_store_time
  ON price_snapshots (product_key, store_id, scraped_at DESC);

CREATE INDEX IF NOT EXISTS idx_price_snapshots_supermarket_store_time
  ON price_snapshots (supermarket_id, store_id, scraped_at DESC);

CREATE INDEX IF NOT EXISTS idx_price_snapshots_source_url
  ON price_snapshots (source_url);
