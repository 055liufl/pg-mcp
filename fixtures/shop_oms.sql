-- =============================================================================
-- Database: shop_oms (MEDIUM)
-- Purpose:  E-commerce / Order Management System — medium-scale schema
-- Scale:    4 schemas, 22 tables, 3 views (1 materialized), 7 enums,
--           1 composite type, ~30 indexes (btree/GIN/partial/expression),
--           ~5,000+ rows
-- Use case: Tests pg-mcp full-context schema injection across multiple
--           schemas, materialized views, composite types, and partial
--           indexes
-- =============================================================================

-- Required extensions
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS btree_gin;
CREATE EXTENSION IF NOT EXISTS citext;

-- Drop existing objects in dependency order
DROP MATERIALIZED VIEW IF EXISTS sales.top_customers           CASCADE;
DROP MATERIALIZED VIEW IF EXISTS sales.monthly_revenue         CASCADE;
DROP VIEW              IF EXISTS catalog.in_stock_products      CASCADE;
DROP SCHEMA IF EXISTS billing CASCADE;
DROP SCHEMA IF EXISTS sales   CASCADE;
DROP SCHEMA IF EXISTS catalog CASCADE;
DROP SCHEMA IF EXISTS users   CASCADE;

-- =============================================================================
-- SCHEMAS
-- =============================================================================

CREATE SCHEMA catalog;  COMMENT ON SCHEMA catalog IS '商品目录：品牌、分类、商品、SKU、图片';
CREATE SCHEMA sales;    COMMENT ON SCHEMA sales   IS '销售：购物车、订单、优惠券、发货';
CREATE SCHEMA billing;  COMMENT ON SCHEMA billing IS '账单：支付方式、支付、发票、退款';
CREATE SCHEMA users;    COMMENT ON SCHEMA users   IS '用户：客户档案、地址、会员等级、登录审计';

-- =============================================================================
-- TYPES
-- =============================================================================

CREATE TYPE catalog.product_status   AS ENUM ('draft', 'active', 'discontinued', 'out_of_stock');
CREATE TYPE sales.order_status       AS ENUM ('pending', 'paid', 'shipped', 'delivered', 'cancelled', 'refunded');
CREATE TYPE sales.shipment_status    AS ENUM ('pending', 'in_transit', 'delivered', 'lost', 'returned');
CREATE TYPE billing.payment_status   AS ENUM ('pending', 'authorized', 'captured', 'failed', 'refunded');
CREATE TYPE billing.refund_reason    AS ENUM ('customer_request', 'damaged', 'wrong_item', 'fraud', 'other');
CREATE TYPE users.address_type       AS ENUM ('billing', 'shipping', 'both');
CREATE TYPE users.loyalty_tier       AS ENUM ('bronze', 'silver', 'gold', 'platinum');

COMMENT ON TYPE catalog.product_status IS '商品上架状态';
COMMENT ON TYPE sales.order_status     IS '订单状态机';
COMMENT ON TYPE sales.shipment_status  IS '发货物流状态';
COMMENT ON TYPE billing.payment_status IS '支付状态';
COMMENT ON TYPE users.loyalty_tier     IS '会员等级';

-- Composite type used by addresses
CREATE TYPE users.postal_address AS (
    line1       VARCHAR(120),
    line2       VARCHAR(120),
    city        VARCHAR(80),
    state       VARCHAR(80),
    postal_code VARCHAR(20),
    country     CHAR(2)
);
COMMENT ON TYPE users.postal_address IS '邮政地址复合类型 (ISO-3166 alpha-2 country)';

-- =============================================================================
-- TABLES: users
-- =============================================================================

CREATE TABLE users.customers (
    id              BIGSERIAL PRIMARY KEY,
    email           CITEXT      NOT NULL UNIQUE,
    full_name       VARCHAR(120) NOT NULL,
    phone           VARCHAR(30),
    date_of_birth   DATE,
    loyalty_tier    users.loyalty_tier NOT NULL DEFAULT 'bronze',
    lifetime_value  NUMERIC(12,2) NOT NULL DEFAULT 0,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    marketing_opt_in BOOLEAN NOT NULL DEFAULT FALSE,
    metadata        JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT customers_lifetime_value_nonneg CHECK (lifetime_value >= 0)
);
COMMENT ON TABLE  users.customers              IS '客户主档案';
COMMENT ON COLUMN users.customers.email        IS '邮箱（大小写不敏感唯一）';
COMMENT ON COLUMN users.customers.loyalty_tier IS '会员等级';
COMMENT ON COLUMN users.customers.lifetime_value IS '历史消费总额（含税前）';
COMMENT ON COLUMN users.customers.metadata     IS 'JSONB 扩展字段';

CREATE TABLE users.addresses (
    id              BIGSERIAL PRIMARY KEY,
    customer_id     BIGINT NOT NULL REFERENCES users.customers(id) ON DELETE CASCADE,
    address_type    users.address_type NOT NULL DEFAULT 'shipping',
    is_default      BOOLEAN NOT NULL DEFAULT FALSE,
    address         users.postal_address NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE  users.addresses          IS '客户地址（支持 billing/shipping）';
COMMENT ON COLUMN users.addresses.address  IS '复合类型，包含国家/邮编/街道';

CREATE TABLE users.logins (
    id          BIGSERIAL PRIMARY KEY,
    customer_id BIGINT REFERENCES users.customers(id) ON DELETE SET NULL,
    ip          INET,
    user_agent  TEXT,
    success     BOOLEAN NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE users.logins IS '登录审计日志（成功+失败）';

-- =============================================================================
-- TABLES: catalog
-- =============================================================================

CREATE TABLE catalog.brands (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(80) NOT NULL UNIQUE,
    slug        VARCHAR(80) NOT NULL UNIQUE,
    country     CHAR(2),
    founded_year SMALLINT
);
COMMENT ON TABLE catalog.brands IS '品牌主档';

CREATE TABLE catalog.categories (
    id          SERIAL PRIMARY KEY,
    parent_id   INTEGER REFERENCES catalog.categories(id) ON DELETE SET NULL,
    name        VARCHAR(120) NOT NULL,
    slug        VARCHAR(120) NOT NULL UNIQUE,
    sort_order  INTEGER NOT NULL DEFAULT 0
);
COMMENT ON TABLE  catalog.categories            IS '商品分类（自引用，支持多级树）';
COMMENT ON COLUMN catalog.categories.parent_id  IS '父分类，根分类为 NULL';

CREATE TABLE catalog.products (
    id            BIGSERIAL PRIMARY KEY,
    sku_prefix    VARCHAR(20) NOT NULL UNIQUE,
    name          VARCHAR(255) NOT NULL,
    slug          VARCHAR(255) NOT NULL UNIQUE,
    description   TEXT,
    brand_id      INTEGER REFERENCES catalog.brands(id) ON DELETE SET NULL,
    category_id   INTEGER NOT NULL REFERENCES catalog.categories(id) ON DELETE RESTRICT,
    base_price    NUMERIC(10,2) NOT NULL,
    cost          NUMERIC(10,2),
    weight_grams  INTEGER,
    status        catalog.product_status NOT NULL DEFAULT 'draft',
    tags          TEXT[] NOT NULL DEFAULT '{}',
    attributes    JSONB,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT products_price_positive CHECK (base_price > 0),
    CONSTRAINT products_cost_nonneg    CHECK (cost IS NULL OR cost >= 0)
);
COMMENT ON TABLE  catalog.products            IS '商品主档';
COMMENT ON COLUMN catalog.products.sku_prefix IS 'SKU 前缀，变体在此基础上拓展';
COMMENT ON COLUMN catalog.products.tags       IS '商品标签数组（用于过滤/搜索）';
COMMENT ON COLUMN catalog.products.attributes IS '商品属性 JSONB，如颜色、尺寸枚举';

CREATE TABLE catalog.product_variants (
    id           BIGSERIAL PRIMARY KEY,
    product_id   BIGINT NOT NULL REFERENCES catalog.products(id) ON DELETE CASCADE,
    sku          VARCHAR(40) NOT NULL UNIQUE,
    color        VARCHAR(40),
    size         VARCHAR(20),
    price        NUMERIC(10,2),
    stock_qty    INTEGER NOT NULL DEFAULT 0,
    is_active    BOOLEAN NOT NULL DEFAULT TRUE,
    CONSTRAINT variants_stock_nonneg CHECK (stock_qty >= 0),
    CONSTRAINT variants_price_nonneg CHECK (price IS NULL OR price >= 0)
);
COMMENT ON TABLE  catalog.product_variants IS '商品 SKU 变体（颜色 / 尺寸）';
COMMENT ON COLUMN catalog.product_variants.price IS '可选 SKU 级价格，NULL 时回退到 product.base_price';

CREATE TABLE catalog.product_images (
    id          BIGSERIAL PRIMARY KEY,
    product_id  BIGINT NOT NULL REFERENCES catalog.products(id) ON DELETE CASCADE,
    url         TEXT NOT NULL,
    alt_text    VARCHAR(255),
    sort_order  INTEGER NOT NULL DEFAULT 0,
    is_primary  BOOLEAN NOT NULL DEFAULT FALSE
);
COMMENT ON TABLE catalog.product_images IS '商品图片，用于 PDP 展示';

-- =============================================================================
-- TABLES: sales
-- =============================================================================

CREATE TABLE sales.carts (
    id          BIGSERIAL PRIMARY KEY,
    customer_id BIGINT REFERENCES users.customers(id) ON DELETE SET NULL,
    session_id  UUID,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    abandoned   BOOLEAN NOT NULL DEFAULT FALSE
);
COMMENT ON TABLE sales.carts IS '购物车（含已放弃车）';

CREATE TABLE sales.cart_items (
    id          BIGSERIAL PRIMARY KEY,
    cart_id     BIGINT  NOT NULL REFERENCES sales.carts(id) ON DELETE CASCADE,
    variant_id  BIGINT  NOT NULL REFERENCES catalog.product_variants(id) ON DELETE CASCADE,
    quantity    INTEGER NOT NULL DEFAULT 1,
    unit_price  NUMERIC(10,2) NOT NULL,
    CONSTRAINT cart_items_qty_pos CHECK (quantity > 0)
);
COMMENT ON TABLE sales.cart_items IS '购物车明细';

CREATE TABLE sales.coupons (
    id           SERIAL PRIMARY KEY,
    code         VARCHAR(40) NOT NULL UNIQUE,
    description  TEXT,
    discount_pct NUMERIC(5,2),       -- mutually exclusive with discount_amt
    discount_amt NUMERIC(10,2),
    valid_from   DATE NOT NULL,
    valid_until  DATE NOT NULL,
    max_uses     INTEGER,
    used_count   INTEGER NOT NULL DEFAULT 0,
    is_active    BOOLEAN NOT NULL DEFAULT TRUE,
    CONSTRAINT coupons_one_discount CHECK (
        (discount_pct IS NOT NULL AND discount_amt IS NULL) OR
        (discount_pct IS NULL AND discount_amt IS NOT NULL)
    ),
    CONSTRAINT coupons_pct_range CHECK (discount_pct IS NULL OR (discount_pct > 0 AND discount_pct <= 100)),
    CONSTRAINT coupons_amt_pos   CHECK (discount_amt IS NULL OR discount_amt > 0),
    CONSTRAINT coupons_valid_window CHECK (valid_until >= valid_from)
);
COMMENT ON TABLE sales.coupons IS '优惠券，按百分比或定额二选一';

CREATE TABLE sales.orders (
    id              BIGSERIAL PRIMARY KEY,
    customer_id     BIGINT NOT NULL REFERENCES users.customers(id) ON DELETE RESTRICT,
    coupon_id       INTEGER REFERENCES sales.coupons(id) ON DELETE SET NULL,
    order_number    VARCHAR(40) NOT NULL UNIQUE,
    status          sales.order_status NOT NULL DEFAULT 'pending',
    subtotal        NUMERIC(12,2) NOT NULL,
    discount        NUMERIC(12,2) NOT NULL DEFAULT 0,
    tax             NUMERIC(12,2) NOT NULL DEFAULT 0,
    shipping_cost   NUMERIC(10,2) NOT NULL DEFAULT 0,
    total           NUMERIC(12,2) NOT NULL,
    currency        CHAR(3) NOT NULL DEFAULT 'USD',
    placed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    fulfilled_at    TIMESTAMPTZ,
    cancelled_at    TIMESTAMPTZ,
    notes           TEXT,
    CONSTRAINT orders_total_nonneg     CHECK (total >= 0),
    CONSTRAINT orders_subtotal_nonneg  CHECK (subtotal >= 0),
    CONSTRAINT orders_discount_nonneg  CHECK (discount >= 0)
);
COMMENT ON TABLE  sales.orders             IS '订单主表';
COMMENT ON COLUMN sales.orders.order_number IS '面向用户的订单号（UI 展示）';
COMMENT ON COLUMN sales.orders.currency    IS 'ISO-4217 三字母币种';

CREATE TABLE sales.order_items (
    id            BIGSERIAL PRIMARY KEY,
    order_id      BIGINT NOT NULL REFERENCES sales.orders(id) ON DELETE CASCADE,
    variant_id    BIGINT NOT NULL REFERENCES catalog.product_variants(id) ON DELETE RESTRICT,
    quantity      INTEGER NOT NULL,
    unit_price    NUMERIC(10,2) NOT NULL,
    line_total    NUMERIC(12,2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    CONSTRAINT order_items_qty_pos    CHECK (quantity > 0),
    CONSTRAINT order_items_price_pos  CHECK (unit_price >= 0)
);
COMMENT ON TABLE  sales.order_items           IS '订单明细';
COMMENT ON COLUMN sales.order_items.line_total IS 'quantity*unit_price，DB 派生';

CREATE TABLE sales.shipments (
    id            BIGSERIAL PRIMARY KEY,
    order_id      BIGINT NOT NULL REFERENCES sales.orders(id) ON DELETE CASCADE,
    carrier       VARCHAR(80),
    tracking_no   VARCHAR(100),
    status        sales.shipment_status NOT NULL DEFAULT 'pending',
    shipped_at    TIMESTAMPTZ,
    delivered_at  TIMESTAMPTZ,
    UNIQUE (carrier, tracking_no)
);
COMMENT ON TABLE sales.shipments IS '订单发货记录（一单可能多次发货）';

CREATE TABLE sales.coupon_redemptions (
    id           BIGSERIAL PRIMARY KEY,
    coupon_id    INTEGER NOT NULL REFERENCES sales.coupons(id) ON DELETE CASCADE,
    order_id     BIGINT  NOT NULL REFERENCES sales.orders(id)  ON DELETE CASCADE,
    redeemed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (coupon_id, order_id)
);
COMMENT ON TABLE sales.coupon_redemptions IS '优惠券核销记录';

-- =============================================================================
-- TABLES: billing
-- =============================================================================

CREATE TABLE billing.payment_methods (
    id          BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES users.customers(id) ON DELETE CASCADE,
    method_type VARCHAR(20) NOT NULL,        -- card, paypal, alipay, wechat, bank
    label       VARCHAR(60),
    last4       CHAR(4),
    expires_on  DATE,
    is_default  BOOLEAN NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pm_known_type CHECK (method_type IN ('card','paypal','alipay','wechat','bank'))
);
COMMENT ON TABLE  billing.payment_methods            IS '客户保存的支付方式';
COMMENT ON COLUMN billing.payment_methods.method_type IS '支付方式类型：card/paypal/alipay/wechat/bank';

CREATE TABLE billing.payments (
    id           BIGSERIAL PRIMARY KEY,
    order_id     BIGINT NOT NULL REFERENCES sales.orders(id) ON DELETE RESTRICT,
    method_id    BIGINT REFERENCES billing.payment_methods(id) ON DELETE SET NULL,
    amount       NUMERIC(12,2) NOT NULL,
    currency     CHAR(3) NOT NULL DEFAULT 'USD',
    status       billing.payment_status NOT NULL DEFAULT 'pending',
    gateway_ref  VARCHAR(120),
    captured_at  TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT payments_amount_pos CHECK (amount > 0)
);
COMMENT ON TABLE  billing.payments         IS '支付流水';
COMMENT ON COLUMN billing.payments.gateway_ref IS '支付网关返回的对账号';

CREATE TABLE billing.invoices (
    id          BIGSERIAL PRIMARY KEY,
    order_id    BIGINT NOT NULL UNIQUE REFERENCES sales.orders(id) ON DELETE CASCADE,
    invoice_no  VARCHAR(40) NOT NULL UNIQUE,
    issued_at   DATE NOT NULL,
    due_at      DATE,
    pdf_url     TEXT
);
COMMENT ON TABLE billing.invoices IS '发票（一单一票）';

CREATE TABLE billing.refunds (
    id           BIGSERIAL PRIMARY KEY,
    payment_id   BIGINT NOT NULL REFERENCES billing.payments(id) ON DELETE CASCADE,
    amount       NUMERIC(12,2) NOT NULL,
    reason       billing.refund_reason NOT NULL DEFAULT 'customer_request',
    notes        TEXT,
    refunded_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT refunds_amount_pos CHECK (amount > 0)
);
COMMENT ON TABLE billing.refunds IS '退款记录';

-- =============================================================================
-- INDEXES
-- =============================================================================

-- catalog
CREATE INDEX products_status_idx        ON catalog.products (status);
CREATE INDEX products_active_idx        ON catalog.products (id) WHERE status = 'active';
CREATE INDEX products_category_idx      ON catalog.products (category_id);
CREATE INDEX products_brand_idx         ON catalog.products (brand_id);
CREATE INDEX products_tags_gin_idx      ON catalog.products USING GIN (tags);
CREATE INDEX products_attrs_gin_idx     ON catalog.products USING GIN (attributes);
CREATE INDEX products_name_trgm_idx     ON catalog.products USING GIN (name gin_trgm_ops);

CREATE INDEX variants_product_idx       ON catalog.product_variants (product_id);
CREATE INDEX variants_in_stock_idx      ON catalog.product_variants (product_id) WHERE is_active = TRUE AND stock_qty > 0;

-- users
CREATE INDEX customers_loyalty_idx      ON users.customers (loyalty_tier);
CREATE INDEX customers_email_lower_idx  ON users.customers (lower(email::text));
CREATE INDEX customers_metadata_gin_idx ON users.customers USING GIN (metadata);

CREATE INDEX addresses_customer_idx     ON users.addresses (customer_id);

CREATE INDEX logins_customer_idx        ON users.logins (customer_id);
CREATE INDEX logins_failed_idx          ON users.logins (occurred_at DESC) WHERE success = FALSE;

-- sales
CREATE INDEX orders_customer_idx        ON sales.orders (customer_id);
CREATE INDEX orders_status_idx          ON sales.orders (status);
CREATE INDEX orders_placed_at_idx       ON sales.orders (placed_at DESC);
CREATE INDEX orders_open_idx            ON sales.orders (placed_at DESC) WHERE status IN ('pending','paid','shipped');
CREATE INDEX orders_search_idx          ON sales.orders USING GIN (status, placed_at);

CREATE INDEX order_items_order_idx      ON sales.order_items (order_id);
CREATE INDEX order_items_variant_idx    ON sales.order_items (variant_id);

CREATE INDEX shipments_order_idx        ON sales.shipments (order_id);

-- billing
CREATE INDEX payments_order_idx         ON billing.payments (order_id);
CREATE INDEX payments_status_idx        ON billing.payments (status);
CREATE INDEX refunds_payment_idx        ON billing.refunds (payment_id);

-- =============================================================================
-- VIEWS
-- =============================================================================

CREATE VIEW catalog.in_stock_products AS
SELECT  p.id              AS product_id,
        p.name,
        p.slug,
        p.brand_id,
        p.category_id,
        p.base_price,
        SUM(v.stock_qty)   AS total_stock,
        COUNT(v.id)        AS active_variant_count
FROM    catalog.products p
JOIN    catalog.product_variants v ON v.product_id = p.id
WHERE   p.status = 'active'
  AND   v.is_active = TRUE
GROUP BY p.id, p.name, p.slug, p.brand_id, p.category_id, p.base_price
HAVING  SUM(v.stock_qty) > 0;

COMMENT ON VIEW catalog.in_stock_products IS '在售且有库存的商品总览';

-- (Materialized views are populated below after data load)

-- =============================================================================
-- DATA LOAD
-- =============================================================================

-- ----- BRANDS -----
INSERT INTO catalog.brands (name, slug, country, founded_year) VALUES
  ('Acme',       'acme',        'US', 1948),
  ('Globex',     'globex',      'US', 1972),
  ('Initech',    'initech',     'US', 1996),
  ('Umbrella',   'umbrella',    'JP', 1968),
  ('Soylent',    'soylent',     'US', 1972),
  ('Hooli',      'hooli',       'US', 2009),
  ('Pied Piper', 'pied-piper',  'US', 2014),
  ('Stark',      'stark',       'US', 1939),
  ('Wayne',      'wayne',       'US', 1882),
  ('TyrellCo',   'tyrellco',    'US', 2019);

-- ----- CATEGORIES -----
-- root categories
INSERT INTO catalog.categories (parent_id, name, slug, sort_order) VALUES
  (NULL, 'Apparel',       'apparel',       1),
  (NULL, 'Electronics',   'electronics',   2),
  (NULL, 'Home & Kitchen','home-kitchen',  3),
  (NULL, 'Beauty',        'beauty',        4),
  (NULL, 'Books',         'books',         5);
-- subs of Apparel
INSERT INTO catalog.categories (parent_id, name, slug, sort_order) VALUES
  (1, 'Mens Tops',    'apparel-mens-tops',    1),
  (1, 'Womens Tops',  'apparel-womens-tops',  2),
  (1, 'Footwear',     'apparel-footwear',     3),
  (1, 'Accessories',  'apparel-accessories',  4);
-- subs of Electronics
INSERT INTO catalog.categories (parent_id, name, slug, sort_order) VALUES
  (2, 'Laptops',  'electronics-laptops',  1),
  (2, 'Phones',   'electronics-phones',   2),
  (2, 'Audio',    'electronics-audio',    3),
  (2, 'Cameras',  'electronics-cameras',  4);
-- subs of Home & Kitchen
INSERT INTO catalog.categories (parent_id, name, slug, sort_order) VALUES
  (3, 'Cookware',     'home-cookware',     1),
  (3, 'Small Appliances','home-small-appl',2),
  (3, 'Furniture',    'home-furniture',    3);

-- ----- PRODUCTS (75 active + 5 draft + 5 discontinued) -----
INSERT INTO catalog.products (sku_prefix, name, slug, description, brand_id, category_id, base_price, cost, weight_grams, status, tags, attributes)
SELECT
  'SKU-' || LPAD(g::text, 4, '0'),
  CASE (g % 5)
    WHEN 0 THEN 'Classic Tee ' || g
    WHEN 1 THEN 'Wireless Headphones Model ' || g
    WHEN 2 THEN 'Stainless Steel Pan ' || g || '"'
    WHEN 3 THEN 'Smartphone Pro ' || g
    ELSE 'Office Chair Ergonomic ' || g
  END,
  'product-' || g,
  'High quality product number ' || g || '. Generated for testing purposes.',
  ((g - 1) % 10) + 1,
  ((g - 1) % 16) + 1,
  ROUND((10 + (g % 500))::numeric, 2),
  ROUND((10 + (g % 500))::numeric * 0.6, 2),
  100 + (g % 5000),
  CASE
    WHEN g <= 75 THEN 'active'::catalog.product_status
    WHEN g <= 80 THEN 'draft'::catalog.product_status
    ELSE 'discontinued'::catalog.product_status
  END,
  CASE (g % 4)
    WHEN 0 THEN ARRAY['new','seasonal']
    WHEN 1 THEN ARRAY['popular','best-seller']
    WHEN 2 THEN ARRAY['eco','sustainable']
    ELSE ARRAY['premium','limited']
  END,
  jsonb_build_object(
    'season', CASE (g % 4) WHEN 0 THEN 'spring' WHEN 1 THEN 'summer' WHEN 2 THEN 'fall' ELSE 'winter' END,
    'gender', CASE (g % 3) WHEN 0 THEN 'unisex' WHEN 1 THEN 'male' ELSE 'female' END,
    'rating', round((random() * 5)::numeric, 1)
  )
FROM generate_series(1, 85) g;

-- ----- PRODUCT VARIANTS (≈3 per product → 255 variants) -----
INSERT INTO catalog.product_variants (product_id, sku, color, size, price, stock_qty, is_active)
SELECT
  p.id,
  p.sku_prefix || '-' || (n || ''),
  (ARRAY['black','white','red','blue','green'])[((p.id + n) % 5) + 1],
  (ARRAY['S','M','L','XL'])[(n % 4) + 1],
  CASE WHEN n = 0 THEN NULL ELSE p.base_price + (n * 5) END,
  CASE
    WHEN p.status = 'active' THEN floor(random() * 100)::int
    WHEN p.status = 'draft' THEN 0
    ELSE 0
  END,
  p.status = 'active'
FROM catalog.products p
CROSS JOIN generate_series(0, 2) n
WHERE p.status IN ('active','draft','discontinued');

-- ----- PRODUCT IMAGES -----
INSERT INTO catalog.product_images (product_id, url, alt_text, sort_order, is_primary)
SELECT
  p.id,
  'https://cdn.example.com/products/' || p.slug || '-' || n || '.jpg',
  p.name || ' image ' || n,
  n,
  (n = 0)
FROM catalog.products p
CROSS JOIN generate_series(0, 1) n;

-- ----- CUSTOMERS (200) -----
INSERT INTO users.customers (email, full_name, phone, date_of_birth, loyalty_tier, lifetime_value, is_active, marketing_opt_in, metadata, created_at)
SELECT
  'customer' || g || '@example.com',
  (ARRAY['Alice','Bob','Carol','Dave','Eve','Frank','Grace','Heidi','Ivan','Judy','Mallory','Niaj','Olivia','Peggy','Sybil','Trent','Victor','Walter','Xavier','Yvonne'])[((g - 1) % 20) + 1]
  || ' ' ||
  (ARRAY['Smith','Jones','Wong','Garcia','Patel','Müller','Suzuki','Chen','Kim','Brown'])[((g - 1) % 10) + 1],
  '+1-555-' || LPAD(g::text, 4, '0'),
  DATE '1980-01-01' + ((g % 14000) || ' days')::interval,
  CASE
    WHEN g % 50 = 0 THEN 'platinum'::users.loyalty_tier
    WHEN g % 10 = 0 THEN 'gold'::users.loyalty_tier
    WHEN g %  4 = 0 THEN 'silver'::users.loyalty_tier
    ELSE 'bronze'::users.loyalty_tier
  END,
  ROUND((random() * 5000)::numeric, 2),
  (g % 13 <> 0),
  (g % 3 = 0),
  jsonb_build_object(
    'preferred_lang', CASE (g % 3) WHEN 0 THEN 'en' WHEN 1 THEN 'zh' ELSE 'es' END,
    'segment',        CASE (g % 4) WHEN 0 THEN 'new' WHEN 1 THEN 'returning' WHEN 2 THEN 'vip' ELSE 'churn-risk' END
  ),
  NOW() - ((g % 720) || ' days')::interval
FROM generate_series(1, 200) g;

-- ----- ADDRESSES (1-2 per customer) -----
INSERT INTO users.addresses (customer_id, address_type, is_default, address)
SELECT
  c.id,
  'shipping'::users.address_type,
  TRUE,
  ROW(
    (100 + c.id) || ' Main St',
    'Apt ' || (c.id % 50),
    (ARRAY['New York','Los Angeles','Chicago','Houston','Phoenix','Philadelphia','San Antonio','San Diego','Dallas','San Jose'])[((c.id - 1) % 10) + 1],
    (ARRAY['NY','CA','IL','TX','AZ','PA','TX','CA','TX','CA'])[((c.id - 1) % 10) + 1],
    LPAD(((c.id * 73) % 100000)::text, 5, '0'),
    'US'
  )::users.postal_address
FROM users.customers c;

INSERT INTO users.addresses (customer_id, address_type, is_default, address)
SELECT
  c.id,
  'billing'::users.address_type,
  FALSE,
  ROW(
    (200 + c.id) || ' Pine Ave',
    NULL,
    'Boston',
    'MA',
    LPAD(((c.id * 11) % 100000)::text, 5, '0'),
    'US'
  )::users.postal_address
FROM users.customers c
WHERE c.id % 4 = 0;

-- ----- COUPONS -----
INSERT INTO sales.coupons (code, description, discount_pct, discount_amt, valid_from, valid_until, max_uses, used_count, is_active) VALUES
  ('WELCOME10',  '新用户首单 9 折', 10,    NULL, DATE '2025-01-01', DATE '2026-12-31', 5000, 412, TRUE),
  ('SUMMER20',   '夏季促销 8 折',    20,    NULL, DATE '2025-06-01', DATE '2025-09-30', 2000, 1873, FALSE),
  ('VIP25',      '会员专享 7.5 折',  25,    NULL, DATE '2025-01-01', DATE '2026-12-31', 1000, 220, TRUE),
  ('FREESHIP',   '免运费 5 美金',  NULL,    5.00, DATE '2025-01-01', DATE '2026-12-31', 100000, 9120, TRUE),
  ('FLAT15',     '满减 15 美金',    NULL,   15.00, DATE '2025-11-01', DATE '2025-12-31', 5000, 200, FALSE),
  ('CLEARANCE',  '清仓单品额外 5 折', 50,  NULL, DATE '2025-09-01', DATE '2025-10-31', 500, 480, FALSE);

-- ----- ORDERS (≈800) -----
WITH src AS (
  SELECT
    g                                      AS seq,
    1 + ((g - 1) % 200)                    AS customer_id,
    'PO-' || LPAD(g::text, 6, '0')         AS order_number,
    NOW() - ((g % 365) || ' days')::interval AS placed_at,
    CASE
      WHEN g % 30 = 0 THEN 'cancelled'::sales.order_status
      WHEN g % 13 = 0 THEN 'pending'::sales.order_status
      WHEN g % 7 = 0  THEN 'paid'::sales.order_status
      WHEN g % 5 = 0  THEN 'shipped'::sales.order_status
      ELSE 'delivered'::sales.order_status
    END                                    AS status,
    CASE
      WHEN g % 11 = 0 THEN 1 -- WELCOME10
      WHEN g % 17 = 0 THEN 4 -- FREESHIP
      WHEN g % 23 = 0 THEN 3 -- VIP25
      ELSE NULL
    END                                    AS coupon_id,
    ROUND((20 + (g % 800))::numeric, 2)    AS subtotal
  FROM generate_series(1, 800) g
)
INSERT INTO sales.orders (customer_id, coupon_id, order_number, status, subtotal, discount, tax, shipping_cost, total, currency, placed_at, fulfilled_at, cancelled_at)
SELECT
  customer_id,
  coupon_id,
  order_number,
  status,
  subtotal,
  CASE WHEN coupon_id IS NULL THEN 0 ELSE ROUND(subtotal * 0.10, 2) END        AS discount,
  ROUND(subtotal * 0.08, 2)                                                    AS tax,
  CASE WHEN coupon_id = 4 THEN 0 ELSE 5.00 END                                 AS shipping_cost,
  ROUND(subtotal + (subtotal * 0.08) + 5.00 - COALESCE(subtotal * 0.10, 0), 2) AS total,
  'USD',
  placed_at,
  CASE WHEN status IN ('shipped','delivered') THEN placed_at + INTERVAL '2 days' END AS fulfilled_at,
  CASE WHEN status = 'cancelled' THEN placed_at + INTERVAL '6 hours' END AS cancelled_at
FROM src;

-- ----- ORDER ITEMS (≈3 per order → 2400) -----
INSERT INTO sales.order_items (order_id, variant_id, quantity, unit_price)
SELECT
  o.id,
  v.id,
  1 + ((o.id + n) % 3),
  v.price
FROM sales.orders o
CROSS JOIN LATERAL (
  SELECT id, price
  FROM   catalog.product_variants
  WHERE  is_active = TRUE
    AND  price IS NOT NULL
  ORDER BY (id * 31 + o.id * 17) % 257
  LIMIT 3
) v
CROSS JOIN generate_series(1, 1) n;

-- ----- COUPON_REDEMPTIONS -----
INSERT INTO sales.coupon_redemptions (coupon_id, order_id)
SELECT coupon_id, id FROM sales.orders WHERE coupon_id IS NOT NULL;

-- ----- SHIPMENTS -----
INSERT INTO sales.shipments (order_id, carrier, tracking_no, status, shipped_at, delivered_at)
SELECT
  o.id,
  (ARRAY['UPS','FedEx','USPS','DHL'])[(o.id % 4) + 1],
  'TRK-' || LPAD(o.id::text, 8, '0'),
  CASE
    WHEN o.status = 'delivered' THEN 'delivered'::sales.shipment_status
    WHEN o.status = 'shipped'   THEN 'in_transit'::sales.shipment_status
    ELSE 'pending'::sales.shipment_status
  END,
  o.fulfilled_at,
  CASE WHEN o.status = 'delivered' THEN o.fulfilled_at + INTERVAL '3 days' END
FROM sales.orders o
WHERE o.status IN ('shipped','delivered');

-- ----- PAYMENT METHODS (≈1.5 per customer) -----
INSERT INTO billing.payment_methods (customer_id, method_type, label, last4, expires_on, is_default)
SELECT
  c.id,
  (ARRAY['card','paypal','alipay','wechat'])[(c.id % 4) + 1],
  'Default ' || (c.id % 4),
  LPAD(((c.id * 13) % 10000)::text, 4, '0'),
  DATE '2027-12-31',
  TRUE
FROM users.customers c;

INSERT INTO billing.payment_methods (customer_id, method_type, label, last4, expires_on, is_default)
SELECT
  c.id,
  'card',
  'Backup',
  LPAD(((c.id * 71) % 10000)::text, 4, '0'),
  DATE '2026-06-30',
  FALSE
FROM users.customers c
WHERE c.id % 3 = 0;

-- ----- PAYMENTS (1 per non-cancelled, non-pending order) -----
INSERT INTO billing.payments (order_id, method_id, amount, currency, status, gateway_ref, captured_at, created_at)
SELECT
  o.id,
  pm.id,
  o.total,
  o.currency,
  CASE
    WHEN o.status IN ('paid','shipped','delivered') THEN 'captured'::billing.payment_status
    ELSE 'pending'::billing.payment_status
  END,
  'gw_' || md5(o.id::text)::varchar,
  CASE WHEN o.status IN ('paid','shipped','delivered') THEN o.placed_at + INTERVAL '5 minutes' END,
  o.placed_at
FROM sales.orders o
JOIN LATERAL (
  SELECT id FROM billing.payment_methods
  WHERE customer_id = o.customer_id AND is_default = TRUE
  LIMIT 1
) pm ON TRUE
WHERE o.status <> 'cancelled';

-- ----- INVOICES (for delivered/shipped orders) -----
INSERT INTO billing.invoices (order_id, invoice_no, issued_at, due_at, pdf_url)
SELECT
  o.id,
  'INV-' || LPAD(o.id::text, 6, '0'),
  o.placed_at::date,
  (o.placed_at + INTERVAL '14 days')::date,
  'https://billing.example.com/invoices/' || o.id || '.pdf'
FROM sales.orders o
WHERE o.status IN ('paid','shipped','delivered');

-- ----- REFUNDS (a few) -----
INSERT INTO billing.refunds (payment_id, amount, reason, notes, refunded_at)
SELECT
  p.id,
  ROUND(p.amount * 0.5, 2),
  CASE (p.id % 4)
    WHEN 0 THEN 'customer_request'::billing.refund_reason
    WHEN 1 THEN 'damaged'::billing.refund_reason
    WHEN 2 THEN 'wrong_item'::billing.refund_reason
    ELSE 'other'::billing.refund_reason
  END,
  'Auto-generated refund for testing',
  p.created_at + INTERVAL '5 days'
FROM billing.payments p
WHERE p.id % 25 = 0;

-- ----- CARTS (a handful, including abandoned) -----
INSERT INTO sales.carts (customer_id, session_id, abandoned, created_at, updated_at)
SELECT
  CASE WHEN g % 5 = 0 THEN NULL ELSE 1 + (g % 200) END,
  gen_random_uuid(),
  (g % 4 = 0),
  NOW() - ((g % 30) || ' days')::interval,
  NOW() - ((g % 30) || ' days')::interval
FROM generate_series(1, 60) g;

INSERT INTO sales.cart_items (cart_id, variant_id, quantity, unit_price)
SELECT
  c.id,
  v.id,
  1 + (c.id % 3),
  v.price
FROM sales.carts c
CROSS JOIN LATERAL (
  SELECT id, price
  FROM   catalog.product_variants
  WHERE  is_active = TRUE AND price IS NOT NULL
  ORDER  BY (id * 17 + c.id * 11) % 511
  LIMIT  2
) v;

-- ----- LOGINS -----
INSERT INTO users.logins (customer_id, ip, user_agent, success, occurred_at)
SELECT
  CASE WHEN g % 13 = 0 THEN NULL ELSE 1 + (g % 200) END,
  ('192.0.2.' || ((g % 250) + 1))::inet,
  CASE (g % 3)
    WHEN 0 THEN 'Mozilla/5.0 (Windows NT 10.0)'
    WHEN 1 THEN 'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0)'
    ELSE 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0)'
  END,
  (g % 11 <> 0),
  NOW() - ((g * 17) % 86400 || ' seconds')::interval - ((g % 30) || ' days')::interval
FROM generate_series(1, 1500) g;

-- =============================================================================
-- MATERIALIZED VIEWS (created after data load so they have content)
-- =============================================================================

CREATE MATERIALIZED VIEW sales.monthly_revenue AS
SELECT
    date_trunc('month', placed_at)::date AS month,
    currency,
    COUNT(*)                              AS order_count,
    SUM(subtotal)                         AS subtotal_sum,
    SUM(discount)                         AS discount_sum,
    SUM(total)                            AS revenue
FROM sales.orders
WHERE status IN ('paid','shipped','delivered')
GROUP BY 1, 2
ORDER BY 1 DESC;

COMMENT ON MATERIALIZED VIEW sales.monthly_revenue IS '每月已确认收入汇总（含税不含退款）';

CREATE UNIQUE INDEX monthly_revenue_pk ON sales.monthly_revenue (month, currency);

CREATE MATERIALIZED VIEW sales.top_customers AS
SELECT
    o.customer_id,
    c.full_name,
    c.loyalty_tier,
    COUNT(DISTINCT o.id) AS order_count,
    SUM(o.total)         AS total_spent,
    MAX(o.placed_at)     AS last_order_at
FROM sales.orders o
JOIN users.customers c ON c.id = o.customer_id
WHERE o.status IN ('paid','shipped','delivered')
GROUP BY o.customer_id, c.full_name, c.loyalty_tier
ORDER BY total_spent DESC;

COMMENT ON MATERIALIZED VIEW sales.top_customers IS '按消费金额排序的客户榜单';

CREATE UNIQUE INDEX top_customers_pk ON sales.top_customers (customer_id);
CREATE INDEX top_customers_total_idx ON sales.top_customers (total_spent DESC);

-- =============================================================================
-- POST-LOAD STATS
-- =============================================================================

REFRESH MATERIALIZED VIEW sales.monthly_revenue;
REFRESH MATERIALIZED VIEW sales.top_customers;
ANALYZE;
