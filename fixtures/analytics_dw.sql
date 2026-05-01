-- =============================================================================
-- Database: analytics_dw (LARGE)
-- Purpose:  Star/snowflake-schema data warehouse — large schema
-- Scale:    5 schemas, 67 tables, 4 views (3 materialized), 11 enums,
--           2 composite types, ~80 indexes, ~150,000+ rows total
-- Use case: Tests pg-mcp's schema retrieval path (table count > 50
--           triggers SCHEMA_MAX_TABLES_FOR_FULL_CONTEXT)
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS btree_gin;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

DROP SCHEMA IF EXISTS reporting CASCADE;
DROP SCHEMA IF EXISTS audit     CASCADE;
DROP SCHEMA IF EXISTS staging   CASCADE;
DROP SCHEMA IF EXISTS fact      CASCADE;
DROP SCHEMA IF EXISTS dim       CASCADE;

CREATE SCHEMA dim;       COMMENT ON SCHEMA dim       IS '维度表：缓慢变化维 / 静态参考表';
CREATE SCHEMA fact;      COMMENT ON SCHEMA fact      IS '事实表：交易/事件级度量';
CREATE SCHEMA staging;   COMMENT ON SCHEMA staging   IS '原始数据落地区，ETL 入口';
CREATE SCHEMA audit;     COMMENT ON SCHEMA audit     IS '审计日志：登录、ETL、数据访问';
CREATE SCHEMA reporting; COMMENT ON SCHEMA reporting IS '面向报表的预聚合 / 物化视图';

-- =============================================================================
-- TYPES
-- =============================================================================
CREATE TYPE dim.gender_t           AS ENUM ('male','female','nonbinary','unknown');
CREATE TYPE dim.customer_segment_t AS ENUM ('new','active','at_risk','churned','vip');
CREATE TYPE dim.channel_t          AS ENUM ('web','mobile_ios','mobile_android','retail','partner','phone','email');
CREATE TYPE fact.order_status_t    AS ENUM ('pending','paid','shipped','delivered','cancelled','refunded');
CREATE TYPE fact.return_reason_t   AS ENUM ('damaged','wrong_item','customer_changed_mind','too_late','quality','other');
CREATE TYPE fact.event_type_t      AS ENUM ('page_view','add_to_cart','remove_from_cart','checkout','purchase','signup','login','search','share');
CREATE TYPE fact.payment_status_t  AS ENUM ('pending','authorized','captured','failed','refunded');
CREATE TYPE fact.shipment_status_t AS ENUM ('label_created','in_transit','out_for_delivery','delivered','exception','returned');
CREATE TYPE staging.load_status_t  AS ENUM ('queued','processing','done','failed');
CREATE TYPE audit.severity_t       AS ENUM ('debug','info','warn','error','critical');
CREATE TYPE reporting.kpi_trend_t  AS ENUM ('up','flat','down');

CREATE TYPE dim.address_t AS (
    line1 VARCHAR(120), line2 VARCHAR(120), city VARCHAR(80),
    state VARCHAR(80),  postal_code VARCHAR(20), country CHAR(2)
);
CREATE TYPE fact.money_t AS (amount NUMERIC(14,2), currency CHAR(3));

COMMENT ON TYPE dim.address_t IS '通用地址复合类型';
COMMENT ON TYPE fact.money_t  IS '金额 + 币种复合类型';

-- =============================================================================
-- DIM TABLES (22)
-- =============================================================================

CREATE TABLE dim.dim_date (
    date_key       INTEGER PRIMARY KEY,        -- YYYYMMDD
    full_date      DATE NOT NULL UNIQUE,
    year           SMALLINT NOT NULL,
    quarter        SMALLINT NOT NULL,
    month          SMALLINT NOT NULL,
    month_name     VARCHAR(12) NOT NULL,
    day_of_month   SMALLINT NOT NULL,
    day_of_week    SMALLINT NOT NULL,
    day_name       VARCHAR(12) NOT NULL,
    week_of_year   SMALLINT NOT NULL,
    is_weekend     BOOLEAN NOT NULL,
    is_holiday     BOOLEAN NOT NULL DEFAULT FALSE,
    fiscal_year    SMALLINT NOT NULL,
    fiscal_quarter SMALLINT NOT NULL
);
COMMENT ON TABLE dim.dim_date IS '日期维（含财年、节假日标记）';

CREATE TABLE dim.dim_time_of_day (
    time_key  INTEGER PRIMARY KEY,             -- HHMM as int (0-2359)
    hour      SMALLINT NOT NULL,
    minute    SMALLINT NOT NULL,
    am_pm     CHAR(2)  NOT NULL,
    daypart   VARCHAR(20) NOT NULL             -- morning/afternoon/evening/night
);
COMMENT ON TABLE dim.dim_time_of_day IS '一天内的时间维（HHMM 粒度）';

CREATE TABLE dim.dim_country (
    country_key  SERIAL PRIMARY KEY,
    iso2         CHAR(2) NOT NULL UNIQUE,
    iso3         CHAR(3) NOT NULL UNIQUE,
    name         VARCHAR(80) NOT NULL,
    region       VARCHAR(40),
    sub_region   VARCHAR(60),
    currency     CHAR(3),
    is_eu        BOOLEAN NOT NULL DEFAULT FALSE
);
COMMENT ON TABLE dim.dim_country IS 'ISO-3166 国家维';

CREATE TABLE dim.dim_region (
    region_key   SERIAL PRIMARY KEY,
    country_key  INTEGER NOT NULL REFERENCES dim.dim_country(country_key),
    code         VARCHAR(10) NOT NULL,
    name         VARCHAR(80) NOT NULL,
    UNIQUE (country_key, code)
);

CREATE TABLE dim.dim_city (
    city_key     SERIAL PRIMARY KEY,
    region_key   INTEGER NOT NULL REFERENCES dim.dim_region(region_key),
    name         VARCHAR(120) NOT NULL,
    population   INTEGER,
    timezone     VARCHAR(40)
);

CREATE TABLE dim.dim_currency (
    currency     CHAR(3) PRIMARY KEY,
    name         VARCHAR(60) NOT NULL,
    symbol       VARCHAR(8),
    decimals     SMALLINT NOT NULL DEFAULT 2
);

CREATE TABLE dim.dim_customer_segment (
    segment_key   SERIAL PRIMARY KEY,
    code          dim.customer_segment_t NOT NULL UNIQUE,
    description   TEXT
);

CREATE TABLE dim.dim_customer (
    customer_key   BIGSERIAL PRIMARY KEY,
    customer_bk    VARCHAR(40) NOT NULL UNIQUE,    -- business key
    email          VARCHAR(255) NOT NULL,
    full_name      VARCHAR(120),
    gender         dim.gender_t NOT NULL DEFAULT 'unknown',
    birth_year     SMALLINT,
    segment_key    INTEGER REFERENCES dim.dim_customer_segment(segment_key),
    home_country_key INTEGER REFERENCES dim.dim_country(country_key),
    address        dim.address_t,
    valid_from     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    valid_to       TIMESTAMPTZ,                    -- SCD2: NULL means current
    is_current     BOOLEAN NOT NULL DEFAULT TRUE,
    metadata       JSONB
);
COMMENT ON TABLE dim.dim_customer IS '客户维（SCD-2，valid_from/valid_to 表示历史版本）';

CREATE TABLE dim.dim_brand (
    brand_key   SERIAL PRIMARY KEY,
    name        VARCHAR(80) NOT NULL UNIQUE,
    country_key INTEGER REFERENCES dim.dim_country(country_key),
    founded     SMALLINT
);

CREATE TABLE dim.dim_supplier (
    supplier_key BIGSERIAL PRIMARY KEY,
    supplier_bk  VARCHAR(40) NOT NULL UNIQUE,
    name         VARCHAR(120) NOT NULL,
    country_key  INTEGER REFERENCES dim.dim_country(country_key),
    rating       NUMERIC(3,2),
    is_active    BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE dim.dim_product_category (
    category_key SERIAL PRIMARY KEY,
    parent_key   INTEGER REFERENCES dim.dim_product_category(category_key),
    name         VARCHAR(120) NOT NULL,
    path         VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE dim.dim_product (
    product_key   BIGSERIAL PRIMARY KEY,
    product_bk    VARCHAR(40) NOT NULL UNIQUE,
    sku           VARCHAR(40) NOT NULL UNIQUE,
    name          VARCHAR(255) NOT NULL,
    category_key  INTEGER NOT NULL REFERENCES dim.dim_product_category(category_key),
    brand_key     INTEGER REFERENCES dim.dim_brand(brand_key),
    supplier_key  BIGINT  REFERENCES dim.dim_supplier(supplier_key),
    list_price    NUMERIC(10,2) NOT NULL,
    cost          NUMERIC(10,2),
    weight_kg     NUMERIC(8,3),
    is_active     BOOLEAN NOT NULL DEFAULT TRUE,
    attributes    JSONB,
    valid_from    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    valid_to      TIMESTAMPTZ,
    is_current    BOOLEAN NOT NULL DEFAULT TRUE
);
COMMENT ON TABLE dim.dim_product IS '商品维（SCD-2）';

CREATE TABLE dim.dim_employee_role (
    role_key   SERIAL PRIMARY KEY,
    name       VARCHAR(60) NOT NULL UNIQUE,
    department VARCHAR(60)
);

CREATE TABLE dim.dim_employee (
    employee_key BIGSERIAL PRIMARY KEY,
    employee_bk  VARCHAR(40) NOT NULL UNIQUE,
    full_name    VARCHAR(120) NOT NULL,
    email        VARCHAR(255) NOT NULL UNIQUE,
    role_key     INTEGER REFERENCES dim.dim_employee_role(role_key),
    hire_date    DATE,
    is_active    BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE dim.dim_store (
    store_key    SERIAL PRIMARY KEY,
    store_bk     VARCHAR(40) NOT NULL UNIQUE,
    name         VARCHAR(120) NOT NULL,
    city_key     INTEGER REFERENCES dim.dim_city(city_key),
    opened_on    DATE,
    is_active    BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE dim.dim_warehouse (
    warehouse_key SERIAL PRIMARY KEY,
    warehouse_bk  VARCHAR(40) NOT NULL UNIQUE,
    name          VARCHAR(120) NOT NULL,
    city_key      INTEGER REFERENCES dim.dim_city(city_key),
    capacity_m3   INTEGER
);

CREATE TABLE dim.dim_channel (
    channel_key SERIAL PRIMARY KEY,
    code        dim.channel_t NOT NULL UNIQUE,
    description TEXT
);

CREATE TABLE dim.dim_campaign (
    campaign_key BIGSERIAL PRIMARY KEY,
    campaign_bk  VARCHAR(40) NOT NULL UNIQUE,
    name         VARCHAR(160) NOT NULL,
    channel_key  INTEGER REFERENCES dim.dim_channel(channel_key),
    started_on   DATE,
    ended_on     DATE,
    budget       NUMERIC(12,2)
);

CREATE TABLE dim.dim_payment_method (
    pm_key   SERIAL PRIMARY KEY,
    name     VARCHAR(40) NOT NULL UNIQUE,
    is_card  BOOLEAN NOT NULL
);

CREATE TABLE dim.dim_device (
    device_key SERIAL PRIMARY KEY,
    type       VARCHAR(20) NOT NULL,             -- desktop/mobile/tablet/tv
    vendor     VARCHAR(40),
    model      VARCHAR(80),
    UNIQUE (type, vendor, model)
);

CREATE TABLE dim.dim_browser (
    browser_key SERIAL PRIMARY KEY,
    name        VARCHAR(40) NOT NULL,
    major_version SMALLINT,
    UNIQUE (name, major_version)
);

CREATE TABLE dim.dim_os (
    os_key      SERIAL PRIMARY KEY,
    family      VARCHAR(20) NOT NULL,            -- Windows/macOS/iOS/Android/Linux
    version     VARCHAR(20),
    UNIQUE (family, version)
);

-- =============================================================================
-- FACT TABLES (15)
-- =============================================================================

CREATE TABLE fact.fact_sales (
    sale_id          BIGSERIAL PRIMARY KEY,
    order_bk         VARCHAR(40) NOT NULL UNIQUE,
    date_key         INTEGER NOT NULL REFERENCES dim.dim_date(date_key),
    time_key         INTEGER REFERENCES dim.dim_time_of_day(time_key),
    customer_key     BIGINT  NOT NULL REFERENCES dim.dim_customer(customer_key),
    channel_key      INTEGER REFERENCES dim.dim_channel(channel_key),
    store_key        INTEGER REFERENCES dim.dim_store(store_key),
    employee_key     BIGINT  REFERENCES dim.dim_employee(employee_key),
    campaign_key     BIGINT  REFERENCES dim.dim_campaign(campaign_key),
    pm_key           INTEGER REFERENCES dim.dim_payment_method(pm_key),
    currency         CHAR(3) NOT NULL REFERENCES dim.dim_currency(currency),
    status           fact.order_status_t NOT NULL,
    gross_amount     NUMERIC(14,2) NOT NULL,
    discount_amount  NUMERIC(14,2) NOT NULL DEFAULT 0,
    tax_amount       NUMERIC(14,2) NOT NULL DEFAULT 0,
    shipping_amount  NUMERIC(14,2) NOT NULL DEFAULT 0,
    net_amount       NUMERIC(14,2) NOT NULL,
    item_count       INTEGER NOT NULL,
    placed_at        TIMESTAMPTZ NOT NULL,
    delivered_at     TIMESTAMPTZ,
    CHECK (gross_amount   >= 0),
    CHECK (net_amount     >= 0),
    CHECK (item_count     >= 1)
);
COMMENT ON TABLE fact.fact_sales IS '订单事实（粒度=订单）';

CREATE TABLE fact.fact_sales_items (
    sales_item_id   BIGSERIAL PRIMARY KEY,
    sale_id         BIGINT NOT NULL REFERENCES fact.fact_sales(sale_id) ON DELETE CASCADE,
    product_key     BIGINT NOT NULL REFERENCES dim.dim_product(product_key),
    quantity        INTEGER NOT NULL,
    unit_price      NUMERIC(10,2) NOT NULL,
    line_amount     NUMERIC(14,2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    margin          NUMERIC(14,2),
    CHECK (quantity > 0)
);
COMMENT ON TABLE fact.fact_sales_items IS '订单行项目事实（粒度=订单+商品）';

CREATE TABLE fact.fact_returns (
    return_id        BIGSERIAL PRIMARY KEY,
    sale_id          BIGINT NOT NULL REFERENCES fact.fact_sales(sale_id) ON DELETE CASCADE,
    sales_item_id    BIGINT REFERENCES fact.fact_sales_items(sales_item_id),
    return_date_key  INTEGER NOT NULL REFERENCES dim.dim_date(date_key),
    quantity         INTEGER NOT NULL,
    refund_amount    NUMERIC(14,2) NOT NULL,
    reason           fact.return_reason_t NOT NULL,
    notes            TEXT,
    CHECK (quantity > 0)
);

CREATE TABLE fact.fact_payments (
    payment_id     BIGSERIAL PRIMARY KEY,
    sale_id        BIGINT NOT NULL REFERENCES fact.fact_sales(sale_id) ON DELETE CASCADE,
    pm_key         INTEGER REFERENCES dim.dim_payment_method(pm_key),
    amount         NUMERIC(14,2) NOT NULL,
    currency       CHAR(3) NOT NULL,
    status         fact.payment_status_t NOT NULL,
    gateway_ref    VARCHAR(120),
    paid_at        TIMESTAMPTZ,
    CHECK (amount > 0)
);

CREATE TABLE fact.fact_shipments (
    shipment_id     BIGSERIAL PRIMARY KEY,
    sale_id         BIGINT NOT NULL REFERENCES fact.fact_sales(sale_id) ON DELETE CASCADE,
    warehouse_key   INTEGER REFERENCES dim.dim_warehouse(warehouse_key),
    carrier         VARCHAR(60),
    tracking_no     VARCHAR(120),
    status          fact.shipment_status_t NOT NULL,
    shipped_at      TIMESTAMPTZ,
    delivered_at    TIMESTAMPTZ,
    UNIQUE (carrier, tracking_no)
);

CREATE TABLE fact.fact_inventory_snapshot (
    snapshot_id    BIGSERIAL PRIMARY KEY,
    date_key       INTEGER NOT NULL REFERENCES dim.dim_date(date_key),
    product_key    BIGINT  NOT NULL REFERENCES dim.dim_product(product_key),
    warehouse_key  INTEGER NOT NULL REFERENCES dim.dim_warehouse(warehouse_key),
    on_hand_qty    INTEGER NOT NULL,
    reserved_qty   INTEGER NOT NULL DEFAULT 0,
    backorder_qty  INTEGER NOT NULL DEFAULT 0,
    UNIQUE (date_key, product_key, warehouse_key)
);
COMMENT ON TABLE fact.fact_inventory_snapshot IS '每日库存快照';

CREATE TABLE fact.fact_web_events (
    event_id      BIGSERIAL PRIMARY KEY,
    customer_key  BIGINT REFERENCES dim.dim_customer(customer_key),
    session_id    UUID NOT NULL,
    date_key      INTEGER NOT NULL REFERENCES dim.dim_date(date_key),
    time_key      INTEGER REFERENCES dim.dim_time_of_day(time_key),
    event_type    fact.event_type_t NOT NULL,
    page_url      TEXT,
    referrer      TEXT,
    device_key    INTEGER REFERENCES dim.dim_device(device_key),
    browser_key   INTEGER REFERENCES dim.dim_browser(browser_key),
    os_key        INTEGER REFERENCES dim.dim_os(os_key),
    properties    JSONB,
    occurred_at   TIMESTAMPTZ NOT NULL
);

CREATE TABLE fact.fact_ad_impressions (
    impression_id   BIGSERIAL PRIMARY KEY,
    campaign_key    BIGINT  NOT NULL REFERENCES dim.dim_campaign(campaign_key),
    customer_key    BIGINT  REFERENCES dim.dim_customer(customer_key),
    date_key        INTEGER NOT NULL REFERENCES dim.dim_date(date_key),
    cost_micros     BIGINT  NOT NULL,
    placement       VARCHAR(80),
    occurred_at     TIMESTAMPTZ NOT NULL
);

CREATE TABLE fact.fact_ad_clicks (
    click_id        BIGSERIAL PRIMARY KEY,
    impression_id   BIGINT REFERENCES fact.fact_ad_impressions(impression_id),
    campaign_key    BIGINT NOT NULL REFERENCES dim.dim_campaign(campaign_key),
    customer_key    BIGINT REFERENCES dim.dim_customer(customer_key),
    date_key        INTEGER NOT NULL REFERENCES dim.dim_date(date_key),
    cost_micros     BIGINT NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL
);

CREATE TABLE fact.fact_email_sends (
    send_id        BIGSERIAL PRIMARY KEY,
    customer_key   BIGINT NOT NULL REFERENCES dim.dim_customer(customer_key),
    campaign_key   BIGINT REFERENCES dim.dim_campaign(campaign_key),
    template_code  VARCHAR(60) NOT NULL,
    sent_at        TIMESTAMPTZ NOT NULL,
    delivered      BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE fact.fact_email_opens (
    open_id        BIGSERIAL PRIMARY KEY,
    send_id        BIGINT NOT NULL REFERENCES fact.fact_email_sends(send_id) ON DELETE CASCADE,
    opened_at      TIMESTAMPTZ NOT NULL,
    user_agent     TEXT
);

CREATE TABLE fact.fact_subscriptions (
    subscription_id BIGSERIAL PRIMARY KEY,
    customer_key    BIGINT NOT NULL REFERENCES dim.dim_customer(customer_key),
    plan_code       VARCHAR(40) NOT NULL,
    started_on      DATE NOT NULL,
    ended_on        DATE,
    mrr             NUMERIC(10,2) NOT NULL,
    is_active       BOOLEAN GENERATED ALWAYS AS (ended_on IS NULL) STORED
);

CREATE TABLE fact.fact_churn_events (
    churn_id       BIGSERIAL PRIMARY KEY,
    subscription_id BIGINT NOT NULL REFERENCES fact.fact_subscriptions(subscription_id),
    churned_on     DATE NOT NULL,
    reason         VARCHAR(120),
    is_voluntary   BOOLEAN NOT NULL
);

CREATE TABLE fact.fact_loyalty_points (
    txn_id        BIGSERIAL PRIMARY KEY,
    customer_key  BIGINT NOT NULL REFERENCES dim.dim_customer(customer_key),
    date_key      INTEGER NOT NULL REFERENCES dim.dim_date(date_key),
    points_delta  INTEGER NOT NULL,
    reason        VARCHAR(60) NOT NULL,
    sale_id       BIGINT REFERENCES fact.fact_sales(sale_id)
);

CREATE TABLE fact.fact_customer_support (
    ticket_id      BIGSERIAL PRIMARY KEY,
    customer_key   BIGINT NOT NULL REFERENCES dim.dim_customer(customer_key),
    employee_key   BIGINT REFERENCES dim.dim_employee(employee_key),
    opened_at      TIMESTAMPTZ NOT NULL,
    closed_at      TIMESTAMPTZ,
    channel_key    INTEGER REFERENCES dim.dim_channel(channel_key),
    sentiment_score NUMERIC(4,3),
    csat_score      SMALLINT
);

-- =============================================================================
-- STAGING TABLES (12)
-- =============================================================================

CREATE TABLE staging.stg_orders_raw (
    raw_id        BIGSERIAL PRIMARY KEY,
    payload       JSONB NOT NULL,
    source        VARCHAR(40) NOT NULL,
    received_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    load_status   staging.load_status_t NOT NULL DEFAULT 'queued',
    error_text    TEXT
);

CREATE TABLE staging.stg_customers_raw (
    raw_id        BIGSERIAL PRIMARY KEY,
    customer_bk   VARCHAR(40) NOT NULL,
    payload       JSONB NOT NULL,
    received_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    load_status   staging.load_status_t NOT NULL DEFAULT 'queued'
);

CREATE TABLE staging.stg_products_raw (
    raw_id        BIGSERIAL PRIMARY KEY,
    product_bk    VARCHAR(40) NOT NULL,
    payload       JSONB NOT NULL,
    received_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    load_status   staging.load_status_t NOT NULL DEFAULT 'queued'
);

CREATE TABLE staging.stg_payments_raw (
    raw_id      BIGSERIAL PRIMARY KEY,
    order_bk    VARCHAR(40) NOT NULL,
    payload     JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE staging.stg_shipments_raw (
    raw_id      BIGSERIAL PRIMARY KEY,
    tracking_no VARCHAR(120),
    payload     JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE staging.stg_clicks_raw (
    raw_id      BIGSERIAL PRIMARY KEY,
    payload     JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE staging.stg_impressions_raw (
    raw_id      BIGSERIAL PRIMARY KEY,
    payload     JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE staging.stg_emails_raw (
    raw_id      BIGSERIAL PRIMARY KEY,
    payload     JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE staging.stg_warehouse_movements (
    raw_id      BIGSERIAL PRIMARY KEY,
    payload     JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE staging.stg_returns_raw (
    raw_id      BIGSERIAL PRIMARY KEY,
    payload     JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE staging.stg_loyalty_raw (
    raw_id      BIGSERIAL PRIMARY KEY,
    payload     JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE staging.stg_support_tickets (
    raw_id      BIGSERIAL PRIMARY KEY,
    payload     JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =============================================================================
-- AUDIT TABLES (8)
-- =============================================================================

CREATE TABLE audit.user_login (
    id           BIGSERIAL PRIMARY KEY,
    customer_key BIGINT REFERENCES dim.dim_customer(customer_key),
    success      BOOLEAN NOT NULL,
    ip           INET,
    user_agent   TEXT,
    occurred_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE audit.admin_action (
    id           BIGSERIAL PRIMARY KEY,
    employee_key BIGINT REFERENCES dim.dim_employee(employee_key),
    action       VARCHAR(60) NOT NULL,
    target       VARCHAR(120),
    severity     audit.severity_t NOT NULL DEFAULT 'info',
    occurred_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE audit.data_access (
    id           BIGSERIAL PRIMARY KEY,
    employee_key BIGINT REFERENCES dim.dim_employee(employee_key),
    schema_name  VARCHAR(60) NOT NULL,
    table_name   VARCHAR(120) NOT NULL,
    rows_read    BIGINT NOT NULL DEFAULT 0,
    occurred_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE audit.etl_runs (
    id            BIGSERIAL PRIMARY KEY,
    pipeline      VARCHAR(60) NOT NULL,
    started_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at   TIMESTAMPTZ,
    rows_in       BIGINT NOT NULL DEFAULT 0,
    rows_out      BIGINT NOT NULL DEFAULT 0,
    status        staging.load_status_t NOT NULL DEFAULT 'processing',
    error_text    TEXT
);

CREATE TABLE audit.quality_checks (
    id           BIGSERIAL PRIMARY KEY,
    pipeline     VARCHAR(60) NOT NULL,
    check_name   VARCHAR(80) NOT NULL,
    passed       BOOLEAN NOT NULL,
    severity     audit.severity_t NOT NULL DEFAULT 'warn',
    details      JSONB,
    checked_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE audit.pipeline_failures (
    id            BIGSERIAL PRIMARY KEY,
    pipeline      VARCHAR(60) NOT NULL,
    failed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    severity      audit.severity_t NOT NULL DEFAULT 'error',
    error_text    TEXT NOT NULL
);

CREATE TABLE audit.permissions_change (
    id            BIGSERIAL PRIMARY KEY,
    employee_key  BIGINT REFERENCES dim.dim_employee(employee_key),
    changed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    role_before   VARCHAR(60),
    role_after    VARCHAR(60) NOT NULL
);

CREATE TABLE audit.export_jobs (
    id            BIGSERIAL PRIMARY KEY,
    employee_key  BIGINT REFERENCES dim.dim_employee(employee_key),
    job_name      VARCHAR(120) NOT NULL,
    rows_exported BIGINT NOT NULL DEFAULT 0,
    file_path     TEXT,
    started_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at   TIMESTAMPTZ
);

-- =============================================================================
-- REPORTING TABLES (10: 7 tables + 3 mat views; 1 plain view added later)
-- =============================================================================

CREATE TABLE reporting.daily_sales (
    date_key    INTEGER PRIMARY KEY REFERENCES dim.dim_date(date_key),
    order_count INTEGER NOT NULL,
    item_count  INTEGER NOT NULL,
    revenue     NUMERIC(14,2) NOT NULL,
    tax         NUMERIC(14,2) NOT NULL,
    discount    NUMERIC(14,2) NOT NULL
);
COMMENT ON TABLE reporting.daily_sales IS '每日销售汇总（手动维护表）';

CREATE TABLE reporting.weekly_active_users (
    iso_week_starts DATE PRIMARY KEY,
    active_users    INTEGER NOT NULL,
    new_users       INTEGER NOT NULL,
    returning_users INTEGER NOT NULL
);

CREATE TABLE reporting.funnel (
    funnel_date    DATE NOT NULL,
    step_name      VARCHAR(40) NOT NULL,
    user_count     INTEGER NOT NULL,
    PRIMARY KEY (funnel_date, step_name)
);

CREATE TABLE reporting.cohort_retention (
    cohort_month   DATE NOT NULL,
    period_offset  SMALLINT NOT NULL,
    active_users   INTEGER NOT NULL,
    PRIMARY KEY (cohort_month, period_offset)
);

CREATE TABLE reporting.marketing_attribution (
    attribution_id BIGSERIAL PRIMARY KEY,
    sale_id        BIGINT REFERENCES fact.fact_sales(sale_id) ON DELETE CASCADE,
    campaign_key   BIGINT REFERENCES dim.dim_campaign(campaign_key),
    weight         NUMERIC(4,3) NOT NULL,
    model          VARCHAR(40) NOT NULL,
    CHECK (weight >= 0 AND weight <= 1)
);

CREATE TABLE reporting.inventory_alerts (
    alert_id       BIGSERIAL PRIMARY KEY,
    product_key    BIGINT NOT NULL REFERENCES dim.dim_product(product_key),
    warehouse_key  INTEGER NOT NULL REFERENCES dim.dim_warehouse(warehouse_key),
    alert_level    audit.severity_t NOT NULL,
    threshold      INTEGER NOT NULL,
    on_hand_qty    INTEGER NOT NULL,
    triggered_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at    TIMESTAMPTZ
);

CREATE TABLE reporting.kpi_daily (
    date_key   INTEGER NOT NULL REFERENCES dim.dim_date(date_key),
    kpi        VARCHAR(40) NOT NULL,
    value      NUMERIC(14,4) NOT NULL,
    trend      reporting.kpi_trend_t,
    PRIMARY KEY (date_key, kpi)
);
COMMENT ON TABLE reporting.kpi_daily IS '日级 KPI 仓（含趋势标签）';

-- =============================================================================
-- INDEXES (most-used hot paths)
-- =============================================================================

-- dim
CREATE INDEX dim_customer_email_idx       ON dim.dim_customer (lower(email));
CREATE INDEX dim_customer_segment_idx     ON dim.dim_customer (segment_key);
CREATE INDEX dim_customer_current_idx     ON dim.dim_customer (customer_bk) WHERE is_current = TRUE;
CREATE INDEX dim_customer_metadata_gin    ON dim.dim_customer USING GIN (metadata);
CREATE INDEX dim_product_name_trgm        ON dim.dim_product USING GIN (name gin_trgm_ops);
CREATE INDEX dim_product_category_idx     ON dim.dim_product (category_key);
CREATE INDEX dim_product_attrs_gin        ON dim.dim_product USING GIN (attributes);
CREATE INDEX dim_product_active_current   ON dim.dim_product (sku) WHERE is_active = TRUE AND is_current = TRUE;
CREATE INDEX dim_campaign_window_idx      ON dim.dim_campaign (started_on, ended_on);

-- fact
CREATE INDEX fact_sales_date_idx          ON fact.fact_sales (date_key);
CREATE INDEX fact_sales_customer_idx      ON fact.fact_sales (customer_key);
CREATE INDEX fact_sales_status_idx        ON fact.fact_sales (status);
CREATE INDEX fact_sales_placed_idx        ON fact.fact_sales (placed_at DESC);
CREATE INDEX fact_sales_paid_only_idx     ON fact.fact_sales (date_key, customer_key) WHERE status IN ('paid','shipped','delivered');
CREATE INDEX fact_sales_items_sale_idx    ON fact.fact_sales_items (sale_id);
CREATE INDEX fact_sales_items_product_idx ON fact.fact_sales_items (product_key);
CREATE INDEX fact_returns_sale_idx        ON fact.fact_returns (sale_id);
CREATE INDEX fact_payments_sale_idx       ON fact.fact_payments (sale_id);
CREATE INDEX fact_shipments_sale_idx      ON fact.fact_shipments (sale_id);
CREATE INDEX fact_inventory_pwd_idx       ON fact.fact_inventory_snapshot (product_key, warehouse_key, date_key DESC);
CREATE INDEX fact_web_events_customer_idx ON fact.fact_web_events (customer_key);
CREATE INDEX fact_web_events_session_idx  ON fact.fact_web_events (session_id);
CREATE INDEX fact_web_events_type_idx     ON fact.fact_web_events (event_type);
CREATE INDEX fact_web_events_props_gin    ON fact.fact_web_events USING GIN (properties);
CREATE INDEX fact_web_events_occurred_idx ON fact.fact_web_events (occurred_at DESC);
CREATE INDEX fact_ad_impressions_camp_idx ON fact.fact_ad_impressions (campaign_key);
CREATE INDEX fact_ad_clicks_camp_idx      ON fact.fact_ad_clicks (campaign_key);
CREATE INDEX fact_email_sends_customer   ON fact.fact_email_sends (customer_key);
CREATE INDEX fact_subscriptions_active   ON fact.fact_subscriptions (customer_key) WHERE ended_on IS NULL;
CREATE INDEX fact_loyalty_customer_idx   ON fact.fact_loyalty_points (customer_key);

-- staging
CREATE INDEX stg_orders_status_idx       ON staging.stg_orders_raw (load_status);
CREATE INDEX stg_orders_received_idx     ON staging.stg_orders_raw (received_at DESC);
CREATE INDEX stg_orders_payload_gin      ON staging.stg_orders_raw USING GIN (payload);
CREATE INDEX stg_customers_bk_idx        ON staging.stg_customers_raw (customer_bk);

-- audit
CREATE INDEX audit_user_login_at_idx     ON audit.user_login (occurred_at DESC);
CREATE INDEX audit_user_login_failed_idx ON audit.user_login (occurred_at DESC) WHERE success = FALSE;
CREATE INDEX audit_admin_severity_idx    ON audit.admin_action (severity);
CREATE INDEX audit_etl_runs_pipeline_idx ON audit.etl_runs (pipeline, started_at DESC);
CREATE INDEX audit_quality_failed_idx    ON audit.quality_checks (checked_at DESC) WHERE passed = FALSE;

-- reporting
CREATE INDEX reporting_attr_sale_idx     ON reporting.marketing_attribution (sale_id);
CREATE INDEX reporting_inv_alerts_open   ON reporting.inventory_alerts (triggered_at DESC) WHERE resolved_at IS NULL;

-- =============================================================================
-- DATA: dim_date (1 year, 365 rows)
-- =============================================================================
INSERT INTO dim.dim_date (date_key, full_date, year, quarter, month, month_name, day_of_month, day_of_week, day_name, week_of_year, is_weekend, is_holiday, fiscal_year, fiscal_quarter)
SELECT
    to_char(d, 'YYYYMMDD')::int,
    d,
    EXTRACT(year FROM d)::smallint,
    EXTRACT(quarter FROM d)::smallint,
    EXTRACT(month FROM d)::smallint,
    to_char(d, 'Month'),
    EXTRACT(day FROM d)::smallint,
    EXTRACT(isodow FROM d)::smallint,
    to_char(d, 'Day'),
    EXTRACT(week FROM d)::smallint,
    EXTRACT(isodow FROM d) IN (6,7),
    FALSE,
    EXTRACT(year FROM d)::smallint,
    EXTRACT(quarter FROM d)::smallint
FROM generate_series(DATE '2025-01-01', DATE '2025-12-31', INTERVAL '1 day') d;

-- =============================================================================
-- DATA: dim_time_of_day (1440 rows)
-- =============================================================================
INSERT INTO dim.dim_time_of_day (time_key, hour, minute, am_pm, daypart)
SELECT
    h * 100 + m,
    h, m,
    CASE WHEN h < 12 THEN 'AM' ELSE 'PM' END,
    CASE
        WHEN h <  6 THEN 'night'
        WHEN h < 12 THEN 'morning'
        WHEN h < 18 THEN 'afternoon'
        ELSE 'evening'
    END
FROM generate_series(0, 23) h
CROSS JOIN generate_series(0, 59) m;

-- =============================================================================
-- DATA: small reference dims
-- =============================================================================
INSERT INTO dim.dim_currency (currency, name, symbol, decimals) VALUES
  ('USD', 'US Dollar',     '$',  2),
  ('EUR', 'Euro',          '€',  2),
  ('GBP', 'British Pound', '£',  2),
  ('JPY', 'Japanese Yen',  '¥',  0),
  ('CNY', 'Chinese Yuan',  '¥',  2);

INSERT INTO dim.dim_country (iso2, iso3, name, region, sub_region, currency, is_eu) VALUES
  ('US','USA','United States','Americas','Northern America','USD', FALSE),
  ('CA','CAN','Canada',       'Americas','Northern America','USD', FALSE),
  ('MX','MEX','Mexico',       'Americas','Latin America',   'USD', FALSE),
  ('GB','GBR','United Kingdom','Europe','Northern Europe',  'GBP', FALSE),
  ('DE','DEU','Germany',      'Europe','Western Europe',    'EUR', TRUE),
  ('FR','FRA','France',       'Europe','Western Europe',    'EUR', TRUE),
  ('JP','JPN','Japan',        'Asia',  'Eastern Asia',      'JPY', FALSE),
  ('CN','CHN','China',        'Asia',  'Eastern Asia',      'CNY', FALSE),
  ('IN','IND','India',        'Asia',  'Southern Asia',     'USD', FALSE),
  ('AU','AUS','Australia',    'Oceania','Australia',        'USD', FALSE);

INSERT INTO dim.dim_region (country_key, code, name) VALUES
  (1,'CA','California'), (1,'NY','New York'), (1,'TX','Texas'), (1,'IL','Illinois'),
  (2,'ON','Ontario'),    (2,'BC','British Columbia'),
  (4,'ENG','England'),   (5,'BY','Bavaria'), (6,'IDF','Île-de-France'),
  (7,'TKO','Tokyo'),     (8,'BJ','Beijing'), (8,'SH','Shanghai'),
  (9,'MH','Maharashtra'),(10,'NSW','New South Wales');

INSERT INTO dim.dim_city (region_key, name, population, timezone) VALUES
  (1,'San Francisco', 880000,'America/Los_Angeles'),
  (1,'Los Angeles',  3970000,'America/Los_Angeles'),
  (2,'New York City',8400000,'America/New_York'),
  (3,'Houston',      2300000,'America/Chicago'),
  (4,'Chicago',      2700000,'America/Chicago'),
  (5,'Toronto',      2900000,'America/Toronto'),
  (6,'Vancouver',     675000,'America/Vancouver'),
  (7,'London',       9000000,'Europe/London'),
  (8,'Munich',       1500000,'Europe/Berlin'),
  (9,'Paris',        2160000,'Europe/Paris'),
  (10,'Tokyo',      13900000,'Asia/Tokyo'),
  (11,'Beijing',    21500000,'Asia/Shanghai'),
  (12,'Shanghai',   24800000,'Asia/Shanghai'),
  (13,'Mumbai',     20000000,'Asia/Kolkata'),
  (14,'Sydney',      5300000,'Australia/Sydney');

INSERT INTO dim.dim_customer_segment (code, description) VALUES
  ('new',     '新注册客户'),
  ('active',  '活跃客户'),
  ('at_risk', '流失风险客户'),
  ('churned', '已流失客户'),
  ('vip',     'VIP 客户');

INSERT INTO dim.dim_brand (name, country_key, founded) VALUES
  ('Acme',1,1948),('Globex',1,1972),('Initech',1,1996),('Umbrella',7,1968),
  ('Soylent',1,1972),('Hooli',1,2009),('Pied Piper',1,2014),('Stark',1,1939),
  ('Wayne',1,1882),('TyrellCo',1,2019),('Nakatomi',7,1988),('Cyberdyne',1,1984);

INSERT INTO dim.dim_supplier (supplier_bk, name, country_key, rating, is_active)
SELECT
    'SUP-' || LPAD(g::text, 4, '0'),
    'Supplier ' || g,
    ((g - 1) % 10) + 1,
    ROUND((1 + (random() * 4))::numeric, 2),
    g % 23 <> 0
FROM generate_series(1, 50) g;

INSERT INTO dim.dim_product_category (parent_key, name, path) VALUES
  (NULL,'Apparel',                'Apparel'),
  (NULL,'Electronics',            'Electronics'),
  (NULL,'Home',                   'Home'),
  (NULL,'Beauty',                 'Beauty'),
  (NULL,'Sports',                 'Sports');

INSERT INTO dim.dim_product_category (parent_key, name, path) VALUES
  (1,'Mens Tops',     'Apparel/Mens Tops'),
  (1,'Womens Tops',   'Apparel/Womens Tops'),
  (1,'Footwear',      'Apparel/Footwear'),
  (2,'Laptops',       'Electronics/Laptops'),
  (2,'Phones',        'Electronics/Phones'),
  (2,'Audio',         'Electronics/Audio'),
  (3,'Cookware',      'Home/Cookware'),
  (3,'Furniture',     'Home/Furniture'),
  (4,'Skin Care',     'Beauty/Skin Care'),
  (5,'Cycling',       'Sports/Cycling');

-- dim_employee_role
INSERT INTO dim.dim_employee_role (name, department) VALUES
  ('SDE',           'Engineering'),
  ('Senior SDE',    'Engineering'),
  ('Tech Lead',     'Engineering'),
  ('Data Analyst',  'Analytics'),
  ('DA Manager',    'Analytics'),
  ('CSR',           'Customer Support'),
  ('Sales Rep',     'Sales'),
  ('Marketing Specialist', 'Marketing'),
  ('Store Manager', 'Retail'),
  ('Warehouse Op',  'Operations');

-- dim_employee (~80)
INSERT INTO dim.dim_employee (employee_bk, full_name, email, role_key, hire_date, is_active)
SELECT
    'EMP-' || LPAD(g::text, 4, '0'),
    'Employee ' || g,
    'employee' || g || '@company.com',
    ((g - 1) % 10) + 1,
    DATE '2018-01-01' + ((g % 1800) || ' days')::interval,
    g % 47 <> 0
FROM generate_series(1, 80) g;

-- dim_store (~15)
INSERT INTO dim.dim_store (store_bk, name, city_key, opened_on, is_active)
SELECT
    'STR-' || LPAD(g::text, 3, '0'),
    'Store #' || g,
    ((g - 1) % 15) + 1,
    DATE '2010-01-01' + ((g * 73) || ' days')::interval,
    g % 17 <> 0
FROM generate_series(1, 15) g;

-- dim_warehouse (~6)
INSERT INTO dim.dim_warehouse (warehouse_bk, name, city_key, capacity_m3) VALUES
  ('WH-001','East Coast DC',  3,  50000),
  ('WH-002','West Coast DC',  1,  60000),
  ('WH-003','South DC',       4,  35000),
  ('WH-004','Midwest DC',     5,  40000),
  ('WH-005','Europe DC',      8, 25000),
  ('WH-006','Asia DC',       11, 30000);

-- dim_channel
INSERT INTO dim.dim_channel (code, description) VALUES
  ('web',            'Website'),
  ('mobile_ios',     'iOS App'),
  ('mobile_android', 'Android App'),
  ('retail',         'Brick & mortar store'),
  ('partner',        'Partner / marketplace'),
  ('phone',          'Phone order'),
  ('email',          'Email order');

-- dim_campaign
INSERT INTO dim.dim_campaign (campaign_bk, name, channel_key, started_on, ended_on, budget)
SELECT
    'CMP-' || LPAD(g::text, 4, '0'),
    (ARRAY['Spring Sale','Summer Splash','Back to School','Fall Refresh','Black Friday','Cyber Monday','Holiday Cheer','Winter Clearance','New Year Reset','Valentine','Mothers Day','Fathers Day'])[((g - 1) % 12) + 1] || ' ' || (2024 + (g % 2)),
    ((g - 1) % 7) + 1,
    DATE '2025-01-01' + ((g * 11) || ' days')::interval,
    DATE '2025-01-01' + ((g * 11 + 30) || ' days')::interval,
    ROUND((1000 + random() * 49000)::numeric, 2)
FROM generate_series(1, 30) g;

-- dim_payment_method
INSERT INTO dim.dim_payment_method (name, is_card) VALUES
  ('Visa',TRUE),('MasterCard',TRUE),('Amex',TRUE),('Discover',TRUE),
  ('PayPal',FALSE),('Apple Pay',FALSE),('Google Pay',FALSE),
  ('Alipay',FALSE),('WeChat Pay',FALSE),('Bank Transfer',FALSE),('Gift Card',FALSE);

-- dim_device / browser / os
INSERT INTO dim.dim_device (type, vendor, model) VALUES
  ('desktop','Apple','MacBook Pro'), ('desktop','Dell','XPS'),
  ('desktop','HP','EliteBook'),      ('mobile','Apple','iPhone 15'),
  ('mobile','Samsung','Galaxy S24'), ('mobile','Google','Pixel 8'),
  ('tablet','Apple','iPad Pro'),     ('tablet','Samsung','Galaxy Tab'),
  ('tv','Samsung','SmartTV'),        ('desktop','Lenovo','ThinkPad');

INSERT INTO dim.dim_browser (name, major_version) VALUES
  ('Chrome',119),('Chrome',120),('Chrome',121),
  ('Safari',17),('Safari',18),
  ('Firefox',119),('Firefox',120),
  ('Edge',119),('Edge',120),
  ('Opera',105);

INSERT INTO dim.dim_os (family, version) VALUES
  ('Windows','10'),('Windows','11'),
  ('macOS','13'),('macOS','14'),
  ('iOS','16'),('iOS','17'),
  ('Android','13'),('Android','14'),
  ('Linux','Ubuntu 22.04'),('Linux','Fedora 39');

-- =============================================================================
-- DATA: dim_customer (2000 rows, SCD2 — all current)
-- =============================================================================
INSERT INTO dim.dim_customer (customer_bk, email, full_name, gender, birth_year, segment_key, home_country_key, address, valid_from, is_current, metadata)
SELECT
    'CUST-' || LPAD(g::text, 6, '0'),
    'cust' || g || '@example.com',
    (ARRAY['Alex','Brooke','Casey','Dakota','Elliot','Finley','Gray','Harper','Jamie','Kai'])[((g - 1) % 10) + 1] || ' ' ||
    (ARRAY['Anderson','Brown','Chen','Davis','Evans','Garcia','Hassan','Ito','Johnson','Kim','Lee','Müller','Novak','Patel'])[((g - 1) % 14) + 1],
    (ARRAY['male','female','female','male','nonbinary','unknown'])[((g - 1) % 6) + 1]::dim.gender_t,
    (1960 + (g % 50))::smallint,
    ((g - 1) % 5) + 1,
    ((g - 1) % 10) + 1,
    ROW(g || ' Main St', NULL, 'CityX', 'StateY', LPAD((g % 99999)::text, 5, '0'), 'US')::dim.address_t,
    NOW() - ((g % 720) || ' days')::interval,
    TRUE,
    jsonb_build_object('preferred_lang', CASE g % 3 WHEN 0 THEN 'en' WHEN 1 THEN 'zh' ELSE 'es' END,
                       'newsletter', g % 2 = 0)
FROM generate_series(1, 2000) g;

-- =============================================================================
-- DATA: dim_product (1000 rows)
-- =============================================================================
INSERT INTO dim.dim_product (product_bk, sku, name, category_key, brand_key, supplier_key, list_price, cost, weight_kg, is_active, attributes, valid_from, is_current)
SELECT
    'PRD-' || LPAD(g::text, 6, '0'),
    'SKU-' || LPAD(g::text, 6, '0'),
    (ARRAY['Premium','Classic','Ultra','Pro','Lite','Basic','Deluxe','Eco','Smart','Plus'])[((g - 1) % 10) + 1] || ' ' ||
    (ARRAY['Tee','Hoodie','Sneaker','Laptop','Phone','Headphones','Pan','Chair','Cleanser','Bike'])[((g - 1) % 10) + 1] || ' ' || g,
    ((g - 1) % 15) + 1,
    ((g - 1) % 12) + 1,
    ((g - 1) % 50) + 1,
    ROUND((10 + (g % 990))::numeric, 2),
    ROUND(((10 + (g % 990)) * 0.6)::numeric, 2),
    ROUND((0.1 + (random() * 9.9))::numeric, 3),
    g % 23 <> 0,
    jsonb_build_object('color',  (ARRAY['black','white','red','blue','green','silver','gold'])[((g - 1) % 7) + 1],
                       'rating', round((3 + random() * 2)::numeric, 1)),
    NOW() - ((g % 365) || ' days')::interval,
    TRUE
FROM generate_series(1, 1000) g;

-- =============================================================================
-- DATA: fact_sales (10,000)
-- =============================================================================
INSERT INTO fact.fact_sales (order_bk, date_key, time_key, customer_key, channel_key, store_key, employee_key, campaign_key, pm_key, currency, status, gross_amount, discount_amount, tax_amount, shipping_amount, net_amount, item_count, placed_at, delivered_at)
SELECT
    'ORD-' || LPAD(g::text, 7, '0'),
    -- date_key uniformly across the 365 days of 2025
    to_char(DATE '2025-01-01' + ((g % 365) || ' days')::interval, 'YYYYMMDD')::int,
    ((g % 24) * 100 + (g % 60)),
    1 + (g % 2000),
    ((g - 1) % 7) + 1,
    1 + (g % 15),
    1 + (g % 80),
    CASE WHEN g % 3 = 0 THEN 1 + (g % 30) END,
    1 + (g % 11),
    'USD',
    (CASE
        WHEN g % 30 = 0 THEN 'cancelled'
        WHEN g % 13 = 0 THEN 'pending'
        WHEN g % 7  = 0 THEN 'paid'
        WHEN g % 5  = 0 THEN 'shipped'
        ELSE 'delivered'
     END)::fact.order_status_t,
    ROUND((20 + (g % 5000))::numeric, 2),
    ROUND(((g % 50))::numeric, 2),
    ROUND((20 + (g % 5000)) * 0.08, 2),
    5.00,
    ROUND((20 + (g % 5000)) * 1.08 - (g % 50) + 5, 2),
    1 + (g % 5),
    DATE '2025-01-01' + ((g % 365) || ' days')::interval + ((g % 86400) || ' seconds')::interval,
    CASE WHEN g % 5 IN (0,1,2,3) THEN DATE '2025-01-01' + ((g % 365) + 3 || ' days')::interval END
FROM generate_series(1, 10000) g;

-- =============================================================================
-- DATA: fact_sales_items (~30,000 — 3 per order on avg)
-- =============================================================================
INSERT INTO fact.fact_sales_items (sale_id, product_key, quantity, unit_price, margin)
SELECT
    s.sale_id,
    1 + ((s.sale_id * 31 + n * 7) % 1000),
    1 + ((s.sale_id + n) % 4),
    ROUND((10 + ((s.sale_id + n * 13) % 500))::numeric, 2),
    ROUND((random() * 100)::numeric, 2)
FROM fact.fact_sales s
CROSS JOIN generate_series(0, 2) n;

-- =============================================================================
-- DATA: fact_returns (~300, ~3% of orders)
-- =============================================================================
INSERT INTO fact.fact_returns (sale_id, sales_item_id, return_date_key, quantity, refund_amount, reason, notes)
SELECT
    s.sale_id,
    (SELECT sales_item_id FROM fact.fact_sales_items WHERE sale_id = s.sale_id LIMIT 1),
    s.date_key,
    1,
    ROUND(s.gross_amount * 0.5, 2),
    (ARRAY['damaged','wrong_item','customer_changed_mind','too_late','quality','other'])[(s.sale_id % 6) + 1]::fact.return_reason_t,
    'Auto-generated return for testing'
FROM fact.fact_sales s
WHERE s.sale_id % 33 = 0
  AND s.status IN ('paid','shipped','delivered');

-- =============================================================================
-- DATA: fact_payments (~9000 — one per non-cancelled order)
-- =============================================================================
INSERT INTO fact.fact_payments (sale_id, pm_key, amount, currency, status, gateway_ref, paid_at)
SELECT
    s.sale_id,
    s.pm_key,
    s.net_amount,
    s.currency,
    CASE
        WHEN s.status = 'cancelled' THEN 'failed'::fact.payment_status_t
        WHEN s.status = 'pending'   THEN 'pending'::fact.payment_status_t
        WHEN s.status = 'refunded'  THEN 'refunded'::fact.payment_status_t
        ELSE 'captured'::fact.payment_status_t
    END,
    'gw_' || encode(gen_random_bytes(8), 'hex'),
    s.placed_at + INTERVAL '5 minutes'
FROM fact.fact_sales s
WHERE s.status <> 'cancelled';

-- =============================================================================
-- DATA: fact_shipments (~7500 — for shipped/delivered)
-- =============================================================================
INSERT INTO fact.fact_shipments (sale_id, warehouse_key, carrier, tracking_no, status, shipped_at, delivered_at)
SELECT
    s.sale_id,
    1 + ((s.sale_id * 7) % 6),
    (ARRAY['UPS','FedEx','USPS','DHL','OnTrac'])[((s.sale_id - 1) % 5) + 1],
    'TRK-' || LPAD(s.sale_id::text, 9, '0'),
    CASE
        WHEN s.status = 'delivered' THEN 'delivered'::fact.shipment_status_t
        WHEN s.status = 'shipped'   THEN 'in_transit'::fact.shipment_status_t
        ELSE 'label_created'::fact.shipment_status_t
    END,
    s.placed_at + INTERVAL '1 day',
    s.delivered_at
FROM fact.fact_sales s
WHERE s.status IN ('shipped','delivered');

-- =============================================================================
-- DATA: fact_inventory_snapshot (~5000)
-- =============================================================================
INSERT INTO fact.fact_inventory_snapshot (date_key, product_key, warehouse_key, on_hand_qty, reserved_qty, backorder_qty)
SELECT
    to_char(DATE '2025-01-01' + ((g % 12) * 30 || ' days')::interval, 'YYYYMMDD')::int,
    1 + ((g * 17) % 1000),
    1 + (g % 6),
    50 + (g % 500),
    g % 20,
    g % 5
FROM generate_series(1, 5000) g
ON CONFLICT (date_key, product_key, warehouse_key) DO NOTHING;

-- =============================================================================
-- DATA: fact_web_events (50,000)
-- =============================================================================
INSERT INTO fact.fact_web_events (customer_key, session_id, date_key, time_key, event_type, page_url, referrer, device_key, browser_key, os_key, properties, occurred_at)
SELECT
    CASE WHEN g % 20 = 0 THEN NULL ELSE 1 + (g % 2000) END,
    -- 5 events per session on avg → 10000 distinct sessions
    md5((g / 5)::text)::uuid,
    to_char(DATE '2025-01-01' + ((g % 365) || ' days')::interval, 'YYYYMMDD')::int,
    ((g % 24) * 100 + (g % 60)),
    (ARRAY['page_view','add_to_cart','remove_from_cart','checkout','purchase','signup','login','search','share'])[((g - 1) % 9) + 1]::fact.event_type_t,
    '/page/' || (g % 200),
    CASE g % 4 WHEN 0 THEN 'https://google.com' WHEN 1 THEN 'https://facebook.com' ELSE NULL END,
    1 + (g % 10),
    1 + (g % 10),
    1 + (g % 10),
    jsonb_build_object('utm_source', (ARRAY['google','facebook','direct','email','tiktok','instagram'])[((g - 1) % 6) + 1],
                       'duration_ms', (g % 30000)),
    DATE '2025-01-01' + ((g % 365) || ' days')::interval + ((g % 86400) || ' seconds')::interval
FROM generate_series(1, 50000) g;

-- =============================================================================
-- DATA: fact_ad_impressions (30,000) and clicks (~3000)
-- =============================================================================
INSERT INTO fact.fact_ad_impressions (campaign_key, customer_key, date_key, cost_micros, placement, occurred_at)
SELECT
    1 + (g % 30),
    CASE WHEN g % 7 = 0 THEN NULL ELSE 1 + (g % 2000) END,
    to_char(DATE '2025-01-01' + ((g % 365) || ' days')::interval, 'YYYYMMDD')::int,
    50000 + (g % 200000),
    (ARRAY['top_banner','sidebar','feed','footer'])[((g - 1) % 4) + 1],
    DATE '2025-01-01' + ((g % 365) || ' days')::interval
FROM generate_series(1, 30000) g;

INSERT INTO fact.fact_ad_clicks (impression_id, campaign_key, customer_key, date_key, cost_micros, occurred_at)
SELECT
    i.impression_id,
    i.campaign_key,
    i.customer_key,
    i.date_key,
    i.cost_micros,
    i.occurred_at + INTERVAL '30 seconds'
FROM fact.fact_ad_impressions i
WHERE i.impression_id % 10 = 0;

-- =============================================================================
-- DATA: fact_email_sends (10000) and opens (~3500)
-- =============================================================================
INSERT INTO fact.fact_email_sends (customer_key, campaign_key, template_code, sent_at, delivered)
SELECT
    1 + (g % 2000),
    CASE WHEN g % 2 = 0 THEN 1 + (g % 30) END,
    'tpl_' || (g % 25),
    DATE '2025-01-01' + ((g % 365) || ' days')::interval,
    g % 25 <> 0
FROM generate_series(1, 10000) g;

INSERT INTO fact.fact_email_opens (send_id, opened_at, user_agent)
SELECT
    s.send_id,
    s.sent_at + INTERVAL '4 hours',
    'Mozilla/5.0'
FROM fact.fact_email_sends s
WHERE s.send_id % 3 = 0;

-- =============================================================================
-- DATA: fact_subscriptions (~500) + churn events
-- =============================================================================
INSERT INTO fact.fact_subscriptions (customer_key, plan_code, started_on, ended_on, mrr)
SELECT
    1 + (g % 2000),
    (ARRAY['basic','pro','team','enterprise'])[((g - 1) % 4) + 1],
    DATE '2024-01-01' + ((g % 540) || ' days')::interval,
    CASE WHEN g % 4 = 0 THEN DATE '2024-01-01' + ((g % 540) + 90 || ' days')::interval END,
    (ARRAY[9.99,19.99,49.99,99.99])[((g - 1) % 4) + 1]
FROM generate_series(1, 500) g;

INSERT INTO fact.fact_churn_events (subscription_id, churned_on, reason, is_voluntary)
SELECT
    subscription_id,
    ended_on,
    (ARRAY['price','features','support','no_longer_needed','found_alternative'])[((subscription_id - 1) % 5) + 1],
    subscription_id % 3 <> 0
FROM fact.fact_subscriptions
WHERE ended_on IS NOT NULL;

-- =============================================================================
-- DATA: fact_loyalty_points (~5000)
-- =============================================================================
INSERT INTO fact.fact_loyalty_points (customer_key, date_key, points_delta, reason, sale_id)
SELECT
    s.customer_key,
    s.date_key,
    GREATEST(1, (s.net_amount / 10)::int),
    'order',
    s.sale_id
FROM fact.fact_sales s
WHERE s.status IN ('paid','shipped','delivered')
  AND s.sale_id % 2 = 0;

INSERT INTO fact.fact_loyalty_points (customer_key, date_key, points_delta, reason, sale_id)
SELECT
    1 + (g % 2000),
    to_char(DATE '2025-01-01' + ((g % 365) || ' days')::interval, 'YYYYMMDD')::int,
    50,
    'signup_bonus',
    NULL
FROM generate_series(1, 200) g;

-- =============================================================================
-- DATA: fact_customer_support (~1500)
-- =============================================================================
INSERT INTO fact.fact_customer_support (customer_key, employee_key, opened_at, closed_at, channel_key, sentiment_score, csat_score)
SELECT
    1 + (g % 2000),
    1 + (g % 80),
    DATE '2025-01-01' + ((g % 365) || ' days')::interval,
    DATE '2025-01-01' + ((g % 365) + (g % 5) || ' days')::interval,
    1 + (g % 7),
    ROUND((random() * 2 - 1)::numeric, 3),
    1 + (g % 5)
FROM generate_series(1, 1500) g;

-- =============================================================================
-- DATA: staging tables (lightweight, ~200 rows each)
-- =============================================================================
INSERT INTO staging.stg_orders_raw (payload, source, load_status)
SELECT
    jsonb_build_object('order_bk','ORD-' || g, 'amount', g % 1000, 'items', g % 5),
    (ARRAY['shopify','woo','custom_api'])[((g - 1) % 3) + 1],
    (ARRAY['queued','processing','done','failed'])[((g - 1) % 4) + 1]::staging.load_status_t
FROM generate_series(1, 300) g;

INSERT INTO staging.stg_customers_raw (customer_bk, payload)
SELECT 'CUST-' || LPAD(g::text, 6, '0'), jsonb_build_object('email','e' || g || '@x.com')
FROM generate_series(1, 200) g;

INSERT INTO staging.stg_products_raw (product_bk, payload)
SELECT 'PRD-' || LPAD(g::text, 6, '0'), jsonb_build_object('sku','SKU-' || g)
FROM generate_series(1, 200) g;

INSERT INTO staging.stg_payments_raw (order_bk, payload)
SELECT 'ORD-' || LPAD(g::text, 7, '0'), jsonb_build_object('amount', g % 1000)
FROM generate_series(1, 150) g;

INSERT INTO staging.stg_shipments_raw (tracking_no, payload)
SELECT 'TRK-' || LPAD(g::text, 9, '0'), jsonb_build_object('carrier', 'UPS')
FROM generate_series(1, 150) g;

INSERT INTO staging.stg_clicks_raw (payload)
SELECT jsonb_build_object('campaign_id', g % 30, 'cost_micros', g * 100)
FROM generate_series(1, 200) g;

INSERT INTO staging.stg_impressions_raw (payload)
SELECT jsonb_build_object('campaign_id', g % 30, 'placement', 'banner')
FROM generate_series(1, 200) g;

INSERT INTO staging.stg_emails_raw (payload)
SELECT jsonb_build_object('template', 'tpl_' || (g % 25), 'recipients', g % 100)
FROM generate_series(1, 100) g;

INSERT INTO staging.stg_warehouse_movements (payload)
SELECT jsonb_build_object('warehouse_bk', 'WH-00' || (1 + (g % 6)), 'qty_change', g % 100 - 50)
FROM generate_series(1, 300) g;

INSERT INTO staging.stg_returns_raw (payload)
SELECT jsonb_build_object('order_bk', 'ORD-' || g, 'reason', 'damaged')
FROM generate_series(1, 100) g;

INSERT INTO staging.stg_loyalty_raw (payload)
SELECT jsonb_build_object('customer_bk', 'CUST-' || LPAD(g::text, 6, '0'), 'points', g % 500)
FROM generate_series(1, 200) g;

INSERT INTO staging.stg_support_tickets (payload)
SELECT jsonb_build_object('customer_bk', 'CUST-' || LPAD(g::text, 6, '0'), 'subject', 'Where is my order?')
FROM generate_series(1, 150) g;

-- =============================================================================
-- DATA: audit tables
-- =============================================================================
INSERT INTO audit.user_login (customer_key, success, ip, user_agent, occurred_at)
SELECT
    CASE WHEN g % 13 = 0 THEN NULL ELSE 1 + (g % 2000) END,
    g % 11 <> 0,
    ('192.0.2.' || ((g % 250) + 1))::inet,
    'Mozilla/5.0',
    NOW() - ((g % 720) || ' hours')::interval
FROM generate_series(1, 5000) g;

INSERT INTO audit.admin_action (employee_key, action, target, severity, occurred_at)
SELECT
    1 + (g % 80),
    (ARRAY['create','update','delete','approve','reject','export'])[((g - 1) % 6) + 1],
    'customer:' || (g % 2000),
    (ARRAY['info','info','info','warn','error'])[((g - 1) % 5) + 1]::audit.severity_t,
    NOW() - ((g % 360) || ' hours')::interval
FROM generate_series(1, 800) g;

INSERT INTO audit.data_access (employee_key, schema_name, table_name, rows_read, occurred_at)
SELECT
    1 + (g % 80),
    (ARRAY['fact','dim','reporting'])[((g - 1) % 3) + 1],
    (ARRAY['fact_sales','dim_customer','reporting.daily_sales','fact_web_events'])[((g - 1) % 4) + 1],
    100 + (g * 7) % 5000,
    NOW() - ((g % 90) || ' days')::interval
FROM generate_series(1, 1500) g;

INSERT INTO audit.etl_runs (pipeline, started_at, finished_at, rows_in, rows_out, status, error_text)
SELECT
    (ARRAY['ingest_orders','ingest_customers','build_kpi','refresh_marts'])[((g - 1) % 4) + 1],
    NOW() - ((g % 200) || ' hours')::interval,
    NOW() - ((g % 200) - 1 || ' hours')::interval,
    1000 + (g % 5000),
    900 + (g % 5000),
    (CASE WHEN g % 23 = 0 THEN 'failed' ELSE 'done' END)::staging.load_status_t,
    CASE WHEN g % 23 = 0 THEN 'connection timeout' END
FROM generate_series(1, 500) g;

INSERT INTO audit.quality_checks (pipeline, check_name, passed, severity, details, checked_at)
SELECT
    (ARRAY['ingest_orders','ingest_customers','build_kpi','refresh_marts'])[((g - 1) % 4) + 1],
    (ARRAY['null_check','range_check','unique_check','referential_check'])[((g - 1) % 4) + 1],
    g % 9 <> 0,
    (ARRAY['info','warn','warn','error'])[((g - 1) % 4) + 1]::audit.severity_t,
    jsonb_build_object('expected', 0, 'actual', g % 100),
    NOW() - ((g % 200) || ' hours')::interval
FROM generate_series(1, 600) g;

INSERT INTO audit.pipeline_failures (pipeline, failed_at, severity, error_text)
SELECT
    (ARRAY['ingest_orders','ingest_customers','build_kpi','refresh_marts'])[((g - 1) % 4) + 1],
    NOW() - ((g % 360) || ' hours')::interval,
    (ARRAY['error','error','critical'])[((g - 1) % 3) + 1]::audit.severity_t,
    'Sample failure ' || g
FROM generate_series(1, 100) g;

INSERT INTO audit.permissions_change (employee_key, changed_at, role_before, role_after)
SELECT
    1 + (g % 80),
    NOW() - ((g % 365) || ' days')::interval,
    (ARRAY['SDE','CSR','Sales Rep'])[((g - 1) % 3) + 1],
    (ARRAY['Senior SDE','Tech Lead','Marketing Specialist','Store Manager'])[((g - 1) % 4) + 1]
FROM generate_series(1, 50) g;

INSERT INTO audit.export_jobs (employee_key, job_name, rows_exported, file_path, started_at, finished_at)
SELECT
    1 + (g % 80),
    'monthly_export_' || g,
    1000 + (g * 11) % 50000,
    '/exports/file_' || g || '.csv',
    NOW() - ((g % 365) || ' days')::interval,
    NOW() - ((g % 365) || ' days')::interval + INTERVAL '30 minutes'
FROM generate_series(1, 100) g;

-- =============================================================================
-- DATA: reporting tables (computed from facts)
-- =============================================================================
INSERT INTO reporting.daily_sales (date_key, order_count, item_count, revenue, tax, discount)
SELECT
    s.date_key,
    COUNT(*),
    SUM(s.item_count),
    SUM(s.net_amount),
    SUM(s.tax_amount),
    SUM(s.discount_amount)
FROM fact.fact_sales s
WHERE s.status IN ('paid','shipped','delivered')
GROUP BY s.date_key;

INSERT INTO reporting.weekly_active_users (iso_week_starts, active_users, new_users, returning_users)
SELECT
    date_trunc('week', placed_at)::date,
    COUNT(DISTINCT customer_key),
    COUNT(DISTINCT customer_key) FILTER (WHERE date_key % 7 = 0),
    COUNT(DISTINCT customer_key) FILTER (WHERE date_key % 7 <> 0)
FROM fact.fact_sales
GROUP BY 1
ORDER BY 1;

INSERT INTO reporting.funnel (funnel_date, step_name, user_count) VALUES
  ('2025-01-15','visit',     1500),
  ('2025-01-15','add_to_cart',900),
  ('2025-01-15','checkout',   450),
  ('2025-01-15','purchase',   320),
  ('2025-02-15','visit',     1700),
  ('2025-02-15','add_to_cart',1020),
  ('2025-02-15','checkout',   500),
  ('2025-02-15','purchase',   360),
  ('2025-03-15','visit',     1820),
  ('2025-03-15','add_to_cart',1100),
  ('2025-03-15','checkout',   540),
  ('2025-03-15','purchase',   400);

INSERT INTO reporting.cohort_retention (cohort_month, period_offset, active_users)
SELECT
    DATE '2025-01-01' + (cohort * INTERVAL '1 month'),
    period,
    GREATEST(0, 200 - cohort * 5 - period * 8 + (cohort * period) % 7)
FROM generate_series(0, 11) cohort
CROSS JOIN generate_series(0, 11) period
WHERE period <= 12 - cohort;

INSERT INTO reporting.marketing_attribution (sale_id, campaign_key, weight, model)
SELECT
    s.sale_id,
    1 + (s.sale_id % 30),
    1.0::numeric,
    'last_touch'
FROM fact.fact_sales s
WHERE s.sale_id % 5 = 0;

INSERT INTO reporting.inventory_alerts (product_key, warehouse_key, alert_level, threshold, on_hand_qty, triggered_at, resolved_at)
SELECT
    1 + (g % 1000),
    1 + (g % 6),
    (ARRAY['warn','error','critical'])[((g - 1) % 3) + 1]::audit.severity_t,
    50,
    g % 50,
    NOW() - ((g % 90) || ' days')::interval,
    CASE WHEN g % 4 <> 0 THEN NOW() - ((g % 90 - 1) || ' days')::interval END
FROM generate_series(1, 200) g;

INSERT INTO reporting.kpi_daily (date_key, kpi, value, trend)
SELECT
    d.date_key,
    k,
    ROUND((random() * 1000)::numeric, 2),
    (ARRAY['up','flat','down'])[(d.date_key + length(k)) % 3 + 1]::reporting.kpi_trend_t
FROM dim.dim_date d
CROSS JOIN unnest(ARRAY['gmv','orders','aov','cvr','cac','ltv']) k
WHERE d.date_key % 3 = 0;

-- =============================================================================
-- MATERIALIZED VIEWS (3) and PLAIN VIEW (1) — built after data exists
-- =============================================================================

CREATE MATERIALIZED VIEW reporting.monthly_revenue_mv AS
SELECT
    date_trunc('month', placed_at)::date AS month,
    currency,
    COUNT(*)                              AS order_count,
    SUM(net_amount)                       AS revenue
FROM fact.fact_sales
WHERE status IN ('paid','shipped','delivered')
GROUP BY 1, 2
ORDER BY 1 DESC;

CREATE UNIQUE INDEX monthly_revenue_mv_pk ON reporting.monthly_revenue_mv (month, currency);

CREATE MATERIALIZED VIEW reporting.top_products_mv AS
SELECT
    si.product_key,
    p.name              AS product_name,
    p.sku,
    SUM(si.quantity)    AS units_sold,
    SUM(si.line_amount) AS revenue,
    COUNT(DISTINCT s.customer_key) AS unique_buyers
FROM fact.fact_sales_items si
JOIN fact.fact_sales       s ON s.sale_id = si.sale_id
JOIN dim.dim_product       p ON p.product_key = si.product_key
WHERE s.status IN ('paid','shipped','delivered')
GROUP BY si.product_key, p.name, p.sku
ORDER BY revenue DESC;

CREATE UNIQUE INDEX top_products_mv_pk      ON reporting.top_products_mv (product_key);
CREATE INDEX        top_products_mv_rev_idx ON reporting.top_products_mv (revenue DESC);

CREATE MATERIALIZED VIEW reporting.customer_ltv_mv AS
SELECT
    c.customer_key,
    c.full_name,
    c.email,
    cs.code                     AS segment,
    COUNT(s.sale_id)            AS lifetime_orders,
    COALESCE(SUM(s.net_amount), 0) AS lifetime_revenue,
    MAX(s.placed_at)            AS last_order_at
FROM dim.dim_customer c
LEFT JOIN dim.dim_customer_segment cs ON cs.segment_key = c.segment_key
LEFT JOIN fact.fact_sales s
       ON s.customer_key = c.customer_key
      AND s.status IN ('paid','shipped','delivered')
WHERE c.is_current = TRUE
GROUP BY c.customer_key, c.full_name, c.email, cs.code;

CREATE UNIQUE INDEX customer_ltv_mv_pk      ON reporting.customer_ltv_mv (customer_key);
CREATE INDEX        customer_ltv_mv_rev_idx ON reporting.customer_ltv_mv (lifetime_revenue DESC);

REFRESH MATERIALIZED VIEW reporting.monthly_revenue_mv;
REFRESH MATERIALIZED VIEW reporting.top_products_mv;
REFRESH MATERIALIZED VIEW reporting.customer_ltv_mv;

-- Plain (non-materialized) view
CREATE VIEW reporting.executive_dashboard AS
SELECT
    (SELECT COUNT(*)               FROM fact.fact_sales WHERE status IN ('paid','shipped','delivered')) AS confirmed_orders,
    (SELECT SUM(net_amount)        FROM fact.fact_sales WHERE status IN ('paid','shipped','delivered')) AS total_revenue,
    (SELECT COUNT(DISTINCT customer_key) FROM fact.fact_sales)                                          AS active_customers,
    (SELECT COUNT(*)               FROM fact.fact_returns)                                              AS returns,
    (SELECT COUNT(*)               FROM fact.fact_subscriptions WHERE ended_on IS NULL)                 AS active_subscriptions,
    (SELECT AVG(csat_score)::numeric(4,2) FROM fact.fact_customer_support WHERE csat_score IS NOT NULL) AS avg_csat,
    NOW() AS as_of;

COMMENT ON VIEW reporting.executive_dashboard IS '管理层一屏指标（实时聚合）';

-- =============================================================================
-- POST-LOAD STATS
-- =============================================================================
ANALYZE;
