# analytics_dw reference

> 大型星型/雪花型数据仓库,5 schema、64 表 + 1 普通视图 + 3 物化视图,共 ~165k 行。表数 > 50,需检索式定位相关表,**不要把所有 64 张表都塞进 SQL 上下文**。

## 1. 路由关键词

当用户的自然语言查询出现以下任一词汇,**应路由到 analytics_dw**:

- DW 词汇:`fact_*` / `dim_*` / `staging` / `audit`(在 ETL 语境)/ `reporting`(报表)/ `cohort` / `funnel` / `KPI` / `LTV` / `attribution` / `mat_view` / `物化视图`
- 业务粒度:`session` / `web_event` / `impression` / `click` / `email_send` / `subscription` / `churn` / `loyalty_points` / `inventory_snapshot`
- 时间粒度:`monthly_revenue` / `weekly_active` / `daily_*` / `cohort_retention`
- 度量:`gross_amount` / `net_amount` / `cost_micros` / `mrr` / `cvr` / `cac` / `aov`
- 显式提到 "数据仓库" / "star schema" / "snowflake schema" / "DW" / "ETL"

歧义判定:出现 "blog/post/comment" → `mini_blog`;出现纯 OLTP 词汇 "购物车/订单状态机/退款记录" 但没有 DW/分析意图 → `shop_oms`。

## 2. 连接

```bash
docker exec -e PGPASSWORD=test pg-mcp-fixtures \
  psql -h localhost -U test -d analytics_dw -c '<SQL>'
```

> 全量 schema 巨大,生成 SQL 时**只引入相关表的列**,不要 `SELECT *` 多表 JOIN 出超大结果。

## 3. 大局观:5 个 schema

| schema | 用途 | 主要表 |
|---|---|---|
| `dim` | 维度表(SCD-2 + 静态参考) | `dim_customer`、`dim_product`、`dim_date`、`dim_country`、`dim_campaign`、`dim_channel` 等 |
| `fact` | 事实表(交易/事件级度量) | `fact_sales`、`fact_sales_items`、`fact_web_events`、`fact_returns`、`fact_payments`、`fact_subscriptions` 等 |
| `staging` | 原始 JSONB 落地区(ETL 入口) | `stg_orders_raw`、`stg_customers_raw` 等 |
| `audit` | 审计日志(登录、ETL、数据访问) | `user_login`、`admin_action`、`etl_runs`、`quality_checks` |
| `reporting` | 预聚合表 + 物化视图 + 普通视图 | `daily_sales`、`kpi_daily`、`monthly_revenue_mv`、`top_products_mv`、`customer_ltv_mv`、`executive_dashboard` |

## 4. 完整对象清单(行数)

### dim(22 表)
| 表 | 行数 | 关键列 |
|---|---|---|
| `dim.dim_date` | 365 | `date_key INTEGER PK`(YYYYMMDD)、`full_date DATE`、`year/quarter/month/day_of_week/is_weekend/is_holiday/fiscal_year` |
| `dim.dim_time_of_day` | 1440 | `time_key INTEGER PK`(HHMM)、`hour/minute/am_pm/daypart` |
| `dim.dim_country` | 10 | `country_key SERIAL PK`、`iso2/iso3/name/region/sub_region/currency/is_eu` |
| `dim.dim_region` | 14 | `country_key FK`、`code/name` |
| `dim.dim_city` | 15 | `region_key FK`、`name/population/timezone` |
| `dim.dim_currency` | 5 | `currency CHAR(3) PK`、`name/symbol/decimals` |
| `dim.dim_customer_segment` | 5 | `segment_key SERIAL PK`、`code dim.customer_segment_t UNIQUE` |
| `dim.dim_customer` | 2000 | `customer_key BIGSERIAL PK`、`customer_bk`、`email`、`gender dim.gender_t`、`segment_key FK`、`address dim.address_t`、`valid_from/valid_to/is_current`(SCD-2)、`metadata JSONB` |
| `dim.dim_brand` | 12 | `brand_key SERIAL PK`、`name UNIQUE`、`country_key FK`、`founded` |
| `dim.dim_supplier` | 50 | `supplier_key BIGSERIAL PK`、`supplier_bk`、`name`、`country_key`、`rating NUMERIC(3,2)`、`is_active` |
| `dim.dim_product_category` | 15 | `category_key SERIAL PK`、`parent_key FK`(自引用)、`path VARCHAR UNIQUE`(如 `Apparel/Mens Tops`) |
| `dim.dim_product` | 1000 | `product_key BIGSERIAL PK`、`product_bk`、`sku UNIQUE`、`category_key FK`、`brand_key FK`、`supplier_key FK`、`list_price`、`cost`、`is_active`、`attributes JSONB`、SCD-2 |
| `dim.dim_employee_role` | 10 | `role_key`、`name/department` |
| `dim.dim_employee` | 80 | `employee_key BIGSERIAL PK`、`employee_bk`、`full_name`、`email UNIQUE`、`role_key FK`、`hire_date`、`is_active` |
| `dim.dim_store` | 15 | `store_key`、`store_bk`、`name`、`city_key FK`、`opened_on`、`is_active` |
| `dim.dim_warehouse` | 6 | `warehouse_key`、`warehouse_bk`、`name`、`city_key FK`、`capacity_m3` |
| `dim.dim_channel` | 7 | `channel_key`、`code dim.channel_t UNIQUE`(web/mobile_ios/mobile_android/retail/partner/phone/email)、`description` |
| `dim.dim_campaign` | 30 | `campaign_key BIGSERIAL PK`、`campaign_bk`、`name`、`channel_key FK`、`started_on/ended_on DATE`、`budget` |
| `dim.dim_payment_method` | 11 | `pm_key`、`name UNIQUE`(Visa/MasterCard/Amex/PayPal/Apple Pay/...)、`is_card BOOLEAN` |
| `dim.dim_device` | 10 | `device_key`、`type`(desktop/mobile/tablet/tv)、`vendor/model` |
| `dim.dim_browser` | 10 | `browser_key`、`name/major_version` |
| `dim.dim_os` | 10 | `os_key`、`family/version` |

### fact(15 表)
| 表 | 行数 | 关键列 |
|---|---|---|
| `fact.fact_sales` | 10000 | `sale_id BIGSERIAL PK`、`order_bk UNIQUE`、`date_key/time_key/customer_key/channel_key/store_key/employee_key/campaign_key/pm_key`(全部 FK)、`currency`、`status fact.order_status_t`、`gross_amount/discount_amount/tax_amount/shipping_amount/net_amount NUMERIC(14,2)`、`item_count`、`placed_at/delivered_at` |
| `fact.fact_sales_items` | 30000 | `sales_item_id`、`sale_id FK`、`product_key FK`、`quantity`、`unit_price`、`line_amount`(GENERATED `quantity*unit_price`)、`margin` |
| `fact.fact_returns` | 252 | `return_id`、`sale_id FK`、`sales_item_id FK`、`return_date_key FK`、`quantity`、`refund_amount`、`reason fact.return_reason_t` |
| `fact.fact_payments` | 9667 | `payment_id`、`sale_id FK`、`pm_key`、`amount/currency`、`status fact.payment_status_t`、`gateway_ref`、`paid_at` |
| `fact.fact_shipments` | 7648 | `shipment_id`、`sale_id FK`、`warehouse_key FK`、`carrier`、`tracking_no`、`status fact.shipment_status_t`、`shipped_at/delivered_at` |
| `fact.fact_inventory_snapshot` | 3000 | `(date_key, product_key, warehouse_key)` 唯一,`on_hand_qty/reserved_qty/backorder_qty` |
| `fact.fact_web_events` | 50000 | `event_id`、`customer_key`(可空,匿名访客)、`session_id UUID`、`date_key/time_key`、`event_type fact.event_type_t`、`page_url/referrer`、`device_key/browser_key/os_key`、`properties JSONB` |
| `fact.fact_ad_impressions` | 30000 | `impression_id`、`campaign_key FK`、`customer_key`、`date_key`、`cost_micros BIGINT`、`placement` |
| `fact.fact_ad_clicks` | 3000 | `click_id`、`impression_id FK`(可空)、`campaign_key`、`customer_key`、`date_key`、`cost_micros` |
| `fact.fact_email_sends` | 10000 | `send_id`、`customer_key`、`campaign_key`、`template_code`、`sent_at`、`delivered BOOLEAN` |
| `fact.fact_email_opens` | 3333 | `open_id`、`send_id FK`、`opened_at`、`user_agent` |
| `fact.fact_subscriptions` | 500 | `subscription_id`、`customer_key`、`plan_code`(basic/pro/team/enterprise)、`started_on/ended_on`、`mrr NUMERIC(10,2)`、`is_active`(GENERATED `ended_on IS NULL`) |
| `fact.fact_churn_events` | 125 | `churn_id`、`subscription_id FK`、`churned_on`、`reason`、`is_voluntary` |
| `fact.fact_loyalty_points` | 4508 | `txn_id`、`customer_key`、`date_key`、`points_delta INTEGER`、`reason VARCHAR`、`sale_id FK` |
| `fact.fact_customer_support` | 1500 | `ticket_id`、`customer_key`、`employee_key`、`opened_at/closed_at`、`channel_key`、`sentiment_score NUMERIC(4,3)`、`csat_score SMALLINT(1-5)` |

### staging(12 表,JSONB payload)
| 表 | 行数 | 备注 |
|---|---|---|
| `staging.stg_orders_raw` | 300 | `payload JSONB`、`source`(shopify/woo/custom_api)、`load_status staging.load_status_t` |
| `staging.stg_customers_raw` | 200 | |
| `staging.stg_products_raw` | 200 | |
| `staging.stg_payments_raw` | 150 | |
| `staging.stg_shipments_raw` | 150 | |
| `staging.stg_clicks_raw` | 200 | |
| `staging.stg_impressions_raw` | 200 | |
| `staging.stg_emails_raw` | 100 | |
| `staging.stg_warehouse_movements` | 300 | |
| `staging.stg_returns_raw` | 100 | |
| `staging.stg_loyalty_raw` | 200 | |
| `staging.stg_support_tickets` | 150 | |

### audit(8 表)
| 表 | 行数 | 关键列 |
|---|---|---|
| `audit.user_login` | 5000 | `customer_key FK`、`success BOOLEAN`、`ip INET`、`user_agent` |
| `audit.admin_action` | 800 | `employee_key FK`、`action`、`target`、`severity audit.severity_t` |
| `audit.data_access` | 1500 | `employee_key`、`schema_name/table_name`、`rows_read` |
| `audit.etl_runs` | 500 | `pipeline`、`started_at/finished_at`、`rows_in/rows_out`、`status staging.load_status_t` |
| `audit.quality_checks` | 600 | `pipeline`、`check_name`、`passed BOOLEAN`、`severity`、`details JSONB` |
| `audit.pipeline_failures` | 100 | `pipeline`、`failed_at`、`severity`、`error_text` |
| `audit.permissions_change` | 50 | `employee_key`、`role_before/role_after` |
| `audit.export_jobs` | 100 | `employee_key`、`job_name`、`rows_exported`、`file_path` |

### reporting(7 表 + 3 MV + 1 视图)
| 对象 | 类型 | 行数 |
|---|---|---|
| `reporting.daily_sales` | 表 | 365(`date_key PK`、`order_count/item_count/revenue/tax/discount`) |
| `reporting.weekly_active_users` | 表 | 53 |
| `reporting.funnel` | 表 | 12 |
| `reporting.cohort_retention` | 表 | 89 |
| `reporting.marketing_attribution` | 表 | 2000 |
| `reporting.inventory_alerts` | 表 | 200 |
| `reporting.kpi_daily` | 表 | 732 |
| `reporting.monthly_revenue_mv` | 物化视图 | 12(月维度收入) |
| `reporting.top_products_mv` | 物化视图 | 1000(商品销售榜) |
| `reporting.customer_ltv_mv` | 物化视图 | 2000(客户终身价值) |
| `reporting.executive_dashboard` | 视图 | 1(高管一屏指标) |

## 5. 枚举类型

```sql
-- dim
dim.gender_t            -- male/female/nonbinary/unknown
dim.customer_segment_t  -- new/active/at_risk/churned/vip
dim.channel_t           -- web/mobile_ios/mobile_android/retail/partner/phone/email
-- fact
fact.order_status_t     -- pending/paid/shipped/delivered/cancelled/refunded
fact.return_reason_t    -- damaged/wrong_item/customer_changed_mind/too_late/quality/other
fact.event_type_t       -- page_view/add_to_cart/remove_from_cart/checkout/purchase/signup/login/search/share
fact.payment_status_t   -- pending/authorized/captured/failed/refunded
fact.shipment_status_t  -- label_created/in_transit/out_for_delivery/delivered/exception/returned
-- staging / audit / reporting
staging.load_status_t   -- queued/processing/done/failed
audit.severity_t        -- debug/info/warn/error/critical
reporting.kpi_trend_t   -- up/flat/down
```

## 6. 复合类型

```sql
dim.address_t  AS (line1 VARCHAR, line2 VARCHAR, city VARCHAR, state VARCHAR, postal_code VARCHAR, country CHAR(2))
fact.money_t   AS (amount NUMERIC(14,2), currency CHAR(3))
```

`dim.dim_customer.address` 即为 `dim.address_t`。访问字段:`(c.address).city`。

## 7. 关键索引(影响查询计划)

- `fact.fact_sales`:`(date_key)`、`(customer_key)`、`(status)`、`(placed_at DESC)`、部分 `(date_key, customer_key) WHERE status IN ('paid','shipped','delivered')`
- `fact.fact_web_events`:`(customer_key)`、`(session_id)`、`(event_type)`、`(properties)` GIN、`(occurred_at DESC)`
- `dim.dim_customer`:`(lower(email))`、`(segment_key)`、部分 `(customer_bk) WHERE is_current = TRUE`、`(metadata)` GIN
- `dim.dim_product`:`(name)` GIN trgm、`(category_key)`、`(attributes)` GIN、部分 `(sku) WHERE is_active=TRUE AND is_current=TRUE`
- `audit.user_login`:`(occurred_at DESC)`、部分 `WHERE success = FALSE`

## 8. 业务约定

- **日期过滤优先用 `date_key`**(整数 YYYYMMDD),性能优于 `placed_at DATE 比较`,例:`s.date_key BETWEEN 20250301 AND 20250331`。但当跨月、跨年时也可以用 `placed_at`。
- **"已成交订单" = `status IN ('paid','shipped','delivered')`**,不含 cancelled / pending / refunded。
- **SCD-2 维度** (`dim_customer` / `dim_product`):`is_current = TRUE` 拿当前版本;事实表里的 `customer_key` / `product_key` 已经指向加载时的版本(可能不是 current)。**做客户/商品维度归一化时,需要在 dim 表上加 `is_current = TRUE`**,否则会重复。
- **业务键 vs 代理键**:`*_bk` 列(例如 `customer_bk`、`product_bk`、`order_bk`)是上游系统主键,通常以 `CUST-/PRD-/ORD-` 等前缀。代理键 `*_key` 是 DW 内部 BIGSERIAL,**JOIN 必须用代理键**。
- **货币**:本演示库订单全部 `USD`,但 `dim_currency` 仍存了 5 种;不要假设非 USD 行存在。
- **`fact_loyalty_points.points_delta`** 可正可负(累积型快照),"用户当前积分余额" = `SUM(points_delta) GROUP BY customer_key`。
- **物化视图刷新**:见 SQL 文件中的 `REFRESH MATERIALIZED VIEW`,数据可能略滞后于事实表。生产中以 fact 现算为准,做面板/排行榜时用 MV。

## 9. 典型查询模板

### Q1:今年(2025)收入最高的前 20 个商品
```sql
-- 用预先聚合好的 MV
SELECT product_name, sku, units_sold, revenue, unique_buyers
FROM   reporting.top_products_mv
ORDER  BY revenue DESC
LIMIT  20;
```

### Q2:三月份各设备类型的日均会话数
```sql
SELECT d.type AS device_type,
       AVG(daily_sessions)::numeric(10,2) AS avg_daily_sessions
FROM (
  SELECT we.date_key,
         dev.type,
         COUNT(DISTINCT we.session_id) AS daily_sessions
  FROM   fact.fact_web_events we
  JOIN   dim.dim_device dev ON dev.device_key = we.device_key
  WHERE  we.date_key BETWEEN 20250301 AND 20250331
  GROUP  BY we.date_key, dev.type
) t
JOIN dim.dim_device d ON d.type = t.type
GROUP  BY d.type
ORDER  BY avg_daily_sessions DESC;
```

### Q3:从 add_to_cart → purchase 的周转化率
```sql
WITH weekly AS (
  SELECT date_trunc('week', occurred_at)::date AS wk,
         event_type,
         COUNT(DISTINCT session_id) AS sessions
  FROM   fact.fact_web_events
  WHERE  event_type IN ('add_to_cart','purchase')
  GROUP  BY 1, 2
)
SELECT wk,
       MAX(sessions) FILTER (WHERE event_type = 'add_to_cart') AS atc_sessions,
       MAX(sessions) FILTER (WHERE event_type = 'purchase')    AS purchase_sessions,
       ROUND(
         MAX(sessions) FILTER (WHERE event_type = 'purchase')::numeric
         / NULLIF(MAX(sessions) FILTER (WHERE event_type = 'add_to_cart'), 0),
         4
       ) AS atc_to_purchase_cvr
FROM   weekly
GROUP  BY wk
ORDER  BY wk;
```

### Q4:上季度(2025-Q1)按 ROI 排序的广告投放
```sql
WITH spend AS (
  SELECT campaign_key, SUM(cost_micros)/1e6 AS total_spend_usd
  FROM   fact.fact_ad_impressions
  WHERE  date_key BETWEEN 20250101 AND 20250331
  GROUP  BY campaign_key
),
attributed AS (
  SELECT ma.campaign_key,
         SUM(s.net_amount * ma.weight) AS attributed_revenue
  FROM   reporting.marketing_attribution ma
  JOIN   fact.fact_sales s ON s.sale_id = ma.sale_id
  WHERE  s.date_key BETWEEN 20250101 AND 20250331
    AND  s.status IN ('paid','shipped','delivered')
  GROUP  BY ma.campaign_key
)
SELECT c.name AS campaign,
       sp.total_spend_usd,
       a.attributed_revenue,
       (a.attributed_revenue / NULLIF(sp.total_spend_usd, 0))::numeric(10,2) AS roi
FROM   dim.dim_campaign c
JOIN   spend sp      ON sp.campaign_key = c.campaign_key
LEFT   JOIN attributed a ON a.campaign_key = c.campaign_key
ORDER  BY roi DESC NULLS LAST
LIMIT  10;
```

### Q5:VIP 客户终身价值排行
```sql
SELECT clv.customer_key, clv.full_name, clv.email,
       clv.lifetime_orders, clv.lifetime_revenue, clv.last_order_at
FROM   reporting.customer_ltv_mv clv
WHERE  clv.segment = 'vip'
ORDER  BY clv.lifetime_revenue DESC
LIMIT  20;
```

### Q6:某月份订阅 churn 数与原因分布
```sql
SELECT date_trunc('month', churned_on)::date AS month,
       reason,
       is_voluntary,
       COUNT(*) AS churn_count
FROM   fact.fact_churn_events
WHERE  churned_on >= DATE '2025-01-01'
  AND  churned_on <  DATE '2025-04-01'
GROUP  BY 1, 2, 3
ORDER  BY month, churn_count DESC;
```

### Q7:连续 7 天没登录成功的客户
```sql
SELECT c.customer_key, c.email, c.full_name,
       MAX(l.occurred_at) FILTER (WHERE l.success) AS last_success_login
FROM   dim.dim_customer c
LEFT   JOIN audit.user_login l ON l.customer_key = c.customer_key
WHERE  c.is_current = TRUE
GROUP  BY c.customer_key, c.email, c.full_name
HAVING COALESCE(MAX(l.occurred_at) FILTER (WHERE l.success), '1970-01-01'::timestamptz)
       < NOW() - INTERVAL '7 days'
ORDER  BY last_success_login NULLS FIRST
LIMIT  50;
```

### Q8:库存告警(未解决,按严重度)
```sql
SELECT a.alert_id,
       p.name AS product, p.sku,
       w.name AS warehouse,
       a.alert_level, a.threshold, a.on_hand_qty, a.triggered_at
FROM   reporting.inventory_alerts a
JOIN   dim.dim_product   p ON p.product_key   = a.product_key
JOIN   dim.dim_warehouse w ON w.warehouse_key = a.warehouse_key
WHERE  a.resolved_at IS NULL
ORDER  BY CASE a.alert_level
            WHEN 'critical' THEN 1
            WHEN 'error'    THEN 2
            ELSE 3
          END,
          a.triggered_at DESC;
```

### Q9:管理层一屏指标(直接查视图)
```sql
SELECT * FROM reporting.executive_dashboard;
```
