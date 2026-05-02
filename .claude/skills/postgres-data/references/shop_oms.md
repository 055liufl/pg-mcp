# shop_oms reference

> 中型电商订单管理系统(Order Management System),4 个 schema,19 表 + 1 视图 + 2 物化视图,约 8.5k 行业务数据。

## 1. 路由关键词

当用户的自然语言查询出现以下任一词汇,**应路由到 shop_oms**:

- 电商核心:`order` / `订单` / `cart` / `购物车` / `customer` / `客户` / `product` / `商品` / `SKU` / `variant` / `品牌` / `category` / `分类`
- 物流与发货:`shipment` / `发货` / `tracking` / `运单` / `carrier` / `快递`
- 计费与支付:`payment` / `支付` / `invoice` / `发票` / `refund` / `退款` / `coupon` / `优惠券` / `redemption`
- 会员:`loyalty` / `会员` / `tier` / `bronze` / `silver` / `gold` / `platinum`
- 字段词:`unit_price` / `lifetime_value` / `subtotal` / `discount`(在交易语境)

歧义判定:出现 "事实表/维度/data warehouse / fact_/dim_" 等 DW 词汇 → `analytics_dw`;出现 "blog/post/comment" → `mini_blog`。

## 2. 连接

```bash
docker exec -e PGPASSWORD=test pg-mcp-fixtures \
  psql -h localhost -U test -d shop_oms -c '<SQL>'
```

> 跨 schema 查询必须**全限定**(`schema.table`),不要依赖 search_path,以避免歧义。

## 3. Schema 总览

| schema | 用途 |
|---|---|
| `catalog` | 商品目录:品牌、分类、商品、SKU 变体、图片 |
| `users` | 用户:客户档案、地址、登录审计 |
| `sales` | 销售:购物车、订单、优惠券、发货 |
| `billing` | 账单:支付方式、支付、发票、退款 |

| 类型 | 全名 | 行数 | 描述 |
|---|---|---|---|
| 表 | `catalog.brands` | 10 | 品牌 |
| 表 | `catalog.categories` | 16 | 商品分类(自引用,父子树) |
| 表 | `catalog.products` | 85 | 商品主档(75 active + 5 draft + 5 discontinued) |
| 表 | `catalog.product_variants` | 255 | SKU 变体(每商品 ~3 个) |
| 表 | `catalog.product_images` | 170 | 商品图片(每商品 2 张) |
| 表 | `users.customers` | 200 | 客户主档 |
| 表 | `users.addresses` | 250 | 客户地址(含 billing/shipping) |
| 表 | `users.logins` | 1500 | 登录日志(成功+失败) |
| 表 | `sales.carts` | 60 | 购物车(含已放弃) |
| 表 | `sales.cart_items` | 120 | 购物车明细 |
| 表 | `sales.coupons` | 6 | 优惠券 |
| 表 | `sales.orders` | 800 | 订单主表 |
| 表 | `sales.order_items` | 2400 | 订单明细 |
| 表 | `sales.shipments` | 612 | 发货记录 |
| 表 | `sales.coupon_redemptions` | 144 | 优惠券核销记录 |
| 表 | `billing.payment_methods` | 266 | 客户保存的支付方式 |
| 表 | `billing.payments` | 774 | 支付流水 |
| 表 | `billing.invoices` | 715 | 发票(一单一票) |
| 表 | `billing.refunds` | 30 | 退款记录 |
| 视图 | `catalog.in_stock_products` | 75 | 在售且有库存的商品总览(预聚合) |
| 物化视图 | `sales.monthly_revenue` | 13 | 每月已确认收入 |
| 物化视图 | `sales.top_customers` | 200 | 按消费金额排序的客户榜 |

## 4. 枚举类型

```sql
CREATE TYPE catalog.product_status   AS ENUM ('draft', 'active', 'discontinued', 'out_of_stock');
CREATE TYPE sales.order_status       AS ENUM ('pending', 'paid', 'shipped', 'delivered', 'cancelled', 'refunded');
CREATE TYPE sales.shipment_status    AS ENUM ('pending', 'in_transit', 'delivered', 'lost', 'returned');
CREATE TYPE billing.payment_status   AS ENUM ('pending', 'authorized', 'captured', 'failed', 'refunded');
CREATE TYPE billing.refund_reason    AS ENUM ('customer_request', 'damaged', 'wrong_item', 'fraud', 'other');
CREATE TYPE users.address_type       AS ENUM ('billing', 'shipping', 'both');
CREATE TYPE users.loyalty_tier       AS ENUM ('bronze', 'silver', 'gold', 'platinum');
```

## 5. 复合类型

```sql
CREATE TYPE users.postal_address AS (
    line1       VARCHAR(120),
    line2       VARCHAR(120),
    city        VARCHAR(80),
    state       VARCHAR(80),
    postal_code VARCHAR(20),
    country     CHAR(2)
);
```

`users.addresses.address` 列即为该复合类型。访问字段用点表达式:`(a.address).city`、`(a.address).country`。

## 6. 表关键字段(摘要)

完整 DDL 见 `fixtures/shop_oms.sql`。下面只列影响 SQL 生成的关键点。

### `users.customers`
- `email CITEXT UNIQUE`(大小写不敏感唯一)
- `loyalty_tier users.loyalty_tier DEFAULT 'bronze'`
- `lifetime_value NUMERIC(12,2)`(已计算字段,**直接用**;不要再用订单聚合)
- `metadata JSONB`,常见键:`preferred_lang`(en/zh/es)、`segment`(new/returning/vip/churn-risk)
- `is_active BOOLEAN`、`marketing_opt_in BOOLEAN`

### `users.addresses`
- `address_type users.address_type`(billing/shipping/both)
- `address users.postal_address`(复合类型)
- 一客户可有多地址,`is_default BOOLEAN`

### `catalog.products`
- `status catalog.product_status`(draft/active/discontinued/out_of_stock)
- `brand_id INTEGER FK → catalog.brands(id)` (ON DELETE SET NULL)
- `category_id INTEGER NOT NULL FK → catalog.categories(id)` (ON DELETE RESTRICT)
- `base_price NUMERIC(10,2)`(基础价,变体可覆盖)
- `tags TEXT[]`(数组,用 `&&`、`@>` 检索)
- `attributes JSONB`,常见键:`season`、`gender`、`rating`

### `catalog.product_variants`
- `sku VARCHAR(40) UNIQUE`
- `price NUMERIC(10,2) NULL`(NULL 时回退到 `products.base_price`)
- `stock_qty INTEGER`(变体级库存)
- `is_active BOOLEAN`

### `sales.orders`
- `customer_id BIGINT NOT NULL FK → users.customers(id)`(ON DELETE RESTRICT)
- `coupon_id INTEGER FK → sales.coupons(id)`(ON DELETE SET NULL)
- `status sales.order_status`
- `subtotal / discount / tax / shipping_cost / total NUMERIC(12,2)`
- `currency CHAR(3) DEFAULT 'USD'` (ISO-4217)
- `placed_at / fulfilled_at / cancelled_at TIMESTAMPTZ`

### `sales.order_items`
- `line_total NUMERIC(12,2) GENERATED ALWAYS AS (quantity * unit_price) STORED`(直接用,不要 `quantity * unit_price`)
- `variant_id BIGINT NOT NULL FK → catalog.product_variants(id)` (ON DELETE RESTRICT)

### `sales.coupons`
- `discount_pct` 与 `discount_amt` 互斥(CHECK 强约束二选一)
- `max_uses` / `used_count`
- `valid_from / valid_until DATE`、`is_active BOOLEAN`

### `billing.payments`
- `status billing.payment_status` (`captured` 表示成功扣款)
- `amount NUMERIC(12,2)`(必正数)
- `method_id BIGINT FK → billing.payment_methods(id)`(SET NULL)

### `billing.refunds`
- `reason billing.refund_reason`(customer_request / damaged / wrong_item / fraud / other)
- `payment_id BIGINT NOT NULL FK → billing.payments(id) ON DELETE CASCADE`

## 7. 关键索引(影响查询计划)

- `catalog.products`:`(status)`、`(category_id)`、`(brand_id)`、`(tags)` GIN、`(attributes)` GIN、`(name)` GIN trgm、部分索引 `WHERE status='active'`
- `catalog.product_variants`:部分索引 `WHERE is_active = TRUE AND stock_qty > 0`
- `users.customers`:`(loyalty_tier)`、`lower(email::text)`、`(metadata)` GIN
- `sales.orders`:`(customer_id)`、`(status)`、`(placed_at DESC)`、部分 `WHERE status IN ('pending','paid','shipped')`、GIN `(status, placed_at)`
- `users.logins`:部分 `WHERE success = FALSE`(快查失败登录)

## 8. 视图与物化视图

### 视图 `catalog.in_stock_products`
```sql
SELECT p.id AS product_id, p.name, p.slug, p.brand_id, p.category_id, p.base_price,
       SUM(v.stock_qty) AS total_stock, COUNT(v.id) AS active_variant_count
FROM   catalog.products p
JOIN   catalog.product_variants v ON v.product_id = p.id
WHERE  p.status = 'active' AND v.is_active = TRUE
GROUP  BY p.id, p.name, p.slug, p.brand_id, p.category_id, p.base_price
HAVING SUM(v.stock_qty) > 0;
```

### 物化视图 `sales.monthly_revenue`
- 列:`month DATE`、`currency CHAR(3)`、`order_count`、`subtotal_sum`、`discount_sum`、`revenue`
- 仅含 `status IN ('paid','shipped','delivered')` 的订单
- 唯一索引 `(month, currency)`
- **直接查询时数据可能滞后**,要求实时数据时改用 `sales.orders` 现算

### 物化视图 `sales.top_customers`
- 列:`customer_id`、`full_name`、`loyalty_tier`、`order_count`、`total_spent`、`last_order_at`
- 排序:`total_spent DESC`
- 索引 `(total_spent DESC)`、唯一 `(customer_id)`
- 可与 `users.customers` JOIN 拿更多列

## 9. 业务约定

- `total = subtotal + tax + shipping_cost - discount`(已在样本数据中计算)
- `order_items.line_total` 是生成列,**不要重复算 `quantity * unit_price`**
- "已成交"通常指 `status IN ('paid','shipped','delivered')`(`pending` 算下单未付,`cancelled` 不计)
- "退款" 走 `billing.refunds.amount`,不要把 `orders.status='refunded'` 当作退款总额
- `users.customers.lifetime_value` 是已写好的累计字段,但**生产应当以 `sales.orders` 聚合为准**;在演示库里两者会差异,优先用聚合
- 优惠券用法:`discount_pct` 与 `discount_amt` 互斥;`coupon_redemptions` 是核销明细
- 订单号格式 `PO-XXXXXX`,客户邮箱格式 `customerN@example.com`,变体 SKU 格式 `SKU-XXXX-N`

## 10. 典型查询模板

### Q1:终生消费最高的前 10 名 platinum 客户
```sql
SELECT c.full_name, c.email, c.loyalty_tier, t.total_spent, t.order_count
FROM   sales.top_customers t
JOIN   users.customers c ON c.id = t.customer_id
WHERE  c.loyalty_tier = 'platinum'
ORDER  BY t.total_spent DESC
LIMIT  10;
```

### Q2:上月各退款原因的总金额
```sql
SELECT r.reason, SUM(r.amount) AS refund_total, COUNT(*) AS refund_count
FROM   billing.refunds r
WHERE  r.refunded_at >= date_trunc('month', NOW()) - INTERVAL '1 month'
  AND  r.refunded_at <  date_trunc('month', NOW())
GROUP  BY r.reason
ORDER  BY refund_total DESC;
```

### Q3:近 90 天退货率最高的前 10 个商品
```sql
WITH ordered AS (
  SELECT oi.variant_id, SUM(oi.quantity) AS qty
  FROM   sales.order_items oi
  JOIN   sales.orders o ON o.id = oi.order_id
  WHERE  o.placed_at >= NOW() - INTERVAL '90 days'
    AND  o.status IN ('paid','shipped','delivered','refunded')
  GROUP  BY oi.variant_id
),
refunded AS (
  SELECT oi.variant_id, SUM(oi.quantity) AS qty
  FROM   billing.refunds r
  JOIN   billing.payments p   ON p.id = r.payment_id
  JOIN   sales.orders     o   ON o.id = p.order_id
  JOIN   sales.order_items oi ON oi.order_id = o.id
  WHERE  r.refunded_at >= NOW() - INTERVAL '90 days'
  GROUP  BY oi.variant_id
)
SELECT v.sku, p.name AS product_name,
       COALESCE(r.qty, 0)::numeric / NULLIF(o.qty, 0) AS return_rate
FROM   ordered o
LEFT   JOIN refunded r              ON r.variant_id = o.variant_id
JOIN   catalog.product_variants v   ON v.id = o.variant_id
JOIN   catalog.products p           ON p.id = v.product_id
WHERE  o.qty >= 5
ORDER  BY return_rate DESC NULLS LAST
LIMIT  10;
```

### Q4:每月已确认订单数与收入(用物化视图)
```sql
SELECT month, currency, order_count, revenue
FROM   sales.monthly_revenue
ORDER  BY month DESC;
```

### Q5:库存不足 10 件的有效 SKU
```sql
SELECT v.sku, p.name, v.color, v.size, v.stock_qty
FROM   catalog.product_variants v
JOIN   catalog.products p ON p.id = v.product_id
WHERE  v.is_active = TRUE
  AND  v.stock_qty < 10
  AND  p.status = 'active'
ORDER  BY v.stock_qty ASC, p.name;
```

### Q6:优惠券使用率(已用 / 上限)
```sql
SELECT code, description, max_uses, used_count, is_active,
       CASE WHEN max_uses IS NULL OR max_uses = 0
            THEN NULL
            ELSE ROUND(used_count::numeric / max_uses, 4)
       END AS usage_rate
FROM   sales.coupons
ORDER  BY usage_rate DESC NULLS LAST;
```

### Q7:某个客户的所有订单(展开商品明细)
```sql
SELECT o.order_number, o.status, o.placed_at, o.total,
       v.sku, p.name AS product, oi.quantity, oi.unit_price, oi.line_total
FROM   sales.orders o
JOIN   sales.order_items oi          ON oi.order_id = o.id
JOIN   catalog.product_variants v    ON v.id = oi.variant_id
JOIN   catalog.products p            ON p.id = v.product_id
WHERE  o.customer_id = $CUSTOMER_ID
ORDER  BY o.placed_at DESC, o.order_number, oi.id;
```
*替换 `$CUSTOMER_ID` 为具体数字字面量(skill 内不要做 SQL 参数绑定)。*

### Q8:复合类型字段访问(取每个客户默认收货地址)
```sql
SELECT c.id, c.full_name,
       (a.address).line1, (a.address).city, (a.address).state, (a.address).country
FROM   users.customers c
JOIN   users.addresses a
       ON a.customer_id = c.id
      AND a.is_default  = TRUE
      AND a.address_type IN ('shipping','both')
ORDER  BY c.id;
```
