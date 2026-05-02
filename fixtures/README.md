# pg-mcp / fixtures

三个可复现的 PostgreSQL 测试数据库，用于端到端验证 pg-mcp。
每个数据库刻意设计为不同的规模等级，以覆盖 [PRD-0001](../specs/0001-pg-mcp-prd.md) 中描述的全部行为范围：

| 数据库        | 规模 | 模式 | 表数 | 视图（物化/普通） | 枚举 | 复合类型 | 索引 | 行数（约） | 用途                                              |
|---------------|------|------|------|-------------------|------|----------|------|------------|---------------------------------------------------|
| `mini_blog`   | 小   | 1 (`public`) | 6  | 0 / 1             | 1    | 0        | 18   | 200        | 全量 schema 注入（≤ 50 表）                        |
| `shop_oms`    | 中   | 4 (`catalog`/`sales`/`billing`/`users`) | 19 | 2 / 1 | 7 | 1 | 61 | 8,300 | 多 schema、物化视图、复合类型、部分索引 |
| `analytics_dw`| 大   | 5 (`dim`/`fact`/`staging`/`audit`/`reporting`) | 64 | 3 / 1 | 11 | 2 | 135 | 165,000+ | 触发 schema 检索（>50 表）        |

`analytics_dw` 的实际表分布：22 维表 + 15 事实表 + 12 临时表 +
8 审计表 + 7 报表表 = **64 张表**，另有 3 个物化视图（在
`pg_tables` 中单独计数，但 pg-mcp 的 schema 发现层可见）。schema 发现层可见的总关系数为 **67**， comfortably 高于默认的
`SCHEMA_MAX_TABLES_FOR_FULL_CONTEXT = 50`。

## 文件

| 文件 | 构建内容 |
|---|---|
| `mini_blog.sql`     | 删除/重建所有对象 + ~200 行种子数据 |
| `shop_oms.sql`      | 同上，含 4 个 schema 和 ~8k 行数据 |
| `analytics_dw.sql`  | 同上，含 5 个 schema 和 ~165k 行生成数据 |
| `Makefile`          | 可复现的构建/验证/清理目标 |
| `README.md`         | 本文件 |

每个 SQL 文件都是**自包含且幂等的** —— 重新运行时会删除现有 schema 并从头重建。Makefile 还会在每次加载前删除并重新创建数据库本身。

## 快速开始（本地 PostgreSQL）

```bash
# 默认：本地 Postgres，localhost:5432，用户=postgres
PGPASSWORD=postgres make all

# 选择要构建的数据库
make mini_blog
make shop_oms
make analytics_dw

# 合理性检查：行数 + schema 对象数
make verify

# 清理
make clean
```

连接参数（通过环境变量或 `make X=Y` 覆盖）：

```
PG_HOST=localhost PG_PORT=5432 PG_USER=postgres PGPASSWORD=
ADMIN_DB=postgres                 # 用于执行 CREATE/DROP DATABASE 的数据库
DB_MINI=mini_blog DB_SHOP=shop_oms DB_DW=analytics_dw
```

示例：构建到远程服务器：

```bash
make all PG_HOST=10.0.0.5 PG_PORT=5432 PG_USER=admin PGPASSWORD=secret
```

## 快速开始（Docker，无需本地 psql）

如果你没有安装 `psql`，Makefile 可以启动一个临时的
`postgres:16-alpine` 容器并加载 fixtures：

```bash
make docker-up                    # 启动容器 `pg-mcp-fixtures`，监听 :5433
make all \
  PSQL='docker exec -i pg-mcp-fixtures psql' \
  SUPER_PSQL='docker exec pg-mcp-fixtures psql' \
  PG_HOST=localhost PG_PORT=5432 PG_USER=test PGPASSWORD=test
make verify \
  PSQL='docker exec -i pg-mcp-fixtures psql' \
  SUPER_PSQL='docker exec pg-mcp-fixtures psql' \
  PG_HOST=localhost PG_PORT=5432 PG_USER=test PGPASSWORD=test
make docker-down
```

（是的，覆盖参数很冗长 —— 如果你经常构建 fixtures，可以把它包成一个 shell 函数。）

## 将 pg-mcp 指向 fixtures

设置 pg-mcp 的连接环境，让它自动发现三个数据库：

```bash
export PG_HOST=localhost
export PG_PORT=5432
export PG_USER=postgres
export PG_PASSWORD=...
export PG_EXCLUDE_DATABASES=template0,template1,postgres
# 可选：仅限制发现 fixtures
export PG_DATABASES=mini_blog,shop_oms,analytics_dw

pg-mcp --transport stdio
```

`mini_blog` 和 `shop_oms` 将走全量 schema 路径；一旦
`analytics_dw` 超过 50 表阈值，检索路径将自动启用（参见 PRD §3.1 "大 schema 处理"）。

## Schema 亮点 —— 每个数据库适合测试什么

**`mini_blog`** —— 小规模、直观的词汇：
- `users / posts / comments / tags / post_tags / audit_log`
- 1 个枚举（`post_status`）、1 个视图（`published_posts`）、1 个 JSONB 上的 GIN 索引、
  1 个部分索引、1 个仅活跃唯一索引、`posts` 上的生成列
- 自然语言冒烟测试：*"Alice 发表了多少条评论？"*、
  *"过去 30 天浏览量最高的 5 篇文章"*

**`shop_oms`** —— 多 schema 电商订单系统，具有真实的外键深度：
- `catalog.products / product_variants / product_images / brands / categories`
- `users.customers / addresses / logins`（复合类型 `postal_address`）
- `sales.orders / order_items / coupons / shipments / coupon_redemptions / carts / cart_items`
- `billing.payment_methods / payments / invoices / refunds`
- 7 个枚举、1 个复合类型、GIN/trigram/部分/表达式索引、生成列
- 2 个物化视图（`sales.monthly_revenue`、`sales.top_customers`）
- 自然语言测试：*"上季度退货率最高的商品是哪些？"*、
  *"终生消费最高的前 10 名白金客户"*、*"上月各退款原因的总金额"*

**`analytics_dw`** —— 星型/雪花型数据仓库，刻意跨越检索阈值：
- 22 张 `dim_*` 表（日期、时段、客户、商品、品牌、供应商、国家、地区、城市等）
- 15 张 `fact_*` 表（销售、销售明细、退货、付款、发货、web_events @ 50k、ad_impressions @ 30k、ad_clicks、email_sends、订阅、流失、忠诚度、客服、库存）
- 12 张临时表、8 张审计表、7 张报表表、3 个物化视图、1 个普通视图
- 11 个枚举、2 个复合类型、135 个索引（btree、部分、表达式、GIN、trigram）
- ~165k 行总计 —— 足够让 EXPLAIN/EXPLAIN 风格查询有意义，又足够轻量可在 ~30s 内加载
- 自然语言测试：*"今年收入最高的前 20 个商品"*、
  *"从 add_to_cart 到 purchase 的周转化率"*、
  *"上季度哪些广告投放的 ROI 最高？"*、
  *"三月各设备类型的日会话数"*

## SQL 文件如何验证

每个 fixture 文件被加载到一个干净的 `postgres:16-alpine` 实例中，并验证：

1. 端到端成功执行（无错误，所有 `INSERT ... CASCADE` 路径达到一致终态）。
2. 每个 schema 的表 / 视图 / 物化视图 / 索引 / 枚举 / 复合类型的预期数量。
3. 数据形状合理（外键满足、物化视图已填充、生成列已计算）。

在 `make all` 后运行 `make verify`，确认打印的数量与本文件顶部的表格一致。

## 注意事项 / 陷阱

- `analytics_dw.sql` 需要 `pgcrypto`（由 `gen_random_bytes` 使用）加上
  `pg_trgm` 和 `btree_gin`。三个扩展都随 PostgreSQL 13+ 捆绑；SQL 文件使用 `CREATE EXTENSION IF NOT EXISTS`。
- `shop_oms.sql` 需要 `pg_trgm`、`btree_gin` 和 `citext`。
- Makefile 假设连接用户有权在列出的数据库上执行 `DROP DATABASE`。使用默认的 `postgres` 超级用户时没问题；在托管实例上你可能需要预先创建空数据库并跳过 DROP/CREATE 行。
- 所有 fixtures 在给定固定 `random()` 种子状态下是确定性的，除非有意随机化的部分（评分、情感、ldp 增量）。然而，行数在各次运行中是稳定的。
