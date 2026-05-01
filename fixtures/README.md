# pg-mcp / fixtures

Three reproducible PostgreSQL test databases used to exercise pg-mcp end-to-end.
Each database is intentionally a different size class to cover the full range
of behaviors described in [PRD-0001](../specs/0001-pg-mcp-prd.md):

| Database        | Scale | Schemas | Tables | Views (m / plain) | Enums | Composite | Indexes | Rows (≈) | Purpose                                              |
|-----------------|-------|---------|--------|-------------------|-------|-----------|---------|----------|------------------------------------------------------|
| `mini_blog`     | S     | 1 (`public`) | 6  | 0 / 1             | 1     | 0         | 18      | 200      | Full-context schema injection (≤ 50 tables)          |
| `shop_oms`      | M     | 4 (`catalog/sales/billing/users`) | 19 | 2 / 1 | 7 | 1 | 61 | 8,300 | Multi-schema, mat-views, composite type, partial idx |
| `analytics_dw`  | L     | 5 (`dim/fact/staging/audit/reporting`) | 64 | 3 / 1 | 11 | 2 | 135 | 165,000+ | Triggers schema retrieval (>50 tables)               |

The actual table breakdown of `analytics_dw`: 22 dim + 15 fact + 12 staging +
8 audit + 7 reporting = **64 tables**, plus 3 materialized views (counted
separately from `pg_tables` but visible to pg-mcp's discovery). Total relations
the schema-discovery layer sees is **67**, comfortably above the default
`SCHEMA_MAX_TABLES_FOR_FULL_CONTEXT = 50`.

## Files

| File | What it builds |
|---|---|
| `mini_blog.sql`     | Drops/recreates all objects + ~200 rows of seed data |
| `shop_oms.sql`      | Same, with 4 schemas and ~8k rows |
| `analytics_dw.sql`  | Same, with 5 schemas and ~165k rows of generated data |
| `Makefile`          | Reproducible build/verify/clean targets |
| `README.md`         | This file |

Every SQL file is **self-contained and idempotent** — re-running it drops the
existing schema(s) and rebuilds from scratch. The Makefile additionally drops
and re-creates the database itself before running each load.

## Quick start (local PostgreSQL)

```bash
# Default: local Postgres on localhost:5432, user=postgres
PGPASSWORD=postgres make all

# Pick what you build
make mini_blog
make shop_oms
make analytics_dw

# Sanity check: row counts + schema-object counts
make verify

# Tear down
make clean
```

Connection knobs (override via env or `make X=Y`):

```
PG_HOST=localhost PG_PORT=5432 PG_USER=postgres PGPASSWORD=
ADMIN_DB=postgres                 # the DB to issue CREATE/DROP DATABASE against
DB_MINI=mini_blog DB_SHOP=shop_oms DB_DW=analytics_dw
```

Example: build into a remote server:

```bash
make all PG_HOST=10.0.0.5 PG_PORT=5432 PG_USER=admin PGPASSWORD=secret
```

## Quick start (Docker, no local psql needed)

If you do not have `psql` installed, the Makefile can spin up a throwaway
`postgres:16-alpine` container and load the fixtures into it:

```bash
make docker-up                    # starts container `pg-mcp-fixtures` on :5433
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

(Yes, the override is verbose — wrap it in a shell function if you build
fixtures often.)

## Pointing pg-mcp at the fixtures

Set the connection environment for `pg-mcp` so it auto-discovers all three
databases:

```bash
export PG_HOST=localhost
export PG_PORT=5432
export PG_USER=postgres
export PG_PASSWORD=...
export PG_EXCLUDE_DATABASES=template0,template1,postgres
# Optional: limit discovery to fixtures only
export PG_DATABASES=mini_blog,shop_oms,analytics_dw

pg-mcp --transport stdio
```

`mini_blog` and `shop_oms` will be served via the full-context path; once
`analytics_dw` crosses the 50-table threshold, the retrieval-based path kicks
in automatically (see PRD §3.1 "Large schema handling").

## Schema highlights — what each DB is good for testing

**`mini_blog`** — small, intuitive vocabulary:
- `users / posts / comments / tags / post_tags / audit_log`
- 1 enum (`post_status`), 1 view (`published_posts`), 1 GIN index on JSONB,
  1 partial index, 1 unique-on-active-only index, generated column on `posts`
- Natural-language smoke tests: *"how many comments has Alice published?"*,
  *"top 5 posts by views in the last 30 days"*

**`shop_oms`** — multi-schema OMS with realistic FK depth:
- `catalog.products / product_variants / product_images / brands / categories`
- `users.customers / addresses / logins` (composite-typed `postal_address`)
- `sales.orders / order_items / coupons / shipments / coupon_redemptions / carts / cart_items`
- `billing.payment_methods / payments / invoices / refunds`
- 7 enums, 1 composite type, GIN/trigram/partial/expression indexes, generated columns
- 2 mat views (`sales.monthly_revenue`, `sales.top_customers`)
- Natural-language tests: *"which products had the highest return rate last quarter?"*,
  *"top 10 platinum customers by lifetime spend"*, *"refund total per reason last month"*

**`analytics_dw`** — star/snowflake DW that intentionally crosses the
retrieval threshold:
- 22 `dim_*` tables (date, time-of-day, customer, product, brand, supplier, country, region, city, etc.)
- 15 `fact_*` tables (sales, sales_items, returns, payments, shipments, web_events @ 50k, ad_impressions @ 30k, ad_clicks, email_sends, subscriptions, churn, loyalty, support, inventory)
- 12 staging tables, 8 audit tables, 7 reporting tables, 3 materialized views, 1 plain view
- 11 enums, 2 composite types, 135 indexes (btree, partial, expression, GIN, trigram)
- ~165k rows total — heavy enough to make EXPLAIN/EXPLAIN-style queries meaningful, light enough to load in ~30s
- Natural-language tests: *"top 20 products by revenue this year"*,
  *"weekly conversion rate from add_to_cart to purchase"*,
  *"which campaigns delivered the highest ROI last quarter?"*,
  *"daily sessions per device type in March"*

## How the SQL files were validated

Each fixture file was loaded into a clean `postgres:16-alpine` instance and
verified for:

1. Successful execution end-to-end (no errors, all `INSERT ... CASCADE` paths
   reach a consistent end state).
2. Expected counts of tables / views / matviews / indexes / enums / composite
   types per schema.
3. Sane data shape (FKs satisfied, materialized views populated, generated
   columns computed).

Run `make verify` after `make all` and confirm the printed counts match the
table at the top of this file.

## Notes / gotchas

- `analytics_dw.sql` requires `pgcrypto` (used by `gen_random_bytes`) plus
  `pg_trgm` and `btree_gin`. All three are bundled with stock PostgreSQL since
  13+; the SQL file uses `CREATE EXTENSION IF NOT EXISTS`.
- `shop_oms.sql` requires `pg_trgm`, `btree_gin`, and `citext`.
- The Makefile assumes the connecting user has permission to `DROP DATABASE`
  on the listed databases. With a stock `postgres` superuser this is fine; on
  managed instances you may need to pre-create empty DBs and skip the DROP/CREATE
  lines.
- All fixtures are deterministic given a fixed seed in `random()`'s state
  except where intentionally randomized (ratings, sentiment, ldp deltas). Row
  counts, however, are stable across runs.
