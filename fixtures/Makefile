# =============================================================================
# pg-mcp / fixtures — Makefile
#
# Reproducibly (re)builds the three test PostgreSQL databases:
#
#   mini_blog      — small (≈6 tables, ≈150 rows)
#   shop_oms       — medium (4 schemas, ≈22 tables, ≈8k rows)
#   analytics_dw   — large (5 schemas, 67 tables/views, ≈170k rows; >50 tables
#                          triggers pg-mcp's schema retrieval path)
#
# Default mode talks to a local/remote PostgreSQL via libpq env vars.
# A "docker" mode is provided to spin up a throwaway Postgres container
# (postgres:16-alpine) and load the fixtures into it — useful on a fresh
# machine without psql installed.
#
# QUICK START (local Postgres):
#     make all                          # build all 3 databases
#     make mini_blog                    # build just one
#     make verify                       # row-count + schema sanity
#     make clean                        # drop all 3 databases
#
# QUICK START (docker mode):
#     make docker-up                    # start postgres:16-alpine on :5433
#     make all PSQL='docker exec -i pg-mcp-fixtures psql' \
#              SUPER_PSQL='docker exec pg-mcp-fixtures psql' \
#              PG_USER=test PGPASSWORD=test
#     make docker-down
# =============================================================================

# ---------- connection parameters (override via env or `make X=Y`) -----------
PG_HOST       ?= localhost
PG_PORT       ?= 5432
PG_USER       ?= postgres
PGPASSWORD    ?=
ADMIN_DB      ?= postgres

export PGPASSWORD

# Base flags — used by every psql invocation.
PSQL_FLAGS    := -h $(PG_HOST) -p $(PG_PORT) -U $(PG_USER) -v ON_ERROR_STOP=1 -X -q

# `psql` for loading SQL files (reads from stdin), plus a separate alias for
# administrative one-shots (CREATE/DROP DATABASE) that talks to ADMIN_DB.
PSQL          ?= psql
SUPER_PSQL    ?= psql

PSQL_LOAD     := $(PSQL) $(PSQL_FLAGS)
PSQL_ADMIN    := $(SUPER_PSQL) $(PSQL_FLAGS) -d $(ADMIN_DB)

# Database names (overridable so multiple sets can coexist on one server).
DB_MINI       ?= mini_blog
DB_SHOP       ?= shop_oms
DB_DW         ?= analytics_dw

# Docker-mode container settings.
DOCKER_NAME   ?= pg-mcp-fixtures
DOCKER_IMAGE  ?= postgres:16-alpine
DOCKER_PORT   ?= 5433

FIXTURES_DIR  := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# =============================================================================
# Top-level targets
# =============================================================================
.PHONY: help all mini_blog shop_oms analytics_dw \
        verify clean check-conn \
        docker-up docker-down docker-logs

help: ## Show this help
	@awk 'BEGIN{FS=":.*##"; printf "Available targets:\n\n"} /^[a-zA-Z_.-]+:.*##/{printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

all: mini_blog shop_oms analytics_dw verify ## Build all 3 databases and verify

# =============================================================================
# Per-database build targets
# =============================================================================

# --- mini_blog (small) -------------------------------------------------------
mini_blog: check-conn ## Build the small mini_blog database
	@echo "==> Building $(DB_MINI) (small)..."
	@$(PSQL_ADMIN) -c "DROP DATABASE IF EXISTS $(DB_MINI);"
	@$(PSQL_ADMIN) -c "CREATE DATABASE $(DB_MINI);"
	@$(PSQL_LOAD)  -d $(DB_MINI) < $(FIXTURES_DIR)mini_blog.sql > /dev/null
	@echo "==> $(DB_MINI) OK"

# --- shop_oms (medium) -------------------------------------------------------
shop_oms: check-conn ## Build the medium shop_oms database
	@echo "==> Building $(DB_SHOP) (medium)..."
	@$(PSQL_ADMIN) -c "DROP DATABASE IF EXISTS $(DB_SHOP);"
	@$(PSQL_ADMIN) -c "CREATE DATABASE $(DB_SHOP);"
	@$(PSQL_LOAD)  -d $(DB_SHOP) < $(FIXTURES_DIR)shop_oms.sql > /dev/null
	@echo "==> $(DB_SHOP) OK"

# --- analytics_dw (large) ----------------------------------------------------
analytics_dw: check-conn ## Build the large analytics_dw database
	@echo "==> Building $(DB_DW) (large) — may take ~30s..."
	@$(PSQL_ADMIN) -c "DROP DATABASE IF EXISTS $(DB_DW);"
	@$(PSQL_ADMIN) -c "CREATE DATABASE $(DB_DW);"
	@$(PSQL_LOAD)  -d $(DB_DW) < $(FIXTURES_DIR)analytics_dw.sql > /dev/null
	@echo "==> $(DB_DW) OK"

# =============================================================================
# Verification
# =============================================================================
verify: ## Show row counts + schema-object summary for all 3 databases
	@echo "==> Verifying $(DB_MINI)..."
	@$(PSQL_LOAD) -d $(DB_MINI) -c "\
		SELECT 'mini_blog' AS db, \
		       (SELECT COUNT(*) FROM pg_tables WHERE schemaname='public') AS tables, \
		       (SELECT COUNT(*) FROM pg_views  WHERE schemaname='public') AS views, \
		       (SELECT COUNT(*) FROM users)                               AS users, \
		       (SELECT COUNT(*) FROM posts)                               AS posts, \
		       (SELECT COUNT(*) FROM comments)                            AS comments;"
	@echo "==> Verifying $(DB_SHOP)..."
	@$(PSQL_LOAD) -d $(DB_SHOP) -c "\
		SELECT 'shop_oms' AS db, \
		       (SELECT COUNT(*) FROM pg_tables WHERE schemaname IN ('catalog','sales','billing','users')) AS tables, \
		       (SELECT COUNT(*) FROM pg_matviews WHERE schemaname IN ('catalog','sales','billing','users')) AS matviews, \
		       (SELECT COUNT(*) FROM users.customers) AS customers, \
		       (SELECT COUNT(*) FROM catalog.products) AS products, \
		       (SELECT COUNT(*) FROM sales.orders)    AS orders, \
		       (SELECT COUNT(*) FROM sales.order_items) AS order_items;"
	@echo "==> Verifying $(DB_DW)..."
	@$(PSQL_LOAD) -d $(DB_DW) -c "\
		SELECT 'analytics_dw' AS db, \
		       (SELECT COUNT(*) FROM pg_tables WHERE schemaname IN ('dim','fact','staging','audit','reporting')) AS tables, \
		       (SELECT COUNT(*) FROM pg_matviews WHERE schemaname IN ('dim','fact','staging','audit','reporting')) AS matviews, \
		       (SELECT COUNT(*) FROM dim.dim_customer) AS customers, \
		       (SELECT COUNT(*) FROM dim.dim_product)  AS products, \
		       (SELECT COUNT(*) FROM fact.fact_sales)  AS sales, \
		       (SELECT COUNT(*) FROM fact.fact_web_events) AS web_events;"
	@echo "==> All 3 databases verified."

# =============================================================================
# Cleanup
# =============================================================================
clean: check-conn ## Drop all 3 fixture databases
	@$(PSQL_ADMIN) -c "DROP DATABASE IF EXISTS $(DB_MINI);"
	@$(PSQL_ADMIN) -c "DROP DATABASE IF EXISTS $(DB_SHOP);"
	@$(PSQL_ADMIN) -c "DROP DATABASE IF EXISTS $(DB_DW);"
	@echo "==> Dropped: $(DB_MINI), $(DB_SHOP), $(DB_DW)"

# =============================================================================
# Connectivity probe
# =============================================================================
check-conn:
	@$(PSQL_ADMIN) -c "SELECT version();" > /dev/null \
	  || { echo "ERROR: cannot connect to PostgreSQL at $(PG_HOST):$(PG_PORT) as $(PG_USER) (db=$(ADMIN_DB))." ; \
	       echo "       Adjust PG_HOST / PG_PORT / PG_USER / PGPASSWORD or use 'make docker-up' first." ; exit 1; }

# =============================================================================
# Docker mode (optional convenience — when no local Postgres is available)
# =============================================================================
docker-up: ## Spin up postgres:16-alpine for fixture testing on $(DOCKER_PORT)
	@if docker ps --format '{{.Names}}' | grep -qx '$(DOCKER_NAME)'; then \
	    echo "==> Container $(DOCKER_NAME) already running."; \
	else \
	    echo "==> Starting $(DOCKER_IMAGE) as $(DOCKER_NAME) on port $(DOCKER_PORT)..."; \
	    docker run -d --rm --name $(DOCKER_NAME) \
	        -e POSTGRES_USER=test -e POSTGRES_PASSWORD=test -e POSTGRES_DB=postgres \
	        -p $(DOCKER_PORT):5432 $(DOCKER_IMAGE) > /dev/null; \
	    for i in $$(seq 1 30); do \
	        docker exec $(DOCKER_NAME) pg_isready -U test 2>/dev/null | grep -q "accepting" && break; \
	        sleep 1; \
	    done; \
	    echo "==> $(DOCKER_NAME) is ready."; \
	    echo "    Connect via: PGPASSWORD=test psql -h localhost -p $(DOCKER_PORT) -U test -d postgres"; \
	    echo "    Or build with: make all PG_PORT=$(DOCKER_PORT) PG_USER=test PGPASSWORD=test"; \
	fi

docker-down: ## Stop and remove the docker fixture container
	-@docker rm -f $(DOCKER_NAME) 2>/dev/null && echo "==> $(DOCKER_NAME) removed."

docker-logs: ## Tail logs from the docker fixture container
	@docker logs -f $(DOCKER_NAME)
