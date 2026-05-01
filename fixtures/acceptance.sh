#!/usr/bin/env bash
# =============================================================================
# fixtures/acceptance.sh
#
# Run the pg-mcp acceptance suite (Test-Plan-0007 §8.A1–§8.A5) against the
# three fixture databases. Bails on the first failure (set -e).
#
# Usage:
#   PG_HOST=localhost PG_PORT=5433 PG_USER=test PG_PASSWORD=test \
#   REDIS_URL=redis://localhost:6380/0 \
#     bash fixtures/acceptance.sh
#
# Defaults match `make docker-up`.
# =============================================================================
set -euo pipefail

# ---- locate repo / ensure cwd ----
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
PKG_DIR="${REPO_ROOT}/src"

# ---- defaults (override via env) ----
: "${PG_HOST:=localhost}"
: "${PG_PORT:=5433}"
: "${PG_USER:=test}"
: "${PG_PASSWORD:=test}"
: "${REDIS_URL:=redis://localhost:6380/0}"
export PG_HOST PG_PORT PG_USER PG_PASSWORD REDIS_URL
export OPENAI_API_KEY="${OPENAI_API_KEY:-sk-test-dummy}"

# ---- runner: invoke uv from src/ so package + tests resolve ----
run_py() {
    (cd "${PKG_DIR}" && uv run python - "$@")
}

# ----------------------------------------------------------------------------
# A1 — schema discovery against all 3 fixture DBs
# ----------------------------------------------------------------------------
echo "=== A1 schema discovery ==="
run_py <<'PY'
import asyncio
from pg_mcp.config import Settings
from pg_mcp.db.pool import ConnectionPoolManager
from pg_mcp.schema.discovery import SchemaDiscovery

EXPECTED = {
    "mini_blog":    (6, 1, 18, 1, 0, 7),
    "shop_oms":     (19, 3, 61, 7, 1, 15),
    "analytics_dw": (64, 4, 135, 11, 2, 23),
}

async def main():
    s = Settings(openai_api_key="dummy")
    p = ConnectionPoolManager(s)
    try:
        for db, (et, ev, ei, ee, ec, ef) in EXPECTED.items():
            schema = await SchemaDiscovery(p, s).load_schema(db)
            got = (
                len(schema.tables),
                len(schema.views),
                len(schema.indexes),
                len(schema.enum_types),
                len(schema.composite_types),
                len(schema.foreign_keys),
            )
            assert got == (et, ev, ei, ee, ec, ef), f"{db}: expected {(et,ev,ei,ee,ec,ef)}, got {got}"
            print(
                f"  {db:14s} tables={got[0]}, views={got[1]}, "
                f"idx={got[2]}, enums={got[3]}, comp={got[4]}, fks={got[5]} OK"
            )
    finally:
        await p.close_all()

asyncio.run(main())
PY

# ----------------------------------------------------------------------------
# A2 — retrieval threshold flips for analytics_dw only
# ----------------------------------------------------------------------------
echo "=== A2 retrieval threshold ==="
run_py <<'PY'
import asyncio
from pg_mcp.config import Settings
from pg_mcp.db.pool import ConnectionPoolManager
from pg_mcp.schema.discovery import SchemaDiscovery
from pg_mcp.schema.retriever import SchemaRetriever

EXPECTED = {"mini_blog": False, "shop_oms": False, "analytics_dw": True}

async def main():
    s = Settings(openai_api_key="dummy")
    p = ConnectionPoolManager(s)
    r = SchemaRetriever(max_tables_for_full=s.schema_max_tables_for_full_context)
    try:
        for db, want in EXPECTED.items():
            schema = await SchemaDiscovery(p, s).load_schema(db)
            got = r.should_use_retrieval(schema)
            assert got is want, f"{db}: expected {want}, got {got}"
            print(f"  {db:14s} should_use_retrieval={got} OK")
    finally:
        await p.close_all()

asyncio.run(main())
PY

# ----------------------------------------------------------------------------
# A3 — SQL executor: LIMIT wrap, EXPLAIN, search_path
# ----------------------------------------------------------------------------
echo "=== A3 SQL executor ==="
run_py <<'PY'
import asyncio
from pg_mcp.config import Settings
from pg_mcp.db.pool import ConnectionPoolManager
from pg_mcp.engine.sql_executor import SqlExecutor

async def main():
    s = Settings(openai_api_key="dummy", max_rows=5)
    p = ConnectionPoolManager(s)
    try:
        ex = SqlExecutor(p, s)

        # 1) LIMIT wrapper truncates to max_rows
        r = await ex.execute(
            "mini_blog",
            "SELECT id, title FROM posts ORDER BY id",
            schema_names=["public"],
        )
        assert r.row_count == 5 and r.truncated is True, r
        print(f"  A3.1 LIMIT wrap: row_count={r.row_count} truncated={r.truncated} OK")

        # 2) EXPLAIN bypasses LIMIT and returns plan
        r = await ex.execute(
            "mini_blog",
            "EXPLAIN SELECT * FROM posts",
            schema_names=["public"],
            is_explain=True,
        )
        assert r.columns == ["QUERY PLAN"], r.columns
        print(f"  A3.2 EXPLAIN: columns={r.columns} OK")

        # 3) search_path resolves unqualified `customers` to users.customers
        r = await ex.execute(
            "shop_oms",
            "SELECT count(*) FROM customers",
            schema_names=["public", "users"],
        )
        assert r.rows[0][0] >= 100, r.rows
        print(f"  A3.3 search_path: customers={r.rows[0][0]} OK")
    finally:
        await p.close_all()

asyncio.run(main())
PY

# ----------------------------------------------------------------------------
# A4 — full QueryEngine pipeline (mock LLM + real PG + Redis)
# ----------------------------------------------------------------------------
echo "=== A4 QueryEngine full pipeline ==="
run_py <<'PY'
import asyncio
import redis.asyncio as redis
from pg_mcp.config import Settings
from pg_mcp.db.pool import ConnectionPoolManager
from pg_mcp.engine.orchestrator import QueryEngine
from pg_mcp.engine.sql_executor import SqlExecutor
from pg_mcp.engine.sql_validator import SqlValidator
from pg_mcp.engine.db_inference import DbInference
from pg_mcp.schema.cache import SchemaCache
from pg_mcp.schema.retriever import SchemaRetriever
from pg_mcp.models.request import QueryRequest
from tests.conftest import MockSqlGenerator, MockResultValidator

async def main():
    s = Settings(
        pg_databases="mini_blog,shop_oms,analytics_dw",
        openai_api_key="dummy",
        enable_validation=False,
    )
    pool = ConnectionPoolManager(s)
    rcl = redis.from_url(s.redis_url)
    cache = SchemaCache(rcl, pool, s)
    cache.set_discovered_databases(s.pg_databases_list)
    await cache.refresh()

    engine = QueryEngine(
        sql_generator=MockSqlGenerator(sql="SELECT count(*) AS posts FROM posts"),
        sql_validator=SqlValidator(),
        sql_executor=SqlExecutor(pool, s),
        schema_cache=cache,
        db_inference=DbInference(cache, s),
        result_validator=MockResultValidator(),
        retriever=SchemaRetriever(s.schema_max_tables_for_full_context),
        settings=s,
    )

    resp = await engine.execute(
        QueryRequest(query="how many posts", database="mini_blog")
    )
    assert resp.error is None, resp.error
    assert resp.row_count == 1
    assert resp.rows[0][0] == 18
    print(f"  A4 ok: rows={resp.rows} db={resp.database}")

    await pool.close_all()
    await rcl.aclose()

asyncio.run(main())
PY

# ----------------------------------------------------------------------------
# A5 — prompt-text size: full vs retrieval compression
# ----------------------------------------------------------------------------
echo "=== A5 prompt context size ==="
run_py <<'PY'
import asyncio
from pg_mcp.config import Settings
from pg_mcp.db.pool import ConnectionPoolManager
from pg_mcp.schema.discovery import SchemaDiscovery
from pg_mcp.schema.retriever import SchemaRetriever

# Allow ±15% to absorb random()-driven JSON variation
TOLERANCE = 0.15
EXPECTED = {  # full_prompt_size, retrieval_size_or_None
    "mini_blog":    (2073, None),
    "shop_oms":     (5824, None),
    "analytics_dw": (13431, 1100),
}

async def main():
    s = Settings(openai_api_key="dummy")
    p = ConnectionPoolManager(s)
    r = SchemaRetriever(max_tables_for_full=s.schema_max_tables_for_full_context)
    try:
        for db, (full_exp, ret_exp) in EXPECTED.items():
            schema = await SchemaDiscovery(p, s).load_schema(db)
            full = len(schema.to_prompt_text())
            assert abs(full - full_exp) / full_exp <= TOLERANCE, (db, full, full_exp)
            if ret_exp is None:
                assert not r.should_use_retrieval(schema), db
                print(f"  {db:14s} full={full} no retrieval OK")
            else:
                assert r.should_use_retrieval(schema), db
                ctx = len(r.retrieve("top revenue products this quarter", schema))
                assert abs(ctx - ret_exp) / ret_exp <= TOLERANCE, (db, ctx, ret_exp)
                print(
                    f"  {db:14s} full={full} retrieval={ctx} "
                    f"compression={(full - ctx) / full * 100:.0f}% OK"
                )
    finally:
        await p.close_all()

asyncio.run(main())
PY

echo
echo "=== ALL ACCEPTED ==="
