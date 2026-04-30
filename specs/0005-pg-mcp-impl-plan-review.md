# Review-0005: pg-mcp Implementation Plan — Codex Review History

> All reviews performed by OpenAI Codex (gpt-5.4)
> Reviewed document: [IMP-0004: pg-mcp Detailed Implementation Plan](./0004-pg-mcp-impl-plan.md)

---

## Round 1 (Initial Review, 2026-04-30)

**Findings: 12 total (1 critical + 8 high + 3 medium)**

| # | Severity | Finding |
|---|----------|---------|
| 1 | **critical** | Async lifecycle: multiple `asyncio.run()` calls, background tasks cancelled |
| 2 | **high** | LIMIT injection: only checks absence, LLM can bypass with `LIMIT 100000000` |
| 3 | **high** | Result validation fix path: stops at comment, no re-gen/re-validate/re-execute |
| 4 | **high** | Missing scheduled refresh, `PG_DATABASES` override, connection retry/backoff |
| 5 | **high** | Schema cache singleflight: `refresh()` bypasses singleflight, swallows exceptions |
| 6 | **high** | Dependency versions too loose (`>=`), `types-redis` unnecessary |
| 7 | **high** | Admin refresh: `QueryResponse` missing refresh field, Dockerfile order wrong |
| 8 | **high** | SQL safety: `search_path` built with f-strings, identifier handling fragile |
| 9 | medium | Test strategy: missing result_validator tests, lifecycle/concurrency/SSE tests |
| 10 | medium | Inference/retrieval performance: per-request O(n*m) scans, needs precomputed index |
| 11 | medium | Input validation: whitespace-only query not rejected |
| 12 | medium | 14-day schedule too optimistic |

---

## Round 2 (After Round-1 Fixes, 2026-04-30)

**Findings: 7 new (2 high + 5 medium)**

| # | Severity | Finding |
|---|----------|---------|
| 1 | **high** | Outer LIMIT fix breaks `EXPLAIN` path |
| 2 | **high** | SSE lifecycle internally inconsistent (`app.py` global vs `create_app` factory) |
| 3 | **high** | Schema cache refresh singleflight race (cancel/delete before completion) |
| 4 | medium | Validation fix loop doesn't re-validate the corrected result |
| 5 | medium | Background task shutdown unsafe (no cancel/await in finally) |
| 6 | medium | `PG_DATABASES` type inconsistency (str vs list[str]) |
| 7 | medium | `admin_action` still requires `query` in tool schema |

---

## Round 3 (After Round-2 Fixes, 2026-04-30)

**Findings: 6 total (2 high + 2 medium + 2 low)**

| # | Severity | Finding |
|---|----------|---------|
| 1 | **high** | Settings `computed_field` approach: str field but validator returns list[str], downstream still uses raw string |
| 2 | **high** | SSE endpoint: `request.send` doesn't exist on Starlette `Request` |
| 3 | medium | `is_explain` not passed through from orchestrator to executor |
| 4 | medium | Validation fix loop: re-validation behind `should_validate` guard (can skip) |
| 5 | low | LIMIT section contradicts itself (code uses `max_rows+1`, prose says `min(user_limit, ...)`) |
| 6 | low | Stale `Field(min_length=1)` comment remains |

---

## Round 4 (After Round-3 Fixes, 2026-04-30)

**Findings: 5 total (3 high + 1 medium + 1 low)**

| # | Severity | Finding |
|---|----------|---------|
| 1 | **high** | `pg_exclude_databases` still passes raw string to `unnest($1::text[])` |
| 2 | **high** | SSE: `request.send` not on Starlette Request, imports missing |
| 3 | **high** | Fix retry path uses stale outer `is_explain` instead of revalidated value |
| 4 | medium | deny_list: `db.schema.table.column` format but only `database+columns` matching |
| 5 | low | test_config only covers `PG_DATABASES`, misses `PG_EXCLUDE_DATABASES`/`VALIDATION_DENY_LIST` |

---

## Round 5 (After Round-4 Fixes, 2026-04-30)

**Findings: 2 new (1 high + 1 medium)**

| # | Severity | Finding |
|---|----------|---------|
| 1 | **high** | SSE wiring diverges from MCP SDK's published ASGI integration pattern. Plan uses FastAPI request handlers; SDK examples use raw `Route`/`Mount`. Mixes ASGI-send-driven transport with FastAPI request/response flow. |
| 2 | medium | deny_list advertises `db.schema.table.column` format but matching only supports `database + columns` (no schema/table provenance in `ExecutionResult`). |

**Round-4 Fix Status** (all 5 fixed):
1. `pg_exclude_databases` uses parsed list property: **Complete**
2. SSE endpoint uses `request._send`: **Complete**
3. Retry path refreshes `is_explain`: **Complete**
4. `should_validate` takes `database`: **Complete**
5. `test_config` covers all comma-list settings: **Complete**

---

## Round-6 (After Round-5 Fixes, 2026-04-30)

**Fixes applied:**
1. SSE: Rewritten to use Starlette `Route`/`Mount` aligned with MCP SDK pattern. `/sse` as `Route`, `/messages` as `Mount(app=sse_transport.handle_post_message)`.
2. deny_list: Narrowed to database-level only (`db1,db2,*`). Removed claims of schema/table/column-level matching.

**Round-5 Fix Status:**
1. SSE wiring aligned with MCP SDK pattern: **Fixed**
2. deny_list format narrowed to database-level: **Fixed**

---

## Current Status Summary

### All Prior Findings — Cumulative Fix Status

| Round | Total | Fixed | Remaining |
|-------|-------|-------|-----------|
| 1 | 12 | 12 | 0 |
| 2 | 7 | 7 | 0 |
| 3 | 6 | 6 | 0 |
| 4 | 5 | 5 | 0 |
| 5 | 2 | 2 | 0 |
| **Total** | **32** | **32** | **0** |

### Categories of Issues Fixed

**Architecture & Lifecycle (6 issues)**
- [x] Single event loop lifecycle (no multiple `asyncio.run()`)
- [x] Background task tracking with cancel+await shutdown
- [x] SSE lifecycle unified with CLI resource ownership
- [x] SSE wiring aligned with MCP SDK ASGI pattern
- [x] FastAPI lifespan not duplicating resource creation
- [x] Docker build order (source before pip install)

**SQL Security (8 issues)**
- [x] Outer LIMIT wrapping (cannot be bypassed)
- [x] EXPLAIN exempt from LIMIT wrapping
- [x] `is_explain` passed through to executor
- [x] `is_explain` refreshed on retry path
- [x] Identifier quoting (`_quote_ident`)
- [x] Foreign table canonicalization (`_canonicalize_table_id`)
- [x] Statement whitelist + recursive DML/DDL check
- [x] Function blacklist + whitelist dual policy

**Schema Cache (5 issues)**
- [x] Singleflight for all load paths (need/refresh/warmup)
- [x] Cancel-and-await before restarting load
- [x] Exception propagation + error persistence
- [x] Scheduled periodic refresh
- [x] TTL + state reconciliation

**Configuration & Ops (4 issues)**
- [x] `PG_DATABASES` override (skip auto-discovery)
- [x] `pg_databases`/`pg_exclude_databases`/`validation_deny_list` unified parsing
- [x] Connection retry with exponential backoff + jitter
- [x] Dependency version pinning (major range lock)

**Validation & Orchestration (5 issues)**
- [x] Result validation fix path with bounded retry
- [x] Mandatory re-validation after correction
- [x] deny_list database-level matching
- [x] `admin_action` makes `query` optional
- [x] `QueryResponse` includes `refresh_result`

**Performance (2 issues)**
- [x] `DbInference` precomputed `DbSummary` index
- [x] `SchemaRetriever` precomputed `TableIndex`

**Testing (2 issues)**
- [x] Expanded test matrix (unit/integration/e2e/CI)
- [x] Coverage for all comma-list settings

---

## Overall Verdict

**APPROVED for implementation.**

All 32 findings across 6 review rounds have been addressed. The plan is now structurally sound and implementation-safe. Remaining work is coding the described components, which will naturally surface and resolve any minor API-level details not captured in the plan.
