# Review-0005: pg-mcp Implementation Plan Review

> Review of [IMP-0004: pg-mcp Detailed Implementation Plan](./0004-pg-mcp-impl-plan.md)
> Reviewer: OpenAI Codex (gpt-5.4)
> Date: 2026-04-30

## Review Scope

- **Completeness**: Are all components from the design document covered? Missing modules or edge cases?
- **Feasibility**: Is the proposed timeline realistic? Overly complex implementations?
- **Risk Areas**: Concurrency, SQL injection prevention, LLM integration stability
- **Testing Strategy**: Coverage sufficiency, gaps in testing matrix
- **Dependencies**: Version correctness and compatibility
- **Architecture Consistency**: Alignment with design document
- **Performance**: Bottlenecks and optimization opportunities
- **Security**: SQL validation, data sanitization, access control

## Findings

### 1. `[critical]` Async Lifecycle is Not Viable

The plan creates long-lived async resources, runs discovery/warmup/read-only checks in separate `asyncio.run(...)` calls, and then starts the server in yet another loop. Background warmup tasks will be cancelled and loop-bound clients/pools can be reused from the wrong event loop.

**References**: [0004-pg-mcp-impl-plan.md](./0004-pg-mcp-impl-plan.md) CLI startup section

**Recommendation**: Move all startup and shutdown into one top-level async lifecycle, and schedule warmup/refresh tasks from FastAPI lifespan or the MCP server startup path.

### 2. `[high]` Row-Limit Enforcement Strategy is Unsafe

The plan only injects `LIMIT` when none exists, which means an LLM-generated `LIMIT 100000000` or `FETCH FIRST ...` can bypass the service cap and force `fetch()` to materialize too much data before the byte-limit logic runs.

**References**: [0004-pg-mcp-impl-plan.md](./0004-pg-mcp-impl-plan.md) SQL executor section, [0002-pg-mcp-design.md](./0002-pg-mcp-design.md) executor design

**Recommendation**: Rewrite the SQL AST to enforce `min(user_limit, max_rows + 1)` at the top level, or always wrap with an outer limit that cannot be bypassed.

### 3. `[high]` Result-Validation Repair Path Not Implemented

The orchestrator reaches `verdict == "fix"` and then stops at a comment instead of re-generating, re-validating, and re-executing as required by the design data flow.

**References**: [0004-pg-mcp-impl-plan.md](./0004-pg-mcp-impl-plan.md) orchestrator section, [0002-pg-mcp-design.md](./0002-pg-mcp-design.md) data flow diagram

**Recommendation**: Make result-validation fixes part of the same bounded retry state machine as SQL generation/validation, with explicit attempt accounting and logging.

### 4. `[high]` Missing Required Behaviors from PRD

Several required behaviors are absent from the implementation plan:
- **Scheduled schema refresh**: Cache has TTL but no scheduler/background worker
- **`PG_DATABASES` override handling**: Plan parses `pg_databases`, but CLI always discovers databases regardless of the override
- **Partial discovery failure behavior**: PRD requires non-blocking startup with warning logs when some DBs fail
- **DB connection retry/backoff**: PRD requires exponential backoff (5 retries, 100ms initial, 3s max, with jitter)

**References**: [0001-pg-mcp-prd.md](./0001-pg-mcp-prd.md) startup, cache refresh, error handling sections

**Recommendation**: Add an explicit startup decision tree for configured-vs-discovered DBs, a periodic refresh worker, and connection retry policy with jitter.

### 5. `[high]` Schema Cache Singleflight Race Conditions

`refresh()` directly runs `_do_load()` for each DB instead of going through singleflight. The referenced design implementation swallows loader exceptions, which would make refreshes look successful even when they fail.

**References**: [0004-pg-mcp-impl-plan.md](./0004-pg-mcp-impl-plan.md) cache section, [0002-pg-mcp-design.md](./0002-pg-mcp-design.md) cache design

**Recommendation**: Make refresh use the same singleflight path, persist failure detail, and define state reconciliation when `READY` exists but the Redis value is missing or corrupt.

### 6. `[high]` Dependency Version Pins Too Loose

The plan uses open-ended `>=` ranges for `mcp`, `openai`, `sqlglot`, etc., even though the same plan calls out MCP SDK drift as a risk. Additionally, `types-redis` is unnecessary with `redis>=5` because redis ships its own typing.

**References**: [0004-pg-mcp-impl-plan.md](./0004-pg-mcp-impl-plan.md) pyproject.toml, risk section

**Recommendation**: Pin tested major ranges (e.g. `mcp>=1.0,<2.0`), remove `types-redis`, and choose whether to stay on Chat Completions intentionally or migrate new work to the Responses API.

### 7. `[high]` Admin Refresh Contract Incomplete + Broken Dockerfile

`QueryResponse` has no field for refresh results even though the admin path is supposed to return per-DB success/failure detail. The Dockerfile runs `pip install .` before copying the package source.

**References**: [0001-pg-mcp-prd.md](./0001-pg-mcp-prd.md) admin action, [0004-pg-mcp-impl-plan.md](./0004-pg-mcp-impl-plan.md) response model, Dockerfile

**Recommendation**: Add a dedicated admin response payload or `refresh` field, and fix the Docker build order (copy source before `pip install .`).

### 8. `[high]` SQL Safety Fragile Around Identifier Handling

`search_path` and session `SET` statements are built with f-strings. The foreign-table check assumes `"{db}.{name}"` without properly resolving quoted or non-public schemas, which can produce both misses and malformed SQL.

**References**: [0004-pg-mcp-impl-plan.md](./0004-pg-mcp-impl-plan.md) executor, validator sections, [0002-pg-mcp-design.md](./0002-pg-mcp-design.md) validator design

**Recommendation**: Canonicalize identifiers from the AST, quote identifiers with a dedicated routine, and avoid string-building for configurable session settings when a safer API exists.

### 9. `[medium]` Test Strategy Insufficient for Riskiest Paths

The unit matrix omits `test_result_validator.py` even though that module is planned. No explicit coverage for: server/app/CLI lifecycle, semaphore rate limiting, schema-cache races, scheduled refresh, SSE transport, `admin_action`, EXPLAIN behavior, or Postgres 14-17 compatibility matrix.

**References**: [0004-pg-mcp-impl-plan.md](./0004-pg-mcp-impl-plan.md) testing strategy sections

**Recommendation**: Add concurrency tests, transport tests, lifecycle tests, and a CI matrix for PG versions.

### 10. `[medium]` Inference/Retrieval Performance Bottleneck

Per request, inference can scan every ready schema across every discovered DB, and retrieval rescans every table/column for the selected DB. This conflicts with the stated low-overhead target as database count grows.

**References**: [0001-pg-mcp-prd.md](./0001-pg-mcp-prd.md) performance SLO, [0004-pg-mcp-impl-plan.md](./0004-pg-mcp-impl-plan.md) inference, retriever sections

**Recommendation**: Precompute compact per-DB summaries or an inverted index at schema-load time and score against that instead of rescanning full objects on every query.

### 11. `[medium]` Input Validation and Data-Exfiltration Underspecified

`QueryRequest` rejects empty strings but not whitespace-only input. The deny-list story is inconsistent because the decision path lacks enough table/column lineage context to implement `db.schema.table.column` matching precisely.

**References**: [0001-pg-mcp-prd.md](./0001-pg-mcp-prd.md) input validation, [0004-pg-mcp-impl-plan.md](./0004-pg-mcp-impl-plan.md) request model, deny list

**Recommendation**: Trim-and-validate query text, and decide whether deny-list enforcement is based on SQL lineage, result metadata, or a stricter all-or-nothing database policy.

### 12. `[medium]` 14-Day Schedule is Optimistic

The plan includes AST-based SQL validation, Redis singleflight cache, async pool management, dual transports, optional LLM result validation, integration tests, E2E tests, Docker, and strict typing/coverage targets. This is more than a 2-week implementation if done robustly.

**References**: [0004-pg-mcp-impl-plan.md](./0004-pg-mcp-impl-plan.md) phase timeline

**Recommendation**: Split delivery into three milestones:
1. **Milestone 1**: Core stdio query path (config, models, validator, executor, orchestrator, server)
2. **Milestone 2**: Cache/ops hardening (Redis cache, singleflight, scheduled refresh, connection retry)
3. **Milestone 3**: SSE/admin/validation/perf (SSE transport, admin endpoints, result validation, performance optimization)

## Overall Assessment

The plan is **directionally aligned** with the design, but it is **not yet implementation-safe**. The biggest blockers are:

1. Async lifecycle (critical)
2. Row-limit enforcement (high)
3. Incomplete validation retry loop (high)
4. Missing operational behaviors around refresh/retry/discovery (high)

**Verdict**: Do not start coding from this plan unchanged. Address the critical and high-severity findings first, then proceed with the three-milestone delivery approach.

## Dependency Check

Verified on 2026-04-30:

| Package | Latest | Python 3.12 Compatible |
|---------|--------|------------------------|
| `mcp` | 1.27.0 | Yes |
| `redis` | 7.4.0 | Yes |
| `sqlglot` | 30.6.0 | Yes |
| `asyncpg` | 0.31.0 | Yes |
| `openai` | 2.33.0 | Yes |
| `fastapi` | 0.136.1 | Yes |

**Notes**:
- `types-redis` should be removed for `redis>=5` (redis ships its own typing)
- Unbounded `>=` version pins are risky for this protocol-heavy system

**Sources**:
- https://pypi.org/project/mcp/
- https://pypi.org/project/redis/
- https://pypi.org/project/sqlglot/
- https://pypi.org/project/asyncpg/
- https://pypi.org/project/openai/
- https://pypi.org/project/fastapi/
- https://pypi.org/project/types-redis/
- https://platform.openai.com/docs/guides/chat-completions
- https://platform.openai.com/docs/api-reference/responses/create
