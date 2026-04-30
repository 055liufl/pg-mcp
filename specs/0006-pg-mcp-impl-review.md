# Review-0006: pg-mcp Implementation Review

> Reviewer: OpenAI Codex (gpt-5.4)
> Date: 2026-04-30
> Scope: Full implementation against Design-0002 and Impl-Plan-0004

## Findings

### 1. `[critical]` Redis State Machine Broken — `bytes` vs `str`

**Files**: `schema/cache.py` (lines 76, 249), `cli.py` (line 57)

**Issue**: `redis.from_url(...)` returns `bytes` for all values by default. `_get_state()` tries `SchemaState(raw)` but `raw` is `bytes`, causing the enum lookup to fail and fall back to `None`. As a result:
- `get_schema()` never observes `SchemaState.READY`
- Schema loading appears to succeed but is never considered "ready"
- Warmup, singleflight, and readiness checks are all unreliable

**Recommendation**: Explicitly decode text keys (state, error) to `str` in `SchemaCache`. Keep compressed schema blobs as `bytes`. Add a regression test that simulates byte-valued Redis responses.

---

### 2. `[critical]` Validator/Executor Schema Resolution Mismatch

**Files**: `engine/sql_validator.py` (line 70), `engine/orchestrator.py` (line 208), `engine/sql_executor.py` (line 94)

**Issue**: The validator treats unqualified tables as `public.*` (e.g., `public.foo`), but the executor only sets `search_path` when `schema_names` is supplied, and the orchestrator never supplies it. This means:
- An unqualified table validates as `public.foo` but executes against the connection's default or stale `search_path`
- Can bypass the foreign-table guard and hit unintended objects

**Recommendation**: Derive an explicit schema list from the selected schema context, pass it on every execution, and use `SET LOCAL search_path` inside the readonly transaction.

---

### 3. `[high]` Deny-List Only Supports Database-Level Matching

**Files**: `engine/result_validator.py` (lines 131, 161)

**Issue**: `validation_deny_list` is implemented as database-only matching, but the design/settings contract describes `db.schema.table.column`-style rules. Fine-grained deny rules will not prevent sensitive sample data from being sent to the validation LLM.

**Recommendation**: Parse hierarchical rules, map them to executed result columns, and strip or downgrade matching data before prompt construction.

---

### 4. `[high]` SchemaCache.refresh() Returns Before Completion

**Files**: `schema/cache.py` (lines 188, 192)

**Issue**: `SchemaCache.refresh()` awaits `_ensure_loading()`, which only spawns background tasks, then immediately inspects state and reports success/failure. This makes admin refresh and periodic refresh results unreliable — the state may still be `LOADING` when the result is returned.

**Recommendation**: Await the actual per-database load tasks and only build `RefreshResult` after they settle.

---

### 5. `[medium]` SSE App Missing `/admin/refresh` Endpoint

**Files**: `app.py` (lines 27, 71)

**Issue**: The SSE app is missing the designed `POST /admin/refresh` endpoint. Only `/health`, `/sse`, and `/messages` are mounted.

**Recommendation**: Add the admin route, call the shared `SchemaCache.refresh()`, and return a structured refresh result.

---

### 6. `[medium]` Precomputation Path Not Wired

**Files**: `schema/cache.py` (line 128), `engine/db_inference.py` (line 100), `schema/retriever.py` (line 121)

**Issue**: `DbInference` summaries are built lazily, never invalidated on refresh, and `SchemaRetriever` rebuilds indexes on-demand unless callers manually inject `_retrieval_index`. This misses the design's performance model and can leave inference stale after schema changes.

**Recommendation**: Build and invalidate retrieval indexes and DB summaries as part of cache load/refresh lifecycle.

---

### 7. `[medium]` View Metadata Incomplete

**Files**: `schema/discovery.py` (line 470)

**Issue**: Every `ViewInfo` is emitted with `columns=[]`, which diverges from the model/design and weakens prompt quality for view-heavy databases.

**Recommendation**: Fetch and populate view columns during discovery (reuse the table+column fetch query or add a dedicated view column query).

---

### 8. `[medium]` DbInference DI Parameter Mismatch

**Files**: `cli.py` (line 90), `engine/db_inference.py` (line 95)

**Issue**: `DbInference` constructor signature expects `(cache, settings)` but the CLI passes `(cache, retriever)`. It happens not to explode now because `_settings` is unused, but this is a strict-typing failure and a bad DI boundary.

**Recommendation**: Pass `settings` instead of `retriever`, and add a startup smoke test plus a real `mypy --strict` gate.

---

### 9. `[medium]` Test Coverage Weaker Than Appears

**Files**: `tests/unit/test_orchestrator.py` (line 214), `tests/e2e/test_mcp_tool.py` (lines 101, 239)

**Issue**:
- The orchestrator "fix loop" test never actually exercises the fix path
- MCP e2e tests mostly perform no assertions or bypass the real tool callbacks
- No app/SSE/admin route tests matching the plan
- No regression tests for cache bytes/state handling or refresh completion

**Recommendation**: Replace placeholder tests with real MCP/FastAPI assertions. Add regressions for cache bytes/state handling and refresh completion.

---

## Verdict

**The implementation is not safe or ready for use.**

The overall component layout is close to the design, and the single-event-loop lifecycle is directionally sound. However, the broken Redis state handling and the validator/executor schema-resolution mismatch are both **release blockers** that must be fixed before trusting this server with real databases.

### Priority Fix Order

1. **Fix Redis state machine** (decode `bytes` → `str` for state/error keys)
2. **Fix validator/executor schema resolution** (pass `schema_names` from orchestrator, use `SET LOCAL search_path`)
3. **Fix `SchemaCache.refresh()` completion** (await actual load tasks)
4. **Fix `DbInference` DI** (pass correct constructor args)
5. **Add `/admin/refresh` endpoint** to SSE app
6. **Populate view columns** in discovery
7. **Strengthen tests** (fix loop, SSE routes, cache bytes regression)
8. **Wire precomputation** (build indexes/summaries on cache load)
9. **Implement hierarchical deny-list** (post-MVP enhancement)

---

## Verification Notes

The following automated checks were attempted but could not be completed in the review environment:
- `mypy --strict`: Not installed in review environment
- `pytest`: Collection failed due to missing `mcp` package in local environment
- `uv`: Cache creation failed

**Recommendation**: Run the full quality gate (`make quality`) in a proper Python 3.12+ environment with all dependencies installed before deploying.
