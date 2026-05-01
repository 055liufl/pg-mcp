# Review-0008: pg-mcp Test Plan Review

> **Reviewer**: OpenAI Codex CLI (gpt-5.4, reasoning=medium)
> **Date**: 2026-05-01
> **Scope**: [Test-Plan-0007](./0007-pg-mcp-test-plan.md) against
> [Design-0002](./0002-pg-mcp-design.md) + [Impl-Plan-0004](./0004-pg-mcp-impl-plan.md)
> **Focus areas (per review prompt)**:
> 1. Coverage gaps vs PRD/Design/Impl
> 2. Executability of every command on a clean machine
> 3. Realistic vs aspirational claims
> 4. Layering and duplication
> 5. Missing security & robustness scenarios

## Findings

- [critical] Verified state and target state are conflated, so the document is not trustworthy as a “current executable plan”. Ref-section in 0007: §0, §1.3, §7.2, §7.3, §11.3, §12.2, §13, §14, §16. Evidence: §0 says “`状态：可直接执行；本计划撰写时已完成 self-verify`” and reports “`316 passed`”, but §7.3 is still “`新增测试文件 tests/integration/test_cli_lifecycle.py`”, §7.2 says SSE is “`P2，可选`”, and §14 explicitly admits “`未跑过的小节`” including CLI lifecycle and CI matrix. Impact: readers cannot tell what is green today versus what is merely intended, which defeats Q3 and makes the gate/coverage narrative unsafe for release decisions. Concrete fix: split the document into `Verified Today` and `Planned Additions`; remove unimplemented/unrun items from current totals and self-verify claims; keep §11.3/§12.2/§13/§7.2/§7.3 in a forward-looking backlog section.

- [high] 0007 misses a first-class test plan for `sql_generator.py`, despite Impl-Plan explicitly requiring it and 0007 admitting that module is mostly uncovered. Ref-section in 0007: §0, §5.1-§5.5, §16. Evidence: §0 lists “`sql_generator.py 34 %（LLM 主路径）`”; §5’s unit inventory has no `test_sql_generator.py`; Impl-Plan 0004 §2.4 explicitly defines “`测试文件: tests/unit/test_sql_generator.py`”. Impact: core behavior around OpenAI timeout mapping, API error mapping, prompt construction, feedback-on-retry, and token/logprob extraction is not planned as executable coverage even though it is on the critical path. Concrete fix: add a dedicated §5.x for `test_sql_generator.py` with cases for timeout, APIError, empty/invalid model output, retry feedback propagation, and prompt contract.

- [high] PRD §3.1 startup/lazy-load behavior is not fully covered, despite 0007 claiming full PRD §3 coverage. Ref-section in 0007: §1.1, §5, §6.4, §7, §8. Evidence: 0007 covers warmup and cache internals (`C-1`..`C-9`), but there is no explicit end-to-end or acceptance case for: startup only discovers DBs and does not eagerly load schema; first query returns `E_SCHEMA_NOT_READY` with `retry_after_ms`; partial database discovery failure does not block startup; TTL expiry triggers reload; LRU behavior is honored/configured. PRD §3.1 requires all of those. Impact: one of the most user-visible lifecycle contracts can regress while the plan still reports “full PRD coverage”. Concrete fix: add tests for `discover_databases()` partial failure, cold-start first-query `E_SCHEMA_NOT_READY`, background warmup after startup, TTL expiry reload, and an explicit non-goal or validation method for Redis LRU since Design 0002 delegates it to Redis config.

- [high] Many commands in §4 and §5 will fail on a clean machine because they assume `cwd=src` but do not establish it. Ref-section in 0007: §4, §5.1-§5.5, §7.2, §10, §11.2-§11.3. Evidence: commands such as `uv run pytest tests/unit/`, `uv run pytest tests/e2e/`, `uv run pytest tests/unit/test_sql_validator.py -v`, `uv run pytest --cov=pg_mcp ... tests/` are written without `cd /home/lfl/pg-mcp/src &&`, while the repo’s `pyproject.toml`, `pg_mcp/`, and `tests/` live under `src/`. Impact: copy-paste reproducibility is broken; the same doc that claims “每条命令都贴出来” gives commands that only work if the reader infers a hidden working directory. Concrete fix: make every command self-contained with either `cd /home/lfl/pg-mcp/src && ...` or `uv --project /home/lfl/pg-mcp/src run ...` and use `src/tests/...` when running from repo root.

- [high] The acceptance aggregation in §8.6 is not executable and can false-green. Ref-section in 0007: §8.6. Evidence: the proposed `fixtures/acceptance.sh` runs `bash -c "$A1"` through `"$A5"`, but 0007 never defines or exports `A1`..`A5`, and no such script exists in the repo. On a shell, `bash -c ""` exits 0. Impact: the “aggregate acceptance suite” can print success without actually running any acceptance case. Concrete fix: replace `"$A1"`-style placeholders with real shell functions or checked-in scripts, and make the wrapper invoke concrete files/commands.

- [high] The “current can pass” transitional gate in §12.1 is brittle and partly broken. Ref-section in 0007: §12.1. Evidence: it first does `cd /home/lfl/pg-mcp/src`, then runs `git diff --name-only origin/master... | ... grep '^src/pg_mcp/' | xargs -r uv run mypy`. From `src/`, paths like `src/pg_mcp/...` do not exist; also `origin/master` is not portable to repos using `main` or shallow clones without that ref. Impact: the gate can fail spuriously, skip changed files, or be unusable on fresh CI runners. Concrete fix: run diff-based commands from repo root, parameterize the base ref (`${BASE_REF:-origin/main}` or `origin/HEAD`), and pass paths that exist from that cwd.

- [medium] 0007 downgrades Impl-Plan §5.5 gaps that were called out as high-priority missing tests, especially SSE lifecycle. Ref-section in 0007: §1.3, §7.2, §16. Evidence: Impl-Plan 0004 §5.5 lists “`SSE transport 生命周期测试`” under “`缺失测试补充（高优先级）`”; 0007 reclassifies it as “`§7.2（设为 P2，非阻塞）`” and later “`当前不在自动 CI 中`”. Impact: the plan stops matching the implementation plan’s stated risk priorities, so “coverage complete” is overstated. Concrete fix: either restore SSE lifecycle to P1 with concrete automated cases, or explicitly amend the implementation plan and explain the risk tradeoff.

- [medium] The test pyramid in §2 does not actually match §§5-§9, and duplication is uneven. Ref-section in 0007: §2, §6.1, §7.1-§7.3, §8, §9. Evidence: §2 says “`每层增加用例时，下层必须已经覆盖该路径`”, but §7.3 introduces new CLI lifecycle coverage without lower-layer support, §7.2 leaves SSE manual-only, and §8 adds real-fixture validation specifically because §6.3 is still mock-backed. At the same time, `admin_action=refresh_schema` is repeated across unit (`O-3`), integration (`A-2/A-3`), E2E (`M-6`), and smoke (`S-3`), while startup partial-failure and cold-load-not-ready flows are not concretely represented in any higher layer. Impact: the pyramid is descriptive, not enforced; expensive layers duplicate some happy paths while key lifecycle paths remain unanchored. Concrete fix: add a traceability table mapping each P0/P1 requirement to one lowest-cost canonical test plus optional higher-layer corroboration.

- [medium] Security coverage in §10 is narrower than the attack surface defined by PRD §3.3 and Design §4.8. Ref-section in 0007: §10. Evidence: §10 covers `pg_sleep`, `pg_read_file`, `dblink`, `lo_import`, foreign table, multi-statement, and basic DDL/DML. Design 0002’s deny lists also include `pg_advisory_lock*`, `pg_notify`, `pg_listening_channels`, `pg_terminate_backend`, `pg_cancel_backend`, `pg_reload_conf`, `set_config`, `current_setting`, `lo_export`, and similar functions; PRD §3.3 also explicitly forbids `COPY ... TO PROGRAM` and `postgres_fdw`-style external access. Impact: high-risk exfiltration, locking, and operational-abuse vectors can regress without tripping the “golden matrix”. Concrete fix: extend §10 with the full deny surface from Design §4.8 and PRD §3.3, and add real test IDs for each family rather than a generic “FAIL_CASES” reference.

- [medium] Runtime-abuse and resource-exhaustion scenarios are underplanned beyond parser-level SQL safety. Ref-section in 0007: §6.5, §8.3, §10, §16. Evidence: executor tests cover nominal timeout/size behaviors, but there are no concrete cases for recursive CTEs, `generate_series` explosions, large sort/temp spill behavior, pathological window queries, or quoted search_path injection strings. §16 also admits “`foreign-table fixture 缺失`”. Impact: the plan does not demonstrate that runtime controls in PRD §3.3/§3.4 and Design §4.9 are effective against abusive but read-only SQL. Concrete fix: add PG-backed abuse tests for recursive CTE timeout, huge rowset soft/hard thresholds, temp-file-limit enforcement, and schema-name quoting with embedded quotes/commas.

- [low] Response-contract and observability coverage is not clearly enumerated, despite being part of the product contract. Ref-section in 0007: §5.3, §7.1, §11.4, §16. Evidence: PRD §3.4/§3.5 and Design §4.3/§7 require fields/events like `request_id`, `schema_loaded_at`, `validation_used`, `warnings`, and structured request lifecycle logs. 0007 mostly checks functional outcomes and admits gaps like `metrics.py 57 %` and broken info-level logging (`GAP-8`). Impact: auditability and operability regressions can slip even if functional tests pass. Concrete fix: add a small contract matrix for response fields and observability events, with explicit assertions per field/event.

## Verdict

**REWRITE**

## Prioritized Fix List

1. Split 0007 into “verified today” versus “target state”, and remove unimplemented/unrun items from current totals and self-verify claims.
2. Repair all commands so they are self-contained from a clean checkout, especially §4/§5/§10/§11/§12.
3. Replace §8.6’s placeholder acceptance wrapper with a real executable script.
4. Add missing coverage for `sql_generator.py` and PRD §3.1 startup/lazy-load semantics.
5. Reconcile priority/coverage claims with Impl-Plan §5.5, especially SSE lifecycle and CLI lifecycle.
6. Expand §10 and executor/acceptance coverage to include the full deny-list and runtime abuse scenarios.
7. Add a requirement-to-test traceability table so the pyramid is enforceable rather than aspirational.
