# Test-Plan-0007: pg-mcp 测试计划

> 依据：PRD-0001 · Design-0002 · Impl-Plan-0004 · Impl-Review-0006 · Test-Plan-Review-0008
> 本文档分两部分：A) **Verified Today**（撰写时已实跑通过）；B) **Planned Additions**（未跑通 / 未实现，跟踪条目）
> 日期：2026-05-01

---

## 0. 摘要 / Snapshot

| 维度 | 实测值 | 来源 |
|---|---|---|
| 用例总数 | **330** | `uv run pytest tests/ -W ignore` 收集 |
| 全量耗时 | **≈21 s** | `330 passed in 21.19s` |
| 行覆盖率 | **86%** | `--cov=pg_mcp` |
| 行+分支覆盖率 | **85%** | `--cov=pg_mcp --cov-branch` |
| 单元测试 | 235 | tests/unit/* |
| 集成测试 | 75 | tests/integration/* |
| 端到端 | 20 | tests/e2e/* |
| 验收脚本 | `fixtures/acceptance.sh` | A1..A5 全部 ALL ACCEPTED |
| 烟雾 | S-1/S-2/S-3 | exit 0 / `{"status":"ok"}` / refresh 三库成功 |

**Quality gate 现状（live, 2026-05-01）**：

- `uv run ruff check pg_mcp/ tests/` → **141 errors**（89 fixable）
- `uv run ruff format --check pg_mcp/ tests/` → **30 files** would be reformatted
- `uv run mypy pg_mcp/` → **33 errors in 11 files**

→ Lint/format/type 三件套尚未清零，列入 §16.1 P1 Backlog；当前 PR 门禁仅强制 `pytest + cov-fail-under=80`，§12.1 给出可执行的过渡门禁。

---

## 1. 范围 / Scope

### 1.1 In scope

- 单元 / 集成 / 端到端 / 验收 / 烟雾 五层测试设计
- SQL 校验黄金矩阵（PASS / FAIL / PARSE_FAIL / FOREIGN_TABLE）
- LLM 主路径 (`sql_generator.py`)、Schema 状态机、Result Validator、Orchestrator 编排
- PRD §3.1 启动/懒加载语义；PRD §3.3 安全底线；PRD §3.4/§3.5 响应契约与可观测性
- 覆盖率 / 质量门禁的过渡方案（今天可过）与目标方案（计入 Backlog）

### 1.2 Out of scope

- 真实 OpenAI 调用的端到端验证（§16.3 P3 manual smoke）
- CI 矩阵（多 Python × 多 PG），见 §16.2 P2
- 性能/负载基准（仅给出 wall-time 上限提示），见 §16.2 P2
- Redis 服务端 LRU 策略本身——Design §6 显式委托给 Redis `maxmemory-policy` 配置；测试只校验配置能正确传入，见 §13 配置类条目

### 1.3 命令书写约定

每条命令必须 self-contained，可直接从 fresh checkout 复制粘贴。本文档允许以下三种等价形式：

```bash
# 形式一（推荐）：先切到 src 再执行
cd /home/lfl/pg-mcp/src && uv run pytest tests/ -W ignore

# 形式二：从仓库根用 --project 指定项目目录
cd /home/lfl/pg-mcp && uv --project src run pytest src/tests -W ignore

# 形式三：使用仓库内已存在的脚本
cd /home/lfl/pg-mcp && bash fixtures/acceptance.sh
```

**禁止**：`uv run pytest ...` 这类隐含 `cwd=src` 的写法；本文档不再出现。

---

## 2. 测试金字塔（带 §13 traceability 兜底）

```
                 ┌──────────────────┐
                 │   Layer 6 烟雾    │  S-1..S-3，可观测的最小活性证据
                 ├──────────────────┤
                 │   Layer 5 验收    │  fixtures/acceptance.sh A1..A5
                 ├──────────────────┤
                 │ Layer 4 端到端 (20)│  MCP 协议 stdio handler 直驱
                 ├──────────────────┤
                 │ Layer 3 集成 (75) │  PG + Redis 真依赖
                 ├──────────────────┤
                 │ Layer 2 单元 (235)│  纯逻辑 + AsyncMock
                 ├──────────────────┤
                 │  Layer 1 静态     │  ruff / format / mypy（过渡未强制）
                 └──────────────────┘
```

> 金字塔本身只是约束 “每个高层用例必有同类低层用例做最小成本兜底”。真正可被 enforce 的是 §13 的 traceability matrix——任何一条 PRD/Design/Impl 条目，至少在 matrix 里有一行对应的 test ID 或 Planned 占位。

---

# Part A — Verified Today

> Part A 列出的所有用例 / 命令 / 输出在 2026-05-01 自验当天实跑通过。Part B 中的条目尚未跑通或尚未实现。

## 3. 环境准备

### 3.1 一次性环境（self-contained from a fresh checkout）

```bash
# 1) 安装依赖
cd /home/lfl/pg-mcp/src && uv sync --extra dev

# 2) 起 PostgreSQL fixture 容器（postgres:16-alpine, :5433）
cd /home/lfl/pg-mcp && cd fixtures && make docker-up

# 3) 起 Redis 容器（redis:7-alpine, :6380）
docker run -d --rm --name pg-mcp-redis -p 6380:6379 redis:7-alpine

# 4) 构建并校验 3 个测试库
cd /home/lfl/pg-mcp/fixtures \
  && PSQL='docker exec -i pg-mcp-fixtures psql' \
     SUPER_PSQL='docker exec pg-mcp-fixtures psql' \
     PG_USER=test PGPASSWORD=test \
     make all && make verify
```

### 3.2 环境变量（pg-mcp 进程读取）

```bash
export OPENAI_API_KEY=sk-test-stub                       # 可为占位；mock 路径不会调用
export PG_DATABASES='mini_blog,shop_oms,analytics_dw'
export DATABASE_URL_MINI_BLOG=postgresql://test:test@127.0.0.1:5433/mini_blog
export DATABASE_URL_SHOP_OMS=postgresql://test:test@127.0.0.1:5433/shop_oms
export DATABASE_URL_ANALYTICS_DW=postgresql://test:test@127.0.0.1:5433/analytics_dw
export REDIS_URL=redis://127.0.0.1:6380/0
```

### 3.3 拆环境

```bash
docker rm -f pg-mcp-redis pg-mcp-fixtures || true
```

---

## 4. 命令快查（self-contained 命令表 + 实测耗时）

| ID | 用途 | 命令 | 实测耗时 |
|---|---|---|---|
| Q-1 | 全量测试 | `cd /home/lfl/pg-mcp/src && uv run pytest tests/ -W ignore` | ≈21 s |
| Q-2 | 仅单元 | `cd /home/lfl/pg-mcp/src && uv run pytest tests/unit/ -W ignore` | ≈3 s |
| Q-3 | 仅集成 | `cd /home/lfl/pg-mcp/src && uv run pytest tests/integration/ -W ignore` | ≈10 s |
| Q-4 | 仅 e2e | `cd /home/lfl/pg-mcp/src && uv run pytest tests/e2e/ -W ignore` | ≈5 s |
| Q-5 | 全量+覆盖率 | `cd /home/lfl/pg-mcp/src && uv run pytest --cov=pg_mcp --cov-report=term-missing tests/ -W ignore` | ≈22 s |
| Q-6 | 行+分支 | `cd /home/lfl/pg-mcp/src && uv run pytest --cov=pg_mcp --cov-branch tests/ -W ignore` | ≈22 s |
| Q-7 | 验收 | `cd /home/lfl/pg-mcp && bash fixtures/acceptance.sh` | <5 s |
| Q-8 | 单文件（黄金矩阵） | `cd /home/lfl/pg-mcp/src && uv run pytest tests/unit/test_sql_validator.py -v` | <2 s |
| Q-9 | 单文件（LLM） | `cd /home/lfl/pg-mcp/src && uv run pytest tests/unit/test_sql_generator.py -v` | <1 s |

---

## 5. 单元测试（Layer 2，实测 235 用例）

### 5.1 `test_sql_validator.py` — SQL 安全黄金矩阵（80 tests，P0）

| Class | 用例数 | 数据源 |
|---|---|---|
| `TestPassCases` | 22 | `tests/fixtures/sql_samples.py::PASS_CASES` |
| `TestFailCases` | 32 | `FAIL_CASES` |
| `TestParseFailures` | 3 | `PARSE_FAIL_CASES` |
| `TestForeignTables` | 5 (2 参数化 + 3 plain) | `FOREIGN_TABLE_CASES` + 内联 |
| `TestFunctionWhitelist` | 4 | 内联 |
| `TestEdgeCases` | 7 | 内联 |

跑：`cd /home/lfl/pg-mcp/src && uv run pytest tests/unit/test_sql_validator.py -v`
覆盖率：当前 92% line+branch，目标 100%（§16.1 P1）。

### 5.2 `test_db_inference.py`（17）

`DbInference` 摘要构建 / 失效；TopK 关键词；自动总结刷新。
跑：`cd /home/lfl/pg-mcp/src && uv run pytest tests/unit/test_db_inference.py -v`

### 5.3 `test_orchestrator.py`（21）

`QueryEngine` 编排：DB 推断、schema 加载、`E_SCHEMA_NOT_READY + retry_after_ms`、生成-校验-执行重试链、Result Validator 接入、错误传播。
跑：`cd /home/lfl/pg-mcp/src && uv run pytest tests/unit/test_orchestrator.py -v`

### 5.4 `test_result_validator.py`（29）

层级 deny-list (`db.schema.table.column` 通配) / 行级 mask / `metadata_only` 降级 / verdict 决策。
跑：`cd /home/lfl/pg-mcp/src && uv run pytest tests/unit/test_result_validator.py -v`

### 5.5 `test_sql_generator.py` — LLM SQL 生成（P0，14 tests，行/分支覆盖 100%）

> 状态：**Verified Today**。文件已存在，使用 `unittest.mock.AsyncMock` 全 mock OpenAI client。

| ID | 用例 | 期望 |
|---|---|---|
| G-1 | 正常生成（含 token 提取） | 返回 SQL；`prompt_tokens` / `completion_tokens` 由 `response.usage` 注入；`avg_logprob` 默认 None |
| G-2 | Markdown fence 清理（参数化 5 种：```sql、plain ```、trailing newline、whitespace、no fence） | 输出 SQL 前后无 ``` 包裹；空白被 strip |
| G-3 | OpenAI 超时（`asyncio.TimeoutError`） | 抛 `LlmTimeoutError` |
| G-4 | OpenAI APIError | 抛 `LlmError`，message 含原文 |
| G-5 | feedback 注入 prompt（重试链路） | user message 含 `Previous attempt feedback: ...` |
| G-6 | 无 feedback prompt 不泄漏占位符 | 不包含 `feedback` 占位 |
| G-7 | 透传 `openai_model` 配置 | `chat.completions.create` 调用 `model=配置值` |
| G-8 | model 输出为 `None` | 返回空 SQL，不抛 |
| G-9 | `response.usage` 缺失 | token 计数置 0，不抛 |
| G-10 | prompt template constant 契约 | 含 `{schema_context}` `{query}` `{feedback}` `SELECT` `Do not use any functions that modify data` |

跑：`cd /home/lfl/pg-mcp/src && uv run pytest tests/unit/test_sql_generator.py -v`

### 5.6 其他单元

| 文件 | 用例数 | 覆盖点 |
|---|---|---|
| `test_config.py` | 30 | Pydantic settings 校验、SecretStr、URL 解析、PG_DATABASES 列表语义 |
| `test_sanitizer.py` | 17 | 字面量遮蔽（`'***'`）、PII mask、SQL 串脱敏 |
| `test_schema_retriever.py` | 22 | 关键词索引、TopK 检索、压缩比 |
| `test_rate_limit.py` | 5 | per-database semaphore 计数 |

跑：`cd /home/lfl/pg-mcp/src && uv run pytest tests/unit/ -v`

---

## 6. 集成测试（Layer 3，实测 75 用例）

### 6.1 `test_app.py`（3）

FastAPI app：`/health` 200、`/admin/refresh` 三库刷新、未知路由 404。

### 6.2 `test_pool.py`（16）

asyncpg pool：`PG_DATABASES` override、连接重试退避、SSL 参数、超时、健康检查、`SET LOCAL` 隔离。

### 6.3 `test_schema_discovery.py`（13）

SQLAlchemy `inspect()` 发现：表 / 视图 / 索引 / 枚举 / 复合类型 / 外键，对照 mini_blog / shop_oms / analytics_dw 实际计数。

### 6.4 `test_schema_cache.py`（17）

Redis 缓存读写、gzip 压缩、observer hooks（loaded / invalidated）、状态机迁移、singleflight、warmup 后台任务。

### 6.5 `test_sql_executor.py`（26）

只读事务 + `statement_timeout` + `work_mem`；`SET LOCAL search_path` 在事务内、随事务 rollback；LIMIT 软/硬截断；EXPLAIN 列名；标识符引用；超时映射。

跑：`cd /home/lfl/pg-mcp/src && uv run pytest tests/integration/ -v`

---

## 7. 端到端（Layer 4，实测 20 用例）

### 7.1 `test_mcp_tool.py`（20）

通过 `server._server.request_handlers[CallToolRequest]` 直驱 MCP handler：

- `query` 工具：成功路径、`E_SCHEMA_NOT_READY` 重试提示、SQL 校验失败重试 → 最终 `E_SQL_INVALID`
- `refresh_schema` 管理工具
- 未知工具名 → `result.root.isError=True`（注意：MCP server 包装层把 `McpError` 转 `CallToolResult`，**不会** raise）
- `QueryResponse` 必含 `request_id`、`database`、`validation_used`

跑：`cd /home/lfl/pg-mcp/src && uv run pytest tests/e2e/ -v`

---

## 8. 验收测试（Layer 5）— `fixtures/acceptance.sh`

### 8.1 入口

```bash
cd /home/lfl/pg-mcp && bash fixtures/acceptance.sh
```

脚本已 checked-in，无任何占位变量；每个检查项是真实的 Python 调用。

### 8.2 检查项

| ID | 主题 | 关键断言 |
|---|---|---|
| A1 | Schema discovery | 三库的 tables/views/idx/enums/comp/fks 实际计数命中 |
| A2 | 检索阈值 | `should_use_retrieval`：mini_blog=False, shop_oms=False, analytics_dw=True |
| A3 | SQL Executor | LIMIT wrap 截断；EXPLAIN 列名；`SET LOCAL search_path` 命中 customers=200 |
| A4 | QueryEngine 全链路 | `mini_blog` 自然语言 → SQL → 行结果 |
| A5 | Prompt context size | full vs retrieval 字节数与压缩比，analytics_dw 压缩 92% |

### 8.3 实测输出（2026-05-01）

```
=== A1 schema discovery ===
  mini_blog      tables=6, views=1, idx=18, enums=1, comp=0, fks=7 OK
  shop_oms       tables=19, views=3, idx=61, enums=7, comp=1, fks=15 OK
  analytics_dw   tables=64, views=4, idx=135, enums=11, comp=2, fks=23 OK
=== A2 retrieval threshold ===
  mini_blog      should_use_retrieval=False OK
  shop_oms       should_use_retrieval=False OK
  analytics_dw   should_use_retrieval=True OK
=== A3 SQL executor ===
  A3.1 LIMIT wrap: row_count=5 truncated=True OK
  A3.2 EXPLAIN: columns=['QUERY PLAN'] OK
  A3.3 search_path: customers=200 OK
=== A4 QueryEngine full pipeline ===
  A4 ok: rows=[[18]] db=mini_blog
=== A5 prompt context size ===
  mini_blog      full=2073 no retrieval OK
  shop_oms       full=5824 no retrieval OK
  analytics_dw   full=13431 retrieval=1109 compression=92% OK
=== ALL ACCEPTED ===
```

---

## 9. 烟雾测试（Layer 6）

| ID | 命令 | 期望 |
|---|---|---|
| S-1 | `cd /home/lfl/pg-mcp/src && uv run pg-mcp --transport stdio < /dev/null` | exit 0（stdin EOF 时干净退出） |
| S-2 | `cd /home/lfl/pg-mcp/src && uv run pg-mcp --transport sse &`，然后 `curl -s http://127.0.0.1:8000/health` | `{"status":"ok"}` |
| S-3 | `curl -s -X POST http://127.0.0.1:8000/admin/refresh` | `{"succeeded":["mini_blog","shop_oms","analytics_dw"],"failed":[]}` |

均在 2026-05-01 实跑通过。

---

## 10. SQL 校验黄金矩阵

### 10.1 必须通过（PASS — 22 cases）

`basic_select, select_star, select_where, cte_simple, cte_multiple, aggregate_group_by, join_query, left_join, subquery, union_query, explain_select, explain_verbose, window_function, select_from_values, distinct_select, order_by_limit, safe_function_upper, safe_function_count, safe_function_coalesce, safe_function_date_trunc, intersect_query, except_query, case_expression`

来源：`src/tests/fixtures/sql_samples.py::PASS_CASES`，由 `TestPassCases` 参数化覆盖。

### 10.2 必须拒绝 — 完整 34 deny functions

> Design §4.8 `DENY_FUNCTIONS` 共 34 项；当前 `FAIL_CASES` 命名覆盖 12 项（每 family 至少 1 个），其余 22 项靠 family-cover 兜底，列入 §16.2 GAP-DENY。

| Family | Function | 当前命名覆盖 |
|---|---|---|
| file_system | `pg_read_file` | yes (`func_pg_read_file`) |
| file_system | `pg_read_binary_file` | yes (`func_pg_read_binary_file`) |
| file_system | `pg_ls_dir` | yes (`func_pg_ls_dir`) |
| file_system | `pg_stat_file` | no (Planned) |
| large_object | `lo_import` | yes (`func_lo_import`) |
| large_object | `lo_export` | yes (`func_lo_export`) |
| large_object | `lo_get` | no (Planned) |
| large_object | `lo_put` | no (Planned) |
| sleep | `pg_sleep` | yes (`func_pg_sleep`) |
| advisory_lock | `pg_advisory_lock` | yes (`func_pg_advisory_lock`) |
| advisory_lock | `pg_advisory_xact_lock` | no (Planned) |
| advisory_lock | `pg_advisory_unlock` | no (Planned) |
| advisory_lock | `pg_advisory_unlock_all` | no (Planned) |
| advisory_lock | `pg_try_advisory_lock` | no (Planned) |
| advisory_lock | `pg_try_advisory_xact_lock` | no (Planned) |
| notify | `pg_notify` | yes (`func_pg_notify`) |
| notify | `pg_listening_channels` | no (Planned) |
| external | `dblink` | yes (`func_dblink`) |
| external | `dblink_exec` | yes (`func_dblink_exec`) |
| external | `dblink_connect` | no (Planned) |
| external | `dblink_disconnect` | no (Planned) |
| external | `dblink_send_query` | no (Planned) |
| external | `dblink_get_result` | no (Planned) |
| process | `pg_terminate_backend` | yes (`func_pg_terminate_backend`) |
| process | `pg_cancel_backend` | no (Planned) |
| config | `pg_reload_conf` | no (Planned) |
| config | `pg_rotate_logfile` | no (Planned) |
| config | `set_config` | yes (`func_set_config`) |
| config | `current_setting` | no (Planned) |
| wal | `pg_switch_wal` | no (Planned) |
| wal | `pg_create_restore_point` | no (Planned) |

合计：**12 / 34 命名覆盖**。其余 22 项的 family-cover 在 §16.2 P2 计入扩展。

### 10.3 statement-level 必拒（来自 `FAIL_CASES`，共 32）

| 类型 | 数量 | 用例 ID |
|---|---|---|
| DML | 7 | `insert, insert_select, update, update_where, delete, delete_where, truncate` |
| DDL | 5 | `drop_table, drop_index, create_table, alter_table, create_index` |
| Privilege | 2 | `grant, revoke` |
| COPY | 3 | `copy_to, copy_from, copy_program` |
| Multi-statement | 2 | `multi_statement, multi_select` |
| Blacklisted functions | 12 | 见 §10.2 [yes] 行 |
| EXPLAIN ANALYZE | 2 | `explain_analyze, explain_analyze_verbose` |
| CALL / 其他 | 3 | `call_procedure, cte_with_insert, select_with_drop`（最后一项触发 `E_SQL_PARSE`） |

PARSE_FAIL（3）：`invalid_syntax, unclosed_string, missing_paren`
FOREIGN_TABLE（2）：`select_foreign_table, select_foreign_qualified`

### 10.4 当前覆盖现状

12/34 deny function 有 named test，其余 22 个靠 family-cover 兜底；该差距登记为 §17 **GAP-DENY**，§16.2 P2 跟进。

---

## 11. 覆盖率门禁

### 11.1 当前实测（live, 2026-05-01）

- 行覆盖：**86%**
- 行+分支：**85%**

### 11.2 起步门禁（PR 必跑，今天可过 — `--cov-fail-under=80`）

```bash
cd /home/lfl/pg-mcp/src && uv run pytest --cov=pg_mcp --cov-fail-under=80 tests/ -W ignore
```

### 11.3 模块级最小（标注当前过 / 未过）

| Module | 实测 | 起步门禁 | 状态 |
|---|---|---|---|
| `pg_mcp/__init__.py` | 100% | 100% | OK |
| `pg_mcp/__main__.py` | 0% | — (immediate-exec entry) | exempt |
| `pg_mcp/app.py` | 83% | 80% | OK |
| `pg_mcp/cli.py` | 0% | — (Planned §16.1) | exempt-now |
| `pg_mcp/config.py` | 100% | 95% | OK |
| `pg_mcp/db/pool.py` | 92% | 85% | OK |
| `pg_mcp/engine/db_inference.py` | 90% | 85% | OK |
| `pg_mcp/engine/orchestrator.py` | 82% | 80% | OK |
| `pg_mcp/engine/result_validator.py` | 93% | 85% | OK |
| `pg_mcp/engine/sql_executor.py` | 100% | 90% | OK |
| `pg_mcp/engine/sql_generator.py` | 100% | 90% | OK |
| `pg_mcp/engine/sql_validator.py` | 92% (branch) | **100% (CLAUDE.md target)** | **GAP** |
| `pg_mcp/models/errors.py` | 100% | 100% | OK |
| `pg_mcp/models/request.py` | 100% | 100% | OK |
| `pg_mcp/models/response.py` | 100% | 100% | OK |
| `pg_mcp/models/schema.py` | 72% | 80% | **GAP** |
| `pg_mcp/observability/logging.py` | 83% | 80% | OK |
| `pg_mcp/observability/metrics.py` | 57% | 80% | **GAP** |
| `pg_mcp/observability/sanitizer.py` | 100% | 100% | OK |
| `pg_mcp/protocols.py` | 100% | 100% | OK |
| `pg_mcp/schema/cache.py` | 77% | 80% | **GAP** (close) |
| `pg_mcp/schema/discovery.py` | 88% | 85% | OK |
| `pg_mcp/schema/retriever.py` | 86% | 85% | OK |
| `pg_mcp/schema/state.py` | 100% | 100% | OK |
| `pg_mcp/server.py` | 89% | 85% | OK |
| **TOTAL** | **85%** | **80%** | OK |

GAP 项已登记到 §17。

---

## 12. 质量门禁

### 12.1 过渡门禁（每个 PR；今天可过 —— 仓库根；BASE_REF 参数化）

```bash
# Run from repo root; do not assume cwd=src.
cd /home/lfl/pg-mcp

# 0) lockfile 未漂移
test -z "$(git -C src status --porcelain uv.lock)" || { echo "uv.lock dirty"; exit 1; }

# 1) tests + 80% line coverage
( cd src && uv run pytest --cov=pg_mcp --cov-fail-under=80 tests/ -W ignore )

# 2) golden security matrix
( cd src && uv run pytest tests/unit/test_sql_validator.py -v )

# 3) PR-diff lint/typecheck (parameterizable base ref; fall back to origin/master if origin/HEAD 未设)
BASE_REF="${BASE_REF:-$(git symbolic-ref --quiet --short refs/remotes/origin/HEAD 2>/dev/null || echo origin/master)}"
git diff --name-only "${BASE_REF}..." \
  | grep -E '^src/.*\.py$' \
  | sed 's|^src/||' \
  | xargs -r -I{} sh -c 'cd src && uv run ruff check {}'
git diff --name-only "${BASE_REF}..." \
  | grep -E '^src/pg_mcp/.*\.py$' \
  | sed 's|^src/||' \
  | xargs -r -I{} sh -c 'cd src && uv run mypy {}'
```

> 全仓 `ruff check` / `ruff format --check` / `mypy` 当前未过（141 / 30 / 33），**目标门禁**（清零）见 §16.1 P1。

---

## 13. 需求 — 测试 Traceability matrix

> 所有 P0/P1 PRD/Design/Impl 条目至少占一行；未实现条目标 `Planned`，并指向 §16 backlog 项。共 34 行。

| # | Requirement | Source | Canonical test | Layer | Status |
|---|---|---|---|---|---|
| 1 | Cold-start E_SCHEMA_NOT_READY + retry_after_ms | PRD §3.1 | `tests/unit/test_orchestrator.py::TestErrorPropagation` | unit | Verified |
| 2 | Schema lazy state machine | PRD §3.1 + Design §6 | `tests/integration/test_schema_cache.py::TestStateMachine` | integration | Verified |
| 3 | Singleflight loader | PRD §3.1 + Design §6 | `tests/integration/test_schema_cache.py::TestSingleflight` | integration | Verified |
| 4 | Background warmup | Impl §5.5 | `tests/integration/test_schema_cache.py::TestWarmup` | integration | Verified |
| 5 | TTL expiry reload | Design §6 | (Planned: §16.1) | integration | Planned |
| 6 | Redis LRU policy（委托给 Redis 配置） | PRD §3.1 + Design §6 | n/a — `redis-cli config get maxmemory-policy` | config | Out-of-test |
| 7 | SQL deny-list functions（family-cover） | PRD §3.3 + Design §4.8 | `tests/unit/test_sql_validator.py::TestFailCases` | unit | Verified (12/34 named) |
| 8 | Multi-statement reject | Design §8.2 | `TestFailCases[multi_statement, multi_select]` | unit | Verified |
| 9 | Foreign table reject | PRD §3.3 | `TestForeignTables` | unit | Verified |
| 10 | EXPLAIN ANALYZE reject | Design §8.2 | `TestFailCases[explain_analyze, explain_analyze_verbose]` | unit | Verified |
| 11 | Function whitelist via schema | PRD §3.3 | `TestFunctionWhitelist` | unit | Verified |
| 12 | Statement timeout enforcement | Design §4.9 | `tests/integration/test_sql_executor.py::TestExecute` | integration | Verified |
| 13 | LIMIT wrap (soft/hard) | Design §4.9 | `TestLimitWrapping` | integration | Verified |
| 14 | Identifier quoting | Design §4.9 | `TestQuoteIdent` | integration | Verified |
| 15 | LLM timeout mapping | Impl §2.4 | `tests/unit/test_sql_generator.py` G-3 | unit | Verified |
| 16 | LLM APIError mapping | Impl §2.4 | G-4 | unit | Verified |
| 17 | Feedback retry prompt | Impl §5.5 | G-5 | unit | Verified |
| 18 | Markdown fence stripping | Impl §2.4 | G-2 (5 参数化) | unit | Verified |
| 19 | Token usage extraction | Impl §2.4 | G-1, G-9 | unit | Verified |
| 20 | Result row truncation (soft) | Design §4.4 | `tests/integration/test_sql_executor.py::TestResultProcessing` | integration | Verified |
| 21 | Result row truncation (hard) | Design §4.4 | `TestResultProcessing` | integration | Verified |
| 22 | QueryResponse field: `request_id` | PRD §3.5 | `tests/e2e/test_mcp_tool.py::TestResponseFormat` | e2e | Verified |
| 23 | QueryResponse field: `validation_used` | PRD §3.5 | `tests/unit/test_orchestrator.py::TestResultValidation` | unit | Verified |
| 24 | QueryResponse field: `warnings` | PRD §3.5 | (Planned: §16.2) | unit | Planned |
| 25 | Log event: `request_received` | Design §7.1 | A4 acceptance | integration | Verified (manually) |
| 26 | Log event: `sql_generated` | Design §7.1 | A4 acceptance | integration | Verified (manually) |
| 27 | Log event: `sql_executed` | Design §7.1 | A4 acceptance | integration | Verified (manually) |
| 28 | CLI startup discovers DBs only（不预加载 schema） | PRD §3.1 | (Planned: §16.1 — `test_cli_lifecycle.py`) | integration | Planned |
| 29 | CLI signal handling / clean shutdown | Impl §5.5 | (Planned: §16.1) | integration | Planned |
| 30 | SSE transport lifecycle | Impl §5.5 | (Planned: §16.1) | e2e | Planned |
| 31 | `PG_DATABASES` override discover | Impl §5.5 | `tests/integration/test_pool.py::TestDiscoverDatabases` | integration | Verified |
| 32 | Connection retry / 退避 | Impl §5.5 | `TestRetryLogic` | integration | Verified |
| 33 | Hierarchical deny-list (`db.*.*`) | Design §5 | `tests/unit/test_result_validator.py::TestHierarchicalDenyList` | unit | Verified |
| 34 | MCP unknown tool → `isError=True` | PRD §3.5 | `tests/e2e/test_mcp_tool.py::TestErrorHandling` | e2e | Verified |

---

## 14. 响应 / 可观测性契约矩阵

### 14.1 `QueryResponse` 字段契约（13 字段）

| Field | 何时 set | 默认 | Canonical test |
|---|---|---|---|
| `request_id` | always | UUID4 | `tests/e2e/test_mcp_tool.py::TestResponseFormat` |
| `database` | always（编排器决议出 DB 后） | n/a | `tests/unit/test_orchestrator.py` |
| `sql` | 当生成成功 | None | `tests/e2e/test_mcp_tool.py` |
| `columns` | 有结果集 | None | `tests/integration/test_sql_executor.py::TestResultProcessing` |
| `column_types` | 有结果集 | None | `TestResultProcessing` |
| `rows` | 有结果集 | None | `TestResultProcessing` |
| `row_count` | 有结果集 | None | `TestResultProcessing` |
| `truncated` | 有结果集 | False | `TestLimitWrapping` |
| `truncated_reason` | `truncated=True` | None | `TestLimitWrapping` |
| `validation_used` | always（result validator 接入与否） | False | `tests/unit/test_orchestrator.py::TestResultValidation` |
| `schema_loaded_at` | always | n/a | (Planned: §16.2) |
| `warnings` | 有降级 / mask / metadata-only 时 | `[]` | (Planned: §16.2) |
| `error` | 失败路径 | None | `tests/e2e/test_mcp_tool.py::TestErrorHandling` |

### 14.2 结构化日志事件契约（8 事件）

| Event | 必含字段 | A4 实测可见 | Canonical test |
|---|---|---|---|
| `request_received` | `request_id, query_length, database, return_type` | Yes | Planned (explicit log assertion §16.1) |
| `schema_loaded` | `database, table_count, cache_hit, elapsed_ms` | partly (`table_count`+`elapsed_ms`) | Planned |
| `sql_generated` | `attempt, prompt_tokens, completion_tokens, logprob, elapsed_ms` | Yes (`logprob=None`) | Planned |
| `sql_validation_failed` | `attempt, reason` | A4 未覆盖 | Planned |
| `sql_executed` | `row_count, truncated, elapsed_ms` | Yes | Planned |
| `result_validated` | `verdict, attempt, data_policy, denied, elapsed_ms` | A4 未覆盖（validator 关闭） | Planned |
| `request_completed` | `total_elapsed_ms` | Yes | Planned |
| `request_failed` | `error_code, error_message` | A4 未覆盖 | Planned |

A4 中均含 `request_id`。CLI 启动日志单独需 §16.1 验证。

---

## 15. Fixture 计数索引

| DB | tables | views | mat-views | idx | enums | comp | fks | rows |
|---|---|---|---|---|---|---|---|---|
| `mini_blog` | 6 | 1 | 0 | 18 | 1 | 0 | 7 | ~200 |
| `shop_oms` | 19 | 3 | 2 | 61 | 7 | 1 | 15 | ~8 000 |
| `analytics_dw` | 64 | 4 | 3 | 135 | 11 | 2 | 23 | ~165 000 |

`analytics_dw` >50 表，触发 `SCHEMA_MAX_TABLES_FOR_FULL_CONTEXT` 检索路径。

---

# Part B — Planned Additions

> 以下条目尚未在 2026-05-01 的自验中跑通或未实现；不计入 §0 总数与 §11 当前覆盖。

## 16. Backlog by priority

### 16.1 P1（block 下一次 PR）

- **CLI lifecycle 测试**（新增 `tests/integration/test_cli_lifecycle.py`）：`cli.py` 当前 0%；覆盖 startup 仅 discover DBs（不预加载 schema）、signal handling / clean shutdown、PG_DATABASES 解析失败的退出码。**对齐 traceability 28/29。**
- **SSE lifecycle 测试**（恢复 P1 对齐 Impl §5.5）：FastAPI app 启动 → /sse 建链 → MCP message 双向 → 优雅关闭。**对齐 traceability 30。**
- `sql_validator.py` branch 92% → 100%（CLAUDE.md 安全关键路径硬要求）。
- TTL expiry reload 集成测试（**对齐 traceability 5**）。
- 质量门禁清零：`ruff check` 141 → 0；`ruff format --check` 30 → 0；`mypy` 33 → 0。
- 显式日志事件断言（`request_received` / `sql_generated` / `sql_executed` / `request_completed`），把 §14.2 全部 Planned 行落地。

### 16.2 P2

- **GAP-DENY**：扩展 `FAIL_CASES` 覆盖全部 34 deny functions（当前 12，新增 22 named test）。
- `COPY ... TO PROGRAM`、`postgres_fdw` 显式 case（PRD §3.3 明列但目前只靠 `copy_program` 与 deny-function family 兜底）。
- Runtime abuse PG-backed 测试：递归 CTE 触发 `statement_timeout`；`generate_series(1, 1e9)` 触发硬上限；大排序触发 `temp_file_limit`；窗口 abuse；`search_path` 含引号/逗号的注入字符串。
- Foreign-table 真 fixture（当前 `TestForeignTables` 仅校验语法层拒绝）。
- `metrics.py` 57% → ≥80%；`models/schema.py` 72% → ≥80%。
- 性能/负载基准（含 200 表 schema 反序列化 < 5 ms 断言）。
- CI 矩阵：Python 3.12 / 3.13 × PG 15 / 16 / 17。
- bypass-LIMIT 回归测试。

### 16.3 P3

- 真 OpenAI sql_generator manual smoke（脱离 mock，跑一次 mini_blog 的 happy path）。

---

## 17. 已知缺口表

| ID | Item | Owner section | Detail |
|---|---|---|---|
| GAP-1 | `cli.py` 0% 覆盖 | §16.1 | 无 lifecycle 测试 |
| GAP-2 | SSE lifecycle 无自动测试 | §16.1 | Impl §5.5 列为高优先级 |
| GAP-3 | `sql_validator.py` 分支 92% | §16.1 | CLAUDE.md 要求 100% |
| GAP-4 | TTL expiry reload 未测 | §16.1 | Design §6 |
| GAP-5 | Lint / format / mypy 未清零 | §16.1 | 141 / 30 / 33 |
| GAP-6 | 日志事件无显式断言 | §16.1 | A4 仅人眼观察 |
| GAP-DENY | 22/34 deny function 仅 family-cover | §16.2 | 见 §10.2 |
| GAP-7 | Runtime abuse 未自动化 | §16.2 | 递归 CTE / 大 series / 大排序 / window abuse / search_path 注入 |
| GAP-8 | `metrics.py` 57% | §16.2 | 计时器 / token 计数路径 |
| GAP-9 | `models/schema.py` 72% | §16.2 | DatabaseSchema 序列化分支 |
| GAP-10 | `schema/cache.py` 77%（差 80% 起步门禁） | §16.2 | 异常路径 |
| GAP-11 | `warnings` / `schema_loaded_at` 无显式契约断言 | §16.2 | §14.1 |
| GAP-12 | Foreign-table 真 fixture 缺失 | §16.2 | 仅语法层 |

---

## 18. 变更记录

| 日期 | 修订 | 说明 |
|---|---|---|
| 2026-05-01 | v2 | 按 Review-0008 重写：拆分 Part A/B；新增 §5.5 sql_generator 计划、§13 traceability、§14 响应/可观测契约；§10.2 列出全部 34 deny；§12.1 修正为仓库根 + `BASE_REF` 参数化；所有命令 self-contained；总数从旧版的旧值更新为 330；移除占位变量 |
| 2026-04-?? | v1 | 初版（已被本版替换） |

---

## 附录 A：常见失败排查

| 现象 | 可能原因 | 处理 |
|---|---|---|
| `pytest` 报 `ModuleNotFoundError: pg_mcp` | 未在 `src/` 下运行 | `cd /home/lfl/pg-mcp/src && uv run pytest ...` |
| 集成测试连不上 PG | 未启容器或端口冲突 | `cd fixtures && make docker-up` 后确认 `:5433` |
| 集成测试连不上 Redis | 未启 redis 容器 | `docker run -d --rm --name pg-mcp-redis -p 6380:6379 redis:7-alpine` |
| `acceptance.sh` 卡住 | 三库未 `make verify` | 重跑 §3.1 第 4 步 |
| `mypy` 报 `error: Cannot find implementation` | 路径未带 `src` 前缀 | 见 §12.1 的 `sed 's|^src/||'` 处理 |
| MCP e2e 期望 raise 却失败 | MCP server 把 `McpError` 转为 `CallToolResult(isError=True)` | 断言 `result.root.isError`，不要 `pytest.raises` |
