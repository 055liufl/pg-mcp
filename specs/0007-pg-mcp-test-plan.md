# Test-Plan-0007: pg-mcp 测试计划

> **依据**：[PRD-0001](./0001-pg-mcp-prd.md) · [Design-0002](./0002-pg-mcp-design.md) · [Impl-Plan-0004](./0004-pg-mcp-impl-plan.md) · [Impl-Review-0006](./0006-pg-mcp-impl-review.md)
> **状态**：可直接执行；本计划撰写时已完成 self-verify（详见 §13）。
> **范围**：覆盖 PRD §3 全部功能需求 + Design §8 测试策略 + Impl §5 测试清单 +
> 在 fixtures 上的真实数据库验收 + 部署前 smoke。

---

## 0. 摘要

| 维度 | 实测数字（2026-05-01） |
|---|---|
| 测试总数 | 316 |
| 单元 / 集成 / E2E | 221 / 75 / 20 |
| 通过率 | 100 %（`uv run pytest tests/` 316 passed in ≈20 s） |
| 行覆盖率 | 84.86 %（`--cov=pg_mcp`） |
| 分支覆盖率 | 83.49 %（`--cov=pg_mcp --cov-branch`） |
| 关键模块行覆盖 | `executor` 100 % / `pool` 95 % / `sql_validator` 95 % / `result_validator` 94 % / `db_inference` 93 % / `discovery` 92 % |
| 已知盲区 | `cli.py` 0 %（启动生命周期）、`__main__.py` 0 %、`metrics.py` 57 %、`sql_generator.py` 34 %（LLM 主路径） |
| 已知质量门禁缺口 | `ruff check` 134 处、`ruff format --check` 29 文件待格式化、`mypy --strict` 33 处。**初始门禁先放宽到通过当前状态**，整改纳入 §16 GAP-7。 |

**整体目标**：在 PR 合入前，本计划列出的全部 P0 / P1 用例必须通过；P2 用例至少
在 `master` 上跑通。覆盖率门禁（**初始**）：整体 ≥ 80 %（line）、≥ 80 %（branch）；
**目标**：≥ 85 % / ≥ 85 %。`sql_validator.py` 100 % 分支必达。

---

## 1. 目标 / 非目标

### 1.1 目标
- **正确性**：所有 PRD §3 列出的功能需求都有对应的可执行测试。
- **安全性**：SQL 校验黑/白名单 + foreign table + 多语句 + EXPLAIN ANALYZE 拒绝
  必须 100 % 命中（详见 §10 必测矩阵）。
- **稳定性**：连接池重试、`SchemaCache` singleflight、`SchemaCache.refresh()` 完成性、
  `RateLimitedError` 限流均覆盖到。
- **可重现**：每条命令都贴出来；失败时贴期望输出对比。
- **真实数据**：使用 `fixtures/` 下的三个数据库做集成与验收，避免"全部 mock"
  导致的虚假绿。

### 1.2 非目标
- **不**测 OpenAI 真实 API 行为（成本 + 不确定性）；用 `MockSqlGenerator` /
  `MockResultValidator` 替代。**E2E** 提供单条带真实 LLM 的回归用例（手动跑，
  不入 CI）。
- **不**测 schema 变更下的迁移行为（pg-mcp 是只读访问层，不感知 DDL）。
- **不**做模糊测试 / 攻击面对抗（独立任务，超出测试计划范围）。
- **不**提供 K8s / 多副本部署测试（运行时无状态，不变形）。

### 1.3 与 Impl-Plan §5.5 缺失测试清单的对照
| Impl §5.5 项 | 本计划位置 |
|---|---|
| 信号量并发限流 | §5.1 (`test_rate_limit.py`) ✅ 已存在 |
| Schema cache singleflight 竞态 | §6.4 (`test_schema_cache.py`) ✅ 已存在 |
| 定时刷新调度 | §6.4 测项 R-3 ✅ 已存在 |
| SSE transport 生命周期 | §7.2（设为 P2，非阻塞） |
| CLI 启动/关闭生命周期 | §7.3（**新增**，本计划补齐） |
| `PG_DATABASES` 覆盖发现 | §6.2 测项 P-2 ✅ 已存在 |
| 连接重试退避 | §6.2 测项 P-3 ✅ 已存在 |
| 结果验证 fix 路径重试 | §5.3 测项 V-3 ✅ 已存在 |
| 大 LIMIT 绕过防护 | §5.4 测项 X-3 ✅ 已存在 |
| Foreign table 引号处理 | §5.4 测项 X-4 ✅ 已存在 |

---

## 2. 测试金字塔

```
                 ┌──────────────┐
                 │ 6  Smoke     │  ≤  5  cases (60s 部署后回归)
                 ├──────────────┤
                 │ 5  Acceptance│  ≤ 15  cases (2-5 min, real PG via fixtures)
                 ├──────────────┤
                 │ 4  E2E       │  ≤ 30  cases (MCP 协议 + handler)
                 ├──────────────┤
                 │ 3  Integration│ ≤ 100  cases (PG/Redis 必需)
                 ├──────────────┤
                 │ 2  Unit      │  ≥ 220  cases (无外部依赖)
                 └──────────────┘
```

约束：**每层增加用例时，下层必须已经覆盖该路径**。例如 SQL 校验规则永远先在
单元层加（最便宜），同时在 acceptance 层留一个 end-to-end 印证。

---

## 3. 环境与前置条件

### 3.1 一次性环境准备

```bash
# 仓库根目录
cd /home/lfl/pg-mcp

# 1) 安装 Python 依赖（Python ≥ 3.12）
cd src && uv sync --extra dev

# 2) 起一份测试用 PostgreSQL（已封装在 fixtures/Makefile）
cd ../fixtures && make docker-up

# 3) 加载 3 套 fixture 数据库
make all PG_PORT=5433 PG_USER=test PGPASSWORD=test \
  PSQL='docker exec -i pg-mcp-fixtures psql' \
  SUPER_PSQL='docker exec pg-mcp-fixtures psql'

# 4) 起一个 Redis 测试实例（集成测试需要）
docker run -d --rm --name pg-mcp-redis -p 6380:6379 redis:7-alpine
```

### 3.2 环境变量（导出到 shell 或 `.env`）

```bash
export PG_HOST=localhost
export PG_PORT=5433
export PG_USER=test
export PG_PASSWORD=test
export PG_DATABASES=mini_blog,shop_oms,analytics_dw
export REDIS_URL=redis://localhost:6380/0
export OPENAI_API_KEY=sk-test-dummy           # 单元/集成测试不会真发
export OPENAI_BASE_URL=                       # 默认即可
```

### 3.3 拆环境

```bash
cd /home/lfl/pg-mcp/fixtures && make docker-down
docker rm -f pg-mcp-redis 2>/dev/null
```

---

## 4. 命令快查

| 目的 | 命令 | 期望耗时 |
|---|---|---|
| 全量测试 | `cd src && uv run pytest tests/ -W ignore` | < 30 s |
| 仅单元 | `uv run pytest tests/unit/` | < 5 s |
| 仅集成 | `uv run pytest tests/integration/` | < 30 s |
| 仅 e2e | `uv run pytest tests/e2e/` | < 5 s |
| 覆盖率 | `uv run pytest --cov=pg_mcp --cov-report=term-missing tests/` | < 30 s |
| Lint | `uv run ruff check pg_mcp/ tests/` | < 5 s |
| Type | `uv run mypy pg_mcp/` | < 30 s |
| 验收 | 见 §8 | 1-3 min |
| 烟雾 | 见 §9 | < 60 s |

---

## 5. 单元测试（Layer 2）

> **目录**：`tests/unit/` · **依赖**：仅 `unittest.mock` · **预算**：< 10 s · **现状**：221 用例

### 5.1 `test_sql_validator.py` — SQL 安全校验（P0，**必须 100 % 分支**）

| ID | 用例 | 期望 |
|---|---|---|
| V-1 | `tests/fixtures/sql_samples.py:PASS_CASES` 全量参数化 | 全部 `valid=True` |
| V-2 | `FAIL_CASES` 全量参数化 | 全部 `valid=False` 且 `code` 命中 |
| V-3 | `PARSE_FAIL_CASES` | `code == "E_SQL_PARSE"` |
| V-4 | `FOREIGN_TABLE_CASES`（含 schema 限定与未限定） | `valid=False`，原因含 `Foreign table` |
| V-5 | EXPLAIN 通过 / EXPLAIN ANALYZE 拒绝 | `is_explain` 标记正确 |
| V-6 | 函数白名单（schema-driven） | 不在 `allowed_functions` 时拒绝 |
| V-7 | 黑名单兜底（即使在白名单中也拒） | `pg_sleep`/`pg_read_file`/`dblink` 必拒 |
| V-8 | 多语句拒绝 | `SELECT 1; DROP TABLE x` → `valid=False` |
| V-9 | CTE 中嵌入 INSERT/UPDATE/DELETE | `Disallowed statement type` |
| V-10 | `schema_names` 解析未限定表 | 未限定 `orders` 命中 `app.orders` 时正确判 foreign |
| V-11 | 函数名大小写不敏感 | `PG_SLEEP(100)` 也被拒 |

**运行**：`uv run pytest tests/unit/test_sql_validator.py -v`
**通过条件**：80/80 用例通过；分支覆盖 = 100 %（用 `--cov=pg_mcp.engine.sql_validator --cov-branch`）。

### 5.2 `test_db_inference.py` — 数据库推断（P0）

| ID | 用例 | 期望 |
|---|---|---|
| I-1 | 仅 1 个库命中 | 直接选定 |
| I-2 | top1/top2 差距 < 15 % | `DbInferAmbiguousError` + candidates ≤ 3 |
| I-3 | 0 命中 | `DbInferNoMatchError` |
| I-4 | 关键词分散到多库 | `CrossDbUnsupportedError` |
| I-5 | 部分库未就绪 | 若仅就绪库可命中 → 选；否则 `SchemaNotReadyError(retry=3000)` |
| I-6 | `DbSummary` 增量构建 | 命中 hook 后 summary 不重复发现 |
| I-7 | 关键词提取过滤停用词 | 长度 < 2 / 在 stopwords 中均被剔除 |

**运行**：`uv run pytest tests/unit/test_db_inference.py -v`
**通过条件**：17/17。

### 5.3 `test_orchestrator.py` — 主编排（P0）

| ID | 用例 | 期望 |
|---|---|---|
| O-1 | Happy path（结果模式） | `QueryResponse.row_count > 0`、`error is None` |
| O-2 | `return_type=sql` 跳过执行 | `executor.execute_calls == 0`，`response.rows is None` |
| O-3 | `admin_action=refresh_schema` | 返回 `refresh_result`；不发起 LLM |
| O-4 | LLM 生成失败重试上限 → `SqlGenerateError` | 重试 N+1 次后抛出 |
| O-5 | 校验失败 → 重试 → 通过 | generator 被调用 2 次 |
| O-6 | 结果验证 verdict=fix 形成闭环 | generator 与 executor 各被调用 2 次 |
| O-7 | `schema_names` 在所有 `executor.execute()` 调用中出现 | 包含 `public` |
| O-8 | `validator.validate(sql, schema, schema_names=...)` 也收到 schema_names | 是 |
| O-9 | 信号量满 → `RateLimitedError` | 是 |
| O-10 | `SqlTimeoutError` 透传，不转换 | 是 |
| O-11 | 大 schema 启用检索路径 | `SchemaRetriever.retrieve()` 被调用 |

**运行**：`uv run pytest tests/unit/test_orchestrator.py -v`
**通过条件**：21/21。

### 5.4 `test_result_validator.py` — 结果验证 + deny-list（P0）

| ID | 用例 | 期望 |
|---|---|---|
| R-1 | `enable_validation=False` → 不触发 | `should_validate==False` |
| R-2 | `JOIN ≥ 2` / 子查询 / 窗口函数 → 触发 | `True` |
| R-3 | 空集 → 触发 | `True` |
| R-4 | logprob < 阈值 → 触发 | `True` |
| R-5 | data_policy=metadata_only → prompt 不含 sample rows | 是 |
| R-6 | data_policy=masked → 敏感列脱敏 | `password=***`, email 掩码 |
| R-7 | 层级 deny rule（db.*.*） | 强制降级到 metadata_only |
| R-8 | 层级 deny rule（db.schema.table） | 仅当 SQL 触及该表才降级 |
| R-9 | 层级 deny rule（db.schema.table.column） | 仅遮罩匹配列 |
| R-10 | `*.public.users` 通配 db | 任意 db 中的 public.users 都降级 |

**运行**：`uv run pytest tests/unit/test_result_validator.py -v`
**通过条件**：29/29。

### 5.5 其他单元用例

| 文件 | 关注点 | 预期用例 |
|---|---|---|
| `test_config.py` | env / .env / SecretStr / 列表解析 | 30 |
| `test_sanitizer.py` | SQL 字符串 `'***'` 替换 / 邮箱手机号 / 身份证 | 17 |
| `test_schema_retriever.py` | TableIndex 构建、retrieve top-N、related FK | 22 |
| `test_rate_limit.py` | Semaphore 满载 / 退出后释放 | 5 |

**全单元运行**：`uv run pytest tests/unit/ -W ignore`
**通过条件**：221/221，运行时长 < 10 s。

---

## 6. 集成测试（Layer 3）

> **目录**：`tests/integration/` · **依赖**：PG（端口 5433） + Redis（端口 6380），均通过 mock 或真实容器
> **预算**：< 60 s · **现状**：75 用例（部分 mock，部分用 fixtures）

### 6.1 `test_app.py` — FastAPI SSE 路由

| ID | 用例 | 期望 |
|---|---|---|
| A-1 | `GET /health` | 200，`{"status":"ok"}` |
| A-2 | `POST /admin/refresh` | 200，正确 JSON shape |
| A-3 | `GET /admin/refresh` | 405（仅 POST） |

### 6.2 `test_pool.py` — 连接池

| ID | 用例 | 期望 |
|---|---|---|
| P-1 | per-DB 连接池缓存 | 第二次返回同一对象 |
| P-2 | `PG_DATABASES` 覆盖自动发现 | discover 跳过 pg_database 查询 |
| P-3 | 连接失败指数退避（mock） | 5 次重试，含抖动 |
| P-4 | `assert_readonly` 检测写权限 → strict 模式抛错 | 是 |
| P-5 | DSN 含 sslmode / sslrootcert | 是 |

### 6.3 `test_schema_discovery.py` — Schema 发现

| ID | 用例 | 期望 |
|---|---|---|
| D-1 | 批量发现表 + 列 + 主键 | 与 mini_blog fixture 计数一致（6 表） |
| D-2 | foreign table 标记 | mini_blog 无 → 0；shop_oms 无 → 0 |
| D-3 | 索引、约束、外键、enum、composite 全部加载 | 与 §15 fixture-counts 一致 |
| D-4 | view 含列（不再返回 `[]`） | shop_oms 的 `in_stock_products` 列数 ≥ 7 |
| D-5 | `allowed_functions` 排除黑名单 | `pg_sleep` ∉ 集合，`length` ∈ 集合 |

> **note**：当前 `test_schema_discovery.py` 用 mock；本计划新增 §8 acceptance
> 直接对 fixtures 验真。

### 6.4 `test_schema_cache.py` — Singleflight + 状态机 + 定时刷新

| ID | 用例 | 期望 |
|---|---|---|
| C-1 | 并发 get 同一 db | `_do_load` 仅调用 1 次 |
| C-2 | UNLOADED → LOADING → READY 状态序列 | mock_redis 看到 `loading`、`ready` |
| C-3 | 加载失败 → FAILED + 错误持久化 | `pg_mcp:error:{db}` 中可读出消息 |
| C-4 | bytes vs str 解码（regression） | `b"ready"` 也能正确进入 READY 分支 |
| C-5 | 损坏数据触发重载 | gzip 反序列化失败 → state 重置为 UNLOADED → 再次加载 |
| C-6 | `refresh()` 必须等待真正完成（regression） | 返回的 `RefreshResult.succeeded` 反映最终状态 |
| C-7 | 加载完成 hook 触发 | `add_loaded_hook` 被调用 1 次 |
| C-8 | 失败/刷新 hook 触发 | `add_invalidated_hook` 被调用 |
| C-9 | warmup 触发所有库 | 每个 db 至少 1 次 _do_load |

### 6.5 `test_sql_executor.py` — 只读执行

| ID | 用例 | 期望 |
|---|---|---|
| E-1 | LIMIT 包裹普通 SELECT | 实际执行的 SQL 包含 `__pg_mcp_q LIMIT N+1` |
| E-2 | EXPLAIN 跳过 LIMIT 包裹 | 原 SQL 直接执行 |
| E-3 | 设置 statement_timeout / work_mem / temp_file_limit | 4 条 SET 全部命中 |
| E-4 | `SET LOCAL search_path` **在事务内** | 调用顺序：`BEGIN ... SET LOCAL ... fetch ... COMMIT` |
| E-5 | 标识符引号化 | `users.addresses` → `"users","addresses"` 等 |
| E-6 | 软阈值截断 → `truncated=True` | 不抛错 |
| E-7 | 硬阈值 → `ResultTooLargeError` | 抛错 |
| E-8 | 单元格超 `MAX_CELL_BYTES` 截断 | 字段后缀 `... [truncated]` |
| E-9 | asyncpg.QueryCanceledError → `SqlTimeoutError` | 转换正确 |

**运行整层**：
```bash
cd /home/lfl/pg-mcp/src
PG_HOST=localhost PG_PORT=5433 PG_USER=test PG_PASSWORD=test \
REDIS_URL=redis://localhost:6380/0 \
  uv run pytest tests/integration/ -W ignore
```
**通过条件**：75/75，运行时长 < 60 s。

---

## 7. 端到端测试（Layer 4）

> **目录**：`tests/e2e/` · **依赖**：mock（默认）；可手动切到真实 LLM。
> **现状**：20 用例

### 7.1 `test_mcp_tool.py` — MCP 协议合规

| ID | 用例 | 期望 |
|---|---|---|
| M-1 | `ListToolsRequest` 返回 1 个 `query` 工具 | inputSchema 含 `query/database/return_type/admin_action` |
| M-2 | `CallToolRequest{name="query"}` happy path | 返回 1 条 `TextContent`，含 `QueryResponse.model_dump_json()` |
| M-3 | 业务错误转 ErrorDetail | DbNotFoundError → `error.code == "E_DB_NOT_FOUND"` |
| M-4 | 未知 tool → `isError=True`，正文含 "Unknown tool" | 不抛 McpError，封装在 result.root |
| M-5 | 缺参 → `isError=True` | 同上 |
| M-6 | `admin_action=refresh_schema` happy path | `response.refresh_result` 非空 |
| M-7 | `return_type=sql` 不执行 SQL | `executor.execute_calls == 0` |

### 7.2 SSE 传输（P2，可选）
- 当前不在自动 CI 中（异步 stream 测试复杂、收益有限）。
- 验收时手动跑：
  ```bash
  pg-mcp --transport sse &
  curl -N http://localhost:8000/sse  # 应保持长连接
  ```

### 7.3 CLI 生命周期（**新增 P1**）

新增测试文件 `tests/integration/test_cli_lifecycle.py`，覆盖：
- 启动 → 后台 warmup task 创建 → 收 SIGTERM → 取消 + 等待 + close pool/redis
- `--transport stdio` 立即返回（stdio_server 等待 stdin）
- `--transport sse` 启动 uvicorn，收信号后干净关闭

**预期断言**：
- 进程退出码 0
- 没有 `Task was destroyed but it is pending!` 警告（用 `-W error::ResourceWarning` 跑）
- pool 与 redis 客户端的 `close*()` 都被调用恰好 1 次

**P0 / P1 / P2 标签**：本节 7.1 / 7.3 是 P0，7.2 是 P2。

**运行**：`uv run pytest tests/e2e/ -W ignore`
**通过条件**：20/20。

---

## 8. 验收测试（Layer 5）— 真实数据库 + 全管道

> 在 `fixtures/` 提供的 3 个真实数据库上端到端验证 pg-mcp 的核心能力。
> 不依赖 OpenAI（用 `MockSqlGenerator`），但 PostgreSQL 是真的。

### 8.1 验收 A1：Schema 发现 in vivo

```bash
# 在 src/ 内运行
cd /home/lfl/pg-mcp/src
PG_HOST=localhost PG_PORT=5433 PG_USER=test PG_PASSWORD=test \
  uv run python - <<'PY'
import asyncio
from pg_mcp.config import Settings
from pg_mcp.db.pool import ConnectionPoolManager
from pg_mcp.schema.discovery import SchemaDiscovery

async def main():
    s = Settings(pg_host="localhost", pg_port=5433, pg_user="test",
                 pg_password="test", openai_api_key="dummy")
    p = ConnectionPoolManager(s)
    try:
        for db, exp_tables in [("mini_blog", 6), ("shop_oms", 19), ("analytics_dw", 64)]:
            schema = await SchemaDiscovery(p, s).load_schema(db)
            assert len(schema.tables) == exp_tables, (db, len(schema.tables))
            print(f"{db:14s}: {len(schema.tables)} tables / {len(schema.views)} views / "
                  f"{len(schema.indexes)} idx / {len(schema.enum_types)} enums / "
                  f"{len(schema.composite_types)} composites / "
                  f"{len(schema.foreign_keys)} fks")
    finally:
        await p.close_all()

asyncio.run(main())
PY
```

**期望输出**（行级匹配）：
```
mini_blog     : 6 tables / 1 views / 18 idx / 1 enums / 0 composites / 7 fks
shop_oms      : 19 tables / 3 views / 61 idx / 7 enums / 1 composites / 15 fks
analytics_dw  : 64 tables / 4 views / 135 idx / 11 enums / 2 composites / 23 fks
```

> ⚠ 数字必须**完全一致**；任何 ±1 偏差说明 fixture 或 discovery 出了 regression。

### 8.2 验收 A2：检索路径触发阈值

```bash
PG_HOST=localhost PG_PORT=5433 PG_USER=test PG_PASSWORD=test \
  uv run python - <<'PY'
import asyncio
from pg_mcp.config import Settings
from pg_mcp.db.pool import ConnectionPoolManager
from pg_mcp.schema.discovery import SchemaDiscovery
from pg_mcp.schema.retriever import SchemaRetriever

async def main():
    s = Settings(pg_host="localhost", pg_port=5433, pg_user="test",
                 pg_password="test", openai_api_key="dummy")
    p = ConnectionPoolManager(s)
    r = SchemaRetriever(max_tables_for_full=s.schema_max_tables_for_full_context)
    try:
        for db, expected in [("mini_blog", False), ("shop_oms", False), ("analytics_dw", True)]:
            schema = await SchemaDiscovery(p, s).load_schema(db)
            actual = r.should_use_retrieval(schema)
            assert actual == expected, (db, actual, expected)
            print(f"{db}: should_use_retrieval={actual}  (threshold={s.schema_max_tables_for_full_context})")
    finally:
        await p.close_all()

asyncio.run(main())
PY
```

**期望**：3 行打印，最后一行 `analytics_dw: should_use_retrieval=True`。

### 8.3 验收 A3：SQL 校验+执行 against fixtures

不要再 mock 数据库；直接打到真实 PG，验证只读事务、LIMIT 包裹、`SET LOCAL`。

```bash
PG_HOST=localhost PG_PORT=5433 PG_USER=test PG_PASSWORD=test \
  uv run python - <<'PY'
import asyncio
from pg_mcp.config import Settings
from pg_mcp.db.pool import ConnectionPoolManager
from pg_mcp.engine.sql_executor import SqlExecutor

async def main():
    s = Settings(pg_host="localhost", pg_port=5433, pg_user="test",
                 pg_password="test", openai_api_key="dummy", max_rows=5)
    p = ConnectionPoolManager(s)
    try:
        ex = SqlExecutor(p, s)
        # 普通 SELECT — 应被 LIMIT 包裹到 6 行
        r = await ex.execute("mini_blog",
                             "SELECT id, title FROM posts ORDER BY id",
                             schema_names=["public"])
        assert r.row_count == 5 and r.truncated is True, r
        print(f"A3.1 ok: row_count={r.row_count} truncated={r.truncated}")

        # EXPLAIN — 跳过 LIMIT 包裹
        r = await ex.execute("mini_blog",
                             "EXPLAIN SELECT * FROM posts",
                             schema_names=["public"], is_explain=True)
        assert any("Seq Scan" in str(c) or "QUERY PLAN" in str(c) for c in r.columns + [r.rows])
        print(f"A3.2 ok: EXPLAIN columns={r.columns}")

        # 多 schema — search_path 必须含两个 schema
        r = await ex.execute("shop_oms",
                             "SELECT count(*) FROM customers",  # users.customers
                             schema_names=["public", "users"])
        assert r.rows[0][0] >= 100
        print(f"A3.3 ok: customers count={r.rows[0][0]}")
    finally:
        await p.close_all()

asyncio.run(main())
PY
```

**期望**：3 行 `ok` 输出，无异常。

### 8.4 验收 A4：QueryEngine 全管道（mock LLM + 真 PG）

```bash
cd /home/lfl/pg-mcp/src
PG_HOST=localhost PG_PORT=5433 PG_USER=test PG_PASSWORD=test \
REDIS_URL=redis://localhost:6380/0 \
  uv run python - <<'PY'
import asyncio
import redis.asyncio as redis
from pg_mcp.config import Settings
from pg_mcp.db.pool import ConnectionPoolManager
from pg_mcp.engine.orchestrator import QueryEngine
from pg_mcp.engine.sql_executor import SqlExecutor
from pg_mcp.engine.sql_validator import SqlValidator
from pg_mcp.engine.db_inference import DbInference
from pg_mcp.engine.result_validator import ResultValidator
from pg_mcp.schema.cache import SchemaCache
from pg_mcp.schema.retriever import SchemaRetriever
from pg_mcp.models.request import QueryRequest
from tests.conftest import MockSqlGenerator, MockResultValidator

async def main():
    s = Settings(pg_host="localhost", pg_port=5433, pg_user="test", pg_password="test",
                 redis_url="redis://localhost:6380/0",
                 pg_databases="mini_blog,shop_oms,analytics_dw",
                 openai_api_key="dummy",
                 enable_validation=False)
    pool = ConnectionPoolManager(s)
    rcl  = redis.from_url(s.redis_url)
    cache = SchemaCache(rcl, pool, s)
    cache.set_discovered_databases(s.pg_databases_list)
    await cache.refresh()        # 同步加载所有 fixtures

    sql_gen = MockSqlGenerator(sql="SELECT count(*) AS posts FROM posts")
    engine = QueryEngine(
        sql_generator=sql_gen,
        sql_validator=SqlValidator(),
        sql_executor=SqlExecutor(pool, s),
        schema_cache=cache,
        db_inference=DbInference(cache, s),
        result_validator=MockResultValidator(),
        retriever=SchemaRetriever(s.schema_max_tables_for_full_context),
        settings=s,
    )

    resp = await engine.execute(QueryRequest(query="how many posts", database="mini_blog"))
    assert resp.error is None, resp.error
    assert resp.row_count == 1
    assert resp.rows[0][0] == 18  # mini_blog has 18 posts
    print(f"A4 ok: {resp.rows} from {resp.database}")

    await pool.close_all()
    await rcl.aclose()

asyncio.run(main())
PY
```

**期望**：`A4 ok: [[18]] from mini_blog`，无异常。

### 8.5 验收 A5：不同规模库的 prompt 体积

```bash
# 在 src/ 内
PG_HOST=localhost PG_PORT=5433 PG_USER=test PG_PASSWORD=test \
  uv run python - <<'PY'
import asyncio
from pg_mcp.config import Settings
from pg_mcp.db.pool import ConnectionPoolManager
from pg_mcp.schema.discovery import SchemaDiscovery
from pg_mcp.schema.retriever import SchemaRetriever

async def main():
    s = Settings(pg_host="localhost", pg_port=5433, pg_user="test",
                 pg_password="test", openai_api_key="dummy")
    p = ConnectionPoolManager(s)
    r = SchemaRetriever(max_tables_for_full=s.schema_max_tables_for_full_context)
    try:
        for db in ("mini_blog", "shop_oms", "analytics_dw"):
            schema = await SchemaDiscovery(p, s).load_schema(db)
            full = schema.to_prompt_text()
            # 中等以下用 full；analytics_dw 用 retrieval
            if r.should_use_retrieval(schema):
                ctx = r.retrieve("top revenue products this quarter", schema)
                print(f"{db}: full={len(full)}  retrieval={len(ctx)}  "
                      f"compression={(len(full) - len(ctx)) / len(full) * 100:.0f}%")
            else:
                print(f"{db}: full={len(full)}  (no retrieval)")
    finally:
        await p.close_all()

asyncio.run(main())
PY
```

**期望**：
- `mini_blog: full≈2073` (no retrieval)
- `shop_oms:  full≈5824` (no retrieval)
- `analytics_dw: full≈13431  retrieval≈1100  compression≈92%`

数字允许 ±10 %（avoid brittle，因为 random() 影响 attributes JSON 长度）。

### 8.6 验收套件聚合脚本

把 8.1 ~ 8.5 收纳到 `fixtures/acceptance.sh`：
```bash
#!/usr/bin/env bash
set -euo pipefail
echo "=== A1 schema discovery ===";   bash -c "$A1"
echo "=== A2 retrieval threshold ==="; bash -c "$A2"
echo "=== A3 executor ===";          bash -c "$A3"
echo "=== A4 full pipeline ===";     bash -c "$A4"
echo "=== A5 prompt sizes ===";      bash -c "$A5"
echo "=== ALL ACCEPTED ==="
```

**通过条件**：脚本退出码 0；`grep -c "ok"` 输出 ≥ 5。

---

## 9. 烟雾测试（Layer 6）— 部署后入口校验

部署后必须在 ≤ 60 s 内通过。前置：§3 环境已启动；`pg-mcp` 命令在 PATH 中
（或用 `uv run pg-mcp`）。

```bash
cd /home/lfl/pg-mcp/src
export PG_HOST=localhost PG_PORT=5433 PG_USER=test PG_PASSWORD=test
export PG_DATABASES=mini_blog,shop_oms,analytics_dw
export REDIS_URL=redis://localhost:6380/0
export OPENAI_API_KEY=sk-test-dummy

# S-1: stdio transport — 收到 EOF (</dev/null) 后干净退出
timeout 5 uv run pg-mcp --transport stdio < /dev/null > /tmp/pg-mcp.out 2>&1
[ $? -eq 0 ] && echo "S-1 ok" || echo "S-1 FAIL"
# 注：进程退出码即可，不依赖具体日志事件——
#     当前 stdlib logging 默认 WARNING 级别，info 事件会被过滤（GAP-8）。

# S-2: SSE transport + /health
uv run pg-mcp --transport sse > /tmp/pg-mcp-sse.out 2>&1 &
PID=$!
for i in 1 2 3 4 5; do
  curl -fsS http://localhost:8000/health 2>/dev/null && break
  sleep 1
done
echo
curl -fsS http://localhost:8000/health | grep -q '"status":"ok"' && echo "S-2 ok" || echo "S-2 FAIL"

# S-3: /admin/refresh 返回 succeeded/failed 列表
curl -fsS -X POST http://localhost:8000/admin/refresh > /tmp/refresh.out
grep -q "succeeded" /tmp/refresh.out && echo "S-3 ok" || echo "S-3 FAIL"

kill $PID 2>/dev/null; wait 2>/dev/null
```

**通过条件**：3 行 `S-* ok`。

**实测输出**（2026-05-01 self-verify）：
```
S-1 ok
{"status":"ok"}
S-2 ok
{"succeeded":["mini_blog","shop_oms","analytics_dw"],"failed":[]}
S-3 ok
```

---

## 10. SQL 校验黄金矩阵（P0，**必须 100 % 命中**）

来自 PRD §3.3 + Design §8.2 + Impl §2.1。所有用例已纳入
`tests/fixtures/sql_samples.py`，参数化运行：

### 10.1 必须通过

| SQL | 理由 |
|---|---|
| `SELECT 1` | 基本 |
| `SELECT * FROM users WHERE id = 1` | WHERE |
| `WITH cte AS (SELECT id FROM users) SELECT * FROM cte` | CTE |
| `SELECT COUNT(*), department FROM employees GROUP BY department` | 聚合 |
| `EXPLAIN SELECT * FROM orders` | EXPLAIN |
| `EXPLAIN (VERBOSE, COSTS) SELECT * FROM orders` | EXPLAIN with options |
| `SELECT name, ROW_NUMBER() OVER (PARTITION BY dept) FROM employees` | 窗口 |
| `SELECT * FROM (VALUES (1, 'a')) AS t(id, name)` | VALUES |

### 10.2 必须拒绝（详见 `FAIL_CASES`）

| SQL | code |
|---|---|
| `INSERT INTO users VALUES (1, 'x')` | E_SQL_UNSAFE |
| `UPDATE users SET name = 'x'` | E_SQL_UNSAFE |
| `DELETE FROM users` | E_SQL_UNSAFE |
| `DROP TABLE users` | E_SQL_UNSAFE |
| `TRUNCATE users` | E_SQL_UNSAFE |
| `CREATE TABLE x (i int)` | E_SQL_UNSAFE |
| `ALTER TABLE users ADD c int` | E_SQL_UNSAFE |
| `GRANT SELECT ON users TO PUBLIC` | E_SQL_UNSAFE |
| `COPY users TO '/tmp/dump'` | E_SQL_UNSAFE |
| `CALL my_proc()` | E_SQL_UNSAFE |
| `EXPLAIN ANALYZE SELECT * FROM users` | E_SQL_UNSAFE（执行查询） |
| `SELECT 1; DROP TABLE x` | E_SQL_UNSAFE（多语句） |
| `SELECT pg_sleep(100)` | E_SQL_UNSAFE（黑名单） |
| `SELECT pg_read_file('/etc/passwd')` | E_SQL_UNSAFE |
| `SELECT dblink('host=evil','SELECT 1')` | E_SQL_UNSAFE |
| `SELECT lo_import('/etc/passwd')` | E_SQL_UNSAFE |
| `SELECT * FROM foreign_table_x` | E_SQL_UNSAFE（外表） |
| `WITH cte AS (INSERT INTO logs VALUES (1) RETURNING id) SELECT * FROM cte` | E_SQL_UNSAFE（CTE 内 DML） |

**运行**：`uv run pytest tests/unit/test_sql_validator.py::TestPassCases tests/unit/test_sql_validator.py::TestFailCases tests/unit/test_sql_validator.py::TestForeignTables -v`
**通过条件**：所有参数化用例通过；任何新增黑名单/白名单变更**必须同步**更新 `sql_samples.py`。

---

## 11. 覆盖率门禁

### 11.1 当前状态（2026-05-01 实测）

```bash
cd /home/lfl/pg-mcp/src
uv run pytest --cov=pg_mcp --cov-report=term-missing tests/ -W ignore
# →  TOTAL  84.86 % (line)

uv run pytest --cov=pg_mcp --cov-branch --cov-report=term-missing tests/ -W ignore
# →  TOTAL  83.49 % (line + branch)
```

### 11.2 起步门禁（**当前可达**，每次 CI 跑）

```bash
uv run pytest --cov=pg_mcp --cov-fail-under=80 tests/ -W ignore
```
**通过条件**：整体行覆盖率 ≥ 80 %。本计划撰写时 84.86 % 通过。

### 11.3 目标门禁（**6 周内达成**）

```bash
uv run pytest --cov=pg_mcp --cov-branch --cov-fail-under=85 tests/ -W ignore
```

### 11.4 模块级最小覆盖率（PR diff 内若涉及）

| 模块 | 行 | 分支 | 当前 |
|---|---|---|---|
| `pg_mcp/engine/sql_validator.py` | 100 % | 100 % | 95 / 89 |
| `pg_mcp/engine/sql_executor.py`  | 95 %  | 90 %  | 100 / 100 ✅ |
| `pg_mcp/engine/orchestrator.py`  | 90 %  | 85 %  | 85 / 84 |
| `pg_mcp/engine/result_validator.py` | 90 % | 85 % | 94 / 91 ✅ |
| `pg_mcp/engine/db_inference.py`  | 90 %  | 85 %  | 93 / 88 ✅ |
| `pg_mcp/schema/discovery.py`     | 90 %  | 85 %  | 92 / 79 |
| `pg_mcp/schema/cache.py`         | 80 %  | 75 %  | 78 / 75 |
| `pg_mcp/schema/retriever.py`     | 85 %  | 80 %  | 88 / 76 |
| `pg_mcp/models/*.py`             | 95 %  | 90 %  | 79 / —（`schema.py` 79 %） |
| **整体（含 cli/app）**            | **80 % → 85 %** | **80 % → 85 %** | **85 / 83** |

`cli.py` / `app.py` / `__main__.py` 当前不计入硬门禁（生命周期测试覆盖；
见 §7.3）。`models/schema.py` 79 % 因 `to_prompt_text()` / `to_summary_text()`
中部分分支（罕见类型组合）未覆盖；可在补 §7.3 时一起加。

---

## 12. 质量门禁（合入前）

> **当前现实**：`ruff check`、`ruff format --check`、`mypy --strict` 都报错
> （详见 §0 / §16 GAP-7）。本节定义"目标"门禁，并提供一个**今天就能用**
> 的"过渡"门禁。

### 12.1 过渡门禁（每个 PR 必跑，**当前可通过**）

按顺序、全部通过才允许合入：

```bash
cd /home/lfl/pg-mcp/src

# 0) lockfile 未漂移
test -z "$(git status --porcelain uv.lock)" || (echo "uv.lock dirty"; exit 1)

# 1) 测试 + 起步覆盖率（80 %）
uv run pytest --cov=pg_mcp --cov-fail-under=80 tests/ -W ignore

# 2) 安全：必测矩阵 100 % 通过（已在 §10）
uv run pytest tests/unit/test_sql_validator.py -v

# 3) PR diff 中变动文件的 ruff & mypy（不阻塞已有 debt）
git diff --name-only origin/master... | grep -E '\.py$' | \
  xargs -r uv run ruff check
git diff --name-only origin/master... | grep -E '\.py$' | \
  grep '^src/pg_mcp/' | xargs -r uv run mypy
```

### 12.2 目标门禁（**6 周内**全量通过）

```bash
cd /home/lfl/pg-mcp/src

# 1) 全仓库 ruff lint
uv run ruff format --check pg_mcp/ tests/
uv run ruff check pg_mcp/ tests/

# 2) 全 src 类型检查
uv run mypy pg_mcp/

# 3) 测试 + 目标覆盖率（85 % 含分支）
uv run pytest --cov=pg_mcp --cov-branch --cov-fail-under=85 tests/ -W ignore

# 4) 安全
uv run pytest tests/unit/test_sql_validator.py -v
```

**任何一步失败 → PR 不可合**。

### 12.3 自动化建议

把 §12.1 的命令配成 GitHub Actions `lint-test` job；§12.2 的目标门禁配成
`quality-strict`，先放在 `continue-on-error: true` 直到全绿。

---

## 13. CI 矩阵

| 维度 | 取值 | 备注 |
|---|---|---|
| Python | 3.12 / 3.13 / 3.14 | 都跑全量 |
| PostgreSQL | 14 / 15 / 16 / 17 | acceptance + integration 跑 |
| Redis | 7 | 整套 |
| 传输 | stdio / sse | smoke 各跑 1 次 |

任意 (Python, PG) 组合失败 → 整个 PR 红。

---

## 14. 自验执行记录（self-verify）

**运行环境**：撰写本计划时已端到端跑通；下文记录可复现的实际命令与输出。

```bash
# §3.1 一次性环境 — 所有 3 步成功
cd /home/lfl/pg-mcp/src && uv sync --extra dev
cd ../fixtures && make docker-up        # → "==> pg-mcp-fixtures is ready."
make all PG_PORT=5433 PG_USER=test PGPASSWORD=test \
  PSQL='docker exec -i pg-mcp-fixtures psql' \
  SUPER_PSQL='docker exec pg-mcp-fixtures psql'    # → 36 s, 全部 OK
docker run -d --rm --name pg-mcp-redis -p 6380:6379 redis:7-alpine    # → PONG
```

```bash
# §4 全量测试
cd /home/lfl/pg-mcp/src && uv run pytest tests/ -W ignore
# → 316 passed in 20.06s
```

```bash
# §11.1 实测覆盖率
uv run pytest --cov=pg_mcp tests/ -W ignore
# → TOTAL 84.86 % (line)
uv run pytest --cov=pg_mcp --cov-branch tests/ -W ignore
# → TOTAL 83.49 % (line+branch)
```

```bash
# §8.1 acceptance A1（实跑）
PG_HOST=localhost PG_PORT=5433 PG_USER=test PG_PASSWORD=test \
  uv run python <<<'... §8.1 脚本 ...'
```
→ 实际打印（**与 §15 fixture-counts 完全一致**）：
```
mini_blog     : 6 tables / 1 views / 18 idx / 1 enums / 0 composites / 7 fks
shop_oms      : 19 tables / 3 views / 61 idx / 7 enums / 1 composites / 15 fks
analytics_dw  : 64 tables / 4 views / 135 idx / 11 enums / 2 composites / 23 fks
```

```bash
# §8.2 检索阈值 — 实测：
mini_blog: should_use_retrieval=False  (threshold=50)
shop_oms: should_use_retrieval=False  (threshold=50)
analytics_dw: should_use_retrieval=True  (threshold=50)
```

```bash
# §8.3 executor — 实测：
A3.1 ok: row_count=5 truncated=True
A3.2 ok: EXPLAIN columns=['QUERY PLAN']
A3.3 ok: customers count=200
```

```bash
# §8.4 全管道 — 实测：
A4 ok: [[18]] from mini_blog
```

```bash
# §8.5 prompt 体积 — 实测：
mini_blog: full=2073  (no retrieval)
shop_oms: full=5824  (no retrieval)
analytics_dw: full=13431  retrieval=1109  compression=92%
```

```bash
# §9 烟雾 — 实测：
S-1 ok
{"status":"ok"}
S-2 ok
{"succeeded":["mini_blog","shop_oms","analytics_dw"],"failed":[]}
S-3 ok
```

> **未跑过的小节（标注于 §16）**：
> - §7.3 CLI lifecycle 集成测试（P1，未实现，需要新增 `tests/integration/test_cli_lifecycle.py`）。
> - §13 CI 矩阵（GitHub Actions 配置；独立 PR）。
> - §11.3 / §12.2 目标门禁：覆盖率与 ruff/mypy 当前未达标；纳入 GAP-7/9。

---

## 15. Fixture 计数索引（与 §6.3 / §8.1 对照）

> 这些数字是 acceptance 与 discovery integration 测试的硬编码期望。
> 每次修改 `fixtures/*.sql` 必须同步更新此处与对应测试。

| | mini_blog | shop_oms | analytics_dw |
|---|---|---|---|
| schemas（含 public） | 1 | 4 | 5 |
| 表 | 6 | 19 | 64 |
| 视图（含 mat-view） | 1 | 3 | 4 |
| enum 类型 | 1 | 7 | 11 |
| composite 类型 | 0 | 1 | 2 |
| 索引（不含 PK 自动） | 7 | 17 | 70 |
| 索引（含 PK / unique 自动） | 18 | 61 | 135 |
| 外键 | 7 | 15 | 23 |
| 约束（CHECK + UNIQUE） | 37 | 134 | 307 |
| 数据行（合计） | ~200 | ~8,300 | ~165,000 |

---

## 16. 已知缺口 / 后续

| ID | 缺口 | 优先级 | 跟踪 |
|---|---|---|---|
| GAP-1 | `cli.py` 单元/集成 0 % 覆盖 | P1 | §7.3 待补 |
| GAP-2 | SSE stream 未自动化 | P2 | §7.2 |
| GAP-3 | 无负载/性能基准 | P2 | 可加 `pytest-benchmark` |
| GAP-4 | `sql_generator.py` 真实 LLM 行为未测 | P3 | 设计上 mock 即可，留 manual smoke |
| GAP-5 | `metrics.timed` 上下文管理器只覆盖 57 % | P3 | 配套 logger 测试时一起加 |
| GAP-6 | foreign-table fixture 缺失 | P2 | 加 `analytics_dw` 中的 postgres_fdw 链路 |
| GAP-7 | `ruff check` 134 / `ruff format` 29 / `mypy --strict` 33 | P1 | §12.2 目标门禁前必须清零 |
| GAP-8 | structlog 受 stdlib logging 默认 WARNING 过滤，info 事件不输出 | P2 | `configure_logging` 应同时调用 `logging.basicConfig(level=...)` |
| GAP-9 | 整体覆盖率距目标 85 % 还差 ≈0.1 % 行 / 1.5 % 分支 | P2 | 补 §7.3 + `metrics.py` 即可达成 |

---

## 17. 变更记录

| 日期 | 改动 |
|---|---|
| 2026-05-01 | 首版；与 0006 review 修复后代码对齐；含 fixtures-based acceptance |

---

## 附录 A：常见失败排查

| 现象 | 排查 |
|---|---|
| `psql: error: connection ... refused` | `make docker-up` 是否跑过、端口是否冲突 |
| 测试报 `Task was destroyed but it is pending!` | 用 `cd src && uv run pytest -W ignore`；非阻塞性，但应记录 |
| 覆盖率掉到 < 85 % | 看 `--cov-report=term-missing` 找新增未覆盖行 |
| `analytics_dw: ... composites=0` | pg-mcp 的 `_fetch_composite_types` 已修（commit `f6a2bcf`）；若回退则查 `t.typrelid` 关联 |
| acceptance A1 计数不匹配 | 检查 fixture SQL 是否被改、`make verify` 输出 |
