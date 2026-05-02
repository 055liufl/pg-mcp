# pg-mcp

PostgreSQL Natural Language Query MCP Server — 通过自然语言查询 PostgreSQL 数据库。

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

pg-mcp 是一个基于 [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) 的服务器，允许用户通过自然语言描述查询需求，系统自动将其转化为 SQL 语句并执行。支持 Claude Desktop、Cursor 等 MCP 客户端接入。

## 核心特性

- **自然语言转 SQL** — 基于 OpenAI GPT 模型，将中文/英文自然语言查询自动转换为 PostgreSQL SQL
- **多数据库自动发现** — 启动时自动发现 PostgreSQL 实例上的所有可访问数据库
- **Schema 智能缓存** — Redis -backed 缓存，支持懒加载、后台预热、定时刷新
- **大 Schema 检索** — 超过阈值（默认 50 表）时自动切换为检索模式，仅提取相关表/列上下文
- **SQL 安全校验** — 基于 SQLGlot AST 分析，语句白名单 + 函数黑名单 + DML 递归检测，确保只读
- **SQL 自动修正** — 校验失败或执行报错（未定义列/表/函数）时，自动将错误反馈给 LLM 重试
- **SQL 跨方言重写** — 自动将 BigQuery / MySQL / Snowflake 风格函数重写为 PostgreSQL 等价物
- **AI 结果验证**（可选）— 对复杂查询、空结果、低置信度 SQL 进行二次验证
- **双重传输协议** — 支持 stdio（Claude Desktop 默认）和 SSE（HTTP 远程接入）
- **只读安全** — 只读事务 + statement_timeout + work_mem 限制，三重防护

## 系统架构

```
┌─────────────────────────────────────────────────────────────┐
│                        MCP Clients                           │
│              (Claude Desktop / Cursor / etc.)                │
└──────────────┬──────────────────────┬───────────────────────┘
               │ stdio                │ SSE (HTTP)
┌──────────────▼──────────────────────▼───────────────────────┐
│                      Transport Layer                         │
│         StdioTransport  │  FastAPI + SseServerTransport      │
│                         │  GET /health  POST /admin/refresh  │
└──────────────┬──────────┴───────────────────────────────────┘
               │
┌──────────────▼──────────────────────────────────────────────┐
│                      MCP Server Layer                        │
│              mcp.Server  ──  Tool: "query"                   │
│         PgMcpError → QueryResponse.error (业务错误转换层)     │
└──────────────┬──────────────────────────────────────────────┘
               │
┌──────────────▼──────────────────────────────────────────────┐
│                      QueryEngine (Orchestrator)              │
│  ┌─────────┐  ┌──────────┐  ┌──────────┐  ┌─────────────┐  │
│  │ DbInfer │→│ SQLGen   │→│ SQLValid │→│ SQLExecutor │  │
│  │(Protocol)│  │(Protocol)│  │(Protocol)│  │ (Protocol)  │  │
│  └─────────┘  └──────────┘  └──────────┘  └──────┬──────┘  │
│                                                   │         │
│                                          ┌────────▼───────┐ │
│                                          │ ResultValidator│ │
│                                          │ (Protocol, opt)│ │
│                                          └────────────────┘ │
└──────────────┬──────────────────────────────────────────────┘
               │
┌──────────────▼──────────────────────────────────────────────┐
│                      Infrastructure Layer                    │
│  ┌───────────────┐  ┌───────────────┐  ┌─────────────────┐  │
│  │ ConnectionMgr │  │ SchemaCache   │  │ FunctionRegistry│  │
│  │ (asyncpg pool)│  │ (Redis)       │  │ (pg_proc 白名单) │  │
│  └───────────────┘  └───────────────┘  └─────────────────┘  │
└─────────────────────────────────────────────────────────────┘
               │                │
        ┌──────▼──────┐  ┌─────▼─────┐
        │ PostgreSQL  │  │   Redis   │
        │  Instances  │  │  Instance │
        └─────────────┘  └───────────┘
```

## 快速开始

### 前置依赖

- Python 3.12+
- PostgreSQL 14+（只读权限用户）
- Redis 7+（用于 schema 缓存）
- OpenAI API Key（支持官方接口和兼容接口）

### 安装

```bash
# 克隆仓库
git clone https://github.com/055liufl/pg-mcp.git
cd pg-mcp/src

# 使用 uv 安装依赖（推荐）
uv sync --extra dev

# 或使用 pip
pip install -e ".[dev]"
```

### 配置

复制 `.env.example` 为 `.env` 并填写你的配置：

```bash
cp ../.env.example ../.env
```

最小可运行配置：

```env
# PostgreSQL（只读用户）
PG_HOST=localhost
PG_PORT=5432
PG_USER=readonly_user
PG_PASSWORD=your_password

# OpenAI
OPENAI_API_KEY=sk-...
OPENAI_MODEL=gpt-5-mini

# Redis
REDIS_URL=redis://localhost:6379/0
```

完整配置项见 [配置说明](#配置说明)。

### 运行

**stdio 模式**（Claude Desktop 默认）：

```bash
pg-mcp --transport stdio
```

**SSE 模式**（HTTP 远程接入）：

```bash
pg-mcp --transport sse
# 服务启动在 http://0.0.0.0:8000
#  health: GET /health
#  SSE:     GET /sse
#  refresh: POST /admin/refresh
```

### 接入 Claude Desktop

在 `claude_desktop_config.json` 中添加：

```json
{
  "mcpServers": {
    "pg-mcp": {
      "command": "pg-mcp",
      "args": ["--transport", "stdio"],
      "env": {
        "PG_HOST": "localhost",
        "PG_PORT": "5432",
        "PG_USER": "readonly_user",
        "PG_PASSWORD": "your_password",
        "OPENAI_API_KEY": "sk-...",
        "REDIS_URL": "redis://localhost:6379/0"
      }
    }
  }
}
```

## MCP Tool API

服务器暴露单一 MCP Tool：`query`

### 输入参数

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `query` | string | 是 | 自然语言查询描述 |
| `database` | string | 否 | 目标数据库。未指定时自动推断 |
| `return_type` | string | 否 | `sql` 仅返回 SQL；`result` 执行并返回结果（默认） |
| `admin_action` | string | 否 | `refresh_schema` 刷新 schema 缓存 |

### 返回结构

```json
{
  "request_id": "uuid",
  "database": "mini_blog",
  "sql": "SELECT COUNT(*) AS total FROM posts",
  "columns": ["total"],
  "column_types": ["int"],
  "rows": [[18]],
  "row_count": 1,
  "truncated": false,
  "truncated_reason": null,
  "validation_used": false,
  "schema_loaded_at": "2026-05-02T08:57:29Z",
  "error": null
}
```

### 使用示例

```python
# 查询文章总数
{"query": "总共有多少篇文章", "database": "mini_blog", "return_type": "result"}

# 仅生成 SQL，不执行
{"query": "列出最早注册的 3 个用户", "database": "mini_blog", "return_type": "sql"}

# 自动推断数据库
{"query": "复购客户平均订单间隔天数", "return_type": "result"}

# 刷新 schema 缓存
{"database": "shop_oms", "admin_action": "refresh_schema"}
```

## 配置说明

所有配置通过环境变量或 `.env` 文件加载。优先级：环境变量 > `.env` > 默认值。

可通过 `PG_MCP_ENV_FILE` 环境变量指定自定义 `.env` 路径。

### 数据库连接

| 变量 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `PG_HOST` | string | `localhost` | PostgreSQL 主机 |
| `PG_PORT` | int | `5432` | PostgreSQL 端口 |
| `PG_USER` | string | — | 用户名（必填） |
| `PG_PASSWORD` | string | — | 密码（必填） |
| `PG_DATABASES` | string | 空 | 指定数据库列表（逗号分隔），空则自动发现 |
| `PG_EXCLUDE_DATABASES` | string | `template0,template1,postgres` | 自动发现时排除的数据库 |
| `PG_SSLMODE` | enum | `prefer` | SSL 模式：`disable`/`allow`/`prefer`/`require`/`verify-ca`/`verify-full` |
| `PG_SSLROOTCERT` | string | 空 | SSL 根证书路径 |
| `DB_POOL_SIZE` | int | `5` | 每数据库连接池大小 |
| `STRICT_READONLY` | bool | `false` | 检测到写权限时是否拒绝启动 |

### OpenAI

| 变量 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `OPENAI_API_KEY` | string | — | API Key（必填） |
| `OPENAI_MODEL` | string | `gpt-5-mini` | 模型名称 |
| `OPENAI_BASE_URL` | string | 空 | 兼容接口基地址（如 Azure、OpenRouter） |
| `OPENAI_TIMEOUT` | int | `60` | API 调用超时（秒） |

### 查询限制

| 变量 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `QUERY_TIMEOUT` | int | `30` | SQL 执行超时（秒） |
| `MAX_ROWS` | int | `1000` | 最大返回行数 |
| `MAX_CELL_BYTES` | int | `4096` | 单个字段最大字节数 |
| `MAX_RESULT_BYTES` | int | `10485760` | 结果集软阈值（10MB，超限截断） |
| `MAX_RESULT_BYTES_HARD` | int | `52428800` | 结果集硬阈值（50MB，直接报错） |
| `SESSION_WORK_MEM` | string | `64MB` | 会话 work_mem |
| `SESSION_TEMP_FILE_LIMIT` | string | `256MB` | 会话临时文件限制 |
| `MAX_CONCURRENT_REQUESTS` | int | `20` | 最大并发请求数 |

### Schema 缓存

| 变量 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `SCHEMA_REFRESH_INTERVAL` | int | `600` | 自动刷新间隔（秒），0 为关闭 |
| `SCHEMA_MAX_TABLES_FOR_FULL_CONTEXT` | int | `50` | 超过此表数启用检索模式 |
| `MAX_RETRIES` | int | `2` | SQL 生成/修正最大重试次数 |

### AI 结果验证（默认关闭）

| 变量 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `ENABLE_VALIDATION` | bool | `false` | 是否启用 AI 结果验证 |
| `VALIDATION_SAMPLE_ROWS` | int | `10` | 验证采样行数 |
| `VALIDATION_DATA_POLICY` | enum | `metadata_only` | 数据策略：`metadata_only`/`masked`/`full` |
| `VALIDATION_DENY_LIST` | string | 空 | 禁止发送到 LLM 的库/表/列规则（逗号分隔） |
| `VALIDATION_CONFIDENCE_THRESHOLD` | float | `-1.0` | 触发验证的 logprob 阈值 |

### 服务器

| 变量 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `REDIS_URL` | string | `redis://localhost:6379/0` | Redis 连接 URL |
| `LOG_LEVEL` | string | `INFO` | 日志级别：`DEBUG`/`INFO`/`WARNING`/`ERROR` |
| `LOG_FORMAT` | string | `json` | 日志格式：`json`/`console` |
| `SSE_HOST` | string | `0.0.0.0` | SSE 模式监听地址 |
| `SSE_PORT` | int | `8000` | SSE 模式监听端口 |

## 安全机制

### 1. SQL 安全校验（SQLGlot AST 级别）

- **语句白名单**：仅允许 `SELECT` / `WITH ... SELECT` / `EXPLAIN`
- **函数白名单**：基于 `pg_proc` 动态构建，仅包含 `IMMUTABLE`/`STABLE` 函数
- **函数黑名单**：显式拒绝 `pg_sleep`、`pg_read_file`、`dblink`、`lo_import` 等高危函数
- **DML/DDL 递归检测**：遍历 AST 拒绝任何 `INSERT`/`UPDATE`/`DELETE`/`CREATE`/`DROP`/`COPY` 等节点
- **单语句限制**：拒绝多语句执行（`;` 分隔）

### 2. 执行层防护

- **只读事务**：每个查询在 `SET TRANSACTION READ ONLY` 下执行
- **超时保护**：`statement_timeout`（默认 30s）+ `idle_in_transaction_session_timeout`（默认 60s）
- **资源限制**：`work_mem`（默认 64MB）+ `temp_file_limit`（默认 256MB）
- **结果截断**：超出行数/字节限制时自动截断，硬阈值时报错

### 3. 数据外发控制

- AI 验证默认关闭，开启后默认仅发送元数据（列名、行数、类型），不发送实际数据
- `VALIDATION_DENY_LIST` 支持按 `db.schema.table.column` 层级配置数据不外发规则
- 日志中 SQL 字符串字面量自动替换为 `'***'`，结果行永不入日志

### 4. 敏感信息保护

- 密码、API Key 使用 Pydantic `SecretStr`，`repr()` 和日志中自动遮掩

## 错误码

| 错误码 | 说明 | 客户端建议 |
|--------|------|-----------|
| `E_INVALID_ARGUMENT` | 输入参数不合法 | 修正参数后重试 |
| `E_DB_CONNECT` | 数据库连接失败 | 稍后重试 |
| `E_DB_NOT_FOUND` | 指定的数据库不存在 | 检查数据库名称 |
| `E_DB_INFER_AMBIGUOUS` | 数据库推断存在多个候选 | 指定 `database` 参数 |
| `E_DB_INFER_NO_MATCH` | 无法匹配到任何数据库 | 指定 `database` 参数 |
| `E_CROSS_DB_UNSUPPORTED` | 不支持跨库查询 | 拆分为单库查询 |
| `E_SCHEMA_NOT_READY` | Schema 加载中 | 等待后重试（含 `retry_after_ms`） |
| `E_SQL_GENERATE` | LLM 生成 SQL 失败 | 重新描述查询需求 |
| `E_SQL_UNSAFE` | SQL 安全校验未通过 | 系统自动重试 |
| `E_SQL_PARSE` | SQL 解析失败 | 系统自动重试 |
| `E_SQL_EXECUTE` | SQL 执行失败 | 检查查询描述是否正确 |
| `E_SQL_TIMEOUT` | SQL 执行超时 | 简化查询范围 |
| `E_VALIDATION_FAILED` | AI 结果验证失败 | 重新描述查询需求 |
| `E_LLM_TIMEOUT` | LLM API 超时 | 稍后重试 |
| `E_LLM_ERROR` | LLM API 异常 | 稍后重试 |
| `E_RESULT_TOO_LARGE` | 结果集超过硬阈值 | 缩小查询范围 |
| `E_RATE_LIMITED` | 请求被限流 | 稍后重试 |

## 项目结构

```
pg-mcp/
├── src/
│   ├── pg_mcp/
│   │   ├── cli.py              # Click CLI 入口
│   │   ├── config.py           # pydantic-settings 配置
│   │   ├── server.py           # MCP Server + Tool 注册
│   │   ├── app.py              # FastAPI app (SSE 模式)
│   │   ├── protocols.py        # Protocol 接口定义（DI 契约）
│   │   ├── engine/             # 核心业务逻辑
│   │   │   ├── orchestrator.py     # QueryEngine 主编排器
│   │   │   ├── db_inference.py     # 数据库自动推断
│   │   │   ├── sql_generator.py    # LLM SQL 生成
│   │   │   ├── sql_rewriter.py     # SQL 跨方言重写
│   │   │   ├── sql_validator.py    # SQLGlot 安全校验
│   │   │   ├── sql_executor.py     # 只读 SQL 执行
│   │   │   └── result_validator.py # AI 结果验证
│   │   ├── schema/             # Schema 发现与缓存
│   │   │   ├── discovery.py        # asyncpg 批量 SQL 发现
│   │   │   ├── cache.py            # Redis 缓存层 + singleflight
│   │   │   ├── retriever.py        # 大 schema 检索
│   │   │   └── state.py            # Schema 状态机
│   │   ├── db/
│   │   │   └── pool.py             # asyncpg 连接池管理
│   │   ├── models/             # Pydantic 数据模型
│   │   │   ├── schema.py           # DatabaseSchema / TableInfo / ColumnInfo
│   │   │   ├── request.py          # QueryRequest
│   │   │   ├── response.py         # QueryResponse
│   │   │   └── errors.py           # 错误码 + 异常层级
│   │   └── observability/
│   │       ├── logging.py          # structlog 配置
│   │       ├── metrics.py          # 计时器 / token 计数器
│   │       └── sanitizer.py        # 日志脱敏 / PII 掩码
│   ├── tests/
│   │   ├── conftest.py             # 共享 fixtures
│   │   ├── unit/                   # 纯逻辑测试（无外部依赖）
│   │   ├── integration/            # 集成测试（需 PG + Redis）
│   │   └── e2e/                    # 端到端 MCP 协议测试
│   └── pyproject.toml
├── fixtures/                   # 测试数据库（mini_blog / shop_oms / analytics_dw）
├── specs/                      # PRD / 设计文档 / 评审记录
├── .env.example
└── CLAUDE.md                   # 开发规范与项目记忆
```

## 开发

### 命令速查

```bash
cd src/

# 安装依赖
uv sync --extra dev

# 运行
uv run pg-mcp --transport stdio
uv run pg-mcp --transport sse

# 测试
uv run pytest tests/ -W ignore              # 全量测试
uv run pytest tests/unit/ -W ignore          # 单元测试
uv run pytest tests/integration/ -W ignore   # 集成测试
uv run pytest -x -q                          # 快速失败模式
uv run pytest --cov=pg_mcp --cov-report=term-missing  # 覆盖率

# 质量检查
uv run ruff check .                          # lint
uv run ruff format .                         # 格式化
uv run mypy pg_mcp/                          # 类型检查
```

### 测试数据库

`fixtures/` 目录包含 3 套测试数据库（ escalating 规模）：

| 数据库 | Schema | 表数 | 数据量 | 用途 |
|--------|--------|------|--------|------|
| `mini_blog` | 1 | 6 | ~200 行 | 全上下文路径测试 |
| `shop_oms` | 4 | 19 | ~8k 行 | 中等规模业务场景 |
| `analytics_dw` | 5 | 64 | ~165k 行 | 大 schema 检索策略测试 |

构建方式：

```bash
cd fixtures
make all      # 构建
make verify   # 验证
make clean    # 清理
```

无本地 PostgreSQL 时：

```bash
cd fixtures
make docker-up   # 启动 postgres:16-alpine 于 :5433
make all         # 构建
```

## 技术栈

| 层 | 技术 | 版本 |
|---|---|---|
| MCP 协议 | `mcp` Python SDK | ≥1.0 |
| HTTP 框架 | FastAPI | ≥0.115 |
| PostgreSQL 驱动 | asyncpg | ≥0.29 |
| LLM 客户端 | openai | ≥1.50 |
| 缓存 | Redis (redis-py async) | ≥5.0 |
| SQL 解析 | SQLGlot | ≥26.0 |
| 配置 | pydantic-settings | ≥2.0 |
| 日志 | structlog | ≥24.0 |
| CLI | click | ≥8.0 |

## 边界与限制

- 仅支持只读查询，不支持任何数据修改操作
- 不支持跨数据库联合查询（检测到时返回 `E_CROSS_DB_UNSUPPORTED`）
- 生成的 SQL 质量依赖于 LLM 能力和 schema 信息完整度
- 大型数据库通过 schema 检索策略缓解 token 限制，但检索质量可能影响生成准确性
- 不支持流式返回大结果集，不支持分页/续取
- 大字段（JSON/BYTEA/TEXT）超过 `MAX_CELL_BYTES` 时会被截断

## License

MIT
