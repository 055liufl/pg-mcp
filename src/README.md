# pg-mcp

PostgreSQL Natural Language Query MCP Server — 通过自然语言查询 PostgreSQL 数据库的 MCP (Model Context Protocol) 服务。

> **MCP** 是 Anthropic 推出的开放协议，让 AI 助手（Claude、Cursor 等）能够安全地调用外部工具。pg-mcp 让 AI 直接用自然语言查询你的 PostgreSQL 数据库。

## 功能特性

- **自然语言转 SQL**: 使用 OpenAI GPT 模型将自然语言查询转换为 PostgreSQL SQL
- **SQL 跨方言转写**: LLM 有时生成 BigQuery / MySQL / Snowflake 风格函数，`SqlRewriter` 自动 transpile 为 PostgreSQL 语法
- **Schema 自动发现与缓存**: 使用 asyncpg 批量 pg_catalog 查询发现数据库结构，Redis 缓存 + gzip 压缩，支持懒加载和后台预热
- **大 Schema 智能检索**: 表数超过阈值时自动切换为检索模式，CJK 关键词提取 + 英文同义词扩展，减少 Token 消耗
- **SQL 安全校验**: 四层 AST 校验 — 语句白名单（仅 `SELECT`/`UNION`/`EXPLAIN`）+ AST 节点黑名单（拒绝 DML/DDL）+ 函数白名单（`pg_proc` 派生）+ 显式函数黑名单（`pg_sleep`、`dblink` 等）
- **数据库自动推断**: 根据查询内容自动推断目标数据库，支持歧义检测和跨库查询拦截
- **双传输模式**: 支持 stdio（Claude Code / Cursor）和 SSE（HTTP）两种 MCP 传输
- **AI 结果验证**（可选）: 对复杂查询结果进行 AI 辅助验证，支持分层 deny_list 和元数据/脱敏/完整三种数据策略
- **并发限流**: `asyncio.Semaphore` 限制最大并发请求数，超出时返回结构化 `RateLimitedError`
- **结果截断**: 单行超 `MAX_CELL_BYTES`、结果集超 `MAX_RESULT_BYTES`、行数超 `MAX_ROWS` 三级截断，硬限制直接拒绝
- **EXPLAIN 支持**: 自动识别 `EXPLAIN` 语句，跳过外层 LIMIT wrapping，保留执行计划分析能力
- **依赖注入架构**: 核心组件通过 Protocol 定义接口，`QueryEngine` 依赖抽象而非具体实现，便于测试和扩展

---

## 目录

- [安装](#安装)
- [环境变量配置](#环境变量配置)
- [注册到 Claude Code](#注册到-claude-code)
- [注册到 Cursor](#注册到-cursor)
- [使用示例](#使用示例)
- [运行模式](#运行模式)
- [日志格式](#日志格式)
- [安全说明](#安全说明)
- [常见问题排查](#常见问题排查)
- [项目结构](#项目结构)
- [测试](#测试)
- [Docker 部署](#docker-部署)

---

## 安装

**环境要求**: Python 3.12+

### 方式一：全局安装（推荐用于 Claude / Cursor）

```bash
cd /path/to/pg-mcp/src
uv tool install .
```

安装后 `pg-mcp` 命令全局可用，AI 客户端可直接调用。

更新代码后重装：
```bash
cd /path/to/pg-mcp/src
uv tool install --force .
```

### 方式二：开发模式安装

```bash
# 使用 uv（推荐）
cd /path/to/pg-mcp/src
uv sync --extra dev

# 或使用 pip
pip install -e ".[dev]"
```

---

## 环境变量配置

pg-mcp 所有配置通过 **`.env` 文件** 读取。`.env` 位于项目根目录（与 `src/` 同级），已加入 `.gitignore`，不会被提交到版本控制。

### 配置优先级

1. **系统环境变量**（最高优先级）
2. **`.env` 文件**（项目根目录）
3. **代码默认值**（最低优先级）

### `.env` 文件路径

pg-mcp 默认从源码所在目录向上查找 `.env` 文件（`src/` → 项目根目录）。如果你通过 `uv tool install` 或 `pip install` 以**非 editable 模式**安装，源码会被复制到 Python 的 `site-packages` 目录，此时默认路径会失效。

**解决方案**：通过 `PG_MCP_ENV_FILE` 环境变量显式指定 `.env` 文件路径。

```bash
# ~/.bashrc 或 ~/.zshrc
export PG_MCP_ENV_FILE="/home/lfl/pg-mcp/.env"
```

配置后重新加载 shell：

```bash
source ~/.bashrc  # 或 source ~/.zshrc
```

验证环境变量已生效：

```bash
echo $PG_MCP_ENV_FILE
# 输出: /home/lfl/pg-mcp/.env
```

> 此方案无需修改 `settings.json` 的 MCP 配置，因为 `PG_MCP_ENV_FILE` 会通过 shell 环境自动传递给 MCP server 子进程。

### 必需配置

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `PG_USER` | — | PostgreSQL 用户名（**强烈建议只读用户**） |
| `PG_PASSWORD` | — | PostgreSQL 密码 |
| `OPENAI_API_KEY` | — | OpenAI API Key |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis 连接地址 |

### PostgreSQL 连接

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `PG_HOST` | `localhost` | PostgreSQL 主机 |
| `PG_PORT` | `5432` | PostgreSQL 端口 |
| `PG_DATABASES` | `''` | 要服务的数据库列表（逗号分隔，空则自动发现） |
| `PG_EXCLUDE_DATABASES` | `template0,template1,postgres` | 排除的数据库 |
| `PG_SSLMODE` | `prefer` | SSL 模式：`disable`/`allow`/`prefer`/`require`/`verify-ca`/`verify-full` |
| `PG_SSLROOTCERT` | `''` | SSL CA 证书路径 |
| `DB_POOL_SIZE` | `5` | 连接池大小 |
| `STRICT_READONLY` | `false` | `true` 时检测到写权限拒绝启动 |

### OpenAI

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `OPENAI_API_KEY` | — | OpenAI API Key |
| `OPENAI_MODEL` | `gpt-5-mini` | 模型名称 |
| `OPENAI_BASE_URL` | `''` | 自定义 API 地址（兼容第三方转发） |
| `OPENAI_TIMEOUT` | `60` | API 请求超时（秒） |

### 查询限制

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `QUERY_TIMEOUT` | `30` | SQL 执行超时（秒） |
| `IDLE_IN_TRANSACTION_SESSION_TIMEOUT` | `60` | 事务空闲超时（秒） |
| `MAX_ROWS` | `1000` | 单查询最大返回行数 |
| `MAX_CELL_BYTES` | `4096` | 单个单元格最大字节数 |
| `MAX_RESULT_BYTES` | `10485760` (10MB) | 结果集软限制 |
| `MAX_RESULT_BYTES_HARD` | `52428800` (50MB) | 结果集硬限制 |
| `SESSION_WORK_MEM` | `64MB` | 会话 work_mem |
| `SESSION_TEMP_FILE_LIMIT` | `256MB` | 临时文件上限 |
| `MAX_CONCURRENT_REQUESTS` | `20` | 最大并发请求数 |

### Schema 缓存

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `SCHEMA_REFRESH_INTERVAL` | `600` | Schema 自动刷新间隔（秒），`0` 关闭 |
| `SCHEMA_MAX_TABLES_FOR_FULL_CONTEXT` | `50` | 表数超过此值时切换为检索模式（减少 Token 消耗） |
| `MAX_RETRIES` | `2` | SQL 生成失败重试次数 |

### AI 结果验证（默认关闭）

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `ENABLE_VALIDATION` | `false` | 是否启用 AI 结果验证 |
| `VALIDATION_SAMPLE_ROWS` | `10` | 验证时采样行数 |
| `VALIDATION_DATA_POLICY` | `metadata_only` | 数据策略：`metadata_only`/`masked`/`full` |
| `VALIDATION_DENY_LIST` | `''` | 敏感字段规则（逗号分隔，格式 `db.schema.table.column`） |
| `VALIDATION_CONFIDENCE_THRESHOLD` | `-1.0` | 置信度阈值 |

### 日志与传输

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `LOG_LEVEL` | `INFO` | 日志级别：`DEBUG`/`INFO`/`WARNING`/`ERROR` |
| `LOG_FORMAT` | `json` | 日志格式：`json` 或 `console` |
| `TRANSPORT` | `stdio` | 传输方式（CLI 参数 `--transport` 优先级更高） |
| `SSE_HOST` | `0.0.0.0` | SSE 模式监听地址 |
| `SSE_PORT` | `8000` | SSE 模式监听端口 |

---

## 注册到 Claude Code

Claude Code（`claude` CLI）通过 `stdio` 模式与 MCP Server 通信。配置分为两部分：

1. **`~/.claude.json`** — 注册 MCP Server（命令路径）
2. **`.env` 文件** — 所有环境变量配置（位于项目根目录）

> **注意**：配置统一放在 `.env` 文件中，**不要**在 `~/.claude.json` 的 `env` 字段中重复配置。

### 1. 配置 ~/.claude.json

编辑 `~/.claude.json`，在 `projects."/path/to/pg-mcp".mcpServers` 中添加：

```json
{
  "projects": {
    "/path/to/pg-mcp": {
      "mcpServers": {
        "pg-mcp": {
          "type": "stdio",
          "command": "/path/to/pg-mcp/src/.venv/bin/pg-mcp",
          "args": [
            "--transport",
            "stdio"
          ]
        }
      }
    }
  }
}
```

- `command` 必须是 `pg-mcp` 的绝对路径（或已加入 PATH 的命令名）
- **不要**在 `~/.claude.json` 中添加 `env` 字段，环境变量统一从 `.env` 读取

### 2. 配置 .env 文件

在项目根目录创建 `.env` 文件：

```bash
# PostgreSQL Configuration
PG_HOST=localhost
PG_PORT=5432
PG_USER=readonly_user
PG_PASSWORD=your_password
PG_DATABASES=db1,db2,db3
PG_EXCLUDE_DATABASES=template0,template1,postgres

# OpenAI Configuration
OPENAI_BASE_URL=https://api.openai.com/v1
OPENAI_API_KEY=sk-your-openai-key
OPENAI_MODEL=gpt-5-mini
OPENAI_TIMEOUT=60

# Redis
REDIS_URL=redis://localhost:6379/0

# Logging
LOG_FORMAT=console
```

### 3. 在 Claude Code 中使用

配置保存后，在 Claude Code 对话中直接提问即可：

```
帮我查询用户表中最近注册的 10 个用户
```

Claude 会自动调用 pg-mcp 工具执行查询并返回结果。你也可以通过 `/mcp` 命令查看已注册的 MCP 服务器列表。

### 配置要点

- `.env` 文件已加入 `.gitignore`，不会被提交到版本控制，可安全存放敏感信息
- `LOG_FORMAT=console` 让日志更易读（调试时有用）
- 数据库用户**强烈建议使用只读权限**，pg-mcp 会执行 SQL 安全校验，但最小权限原则更安全

---

## 注册到 Cursor

Cursor 同样支持 MCP 协议，配置方式与 Claude Code 类似。

### 方式一：项目级配置（推荐）

1. 在项目根目录创建 `.cursor/mcp.json`：

```json
{
  "mcpServers": {
    "pg-mcp": {
      "command": "/path/to/pg-mcp/src/.venv/bin/pg-mcp",
      "args": ["--transport", "stdio"]
    }
  }
}
```

2. 在项目根目录创建 `.env` 文件（同 Claude Code 配置）：

```bash
# PostgreSQL Configuration
PG_HOST=localhost
PG_PORT=5432
PG_USER=readonly_user
PG_PASSWORD=your_password

# OpenAI Configuration
OPENAI_API_KEY=sk-your-openai-key
OPENAI_MODEL=gpt-5-mini

# Redis
REDIS_URL=redis://localhost:6379/0

# Logging
LOG_FORMAT=console
```

### 方式二：全局配置

Cursor 全局 MCP 配置路径：

- **macOS**: `~/.cursor/mcp.json`
- **Windows**: `%USERPROFILE%/.cursor/mcp.json`
- **Linux**: `~/.cursor/mcp.json`

配置内容与上面相同。

### 方式三：Cursor 设置界面（v0.45+）

1. 打开 Cursor Settings → MCP
2. 点击 "Add New MCP Server"
3. 填写：
   - **Name**: `pg-mcp`
   - **Type**: `command`
   - **Command**: `/path/to/pg-mcp/src/.venv/bin/pg-mcp --transport stdio`
4. 在项目根目录创建 `.env` 文件存放所有配置

### 验证配置

配置完成后，在 Cursor 的 Chat 面板中，你应该能看到 `pg-mcp` 工具可用。可以测试：

```
帮我查询用户表中最近注册的 10 个用户
```

---

## 使用示例

配置完成后，在 Claude 或 Cursor 中直接用自然语言提问即可，AI 会自动调用 pg-mcp 执行查询。

### 示例 1：简单查询

**你**: 查询所有用户表中有多少用户

**AI** (调用 pg-mcp):
```sql
SELECT COUNT(*) FROM users;
```
**结果**: `1,234`

### 示例 2：条件查询

**你**: 找出过去 30 天内注册且邮箱域名是 gmail.com 的用户

**AI** (调用 pg-mcp):
```sql
SELECT id, username, email, created_at
FROM users
WHERE created_at >= NOW() - INTERVAL '30 days'
  AND email LIKE '%@gmail.com'
ORDER BY created_at DESC;
```

### 示例 3：聚合分析

**你**: 按月份统计订单数量和总金额

**AI** (调用 pg-mcp):
```sql
SELECT
  DATE_TRUNC('month', created_at) AS month,
  COUNT(*) AS order_count,
  SUM(total_amount) AS total_revenue
FROM orders
GROUP BY DATE_TRUNC('month', created_at)
ORDER BY month DESC;
```

### 示例 4：跨表查询

**你**: 显示每个用户的用户名、订单数量和总消费金额

**AI** (调用 pg-mcp):
```sql
SELECT
  u.username,
  COUNT(o.id) AS order_count,
  COALESCE(SUM(o.total_amount), 0) AS total_spent
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
GROUP BY u.id, u.username
ORDER BY total_spent DESC;
```

### 示例 5：Schema 探索

**你**: 数据库里有哪些表？每张表有哪些字段？

**AI** (调用 pg-mcp):
```
数据库包含以下表：
- users (id, username, email, password_hash, created_at, updated_at)
- orders (id, user_id, status, total_amount, created_at, updated_at)
- order_items (id, order_id, product_id, quantity, price)
- products (id, name, description, price, stock, category_id)
- categories (id, name, parent_id)
```

### 多数据库场景

如果 PostgreSQL 中有多个数据库，pg-mcp 会自动推断你要查询的是哪个：

**你**: analytics_dw 数据库中，上个月的销售额是多少？

AI 会自动识别目标数据库为 `analytics_dw` 并执行查询。

---

## 运行模式

### 前置依赖

启动 pg-mcp 前，确保以下服务已运行：

```bash
# PostgreSQL（必需）
pg_isready -h localhost -p 5432

# Redis（必需 — Schema 缓存依赖）
redis-cli ping   # 返回 PONG 表示正常
# 如果未安装 Redis，可用 Docker 快速启动：
# docker run -d -p 6379:6379 --name redis redis:7-alpine
```

### stdio 模式（Claude Code / Cursor）

确保 `.env` 文件已配置好，然后直接运行：

```bash
pg-mcp --transport stdio
```

或临时覆盖某些变量：

```bash
LOG_LEVEL=DEBUG pg-mcp --transport stdio
```

AI 客户端通过标准输入输出与 pg-mcp 通信。这是 Claude Code 和 Cursor 的默认模式。

启动后终端应保持静默（仅输出日志），等待 MCP 协议消息。如果 Claude Code 已配置好，直接在其对话中提问即可触发工具调用。

### SSE 模式（HTTP 服务）

确保 `.env` 文件已配置好，然后运行：

```bash
pg-mcp --transport sse
# 默认监听 http://0.0.0.0:8000
```

SSE 模式适合需要独立部署为 HTTP 服务的场景，其他 MCP 客户端可以通过 HTTP SSE 连接。

自定义端口：
```bash
SSE_PORT=9000 pg-mcp --transport sse
```

验证服务是否启动：
```bash
curl http://localhost:8000/sse
# 正常应返回 SSE 流（持续连接）
```

---

## 日志格式

pg-mcp 支持两种日志输出格式：

### JSON 格式（默认）

```bash
LOG_FORMAT=json pg-mcp --transport stdio
```

输出示例：
```json
{"event": "databases_discovered", "timestamp": "2026-05-01T12:34:56.789Z", "log_level": "info", "count": 3}
```

适合日志采集系统（如 ELK、Loki）解析。

### Console 格式（人类可读）

```bash
LOG_FORMAT=console pg-mcp --transport stdio
```

输出示例：
```
2026-05-01T12:34:56.789Z [info     ] databases_discovered           count=3 databases=['mini_blog', 'shop_oms', 'analytics_dw']
```

适合本地开发调试，直接阅读。

---

## 安全说明

### 三重 SQL 防护

1. **AST 级安全校验**: 所有 SQL 通过 SQLGlot 解析为 AST，仅允许 `SELECT`/`UNION`/`EXPLAIN` 语句
2. **函数黑名单**: 禁止危险函数（`pg_sleep`、`pg_read_file`、`dblink`、`lo_import` 等）
3. **只读事务**: asyncpg `readonly=True` 事务包装器 + `statement_timeout` + `idle_in_transaction_session_timeout` + `work_mem` + `temp_file_limit` + 外层 LIMIT wrapping（EXPLAIN 豁免）

### 权限建议

```sql
-- 创建只读用户示例
CREATE USER pg_mcp_readonly WITH PASSWORD 'secure_password';

-- 对目标数据库授予只读权限
GRANT CONNECT ON DATABASE your_db TO pg_mcp_readonly;
\c your_db
GRANT USAGE ON SCHEMA public TO pg_mcp_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO pg_mcp_readonly;

-- 对新表自动继承权限
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT SELECT ON TABLES TO pg_mcp_readonly;
```

### 其他安全措施

- 敏感配置（密码、API Key）使用 `SecretStr` 自动脱敏
- 日志中 SQL 字符串字面量替换为 `'***'`，结果行永不写入日志
- 支持 `STRICT_READONLY=true` 强制拒绝写权限用户启动
- 17 个细分业务异常（`DbNotFoundError`、`SqlUnsafeError`、`RateLimitedError` 等），MCP 层统一转换为结构化 `ErrorDetail`

---

## 常见问题排查

### pg-mcp 命令找不到

```bash
which pg-mcp
# 如果无输出，确认 ~/.local/bin 在 PATH 中
export PATH="$HOME/.local/bin:$PATH"
```

### Claude Code 无法识别工具

1. 检查 `~/.claude.json` 的 JSON 语法是否正确
2. 确认 `.env` 文件位于**项目根目录**（pg-mcp 进程的 CWD 必须能访问到 `.env`）
3. 确认 `pg-mcp` 命令可通过终端直接执行：`which pg-mcp`
4. 在 Claude Code 中运行 `/mcp` 查看已注册的 MCP 服务器列表
5. 检查 Claude Code 日志：`~/.claude/logs/` 目录下的日志文件
6. 验证 `.env` 是否被正确加载：
   ```bash
   cd /path/to/pg-mcp && python -c "from pg_mcp.config import Settings; s=Settings(); print(s.pg_user, s.pg_port)"
   ```

### 数据库连接失败

```
DbConnectError: 连接池创建失败（5 次重试）: [Errno 111] Connect call failed ('127.0.0.1', 5432)
```

- 确认 PostgreSQL 正在运行
- 确认 `PG_HOST`/`PG_PORT` 配置正确（Docker 常用 5433）
- 确认用户名密码正确
- 检查防火墙/网络连通性：
  ```bash
  pg_isready -h $PG_HOST -p $PG_PORT -U $PG_USER
  ```

### Redis 连接失败

```
ConnectionError: Error 111 connecting to localhost:6379
```

- 启动 Redis：`redis-server --daemonize yes`
- 或使用 Docker：`docker run -d -p 6379:6379 redis:7-alpine`
- 修改 `REDIS_URL` 指向正确的 Redis 实例

### OpenAI API 错误

- 确认 `OPENAI_API_KEY` 有效且未过期
- 检查 `OPENAI_BASE_URL` 是否正确（使用第三方转发时）
- 检查网络是否能访问 OpenAI API

### 权限警告

```
[warning] readonly_check_failed: 数据库用户拥有管理权限
```

这是警告而非错误。建议创建只读用户消除此警告。若确实需要，可忽略。

### 端口 8000 被占用（SSE 模式）

```bash
# 查找占用进程
lsof -i :8000
# 或更换端口
SSE_PORT=9000 pg-mcp --transport sse
```

---

## 项目结构

```
pg_mcp/
├── __init__.py         # 包入口
├── __main__.py         # python -m pg_mcp 入口
├── cli.py              # Click CLI 入口 (pg-mcp 命令)
├── config.py           # pydantic-settings 配置（.env 文件在项目根目录）
├── server.py           # MCP Server + Tool 注册
├── app.py              # FastAPI app (SSE 模式 + /health + /admin/refresh)
├── protocols.py        # Protocol 接口定义（依赖注入抽象层）
├── engine/             # 核心引擎
│   ├── orchestrator.py     # QueryEngine 主编排器（多阶段流水线 + retry/fix 循环）
│   ├── db_inference.py     # 数据库自动推断（向量相似度匹配）
│   ├── sql_generator.py    # LLM SQL 生成（带 retry + feedback）
│   ├── sql_rewriter.py     # 跨方言 SQL transpile（BQ/MySQL/Snowflake → PG）
│   ├── sql_validator.py    # SQLGlot AST 安全校验
│   ├── sql_executor.py     # 只读 SQL 执行（SET TRANSACTION READ ONLY）
│   └── result_validator.py # AI 结果验证（分层 deny_list）
├── schema/             # Schema 发现与缓存
│   ├── discovery.py    # asyncpg 批量 pg_catalog 查询（避免 N+1）
│   ├── cache.py        # Redis 缓存层（gzip 压缩 + 后台预热）
│   ├── retriever.py    # 大 schema 检索（CJK 分词 + 英文同义词）
│   └── state.py        # Schema 状态机
├── db/
│   └── pool.py         # asyncpg 连接池管理（per-db lazy + retry）
├── models/             # Pydantic v2 数据模型
│   ├── schema.py       # DatabaseSchema / TableInfo / ColumnInfo / ViewInfo /
│   │                   # IndexInfo / ForeignKeyInfo / ConstraintInfo /
│   │                   # EnumTypeInfo / CompositeTypeInfo
│   ├── request.py      # QueryRequest
│   ├── response.py     # QueryResponse / ErrorDetail / AdminRefreshResult
│   └── errors.py       # ErrorCode + 17 个业务异常子类
└── observability/
    ├── logging.py       # structlog 日志（JSON / Console）
    ├── metrics.py       # 计时器 / token 计数
    └── sanitizer.py     # 日志脱敏 / PII 掩码 / SQL 字面量替换
```

---

## 测试

```bash
cd /path/to/pg-mcp/src

# 单元测试（无外部依赖，纯逻辑）
uv run pytest tests/unit/ -W ignore

# 集成测试（需 PostgreSQL + Redis）
uv run pytest tests/integration/ -W ignore

# 端到端测试（MCP 协议完整流程）
uv run pytest tests/e2e/ -W ignore

# 全量测试
uv run pytest tests/ -W ignore

# 覆盖率
uv run pytest --cov=pg_mcp --cov-report=term-missing

# 代码质量
uv run ruff check .       # lint
uv run ruff format .      # format
uv run mypy pg_mcp/       # 类型检查
```

### 测试分层

| 层 | 目录 | 依赖 | 关注点 |
|---|---|---|---|
| 单元测试 | `tests/unit/` | 无外部依赖 | 配置、校验、推断、检索、rewriter、sanitizer、rate limit |
| 集成测试 | `tests/integration/` | PG + Redis | 连接池、schema 发现、SQL 执行、缓存、FastAPI |
| 端到端 | `tests/e2e/` | mock | MCP 协议完整流程（call_tool / list_tools / 错误处理） |

### 测试数据库（fixtures）

```bash
cd /path/to/pg-mcp/fixtures

# 启动 Docker PostgreSQL
make docker-up

# 构建测试数据
make all PG_PORT=5433 PG_USER=test PGPASSWORD=test

# 验证
make verify PG_PORT=5433 PG_USER=test PGPASSWORD=test

# 清理
make clean PG_PORT=5433 PG_USER=test PGPASSWORD=test
make docker-down
```

---

## Docker 部署

Dockerfile 位于 `src/Dockerfile`，使用多阶段构建（builder + runtime），非 root 用户运行。

```bash
# 构建镜像（在 src/ 目录下）
cd /path/to/pg-mcp/src
docker build -t pg-mcp:latest .

# 运行容器（SSE 模式）
# 注意：--env-file 指向项目根目录的 .env 文件
docker run --rm -it \
  --env-file /path/to/pg-mcp/.env \
  -p 8000:8000 \
  pg-mcp:latest \
  pg-mcp --transport sse
```

---

## License

MIT
