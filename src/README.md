# pg-mcp

PostgreSQL Natural Language Query MCP Server — 通过自然语言查询 PostgreSQL 数据库的 MCP (Model Context Protocol) 服务。

## 功能特性

- **自然语言转 SQL**: 使用 OpenAI GPT 模型将自然语言查询转换为 PostgreSQL SQL
- **Schema 自动发现与缓存**: 自动发现数据库结构，缓存于 Redis，支持懒加载和后台预热
- **SQL 安全校验**: 基于 SQLGlot AST 的白名单/黑名单双重校验，确保只读安全
- **数据库自动推断**: 根据查询内容自动推断目标数据库，支持歧义检测
- **双传输模式**: 支持 stdio（Claude Desktop 等）和 SSE（HTTP）两种 MCP 传输
- **AI 结果验证**（可选）: 对复杂查询结果进行 AI 辅助验证

## 快速开始

### 安装

```bash
# 使用 uv（推荐）
uv sync

# 或使用 pip
pip install -e ".[dev]"
```

### 配置

复制示例环境文件并编辑：

```bash
cp .env.example .env
# 编辑 .env，填写 PostgreSQL 和 OpenAI 配置
```

必需的环境变量：

| 变量 | 说明 |
|------|------|
| `PG_USER` | PostgreSQL 用户名（建议只读用户） |
| `PG_PASSWORD` | PostgreSQL 密码 |
| `OPENAI_API_KEY` | OpenAI API Key |
| `REDIS_URL` | Redis 连接地址 |

### 运行

**stdio 模式**（Claude Desktop 等客户端）：

```bash
pg-mcp --transport stdio
```

**SSE 模式**（HTTP 服务）：

```bash
pg-mcp --transport sse
# 默认监听 http://0.0.0.0:8000
```

### Claude Desktop 配置

在 `claude_desktop_config.json` 中添加：

```json
{
  "mcpServers": {
    "pg-mcp": {
      "command": "pg-mcp",
      "args": ["--transport", "stdio"],
      "env": {
        "PG_HOST": "localhost",
        "PG_USER": "readonly_user",
        "PG_PASSWORD": "your_password",
        "OPENAI_API_KEY": "sk-your-key",
        "REDIS_URL": "redis://localhost:6379/0"
      }
    }
  }
}
```

## 架构

```
MCP Clients (Claude Desktop / Cursor / etc.)
    │
    ├─ stdio ─┐
    └─ SSE  ──┼──► MCP Server ──► QueryEngine (Orchestrator)
              │                      │
              │    ┌──────────────┬──┴──┬──────────────┐
              │    │              │     │              │
              │ SQL Generator  SQL Validator  SQL Executor
              │ (OpenAI)       (SQLGlot)     (asyncpg)
              │    │              │     │              │
              │    └──────────────┴──┬──┴──────────────┘
              │                      │
              │              Schema Cache (Redis)
              │              Schema Discovery (asyncpg)
              │
              PostgreSQL          Redis
```

## 项目结构

```
pg_mcp/
├── cli.py              # Click CLI 入口
├── config.py           # pydantic-settings 配置
├── server.py           # MCP Server + Tool 注册
├── app.py              # FastAPI app (SSE 模式)
├── protocols.py        # Protocol 接口定义
├── models/             # Pydantic 数据模型
│   ├── schema.py       # DatabaseSchema / TableInfo / ColumnInfo
│   ├── request.py      # QueryRequest
│   ├── response.py     # QueryResponse
│   └── errors.py       # ErrorCode + 异常层级
├── engine/             # 核心引擎
│   ├── orchestrator.py     # QueryEngine 主编排器
│   ├── db_inference.py     # 数据库自动推断
│   ├── sql_generator.py    # LLM SQL 生成
│   ├── sql_validator.py    # SQLGlot 安全校验
│   ├── sql_executor.py     # 只读 SQL 执行
│   └── result_validator.py # AI 结果验证
├── schema/             # Schema 发现与缓存
│   ├── discovery.py    # asyncpg 批量 SQL schema 发现
│   ├── cache.py        # Redis 缓存层
│   ├── retriever.py    # 大 schema 检索
│   └── state.py        # Schema 状态机
├── db/
│   └── pool.py         # asyncpg 连接池管理
└── observability/
    ├── logging.py       # structlog JSON 日志
    ├── metrics.py       # 计时器
    └── sanitizer.py     # 日志脱敏
```

## 测试

```bash
# 单元测试（无外部依赖）
make test-unit
# 或: uv run pytest tests/unit/ -v

# 集成测试（需 PostgreSQL + Redis）
make test-integration
# 或: uv run pytest tests/integration/ -v

# 端到端测试
make test-e2e

# 全量测试
make test-all

# 覆盖率
make coverage
```

## 代码质量

```bash
make quality    # lint + typecheck + test-unit
make lint       # ruff check
make format     # ruff format
make typecheck  # mypy
```

## Docker 部署

```bash
# 构建镜像
make docker-build
# 或: docker build -t pg-mcp:latest .

# 运行容器
make docker-run
# 或: docker run --rm -it --env-file .env -p 8000:8000 pg-mcp:latest
```

## 安全说明

- **只读用户**: 强烈建议使用 PostgreSQL 只读用户运行
- `STRICT_READONLY=true`: 检测到写权限时拒绝启动
- SQL 执行使用 `SET TRANSACTION READ ONLY` + `statement_timeout`
- 所有 SQL 通过 AST 级安全校验（语句白名单 + 函数黑名单）
- 敏感配置使用 `SecretStr` 自动脱敏
- 日志中 SQL 字符串字面量替换为 `'***'`

## 配置参考

详见 `.env.example` 了解所有可用配置项。

## License

MIT
