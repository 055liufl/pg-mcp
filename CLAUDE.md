# pg-mcp

PostgreSQL Natural Language Query MCP Server — 通过自然语言查询 PostgreSQL 数据库。

## 项目概览

- **PRD**: `specs/0001-pg-mcp-prd.md`
- **设计文档**: `specs/0002-pg-mcp-design.md`
- **技术栈**: Python 3.12+ / FastAPI / asyncpg / SQLAlchemy 2.0 / OpenAI SDK / Redis / SQLGlot / MCP SDK
- **包管理**: uv（优先）或 pip，`pyproject.toml` 管理依赖
- **入口**: `pg_mcp/cli.py` → `pg-mcp --transport stdio|sse`

## 命令速查

```bash
# 依赖
uv sync                              # 安装依赖
uv add <pkg>                         # 添加依赖

# 运行
pg-mcp --transport stdio             # stdio 模式
pg-mcp --transport sse               # SSE 模式 (FastAPI)

# 测试
uv run pytest                        # 全量测试
uv run pytest tests/unit/            # 单元测试
uv run pytest tests/integration/     # 集成测试（需 PG + Redis）
uv run pytest -x -q                  # 快速失败模式
uv run pytest --cov=pg_mcp --cov-report=term-missing  # 覆盖率

# 质量
uv run ruff check .                  # lint
uv run ruff format .                 # format
uv run mypy pg_mcp/                  # 类型检查
```

## 项目结构

```
pg_mcp/
├── cli.py                      # click CLI 入口
├── config.py                   # pydantic-settings 配置
├── server.py                   # MCP Server + Tool 注册
├── app.py                      # FastAPI app (SSE 模式)
├── engine/                     # 核心业务逻辑
│   ├── orchestrator.py         # QueryEngine 主编排器
│   ├── db_inference.py         # 数据库自动推断
│   ├── sql_generator.py        # LLM SQL 生成
│   ├── sql_validator.py        # SQLGlot 安全校验
│   ├── sql_executor.py         # 只读 SQL 执行
│   └── result_validator.py     # AI 结果验证
├── schema/                     # Schema 发现与缓存
│   ├── discovery.py            # SQLAlchemy inspect
│   ├── cache.py                # Redis 缓存层
│   ├── retriever.py            # 大 schema 检索
│   └── state.py                # Schema 状态机
├── db/
│   └── pool.py                 # asyncpg 连接池管理
├── models/                     # Pydantic 数据模型
│   ├── schema.py               # DatabaseSchema / TableInfo / ColumnInfo
│   ├── request.py              # QueryRequest
│   ├── response.py             # QueryResponse
│   └── errors.py               # ErrorCode + 异常层级
└── observability/
    ├── logging.py              # structlog 配置
    ├── metrics.py              # 计时器 / token 计数器
    └── sanitizer.py            # 日志脱敏 / PII 掩码
tests/
├── conftest.py                 # 共享 fixtures
├── unit/                       # 纯逻辑测试，无外部依赖
│   ├── test_sql_validator.py
│   ├── test_db_inference.py
│   ├── test_schema_retriever.py
│   └── test_config.py
├── integration/                # 需要 PG/Redis 的测试
│   ├── test_schema_discovery.py
│   ├── test_sql_executor.py
│   └── test_query_engine.py
└── e2e/                        # 端到端 MCP 协议测试
    └── test_mcp_tool.py
```

## 编码规范

### Python 风格

- **Python 3.12+**：使用 `type` 别名、`X | Y` union 语法、`match/case`。
- **全量类型注解**：所有函数签名、返回值、类属性必须有类型注解。通过 `mypy --strict` 检查。
- **Pydantic v2 数据模型**：所有 DTO 使用 `pydantic.BaseModel`，禁止裸 dict 传递结构化数据。
- **async/await 原生异步**：所有 I/O 操作必须 async。绝不在 async 函数中调用阻塞 I/O。
- **f-string 优先**：禁止 `%` 格式化和 `.format()`。
- **pathlib 而非 os.path**。
- **命名**：模块/变量 `snake_case`，类 `PascalCase`，常量 `UPPER_SNAKE_CASE`，私有 `_leading_underscore`。
- **import 顺序**：stdlib → 第三方 → 项目内。由 ruff 的 isort 规则自动排序。
- **Docstring**：公开 API 使用 Google style docstring。内部方法仅在逻辑不自明时添加注释。
- **无裸 except**：始终捕获具体异常类型，禁止 `except Exception` 吞掉错误。

### SOLID / DRY / 设计原则

- **Single Responsibility**：每个模块/类只做一件事。`SqlValidator` 只做校验，`SqlExecutor` 只做执行，`QueryEngine` 只做编排。
- **Open/Closed**：函数黑名单、AST 节点白名单通过 `frozenset` 常量定义，扩展时只改常量不改逻辑。
- **Dependency Inversion**：`QueryEngine` 依赖抽象（Protocol），不直接 `import` 具体实现类。组件通过构造函数注入。
- **DRY**：schema 检索的关键词提取逻辑集中在 `SchemaRetriever`，推断和检索共用。数据模型定义一次，序列化/反序列化由 Pydantic 处理。
- **最小接口**：对外暴露的方法尽量少。内部方法以 `_` 开头。
- **组合优于继承**：`QueryEngine` 通过组合持有各组件实例，不使用继承链。
- **异常层级清晰**：所有业务异常继承 `PgMcpError`，每个错误码对应一个异常子类。不使用 string error code 做 if/else 判断。

### 性能要求

- **连接池复用**：asyncpg 连接池 per-database，禁止每次请求创建新连接。
- **Schema 缓存**：Redis + gzip 压缩。200 表 schema 压缩后 ~30KB，反序列化 < 5ms。
- **并发控制**：`asyncio.Semaphore` 限流，不使用线程池。
- **懒加载**：schema 按需加载 + 后台预热，启动不阻塞。
- **避免 N+1**：schema 发现使用 SQLAlchemy `inspect()` 批量获取，不逐表查询。
- **零拷贝序列化**：Pydantic `.model_dump_json()` 直接输出 JSON，不经过中间 dict。
- **LLM 调用优化**：temperature=0 保证确定性；大 schema 走检索策略控制 token 量。

### 安全底线

- SQL 执行**只读事务** + **statement_timeout** + **work_mem 限制**，三重防护。
- SQLGlot AST 校验：语句白名单（`Select`/`Union`/`Explain`）+ 函数黑名单 + DML 递归检测。
- `SecretStr` 包装所有敏感字段（password, api_key），`repr()` 自动遮掩。
- 日志中 SQL 字符串字面量替换为 `'***'`，结果行永不入日志。

## 测试规范

### 覆盖率目标

- **整体 ≥ 85%**，核心模块（`engine/`、`schema/`）≥ 90%。
- `sql_validator.py` 必须 100% 分支覆盖（安全关键路径）。

### 测试分层

| 层 | 目录 | 依赖 | 速度 | 关注点 |
|----|------|------|------|--------|
| 单元测试 | `tests/unit/` | 无外部依赖 | < 10s | 纯逻辑：校验、推断、检索、配置 |
| 集成测试 | `tests/integration/` | PG + Redis | < 60s | 连接池、schema 发现、SQL 执行、缓存 |
| 端到端 | `tests/e2e/` | PG + Redis + OpenAI (可 mock) | < 120s | MCP 协议完整流程 |

### 测试编写规则

- **Arrange-Act-Assert** 三段式结构，用空行分隔。
- **一个测试只测一个行为**，测试函数名 `test_<被测行为>_<条件>_<预期结果>`。
- **Fixtures 而非 setUp/tearDown**：使用 `conftest.py` 中的 pytest fixtures，async fixtures 使用 `pytest-asyncio`。
- **Mock 外部依赖**：LLM 调用、Redis、PostgreSQL 在单元测试中必须 mock。使用 `unittest.mock.AsyncMock`。
- **参数化测试**：对校验规则使用 `@pytest.mark.parametrize`，覆盖允许/拒绝样例。
- **Golden tests**：SQL 校验的通过/拒绝案例维护在 `tests/fixtures/sql_samples.py` 中，新增安全规则必须同步新增测试用例。
- **不测实现，测行为**：不 assert 内部状态，只 assert 公开 API 的输入输出。

### SQL 校验必测案例（最低要求）

```python
# 必须通过
"SELECT 1"
"SELECT * FROM users WHERE id = 1"
"WITH cte AS (SELECT ...) SELECT * FROM cte"
"SELECT COUNT(*), department FROM employees GROUP BY department"
"EXPLAIN SELECT * FROM orders"

# 必须拒绝
"INSERT INTO users VALUES (1, 'x')"
"UPDATE users SET name = 'x'"
"DELETE FROM users"
"DROP TABLE users"
"SELECT * FROM users; DROP TABLE users"
"SELECT pg_sleep(100)"
"SELECT pg_read_file('/etc/passwd')"
"SELECT dblink('host=evil', 'SELECT 1')"
"SELECT lo_import('/etc/passwd')"
"COPY users TO '/tmp/dump'"
```

## 错误处理模式

```python
# 正确：在 orchestrator 中统一捕获并转换
async def execute(self, request: QueryRequest) -> QueryResponse:
    try:
        return await self._do_execute(request)
    except PgMcpError:
        raise                              # 业务异常直接上抛
    except asyncpg.PostgresError as e:
        raise SqlExecuteError(str(e))      # 转换为业务异常
    except openai.APIError as e:
        raise LlmError(str(e))             # 转换为业务异常

# 正确：MCP Server 层捕获 PgMcpError 转为统一响应
@mcp_server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    try:
        response = await engine.execute(QueryRequest(**arguments))
    except PgMcpError as e:
        response = QueryResponse(error=ErrorDetail(code=e.code, message=str(e)))
    return [TextContent(type="text", text=response.model_dump_json())]
```

## 依赖注入模式

```python
# 使用 Protocol 定义接口
class SqlValidatorProtocol(Protocol):
    def validate(self, sql: str, schema: DatabaseSchema | None = None) -> ValidationResult: ...

class SqlGeneratorProtocol(Protocol):
    async def generate(self, query: str, schema: str, feedback: str | None = None) -> SqlGenerationResult: ...

# QueryEngine 依赖抽象
class QueryEngine:
    def __init__(
        self,
        sql_generator: SqlGeneratorProtocol,
        sql_validator: SqlValidatorProtocol,
        sql_executor: SqlExecutorProtocol,
        schema_cache: SchemaCacheProtocol,
        ...
    ): ...
```

## Git 工作流

- 分支命名：`feat/<feature>`、`fix/<issue>`、`refactor/<scope>`
- Commit message：conventional commits 格式（`feat:`, `fix:`, `refactor:`, `test:`, `docs:`）
- 提交前必须通过：`ruff check` + `ruff format --check` + `mypy` + `pytest`
