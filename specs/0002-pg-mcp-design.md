# Design-0002: PostgreSQL Natural Language Query MCP Server

> 基于 [PRD-0001](./0001-pg-mcp-prd.md) 的技术设计文档

## 1. 技术选型

| 层 | 技术 | 版本 | 选型理由 |
|---|---|---|---|
| MCP 协议 | `mcp` Python SDK | ≥1.0 | 官方实现，支持 stdio/SSE |
| HTTP 框架 | FastAPI | ≥0.115 | SSE 传输层 + 健康检查 + admin 端点 |
| PostgreSQL 驱动 | asyncpg | ≥0.29 | 高性能异步驱动，原生支持 prepared statements |
| Schema 发现 | asyncpg + 批量 SQL | - | 直接查 `pg_catalog`，避免 SQLAlchemy inspect N+1 |
| LLM 客户端 | openai | ≥1.50 | 官方 SDK，async 支持 |
| 缓存 | Redis (redis-py async) | ≥5.0 | 跨实例共享、内建 TTL、持久化 |
| SQL 解析 | SQLGlot | ≥26.0 | 纯 Python、无 C 依赖、支持 PG 方言、AST 遍历 API 完善 |
| 配置管理 | pydantic-settings | ≥2.0 | 类型安全，支持 env + dotenv + TOML |
| 日志 | structlog | ≥24.0 | 结构化 JSON 日志 |
| 并发控制 | asyncio.Semaphore | stdlib | 轻量级请求级限流 |

**PRD 偏差说明：** PRD 要求 `pglast`，设计选用 SQLGlot。理由：纯 Python 无需编译 libpg_query C 库，Docker/CI 部署零摩擦，多方言扩展性好。SQLGlot 对 PostgreSQL 方言的 AST 解析能力满足安全校验需求。**需同步更新 PRD §5 技术约束，将 pglast 硬约束改为 SQLGlot，并补充验收标准：SQL 校验必测案例全部通过。**

**Schema 发现偏差说明：** 原设计使用 SQLAlchemy `inspect()` API，经 review 发现存在 N+1 查询问题（每表独立调用 `get_columns/get_pk_constraint/get_indexes/get_foreign_keys`）。改为直接使用 asyncpg + 批量 SQL 查询 `pg_catalog` 系统表，单次往返获取全部元数据。

## 2. 系统架构

```
┌─────────────────────────────────────────────────────────────┐
│                        MCP Clients                          │
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
│  │ DBInfer  │→│ SQLGen   │→│ SQLValid │→│ SQLExecutor │  │
│  │(Protocol)│  │(Protocol)│  │(Protocol)│  │ (Protocol)  │  │
│  └─────────┘  └──────────┘  └──────────┘  └──────┬──────┘  │
│                                                   │         │
│                                          ┌────────▼───────┐ │
│                                          │ ResultValidator │ │
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

## 3. 项目结构

```
pg_mcp/
├── __init__.py
├── __main__.py                 # python -m pg_mcp 入口
├── cli.py                      # click CLI (--transport stdio|sse)
├── config.py                   # pydantic-settings 配置
├── server.py                   # MCP Server 初始化 + Tool 注册 + 错误转换层
├── app.py                      # FastAPI app (SSE 模式)
├── protocols.py                # Protocol 接口定义（DI 契约）
│
├── engine/
│   ├── __init__.py
│   ├── orchestrator.py         # QueryEngine 主编排器（依赖注入）
│   ├── db_inference.py         # 数据库自动推断
│   ├── sql_generator.py        # LLM SQL 生成（含 timeout + retry）
│   ├── sql_validator.py        # SQLGlot 安全校验（函数白名单 + 黑名单）
│   ├── sql_executor.py         # 只读 SQL 执行
│   └── result_validator.py     # AI 结果验证（含 deny_list 过滤）
│
├── schema/
│   ├── __init__.py
│   ├── discovery.py            # asyncpg 批量 SQL schema 发现
│   ├── cache.py                # Redis 缓存层（singleflight 加载）
│   ├── retriever.py            # 大 schema 检索（关键词匹配）
│   └── state.py                # Schema 加载状态机
│
├── db/
│   ├── __init__.py
│   ├── pool.py                 # asyncpg 连接池管理
│   └── function_registry.py    # pg_proc 函数白名单注册
│
├── models/
│   ├── __init__.py
│   ├── schema.py               # DatabaseSchema / TableInfo / ColumnInfo ...
│   ├── request.py              # QueryRequest
│   ├── response.py             # QueryResponse
│   └── errors.py               # ErrorCode + PgMcpError 异常层级
│
└── observability/
    ├── __init__.py
    ├── logging.py              # structlog 配置
    ├── metrics.py              # 计时器 / token 计数器
    └── sanitizer.py            # 日志脱敏 / PII 掩码
```

## 4. 组件详细设计

### 4.1 配置管理 (`config.py`)

```python
from pydantic_settings import BaseSettings
from pydantic import Field, SecretStr
from enum import Enum

class SslMode(str, Enum):
    disable = "disable"
    allow = "allow"
    prefer = "prefer"
    require = "require"
    verify_ca = "verify-ca"
    verify_full = "verify-full"

class ValidationDataPolicy(str, Enum):
    metadata_only = "metadata_only"
    masked = "masked"
    full = "full"

class Settings(BaseSettings):
    model_config = {"env_prefix": "", "env_file": ".env"}

    # PostgreSQL
    pg_host: str = "localhost"
    pg_port: int = 5432
    pg_user: str
    pg_password: SecretStr
    pg_databases: str = ""
    pg_exclude_databases: str = "template0,template1,postgres"
    pg_sslmode: SslMode = SslMode.prefer
    pg_sslrootcert: str = ""
    db_pool_size: int = 5
    strict_readonly: bool = False

    # OpenAI
    openai_api_key: SecretStr
    openai_model: str = "gpt-5-mini"
    openai_base_url: str | None = None
    openai_timeout: int = 60               # LLM 调用超时秒数

    # 查询限制
    query_timeout: int = 30
    idle_in_transaction_session_timeout: int = 60  # 事务空闲超时秒数
    max_rows: int = 1000
    max_cell_bytes: int = 4096
    max_result_bytes: int = 10 * 1024 * 1024
    max_result_bytes_hard: int = 50 * 1024 * 1024
    session_work_mem: str = "64MB"
    session_temp_file_limit: str = "256MB"
    max_concurrent_requests: int = 20

    # AI 验证
    enable_validation: bool = False
    validation_sample_rows: int = 10
    validation_data_policy: ValidationDataPolicy = ValidationDataPolicy.metadata_only
    validation_deny_list: str = ""          # 格式: "db.schema.table.column,..."
    validation_confidence_threshold: float = -1.0

    # Schema
    max_retries: int = 2
    schema_refresh_interval: int = 600
    schema_max_tables_for_full_context: int = 50

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # 日志 & 传输
    log_level: str = "INFO"
    transport: str = "stdio"
    sse_host: str = "0.0.0.0"
    sse_port: int = 8000
```

### 4.2 Protocol 接口 (`protocols.py`)

所有核心组件通过 Protocol 定义接口，QueryEngine 通过构造函数注入实现。

```python
from typing import Protocol

class SqlGeneratorProtocol(Protocol):
    async def generate(self, query: str, schema_context: str,
                       feedback: str | None = None) -> SqlGenerationResult: ...

class SqlValidatorProtocol(Protocol):
    def validate(self, sql: str, schema: DatabaseSchema | None = None) -> ValidationResult: ...

class SqlExecutorProtocol(Protocol):
    async def execute(self, database: str, sql: str,
                      schema_names: list[str] | None = None) -> ExecutionResult: ...

class SchemaCacheProtocol(Protocol):
    async def get_schema(self, database: str) -> DatabaseSchema: ...
    async def refresh(self, database: str | None = None) -> RefreshResult: ...
    def discovered_databases(self) -> list[str]: ...

class DbInferenceProtocol(Protocol):
    async def infer(self, user_query: str) -> str: ...

class ResultValidatorProtocol(Protocol):
    def should_validate(self, sql: str, result: ExecutionResult,
                        generation: SqlGenerationResult) -> bool: ...
    async def validate(self, user_query: str, sql: str,
                       result: ExecutionResult, schema: DatabaseSchema) -> ValidationVerdict: ...
```

### 4.3 数据模型 (`models/`)

#### 4.3.1 Schema 数据模型

```python
from pydantic import BaseModel, Field
from datetime import datetime

class ColumnInfo(BaseModel):
    name: str
    type: str
    nullable: bool
    default: str | None = None
    comment: str | None = None
    is_primary_key: bool = False

class TableInfo(BaseModel):
    schema_name: str
    table_name: str
    columns: list[ColumnInfo]
    comment: str | None = None
    is_foreign: bool = False              # foreign table 标记

class ViewInfo(BaseModel):
    schema_name: str
    view_name: str
    columns: list[ColumnInfo]
    definition: str | None = None
    is_materialized: bool = False

class IndexInfo(BaseModel):
    schema_name: str
    table_name: str
    index_name: str
    columns: list[str]
    index_type: str
    is_unique: bool

class ForeignKeyInfo(BaseModel):
    constraint_name: str
    source_schema: str
    source_table: str
    source_columns: list[str]
    target_schema: str
    target_table: str
    target_columns: list[str]

class ConstraintInfo(BaseModel):
    schema_name: str
    table_name: str
    constraint_name: str
    constraint_type: str                  # CHECK, UNIQUE, EXCLUSION
    definition: str

class EnumTypeInfo(BaseModel):
    schema_name: str
    type_name: str
    values: list[str]

class CompositeTypeInfo(BaseModel):
    schema_name: str
    type_name: str
    attributes: list[ColumnInfo]

class DatabaseSchema(BaseModel):
    database: str
    tables: list[TableInfo]
    views: list[ViewInfo]                 # 含普通视图 + 物化视图
    indexes: list[IndexInfo]
    foreign_keys: list[ForeignKeyInfo]
    constraints: list[ConstraintInfo]
    enum_types: list[EnumTypeInfo]
    composite_types: list[CompositeTypeInfo]
    allowed_functions: set[str]           # 从 pg_proc 加载的安全函数白名单
    loaded_at: datetime

    def table_count(self) -> int:
        return len(self.tables)

    def foreign_table_ids(self) -> set[str]:
        return {f"{t.schema_name}.{t.table_name}" for t in self.tables if t.is_foreign}

    def to_prompt_text(self) -> str:
        """将 schema 序列化为 LLM prompt 可用的纯文本"""
        ...

    def to_summary_text(self) -> str:
        """压缩摘要：仅表名+列名，用于推断"""
        ...
```

#### 4.3.2 请求/响应模型

```python
class QueryResponse(BaseModel):
    request_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    database: str | None = None
    sql: str | None = None
    columns: list[str] | None = None
    column_types: list[str] | None = None
    rows: list[list] | None = None
    row_count: int | None = None
    truncated: bool = False
    truncated_reason: str | None = None
    validation_used: bool = False
    schema_loaded_at: str | None = None
    warnings: list[str] = Field(default_factory=list)
    error: ErrorDetail | None = None
```

### 4.4 MCP Server 错误转换层 (`server.py`)

```python
@mcp_server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    if name != "query":
        raise McpError(INVALID_PARAMS, f"Unknown tool: {name}")

    try:
        request = QueryRequest(**arguments)
    except ValidationError as e:
        raise McpError(INVALID_PARAMS, str(e))              # 协议级错误

    try:
        response = await query_engine.execute(request)       # 业务逻辑
    except PgMcpError as e:
        # 业务错误 → 统一响应 error 字段
        response = QueryResponse(
            error=ErrorDetail(
                code=e.code,
                message=str(e),
                retry_after_ms=getattr(e, "retry_after_ms", None),
                candidates=getattr(e, "candidates", None),
            )
        )

    return [TextContent(type="text", text=response.model_dump_json())]
```

### 4.5 连接池管理 (`db/pool.py`)

```python
class ConnectionPoolManager:
    async def assert_readonly(self):
        """检查 PG 用户写权限，不仅是 superuser"""
        pool = await self.get_pool("postgres")
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT rolsuper, rolcreaterole, rolcreatedb "
                "FROM pg_roles WHERE rolname = current_user"
            )
            if row["rolsuper"] or row["rolcreaterole"] or row["rolcreatedb"]:
                if self._settings.strict_readonly:
                    raise RuntimeError("STRICT_READONLY: 用户拥有管理权限，拒绝启动")
                log.warning("readonly_check_failed",
                            msg="数据库用户拥有管理权限，强烈建议使用只读用户")

            # 探测写权限
            has_write = await conn.fetchval(
                "SELECT EXISTS("
                "  SELECT 1 FROM information_schema.role_table_grants "
                "  WHERE grantee = current_user "
                "  AND privilege_type IN ('INSERT','UPDATE','DELETE','TRUNCATE')"
                "  LIMIT 1"
                ")"
            )
            if has_write:
                if self._settings.strict_readonly:
                    raise RuntimeError("STRICT_READONLY: 用户拥有表写权限，拒绝启动")
                log.warning("readonly_check_failed",
                            msg="用户拥有表写权限，SQL 执行依赖只读事务保护")
```

### 4.6 Schema 发现（批量 SQL） (`schema/discovery.py`)

```python
class SchemaDiscovery:
    async def load_schema(self, database: str) -> DatabaseSchema:
        """使用批量 SQL 一次性获取全部元数据，避免 N+1"""
        pool = await self._pool_mgr.get_pool(database)
        async with pool.acquire() as conn:
            # 批量获取所有表和列（含 foreign table 标记）
            tables_and_cols = await conn.fetch("""
                SELECT c.table_schema, c.table_name, c.column_name, c.data_type,
                       c.is_nullable, c.column_default, c.ordinal_position,
                       col_description(
                           (c.table_schema || '.' || c.table_name)::regclass,
                           c.ordinal_position
                       ) AS column_comment,
                       obj_description((c.table_schema || '.' || c.table_name)::regclass)
                           AS table_comment,
                       t.table_type,
                       ft.foreign_table_name IS NOT NULL AS is_foreign
                FROM information_schema.columns c
                JOIN information_schema.tables t
                    ON c.table_schema = t.table_schema AND c.table_name = t.table_name
                LEFT JOIN information_schema.foreign_tables ft
                    ON c.table_schema = ft.foreign_table_schema
                    AND c.table_name = ft.foreign_table_name
                WHERE c.table_schema NOT IN ('pg_catalog','information_schema','pg_toast')
                ORDER BY c.table_schema, c.table_name, c.ordinal_position
            """)

            # 批量获取主键
            pks = await conn.fetch("""
                SELECT kcu.table_schema, kcu.table_name, kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu USING (constraint_name, table_schema)
                WHERE tc.constraint_type = 'PRIMARY KEY'
            """)

            # 批量获取索引、外键、约束、枚举、复合类型、物化视图...
            # （类似模式，每种元数据一条批量 SQL）

            # 获取安全函数白名单
            allowed_functions = await self._load_allowed_functions(conn)

            # 组装 DatabaseSchema
            return self._assemble(tables_and_cols, pks, ..., allowed_functions)

    async def _load_allowed_functions(self, conn) -> set[str]:
        """查询 pg_proc 获取 IMMUTABLE/STABLE 内置函数"""
        rows = await conn.fetch("""
            SELECT p.proname
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE n.nspname IN ('pg_catalog', 'public')
              AND p.provolatile IN ('i', 's')
              AND p.proname NOT IN (
                  'pg_read_file','pg_read_binary_file','pg_ls_dir','pg_stat_file',
                  'lo_import','lo_export','lo_get','lo_put',
                  'pg_sleep','pg_advisory_lock','pg_advisory_xact_lock',
                  'pg_advisory_unlock','pg_advisory_unlock_all',
                  'pg_try_advisory_lock','pg_try_advisory_xact_lock',
                  'pg_notify','pg_listening_channels',
                  'dblink','dblink_exec','dblink_connect','dblink_disconnect',
                  'pg_terminate_backend','pg_cancel_backend',
                  'pg_reload_conf','set_config',
                  'pg_switch_wal','pg_create_restore_point'
              )
        """)
        return {r["proname"] for r in rows}
```

### 4.7 Schema 缓存（Singleflight 加载） (`schema/cache.py`)

```python
class SchemaCache:
    PREFIX = "pg_mcp"

    def __init__(self, redis: Redis, pool_mgr: ConnectionPoolManager, settings: Settings):
        self._redis = redis
        self._discovery = SchemaDiscovery(pool_mgr, settings)
        self._settings = settings
        self._databases: list[str] = []
        # Singleflight: 每库最多一个加载任务
        self._inflight: dict[str, asyncio.Task] = {}
        self._inflight_lock = asyncio.Lock()

    async def get_schema(self, database: str) -> DatabaseSchema:
        state = await self._get_state(database)
        if state == SchemaState.READY:
            cached = await self._redis.get(f"{self.PREFIX}:schema:{database}")
            if cached:
                return DatabaseSchema.model_validate_json(gzip.decompress(cached))

        if state in (SchemaState.LOADING, SchemaState.UNLOADED, SchemaState.FAILED, None):
            await self._ensure_loading(database)
            raise SchemaNotReadyError(
                f"Schema for {database} is loading",
                retry_after_ms=2000,
            )

    async def _ensure_loading(self, database: str):
        """Singleflight: 确保每个库最多只有一个加载任务"""
        async with self._inflight_lock:
            if database in self._inflight and not self._inflight[database].done():
                return  # 已有进行中的任务
            self._inflight[database] = asyncio.create_task(self._do_load(database))

    async def _do_load(self, database: str):
        await self._set_state(database, SchemaState.LOADING)
        try:
            schema = await self._discovery.load_schema(database)
            compressed = gzip.compress(schema.model_dump_json().encode())
            await self._redis.set(
                f"{self.PREFIX}:schema:{database}",
                compressed,
                ex=self._settings.schema_refresh_interval or None,
            )
            await self._set_state(database, SchemaState.READY)
        except Exception as e:
            await self._set_state(database, SchemaState.FAILED)
            log.error("schema_load_failed", database=database, error=str(e))
        finally:
            async with self._inflight_lock:
                self._inflight.pop(database, None)

    async def refresh(self, database: str | None = None) -> RefreshResult:
        """刷新 schema，返回每库成功/失败详情"""
        targets = [database] if database else list(self._databases)
        for db in targets:
            await self._set_state(db, SchemaState.UNLOADED)

        results = await asyncio.gather(
            *[self._do_load(db) for db in targets],
            return_exceptions=True,
        )

        succeeded, failed = [], []
        for db, result in zip(targets, results):
            if isinstance(result, Exception):
                failed.append({"database": db, "error": str(result)})
            else:
                succeeded.append(db)

        return RefreshResult(succeeded=succeeded, failed=failed)
```

### 4.8 SQL 安全校验（白名单函数 + 黑名单兜底） (`engine/sql_validator.py`)

```python
import sqlglot
from sqlglot import exp

# 显式禁止的高风险函数（即使在 pg_proc 白名单中也拒绝）
DENY_FUNCTIONS = frozenset({
    "pg_read_file", "pg_read_binary_file", "pg_ls_dir", "pg_stat_file",
    "lo_import", "lo_export", "lo_get", "lo_put",
    "pg_sleep", "pg_advisory_lock", "pg_advisory_xact_lock",
    "pg_advisory_unlock", "pg_advisory_unlock_all",
    "pg_try_advisory_lock", "pg_try_advisory_xact_lock",
    "pg_notify", "pg_listening_channels",
    "dblink", "dblink_exec", "dblink_connect", "dblink_disconnect",
    "dblink_send_query", "dblink_get_result",
    "pg_terminate_backend", "pg_cancel_backend",
    "pg_reload_conf", "pg_rotate_logfile",
    "set_config", "current_setting",
    "pg_switch_wal", "pg_create_restore_point",
})

BLOCKED_NODE_TYPES = (
    exp.Insert, exp.Update, exp.Delete,
    exp.Create, exp.Drop, exp.AlterTable,
    exp.Grant, exp.Command,
)

class SqlValidator:
    def validate(self, sql: str, schema: DatabaseSchema | None = None) -> ValidationResult:
        # 1. 解析
        try:
            parsed = sqlglot.parse(sql, dialect="postgres")
        except sqlglot.errors.ParseError as e:
            return ValidationResult(valid=False, code="E_SQL_PARSE",
                                    reason=f"SQL 语法错误: {e}")

        # 2. 单语句检查
        stmts = [s for s in parsed if s is not None]
        if len(stmts) != 1:
            return ValidationResult(valid=False, code="E_SQL_UNSAFE",
                                    reason=f"仅允许单条语句，检测到 {len(stmts)} 条")
        ast = stmts[0]

        # 3. 语句级白名单
        if isinstance(ast, exp.Command):
            cmd = ast.this.upper() if ast.this else ""
            if cmd == "EXPLAIN":
                # 禁止 EXPLAIN ANALYZE（会实际执行查询）
                rest = ast.expression.sql() if ast.expression else ""
                if "ANALYZE" in rest.upper():
                    return ValidationResult(valid=False, code="E_SQL_UNSAFE",
                                            reason="禁止 EXPLAIN ANALYZE")
                return ValidationResult(valid=True, is_explain=True)
            return ValidationResult(valid=False, code="E_SQL_UNSAFE",
                                    reason=f"禁止的命令: {cmd}")

        if not isinstance(ast, (exp.Select, exp.Union, exp.Intersect, exp.Except, exp.Subquery)):
            return ValidationResult(valid=False, code="E_SQL_UNSAFE",
                                    reason=f"仅允许 SELECT 语句，检测到: {type(ast).__name__}")

        # 4. 递归检查子树中的 DML/DDL
        for node in ast.walk():
            if isinstance(node, BLOCKED_NODE_TYPES):
                return ValidationResult(valid=False, code="E_SQL_UNSAFE",
                                        reason=f"禁止的语句类型: {type(node).__name__}")

        # 5. 函数调用检查：白名单 + 黑名单双重策略
        allowed_funcs = schema.allowed_functions if schema else None
        for func in ast.find_all(exp.Func, exp.Anonymous):
            func_name = self._extract_func_name(func).lower()
            if not func_name:
                continue

            # 黑名单兜底：无论白名单如何都拒绝
            if func_name in DENY_FUNCTIONS:
                return ValidationResult(valid=False, code="E_SQL_UNSAFE",
                                        reason=f"禁止调用高风险函数: {func_name}")

            # 白名单检查：如果有函数白名单，不在其中则拒绝
            if allowed_funcs is not None and func_name not in allowed_funcs:
                return ValidationResult(valid=False, code="E_SQL_UNSAFE",
                                        reason=f"函数不在允许列表中: {func_name}")

        # 6. Foreign table 检查
        if schema:
            foreign_ids = schema.foreign_table_ids()
            if foreign_ids:  # fail-closed: 有 foreign table 信息时才检查
                for table in ast.find_all(exp.Table):
                    table_id = f"{table.db or 'public'}.{table.name}"
                    if table_id in foreign_ids:
                        return ValidationResult(valid=False, code="E_SQL_UNSAFE",
                                                reason=f"禁止访问 foreign table: {table_id}")

        return ValidationResult(valid=True)

    def _extract_func_name(self, node) -> str:
        if isinstance(node, exp.Anonymous):
            return node.this if isinstance(node.this, str) else ""
        return (node.sql_name() if hasattr(node, "sql_name") else
                type(node).__name__)
```

### 4.9 SQL 执行 (`engine/sql_executor.py`)

```python
class SqlExecutor:
    async def execute(self, database: str, sql: str,
                      schema_names: list[str] | None = None) -> ExecutionResult:
        pool = await self._pool_mgr.get_pool(database)
        async with pool.acquire() as conn:
            # 完整的会话级安全参数
            timeout_s = self._settings.query_timeout
            idle_timeout_s = self._settings.idle_in_transaction_session_timeout
            await conn.execute(f"SET statement_timeout = '{timeout_s}s'")
            await conn.execute(f"SET idle_in_transaction_session_timeout = '{idle_timeout_s}s'")
            await conn.execute(f"SET work_mem = '{self._settings.session_work_mem}'")
            await conn.execute(f"SET temp_file_limit = '{self._settings.session_temp_file_limit}'")
            await conn.execute("SET max_parallel_workers_per_gather = 2")

            # 设置 search_path 为受控 schema 列表
            if schema_names:
                safe_schemas = ",".join(f'"{s}"' for s in schema_names)
                await conn.execute(f"SET search_path = {safe_schemas}")

            try:
                async with conn.transaction(readonly=True):
                    limited_sql = self._apply_limit(sql)
                    rows = await conn.fetch(limited_sql)
            except asyncpg.QueryCanceledError:
                raise SqlTimeoutError(f"查询超时（{timeout_s}s）")
            except asyncpg.PostgresError as e:
                raise SqlExecuteError(str(e))

        return self._process_result(rows)

    def _apply_limit(self, sql: str) -> str:
        """如果 SQL 没有 LIMIT，包裹为子查询并注入 LIMIT"""
        stripped = sql.strip().rstrip(";")
        upper = stripped.upper()
        # 检查顶层是否已有 LIMIT（排除子查询中的 LIMIT）
        depth = 0
        has_top_limit = False
        for token in upper.split():
            if "(" in token:
                depth += token.count("(")
            if ")" in token:
                depth -= token.count(")")
            if token == "LIMIT" and depth == 0:
                has_top_limit = True
                break
        if not has_top_limit:
            return f"SELECT * FROM ({stripped}) AS __q LIMIT {self._settings.max_rows + 1}"
        return stripped
```

### 4.10 数据库推断 (`engine/db_inference.py`)

```python
class DbInference:
    AMBIGUITY_THRESHOLD = 0.15

    async def infer(self, user_query: str) -> str:
        databases = self._cache.discovered_databases()
        if not databases:
            raise DbInferNoMatchError("无可用数据库")

        scored: list[tuple[str, float]] = []
        not_ready: list[str] = []

        for db in databases:
            try:
                schema = await self._cache.get_schema(db)
                score = self._score(schema, keywords)
                scored.append((db, score))
            except SchemaNotReadyError:
                not_ready.append(db)

        # 如果有未就绪的库且无可用结果，提示重试
        if not scored and not_ready:
            raise SchemaNotReadyError(
                f"数据库 schema 尚未就绪: {not_ready}",
                retry_after_ms=3000,
            )

        if not scored or scored[0][1] == 0:
            if not_ready:
                raise SchemaNotReadyError(
                    f"部分库未就绪 ({not_ready})，无法完成推断",
                    retry_after_ms=3000,
                )
            raise DbInferNoMatchError(f"查询无法匹配到任何数据库")

        # 跨库检测：基于实体命中分布
        multi_hit = [(db, s) for db, s in scored if s > 0]
        if len(multi_hit) > 1:
            hit_dbs = [db for db, _ in multi_hit]
            if self._entity_spread_cross_db(keywords, hit_dbs):
                raise CrossDbUnsupportedError(f"查询涉及多个数据库: {hit_dbs}")

        # 歧义检测
        scored.sort(key=lambda x: -x[1])
        if len(scored) >= 2:
            top1, top2 = scored[0][1], scored[1][1]
            if top1 > 0 and (top1 - top2) / top1 < self.AMBIGUITY_THRESHOLD:
                raise DbInferAmbiguousError(
                    message=f"多个候选: {scored[0][0]}, {scored[1][0]}",
                    candidates=[s[0] for s in scored[:3]],
                )

        return scored[0][0]

    def _entity_spread_cross_db(self, keywords: list[str], dbs: list[str]) -> bool:
        """检查关键词是否分散命中不同库（而非集中在一个库）"""
        # 如果每个关键词的最佳命中库不同，说明跨库意图
        best_db_per_kw = {}
        for kw in keywords:
            # ... 计算每个关键词在各库的命中分
            pass
        unique_best_dbs = set(best_db_per_kw.values())
        return len(unique_best_dbs) > 1
```

### 4.11 结果验证（含 deny_list 过滤） (`engine/result_validator.py`)

```python
class ResultValidator:
    def _build_prompt(self, query: str, sql: str,
                      result: ExecutionResult, schema: DatabaseSchema) -> str:
        parts = [
            f"User question: {query}",
            f"Generated SQL:\n```sql\n{sql}\n```",
            f"Result: {result.row_count} rows, columns: {result.columns}",
            f"Column types: {result.column_types}",
        ]

        # deny_list 过滤：检查是否涉及敏感表/列
        if self._is_denied(result.columns, schema.database):
            # 命中 deny_list，强制降级为 metadata_only
            return "\n\n".join(parts)

        policy = self._settings.validation_data_policy
        if policy == ValidationDataPolicy.full and result.rows:
            sample = result.rows[:self._settings.validation_sample_rows]
            parts.append(f"Sample rows:\n{json.dumps(sample, ensure_ascii=False)}")
        elif policy == ValidationDataPolicy.masked and result.rows:
            sample = self._mask_pii(result.rows[:self._settings.validation_sample_rows],
                                    result.columns)
            parts.append(f"Sample rows (masked):\n{json.dumps(sample, ensure_ascii=False)}")

        return "\n\n".join(parts)

    def _is_denied(self, columns: list[str], database: str) -> bool:
        """检查当前查询是否命中 validation_deny_list"""
        if not self._settings.validation_deny_list:
            return False
        deny_rules = [r.strip().lower() for r in self._settings.validation_deny_list.split(",")]
        for rule in deny_rules:
            parts = rule.split(".")
            if parts[0] == database.lower() or parts[0] == "*":
                return True
        return False

    async def validate(self, user_query: str, sql: str,
                       result: ExecutionResult, schema: DatabaseSchema) -> ValidationVerdict:
        try:
            response = await asyncio.wait_for(
                self._client.chat.completions.create(
                    model=self._model,
                    messages=[
                        {"role": "system", "content": VALIDATION_PROMPT},
                        {"role": "user", "content": self._build_prompt(
                            user_query, sql, result, schema)},
                    ],
                    temperature=0,
                    response_format={"type": "json_object"},
                ),
                timeout=self._settings.openai_timeout,
            )
        except asyncio.TimeoutError:
            raise LlmTimeoutError("结果验证 LLM 调用超时")
        except openai.APIError as e:
            raise LlmError(f"结果验证 LLM 调用失败: {e}")

        return ValidationVerdict.model_validate_json(response.choices[0].message.content)
```

### 4.12 主编排器（依赖注入） (`engine/orchestrator.py`)

```python
class QueryEngine:
    def __init__(
        self,
        sql_generator: SqlGeneratorProtocol,
        sql_validator: SqlValidatorProtocol,
        sql_executor: SqlExecutorProtocol,
        schema_cache: SchemaCacheProtocol,
        db_inference: DbInferenceProtocol,
        result_validator: ResultValidatorProtocol,
        retriever: SchemaRetriever,
        settings: Settings,
    ):
        self._sql_gen = sql_generator
        self._sql_val = sql_validator
        self._sql_exec = sql_executor
        self._cache = schema_cache
        self._db_inference = db_inference
        self._result_val = result_validator
        self._retriever = retriever
        self._settings = settings
        self._semaphore = asyncio.Semaphore(settings.max_concurrent_requests)

    async def execute(self, request: QueryRequest) -> QueryResponse:
        request_id = str(uuid.uuid4())
        log = structlog.get_logger().bind(request_id=request_id)

        # 非阻塞并发控制
        acquired = self._semaphore._value > 0  # 仅用于快速检查
        if not acquired:
            # 尝试非阻塞 acquire 确认
            try:
                self._semaphore.release()  # 不可靠，改用 try_acquire
            except ValueError:
                pass
        # 正确方式：使用 wait_for 实现非阻塞
        try:
            await asyncio.wait_for(self._semaphore.acquire(), timeout=0.01)
        except asyncio.TimeoutError:
            raise RateLimitedError("服务繁忙，请稍后重试")

        try:
            return await self._do_execute(request, request_id, log)
        finally:
            self._semaphore.release()
```

## 5. 数据流

```
Request
  │
  ▼
InputValidation ──error──▶ E_INVALID_ARGUMENT
  │
  ▼
AdminAction? ──yes──▶ refresh_schema → return (含成功/失败详情)
  │ no
  ▼
ResolveDatabase
  ├─ explicit ──not found──▶ E_DB_NOT_FOUND
  └─ infer ──ambiguous──▶ E_DB_INFER_AMBIGUOUS
            ──no match──▶ E_DB_INFER_NO_MATCH
            ──cross db──▶ E_CROSS_DB_UNSUPPORTED
            ──not ready──▶ E_SCHEMA_NOT_READY (retry_after_ms)
  │
  ▼
LoadSchema ──not ready──▶ E_SCHEMA_NOT_READY (retry_after_ms)
  │
  ▼
RetrieveSchemaContext (full | search-based)
  │
  ▼
┌─▶ GenerateSQL (LLM, timeout + retry/backoff)
│   ──timeout──▶ E_LLM_TIMEOUT
│   ──error──▶ E_LLM_ERROR
│     │
│     ▼
│   ValidateSQL (SQLGlot: stmt whitelist + func whitelist + deny + foreign table)
│     │
│     ├─ unsafe ──retry ≤ MAX_RETRIES──┐
│     │                                 │
│     └─ pass                          ─┘
│         │
│   return_type=sql? ──yes──▶ return {sql}
│         │ no
│         ▼
│   ExecuteSQL (asyncpg, readonly tx, search_path, statement_timeout)
│     ├─ timeout ──▶ E_SQL_TIMEOUT
│     ├─ error ──▶ E_SQL_EXECUTE
│     ├─ hard limit ──▶ E_RESULT_TOO_LARGE
│     └─ ok (soft truncation possible)
│         │
│         ▼
│   ShouldValidateResult? (deny_list → skip)
│     ├─ no ──▶ return response
│     └─ yes
│         │
│         ▼
│   ValidateResult (LLM, timeout + error mapping)
│     ├─ pass ──▶ return response
│     ├─ fix ──▶ re-validate & re-execute ──retry──┐
│     └─ fail ──▶ E_VALIDATION_FAILED              │
│                                                   │
└───────────────────────────────────────────────────┘
```

## 6. Redis Key 设计

| Key | 类型 | TTL | 说明 |
|-----|------|-----|------|
| `pg_mcp:databases` | SET | 无 | 已发现的数据库名集合 |
| `pg_mcp:state:{db}` | STRING | 无 | Schema 状态 |
| `pg_mcp:schema:{db}` | STRING (gzip JSON) | `SCHEMA_REFRESH_INTERVAL` | 完整 DatabaseSchema |

LRU 淘汰交给 Redis 实例级配置（`maxmemory-policy allkeys-lru`），应用层不额外实现。

## 7. 可观测性设计

### 7.1 每次请求记录的关键事件

| 事件 | 记录字段 |
|------|---------|
| `request_received` | request_id, query_length, database, return_type |
| `schema_loaded` | database, table_count, cache_hit, elapsed_ms |
| `sql_generated` | attempt, prompt_tokens, completion_tokens, logprob, elapsed_ms |
| `sql_validation_failed` | attempt, reason |
| `sql_executed` | row_count, truncated, elapsed_ms |
| `result_validated` | verdict, attempt, data_policy, denied, elapsed_ms |
| `request_completed` | total_elapsed_ms |
| `request_failed` | error_code, error_message |

### 7.2 脱敏规则

- SQL 中的字符串字面量替换为 `'***'`
- 结果行不写入日志
- `pg_password`、`openai_api_key` 使用 `SecretStr`，`repr()` 自动遮掩

## 8. 测试策略

### 8.1 分层

| 层 | 依赖 | 关注点 |
|----|------|--------|
| 单元测试 | 无外部依赖（mock Protocol） | SQL 校验规则、推断逻辑、错误映射、schema 检索、LIMIT 注入 |
| 集成测试 | PG + Redis | 连接池、schema 发现、SQL 执行、缓存一致性、singleflight |
| 端到端 | PG + Redis + OpenAI (可 mock) | MCP 协议完整流程、错误分层（协议级 vs 业务级） |

### 8.2 SQL 校验必测矩阵

| 输入 | 预期 | 覆盖规则 |
|------|------|---------|
| `SELECT 1` | 通过 | 基本 SELECT |
| `WITH cte AS (...) SELECT ...` | 通过 | CTE |
| `EXPLAIN SELECT ...` | 通过 (is_explain=True) | EXPLAIN |
| `EXPLAIN ANALYZE SELECT ...` | 拒绝 | EXPLAIN ANALYZE 执行查询 |
| `INSERT INTO ...` | 拒绝 | DML |
| `SELECT pg_sleep(100)` | 拒绝 | 黑名单函数 |
| `SELECT my_custom_volatile_func()` | 拒绝 | 不在白名单 |
| `SELECT * FROM foreign_table` | 拒绝 | foreign table |
| `SELECT 1; DROP TABLE x` | 拒绝 | 多语句 |

### 8.3 Protocol mock 示例

```python
class MockSqlGenerator:
    def __init__(self, sql: str = "SELECT 1", logprob: float = 0.0):
        self._sql = sql
        self._logprob = logprob

    async def generate(self, query, schema_context, feedback=None):
        return SqlGenerationResult(sql=self._sql, avg_logprob=self._logprob,
                                   prompt_tokens=100, completion_tokens=50)

# 测试中注入 mock
engine = QueryEngine(
    sql_generator=MockSqlGenerator("SELECT count(*) FROM users"),
    sql_validator=SqlValidator(),
    ...
)
```

## 9. 依赖清单

```toml
[project]
name = "pg-mcp"
requires-python = ">=3.12"
dependencies = [
    "mcp>=1.0",
    "fastapi>=0.115",
    "uvicorn[standard]>=0.29",
    "asyncpg>=0.29",
    "openai>=1.50",
    "redis[hiredis]>=5.0",
    "sqlglot>=26.0",
    "pydantic>=2.0",
    "pydantic-settings>=2.0",
    "structlog>=24.0",
    "click>=8.0",
]

[project.scripts]
pg-mcp = "pg_mcp.cli:main"
```

> 注意：SQLAlchemy 已从依赖中移除，schema 发现改为 asyncpg + 批量 SQL。

## 10. 部署

### 10.1 Docker

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml .
RUN pip install --no-cache-dir .
COPY pg_mcp/ pg_mcp/
ENTRYPOINT ["pg-mcp"]
CMD ["--transport", "sse"]
```

### 10.2 MCP 客户端配置 (Claude Desktop)

```json
{
  "mcpServers": {
    "pg-mcp": {
      "command": "pg-mcp",
      "args": ["--transport", "stdio"],
      "env": {
        "PG_HOST": "localhost",
        "PG_USER": "readonly_user",
        "PG_PASSWORD": "xxx",
        "OPENAI_API_KEY": "sk-xxx",
        "REDIS_URL": "redis://localhost:6379/0"
      }
    }
  }
}
```

### 10.3 外部依赖

| 服务 | 必须 | 说明 |
|------|------|------|
| PostgreSQL 14-17 | 是 | 目标数据库 |
| Redis 7+ (配置 `maxmemory-policy allkeys-lru`) | 是 | Schema 缓存 |
| OpenAI API | 是 | SQL 生成 + 结果验证 |
