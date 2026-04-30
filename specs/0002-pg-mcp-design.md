# Design-0002: PostgreSQL Natural Language Query MCP Server

> 基于 [PRD-0001](./0001-pg-mcp-prd.md) 的技术设计文档

## 1. 技术选型

| 层 | 技术 | 版本 | 选型理由 |
|---|---|---|---|
| MCP 协议 | `mcp` Python SDK | ≥1.0 | 官方实现，支持 stdio/SSE |
| HTTP 框架 | FastAPI | ≥0.115 | SSE 传输层 + 健康检查 + admin 端点 |
| PostgreSQL 驱动 | asyncpg | ≥0.29 | 高性能异步驱动，原生支持 prepared statements |
| Schema 发现 | SQLAlchemy 2.0 (async) | ≥2.0 | `inspect()` API 简化元数据读取 |
| LLM 客户端 | openai | ≥1.50 | 官方 SDK，async 支持 |
| 缓存 | Redis (redis-py async) | ≥5.0 | 跨实例共享、内建 TTL/LRU、持久化 |
| SQL 解析 | SQLGlot | ≥26.0 | 纯 Python、无 C 依赖、支持 PG 方言、AST 遍历 API 完善 |
| 配置管理 | pydantic-settings | ≥2.0 | 类型安全，支持 env + dotenv + TOML |
| 日志 | structlog | ≥24.0 | 结构化 JSON 日志 |
| 并发控制 | asyncio.Semaphore | stdlib | 轻量级请求级限流 |

**PRD 偏差说明：** PRD 要求 `pglast`，设计选用 SQLGlot。理由：纯 Python 无需编译 libpg_query C 库，Docker/CI 部署零摩擦，多方言扩展性好。SQLGlot 对 PostgreSQL 方言的 AST 解析能力满足安全校验需求。

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
│              InputValidator  →  RequestRouter                │
└──────────────┬──────────────────────────────────────────────┘
               │
┌──────────────▼──────────────────────────────────────────────┐
│                      QueryEngine (Orchestrator)              │
│  ┌─────────┐  ┌──────────┐  ┌──────────┐  ┌─────────────┐  │
│  │ DBInfer  │→│ SQLGen   │→│ SQLValid │→│ SQLExecutor │  │
│  │         │  │ (OpenAI) │  │ (SQLGlot)│  │ (asyncpg)   │  │
│  └─────────┘  └──────────┘  └──────────┘  └──────┬──────┘  │
│                                                   │         │
│                                          ┌────────▼───────┐ │
│                                          │ ResultValidator │ │
│                                          │ (OpenAI, opt)  │ │
│                                          └────────────────┘ │
└──────────────┬──────────────────────────────────────────────┘
               │
┌──────────────▼──────────────────────────────────────────────┐
│                      Infrastructure Layer                    │
│  ┌───────────────┐  ┌───────────────┐  ┌─────────────────┐  │
│  │ ConnectionMgr │  │ SchemaCache   │  │ ConcurrencyCtrl │  │
│  │ (asyncpg pool)│  │ (Redis)       │  │ (Semaphore)     │  │
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
├── server.py                   # MCP Server 初始化 + Tool 注册
├── app.py                      # FastAPI app (SSE 模式)
│
├── engine/
│   ├── __init__.py
│   ├── orchestrator.py         # QueryEngine 主编排器
│   ├── db_inference.py         # 数据库自动推断
│   ├── sql_generator.py        # LLM SQL 生成
│   ├── sql_validator.py        # SQLGlot 安全校验
│   ├── sql_executor.py         # 只读 SQL 执行
│   └── result_validator.py     # AI 结果验证
│
├── schema/
│   ├── __init__.py
│   ├── discovery.py            # SQLAlchemy inspect schema 发现
│   ├── cache.py                # Redis 缓存层
│   ├── retriever.py            # 大 schema 检索（关键词匹配）
│   └── state.py                # Schema 加载状态机
│
├── db/
│   ├── __init__.py
│   └── pool.py                 # asyncpg 连接池管理
│
├── models/
│   ├── __init__.py
│   ├── config.py               # 配置数据模型
│   ├── schema.py               # DatabaseSchema / TableInfo / ColumnInfo ...
│   ├── request.py              # QueryRequest
│   ├── response.py             # QueryResponse
│   └── errors.py               # ErrorCode enum + PgMcpError 异常层级
│
└── observability/
    ├── __init__.py
    ├── logging.py              # structlog 配置
    ├── metrics.py              # 计时器 / token 计数器
    └── sanitizer.py            # 日志脱敏 + PII 掩码
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
    pg_databases: str = ""              # 逗号分隔，空=自动发现
    pg_exclude_databases: str = "template0,template1,postgres"
    pg_sslmode: SslMode = SslMode.prefer
    pg_sslrootcert: str = ""
    db_pool_size: int = 5
    strict_readonly: bool = False

    # OpenAI
    openai_api_key: SecretStr
    openai_model: str = "gpt-5-mini"
    openai_base_url: str | None = None

    # 查询限制
    query_timeout: int = 30
    max_rows: int = 1000
    max_cell_bytes: int = 4096
    max_result_bytes: int = 10 * 1024 * 1024       # 10MB 软阈值
    max_result_bytes_hard: int = 50 * 1024 * 1024   # 50MB 硬阈值
    session_work_mem: str = "64MB"
    session_temp_file_limit: str = "256MB"
    max_concurrent_requests: int = 20

    # AI 验证
    enable_validation: bool = False
    validation_sample_rows: int = 10
    validation_data_policy: ValidationDataPolicy = ValidationDataPolicy.metadata_only
    validation_deny_list: str = ""
    validation_confidence_threshold: float = -1.0

    # Schema
    max_retries: int = 2
    schema_refresh_interval: int = 600
    schema_max_tables_for_full_context: int = 50

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # 日志 & 传输
    log_level: str = "INFO"
    transport: str = "stdio"            # stdio | sse
    sse_host: str = "0.0.0.0"
    sse_port: int = 8000
```

### 4.2 数据模型 (`models/`)

#### 4.2.1 Schema 数据模型

```python
from pydantic import BaseModel
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
    index_type: str               # btree, gin, gist, ...
    is_unique: bool

class ForeignKeyInfo(BaseModel):
    constraint_name: str
    source_schema: str
    source_table: str
    source_columns: list[str]
    target_schema: str
    target_table: str
    target_columns: list[str]

class EnumTypeInfo(BaseModel):
    schema_name: str
    type_name: str
    values: list[str]

class DatabaseSchema(BaseModel):
    database: str
    tables: list[TableInfo]
    views: list[ViewInfo]
    indexes: list[IndexInfo]
    foreign_keys: list[ForeignKeyInfo]
    enum_types: list[EnumTypeInfo]
    loaded_at: datetime

    def table_count(self) -> int:
        return len(self.tables)

    def to_prompt_text(self) -> str:
        """将 schema 序列化为 LLM prompt 可用的纯文本"""
        ...

    def to_summary_text(self) -> str:
        """压缩摘要：仅表名+列名，用于推断"""
        ...
```

#### 4.2.2 请求/响应模型

```python
import uuid
from enum import Enum

class ReturnType(str, Enum):
    sql = "sql"
    result = "result"

class QueryRequest(BaseModel):
    query: str
    database: str | None = None
    return_type: ReturnType = ReturnType.result
    admin_action: str | None = None

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
    warnings: list[str] = []
    error: ErrorDetail | None = None

class ErrorDetail(BaseModel):
    code: str                    # E_xxx
    message: str
    retry_after_ms: int | None = None   # 仅 E_SCHEMA_NOT_READY
    candidates: list[str] | None = None # 仅 E_DB_INFER_AMBIGUOUS
```

#### 4.2.3 异常层级

```python
class PgMcpError(Exception):
    """所有业务异常基类"""
    code: str
    message: str

class InvalidArgumentError(PgMcpError):      code = "E_INVALID_ARGUMENT"
class DbConnectError(PgMcpError):            code = "E_DB_CONNECT"
class DbNotFoundError(PgMcpError):           code = "E_DB_NOT_FOUND"
class DbInferAmbiguousError(PgMcpError):     code = "E_DB_INFER_AMBIGUOUS"
    candidates: list[str]
class DbInferNoMatchError(PgMcpError):       code = "E_DB_INFER_NO_MATCH"
class CrossDbUnsupportedError(PgMcpError):   code = "E_CROSS_DB_UNSUPPORTED"
class SchemaNotReadyError(PgMcpError):       code = "E_SCHEMA_NOT_READY"
    retry_after_ms: int = 2000
class SqlGenerateError(PgMcpError):          code = "E_SQL_GENERATE"
class SqlUnsafeError(PgMcpError):            code = "E_SQL_UNSAFE"
class SqlParseError(PgMcpError):             code = "E_SQL_PARSE"
class SqlExecuteError(PgMcpError):           code = "E_SQL_EXECUTE"
class SqlTimeoutError(PgMcpError):           code = "E_SQL_TIMEOUT"
class ValidationFailedError(PgMcpError):     code = "E_VALIDATION_FAILED"
class LlmTimeoutError(PgMcpError):           code = "E_LLM_TIMEOUT"
class LlmError(PgMcpError):                 code = "E_LLM_ERROR"
class ResultTooLargeError(PgMcpError):       code = "E_RESULT_TOO_LARGE"
class RateLimitedError(PgMcpError):          code = "E_RATE_LIMITED"
```

### 4.3 MCP Server + FastAPI 集成 (`server.py`, `app.py`)

#### 4.3.1 MCP Server 注册

```python
from mcp.server import Server
from mcp.types import Tool, TextContent

mcp_server = Server("pg-mcp")

@mcp_server.list_tools()
async def list_tools() -> list[Tool]:
    return [Tool(
        name="query",
        description="通过自然语言查询 PostgreSQL 数据库，返回 SQL 或查询结果",
        inputSchema={
            "type": "object",
            "properties": {
                "query":        {"type": "string", "maxLength": 2000},
                "database":     {"type": "string"},
                "return_type":  {"type": "string", "enum": ["sql", "result"], "default": "result"},
                "admin_action": {"type": "string", "enum": ["refresh_schema"]},
            },
            "required": ["query"],
        },
    )]

@mcp_server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    if name != "query":
        raise McpError(INVALID_PARAMS, f"Unknown tool: {name}")  # 协议级错误

    try:
        request = QueryRequest(**arguments)                      # 协议级校验
    except ValidationError as e:
        raise McpError(INVALID_PARAMS, str(e))

    response = await query_engine.execute(request)               # 业务逻辑
    return [TextContent(type="text", text=response.model_dump_json())]
```

#### 4.3.2 双传输模式

```python
# cli.py
import click

@click.command()
@click.option("--transport", type=click.Choice(["stdio", "sse"]), default="stdio")
def main(transport: str):
    settings = Settings()
    lifecycle = AppLifecycle(settings)

    if transport == "stdio":
        from mcp.server.stdio import stdio_server
        asyncio.run(run_stdio(lifecycle))
    else:
        import uvicorn
        app = create_fastapi_app(lifecycle)
        uvicorn.run(app, host=settings.sse_host, port=settings.sse_port)

# app.py — SSE 模式
from mcp.server.sse import SseServerTransport

def create_fastapi_app(lifecycle: AppLifecycle) -> FastAPI:
    app = FastAPI(title="pg-mcp", lifespan=lifecycle.lifespan)
    sse_transport = SseServerTransport("/messages/")

    app.mount("/mcp", sse_transport.get_asgi_app())

    @app.get("/health")
    async def health():
        return {"status": "ok", "databases": list(lifecycle.schema_cache.ready_databases())}

    @app.post("/admin/refresh-schema")
    async def refresh_schema(database: str | None = None):
        result = await lifecycle.schema_cache.refresh(database)
        return result

    return app
```

#### 4.3.3 应用生命周期

```python
# lifecycle.py
from contextlib import asynccontextmanager

class AppLifecycle:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.pool_manager: ConnectionPoolManager
        self.schema_cache: SchemaCache
        self.query_engine: QueryEngine

    @asynccontextmanager
    async def lifespan(self, app=None):
        # ── 启动 ──
        self.pool_manager = ConnectionPoolManager(self.settings)
        redis = Redis.from_url(self.settings.redis_url)
        self.schema_cache = SchemaCache(redis, self.pool_manager, self.settings)
        self.query_engine = QueryEngine(
            pool_manager=self.pool_manager,
            schema_cache=self.schema_cache,
            settings=self.settings,
        )

        # 1. 发现数据库列表（不加载 schema）
        await self.schema_cache.discover_databases()

        # 2. 如果 STRICT_READONLY，检查用户权限
        if self.settings.strict_readonly:
            await self.pool_manager.assert_readonly()

        # 3. 后台预热 schema + 定时刷新
        prewarmer = asyncio.create_task(self.schema_cache.prewarm_all())
        refresher = None
        if self.settings.schema_refresh_interval > 0:
            refresher = asyncio.create_task(
                self.schema_cache.periodic_refresh(self.settings.schema_refresh_interval)
            )

        yield

        # ── 关闭 ──
        prewarmer.cancel()
        if refresher:
            refresher.cancel()
        await self.pool_manager.close_all()
        await redis.aclose()
```

### 4.4 连接池管理 (`db/pool.py`)

```python
import asyncpg

class ConnectionPoolManager:
    def __init__(self, settings: Settings):
        self._settings = settings
        self._pools: dict[str, asyncpg.Pool] = {}

    async def get_pool(self, database: str) -> asyncpg.Pool:
        if database not in self._pools:
            ssl_ctx = self._build_ssl_context()
            self._pools[database] = await asyncpg.create_pool(
                host=self._settings.pg_host,
                port=self._settings.pg_port,
                user=self._settings.pg_user,
                password=self._settings.pg_password.get_secret_value(),
                database=database,
                min_size=1,
                max_size=self._settings.db_pool_size,
                command_timeout=self._settings.query_timeout,
                ssl=ssl_ctx,
            )
        return self._pools[database]

    async def assert_readonly(self):
        """检查 PG 用户是否为只读角色"""
        pool = await self.get_pool("postgres")
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT rolsuper, rolcreaterole, rolcreatedb "
                "FROM pg_roles WHERE rolname = current_user"
            )
            if row["rolsuper"]:
                raise RuntimeError("STRICT_READONLY: 当前用户为 superuser，拒绝启动")

    async def close_all(self):
        for pool in self._pools.values():
            await pool.close()
        self._pools.clear()
```

### 4.5 Schema 发现与缓存 (`schema/`)

#### 4.5.1 状态机 (`schema/state.py`)

```python
class SchemaState(str, Enum):
    UNLOADED = "unloaded"
    LOADING  = "loading"
    READY    = "ready"
    FAILED   = "failed"
```

#### 4.5.2 发现 (`schema/discovery.py`)

```python
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import inspect as sa_inspect

class SchemaDiscovery:
    def __init__(self, settings: Settings):
        self._settings = settings

    async def discover_databases(self) -> list[str]:
        """连接默认库，查询 pg_database 获取可访问数据库列表"""
        engine = create_async_engine(self._build_url("postgres"))
        async with engine.connect() as conn:
            result = await conn.execute(text(
                "SELECT datname FROM pg_database "
                "WHERE datistemplate = false AND datallowconn = true"
            ))
            all_dbs = [row[0] for row in result]
        await engine.dispose()

        exclude = set(self._settings.pg_exclude_databases.split(","))
        if self._settings.pg_databases:
            allowed = set(self._settings.pg_databases.split(","))
            return [db for db in all_dbs if db in allowed and db not in exclude]
        return [db for db in all_dbs if db not in exclude]

    async def load_schema(self, database: str) -> DatabaseSchema:
        """通过 SQLAlchemy inspect 读取完整 schema"""
        engine = create_async_engine(self._build_url(database))
        try:
            def _inspect(sync_conn):
                inspector = sa_inspect(sync_conn)
                tables, views, indexes, fks, enums = [], [], [], [], []

                for schema_name in inspector.get_schema_names():
                    if schema_name in ("pg_catalog", "information_schema", "pg_toast"):
                        continue

                    for table_name in inspector.get_table_names(schema=schema_name):
                        cols = [ColumnInfo(
                            name=c["name"],
                            type=str(c["type"]),
                            nullable=c["nullable"],
                            default=str(c["default"]) if c["default"] else None,
                            comment=c.get("comment"),
                            is_primary_key=c["name"] in pk_cols,
                        ) for c in inspector.get_columns(table_name, schema=schema_name)
                            for pk_cols in [
                                [pk["name"] for pk in inspector.get_pk_constraint(
                                    table_name, schema=schema_name
                                ).get("constrained_columns", [])]
                            ]]

                        tables.append(TableInfo(
                            schema_name=schema_name,
                            table_name=table_name,
                            columns=cols,
                            comment=inspector.get_table_comment(
                                table_name, schema=schema_name
                            ).get("text"),
                        ))

                        for idx in inspector.get_indexes(table_name, schema=schema_name):
                            indexes.append(IndexInfo(
                                schema_name=schema_name,
                                table_name=table_name,
                                index_name=idx["name"],
                                columns=idx["column_names"],
                                index_type=idx.get("dialect_options", {}).get(
                                    "postgresql_using", "btree"
                                ),
                                is_unique=idx["unique"],
                            ))

                        for fk in inspector.get_foreign_keys(table_name, schema=schema_name):
                            fks.append(ForeignKeyInfo(
                                constraint_name=fk["name"] or "",
                                source_schema=schema_name,
                                source_table=table_name,
                                source_columns=fk["constrained_columns"],
                                target_schema=fk.get("referred_schema") or schema_name,
                                target_table=fk["referred_table"],
                                target_columns=fk["referred_columns"],
                            ))

                    # Views
                    for view_name in inspector.get_view_names(schema=schema_name):
                        view_cols = [ColumnInfo(name=c["name"], type=str(c["type"]),
                                     nullable=c["nullable"])
                                     for c in inspector.get_columns(view_name, schema=schema_name)]
                        view_def = inspector.get_view_definition(view_name, schema=schema_name)
                        views.append(ViewInfo(
                            schema_name=schema_name, view_name=view_name,
                            columns=view_cols, definition=view_def,
                        ))

                # Enum types (via raw SQL)
                # ...

                return DatabaseSchema(
                    database=database, tables=tables, views=views,
                    indexes=indexes, foreign_keys=fks, enum_types=enums,
                    loaded_at=datetime.utcnow(),
                )

            async with engine.connect() as conn:
                schema = await conn.run_sync(_inspect)
            return schema
        finally:
            await engine.dispose()
```

#### 4.5.3 Redis 缓存 (`schema/cache.py`)

```python
import gzip, json
from redis.asyncio import Redis

class SchemaCache:
    PREFIX = "pg_mcp"

    def __init__(self, redis: Redis, pool_mgr: ConnectionPoolManager, settings: Settings):
        self._redis = redis
        self._discovery = SchemaDiscovery(settings)
        self._pool_mgr = pool_mgr
        self._settings = settings
        self._databases: list[str] = []
        self._loading_locks: dict[str, asyncio.Lock] = {}

    # ── 数据库发现 ──

    async def discover_databases(self):
        self._databases = await self._discovery.discover_databases()
        await self._redis.sadd(f"{self.PREFIX}:databases", *self._databases)
        for db in self._databases:
            state = await self._get_state(db)
            if state is None:
                await self._set_state(db, SchemaState.UNLOADED)

    # ── 状态机 ──

    async def _get_state(self, db: str) -> SchemaState | None:
        val = await self._redis.get(f"{self.PREFIX}:state:{db}")
        return SchemaState(val.decode()) if val else None

    async def _set_state(self, db: str, state: SchemaState):
        await self._redis.set(f"{self.PREFIX}:state:{db}", state.value)

    # ── Schema 获取（懒加载） ──

    async def get_schema(self, database: str) -> DatabaseSchema:
        state = await self._get_state(database)

        if state == SchemaState.READY:
            cached = await self._redis.get(f"{self.PREFIX}:schema:{database}")
            if cached:
                return DatabaseSchema.model_validate_json(gzip.decompress(cached))

        if state == SchemaState.LOADING:
            raise SchemaNotReadyError(f"Schema for {database} is loading")

        # state == UNLOADED or FAILED → 触发加载
        await self._load_schema(database)
        raise SchemaNotReadyError(f"Schema loading triggered for {database}")

    async def _load_schema(self, database: str):
        if database not in self._loading_locks:
            self._loading_locks[database] = asyncio.Lock()

        if self._loading_locks[database].locked():
            return  # 已有加载任务

        asyncio.create_task(self._do_load(database))

    async def _do_load(self, database: str):
        async with self._loading_locks[database]:
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
            except Exception:
                await self._set_state(database, SchemaState.FAILED)
                raise

    # ── 预热 & 刷新 ──

    async def prewarm_all(self):
        tasks = [self._do_load(db) for db in self._databases]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def periodic_refresh(self, interval: int):
        while True:
            await asyncio.sleep(interval)
            await self.prewarm_all()

    async def refresh(self, database: str | None = None):
        targets = [database] if database else self._databases
        for db in targets:
            await self._set_state(db, SchemaState.UNLOADED)
        await asyncio.gather(*[self._do_load(db) for db in targets], return_exceptions=True)
        return {"refreshed": targets}

    def ready_databases(self) -> list[str]:
        """同步返回内存中已知的数据库列表（状态可能异步变化）"""
        return list(self._databases)
```

#### 4.5.4 大 Schema 检索 (`schema/retriever.py`)

```python
import re

class SchemaRetriever:
    def __init__(self, settings: Settings):
        self._max_tables = settings.schema_max_tables_for_full_context

    def retrieve(self, schema: DatabaseSchema, user_query: str) -> str:
        """如果表数量 <= 阈值，返回完整 schema；否则返回检索子集"""
        if schema.table_count() <= self._max_tables:
            return schema.to_prompt_text()

        keywords = self._extract_keywords(user_query)
        relevant_tables = self._match_tables(schema, keywords)

        # 补充外键关联表（一度关联）
        related = set()
        relevant_names = {(t.schema_name, t.table_name) for t in relevant_tables}
        for fk in schema.foreign_keys:
            src = (fk.source_schema, fk.source_table)
            tgt = (fk.target_schema, fk.target_table)
            if src in relevant_names:
                related.add(tgt)
            elif tgt in relevant_names:
                related.add(src)

        for t in schema.tables:
            if (t.schema_name, t.table_name) in related:
                relevant_tables.append(t)

        # 构建精简 schema 文本
        subset = DatabaseSchema(
            database=schema.database,
            tables=relevant_tables,
            views=[v for v in schema.views
                   if any(k in v.view_name.lower() for k in keywords)],
            indexes=[i for i in schema.indexes
                     if (i.schema_name, i.table_name)
                     in {(t.schema_name, t.table_name) for t in relevant_tables}],
            foreign_keys=[fk for fk in schema.foreign_keys
                          if (fk.source_schema, fk.source_table) in relevant_names
                          or (fk.target_schema, fk.target_table) in relevant_names],
            enum_types=schema.enum_types,
            loaded_at=schema.loaded_at,
        )
        return subset.to_prompt_text()

    def _extract_keywords(self, query: str) -> list[str]:
        # 中文分词 + 英文 token 化，过滤停用词
        tokens = re.findall(r'[\w\u4e00-\u9fff]+', query.lower())
        stopwords = {"的", "和", "在", "是", "了", "查询", "所有", "每个",
                     "the", "a", "an", "of", "in", "for", "all", "each"}
        return [t for t in tokens if t not in stopwords and len(t) > 1]

    def _match_tables(self, schema: DatabaseSchema, keywords: list[str]) -> list[TableInfo]:
        scored = []
        for table in schema.tables:
            score = 0
            searchable = f"{table.table_name} {table.comment or ''} " + \
                         " ".join(c.name + " " + (c.comment or "") for c in table.columns)
            searchable = searchable.lower()
            for kw in keywords:
                if kw in searchable:
                    score += 1
            if score > 0:
                scored.append((score, table))
        scored.sort(key=lambda x: -x[0])
        return [t for _, t in scored[:self._max_tables]]
```

### 4.6 SQL 生成 (`engine/sql_generator.py`)

```python
from openai import AsyncOpenAI

SYSTEM_PROMPT = """You are a PostgreSQL SQL expert. Generate a single SQL query based on the user's
natural language request and the provided database schema.

Rules:
- Generate ONLY a single SELECT statement (WITH/CTE is allowed)
- Use proper PostgreSQL syntax and functions
- Use qualified table names (schema.table) when schema is not public
- Include appropriate WHERE, GROUP BY, ORDER BY, LIMIT as needed
- NEVER generate INSERT, UPDATE, DELETE, DROP, or any DDL/DML
- NEVER use pg_sleep, dblink, pg_read_file, or advisory locks
- Return ONLY the raw SQL, no markdown, no explanation"""

class SqlGenerator:
    def __init__(self, settings: Settings):
        self._client = AsyncOpenAI(
            api_key=settings.openai_api_key.get_secret_value(),
            base_url=settings.openai_base_url,
        )
        self._model = settings.openai_model

    async def generate(
        self,
        user_query: str,
        schema_context: str,
        error_feedback: str | None = None,
    ) -> SqlGenerationResult:
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": self._build_user_prompt(
                user_query, schema_context, error_feedback
            )},
        ]

        response = await self._client.chat.completions.create(
            model=self._model,
            messages=messages,
            temperature=0,
            max_tokens=2048,
            logprobs=True,
            top_logprobs=1,
        )

        choice = response.choices[0]
        sql = choice.message.content.strip().strip("```sql").strip("```").strip()
        avg_logprob = self._calc_avg_logprob(choice.logprobs)

        return SqlGenerationResult(
            sql=sql,
            avg_logprob=avg_logprob,
            prompt_tokens=response.usage.prompt_tokens,
            completion_tokens=response.usage.completion_tokens,
        )

    def _build_user_prompt(self, query: str, schema: str, feedback: str | None) -> str:
        parts = [f"Database schema:\n{schema}\n\nUser query: {query}"]
        if feedback:
            parts.append(f"\nPrevious attempt failed: {feedback}\nPlease fix the SQL.")
        return "\n".join(parts)

    def _calc_avg_logprob(self, logprobs) -> float:
        if not logprobs or not logprobs.content:
            return 0.0
        vals = [t.logprob for t in logprobs.content if t.logprob is not None]
        return sum(vals) / len(vals) if vals else 0.0

class SqlGenerationResult(BaseModel):
    sql: str
    avg_logprob: float
    prompt_tokens: int
    completion_tokens: int
```

### 4.7 SQL 安全校验 (`engine/sql_validator.py`)

```python
import sqlglot
from sqlglot import exp
from sqlglot.dialects.postgres import Postgres

# 显式禁止的高风险函数
BLOCKED_FUNCTIONS = frozenset({
    # 文件系统
    "pg_read_file", "pg_read_binary_file", "pg_ls_dir",
    "pg_stat_file",
    # 大对象
    "lo_import", "lo_export", "lo_get", "lo_put",
    # 锁与通知
    "pg_sleep", "pg_advisory_lock", "pg_advisory_xact_lock",
    "pg_advisory_unlock", "pg_advisory_unlock_all",
    "pg_try_advisory_lock", "pg_try_advisory_xact_lock",
    "pg_notify", "pg_listening_channels",
    # 外部数据
    "dblink", "dblink_exec", "dblink_connect", "dblink_disconnect",
    "dblink_send_query", "dblink_get_result",
    # 危险系统函数
    "pg_terminate_backend", "pg_cancel_backend",
    "pg_reload_conf", "pg_rotate_logfile",
    "set_config", "current_setting",
    "pg_switch_wal", "pg_create_restore_point",
})

# 禁止的 AST 节点类型（DML/DDL）
BLOCKED_NODE_TYPES = (
    exp.Insert, exp.Update, exp.Delete,
    exp.Create, exp.Drop, exp.AlterTable,
    exp.Grant,
    exp.Command,  # COPY, CALL 等
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
        #    允许: SELECT, UNION/INTERSECT/EXCEPT (含 CTE)
        #    允许: EXPLAIN (作为 Command)
        if isinstance(ast, exp.Command):
            if ast.this and ast.this.upper() == "EXPLAIN":
                return ValidationResult(valid=True, is_explain=True)
            return ValidationResult(valid=False, code="E_SQL_UNSAFE",
                                    reason=f"禁止的命令: {ast.this}")

        if not isinstance(ast, (exp.Select, exp.Union, exp.Intersect, exp.Except, exp.Subquery)):
            return ValidationResult(valid=False, code="E_SQL_UNSAFE",
                                    reason=f"仅允许 SELECT 语句，检测到: {type(ast).__name__}")

        # 4. 递归检查子树中的 DML/DDL
        for node in ast.walk():
            if isinstance(node, BLOCKED_NODE_TYPES):
                return ValidationResult(valid=False, code="E_SQL_UNSAFE",
                                        reason=f"禁止的语句类型: {type(node).__name__}")

        # 5. 函数调用检查
        for func in ast.find_all(exp.Func):
            func_name = (func.sql_name() if hasattr(func, "sql_name") else
                         type(func).__name__).lower()
            if func_name in BLOCKED_FUNCTIONS:
                return ValidationResult(valid=False, code="E_SQL_UNSAFE",
                                        reason=f"禁止调用函数: {func_name}")

        # Anonymous 函数（SQLGlot 无法识别的函数）
        for anon in ast.find_all(exp.Anonymous):
            name = anon.this.lower() if isinstance(anon.this, str) else ""
            if name in BLOCKED_FUNCTIONS:
                return ValidationResult(valid=False, code="E_SQL_UNSAFE",
                                        reason=f"禁止调用函数: {name}")

        # 6. Foreign table 检查（需 schema 信息）
        if schema:
            foreign_tables = self._get_foreign_tables(schema)
            for table in ast.find_all(exp.Table):
                table_id = f"{table.db or 'public'}.{table.name}"
                if table_id in foreign_tables:
                    return ValidationResult(valid=False, code="E_SQL_UNSAFE",
                                            reason=f"禁止访问 foreign table: {table_id}")

        return ValidationResult(valid=True)

    def _get_foreign_tables(self, schema: DatabaseSchema) -> set[str]:
        # 从 schema 发现中获取 foreign table 列表
        # 需在 SchemaDiscovery 中额外查询 information_schema.foreign_tables
        return set()

class ValidationResult(BaseModel):
    valid: bool
    code: str | None = None
    reason: str | None = None
    is_explain: bool = False
```

### 4.8 SQL 执行 (`engine/sql_executor.py`)

```python
import asyncpg
import sys

class SqlExecutor:
    def __init__(self, pool_mgr: ConnectionPoolManager, settings: Settings):
        self._pool_mgr = pool_mgr
        self._settings = settings

    async def execute(self, database: str, sql: str) -> ExecutionResult:
        pool = await self._pool_mgr.get_pool(database)
        async with pool.acquire() as conn:
            # 会话级安全参数
            await conn.execute(f"SET statement_timeout = '{self._settings.query_timeout}s'")
            await conn.execute(f"SET work_mem = '{self._settings.session_work_mem}'")
            await conn.execute(f"SET temp_file_limit = '{self._settings.session_temp_file_limit}'")
            await conn.execute("SET max_parallel_workers_per_gather = 2")

            try:
                async with conn.transaction(readonly=True):
                    # 添加 LIMIT 保护
                    limited_sql = self._apply_limit(sql)
                    rows = await conn.fetch(limited_sql)
            except asyncpg.QueryCanceledError:
                raise SqlTimeoutError(f"查询超时（{self._settings.query_timeout}s）")
            except asyncpg.PostgresError as e:
                raise SqlExecuteError(str(e))

        return self._process_result(rows)

    def _apply_limit(self, sql: str) -> str:
        """如果 SQL 没有 LIMIT，注入 MAX_ROWS+1 的 LIMIT 以检测截断"""
        upper = sql.upper().strip().rstrip(";")
        if "LIMIT" not in upper.split(")")[-1]:  # 粗略检查顶层
            return f"({sql}) AS __q LIMIT {self._settings.max_rows + 1}"
        return sql

    def _process_result(self, rows: list[asyncpg.Record]) -> ExecutionResult:
        if not rows:
            return ExecutionResult(columns=[], column_types=[], rows=[], row_count=0)

        columns = list(rows[0].keys())
        column_types = [type(rows[0][c]).__name__ for c in columns]

        truncated = len(rows) > self._settings.max_rows
        result_rows = rows[:self._settings.max_rows]

        # 序列化 + 大小检查
        processed_rows = []
        total_bytes = 0
        for row in result_rows:
            processed_row = []
            for col in columns:
                val = self._serialize_cell(row[col])
                processed_row.append(val)
            processed_rows.append(processed_row)

            row_bytes = sum(len(str(v).encode()) for v in processed_row)
            total_bytes += row_bytes

            if total_bytes > self._settings.max_result_bytes_hard:
                raise ResultTooLargeError(
                    f"结果集超过硬阈值 {self._settings.max_result_bytes_hard} bytes")

        truncated_reason = None
        if total_bytes > self._settings.max_result_bytes:
            # 软截断：按比例截取
            ratio = self._settings.max_result_bytes / total_bytes
            keep = max(1, int(len(processed_rows) * ratio))
            processed_rows = processed_rows[:keep]
            truncated = True
            truncated_reason = f"结果集超过软阈值 {self._settings.max_result_bytes} bytes"
        elif truncated:
            truncated_reason = f"结果行数超过 {self._settings.max_rows}"

        return ExecutionResult(
            columns=columns,
            column_types=column_types,
            rows=processed_rows,
            row_count=len(processed_rows),
            total_rows_before_limit=len(rows),
            truncated=truncated,
            truncated_reason=truncated_reason,
        )

    def _serialize_cell(self, value) -> str | None:
        if value is None:
            return None
        s = str(value)
        if len(s.encode()) > self._settings.max_cell_bytes:
            return s[:self._settings.max_cell_bytes] + "...[truncated]"
        return s
```

### 4.9 数据库推断 (`engine/db_inference.py`)

```python
class DbInference:
    AMBIGUITY_THRESHOLD = 0.15  # top1-top2 差距 < 15% 视为歧义

    def __init__(self, schema_cache: SchemaCache):
        self._cache = schema_cache

    async def infer(self, user_query: str) -> str:
        databases = self._cache.ready_databases()
        if not databases:
            raise DbInferNoMatchError("无可用数据库")

        scored: list[tuple[str, float]] = []
        keywords = SchemaRetriever._extract_keywords(None, user_query)  # 复用

        multi_db_hit = []
        for db in databases:
            try:
                schema = await self._cache.get_schema(db)
            except SchemaNotReadyError:
                continue

            score = self._score(schema, keywords)
            if score > 0:
                multi_db_hit.append(db)
            scored.append((db, score))

        scored.sort(key=lambda x: -x[1])

        if not scored or scored[0][1] == 0:
            raise DbInferNoMatchError(f"查询 '{user_query}' 无法匹配到任何数据库")

        # 跨库检测
        if len(multi_db_hit) > 1 and self._looks_cross_db(user_query, multi_db_hit):
            raise CrossDbUnsupportedError(f"查询涉及多个数据库: {multi_db_hit}")

        # 歧义检测
        if len(scored) >= 2:
            top1, top2 = scored[0][1], scored[1][1]
            if top1 > 0 and (top1 - top2) / top1 < self.AMBIGUITY_THRESHOLD:
                raise DbInferAmbiguousError(
                    message=f"多个候选数据库: {scored[0][0]}, {scored[1][0]}",
                    candidates=[s[0] for s in scored[:3]],
                )

        return scored[0][0]

    def _score(self, schema: DatabaseSchema, keywords: list[str]) -> float:
        score = 0.0
        for table in schema.tables:
            searchable = (f"{table.table_name} {table.comment or ''} " +
                          " ".join(c.name for c in table.columns)).lower()
            for kw in keywords:
                if kw in searchable:
                    # 表名命中权重 3，列名命中权重 1，注释命中权重 2
                    if kw in table.table_name.lower():
                        score += 3.0
                    elif table.comment and kw in table.comment.lower():
                        score += 2.0
                    else:
                        score += 1.0
        return score

    def _looks_cross_db(self, query: str, dbs: list[str]) -> bool:
        """简单启发：查询中是否显式提及多个库名"""
        mentioned = [db for db in dbs if db.lower() in query.lower()]
        return len(mentioned) > 1
```

### 4.10 结果验证 (`engine/result_validator.py`)

```python
VALIDATION_PROMPT = """You are a SQL query validator. Given a user's question, the generated SQL,
and query result metadata, determine if the SQL correctly answers the question.

Respond with JSON only:
{
  "verdict": "pass" | "fix" | "fail",
  "reason": "brief explanation",
  "suggested_sql": "corrected SQL (only when verdict is fix)"
}"""

class ResultValidator:
    def __init__(self, settings: Settings):
        self._client = AsyncOpenAI(
            api_key=settings.openai_api_key.get_secret_value(),
            base_url=settings.openai_base_url,
        )
        self._model = settings.openai_model
        self._settings = settings

    def should_validate(self, sql: str, result: ExecutionResult,
                        generation: SqlGenerationResult) -> bool:
        if not self._settings.enable_validation:
            return False
        # JOIN >= 2
        if sql.upper().count("JOIN") >= 2:
            return True
        # 子查询或窗口函数
        if "OVER(" in sql.upper() or sql.upper().count("SELECT") > 1:
            return True
        # 空结果
        if result.row_count == 0:
            return True
        # 低置信度
        if generation.avg_logprob < self._settings.validation_confidence_threshold:
            return True
        return False

    async def validate(self, user_query: str, sql: str,
                       result: ExecutionResult, schema: DatabaseSchema) -> ValidationVerdict:
        user_content = self._build_prompt(user_query, sql, result, schema)
        response = await self._client.chat.completions.create(
            model=self._model,
            messages=[
                {"role": "system", "content": VALIDATION_PROMPT},
                {"role": "user", "content": user_content},
            ],
            temperature=0,
            response_format={"type": "json_object"},
        )
        return ValidationVerdict.model_validate_json(response.choices[0].message.content)

    def _build_prompt(self, query: str, sql: str,
                      result: ExecutionResult, schema: DatabaseSchema) -> str:
        parts = [
            f"User question: {query}",
            f"Generated SQL:\n```sql\n{sql}\n```",
            f"Result: {result.row_count} rows, columns: {result.columns}",
            f"Column types: {result.column_types}",
        ]

        policy = self._settings.validation_data_policy
        if policy == ValidationDataPolicy.full and result.rows:
            sample = result.rows[:self._settings.validation_sample_rows]
            parts.append(f"Sample rows:\n{json.dumps(sample, ensure_ascii=False)}")
        elif policy == ValidationDataPolicy.masked and result.rows:
            sample = self._mask_pii(result.rows[:self._settings.validation_sample_rows],
                                    result.columns)
            parts.append(f"Sample rows (masked):\n{json.dumps(sample, ensure_ascii=False)}")
        # metadata_only: 不发送数据行

        return "\n\n".join(parts)

    def _mask_pii(self, rows: list[list], columns: list[str]) -> list[list]:
        pii_patterns = {"email", "phone", "name", "address", "ssn", "password", "token"}
        pii_cols = {i for i, c in enumerate(columns) if any(p in c.lower() for p in pii_patterns)}
        return [[("***" if i in pii_cols else v) for i, v in enumerate(row)] for row in rows]

class ValidationVerdict(BaseModel):
    verdict: str          # pass | fix | fail
    reason: str
    suggested_sql: str | None = None
```

### 4.11 主编排器 (`engine/orchestrator.py`)

```python
import asyncio
import time

class QueryEngine:
    def __init__(self, pool_manager, schema_cache, settings):
        self._pool = pool_manager
        self._cache = schema_cache
        self._settings = settings
        self._semaphore = asyncio.Semaphore(settings.max_concurrent_requests)
        self._db_inference = DbInference(schema_cache)
        self._sql_gen = SqlGenerator(settings)
        self._sql_val = SqlValidator()
        self._sql_exec = SqlExecutor(pool_manager, settings)
        self._result_val = ResultValidator(settings)
        self._retriever = SchemaRetriever(settings)

    async def execute(self, request: QueryRequest) -> QueryResponse:
        request_id = str(uuid.uuid4())
        log = structlog.get_logger().bind(request_id=request_id)
        t0 = time.monotonic()

        # 并发控制
        if self._semaphore._value <= 0:
            raise RateLimitedError("服务繁忙，请稍后重试")
        async with self._semaphore:
            return await self._do_execute(request, request_id, log, t0)

    async def _do_execute(self, req: QueryRequest, request_id: str,
                          log, t0: float) -> QueryResponse:
        # ── 0. Admin action ──
        if req.admin_action == "refresh_schema":
            result = await self._cache.refresh(req.database)
            return QueryResponse(request_id=request_id, warnings=[str(result)])

        # ── 1. 输入校验 ──
        if not req.query or not req.query.strip():
            raise InvalidArgumentError("query 不能为空")
        if len(req.query) > 2000:
            raise InvalidArgumentError("query 超过 2000 字符限制")

        # ── 2. 确定数据库 ──
        if req.database:
            if req.database not in self._cache.ready_databases():
                # 可能存在但 schema 未加载
                try:
                    await self._cache.get_schema(req.database)
                except SchemaNotReadyError:
                    raise
                raise DbNotFoundError(f"数据库 '{req.database}' 不存在")
            database = req.database
        else:
            database = await self._db_inference.infer(req.query)
        log = log.bind(database=database)

        # ── 3. 获取 Schema ──
        schema = await self._cache.get_schema(database)
        schema_context = self._retriever.retrieve(schema, req.query)
        log.info("schema_loaded", table_count=schema.table_count(),
                 elapsed_ms=self._elapsed(t0))

        # ── 4. SQL 生成 + 校验 (含重试) ──
        sql = None
        error_feedback = None
        for attempt in range(1 + self._settings.max_retries):
            t_gen = time.monotonic()
            gen_result = await self._sql_gen.generate(
                req.query, schema_context, error_feedback)
            log.info("sql_generated", attempt=attempt,
                     tokens=gen_result.prompt_tokens + gen_result.completion_tokens,
                     logprob=gen_result.avg_logprob,
                     elapsed_ms=self._elapsed(t_gen))

            # 校验
            val_result = self._sql_val.validate(gen_result.sql, schema)
            if val_result.valid:
                sql = gen_result.sql
                break
            else:
                log.warning("sql_validation_failed", reason=val_result.reason,
                            attempt=attempt)
                error_feedback = f"Safety check failed: {val_result.reason}"

        if sql is None:
            raise SqlUnsafeError(f"多次重试后仍无法生成安全 SQL: {val_result.reason}")

        # ── 5. return_type=sql → 直接返回 ──
        if req.return_type == ReturnType.sql:
            return QueryResponse(
                request_id=request_id,
                database=database,
                sql=sql,
                schema_loaded_at=schema.loaded_at.isoformat(),
            )

        # ── 6. 执行 SQL ──
        t_exec = time.monotonic()
        exec_result = await self._sql_exec.execute(database, sql)
        log.info("sql_executed", row_count=exec_result.row_count,
                 truncated=exec_result.truncated, elapsed_ms=self._elapsed(t_exec))

        # ── 7. 结果验证（可选） ──
        validation_used = False
        if self._result_val.should_validate(sql, exec_result, gen_result):
            validation_used = True
            for v_attempt in range(1 + self._settings.max_retries):
                t_val = time.monotonic()
                verdict = await self._result_val.validate(
                    req.query, sql, exec_result, schema)
                log.info("result_validated", verdict=verdict.verdict,
                         attempt=v_attempt, elapsed_ms=self._elapsed(t_val))

                if verdict.verdict == "pass":
                    break
                elif verdict.verdict == "fix" and verdict.suggested_sql:
                    # 对修正 SQL 再做安全校验
                    fix_val = self._sql_val.validate(verdict.suggested_sql, schema)
                    if fix_val.valid:
                        sql = verdict.suggested_sql
                        exec_result = await self._sql_exec.execute(database, sql)
                    else:
                        break  # 修正 SQL 不安全，放弃
                elif verdict.verdict == "fail":
                    raise ValidationFailedError(verdict.reason)

        # ── 8. 组装响应 ──
        log.info("request_completed", elapsed_ms=self._elapsed(t0))
        return QueryResponse(
            request_id=request_id,
            database=database,
            sql=sql,
            columns=exec_result.columns,
            column_types=exec_result.column_types,
            rows=exec_result.rows,
            row_count=exec_result.row_count,
            truncated=exec_result.truncated,
            truncated_reason=exec_result.truncated_reason,
            validation_used=validation_used,
            schema_loaded_at=schema.loaded_at.isoformat(),
        )

    def _elapsed(self, t0: float) -> int:
        return int((time.monotonic() - t0) * 1000)
```

## 5. 数据流

```
Request
  │
  ▼
InputValidation ──error──▶ E_INVALID_ARGUMENT
  │
  ▼
AdminAction? ──yes──▶ refresh_schema → return
  │ no
  ▼
ResolveDatabase
  ├─ explicit ──not found──▶ E_DB_NOT_FOUND
  └─ infer ──ambiguous──▶ E_DB_INFER_AMBIGUOUS
            ──no match──▶ E_DB_INFER_NO_MATCH
            ──cross db──▶ E_CROSS_DB_UNSUPPORTED
  │
  ▼
LoadSchema ──not ready──▶ E_SCHEMA_NOT_READY (retry_after_ms)
  │
  ▼
RetrieveSchemaContext (full | search-based)
  │
  ▼
┌─▶ GenerateSQL (LLM) ──fail──▶ E_SQL_GENERATE / E_LLM_*
│     │
│     ▼
│   ValidateSQL (SQLGlot AST)
│     │
│     ├─ unsafe ──retry ≤ MAX_RETRIES──┐
│     │                                 │
│     └─ pass                          ─┘
│         │
│   return_type=sql? ──yes──▶ return {sql}
│         │ no
│         ▼
│   ExecuteSQL (asyncpg, readonly tx)
│     ├─ timeout ──▶ E_SQL_TIMEOUT
│     ├─ error ──▶ E_SQL_EXECUTE
│     ├─ hard limit ──▶ E_RESULT_TOO_LARGE
│     └─ ok (soft truncation possible)
│         │
│         ▼
│   ShouldValidateResult?
│     ├─ no ──▶ return response
│     └─ yes
│         │
│         ▼
│   ValidateResult (LLM)
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
| `pg_mcp:state:{db}` | STRING | 无 | Schema 状态：`unloaded`/`loading`/`ready`/`failed` |
| `pg_mcp:schema:{db}` | STRING (gzip JSON) | `SCHEMA_REFRESH_INTERVAL` | 完整 DatabaseSchema 序列化 |
| `pg_mcp:schema_summary:{db}` | STRING | 同上 | 压缩摘要文本（表名+列名），用于推断 |

序列化策略：`DatabaseSchema.model_dump_json()` → `gzip.compress()` → Redis SET。典型 200 表 schema 压缩后约 20-50KB。

## 7. 可观测性设计

### 7.1 日志格式

```json
{
  "timestamp": "2026-04-30T10:15:30.123Z",
  "level": "info",
  "event": "sql_executed",
  "request_id": "a1b2c3d4",
  "database": "ecommerce",
  "row_count": 42,
  "truncated": false,
  "elapsed_ms": 156
}
```

### 7.2 每次请求记录的关键事件

| 事件 | 记录字段 |
|------|---------|
| `request_received` | request_id, query_length, database, return_type |
| `schema_loaded` | database, table_count, cache_hit, elapsed_ms |
| `sql_generated` | attempt, prompt_tokens, completion_tokens, logprob, elapsed_ms |
| `sql_validation_failed` | attempt, reason |
| `sql_executed` | row_count, truncated, elapsed_ms |
| `result_validated` | verdict, attempt, data_policy, elapsed_ms |
| `request_completed` | total_elapsed_ms |
| `request_failed` | error_code, error_message |

### 7.3 脱敏规则

- SQL 中的字符串字面量替换为 `'***'`
- 结果行不写入日志
- `pg_password`、`openai_api_key` 使用 `SecretStr`，`repr()` 自动遮掩

## 8. 依赖清单

```toml
[project]
name = "pg-mcp"
requires-python = ">=3.11"
dependencies = [
    "mcp>=1.0",
    "fastapi>=0.115",
    "uvicorn[standard]>=0.29",
    "asyncpg>=0.29",
    "sqlalchemy[asyncio]>=2.0",
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

## 9. 部署

### 9.1 Docker

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml .
RUN pip install --no-cache-dir .
COPY pg_mcp/ pg_mcp/
ENTRYPOINT ["pg-mcp"]
CMD ["--transport", "sse"]
```

### 9.2 MCP 客户端配置 (Claude Desktop)

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

### 9.3 外部依赖

| 服务 | 必须 | 说明 |
|------|------|------|
| PostgreSQL 14-17 | 是 | 目标数据库 |
| Redis 7+ | 是 | Schema 缓存 |
| OpenAI API | 是 | SQL 生成 + 结果验证 |
