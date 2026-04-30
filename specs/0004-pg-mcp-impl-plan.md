# IMP-0004: pg-mcp 详细实现计划

> 基于 [Design-0002](./0002-pg-mcp-design.md) 的技术实现计划，编号 0004 与 PRD(0001)、Design(0002)、Review(0003) 保持连贯。

## 0. 实施原则与约束

- **分 3 个里程碑交付**，不追求 14 天全量上线：
  - **Milestone 1**：核心 stdio 查询路径（config, models, validator, executor, orchestrator, server, CLI）
  - **Milestone 2**：缓存/运营硬ening（Redis cache, singleflight, scheduled refresh, connection retry, PG_DATABASES override）
  - **Milestone 3**：SSE/管理/验证/性能（SSE transport, admin endpoints, result validation, inference/retrieval 性能优化）
- **自底向上开发**：models → errors → config → protocols → 基础设施 → engine → server → CLI
- **每个模块完成后立即写对应单元测试**，不堆在最后
- **集成测试最后统一编写**，但提前设计好 fixtures
- **Python 3.12+ 语法特性**：`type` 别名、`X | Y` union、`match/case`
- **全量类型注解 + mypy --strict**
- **代码即文档**：复杂逻辑必须有注释，公开 API 必须有 docstring

---

## 1. Phase 1: 项目骨架与核心模型（Milestone 1, Day 1-2）

### 1.1 项目初始化

**文件**: `pyproject.toml`

```toml
[project]
name = "pg-mcp"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "mcp>=1.0,<2.0",
    "fastapi>=0.115,<1.0",
    "uvicorn[standard]>=0.29,<1.0",
    "asyncpg>=0.29,<1.0",
    "openai>=1.50,<2.0",
    "redis[hiredis]>=5.0,<6.0",
    "sqlglot>=26.0,<31.0",
    "pydantic>=2.0,<3.0",
    "pydantic-settings>=2.0,<3.0",
    "structlog>=24.0,<30.0",
    "click>=8.0,<9.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0,<9.0",
    "pytest-asyncio>=0.24,<1.0",
    "pytest-cov>=5.0,<7.0",
    "ruff>=0.6,<1.0",
    "mypy>=1.11,<2.0",
]

[project.scripts]
pg-mcp = "pg_mcp.cli:main"

[tool.ruff]
target-version = "py312"
line-length = 100

[tool.ruff.lint]
select = ["E", "F", "W", "I", "N", "UP", "B", "C4", "SIM", "ASYNC"]

[tool.mypy]
python_version = "3.12"
strict = true
warn_return_any = true
warn_unused_configs = true
disallow_untyped_defs = true

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
addopts = "-q"
```

**文件**: `pg_mcp/__init__.py`, `pg_mcp/__main__.py`

```python
# __main__.py
from pg_mcp.cli import main

if __name__ == "__main__":
    main()
```

**验收标准**:
- [ ] `uv sync` 成功安装所有依赖
- [ ] `uv run ruff check .` 通过（空目录通过视为通过）
- [ ] `uv run mypy pg_mcp/` 通过

### 1.2 数据模型层 (`models/`)

**文件**: `pg_mcp/models/errors.py`

实现完整的异常层级：

```python
class ErrorCode(str, Enum):
    E_INVALID_ARGUMENT = "E_INVALID_ARGUMENT"
    E_DB_CONNECT = "E_DB_CONNECT"
    E_DB_NOT_FOUND = "E_DB_NOT_FOUND"
    E_DB_INFER_AMBIGUOUS = "E_DB_INFER_AMBIGUOUS"
    E_DB_INFER_NO_MATCH = "E_DB_INFER_NO_MATCH"
    E_CROSS_DB_UNSUPPORTED = "E_CROSS_DB_UNSUPPORTED"
    E_SCHEMA_NOT_READY = "E_SCHEMA_NOT_READY"
    E_SQL_GENERATE = "E_SQL_GENERATE"
    E_SQL_UNSAFE = "E_SQL_UNSAFE"
    E_SQL_PARSE = "E_SQL_PARSE"
    E_SQL_EXECUTE = "E_SQL_EXECUTE"
    E_SQL_TIMEOUT = "E_SQL_TIMEOUT"
    E_VALIDATION_FAILED = "E_VALIDATION_FAILED"
    E_LLM_TIMEOUT = "E_LLM_TIMEOUT"
    E_LLM_ERROR = "E_LLM_ERROR"
    E_RESULT_TOO_LARGE = "E_RESULT_TOO_LARGE"
    E_RATE_LIMITED = "E_RATE_LIMITED"

class PgMcpError(Exception):
    """所有业务异常的基类"""
    code: ErrorCode = ErrorCode.E_INVALID_ARGUMENT
    retry_after_ms: int | None = None
    candidates: list[str] | None = None

class InvalidArgumentError(PgMcpError): code = ErrorCode.E_INVALID_ARGUMENT
class DbConnectError(PgMcpError): code = ErrorCode.E_DB_CONNECT
class DbNotFoundError(PgMcpError): code = ErrorCode.E_DB_NOT_FOUND
class DbInferAmbiguousError(PgMcpError):
    code = ErrorCode.E_DB_INFER_AMBIGUOUS
    def __init__(self, message: str, candidates: list[str]):
        super().__init__(message)
        self.candidates = candidates
class DbInferNoMatchError(PgMcpError): code = ErrorCode.E_DB_INFER_NO_MATCH
class CrossDbUnsupportedError(PgMcpError): code = ErrorCode.E_CROSS_DB_UNSUPPORTED
class SchemaNotReadyError(PgMcpError):
    code = ErrorCode.E_SCHEMA_NOT_READY
    def __init__(self, message: str, retry_after_ms: int = 2000):
        super().__init__(message)
        self.retry_after_ms = retry_after_ms
class SqlGenerateError(PgMcpError): code = ErrorCode.E_SQL_GENERATE
class SqlUnsafeError(PgMcpError): code = ErrorCode.E_SQL_UNSAFE
class SqlParseError(PgMcpError): code = ErrorCode.E_SQL_PARSE
class SqlExecuteError(PgMcpError): code = ErrorCode.E_SQL_EXECUTE
class SqlTimeoutError(PgMcpError): code = ErrorCode.E_SQL_TIMEOUT
class ValidationFailedError(PgMcpError): code = ErrorCode.E_VALIDATION_FAILED
class LlmTimeoutError(PgMcpError): code = ErrorCode.E_LLM_TIMEOUT
class LlmError(PgMcpError): code = ErrorCode.E_LLM_ERROR
class ResultTooLargeError(PgMcpError): code = ErrorCode.E_RESULT_TOO_LARGE
class RateLimitedError(PgMcpError): code = ErrorCode.E_RATE_LIMITED
```

**文件**: `pg_mcp/models/schema.py`

实现所有 schema 数据模型（详见 Design §4.3.1）。关键实现点：

- `DatabaseSchema.to_prompt_text()`：将 schema 序列化为 LLM prompt 可用的纯文本格式
  - 格式示例：表名(列名:类型 [PK] [comment], ...)
  - 外键关系单独列出
  - 枚举类型单独列出
- `DatabaseSchema.to_summary_text()`：仅表名+列名，用于推断阶段（压缩 token）
- `DatabaseSchema.foreign_table_ids()`：返回 `set[str]` 格式为 `"schema.table"`

**文件**: `pg_mcp/models/request.py`

```python
from pydantic import field_validator

class QueryRequest(BaseModel):
    query: str = Field(default="", min_length=0, max_length=2000)
    database: str | None = None
    return_type: Literal["sql", "result"] = "result"
    admin_action: Literal["refresh_schema"] | None = None

    @model_validator(mode="after")
    def _check_query_or_admin(self) -> "QueryRequest":
        # admin_action 模式下 query 可为空；否则必须非空非纯空白
        if not self.admin_action:
            stripped = self.query.strip()
            if not stripped:
                raise ValueError("query is required when admin_action is not set")
            self.query = stripped
        return self
```

**文件**: `pg_mcp/models/response.py`

```python
class ErrorDetail(BaseModel):
    code: str
    message: str
    retry_after_ms: int | None = None
    candidates: list[str] | None = None

class AdminRefreshResult(BaseModel):
    succeeded: list[str]
    failed: list[dict[str, str]]

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
    refresh_result: AdminRefreshResult | None = None  # admin_action=refresh_schema 时填充
    warnings: list[str] = Field(default_factory=list)
    error: ErrorDetail | None = None
```

**文件**: `pg_mcp/models/__init__.py`

导出所有公共模型。

**验收标准**:
- [ ] 所有模型通过 mypy --strict
- [ ] `DatabaseSchema.model_validate_json()` 和 `.model_dump_json()` 工作正常
- [ ] 异常层级可通过 `isinstance(e, PgMcpError)` 和 `e.code` 正确区分

### 1.3 配置管理 (`config.py`)

**文件**: `pg_mcp/config.py`

实现 `Settings`（详见 Design §4.1）。关键注意点：

- `SecretStr` 用于 `pg_password` 和 `openai_api_key`
- 添加验证器：`pg_user` 和 `pg_password` 不能为空字符串
- `pg_databases` / `pg_exclude_databases` / `validation_deny_list`：逗号分隔字符串，下游统一用 property 解析

```python
from pydantic import field_validator, computed_field

class Settings(BaseSettings):
    # ... 其他字段 ...
    pg_databases: str = ""                          # 原始环境变量值，保留 str
    pg_exclude_databases: str = "template0,template1,postgres"
    validation_deny_list: str = ""

    @computed_field
    @property
    def pg_databases_list(self) -> list[str]:
        if not self.pg_databases:
            return []
        return [item.strip() for item in self.pg_databases.split(",") if item.strip()]

    @computed_field
    @property
    def pg_exclude_databases_list(self) -> list[str]:
        return [item.strip() for item in self.pg_exclude_databases.split(",") if item.strip()]

    @computed_field
    @property
    def validation_deny_list_items(self) -> list[str]:
        if not self.validation_deny_list:
            return []
        return [item.strip() for item in self.validation_deny_list.split(",") if item.strip()]
```

**注意**：保持原始字段为 `str`（环境变量直接映射），所有解析通过 `computed_field` property 统一进行。下游调用方使用 `settings.pg_databases_list` 等 property。

**验收标准**:
- [ ] 环境变量覆盖默认值
- [ ] `.env` 文件加载正常
- [ ] `repr(settings.pg_password)` 输出 `SecretStr('**********')`

### 1.4 Protocol 接口 (`protocols.py`)

**文件**: `pg_mcp/protocols.py`

定义所有组件的 Protocol 接口（详见 Design §4.2）。

同时定义中间结果类型：

```python
class SqlGenerationResult(BaseModel):
    sql: str
    prompt_tokens: int
    completion_tokens: int
    avg_logprob: float | None = None

class ValidationResult(BaseModel):
    valid: bool
    code: str | None = None          # 仅 valid=False 时有值
    reason: str | None = None        # 仅 valid=False 时有值
    is_explain: bool = False         # EXPLAIN 语句标记

class ExecutionResult(BaseModel):
    columns: list[str]
    column_types: list[str]
    rows: list[list]
    row_count: int
    truncated: bool = False
    truncated_reason: str | None = None

class ValidationVerdict(BaseModel):
    verdict: Literal["pass", "fix", "fail"]
    reason: str | None = None
    suggested_sql: str | None = None  # verdict=fix 时提供

class RefreshResult(BaseModel):
    succeeded: list[str]
    failed: list[dict[str, str]]
```

**验收标准**:
- [ ] 所有 Protocol 可被 `isinstance` 检查（通过 `@runtime_checkable`）
- [ ] 中间结果模型可通过 JSON 序列化/反序列化

---

## 2. Phase 2: 核心引擎开发（Milestone 1, Day 3-5）

### 2.1 SQL 安全校验 (`engine/sql_validator.py`)

**文件**: `pg_mcp/engine/sql_validator.py`

实现 SQLGlot AST 级别的安全校验（详见 Design §4.8）。

**核心实现要点**:

1. **解析阶段**：`sqlglot.parse(sql, dialect="postgres")`
   - 解析失败 → `ValidationResult(valid=False, code="E_SQL_PARSE", reason=...)`

2. **单语句检查**：过滤 `None` 后检查长度为 1
   - `len(stmts) != 1` → `E_SQL_UNSAFE` (多语句)

3. **语句级白名单**：
   - `exp.Select`, `exp.Union`, `exp.Intersect`, `exp.Except` → 通过
   - `exp.Command` 且为 `EXPLAIN` → 进一步检查是否含 `ANALYZE`
   - 其他 → `E_SQL_UNSAFE`

4. **递归 DML/DDL 检测**：遍历 AST 所有节点，遇到 `BLOCKED_NODE_TYPES` 中任意类型即拒绝

5. **函数安全策略**（双层）：
   - 黑名单兜底：`DENY_FUNCTIONS` 中的函数无条件拒绝
   - 白名单检查：传入 `schema.allowed_functions` 时，不在白名单中的函数拒绝
   - 注意：函数名提取需处理 `exp.Anonymous`（如 `dblink(...)`）和 `exp.Func` 子类

6. **Foreign table 检查**：
   - 从 `schema.foreign_table_ids()` 获取禁止访问的表（格式：`"schema.table"`，已规范化）
   - 遍历 AST 中的 `exp.Table` 节点，使用 `_canonicalize_table_id()` 规范化标识符后检查
   - 规范化规则：统一 lowercase，去除多余引号，默认 schema 为 `public`
   ```python
   def _canonicalize_table_id(table: exp.Table) -> str:
       schema = (table.db or "public").lower().strip('"')
       name = table.name.lower().strip('"')
       return f"{schema}.{name}"
   ```

**边界情况处理**:
- CTE (`WITH ...`) 中的子查询也需要递归检查
- `SELECT * FROM (VALUES (...))` 这种构造不含 `exp.Table`，应允许
- `exp.Anonymous` 的函数名提取：`node.this` 可能是字符串或其他类型，需防御性处理

**验收标准**:
- [ ] 必测矩阵全部通过（Design §8.2）
- [ ] 100% 分支覆盖
- [ ] mypy --strict 通过

**测试文件**: `tests/unit/test_sql_validator.py`

参数化测试覆盖：
- 通过案例：`SELECT 1`, `WITH ... SELECT`, `EXPLAIN SELECT`, 聚合查询, JOIN 查询
- 拒绝案例：`INSERT`, `UPDATE`, `DELETE`, `DROP`, `CREATE`, `GRANT`, `COPY`, `CALL`
- 函数黑名单：`pg_sleep`, `pg_read_file`, `lo_import`, `dblink`
- 多语句：`SELECT 1; DROP TABLE x`
- EXPLAIN ANALYZE 拒绝
- 不在白名单的自定义函数拒绝
- Foreign table 拒绝

### 2.2 数据库推断 (`engine/db_inference.py`)

**文件**: `pg_mcp/engine/db_inference.py`

实现数据库自动推断逻辑（详见 Design §4.10）。

**核心实现要点**:

1. **预计算倒排索引**：在 schema 加载时（非每次查询时）构建每个数据库的 `DbSummary`
   ```python
   @dataclass
   class DbSummary:
       database: str
       table_names: set[str]          # 所有表名 lowercase
       column_names: set[str]         # 所有列名 lowercase
       table_comments: list[str]      # 表注释文本
       column_comments: list[str]     # 列注释文本
       table_count: int
   ```
   - `SchemaCache` 加载 schema 后同步构建 `DbSummary` 存入内存字典
   - 推断时只扫描轻量 `DbSummary`，不反序列化完整 `DatabaseSchema`

2. **关键词提取**：从用户查询中提取关键词
   - 简单实现：分词（去除停用词）+ 保留长度 >= 2 的词
   - 保留数字（如 "2024"）和英文单词
   - 中文查询：按字/词切分（可用 jieba，但为减少依赖，先用简单规则：保留所有非停用词）

3. **相关性评分**：对每个数据库的 `DbSummary` 计算得分
   - 表名精确匹配：+10 分
   - 表名部分匹配：+5 分
   - 列名精确匹配：+3 分
   - 列注释匹配：+2 分
   - 表注释匹配：+2 分
   - 大小写不敏感匹配

4. **歧义检测**：`AMBIGUITY_THRESHOLD = 0.15`
   - `top1 > 0` 且 `(top1 - top2) / top1 < 0.15` → `DbInferAmbiguousError`
   - 返回前 3 个候选

5. **跨库检测**：`_entity_spread_cross_db`
   - 对每个关键词，找出得分最高的数据库
   - 如果最佳数据库分布在 2 个以上 → `CrossDbUnsupportedError`
   - 简化：如果有 >=2 个数据库得分 > 0 且各自有独立的最佳命中实体

6. **未就绪处理**：
   - 尝试获取每个库的 schema，遇到 `SchemaNotReadyError` 记录到 `not_ready`
   - 如果所有库都未就绪 → 返回 `SchemaNotReadyError(retry_after_ms=3000)`
   - 部分就绪时，如果能确定唯一目标则使用；否则返回未就绪错误

**验收标准**:
- [ ] 单元测试覆盖所有推断分支
- [ ] 歧义检测阈值准确
- [ ] 跨库检测正确识别分散实体
- [ ] 推断延迟 < 50ms（100 库场景）

**测试文件**: `tests/unit/test_db_inference.py`

Mock `SchemaCacheProtocol`，提供预置 schema 数据：
- 单库直接命中
- 多库歧义（top1/top2 差距 < 15%）
- 无匹配
- 跨库（不同关键词命中不同库）
- 部分库未就绪

### 2.3 Schema 检索 (`schema/retriever.py`)

**文件**: `pg_mcp/schema/retriever.py`

实现大 schema 的检索策略。

**核心实现要点**:

```python
@dataclass
class TableIndex:
    """预计算的每表检索索引，在 schema 加载时构建"""
    table_name: str
    all_terms: set[str]  # 表名、列名、注释的分词集合 lowercase

class SchemaRetriever:
    def __init__(self, max_tables_for_full: int = 50):
        self._max_tables = max_tables_for_full

    def should_use_retrieval(self, schema: DatabaseSchema) -> bool:
        return schema.table_count() > self._max_tables

    def build_index(self, schema: DatabaseSchema) -> list[TableIndex]:
        """在 schema 加载完成后预计算倒排索引"""
        indices = []
        for table in schema.tables:
            terms = {table.table_name.lower()}
            for col in table.columns:
                terms.add(col.name.lower())
                if col.comment:
                    terms.update(self._tokenize(col.comment.lower()))
            if table.comment:
                terms.update(self._tokenize(table.comment.lower()))
            indices.append(TableIndex(table.table_name, terms))
        return indices

    def retrieve(self, user_query: str, schema: DatabaseSchema) -> str:
        """返回精简后的 schema 文本，用于 LLM prompt"""
        keywords = self._extract_keywords(user_query)

        # 使用预计算索引评分（O(n_tables) 而非 O(n_tables * n_columns)）
        scored_tables: list[tuple[TableInfo, float]] = []
        for table, idx in zip(schema.tables, schema._retrieval_index):
            score = self._score_by_index(idx, keywords)
            scored_tables.append((table, score))

        # 取 top N（可配置，如 20 张）
        scored_tables.sort(key=lambda x: -x[1])
        top_tables = [t for t, s in scored_tables[:20] if s > 0]

        # 如果没有命中，返回前 N 张表（兜底）
        if not top_tables:
            top_tables = schema.tables[:20]

        # 包含相关表的外键关系（用于 JOIN 推断）
        related_fks = self._get_related_foreign_keys(top_tables, schema)

        return self._build_context(top_tables, related_fks, schema)
```

**验收标准**:
- [ ] 关键词与表/列/注释匹配正确
- [ ] 大 schema 时只返回相关子集
- [ ] 无命中时返回兜底表集合
- [ ] 外键关系正确关联
- [ ] 检索延迟 < 5ms（200 表场景）

**测试文件**: `tests/unit/test_schema_retriever.py`

### 2.4 SQL 生成器 (`engine/sql_generator.py`)

**文件**: `pg_mcp/engine/sql_generator.py`

实现 LLM SQL 生成。

**核心实现要点**:

```python
SQL_GENERATION_PROMPT = """You are a PostgreSQL SQL expert. Given the database schema below, generate a SQL query to answer the user's question.

Database Schema:
{schema_context}

User Question: {query}

Requirements:
- Generate only SELECT queries (or WITH ... SELECT)
- Do not use any functions that modify data
- Ensure the query is syntactically correct PostgreSQL
- Use appropriate JOINs when multiple tables are needed
- Add LIMIT if the user asks for a limited number of results

{feedback}

Respond with ONLY the SQL query, no explanations."""

class SqlGenerator:
    def __init__(self, client: AsyncOpenAI, settings: Settings):
        self._client = client
        self._settings = settings
        self._model = settings.openai_model

    async def generate(
        self,
        query: str,
        schema_context: str,
        feedback: str | None = None,
    ) -> SqlGenerationResult:
        feedback_text = f"\nPrevious attempt feedback: {feedback}" if feedback else ""
        prompt = SQL_GENERATION_PROMPT.format(
            schema_context=schema_context,
            query=query,
            feedback=feedback_text,
        )

        try:
            response = await asyncio.wait_for(
                self._client.chat.completions.create(
                    model=self._model,
                    messages=[
                        {"role": "system", "content": "You generate PostgreSQL SQL queries."},
                        {"role": "user", "content": prompt},
                    ],
                    temperature=0,
                ),
                timeout=self._settings.openai_timeout,
            )
        except asyncio.TimeoutError:
            raise LlmTimeoutError("SQL 生成 LLM 调用超时")
        except openai.APIError as e:
            raise LlmError(f"SQL 生成 LLM 调用失败: {e}")

        sql = response.choices[0].message.content or ""
        # 清理 SQL：去除 markdown 代码块标记
        sql = self._clean_sql(sql)

        usage = response.usage
        return SqlGenerationResult(
            sql=sql,
            prompt_tokens=usage.prompt_tokens if usage else 0,
            completion_tokens=usage.completion_tokens if usage else 0,
            avg_logprob=None,  # GPT-5-mini 可能不提供 logprob
        )

    def _clean_sql(self, sql: str) -> str:
        """去除 markdown 代码块标记和多余空白"""
        sql = sql.strip()
        if sql.startswith("```sql"):
            sql = sql[6:]
        elif sql.startswith("```"):
            sql = sql[3:]
        if sql.endswith("```"):
            sql = sql[:-3]
        return sql.strip()
```

**验收标准**:
- [ ] 正确调用 OpenAI API
- [ ] 超时正确转换为 `LlmTimeoutError`
- [ ] API 错误正确转换为 `LlmError`
- [ ] Markdown 代码块正确清理
- [ ] token 用量正确记录

**测试文件**: `tests/unit/test_sql_generator.py`

Mock `AsyncOpenAI` 的 `chat.completions.create`：
- 正常生成
- 超时
- API 错误
- Markdown 代码块清理

### 2.5 SQL 执行器 (`engine/sql_executor.py`)

**文件**: `pg_mcp/engine/sql_executor.py`

实现只读 SQL 执行（详见 Design §4.9）。

**方法签名**：
```python
class SqlExecutor:
    async def execute(
        self,
        database: str,
        sql: str,
        schema_names: list[str] | None = None,
        is_explain: bool = False,
    ) -> ExecutionResult:
        """执行 SQL。is_explain=True 时跳过 LIMIT 包裹，直接执行原 SQL。"""
```

**核心实现要点**:

1. **会话级安全配置**（使用 `SET` 参数化语句，禁止 f-string 拼接）：
   ```python
   # 使用 asyncpg 的参数化 SET（若不支持则使用 conn.execute 但确保值来自配置校验）
   await conn.execute(f"SET statement_timeout = '{timeout_s}s'")  # timeout_s 为内部 int
   await conn.execute(f"SET idle_in_transaction_session_timeout = '{idle_timeout_s}s'")
   await conn.execute(f"SET work_mem = '{self._settings.session_work_mem}'")
   await conn.execute(f"SET temp_file_limit = '{self._settings.session_temp_file_limit}'")
   await conn.execute("SET max_parallel_workers_per_gather = 2")
   ```
   > **安全说明**：上述 `SET` 中的值全部来自 `Settings` 的 Pydantic 校验字段（`int` 或预定义枚举），不来自用户输入。若需额外安全层，可通过 `SELECT set_config('name', 'value', false)` 使用参数化方式。

2. **search_path 设置**（使用标识符引用工具）：
   ```python
   def _quote_ident(ident: str) -> str:
       """使用 PostgreSQL 标准双引号规则引用标识符"""
       return '"' + ident.replace('"', '""') + '"'

   if schema_names:
       safe_schemas = ",".join(_quote_ident(s) for s in schema_names)
       await conn.execute(f"SET search_path = {safe_schemas}")
   ```

3. **只读事务**：`async with conn.transaction(readonly=True)`

4. **LIMIT 注入**（**外层强制包裹，不可绕过，EXPLAIN 豁免**）：
   ```python
   def _apply_limit(self, sql: str, is_explain: bool = False) -> str:
       """始终在外层包裹 LIMIT，防止 LLM 注入超大 LIMIT 绕过。EXPLAIN 语句跳过包裹。"""
       if is_explain:
           # EXPLAIN 不包裹 LIMIT，直接原样执行
           return sql.strip().rstrip(";")
       stripped = sql.strip().rstrip(";")
       limit = self._settings.max_rows + 1
       return f"SELECT * FROM ({stripped}) AS __pg_mcp_q LIMIT {limit}"
   ```
   - **普通 SELECT**：无论原 SQL 是否含 `LIMIT`，都包裹为外层 `SELECT * FROM (...) LIMIT (max_rows + 1)`
   - **EXPLAIN 语句**：跳过 LIMIT 包裹，直接执行原 SQL（validator 返回 `is_explain=True`）
   - 外层 LIMIT 固定使用 `max_rows + 1`，保证结果截断检测始终可用
   - 返回 `max_rows + 1` 行用于检测是否被截断
   - **AST 层校验**：先用 SQLGlot 解析确认是单条 SELECT，再包裹，避免语法错误

5. **结果处理**：
   - 遍历 `Record` 对象，提取值
   - 单个字段超过 `MAX_CELL_BYTES` 时截断并标记
   - 结果集大小双阈值检测：
     - 软阈值：截断返回，`truncated=True`
     - 硬阈值：抛出 `ResultTooLargeError`

6. **类型转换**：asyncpg 返回的 Python 类型 → 可 JSON 序列化的类型
   - `datetime` → ISO 格式字符串
   - `Decimal` → float
   - `UUID` → str
   - `bytes` → base64 编码（或标记为二进制）

**结果大小估算**：
```python
def _estimate_result_bytes(rows: list[list], columns: list[str]) -> int:
    """粗略估算结果集 JSON 序列化后的大小"""
    overhead = len(json.dumps(columns))
    for row in rows:
        overhead += len(json.dumps(row))
    return overhead
```

**验收标准**:
- [ ] 只读事务正确设置
- [ ] 超时正确触发 `SqlTimeoutError`
- [ ] LIMIT 正确注入
- [ ] 字段截断正确标记
- [ ] 结果大小双阈值正确工作
- [ ] 类型正确转换为 JSON 可序列化

**测试文件**: `tests/integration/test_sql_executor.py`

需要真实 PostgreSQL 连接。使用 `pytest-asyncio` 的 async fixtures。

### 2.6 结果验证器 (`engine/result_validator.py`)

**文件**: `pg_mcp/engine/result_validator.py`

实现 AI 结果验证（详见 Design §4.11）。

**核心实现要点**:

1. **触发条件判断** (`should_validate`)：
   - `enable_validation=False` → 不触发
   - 检查 SQL 复杂度：JOIN >= 2 个表、含子查询、含窗口函数
   - 结果为空集
   - logprob 低于阈值（如提供）
   - deny_list 命中 → 强制降级为 metadata_only（传入 `database` 和 `columns` 进行匹配）

   **方法签名**：`should_validate(self, database: str, sql: str, result: ExecutionResult, generation: SqlGenerationResult) -> bool`

2. **验证 prompt 构建**：
   - 包含：用户问题、SQL、结果元信息（列名、行数、类型）
   - 根据 policy 决定是否包含采样数据

3. **deny_list 处理**：
   - 格式：`"db.schema.table.column,db.*.*.*"`
   - 通配符 `*` 匹配任意层级
   - 命中时强制降级为 metadata_only
   - **策略说明**：deny_list 基于结果元数据（列名）匹配，而非 SQL 解析后的 lineage。对于精确的列级控制，在 `_is_denied()` 中检查 `database` + `columns`；对于粗粒度控制，支持 `db.*` 全库禁用。不追求 SQL lineage 级别的精确匹配（避免复杂解析），以"宁错杀"方式保护敏感数据。

4. **PII 掩码**（masked policy）：
   - 简单规则：email 正则替换、手机号替换、身份证替换
   - 列名包含 `password`, `token`, `secret`, `ssn` 等 → 整列掩码

5. **LLM 调用**：
   - `response_format={"type": "json_object"}`
   - 期望返回 `{"verdict": "pass|fix|fail", "reason": "...", "suggested_sql": "..."}`

**验收标准**:
- [ ] 触发条件正确判断
- [ ] deny_list 正确过滤
- [ ] 三种 policy 正确应用
- [ ] LLM 超时/错误正确转换

**测试文件**: `tests/unit/test_result_validator.py`

### 2.7 主编排器 (`engine/orchestrator.py`)

**文件**: `pg_mcp/engine/orchestrator.py`

实现 `QueryEngine` 主流程编排（详见 Design §4.12）。

**核心实现要点**:

```python
class QueryEngine:
    def __init__(...):
        # 依赖注入（详见 Design §4.12）
        ...
        self._semaphore = asyncio.Semaphore(settings.max_concurrent_requests)

    async def execute(self, request: QueryRequest) -> QueryResponse:
        request_id = str(uuid.uuid4())
        log = structlog.get_logger().bind(request_id=request_id)

        start_time = time.monotonic()

        # 1. 并发控制
        try:
            await asyncio.wait_for(self._semaphore.acquire(), timeout=0.01)
        except asyncio.TimeoutError:
            raise RateLimitedError("服务繁忙，请稍后重试")

        try:
            return await self._do_execute(request, request_id, log)
        finally:
            self._semaphore.release()

    async def _do_execute(self, request, request_id, log) -> QueryResponse:
        # 2. 输入校验（QueryRequest 已通过 Pydantic model_validator 校验）

        # 3. admin_action 处理
        if request.admin_action == "refresh_schema":
            result = await self._cache.refresh(request.database)
            return QueryResponse(
                request_id=request_id,
                database=request.database,
                sql=None,
                refresh_result=AdminRefreshResult(
                    succeeded=result.succeeded,
                    failed=result.failed,
                ),
            )

        # 4. 确定目标数据库
        if request.database:
            if request.database not in self._cache.discovered_databases():
                raise DbNotFoundError(f"数据库不存在: {request.database}")
            database = request.database
        else:
            database = await self._db_inference.infer(request.query)

        # 5. 加载 schema
        schema = await self._cache.get_schema(database)
        schema_loaded_at = schema.loaded_at.isoformat()

        # 6. 构建 schema 上下文
        if self._retriever.should_use_retrieval(schema):
            schema_context = self._retriever.retrieve(request.query, schema)
        else:
            schema_context = schema.to_prompt_text()

        # 7. SQL 生成（含重试）
        feedback: str | None = None
        for attempt in range(self._settings.max_retries + 1):
            try:
                gen_result = await self._sql_gen.generate(
                    request.query, schema_context, feedback
                )
            except (LlmTimeoutError, LlmError):
                raise  # 直接上抛

            sql = gen_result.sql

            # 8. SQL 校验
            val_result = self._sql_val.validate(sql, schema)
            if val_result.valid:
                is_explain = val_result.is_explain
                break

            if attempt < self._settings.max_retries:
                feedback = f"Previous SQL was rejected: {val_result.reason}"
                log.warning("sql_validation_failed", attempt=attempt, reason=val_result.reason)
            else:
                if val_result.code == "E_SQL_PARSE":
                    raise SqlParseError(val_result.reason or "SQL 解析失败")
                raise SqlUnsafeError(val_result.reason or "SQL 安全校验未通过")
        else:
            # 所有重试都用尽且未成功
            raise SqlGenerateError("无法生成合法的 SQL")

        # 9. return_type=sql 直接返回
        if request.return_type == "sql":
            return QueryResponse(
                request_id=request_id,
                database=database,
                sql=sql,
                schema_loaded_at=schema_loaded_at,
            )

        # 10. SQL 执行
        try:
            exec_result = await self._sql_exec.execute(database, sql, is_explain=is_explain)
        except SqlTimeoutError:
            raise
        except asyncpg.PostgresError as e:
            raise SqlExecuteError(str(e))

        # 11. 结果验证（可选）
        validation_used = False
        if self._result_val.should_validate(database, sql, exec_result, gen_result):
            validation_used = True
            verdict = await self._result_val.validate(
                request.query, sql, exec_result, schema
            )
            if verdict.verdict == "fix":
                # 纳入同一重试状态机：验证反馈 → 重新生成 → 校验 → 执行 → 重新验证
                # 使用独立的 validation_attempt 计数，与 SQL 生成重试共用 max_retries 上限
                val_feedback = f"Result validation feedback: {verdict.reason}"
                if verdict.suggested_sql:
                    val_feedback += f" Suggested SQL: {verdict.suggested_sql}"
                for val_attempt in range(self._settings.max_retries + 1):
                    try:
                        gen_result = await self._sql_gen.generate(
                            request.query, schema_context, val_feedback
                        )
                    except (LlmTimeoutError, LlmError):
                        raise
                    sql = gen_result.sql
                    val_result = self._sql_val.validate(sql, schema)
                    if not val_result.valid:
                        if val_attempt < self._settings.max_retries:
                            val_feedback = f"Fix attempt {val_attempt + 1} rejected: {val_result.reason}"
                            continue
                        raise SqlUnsafeError(val_result.reason or "修正 SQL 安全校验未通过")
                    # 更新 is_explain（修正后的 SQL 类型可能改变）
                    is_explain = val_result.is_explain
                    # 重新执行
                    try:
                        exec_result = await self._sql_exec.execute(database, sql, is_explain=is_explain)
                    except SqlTimeoutError:
                        raise
                    except asyncpg.PostgresError as e:
                        raise SqlExecuteError(str(e))
                    # 修正后强制重新验证结果（不依赖 should_validate 判断）
                    verdict = await self._result_val.validate(
                        request.query, sql, exec_result, schema
                    )
                    if verdict.verdict == "fix":
                        if val_attempt < self._settings.max_retries:
                            val_feedback = f"Fix attempt {val_attempt + 1} result feedback: {verdict.reason}"
                            continue
                        raise ValidationFailedError("结果验证反复修正后仍不满足")
                    elif verdict.verdict == "fail":
                        raise ValidationFailedError(verdict.reason or "结果验证失败")
                    break
                else:
                    raise ValidationFailedError("结果验证修正后仍无法生成合法 SQL")
            elif verdict.verdict == "fail":
                raise ValidationFailedError(verdict.reason or "结果验证失败")

        # 12. 组装响应
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
            schema_loaded_at=schema_loaded_at,
        )
```

**关键设计决策**:
- SQL 生成失败 → 校验失败 → 反馈重试的循环是内层的
- 结果验证失败后的修正也走同样的 SQL 生成路径
- 为防止无限循环，总重试次数由 `max_retries` 控制

**验收标准**:
- [ ] 完整流程走通
- [ ] 各环节错误正确转换
- [ ] 并发控制正确工作
- [ ] 重试逻辑正确

**测试文件**: `tests/unit/test_orchestrator.py`（Mock 所有依赖）

---

## 3. Phase 3: 基础设施层（Milestone 1+2, Day 6-8）

### 3.1 连接池管理 (`db/pool.py`)

**文件**: `pg_mcp/db/pool.py`

```python
import random

class ConnectionPoolManager:
    def __init__(self, settings: Settings):
        self._settings = settings
        self._pools: dict[str, asyncpg.Pool] = {}
        self._lock = asyncio.Lock()

    async def get_pool(self, database: str) -> asyncpg.Pool:
        if database in self._pools:
            return self._pools[database]

        async with self._lock:
            if database in self._pools:
                return self._pools[database]

            dsn = self._build_dsn(database)
            pool = await self._create_pool_with_retry(dsn)
            self._pools[database] = pool
            return pool

    async def _create_pool_with_retry(self, dsn: str) -> asyncpg.Pool:
        """指数退避重试创建连接池，最多 5 次"""
        max_retries = 5
        base_delay = 0.1
        max_delay = 3.0
        last_error = None
        for attempt in range(max_retries):
            try:
                return await asyncpg.create_pool(
                    dsn,
                    min_size=1,
                    max_size=self._settings.db_pool_size,
                    command_timeout=self._settings.query_timeout,
                )
            except (asyncpg.PostgresError, OSError, asyncio.TimeoutError) as e:
                last_error = e
                if attempt == max_retries - 1:
                    break
                delay = min(base_delay * (2 ** attempt), max_delay)
                jitter = random.uniform(0, delay * 0.1)
                await asyncio.sleep(delay + jitter)
        raise DbConnectError(f"连接池创建失败（{max_retries} 次重试）: {last_error}")

    def _build_dsn(self, database: str) -> str:
        sslmode = self._settings.pg_sslmode.value
        dsn = f"postgresql://{self._settings.pg_user}:{self._settings.pg_password.get_secret_value()}@{self._settings.pg_host}:{self._settings.pg_port}/{database}?sslmode={sslmode}"
        if self._settings.pg_sslrootcert:
            dsn += f"&sslrootcert={self._settings.pg_sslrootcert}"
        return dsn

    async def discover_databases(self) -> list[str]:
        """发现可访问的数据库列表"""
        # 若配置了 PG_DATABASES，直接使用，跳过发现
        if self._settings.pg_databases_list:
            return list(self._settings.pg_databases_list)

        pool = await self.get_pool("postgres")
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT datname FROM pg_database
                WHERE datallowconn = true
                  AND datname NOT IN (
                      SELECT unnest($1::text[])
                  )
                ORDER BY datname
            """, self._settings.pg_exclude_databases_list)
            return [r["datname"] for r in rows]

    async def assert_readonly(self) -> None:
        """检查用户权限（详见 Design §4.5）"""
        ...

    async def close_all(self) -> None:
        for pool in self._pools.values():
            await pool.close()
        self._pools.clear()
```

**验收标准**:
- [ ] 每个数据库独立连接池
- [ ] DSN 正确构建（含 SSL 配置）
- [ ] 数据库发现正确排除系统库
- [ ] 只读检查正确工作
- [ ] 连接失败时指数退避重试（最多 5 次，含抖动）
- [ ] `PG_DATABASES` 配置时跳过自动发现

**测试文件**: `tests/integration/test_pool.py`

### 3.2 Schema 发现 (`schema/discovery.py`)

**文件**: `pg_mcp/schema/discovery.py`

实现批量 SQL schema 发现（详见 Design §4.6）。

**需要实现的批量查询**:

1. **表和列**：`information_schema.columns` + `information_schema.tables` + `foreign_tables`
2. **主键**：`information_schema.table_constraints` + `key_column_usage`
3. **索引**：`pg_indexes` + `pg_index` + `pg_am`
4. **外键**：`information_schema.table_constraints` + `key_column_usage` + `constraint_column_usage`
5. **约束**：`information_schema.table_constraints` + `check_constraints`
6. **枚举类型**：`pg_type` + `pg_enum`
7. **复合类型**：`pg_type` + `pg_attribute` + `pg_attrdef`
8. **视图和物化视图**：`information_schema.views` + `pg_matviews`
9. **函数白名单**：`pg_proc`（详见 Design §4.6）

**组装逻辑**：
- 使用字典按 `(schema, table)` 分组构建 `TableInfo`
- 标记主键列
- 组装 `DatabaseSchema`

**验收标准**:
- [ ] 单次往返获取全部元数据（或少数几次）
- [ ] 200 表数据库加载 < 5s
- [ ] 所有元数据类型正确组装

**测试文件**: `tests/integration/test_schema_discovery.py`

### 3.3 Schema 缓存 (`schema/cache.py`)

**文件**: `pg_mcp/schema/cache.py`

实现 Redis + Singleflight 缓存（详见 Design §4.7）。

**核心实现要点**:

- 使用 `redis-py` 的 async API (`redis.asyncio`)
- `singleflight`：每个库同时只能有一个加载任务，所有加载（按需/预热/刷新）统一走 `_ensure_loading` → `_do_load` 路径
- 状态存储在 Redis：`pg_mcp:state:{db}`
- Schema 存储：`pg_mcp:schema:{db}`，gzip 压缩
- 后台预热：`asyncio.create_task` 启动预热（在统一事件循环中）
- **定时自动刷新**：启动时创建 `asyncio.Task` 运行 `_periodic_refresh()` 协程，间隔 `schema_refresh_interval`

**状态转换**：
```
UNLOADED → LOADING → READY
              ↓
            FAILED
```

**refresh 统一 singleflight**：
```python
async def refresh(self, database: str | None = None) -> RefreshResult:
    targets = [database] if database else list(self._databases)

    # 第一步：取消所有旧任务并等待取消完成
    for db in targets:
        await self._set_state(db, SchemaState.UNLOADED)
        async with self._inflight_lock:
            old_task = self._inflight.pop(db, None)
        if old_task is not None and not old_task.done():
            old_task.cancel()
            try:
                await old_task
            except asyncio.CancelledError:
                pass

    # 第二步：统一走 singleflight 路径启动新加载
    tasks = [self._ensure_loading(db) for db in targets]
    await asyncio.gather(*tasks, return_exceptions=True)

    # 第三步：收集结果（基于最终状态）
    succeeded, failed = [], []
    for db in targets:
        state = await self._get_state(db)
        if state == SchemaState.READY:
            succeeded.append(db)
        else:
            err = await self._redis.get(f"{self.PREFIX}:error:{db}")
            failed.append({"database": db, "error": err or "unknown"})

    return RefreshResult(succeeded=succeeded, failed=failed)
```

**`_do_load` 错误持久化**：
```python
async def _do_load(self, database: str):
    await self._set_state(database, SchemaState.LOADING)
    try:
        schema = await self._discovery.load_schema(database)
        compressed = gzip.compress(schema.model_dump_json().encode())
        await self._redis.set(...)
        await self._redis.delete(f"{self.PREFIX}:error:{database}")
        await self._set_state(database, SchemaState.READY)
    except Exception as e:
        await self._set_state(database, SchemaState.FAILED)
        await self._redis.set(f"{self.PREFIX}:error:{database}", str(e), ex=3600)
        log.error("schema_load_failed", database=database, error=str(e))
        raise  # 向上传播，让调用方知道失败
    finally:
        async with self._inflight_lock:
            self._inflight.pop(database, None)
```

**状态一致性**：当 Redis 中 `state=READY` 但 `schema:{db}` key 缺失或损坏时，降级为 `UNLOADED` 并触发重新加载。

**验收标准**:
- [ ] 首次获取触发加载，返回 `SchemaNotReadyError`
- [ ] 并发请求同一库只触发一次加载
- [ ] 加载完成后缓存命中
- [ ] TTL 过期后重新加载
- [ ] refresh 统一走 singleflight 路径
- [ ] `_do_load` 异常向上传播并持久化错误详情
- [ ] 定时刷新任务正常周期性运行

**测试文件**: `tests/integration/test_schema_cache.py`

### 3.4 Schema 状态机 (`schema/state.py`)

**文件**: `pg_mcp/schema/state.py`

```python
from enum import Enum

class SchemaState(str, Enum):
    UNLOADED = "unloaded"
    LOADING = "loading"
    READY = "ready"
    FAILED = "failed"
```

**验收标准**:
- [ ] 状态枚举可用

### 3.5 函数注册表 (`db/function_registry.py`)

**文件**: `pg_mcp/db/function_registry.py`

从 `pg_proc` 加载安全函数白名单。实际上与 `SchemaDiscovery._load_allowed_functions` 合并实现，无需单独文件。可在 `discovery.py` 中实现。

---

## 4. Phase 4: Server 层与 CLI（Milestone 1, Day 8-9）

### 4.1 可观测性 (`observability/`)

**文件**: `pg_mcp/observability/logging.py`

```python
import structlog

def configure_logging(log_level: str) -> None:
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
```

**文件**: `pg_mcp/observability/sanitizer.py`

```python
import re

STRING_LITERAL_RE = re.compile(r"'[^']*'")

def sanitize_sql(sql: str) -> str:
    """将 SQL 中的字符串字面量替换为 '***'"""
    return STRING_LITERAL_RE.sub("'***'", sql)

# PII 检测模式（简单规则）
EMAIL_RE = re.compile(r'[\w.-]+@[\w.-]+\.\w+')
PHONE_RE = re.compile(r'\b1[3-9]\d{9}\b')

def mask_pii(value: str) -> str:
    value = EMAIL_RE.sub("***@***.***", value)
    value = PHONE_RE.sub("***PHONE***", value)
    return value
```

**文件**: `pg_mcp/observability/metrics.py`

```python
from contextlib import asynccontextmanager
from collections.abc import AsyncGenerator
import time

@asynccontextmanager
async def timed(log, event: str) -> AsyncGenerator[dict, None]:
    start = time.monotonic()
    extra = {}
    try:
        yield extra
    finally:
        elapsed_ms = int((time.monotonic() - start) * 1000)
        log.info(event, elapsed_ms=elapsed_ms, **extra)
```

**验收标准**:
- [ ] JSON 结构化日志输出
- [ ] SQL 脱敏正确
- [ ] 计时器正确记录耗时

### 4.2 MCP Server (`server.py`)

**文件**: `pg_mcp/server.py`

```python
from mcp.server import Server
from mcp.types import TextContent
from mcp.server.models import INVALID_PARAMS

class PgMcpServer:
    def __init__(self, query_engine: QueryEngine):
        self._engine = query_engine
        self._server = Server("pg-mcp")
        self._setup_tools()

    def _setup_tools(self) -> None:
        @self._server.list_tools()
        async def list_tools() -> list[Tool]:
            return [Tool(
                name="query",
                description="Execute natural language queries against PostgreSQL databases",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "Natural language query (required unless admin_action is set)"},
                        "database": {"type": "string", "description": "Target database name"},
                        "return_type": {"type": "string", "enum": ["sql", "result"]},
                        "admin_action": {"type": "string", "enum": ["refresh_schema"]},
                    },
                    "required": [],  # query 在 admin_action 模式下可选，由业务层校验
                },
            )]

        @self._server.call_tool()
        async def call_tool(name: str, arguments: dict) -> list[TextContent]:
            if name != "query":
                raise McpError(INVALID_PARAMS, f"Unknown tool: {name}")

            try:
                request = QueryRequest(**arguments)
            except ValidationError as e:
                raise McpError(INVALID_PARAMS, str(e))

            try:
                response = await self._engine.execute(request)
            except PgMcpError as e:
                response = QueryResponse(
                    error=ErrorDetail(
                        code=e.code.value,
                        message=str(e),
                        retry_after_ms=e.retry_after_ms,
                        candidates=e.candidates,
                    )
                )

            return [TextContent(type="text", text=response.model_dump_json())]

    async def run_stdio(self) -> None:
        from mcp.server.stdio import stdio_server
        async with stdio_server() as (read_stream, write_stream):
            await self._server.run(
                read_stream, write_stream, self._server.create_initialization_options()
            )

    async def run_sse(self, host: str, port: int) -> None:
        # 由 FastAPI app 处理 SSE 传输
        pass
```

**验收标准**:
- [ ] Tool 定义正确
- [ ] 协议级错误通过 MCP error 返回
- [ ] 业务级错误通过统一响应返回
- [ ] stdio 模式可运行

### 4.3 FastAPI App (`app.py`)

**文件**: `pg_mcp/app.py`

```python
from fastapi import FastAPI, Request
from fastapi.responses import Response
from mcp.server.sse import SseServerTransport
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan：启动时资源已就绪，关闭时统一清理"""
    # 资源在 CLI _run_server() 中创建后传入，此处无需额外初始化
    yield
    # 关闭时由 CLI finally 统一处理，此处不重复关闭

def create_app(server: PgMcpServer, cache: SchemaCache) -> FastAPI:
    app = FastAPI(title="pg-mcp", lifespan=lifespan)
    sse_transport = SseServerTransport("/messages")

    @app.get("/health")
    async def health() -> dict:
        return {"status": "ok"}

    @app.post("/admin/refresh")
    async def refresh_schema(database: str | None = None) -> dict:
        result = await cache.refresh(database)
        return {
            "succeeded": result.succeeded,
            "failed": result.failed,
        }

    @app.get("/sse")
    async def sse_endpoint(request: Request) -> Response:
        # Starlette Request 封装 ASGI scope/receive/send，
        # _send 是底层 ASGI send callable，MCP SDK SSE 传输需要此接口
        async with sse_transport.connect_sse(
            request.scope, request.receive, request._send
        ) as (read_stream, write_stream):
            await server._server.run(
                read_stream, write_stream, server._server.create_initialization_options()
            )
        return Response(status_code=200)

    @app.post("/messages")
    async def messages_endpoint(request: Request) -> Response:
        return await sse_transport.handle_post_message(
            request.scope, request.receive, request._send
        )

    return app
```

**验收标准**:
- [ ] `/health` 返回正确
- [ ] `/admin/refresh` 触发 schema 刷新并返回结果
- [ ] SSE 端点可连接
- [ ] lifespan 与 CLI 资源生命周期一致

### 4.4 CLI (`cli.py`)

**文件**: `pg_mcp/cli.py`

```python
import click
import asyncio
from contextlib import asynccontextmanager

@click.command()
@click.option("--transport", type=click.Choice(["stdio", "sse"]), default="stdio")
def main(transport: str) -> None:
    settings = Settings()
    configure_logging(settings.log_level)
    asyncio.run(_run_server(transport, settings))

async def _run_server(transport: str, settings: Settings) -> None:
    """统一 async 生命周期：所有长生命周期资源在此协程内创建和销毁"""
    pool_mgr = ConnectionPoolManager(settings)
    redis_client = redis.asyncio.from_url(settings.redis_url)
    cache = SchemaCache(redis_client, pool_mgr, settings)

    # 跟踪后台任务，确保关闭时正确清理
    bg_tasks: set[asyncio.Task] = set()

    try:
        # 发现数据库（若 PG_DATABASES 配置则直接使用）
        databases = await pool_mgr.discover_databases()
        cache.set_discovered_databases(databases)

        # 只读检查（不阻塞，失败时记录警告或退出）
        await pool_mgr.assert_readonly()

        # 初始化引擎
        openai_client = AsyncOpenAI(
            api_key=settings.openai_api_key.get_secret_value(),
            base_url=settings.openai_base_url,
        )
        engine = QueryEngine(...)
        server = PgMcpServer(engine)

        # 后台预热
        t = asyncio.create_task(cache.warmup_all())
        bg_tasks.add(t)
        t.add_done_callback(bg_tasks.discard)

        # 定时刷新任务
        if settings.schema_refresh_interval > 0:
            t = asyncio.create_task(cache.run_periodic_refresh())
            bg_tasks.add(t)
            t.add_done_callback(bg_tasks.discard)

        if transport == "stdio":
            await server.run_stdio()
        else:
            # SSE 模式：传递共享资源给 FastAPI
            await _run_sse(server, cache, settings)
    finally:
        # 取消并等待所有后台任务完成
        for task in bg_tasks:
            if not task.done():
                task.cancel()
        if bg_tasks:
            await asyncio.gather(*bg_tasks, return_exceptions=True)
        # 关闭资源
        await pool_mgr.close_all()
        await redis_client.aclose()

async def _run_sse(server: PgMcpServer, cache: SchemaCache, settings: Settings) -> None:
    import uvicorn
    from pg_mcp.app import create_app
    app = create_app(server, cache)
    config = uvicorn.Config(app, host=settings.sse_host, port=settings.sse_port)
    uvicorn_server = uvicorn.Server(config)
    await uvicorn_server.serve()
```

**启动流程**（统一事件循环内）：
```
1. 加载配置
2. 初始化连接池管理器、Redis、SchemaCache
3. 发现数据库清单（PG_DATABASES 覆盖时直接使用）
4. 只读权限检查
5. 初始化 QueryEngine + PgMcpServer
6. 后台预热（asyncio.create_task + 跟踪）
7. 定时刷新任务（asyncio.create_task + 跟踪）
8. 启动 MCP Server（stdio 或 SSE）
9. finally: 取消并等待后台任务，关闭 pool 和 Redis
```

**关键原则**：
- 所有 async 资源在同个事件循环内创建和销毁
- 后台任务通过 `set[asyncio.Task]` 跟踪，`finally` 中先 cancel 再 await
- SSE 模式共享 CLI 创建的资源（通过 `create_app(server, cache)` 传入）
- FastAPI lifespan 不重复创建/销毁资源

**验收标准**:
- [ ] `pg-mcp --transport stdio` 可启动
- [ ] `pg-mcp --transport sse` 可启动
- [ ] 所有 async 资源在同个事件循环内生命周期管理
- [ ] 关闭时先 cancel/await 后台任务，再关闭 pool 和 Redis

---

## 5. Phase 5: 测试（Milestone 1+2, Day 10-13）

### 5.1 单元测试 (`tests/unit/`)

| 测试文件 | 覆盖模块 | 关键用例 |
|---------|---------|---------|
| `test_sql_validator.py` | `sql_validator.py` | 白名单、黑名单、多语句、EXPLAIN、EXPLAIN ANALYZE 拒绝、foreign table、函数名规范化 |
| `test_db_inference.py` | `db_inference.py` | 单库命中、歧义、无匹配、跨库、未就绪、倒排索引性能 |
| `test_schema_retriever.py` | `retriever.py` | 关键词匹配、大 schema 检索、外键关联、预计算索引 |
| `test_config.py` | `config.py` | 环境变量覆盖、默认值、SecretStr、PG_DATABASES/PG_EXCLUDE_DATABASES/VALIDATION_DENY_LIST 解析 |
| `test_sanitizer.py` | `sanitizer.py` | SQL 脱敏、PII 掩码 |
| `test_orchestrator.py` | `orchestrator.py` | 完整流程、错误转换、重试逻辑、结果验证 fix 路径 |
| `test_result_validator.py` | `result_validator.py` | 触发条件、deny_list、policy 降级、verdict 解析 |
| `test_rate_limit.py` | `orchestrator.py` | 信号量并发控制、超时拒绝 |

**Mock 策略**：
- `MockSqlGenerator`：返回预置 SQL
- `MockSqlValidator`：可配置通过/拒绝
- `MockSqlExecutor`：返回预置结果
- `MockSchemaCache`：返回预置 schema
- `MockDbInference`：返回预置数据库名
- `MockResultValidator`：可配置 verdict

### 5.2 集成测试 (`tests/integration/`)

需要真实 PostgreSQL + Redis 环境。使用 Docker Compose 或要求本地运行。

| 测试文件 | 覆盖模块 | 依赖 |
|---------|---------|------|
| `test_pool.py` | `db/pool.py` | PostgreSQL |
| `test_schema_discovery.py` | `schema/discovery.py` | PostgreSQL |
| `test_sql_executor.py` | `engine/sql_executor.py` | PostgreSQL |
| `test_schema_cache.py` | `schema/cache.py` | PostgreSQL + Redis |
| `test_query_engine.py` | `engine/orchestrator.py` | PostgreSQL + Redis + OpenAI (可 mock) |
| `test_singleflight.py` | `schema/cache.py` | PostgreSQL + Redis |
| `test_admin_refresh.py` | `engine/orchestrator.py` + `schema/cache.py` | PostgreSQL + Redis |

**Fixtures** (`tests/conftest.py`)：
```python
@pytest.fixture(scope="session")
async def postgres_pool():
    """共享的 PostgreSQL 连接池"""
    pool = await asyncpg.create_pool("postgresql://test:test@localhost:5432/test")
    yield pool
    await pool.close()

@pytest.fixture
async def redis_client():
    client = redis.asyncio.from_url("redis://localhost:6379/1")
    yield client
    await client.flushdb()
```

### 5.3 端到端测试 (`tests/e2e/`)

| 测试文件 | 覆盖 | 依赖 |
|---------|------|------|
| `test_mcp_tool.py` | MCP stdio 协议完整流程 | PostgreSQL + Redis + OpenAI (mock) |
| `test_mcp_sse.py` | MCP SSE 传输完整流程 | PostgreSQL + Redis + OpenAI (mock) |
| `test_explain.py` | EXPLAIN 语句端到端 | PostgreSQL + OpenAI (mock) |
| `test_admin_action.py` | admin_action=refresh_schema 端到端 | PostgreSQL + Redis |

使用 `mcp.Client` 或直接调用 `server.call_tool` 测试完整链路。

### 5.4 CI 矩阵建议

| 维度 | 值 |
|------|-----|
| PostgreSQL 版本 | 14, 15, 16, 17 |
| Python 版本 | 3.12, 3.13 |
| 传输方式 | stdio, SSE |

### 5.5 缺失测试补充（高优先级）

- [ ] 信号量并发限流测试（模拟 20+ 并发请求）
- [ ] Schema cache singleflight 竞态测试（并发获取同一未就绪库）
- [ ] 定时刷新任务调度测试（mock asyncio.sleep）
- [ ] SSE transport 生命周期测试（连接/断开/重连）
- [ ] CLI 启动/关闭生命周期测试（FastAPI lifespan / stdio_server）
- [ ] PG_DATABASES 覆盖发现测试（跳过自动发现）
- [ ] 连接重试退避测试（模拟连接失败）
- [ ] 结果验证 fix 路径重试测试（mock verdict=fix）
- [ ] 大 LIMIT 绕过防护测试（`LIMIT 100000000` 被外层 LIMIT 截断）
- [ ] Foreign table 引号处理测试（`"Schema"."Table"`）

---

## 6. Phase 6: 质量门禁与收尾（Milestone 1 验收, Day 14-16）

### 6.1 质量检查清单

- [ ] `uv run ruff check .` 无错误
- [ ] `uv run ruff format .` 格式化后无变更
- [ ] `uv run mypy pg_mcp/` 无类型错误
- [ ] `uv run pytest tests/unit/` 全部通过（< 10s）
- [ ] `uv run pytest tests/integration/` 全部通过（需 PG + Redis）
- [ ] `uv run pytest --cov=pg_mcp --cov-report=term-missing`
  - 整体覆盖率 >= 85%
  - `engine/`、`schema/` 覆盖率 >= 90%
  - `sql_validator.py` 覆盖率 100%

### 6.2 文档更新

- [ ] 更新 `README.md`：安装、配置、运行说明
- [ ] 更新 `CLAUDE.md`：如有新增约定
- [ ] 补充 `specs/0003-pg-mcp-design-review.md`（如有 review 结果）

### 6.3 Docker 构建

```dockerfile
FROM python:3.12-slim
WORKDIR /app
# 先复制源码和配置文件，再安装（确保包内容完整）
COPY pyproject.toml .
COPY pg_mcp/ pg_mcp/
RUN pip install --no-cache-dir .
ENTRYPOINT ["pg-mcp"]
CMD ["--transport", "sse"]
```

---

## 7. 风险与应对

| 风险 | 影响 | 应对策略 |
|------|------|---------|
| SQLGlot 无法完全替代 pglast | SQL 校验准确率下降 | 补充边界 case 测试，必要时增加正则兜底 |
| asyncpg 连接池在大量数据库时资源耗尽 | 性能/稳定性 | 限制并发加载数，使用连接池参数调优 |
| LLM 生成 SQL 质量不稳定 | 用户体验 | 增加重试 + 结果验证，prompt 优化 |
| Schema 过大导致 Redis 内存压力 | 性能/成本 | gzip 压缩 + LRU 淘汰 + 检索策略 |
| 中文查询分词不准确 | 推断/检索质量 | 先用简单规则，后续可引入 jieba |
| MCP SDK API 变动 | 兼容性 | 锁定 major 版本范围（`>=1.0,<2.0`），升级前 review changelog |
| 多次 `asyncio.run()` 导致资源错乱 | 启动失败/不可恢复 | 统一单事件循环生命周期 |
| LLM 注入超大 LIMIT 绕过 | 内存/性能风险 | 外层强制 LIMIT 包裹 |
| Schema cache singleflight 竞态 | 重复加载/状态不一致 | refresh 统一走 singleflight，异常向上传播 |
| 连接失败无重试 | 启动不可用 | 指数退避 + 抖动重试（最多 5 次） |
| 标识符未规范化导致 foreign table 漏检 | 安全绕过 | AST 层规范化标识符，专用 `_quote_ident` 函数 |

---

## 8. 附录：文件清单

### 源代码

```
pg_mcp/
├── __init__.py
├── __main__.py
├── cli.py
├── config.py
├── server.py
├── app.py
├── protocols.py
├── engine/
│   ├── __init__.py
│   ├── orchestrator.py
│   ├── db_inference.py
│   ├── sql_generator.py
│   ├── sql_validator.py
│   ├── sql_executor.py
│   └── result_validator.py
├── schema/
│   ├── __init__.py
│   ├── discovery.py
│   ├── cache.py
│   ├── retriever.py
│   └── state.py
├── db/
│   ├── __init__.py
│   └── pool.py
├── models/
│   ├── __init__.py
│   ├── schema.py
│   ├── request.py
│   ├── response.py
│   └── errors.py
└── observability/
    ├── __init__.py
    ├── logging.py
    ├── metrics.py
    └── sanitizer.py
```

### 测试代码

```
tests/
├── conftest.py
├── fixtures/
│   └── sql_samples.py
├── unit/
│   ├── test_sql_validator.py
│   ├── test_db_inference.py
│   ├── test_schema_retriever.py
│   ├── test_config.py
│   ├── test_sanitizer.py
│   └── test_orchestrator.py
├── integration/
│   ├── test_pool.py
│   ├── test_schema_discovery.py
│   ├── test_sql_executor.py
│   ├── test_schema_cache.py
│   └── test_query_engine.py
└── e2e/
    └── test_mcp_tool.py
```

### 配置

```
pyproject.toml
Dockerfile
.env.example
```
