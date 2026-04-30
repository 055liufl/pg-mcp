# PRD-0001: PostgreSQL Natural Language Query MCP Server

## 1. 概述

构建一个基于 Python 的 MCP (Model Context Protocol) Server，允许用户通过自然语言描述查询需求，系统自动将其转化为 SQL 语句并执行。服务器连接 PostgreSQL 数据库，启动时自动发现可访问的数据库列表并按需缓存 schema 信息，利用 OpenAI GPT-5-mini 模型进行自然语言到 SQL 的转换，并对生成的 SQL 进行安全校验和结果验证。

## 2. 目标用户

- 需要查询 PostgreSQL 数据库但不熟悉 SQL 的业务人员
- 希望通过 AI 辅助工具提高查询效率的开发者
- 任何通过 MCP 协议接入此服务的 AI 客户端（如 Claude Desktop、Cursor 等）

## 3. 核心功能需求

### 3.1 数据库 Schema 自动发现与缓存

**启动阶段：**

- 服务器启动时，连接配置中指定的 PostgreSQL 实例
- 自动发现该实例上所有用户有权访问的数据库，排除默认排除列表中的数据库（默认排除：`template0`、`template1`、`postgres`；可通过 `PG_EXCLUDE_DATABASES` 配置覆盖）
- 部分数据库发现失败时不阻塞服务启动，记录告警日志，仅加载成功的数据库
- 启动阶段仅发现数据库清单，**不立即加载 schema**

**Schema 加载策略：**

- 采用按需懒加载 + 后台预热策略
- Schema 加载状态机：`unloaded` → `loading` → `ready` | `failed`
- 首次查询某数据库时触发 schema 加载，加载完成前返回 `E_SCHEMA_NOT_READY`（附带 `retry_after_ms` 建议值，如 2000ms）
- 启动后在后台异步预热所有已发现的数据库 schema
- 对每个可访问的数据库，读取并缓存以下 schema 信息：
  - **Schemas**：所有非系统 schema（排除 `pg_catalog`、`information_schema` 等）
  - **Tables**：表名、列名、列类型、列注释（COMMENT）、是否可空、默认值、主键信息
  - **Views**：视图名、列名、列类型、视图定义
  - **Materialized Views**：物化视图名、列信息、索引信息
  - **Indexes**：索引名、所属表、索引列、索引类型（B-tree、GIN、GiST 等）、是否唯一
  - **Custom Types**：枚举类型及其值、复合类型及其字段
  - **Foreign Keys**：外键关系（源表/列 → 目标表/列）
  - **Constraints**：CHECK 约束、UNIQUE 约束等
- 缓存存储在内存中，每条缓存记录包含 `schema_loaded_at` 时间戳
- 支持 TTL 缓存过期（可配置，默认 600 秒 / 10 分钟）与 LRU 淘汰（应对大量数据库场景）

**缓存刷新：**

- 支持定时自动刷新（可配置刷新间隔，默认 600 秒 / 10 分钟）
- 在 `query` tool 中支持通过显式参数 `admin_action=refresh_schema` 触发手动刷新，不依赖自然语言识别

**大 Schema 处理：**

- 当数据库 schema 过大（表数量超过 `SCHEMA_MAX_TABLES_FOR_FULL_CONTEXT` 阈值）时，不将完整 schema 注入 prompt
- 采用 schema 摘要 + 检索策略：先根据用户查询检索相关表/列，再拼接最小上下文发送给 LLM
- schema 检索可基于表名/列名/注释与用户查询的关键词匹配

### 3.2 自然语言到 SQL 的转换

**输入参数：**

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `query` | string | 是 | 用户的自然语言查询描述（最大 2000 字符） |
| `database` | string | 否 | 目标数据库名称。如未指定，由系统根据 query 内容自动推断 |
| `return_type` | enum | 否 | `sql` 或 `result`，默认为 `result` |
| `admin_action` | string | 否 | 管理操作，当前支持 `refresh_schema` |

**输入校验：**

- `query` 不允许为空或纯空白字符，超过最大长度返回 `E_INVALID_ARGUMENT`
- `return_type` 仅接受 `sql` 或 `result`，其他值返回 `E_INVALID_ARGUMENT`
- `database` 如指定，必须存在于已发现的数据库列表中，否则返回 `E_DB_NOT_FOUND`

**转换流程：**

1. 接收用户的自然语言输入，执行输入校验
2. 如 `admin_action=refresh_schema`，执行 schema 刷新并返回刷新结果
3. 确定目标数据库（用户指定或系统推断）
4. 从缓存中提取目标数据库的 schema 信息（大 schema 时走检索策略）
5. 构建 prompt，包含 schema 上下文和用户查询，调用 OpenAI GPT-5-mini 生成 SQL
6. 对生成的 SQL 进行安全校验（见 3.3）
7. 分支处理：
   - `return_type=sql`：仅返回生成的 SQL 语句，**不执行**
   - `return_type=result`：执行 SQL 并验证结果（见 3.4），返回查询结果

**数据库自动推断：**

- 当用户未指定目标数据库时，系统需根据查询内容和各数据库的 schema 信息推断最匹配的数据库
- 推断逻辑：将用户查询与各数据库的表名/列名/注释进行关键词匹配，计算相关性得分（命中的表/列数量加权求和）
- 置信度判定规则：
  - 当仅有一个数据库的 schema 命中用户查询中的关键实体时，直接选定
  - 当存在多个候选数据库且 top1 与 top2 得分差距 < 15% 时，返回错误码 `E_DB_INFER_AMBIGUOUS` 并列出候选数据库，要求用户明确指定
  - 当无任何数据库命中时，返回错误码 `E_DB_INFER_NO_MATCH`
  - 当用户查询语义涉及多个数据库的表（跨库查询意图），返回 `E_CROSS_DB_UNSUPPORTED`

### 3.3 SQL 安全校验

生成的 SQL 必须经过严格的安全校验，确保只允许只读查询操作：

**允许的语句类型（白名单）：**

- `SELECT`
- `WITH ... SELECT`（CTE 查询）
- `EXPLAIN`（仅查询计划，不执行）— EXPLAIN 结果不走业务语义验证，直接返回计划文本

**禁止的语句类型（包括但不限于）：**

- `INSERT`、`UPDATE`、`DELETE`、`TRUNCATE`
- `CREATE`、`ALTER`、`DROP`
- `GRANT`、`REVOKE`
- `COPY`
- `CALL`（存储过程调用）
- 任何 DDL/DML 操作

**函数安全策略：**

- 默认拒绝所有函数调用，仅允许白名单中的安全函数类别
- 允许的函数：PostgreSQL 内置的 `IMMUTABLE` 和 `STABLE` 函数（聚合函数、数学函数、字符串函数、日期函数、类型转换函数等）
- 显式禁止的高风险函数（即使标记为 STABLE/IMMUTABLE 也拒绝）：
  - 文件系统访问：`pg_read_file`、`pg_read_binary_file`、`pg_ls_dir`
  - 大对象操作：`lo_import`、`lo_export`
  - 锁与通知：`pg_sleep`、`pg_advisory_lock*`、`pg_notify`
  - 外部数据：`dblink*` 系列函数
- 禁止使用 `dblink`、`postgres_fdw` 等外部数据访问扩展
- 禁止访问 foreign table
- 禁止 `COPY ... TO PROGRAM` 及其变体

**校验方式：**

- **必须**使用 `pglast`（基于 PostgreSQL 官方解析器）对 SQL 进行 AST 级别的解析
- 语句级白名单：顶层 AST 节点必须为 `SelectStmt`（含 CTE）或 `ExplainStmt`
- 遍历 AST 检查函数调用节点（`FuncCall`），对照函数安全策略判定
- 遍历 AST 检查对象引用，拒绝访问 foreign table
- 禁止多语句执行（AST 解析结果必须为单条语句）
- 校验失败时，返回错误码 `E_SQL_UNSAFE` 和明确的拒绝原因

**会话级资源限制（在数据库连接上设置）：**

- `search_path` 设置为目标 schema，防止通过 schema 切换绕过限制
- `work_mem` 限制（可配置，默认 64MB）
- `temp_file_limit` 限制（可配置，默认 256MB）

### 3.4 SQL 执行与结果验证

**SQL 执行：**

- 使用只读事务执行 SQL（`SET TRANSACTION READ ONLY`）
- 设置语句超时时间（`statement_timeout`），防止长时间运行的查询（可配置，默认 30 秒）
- 设置事务空闲超时（`idle_in_transaction_session_timeout`），防止连接泄漏
- 限制会话并行度（`max_parallel_workers_per_gather`），防止单查询占用过多资源
- 限制返回行数上限（可配置，默认 1000 行）
- 限制单个字段最大字节数（`MAX_CELL_BYTES`，默认 4096），超限截断并标记
- 结果集大小处理策略（双阈值）：
  - **软阈值**（`MAX_RESULT_BYTES`，默认 10MB）：截断返回，`truncated=true`，正常响应
  - **硬阈值**（`MAX_RESULT_BYTES_HARD`，默认 50MB）：直接返回 `E_RESULT_TOO_LARGE`，不返回数据
  - `truncated=true` 和 `error` 不会同时出现
- 执行失败时，收集错误信息用于后续修正

**结果验证（AI 辅助，默认关闭）：**

验证功能默认关闭（`ENABLE_VALIDATION=false`），需显式开启。开启后仅在以下场景触发：

- 生成的 SQL 包含复杂逻辑（JOIN >= 2 个表、包含子查询、包含窗口函数）
- 查询结果为空集
- LLM 生成 SQL 时返回的 logprob 低于置信度阈值（`VALIDATION_CONFIDENCE_THRESHOLD`，默认 -1.0）

**数据外发安全策略：**

触发验证时，发送给 LLM 的数据受 `VALIDATION_DATA_POLICY` 控制：

| 策略 | 说明 |
|------|------|
| `metadata_only`（默认） | 仅发送 SQL、列名、行数、数据类型，不发送实际数据 |
| `masked` | 发送脱敏后的采样数据（PII 列自动掩码） |
| `full` | 发送原始采样数据（仅在确认无敏感数据时使用） |

- 支持按库/表/列配置"禁止发送到 LLM"的规则（`VALIDATION_DENY_LIST`）
- 响应中包含 `validation_used: true/false` 字段，用于审计

触发验证时，将以下信息发送给 OpenAI GPT-5-mini：

- 用户的原始自然语言输入
- 生成的 SQL 语句
- 查询结果元信息（列名、行数、数据类型）
- 根据 `VALIDATION_DATA_POLICY` 决定是否包含采样数据行

AI 验证的判断维度：

- SQL 是否正确地理解了用户意图
- 返回的结果是否看起来合理和有意义
- 是否存在明显的逻辑错误（如应该有 WHERE 条件但没有）

验证结果为以下之一：

- **通过**：结果合理，直接返回给用户
- **修正**：SQL 有偏差，AI 提供修正建议，重新生成 SQL（最多重试 N 次，可配置，默认 2 次）
- **失败**：无法生成满足需求的 SQL，返回错误码 `E_VALIDATION_FAILED` 及原因

### 3.5 MCP Server 接口定义

服务器暴露单一 MCP Tool：

#### Tool: `query`

核心查询工具，接受自然语言并返回 SQL 或查询结果。

**输入：**

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `query` | string | 是 | 自然语言查询描述（最大 2000 字符） |
| `database` | string | 否 | 目标数据库名称 |
| `return_type` | string | 否 | `sql` 或 `result`，默认 `result` |
| `admin_action` | string | 否 | 管理操作：`refresh_schema` |

**输出（统一响应结构）：**

```json
{
  "request_id": "uuid",
  "database": "选定的数据库名",
  "sql": "生成的 SQL 语句",
  "columns": ["列名1", "列名2"],
  "column_types": ["text", "integer"],
  "rows": [["值1", "值2"]],
  "row_count": 42,
  "truncated": false,
  "truncated_reason": null,
  "validation_used": false,
  "schema_loaded_at": "2026-04-28T10:00:00Z",
  "warnings": [],
  "error": null
}
```

- `return_type=sql` 时：`rows`、`columns`、`column_types`、`row_count` 为 `null`
- `return_type=result` 时：包含查询结果
- `truncated=true` 时：`truncated_reason` 说明截断原因，`error` 为 `null`（正常响应）
- 出错时：`error` 字段包含错误对象 `{"code": "E_xxx", "message": "..."}`，`truncated` 为 `false`
- `admin_action=refresh_schema` 时：返回刷新状态信息

**错误返回分层：**

| 错误类型 | 返回方式 | 示例 |
|----------|----------|------|
| 协议级错误（参数缺失、tool 调用格式错误） | MCP 协议标准 error | 缺少必填参数 `query` |
| 业务级错误（可恢复，客户端可据此调整请求） | 统一响应 `error` 字段 | `E_DB_INFER_AMBIGUOUS`、`E_SQL_UNSAFE` |

### 3.6 错误码定义

| 错误码 | 说明 | 客户端建议行为 |
|--------|------|---------------|
| `E_INVALID_ARGUMENT` | 输入参数不合法（空 query、非法 return_type 等） | 修正参数后重试 |
| `E_DB_CONNECT` | 数据库连接失败 | 稍后重试 |
| `E_DB_NOT_FOUND` | 指定的数据库不存在 | 检查数据库名称 |
| `E_DB_INFER_AMBIGUOUS` | 数据库推断存在多个候选，需用户明确指定 | 指定 `database` 参数 |
| `E_DB_INFER_NO_MATCH` | 无法根据查询内容匹配到任何数据库 | 指定 `database` 参数 |
| `E_CROSS_DB_UNSUPPORTED` | 查询涉及多个数据库，不支持跨库查询 | 拆分为单库查询 |
| `E_SCHEMA_NOT_READY` | 目标数据库 schema 尚未加载完成（附 `retry_after_ms`） | 等待后重试 |
| `E_SQL_GENERATE` | LLM 生成 SQL 失败 | 重新描述查询需求 |
| `E_SQL_UNSAFE` | SQL 安全校验未通过 | 系统自动重试，用户无需操作 |
| `E_SQL_PARSE` | SQL 解析失败（语法错误） | 系统自动重试，用户无需操作 |
| `E_SQL_EXECUTE` | SQL 执行失败 | 检查查询描述是否正确 |
| `E_SQL_TIMEOUT` | SQL 执行超时 | 简化查询范围 |
| `E_VALIDATION_FAILED` | AI 结果验证失败，无法生成满足需求的 SQL | 重新描述查询需求 |
| `E_LLM_TIMEOUT` | LLM API 调用超时 | 稍后重试 |
| `E_LLM_ERROR` | LLM API 调用异常 | 稍后重试 |
| `E_RESULT_TOO_LARGE` | 结果集超过硬阈值限制 | 缩小查询范围或添加过滤条件 |
| `E_RATE_LIMITED` | 请求被限流 | 等待后重试 |

## 4. 非功能性需求

### 4.1 配置管理

服务器应支持以下配置项（通过环境变量或配置文件）。优先级：环境变量 > 配置文件 > 默认值。所有配置仅在启动时读取，运行时不可变更。

| 配置项 | 类型 | 说明 | 默认值 |
|--------|------|------|--------|
| `PG_HOST` | string | PostgreSQL 主机地址 | `localhost` |
| `PG_PORT` | int | PostgreSQL 端口 | `5432` |
| `PG_USER` | string | PostgreSQL 用户名 | 必填 |
| `PG_PASSWORD` | string | PostgreSQL 密码 | 必填 |
| `PG_DATABASES` | string | 指定要连接的数据库列表（逗号分隔），为空则自动发现 | 空（自动发现） |
| `PG_EXCLUDE_DATABASES` | string | 自动发现时排除的数据库（逗号分隔） | `template0,template1,postgres` |
| `PG_SSLMODE` | enum | SSL 模式：`disable`/`allow`/`prefer`/`require`/`verify-ca`/`verify-full` | `prefer` |
| `PG_SSLROOTCERT` | string | SSL 根证书路径 | 空 |
| `DB_POOL_SIZE` | int | 每个数据库的连接池大小 | `5` |
| `MAX_CONCURRENT_REQUESTS` | int | 最大并发请求数 | `20` |
| `STRICT_READONLY` | bool | 如检测到非只读用户是否拒绝启动 | `false` |
| `OPENAI_API_KEY` | string | OpenAI API Key | 必填 |
| `OPENAI_MODEL` | string | 使用的 OpenAI 模型 | `gpt-5-mini` |
| `OPENAI_BASE_URL` | string | OpenAI API 基地址（支持兼容接口） | 官方默认 |
| `QUERY_TIMEOUT` | int | SQL 查询超时时间（秒） | `30` |
| `MAX_ROWS` | int | 查询结果最大返回行数 | `1000` |
| `MAX_CELL_BYTES` | int | 单个字段最大字节数 | `4096` |
| `MAX_RESULT_BYTES` | int | 结果集软阈值（截断） | `10485760`（10MB） |
| `MAX_RESULT_BYTES_HARD` | int | 结果集硬阈值（报错） | `52428800`（50MB） |
| `SESSION_WORK_MEM` | string | 会话 work_mem 限制 | `64MB` |
| `SESSION_TEMP_FILE_LIMIT` | string | 会话 temp_file_limit | `256MB` |
| `ENABLE_VALIDATION` | bool | 是否启用 AI 结果验证 | `false` |
| `VALIDATION_SAMPLE_ROWS` | int | 发送给 AI 验证的结果采样行数 | `10` |
| `VALIDATION_DATA_POLICY` | enum | 验证数据策略：`metadata_only`/`masked`/`full` | `metadata_only` |
| `VALIDATION_DENY_LIST` | string | 禁止发送到 LLM 的库/表/列规则 | 空 |
| `VALIDATION_CONFIDENCE_THRESHOLD` | float | 触发验证的 logprob 阈值 | `-1.0` |
| `MAX_RETRIES` | int | SQL 生成最大重试次数 | `2` |
| `SCHEMA_REFRESH_INTERVAL` | int | Schema 自动刷新间隔（秒） | `600` |
| `SCHEMA_MAX_TABLES_FOR_FULL_CONTEXT` | int | 超过此表数量时启用 schema 检索策略 | `50` |
| `LOG_LEVEL` | enum | 日志级别 | `INFO` |

### 4.2 安全性

- 数据库连接**必须**使用只读权限的用户；如检测到连接用户具有写权限，`STRICT_READONLY=true` 时拒绝启动，否则在日志中输出强烈警告
- 所有 SQL 执行均在只读事务中运行
- SQL **必须**通过 `pglast` AST 级别的安全校验（语句白名单 + 函数安全策略 + 对象级检查）
- 敏感配置（密码、API Key）不应出现在日志中
- 支持 SSL 连接到 PostgreSQL，通过 `PG_SSLMODE` 和 `PG_SSLROOTCERT` 配置
- AI 验证功能默认关闭，开启后受 `VALIDATION_DATA_POLICY` 和 `VALIDATION_DENY_LIST` 控制，防止敏感数据外发

### 4.3 性能

- Schema 缓存采用懒加载 + 后台预热，避免启动时阻塞
- 支持多个数据库的 schema 并发加载
- 连接池管理：每个数据库独立连接池（`DB_POOL_SIZE`），请求级别的并发控制（`MAX_CONCURRENT_REQUESTS`）
- 超过并发上限时返回 `E_RATE_LIMITED`
- 性能目标（SLO，基准条件：并发 20、schema 200 表、结果 1000 行）：
  - 端到端延迟 P95 ≤ 15s（含 LLM 调用，不含超时失败请求）
  - Schema 命中缓存时的查询开销 < 50ms（不含 LLM 和 SQL 执行时间）

### 4.4 可观测性

- 结构化日志输出（JSON 格式），包含 `request_id`、耗时等关键字段
- 日志中的 SQL 和查询结果需脱敏处理（遮掩可能的 PII 字段值）
- 记录每次 LLM 调用的 token 用量（prompt tokens / completion tokens）
- 记录 SQL 生成、校验、执行、验证的每个阶段耗时
- 记录 schema 缓存命中/未命中统计
- 记录 AI 验证是否触发及是否发生数据外发

### 4.5 错误处理

- 协议级错误通过 MCP 标准 error 返回；业务级错误通过统一响应 `error` 字段返回（见 3.5 错误返回分层）
- LLM 调用失败时提供明确的错误信息和错误码
- 数据库连接失败时采用指数退避重连策略（最多 5 次，初始间隔 100ms，最大间隔 3s，含随机抖动）
- SQL 执行失败时提供可读的错误描述

## 5. 技术约束

- 编程语言：Python 3.11+
- MCP 协议：使用官方 `mcp` Python SDK
- PostgreSQL 驱动：`asyncpg` 或 `psycopg` (v3, async)
- OpenAI 客户端：`openai` Python SDK
- SQL 解析：**必须**使用 `pglast` ≥ 6.0（基于 PostgreSQL 官方 libpg_query 解析器），支持 PostgreSQL 14-17
- 传输方式：支持 stdio 和 SSE 两种 MCP 传输方式

## 6. 典型使用流程

### 6.1 返回查询结果（return_type=result）

```
用户（通过 MCP 客户端）                     MCP Server
        |                                       |
        |  ---- query ----------------------->  |
        |  "查询过去30天订单总金额，按城市分组"      |
        |  database: "ecommerce"                |
        |  return_type: "result"                |
        |                                       |
        |                          [从缓存获取 ecommerce schema]
        |                          [构建 prompt + 调用 GPT-5-mini]
        |                          [生成 SQL]
        |                          [pglast AST 安全校验 → 通过]
        |                          [只读事务执行 SQL]
        |                          [风险评估 → 是否触发 AI 验证]
        |                          [验证通过（或未触发）]
        |                                       |
        |  <---- 返回统一响应 -----------------  |
        |  {request_id, database, sql,          |
        |   columns, rows, row_count, ...}      |
```

### 6.2 仅返回 SQL（return_type=sql）

```
用户（通过 MCP 客户端）                     MCP Server
        |                                       |
        |  ---- query ----------------------->  |
        |  "查询每个部门的平均薪资"               |
        |  database: "hr"                       |
        |  return_type: "sql"                   |
        |                                       |
        |                          [从缓存获取 hr schema]
        |                          [构建 prompt + 调用 GPT-5-mini]
        |                          [生成 SQL]
        |                          [pglast AST 安全校验 → 通过]
        |                          [不执行 SQL]
        |                                       |
        |  <---- 返回统一响应 -----------------  |
        |  {request_id, database, sql,          |
        |   columns: null, rows: null, ...}     |
```

## 7. 边界与限制

- 仅支持只读查询，不支持任何数据修改操作
- 不支持跨数据库的联合查询（检测到时返回 `E_CROSS_DB_UNSUPPORTED`）
- 不处理数据库权限管理（依赖数据库用户本身的权限）
- 生成的 SQL 质量依赖于 LLM 的能力和 schema 信息的完整度
- 大型数据库（数百张表）通过 schema 检索策略缓解 token 限制，但检索质量可能影响 SQL 生成准确性
- 不支持流式返回大结果集，不支持分页/续取
- 大字段（JSON/BYTEA/TEXT）超过 `MAX_CELL_BYTES` 时会被截断

## 8. 未来扩展（不在当前版本范围内）

- 查询历史记录与常用查询收藏
- 支持多种数据库类型（MySQL、SQLite 等）
- 支持 schema 变更的 webhook 通知自动刷新
- 查询结果的可视化（图表生成）
- 多轮对话上下文支持（基于之前的查询结果追问）
- 细粒度的表/列级别访问控制
- MCP 客户端鉴权与数据库白名单授权（按 client_id 映射可访问范围）
- 分页/游标续取协议
