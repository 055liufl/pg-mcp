按严重级别整理的审查结果如下（对照 PRD `0001` 与设计文档 `0002`）。

**Critical**

1.  
Location: 设计 §4.7 SQL 安全校验（函数策略，约 L951-L964） vs PRD §3.3（L111-L115）  
Severity: Critical  
Category: Security  
Description: PRD 要求“默认拒绝所有函数，仅允许安全白名单（IMMUTABLE/STABLE）”；设计实际是“黑名单拦截少量函数”，其余函数默认放行，存在明显绕过面（尤其扩展函数/未列入黑名单函数）。  
Suggestion: 改为白名单机制：查询 `pg_proc` + `pg_namespace` 建立允许函数集（内置 + volatility in {i,s}），并保留显式 denylist 兜底。

2.  
Location: 设计 §4.8 `_apply_limit`（L1021-L1026）  
Severity: Critical  
Category: Feasibility  
Description: SQL 重写为 `({sql}) AS __q LIMIT n` 语法错误（缺少 `SELECT * FROM`），会导致大量无 LIMIT 查询执行失败。  
Suggestion: 改为 AST 级改写或安全包裹：`SELECT * FROM ({sql_without_semicolon}) AS __q LIMIT ...`，并增加单元测试覆盖常见 SELECT/CTE/ORDER BY 场景。

3.  
Location: 设计 §4.3.1 `call_tool`（L355-L366）+ §4.11 异常抛出路径  
Severity: Critical  
Category: Consistency  
Description: PRD 要求“业务错误走统一响应 `error` 字段”；当前 `query_engine.execute()` 抛异常后无统一转换层，会变成协议异常，和 PRD 错误分层冲突。  
Suggestion: 在 `call_tool` 增加 `except PgMcpError` 映射到 `QueryResponse.error`；仅参数/协议错误走 MCP error。

**High**

4.  
Location: 设计 §1 技术选型（L15-L20） vs PRD §3.3/§5（L126, L347）  
Severity: High  
Category: Consistency  
Description: PRD 明确“必须使用 pglast”；设计主动偏离为 SQLGlot，违反硬约束。  
Suggestion: 若 PRD 不变，改回 `pglast`；若确需 SQLGlot，先变更 PRD并补充风险评估与验收标准。

5.  
Location: 设计 §4.7 foreign table 检查（L966-L980） vs PRD §3.3（L121）  
Severity: High  
Category: Security  
Description: `_get_foreign_tables()` 返回空集合，等于未实现“禁止访问 foreign table”。  
Suggestion: 在 schema discovery 增加 foreign table 元数据采集并纳入校验；未加载到该元数据时应 fail-closed（拒绝执行）。

6.  
Location: 设计 §4.4 `assert_readonly`（L486-L495） vs PRD §4.2（L308）  
Severity: High  
Category: Security  
Description: 仅检查 `rolsuper`，未检查实际写权限（表级 DML/DDL 权限、继承角色等）；且 `STRICT_READONLY=false` 时未体现“强警告”。  
Suggestion: 启动时执行权限探针（基于 `has_*_privilege`）并输出风险日志；`STRICT_READONLY=true` 时拒绝启动。

7.  
Location: 设计 §4.8 会话参数（L1003-L1007） vs PRD §3.3/§3.4（L135, L145）  
Severity: High  
Category: Security  
Description: 未设置 `search_path`、未设置 `idle_in_transaction_session_timeout`；PRD要求的会话级防护不完整。  
Suggestion: 在每次执行前显式 `SET search_path`（受控 schema 列表）及 `SET idle_in_transaction_session_timeout`。

8.  
Location: 设计 §4.2.1/§4.5.2 schema模型与发现（L211-L247, L550+） vs PRD §3.1（L34-L38）  
Severity: High  
Category: Completeness  
Description: PRD 要求物化视图、约束（CHECK/UNIQUE）、复合类型；设计仅有 `views/indexes/fk/enums`，缺失关键元数据。  
Suggestion: 扩展 `DatabaseSchema` 与发现逻辑：materialized views、constraints、composite types，保持与 PRD 一致。

9.  
Location: 设计 §4.5.3 `SchemaCache` 并发（L681-L704）  
Severity: High  
Category: Architecture  
Description: `_loading_locks` 无全局保护，`locked()` 检查与 `create_task` 间有竞态；并发请求可创建多个加载任务，导致重复加载/状态抖动。  
Suggestion: 使用原子“每库单飞”机制（per-db task registry + lock），并在 Redis 侧用分布式锁防多实例冲突。

10.  
Location: 设计 §4.5.3 `refresh()`（L717-L722）  
Severity: High  
Category: Error handling  
Description: `gather(..., return_exceptions=True)` 后直接返回 refreshed，失败被吞掉；与“返回明确错误信息”不一致。  
Suggestion: 汇总每库成功/失败详情，失败时返回业务错误或部分成功结构。

11.  
Location: 设计 §4.6/§4.10 OpenAI 调用 + §4.4 连接池（无重试退避） vs PRD §4.5（L337-L339）  
Severity: High  
Category: Completeness  
Description: 缺少 LLM timeout/异常映射（`E_LLM_TIMEOUT/E_LLM_ERROR`），缺少 DB 连接指数退避重连。  
Suggestion: 为 OpenAI/DB 调用加 `timeout + retry/backoff + error code mapping`，并在日志记录 attempt。

12.  
Location: 设计 §4.10 `_build_prompt`（L1219-L1227） vs PRD §3.4（L173）  
Severity: High  
Category: Security  
Description: `validation_deny_list` 配置存在但未执行，无法阻止敏感库/表/列外发到 LLM。  
Suggestion: 在构建验证 payload 前执行 deny 规则过滤；命中时降级到 `metadata_only` 或直接拒绝验证。

13.  
Location: 设计 §4.11 并发限流（L1266-L1269） vs PRD §4.3（L320）  
Severity: High  
Category: Performance  
Description: 通过私有属性 `_semaphore._value` 判断并发，存在竞态且依赖内部实现；可能出现排队而非立即 `E_RATE_LIMITED`。  
Suggestion: 用非阻塞 acquire 或独立令牌桶/队列，保证超限时稳定返回 `E_RATE_LIMITED`。

**Medium**

14.  
Location: 设计 §6 Redis Key（L1450-L1453）+ §4.5.3 实现  
Severity: Medium  
Category: Consistency  
Description: PRD 要求 TTL+LRU；设计未实现应用层 LRU，且 `schema_summary` key 设计了但未生产/消费。  
Suggestion: 明确 LRU 责任边界（Redis eviction policy 或应用内 LRU），补齐 `schema_summary` 读写链路或删除该 key 设计。

15.  
Location: 设计 §4.9 DB 推断（L1103-L1121） vs PRD §3.2（L84-L90）  
Severity: Medium  
Category: Completeness  
Description: 推断时 `SchemaNotReady` 直接跳过库，易误报 `E_DB_INFER_NO_MATCH`；跨库意图仅靠“是否提到库名”判断，语义检测不足。  
Suggestion: 对未就绪库返回可重试语义（或等待最短窗口）；跨库检测加入“实体命中分布”而非仅字符串提库名。

16.  
Location: 设计 §4.5.2 schema 发现实现（L554-L575）  
Severity: Medium  
Category: Performance  
Description: `get_columns/get_pk_constraint/get_indexes/get_foreign_keys` 在循环中高频调用，可能形成严重 N+1 元数据查询开销。  
Suggestion: 使用批量 SQL 拉取 `pg_catalog` 元数据，减少 round-trip；增加大 schema 基准测试。

17.  
Location: 设计全文（缺少测试章节；`QueryEngine`内部直接 new 依赖，L1253-L1259）  
Severity: Medium  
Category: Architecture  
Description: 可测试性不足：缺少测试策略与分层契约，核心依赖在构造器内固定实例化，不利于 mock LLM/DB/Cache 失败场景。  
Suggestion: 引入接口抽象与依赖注入（Protocol/Factory），补充单元测试矩阵（安全校验、错误映射、并发、缓存失效）。

18.  
Location: 设计 §4.2.2 `warnings: list[str] = []`（L289）  
Severity: Low  
Category: Feasibility  
Description: 可变默认值有共享风险（Pydantic虽可处理，但不建议在设计示例中保留此模式）。  
Suggestion: 使用 `Field(default_factory=list)`。

如果你需要，我可以下一步给出一份“按 PRD 条款编号的修订清单（可直接用于 design v2）”。