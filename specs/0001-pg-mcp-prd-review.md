以下是对 `./specs/0001-pg-mcp-prd.md` 的结构化评审（按严重级别排序）。

## Critical

| Location | Severity | Category | Description | Suggestion |
|---|---|---|---|---|
| 3.4 结果验证（L129-L135），4.1 配置默认值（L226） | Critical | Completeness | 文档要求将“查询结果前 N 行”发送给外部 LLM 做验证，且 `ENABLE_VALIDATION=true` 默认开启。未定义脱敏、数据分级、出境合规或租户同意机制，存在高敏数据外发风险。 | 默认关闭验证（`ENABLE_VALIDATION=false`）；新增 `VALIDATION_DATA_POLICY`（脱敏/哈希/禁发敏感列）；支持按库/表/列配置“禁止发送到 LLM”；在响应和日志中标注“是否发生外发”。 |
| 2 目标用户（L11），8 未来扩展（L337） | Critical | Completeness | 当前版本面向“任何 MCP 客户端接入”，但客户端鉴权/授权被放到未来版本，意味着一旦 MCP 入口可达，可能直接暴露数据库查询能力。 | 将“客户端鉴权与授权”提升为 MVP 必需项：至少要求 API token + 客户端白名单 + 数据库访问范围映射；未通过鉴权直接拒绝请求。 |
| 3.3 SQL 安全校验（L94-L106） | Critical | Feasibility | 仅靠语句类型白名单 + 少量函数黑名单不足以保证只读安全。`SELECT` 可调用用户自定义 `VOLATILE` 函数、锁/通知类函数或高开销函数，仍可能造成副作用/资源攻击。 | 增加“函数调用默认拒绝”或“仅允许 `pg_proc.provolatile in ('i','s')` 的函数”；增加 schema 级函数白名单；禁止 `pg_sleep`、advisory lock、notify 等高风险调用；并加入查询成本限制（`statement_timeout` 外再加 `max_locks`/`work_mem`/`temp_file_limit` 策略）。 |
| 3.3 校验方式（L103） | Critical | Clarity | “仅允许白名单 AST 节点类型，拒绝所有非白名单节点”表述过于绝对，实际 `SELECT` 包含大量节点，若按字面执行会导致几乎不可实现或误拒绝。 | 改为“语句级白名单 + 关键危险节点黑名单 + 函数/对象级策略”；给出可测试的 AST 规则清单（示例节点集合）及允许/拒绝样例。 |

## High

| Location | Severity | Category | Description | Suggestion |
|---|---|---|---|---|
| 3.1 启动阶段（L19-L22） vs 3.1 缓存策略（L31） | High | Consistency | 前文要求启动时“读取并缓存每个可访问数据库 schema”，后文又要求“首次查询时懒加载”。两者冲突。 | 明确二选一：建议“启动只发现数据库清单，schema 按需加载 + 后台预热”；补充状态机（未加载/加载中/可用/失败）。 |
| 3.5 统一响应（L164-L184） vs 4.5 错误处理（L261） | High | Consistency | 一处定义“所有错误放入业务响应 `error` 字段”，另一处要求“通过 MCP 协议标准错误格式返回”，缺少边界。 | 规定分层：参数校验/工具级失败走 MCP error；业务可恢复错误走统一响应 `error`；给出错误映射表。 |
| 3.5 返回说明（L182） vs 3.4 截断策略（L116-L119） vs 3.6 `E_RESULT_TOO_LARGE`（L200） | High | Consistency | 文档同时说“return_type=result 返回完整结果”与“超限截断”，又定义“结果过大错误码”，未说明何时截断、何时报错。 | 明确策略优先级：例如“默认截断返回 + `truncated=true`；仅当 `STRICT_RESULT_LIMIT=true` 才返回 `E_RESULT_TOO_LARGE`”。 |
| 3.1 TTL 默认不过期（L32），缓存刷新默认禁用（L37） | High | Feasibility | 默认组合会导致 schema 长期陈旧，SQL 生成与真实库结构漂移，错误率升高。 | 默认启用周期刷新（如 5-15 分钟）；或至少开启基于 DDL 变更检测/版本戳刷新；返回中加入 schema 版本时间戳。 |
| 3.2 数据库推断（L69-L73） | High | Clarity | “得分接近”“显著阈值”未定义，无法测试一致性。 | 定义确定公式与阈值（如 top1-top2 < 0.15 判定歧义）；写入可回归测试样例。 |
| 3.4 触发验证条件（L125-L127） | High | Clarity | “复杂逻辑”“置信度较低”未定义来源和计算方式，不可验证。 | 量化规则：如 JOIN>=2、含窗口函数/子查询触发；置信度采用模型 logprob 或规则代理分；配置项化阈值。 |
| 4.3 性能（L243-L247） | High | Completeness | 缺少并发、连接池、每客户端配额、背压策略，存在资源争抢风险。 | 增加 `MAX_CONCURRENT_REQUESTS`、`DB_POOL_SIZE`、队列长度、限流/熔断策略，并定义超限错误码。 |

## Medium

| Location | Severity | Category | Description | Suggestion |
|---|---|---|---|---|
| 3.1 缓存刷新（L36） | Medium | Clarity | 通过自然语言识别“refresh schema”作为控制指令，容易误触发且不可预测。 | 将刷新改为显式参数（如 `admin_action=refresh_schema`）或独立 MCP Tool。 |
| 4.2 安全性（L234） vs 4.1 配置表（L209-L231） | Medium | Consistency | 文中出现 `STRICT_READONLY=true`，但配置清单未定义该项。 | 在配置表补充 `STRICT_READONLY`（默认值、类型、生效方式）。 |
| 3.6 错误码（L201） | Medium | Completeness | 定义了 `E_SCHEMA_NOT_READY`，但未说明客户端应等待、重试还是失败终止。 | 补充语义：返回 `retry_after_ms`；规定是否自动等待 schema 加载完成。 |
| 3.2/3.5 输入定义（L49-L54, L158-L163） | Medium | Completeness | 缺少参数合法性规则（空字符串、超长 query、非法 `return_type`、database 名格式）。 | 增加输入校验规范与对应错误码（如 `E_INVALID_ARGUMENT`、`E_QUERY_TOO_LONG`）。 |
| 4.3 SLO（L245-L246） | Medium | Clarity | SLO 未定义测试负载模型（并发数、schema规模、结果规模、网络条件），难验收。 | 补充基准条件（如并发 20、schema 200 表、结果 1k 行、LLM 网络 RTT 范围）。 |
| 3.4 结果上限（L116-L119） + 7 限制（L326） | Medium | Completeness | 仅有截断，无分页/续取机制，实际可用性受限。 | 增加分页参数（`limit/offset` 或 cursor）及续取协议。 |

## Low

| Location | Severity | Category | Description | Suggestion |
|---|---|---|---|---|
| 3.1 数据库排除规则（L20） | Low | Clarity | 仅举例排除 `template0/template1`，未说明 `postgres` 等维护库是否默认纳入。 | 明确默认排除/包含列表，并允许配置覆盖。 |
| 5 技术约束（L269） | Low | Feasibility | 强制 `pglast` 但未约束版本与目标 PostgreSQL 大版本兼容矩阵。 | 增加“支持 PG 版本范围 + pglast 最低版本 + 兼容性测试要求”。 |
| 4.4 可观测性（L250-L253） | Low | Completeness | 只定义记录内容，未定义敏感字段脱敏策略（SQL/结果中可能含 PII）。 | 增加日志脱敏规范（字段级屏蔽、采样、保留周期、访问审计）。 |

如果你需要，我可以下一步把这些问题直接整理成“可执行的 PRD 修订清单（逐条改写建议文本）”，便于你直接回填到文档。