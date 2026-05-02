---
name: postgres-data
description: 自然语言查询本机 docker 中的三个 PostgreSQL fixture 数据库(mini_blog / shop_oms / analytics_dw)。skill 会路由到对应数据库、生成只读 SQL、安全校验、用 psql 执行、自评结果置信度,并在置信度低时自动重试。当用户用中文/英文描述需求(例如"查一下 alice 的评论数"、"上个月退货金额"、"今年最热销的商品")且涉及这三个 fixture 库时触发。可选返回 SQL 而非结果。
---

# postgres-data

## 触发条件

当用户的请求满足**全部**以下条件时触发本 skill:

1. 自然语言中提到 `mini_blog` / `shop_oms` / `analytics_dw` 的某个数据,或者使用了三个数据库中已知的业务词汇(参见 `references/*.md` 第 1 节"路由关键词")
2. 用户希望"查询"/"统计"/"找出"/"列出"等读取意图,**而不是**修改/插入/删除数据
3. 用户没有明确要求使用其它数据库或 MCP 工具

## 输入约定

skill 接受单个自然语言字符串。skill 会自行从中识别:

- **目标数据库**:由路由关键词决定(见下文步骤 1)
- **输出形式**:
  - 默认 → 返回**结果**(若干样本行 + 简短解释)
  - 若用户说"只要 SQL"、"给我 SQL"、"只生成 SQL 别执行"、"return SQL only"、"don't run" 等 → 只返回 **SQL**
- **数量上限**:用户没指定时默认 `LIMIT 100`(聚合查询除外)

## 工作流程

### 步骤 1:路由(选数据库)

读取本 skill 的三份 reference,匹配关键词:

- `references/mini_blog.md` § 1
- `references/shop_oms.md` § 1
- `references/analytics_dw.md` § 1

**路由优先级规则:**

1. 用户**显式说出**数据库名 → 直接采用。
2. 出现"博客 / blog / post / comment / 文章 / 标签" → `mini_blog`。
3. 出现"事实表 / 维度 / fact_/dim_ / 数据仓库 / DW / cohort / funnel / KPI" → `analytics_dw`。
4. 出现"订单 / 客户 / 商品 / 购物车 / 优惠券 / 退款 / 发票 / 支付" 而**没有**第 3 条的 DW 词汇 → `shop_oms`。
5. 出现 fixture 库共有的词(如"用户"、"统计")**没有**特异关键词 → 在最终回答前向用户**澄清**(`AskUserQuestion`),不要猜。
6. 多重歧义(`mini_blog` 和 `shop_oms` 都说得通) → 优先选规模更小、对应词汇更精确的库;无法判断时澄清。

> 路由错误几乎是评分 < 7 的最常见原因。**步骤 7 评分时必须重新审视路由是否正确**。

### 步骤 2:加载 reference

读取 `.claude/skills/postgres-data/references/<chosen_db>.md` **全文**到上下文。这是所有列名/枚举值/索引/视图的事实来源,不要凭记忆写。

### 步骤 3:生成 SQL

根据 reference + 用户意图,生成一段 PostgreSQL 16 兼容的只读 SQL。

**硬性规则**(违反任何一条都不能交付,直接重写):
- 顶层只能是 `SELECT` / `WITH ... SELECT` / `EXPLAIN [SELECT|WITH ...]`
- 单条语句,不允许内嵌 `;` 拼接
- 跨 schema 时**全限定** `schema.table`(`shop_oms` / `analytics_dw` 必须)
- 用户提到的具体值用 SQL **字面量** 嵌入,不要把字符串拼接代入
- 非聚合查询末尾**强制 `LIMIT`**,默认 100,除非用户明确数量
- 引用 reference 中已存在的列名,**不允许**编造列(写之前查 § 5 / § 6 表 schema)
- 仅在事实表上使用列举字段,**避免**对 `fact.fact_web_events` / `fact.fact_ad_impressions` 这类大表写 `SELECT *`

详细规则见 `safety.md`。

### 步骤 4:安全校验

对生成的 SQL 按 `safety.md` 顺序校验:

1. 解析顶层语句类型,确认在白名单内
2. 切分检查多语句
3. 提取所有函数调用,与 `safety.md` 第 2 节的黑名单比对
4. 检查 CTE 是否含 DML
5. 检查 LIMIT、SELECT * on huge table、敏感对象访问

任何一条不通过 → 回到步骤 3 重写。**不要**带着不安全的 SQL 进入步骤 5。

### 步骤 5:用 psql 执行

只有一条命令格式:

```bash
docker exec -e PGPASSWORD=test pg-mcp-fixtures \
  psql -h localhost -U test -d <chosen_db> \
       -v ON_ERROR_STOP=1 \
       -P pager=off -P expanded=auto \
       -c '<SQL>'
```

实战要点:
- `<SQL>` 用**双引号**包裹整个字符串传给 shell;SQL 内部的字符串字面量保持单引号。
- 若 SQL 含 `'`(几乎一定有),用 heredoc 反而更稳:

```bash
docker exec -i -e PGPASSWORD=test pg-mcp-fixtures \
  psql -h localhost -U test -d <chosen_db> -v ON_ERROR_STOP=1 -P pager=off -P expanded=auto <<'SQL'
<your sql here>
SQL
```

- 想拿到机器友好的 TSV 用 `-At -F$'\t'`;想看好看的列表用默认。
- `-v ON_ERROR_STOP=1` 让 psql 一遇错就退出,便于检测。

### 步骤 6:执行失败处理

如果 psql 返回非零状态或输出含 `ERROR:` :

1. **完整阅读 psql 输出**(列出错误行号、提示)
2. 深度思考根因 —— 列名拼错?枚举值大小写不对?未限定 schema?表不存在?
3. 回到步骤 3 修正 **同一份** SQL,**不要**简单地"再试一次"。
4. 重新走步骤 4 → 5。
5. 同一个错误连续两次 → 检查路由,可能选错库。
6. 累计 3 次执行失败仍未跑通 → 不再硬试,告知用户并把已尝试的 SQL + 错误一并展示。

### 步骤 7:结果评分

按 `scoring.md` 的 5 维度 0-2 打分,合成 0-10 分:

| 总分 | 行动 |
|---|---|
| 9-10 | 接受,直接返回 |
| 7-8 | 接受,在最终回答附一句"置信度 X/10,因为..." |
| 4-6 | **不接受**,重新生成 SQL(最多 3 次重试) |
| 0-3 | **不接受**,且通常需要重新审视路由或澄清需求 |

评分时必须填写 `scoring.md` § 7 的自评模板(放在 thinking 里即可,不必输出给用户,除非用户要求查看推理)。

### 步骤 8:重试预算

- 总执行次数 ≤ **4**(初次 + 3 次重试)。
- 第 4 次仍 < 7 分:**不再重试**。把当前最佳 SQL + 部分结果 + 评分 + 不确定原因如实告诉用户。
- 同一会话内,同一用户问题改写不超过 5 次提问时,skill 主动提示用户"我已经尝试 N 次,可能问题本身有歧义,能否补充 X / Y?"

### 步骤 9:输出

默认输出格式(返回**结果**模式):

```markdown
**数据库**:`<db>`

**SQL**(已通过只读校验):
```sql
<最终 SQL>
```

**结果**(共 N 行,展示前 K 行):
| col1 | col2 | ... |
|---|---|---|
| ... | ... | ... |

**置信度**:X/10。<可选的不确定性说明>
```

仅 SQL 模式(用户说"只要 SQL"):

```markdown
**数据库**:`<db>`

```sql
<最终 SQL>
```
```

**绝不**输出:
- 数据库密码、连接字符串、`.env` 内容
- `pg_stat_activity.query` 的内容
- 超过 100 行/字段超过 500 字符的"原始数据倾倒"

## 端到端示例

### 示例 1:返回结果(默认)

用户:"列出 alice 写过的已发布文章"

1. 路由:出现 "alice"、"已发布文章" → `mini_blog`
2. 加载 `references/mini_blog.md`
3. 生成:
   ```sql
   SELECT p.id, p.title, p.slug, p.published_at, p.view_count
   FROM   posts p
   JOIN   users u ON u.id = p.author_id
   WHERE  u.username = 'alice'
     AND  p.status = 'published'
   ORDER  BY p.published_at DESC
   LIMIT  100;
   ```
4. 安全校验:通过(SELECT,LIMIT 100,无黑名单函数)
5. 执行 psql,得 3 行
6. 评分:
   - A 意图覆盖 2(完全覆盖)
   - B 列正确 2
   - C 过滤聚合 2
   - D 行数合理 2(3 行符合小演示库)
   - E 数值看起来对 2
   - 总分 10/10
7. 返回结果。

### 示例 2:仅返回 SQL

用户:"给我一条 SQL,统计 shop_oms 上月每种 refund_reason 的总金额,只要 SQL 别执行"

1. 路由:出现 "shop_oms" 显式 → `shop_oms`
2. 输出形式:用户明说 "只要 SQL 别执行" → SQL-only
3. 生成、安全校验、**跳过** psql 执行,直接返回:
   ```sql
   SELECT r.reason, SUM(r.amount) AS refund_total, COUNT(*) AS refund_count
   FROM   billing.refunds r
   WHERE  r.refunded_at >= date_trunc('month', NOW()) - INTERVAL '1 month'
     AND  r.refunded_at <  date_trunc('month', NOW())
   GROUP  BY r.reason
   ORDER  BY refund_total DESC;
   ```
4. (SQL-only 模式不评分,但仍要校验语法/安全)

## 文件结构

```
.claude/skills/postgres-data/
├── SKILL.md                    # 本文件 — 主入口
├── safety.md                   # SQL 安全校验规则
├── scoring.md                  # 结果置信度评分规则
└── references/
    ├── mini_blog.md            # mini_blog 完整参考
    ├── shop_oms.md             # shop_oms 完整参考
    └── analytics_dw.md         # analytics_dw 完整参考
```

## 维护指引

- 新增 fixture 数据库时:在 `references/` 下新增 `<db>.md`,按现有结构(路由关键词 / 连接 / Schema 总览 / 类型 / 表 / 索引 / 视图 / 业务约定 / 模板)填写,并在 SKILL.md 步骤 1 路由规则中加入相应关键词。
- 修改了 fixture SQL 后(行数变化、表新增):至少更新 `references/<db>.md` 的"行数"和"对象清单"两节;典型查询不必每次都改,但至少跑一次保证仍能 work。
- 新增危险函数到 PG 内置:在 `safety.md` § 2 黑名单里追加;新增的"必须拒绝"用例追加到 § 7.2。
