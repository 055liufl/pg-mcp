# SQL 安全规则(skill 内强制约束)

> 本文件被 SKILL.md 引用,作为生成 / 校验 SQL 的硬规则。**违反任意一条都必须重新生成,不可放行执行**。

## 1. 只读模式

### 1.1 语句白名单(顶层)

允许的顶层语句**仅限**:

- `SELECT ...`
- `WITH ... SELECT ...`(CTE,顶层必须落到 SELECT)
- `EXPLAIN [ANALYZE] SELECT ...` / `EXPLAIN [ANALYZE] WITH ... SELECT ...`
- `VALUES (...)`(只读字面量,极少使用)

**禁止**任何形式的:

- DML:`INSERT` / `UPDATE` / `DELETE` / `MERGE` / `TRUNCATE` / `COPY ... FROM` / `COPY ... TO`
- DDL:`CREATE` / `DROP` / `ALTER` / `RENAME` / `COMMENT ON`
- DCL:`GRANT` / `REVOKE`
- TCL:`BEGIN` / `COMMIT` / `ROLLBACK` / `SAVEPOINT` / `SET LOCAL ...`(连接由 psql 管理)
- 维护:`VACUUM` / `ANALYZE` / `REINDEX` / `CLUSTER` / `LOCK` / `LISTEN` / `NOTIFY` / `LOAD`
- 复制:`CHECKPOINT` / `REFRESH MATERIALIZED VIEW`

### 1.2 多语句

**禁止**单次提交多条语句(`;` 分隔)。`psql -c '<SQL>'` 一次只发一条;若必须有 CTE,合并到一条 SELECT 即可。

如果生成的 SQL 含有除末尾 `;` 之外的内嵌 `;`,**整段拒绝**。

### 1.3 写操作伪装防御

下列模式同样拒绝(尽管语法是 SELECT):

- `SELECT * FROM pg_advisory_lock(...)`
- `SELECT pg_terminate_backend(pid)`
- `SELECT pg_cancel_backend(pid)`
- `SELECT * FROM dblink_exec(...)`(执行远端写)
- `WITH x AS (DELETE ... RETURNING *) SELECT * FROM x`(写 CTE)

如果 CTE 内含非 SELECT,**整段拒绝**。

## 2. 函数黑名单(无论上下文)

只要 SQL 中出现以下函数名(大小写不敏感),**整段拒绝**。允许在字符串字面量里出现该名字,但**不能作为函数调用**。

### 2.1 危险/破坏型
```
pg_terminate_backend  pg_cancel_backend  pg_advisory_lock  pg_advisory_xact_lock
pg_advisory_unlock    pg_reload_conf     pg_rotate_logfile  pg_backup_start
pg_backup_stop        pg_promote         pg_create_restore_point
```

### 2.2 文件 / OS / 网络访问
```
pg_read_file          pg_read_binary_file  pg_ls_dir         pg_ls_logdir
pg_ls_waldir          pg_stat_file         lo_import         lo_export
copy_from             copy_to              dblink            dblink_exec
dblink_connect        dblink_send_query    dblink_get_result postgres_fdw_*
```

### 2.3 长时间挂起 / 资源消耗
```
pg_sleep              pg_sleep_for         pg_sleep_until    pg_stat_reset
generate_series       -- 不在黑名单,但生成超过 100k 行需明确 LIMIT
```

### 2.4 用户/权限/系统状态
```
current_user / session_user / current_role  -- 允许只读引用,不允许 SET ROLE
pg_read_server_files  pg_write_server_files  pg_execute_server_program
set_config            current_setting       -- 允许只读,但不允许 SET LOCAL ...
```

### 2.5 元数据写入接口
```
pg_replication_slot_advance  pg_create_physical_replication_slot
pg_drop_replication_slot     pg_logical_emit_message
```

如果用户自己问 "数据库版本" 这种无害问题,允许 `SELECT version()` 和 `SELECT current_database()`。

## 3. 注入与字面量

skill 不接受外部字符串拼接到 SQL,**所有用户提到的值必须以 SQL 字面量形式嵌入**(整数、`'string'`、`E'...'`、`DATE '...'`、`TIMESTAMP '...'`)。

- 用户名/email 等字符串必须单引号包裹,内部单引号双写转义:`'O''Brien'`,**不要**用 `"`。
- 不要构造 `dynamic SQL`(`EXECUTE format(...)`)。
- 不要使用 `SET search_path = ...`;直接全限定 `schema.table`。
- 多 schema 的 `analytics_dw` / `shop_oms` 必须使用 `schema.table` 全限定。
- 整数字面量直接写,不要 `'1'::int`(可读但多余)。

## 4. 资源约束

每条 SQL 必须满足:

- **强制 `LIMIT`**:除非是聚合(`GROUP BY` / `COUNT(*)` / `SUM(...)` 等单标量返回),否则末尾**强制 `LIMIT`**(默认 `100`,scoring 阶段确认满意度;数据量明显 ≤100 时可以不加)。
- 禁止 `SELECT *` 在事实表 / 大表(`fact_*`、`fact_web_events`、`fact_ad_impressions`)上 —— **必须显式列出列**。
- 跨 fact JOIN(尤其 `fact_web_events` 与 `fact_sales`)必须先用 `WHERE` 缩小一侧再 JOIN,避免笛卡尔积。
- `EXPLAIN ANALYZE` **不允许**(会真正执行;需要计划用 `EXPLAIN`,但默认禁用)。

## 5. 敏感信息

- **结果输出**不得包含或回显:数据库密码、用户名(连接级别)、API key、token、`.env` 内容、文件路径、`pg_settings` 中含 `password`/`secret` 字段的行。
- 业务敏感字段(`email` / `phone` / `ip` / `user_agent` / `address`)允许返回但要谨慎截断:展示 ≤ 5 行样本,字符串列裁剪到 100 字符以内(在 SELECT 里 `LEFT(col, 100)`)。
- **绝不**生成 `SELECT * FROM pg_shadow`、`pg_authid`、`pg_user_mappings` 等。
- **绝不**查询 `pg_stat_activity` 中的 `query` 列(可能含其他用户的明文 SQL 包括秘密)。

## 6. 拒绝判定流程(伪代码)

```python
def is_safe(sql: str) -> tuple[bool, str | None]:
    # 1) 顶层语句白名单
    if not parses_as_select_or_explain(sql):  return False, "non-SELECT top-level"
    # 2) 多语句
    if has_extra_semicolons(sql):              return False, "multiple statements"
    # 3) 函数黑名单
    for fn in extract_function_calls(sql):
        if fn.lower() in BANNED_FUNCS:         return False, f"banned function: {fn}"
    # 4) 写操作伪装(CTE 含非 SELECT)
    if any_cte_is_dml(sql):                    return False, "DML inside CTE"
    # 5) 资源约束
    if missing_limit_for_non_aggregate(sql):   return False, "missing LIMIT"
    if select_star_on_huge_table(sql):         return False, "SELECT * on fact table"
    # 6) 敏感对象
    if touches_sensitive_catalog(sql):         return False, "sensitive catalog access"
    return True, None
```

`extract_function_calls` 可以用一个简单的正则:`\b([A-Za-z_][A-Za-z0-9_]*)\s*\(`,并对每个候选名做 `lower()` 后查表。**不要试图自己写 SQLGlot**,直接用名字白名单/黑名单匹配即可,黑名单严格成立才拒。

## 7. 例子

### 7.1 必须放行
```sql
SELECT id, title FROM posts WHERE status = 'published' LIMIT 10;

WITH recent AS (
  SELECT id FROM sales.orders WHERE placed_at >= NOW() - INTERVAL '7 days'
)
SELECT count(*) FROM recent;

EXPLAIN SELECT count(*) FROM fact.fact_sales WHERE date_key >= 20250101;
```

### 7.2 必须拒绝
```sql
-- 写操作
INSERT INTO posts (title) VALUES ('x');
UPDATE users SET email='x' WHERE id=1;
DELETE FROM comments;
DROP TABLE x;
TRUNCATE users;
COPY users TO '/tmp/dump.csv';

-- 多语句
SELECT 1; DROP TABLE users;

-- 危险函数
SELECT pg_sleep(60);
SELECT pg_read_file('/etc/passwd');
SELECT * FROM dblink('host=evil', 'SELECT 1');
SELECT lo_import('/etc/shadow');

-- 提权 / 进程操作
SELECT pg_terminate_backend(pid) FROM pg_stat_activity;
SELECT set_config('search_path', 'evil', false);

-- CTE 偷塞 DML
WITH x AS (DELETE FROM posts RETURNING *) SELECT * FROM x;
```
