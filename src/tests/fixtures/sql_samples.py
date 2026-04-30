"""SQL validation test cases: pass/fail samples for golden tests."""

# =============================================================================
# PASS CASES: SQL statements that should be accepted by the validator
# =============================================================================
PASS_CASES: list[tuple[str, str]] = [
    ("basic_select", "SELECT 1"),
    ("select_star", "SELECT * FROM users WHERE id = 1"),
    ("select_where", "SELECT name, email FROM users WHERE active = true"),
    ("cte_simple", "WITH cte AS (SELECT id FROM users) SELECT * FROM cte"),
    ("cte_multiple", """
        WITH
            cte1 AS (SELECT id FROM users),
            cte2 AS (SELECT user_id FROM orders)
        SELECT * FROM cte1 JOIN cte2 ON cte1.id = cte2.user_id
    """),
    ("aggregate_group_by", """
        SELECT COUNT(*), department FROM employees GROUP BY department
    """),
    ("join_query", """
        SELECT u.name, o.total
        FROM users u
        JOIN orders o ON u.id = o.user_id
    """),
    ("left_join", """
        SELECT u.name, o.total
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
    """),
    ("subquery", """
        SELECT * FROM (SELECT id FROM users WHERE active = true) AS active_users
    """),
    ("union_query", """
        SELECT id FROM users WHERE active = true
        UNION
        SELECT id FROM archived_users
    """),
    ("explain_select", "EXPLAIN SELECT * FROM orders"),
    ("explain_verbose", "EXPLAIN (VERBOSE, COSTS) SELECT * FROM orders"),
    ("window_function", """
        SELECT name,
               ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC)
        FROM employees
    """),
    ("select_from_values", "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)"),
    ("distinct_select", "SELECT DISTINCT department FROM employees"),
    ("order_by_limit", "SELECT * FROM users ORDER BY created_at DESC LIMIT 10"),
    ("safe_function_upper", "SELECT UPPER(name) FROM users"),
    ("safe_function_count", "SELECT COUNT(*) FROM orders"),
    ("safe_function_coalesce", "SELECT COALESCE(name, 'Unknown') FROM users"),
    ("safe_function_date_trunc", "SELECT DATE_TRUNC('month', created_at) FROM orders"),
    ("intersect_query", """
        SELECT id FROM users
        INTERSECT
        SELECT user_id FROM orders
    """),
    ("except_query", """
        SELECT id FROM users
        EXCEPT
        SELECT user_id FROM deleted_accounts
    """),
    ("case_expression", """
        SELECT name,
               CASE WHEN age >= 18 THEN 'adult' ELSE 'minor' END AS category
        FROM users
    """),
]

# =============================================================================
# FAIL CASES: SQL statements that should be rejected by the validator
# =============================================================================
FAIL_CASES: list[tuple[str, str, str]] = [
    # DML statements
    ("insert", "INSERT INTO users VALUES (1, 'x')", "E_SQL_UNSAFE"),
    ("insert_select", "INSERT INTO users SELECT * FROM temp_users", "E_SQL_UNSAFE"),
    ("update", "UPDATE users SET name = 'x'", "E_SQL_UNSAFE"),
    ("update_where", "UPDATE users SET name = 'x' WHERE id = 1", "E_SQL_UNSAFE"),
    ("delete", "DELETE FROM users", "E_SQL_UNSAFE"),
    ("delete_where", "DELETE FROM users WHERE id = 1", "E_SQL_UNSAFE"),
    ("truncate", "TRUNCATE TABLE users", "E_SQL_UNSAFE"),

    # DDL statements
    ("drop_table", "DROP TABLE users", "E_SQL_UNSAFE"),
    ("drop_index", "DROP INDEX idx_users", "E_SQL_UNSAFE"),
    ("create_table", "CREATE TABLE temp (id INT)", "E_SQL_UNSAFE"),
    ("alter_table", "ALTER TABLE users ADD COLUMN age INT", "E_SQL_UNSAFE"),
    ("create_index", "CREATE INDEX idx ON users(name)", "E_SQL_UNSAFE"),

    # Privilege statements
    ("grant", "GRANT SELECT ON users TO readonly", "E_SQL_UNSAFE"),
    ("revoke", "REVOKE SELECT ON users FROM readonly", "E_SQL_UNSAFE"),

    # COPY
    ("copy_to", "COPY users TO '/tmp/dump'", "E_SQL_UNSAFE"),
    ("copy_from", "COPY users FROM '/tmp/dump'", "E_SQL_UNSAFE"),
    ("copy_program", "COPY users TO PROGRAM 'cat'", "E_SQL_UNSAFE"),

    # Multi-statement
    ("multi_statement", "SELECT 1; DROP TABLE users", "E_SQL_UNSAFE"),
    ("multi_select", "SELECT 1; SELECT 2", "E_SQL_UNSAFE"),

    # Blacklisted functions
    ("func_pg_sleep", "SELECT pg_sleep(100)", "E_SQL_UNSAFE"),
    ("func_pg_read_file", "SELECT pg_read_file('/etc/passwd')", "E_SQL_UNSAFE"),
    ("func_pg_read_binary_file", "SELECT pg_read_binary_file('/etc/passwd')", "E_SQL_UNSAFE"),
    ("func_pg_ls_dir", "SELECT pg_ls_dir('.')", "E_SQL_UNSAFE"),
    ("func_lo_import", "SELECT lo_import('/etc/passwd')", "E_SQL_UNSAFE"),
    ("func_lo_export", "SELECT lo_export(123, '/tmp/out')", "E_SQL_UNSAFE"),
    ("func_dblink", "SELECT dblink('host=evil', 'SELECT 1')", "E_SQL_UNSAFE"),
    ("func_dblink_exec", "SELECT dblink_exec('host=evil', 'DROP TABLE x')", "E_SQL_UNSAFE"),
    ("func_pg_advisory_lock", "SELECT pg_advisory_lock(1)", "E_SQL_UNSAFE"),
    ("func_pg_notify", "SELECT pg_notify('channel', 'payload')", "E_SQL_UNSAFE"),
    ("func_pg_terminate_backend", "SELECT pg_terminate_backend(123)", "E_SQL_UNSAFE"),
    ("func_set_config", "SELECT set_config('search_path', 'public', false)", "E_SQL_UNSAFE"),

    # EXPLAIN ANALYZE (executes query)
    ("explain_analyze", "EXPLAIN ANALYZE SELECT * FROM users", "E_SQL_UNSAFE"),
    ("explain_analyze_verbose", "EXPLAIN (ANALYZE, VERBOSE) SELECT * FROM users", "E_SQL_UNSAFE"),

    # CALL / stored procedure
    ("call_procedure", "CALL some_procedure()", "E_SQL_UNSAFE"),

    # Nested DML inside CTE
    ("cte_with_insert", """
        WITH cte AS (INSERT INTO logs VALUES (1) RETURNING id)
        SELECT * FROM cte
    """, "E_SQL_UNSAFE"),

    # Nested DDL inside subquery (if parser catches it)
    ("select_with_drop", "SELECT * FROM (DROP TABLE users) t", "E_SQL_UNSAFE"),
]

# =============================================================================
# PARSE FAIL CASES: SQL that fails to parse
# =============================================================================
PARSE_FAIL_CASES: list[tuple[str, str]] = [
    ("invalid_syntax", "SELECT FROM WHERE"),
    ("unclosed_string", "SELECT * FROM users WHERE name = 'unclosed"),
    ("missing_paren", "SELECT * FROM (SELECT 1"),
]

# =============================================================================
# FOREIGN TABLE CASES: Tests for foreign table access denial
# =============================================================================
FOREIGN_TABLE_CASES: list[tuple[str, str, str]] = [
    ("select_foreign_table", 'SELECT * FROM "public"."foreign_data"', "E_SQL_UNSAFE"),
    ("select_foreign_qualified", "SELECT * FROM public.foreign_data", "E_SQL_UNSAFE"),
]
