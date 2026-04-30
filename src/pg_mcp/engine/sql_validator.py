"""SQLGlot AST validation with whitelist/blacklist and EXPLAIN handling."""

from __future__ import annotations

import sqlglot
from sqlglot import exp

from pg_mcp.models.errors import SqlParseError, SqlUnsafeError
from pg_mcp.models.schema import DatabaseSchema
from pg_mcp.protocols import ValidationResult


# Explicitly denied high-risk functions (rejected even if in pg_proc whitelist)
DENY_FUNCTIONS: frozenset[str] = frozenset({
    "pg_read_file",
    "pg_read_binary_file",
    "pg_ls_dir",
    "pg_stat_file",
    "lo_import",
    "lo_export",
    "lo_get",
    "lo_put",
    "pg_sleep",
    "pg_advisory_lock",
    "pg_advisory_xact_lock",
    "pg_advisory_unlock",
    "pg_advisory_unlock_all",
    "pg_try_advisory_lock",
    "pg_try_advisory_xact_lock",
    "pg_notify",
    "pg_listening_channels",
    "dblink",
    "dblink_exec",
    "dblink_connect",
    "dblink_disconnect",
    "dblink_send_query",
    "dblink_get_result",
    "pg_terminate_backend",
    "pg_cancel_backend",
    "pg_reload_conf",
    "pg_rotate_logfile",
    "set_config",
    "current_setting",
    "pg_switch_wal",
    "pg_create_restore_point",
})

# AST node types that are unconditionally blocked
BLOCKED_NODE_TYPES: tuple[type[exp.Expression], ...] = (
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Create,
    exp.Drop,
    exp.AlterTable,
    exp.Grant,
    exp.Command,
)

# Allowed top-level statement types
_ALLOWED_STATEMENT_TYPES: tuple[type[exp.Expression], ...] = (
    exp.Select,
    exp.Union,
    exp.Intersect,
    exp.Except,
    exp.Subquery,
)


def _canonicalize_table_id(table: exp.Table) -> str:
    """Normalize a table identifier to 'schema.name' lowercase form."""
    schema = (table.db or "public").lower().strip('"')
    name = table.name.lower().strip('"')
    return f"{schema}.{name}"


class SqlValidator:
    """AST-level SQL validator using SQLGlot.

    Enforces:
    - Single-statement queries only
    - Statement-type whitelist (SELECT, UNION, INTERSECT, EXCEPT, EXPLAIN)
    - Recursive DML/DDL detection
    - Function blacklist (always denied) + whitelist (schema-driven)
    - Foreign table access prohibition
    """

    def validate(self, sql: str, schema: DatabaseSchema | None = None) -> ValidationResult:
        """Validate a SQL string for safety.

        Args:
            sql: The SQL query to validate.
            schema: Optional database schema for function whitelist and foreign table checks.

        Returns:
            ValidationResult indicating whether the SQL is safe to execute.
        """
        # 1. Parse
        try:
            parsed = sqlglot.parse(sql, dialect="postgres")
        except sqlglot.errors.ParseError as e:
            return ValidationResult(
                valid=False,
                code="E_SQL_PARSE",
                reason=f"SQL syntax error: {e}",
            )

        # 2. Single-statement check
        stmts = [s for s in parsed if s is not None]
        if len(stmts) != 1:
            return ValidationResult(
                valid=False,
                code="E_SQL_UNSAFE",
                reason=f"Only single statements allowed, found {len(stmts)}",
            )
        ast = stmts[0]

        # 3. Statement-level whitelist
        if isinstance(ast, exp.Command):
            cmd = ast.this.upper() if ast.this else ""
            if cmd == "EXPLAIN":
                rest = ast.expression.sql() if ast.expression else ""
                if "ANALYZE" in rest.upper():
                    return ValidationResult(
                        valid=False,
                        code="E_SQL_UNSAFE",
                        reason="EXPLAIN ANALYZE is not allowed (executes query)",
                    )
                return ValidationResult(valid=True, is_explain=True)
            return ValidationResult(
                valid=False,
                code="E_SQL_UNSAFE",
                reason=f"Disallowed command: {cmd}",
            )

        if not isinstance(ast, _ALLOWED_STATEMENT_TYPES):
            return ValidationResult(
                valid=False,
                code="E_SQL_UNSAFE",
                reason=f"Only SELECT statements allowed, found: {type(ast).__name__}",
            )

        # 4. Recursive DML/DDL detection in the AST subtree
        for node in ast.walk():
            if isinstance(node, BLOCKED_NODE_TYPES):
                return ValidationResult(
                    valid=False,
                    code="E_SQL_UNSAFE",
                    reason=f"Disallowed statement type: {type(node).__name__}",
                )

        # 5. Function call checks: blacklist + whitelist
        allowed_funcs = schema.allowed_functions if schema else None
        for func in ast.find_all(exp.Func, exp.Anonymous):
            func_name = self._extract_func_name(func).lower()
            if not func_name:
                continue

            # Blacklist override: always deny
            if func_name in DENY_FUNCTIONS:
                return ValidationResult(
                    valid=False,
                    code="E_SQL_UNSAFE",
                    reason=f"Disallowed high-risk function: {func_name}",
                )

            # Whitelist check: if schema provides allowed functions, reject unknowns
            if allowed_funcs is not None and func_name not in allowed_funcs:
                return ValidationResult(
                    valid=False,
                    code="E_SQL_UNSAFE",
                    reason=f"Function not in allowlist: {func_name}",
                )

        # 6. Foreign table check
        if schema is not None:
            foreign_ids = schema.foreign_table_ids()
            if foreign_ids:
                for table in ast.find_all(exp.Table):
                    table_id = _canonicalize_table_id(table)
                    if table_id in foreign_ids:
                        return ValidationResult(
                            valid=False,
                            code="E_SQL_UNSAFE",
                            reason=f"Foreign table access denied: {table_id}",
                        )

        return ValidationResult(valid=True)

    def _extract_func_name(self, node: exp.Func | exp.Anonymous) -> str:
        """Extract the function name from an AST function node.

        Handles both built-in SQLGlot Func subclasses and Anonymous nodes
        (e.g. dblink(...)).
        """
        if isinstance(node, exp.Anonymous):
            if isinstance(node.this, str):
                return node.this
            return ""
        return (
            node.sql_name()
            if hasattr(node, "sql_name")
            else type(node).__name__
        )
