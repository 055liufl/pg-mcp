"""Protocol interfaces and intermediate result types.

All core engine components declare their public surface through
:class:`typing.Protocol` definitions.  This allows :class:`QueryEngine`
to depend on abstractions rather than concrete implementations, enabling
easy mocking in unit tests and clean dependency injection.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel

from pg_mcp.models.schema import DatabaseSchema


class SqlGenerationResult(BaseModel):
    """Result of a single LLM SQL generation attempt."""

    sql: str
    prompt_tokens: int
    completion_tokens: int
    avg_logprob: float | None = None


class ValidationResult(BaseModel):
    """Result of SQL safety validation."""

    valid: bool
    code: str | None = None
    reason: str | None = None
    is_explain: bool = False


class ExecutionResult(BaseModel):
    """Result of executing a SQL query against PostgreSQL."""

    columns: list[str]
    column_types: list[str]
    rows: list[list[Any]]
    row_count: int
    truncated: bool = False
    truncated_reason: str | None = None


class ValidationVerdict(BaseModel):
    """Verdict returned by AI result validation."""

    verdict: str
    reason: str | None = None
    suggested_sql: str | None = None


class RefreshResult(BaseModel):
    """Result of a schema refresh operation."""

    succeeded: list[str]
    failed: list[dict[str, str]]


# ---------------------------------------------------------------------------
# Protocol definitions
# ---------------------------------------------------------------------------


@runtime_checkable
class SqlGeneratorProtocol(Protocol):
    """Generates SQL from a natural-language query and schema context."""

    async def generate(
        self,
        query: str,
        schema_context: str,
        feedback: str | None = None,
    ) -> SqlGenerationResult:
        """Generate a SQL query.

        Args:
            query: The natural-language user query.
            schema_context: Textual representation of the relevant schema.
            feedback: Optional feedback from a previous failed attempt.

        Returns:
            A :class:`SqlGenerationResult` containing the generated SQL
            and token usage statistics.
        """
        ...


@runtime_checkable
class SqlRewriterProtocol(Protocol):
    """Rewrite raw LLM-generated SQL to canonical PostgreSQL form.

    Translates non-PostgreSQL function names from BigQuery / MySQL /
    Snowflake dialects to their PostgreSQL equivalents (e.g.
    ``timestamp_trunc`` → ``date_trunc``, ``safe_cast`` → ``CAST``).
    Implementations must be a no-op for SQL that is already canonical
    PostgreSQL and must fall back to the original SQL when parsing fails.
    """

    def rewrite(self, sql: str) -> str:
        """Return SQL with cross-dialect functions rewritten.

        Args:
            sql: Raw SQL string from the LLM.

        Returns:
            The rewritten SQL, or the original SQL if no rewriting was
            possible (e.g. parse error). Never raises.
        """
        ...


@runtime_checkable
class SqlValidatorProtocol(Protocol):
    """Validates SQL for safety before execution."""

    def validate(
        self,
        sql: str,
        schema: DatabaseSchema | None = None,
        schema_names: list[str] | None = None,
    ) -> ValidationResult:
        """Validate the given SQL statement.

        Args:
            sql: The SQL string to validate.
            schema: Optional schema metadata for function whitelist and
                foreign-table checks.
            schema_names: Optional ordered ``search_path`` list used to
                resolve unqualified tables the same way the executor will.

        Returns:
            A :class:`ValidationResult` indicating whether the SQL is safe.
        """
        ...


@runtime_checkable
class SqlExecutorProtocol(Protocol):
    """Executes read-only SQL against a target database."""

    async def execute(
        self,
        database: str,
        sql: str,
        schema_names: list[str] | None = None,
        is_explain: bool = False,
    ) -> ExecutionResult:
        """Execute the SQL statement in a read-only transaction.

        Args:
            database: Target database name.
            sql: The SQL statement to execute.
            schema_names: Optional ordered list of schema names for
                ``search_path``.
            is_explain: Whether the SQL is an ``EXPLAIN`` statement.  When
                ``True``, the outer LIMIT wrapper is skipped.

        Returns:
            An :class:`ExecutionResult` with columns, rows, and metadata.
        """
        ...


@runtime_checkable
class SchemaCacheProtocol(Protocol):
    """Caches and retrieves schema metadata per database."""

    async def get_schema(self, database: str) -> DatabaseSchema:
        """Retrieve the schema for *database*, loading it if necessary.

        Raises:
            SchemaNotReadyError: If the schema is still loading.
        """
        ...

    async def refresh(self, database: str | None = None) -> RefreshResult:
        """Refresh the schema for one or all databases.

        Args:
            database: Specific database to refresh, or ``None`` for all.

        Returns:
            A :class:`RefreshResult` with per-database success/failure info.
        """
        ...

    def discovered_databases(self) -> list[str]:
        """Return the list of databases known to the cache."""
        ...


@runtime_checkable
class DbInferenceProtocol(Protocol):
    """Infers the target database from a natural-language query."""

    async def infer(self, user_query: str) -> str:
        """Infer the most likely database for *user_query*.

        Raises:
            DbInferAmbiguousError: When multiple databases score similarly.
            DbInferNoMatchError: When no database matches the query.
            CrossDbUnsupportedError: When the query appears to span DBs.
            SchemaNotReadyError: When schemas are still loading.
        """
        ...


@runtime_checkable
class ResultValidatorProtocol(Protocol):
    """Optional AI-powered validation of query results."""

    def should_validate(
        self,
        database: str,
        sql: str,
        result: ExecutionResult,
        generation: SqlGenerationResult,
    ) -> bool:
        """Determine whether result validation should run.

        Args:
            database: The database the query ran against.
            sql: The executed SQL.
            result: The execution result.
            generation: Metadata from the SQL generation step.

        Returns:
            ``True`` if validation should be triggered.
        """
        ...

    async def validate(
        self,
        user_query: str,
        sql: str,
        result: ExecutionResult,
        schema: DatabaseSchema,
    ) -> ValidationVerdict:
        """Validate the query result.

        Args:
            user_query: Original natural-language query.
            sql: The SQL that was executed.
            result: The execution result.
            schema: Schema metadata for the target database.

        Returns:
            A :class:`ValidationVerdict` with ``pass``, ``fix``, or ``fail``.
        """
        ...
