"""Database inference with precomputed DbSummary."""

from __future__ import annotations

from dataclasses import dataclass

from pg_mcp.config import Settings
from pg_mcp.models.errors import (
    CrossDbUnsupportedError,
    DbInferAmbiguousError,
    DbInferNoMatchError,
    SchemaNotReadyError,
)
from pg_mcp.models.schema import DatabaseSchema
from pg_mcp.protocols import SchemaCacheProtocol

# Stop words for keyword extraction (simple English + common SQL terms)
_STOP_WORDS: frozenset[str] = frozenset({
    "the", "a", "an", "is", "are", "was", "were", "be", "been", "being",
    "have", "has", "had", "do", "does", "did", "will", "would", "could",
    "should", "may", "might", "must", "shall", "can", "need", "dare",
    "ought", "used", "to", "of", "in", "for", "on", "with", "at", "by",
    "from", "as", "into", "through", "during", "before", "after", "above",
    "below", "between", "under", "again", "further", "then", "once", "here",
    "there", "when", "where", "why", "how", "all", "each", "few", "more",
    "most", "other", "some", "such", "no", "nor", "not", "only", "own",
    "same", "so", "than", "too", "very", "just", "and", "but", "if", "or",
    "because", "until", "while", "what", "which", "who", "this", "that",
    "these", "those", "am", "it", "its", "itself", "they", "them", "their",
    "theirs", "themselves", "you", "your", "yours", "yourself", "yourselves",
    "he", "him", "his", "himself", "she", "her", "hers", "herself", "we",
    "us", "our", "ours", "ourselves", "i", "me", "my", "mine", "myself",
    "show", "list", "get", "find", "give", "tell", "please", "query",
    "select", "order", "group", "having", "limit", "offset",
    "join", "inner", "outer", "left", "right", "full", "cross", "natural",
    "using", "distinct", "union", "intersect", "except",
    "count", "sum", "avg", "min", "max", "database", "table", "column",
})



_ScoreEntry = tuple[str, float]


@dataclass
class DbSummary:
    """Lightweight precomputed summary of a database schema for inference."""

    database: str
    table_names: set[str]
    column_names: set[str]
    table_comments: list[str]
    column_comments: list[str]
    table_count: int

    @classmethod
    def from_schema(cls, schema: DatabaseSchema) -> DbSummary:
        """Build a DbSummary from a full DatabaseSchema."""
        table_names: set[str] = set()
        column_names: set[str] = set()
        table_comments: list[str] = []
        column_comments: list[str] = []

        for table in schema.tables:
            table_names.add(table.table_name.lower())
            if table.comment:
                table_comments.append(table.comment.lower())
            for col in table.columns:
                column_names.add(col.name.lower())
                if col.comment:
                    column_comments.append(col.comment.lower())

        return cls(
            database=schema.database,
            table_names=table_names,
            column_names=column_names,
            table_comments=table_comments,
            column_comments=column_comments,
            table_count=len(schema.tables),
        )


class DbInference:
    """Infers the target database from a natural language query.

    Uses precomputed lightweight DbSummary objects (built when schema is loaded)
    to score each database against extracted keywords from the user query.
    """

    AMBIGUITY_THRESHOLD: float = 0.15

    def __init__(self, cache: SchemaCacheProtocol, settings: Settings) -> None:
        self._cache = cache
        self._settings = settings
        self._summaries: dict[str, DbSummary] = {}

    def build_summary(self, schema: DatabaseSchema) -> None:
        """Precompute and store a DbSummary for the given schema.

        Called by SchemaCache after schema loading is complete.
        """
        self._summaries[schema.database] = DbSummary.from_schema(schema)

    def remove_summary(self, database: str) -> None:
        """Remove a DbSummary (e.g. on cache eviction or refresh)."""
        self._summaries.pop(database, None)

    async def infer(self, user_query: str) -> str:
        """Infer the most likely database for a user query.

        Args:
            user_query: Natural language query from the user.

        Returns:
            Name of the inferred database.

        Raises:
            DbInferNoMatchError: No database matches the query.
            DbInferAmbiguousError: Multiple databases are similarly likely.
            CrossDbUnsupportedError: Query appears to span multiple databases.
            SchemaNotReadyError: All databases are still loading.
        """
        keywords = self._extract_keywords(user_query)
        if not keywords:
            raise DbInferNoMatchError("No searchable keywords found in query")

        databases = self._cache.discovered_databases()
        if not databases:
            raise DbInferNoMatchError("No databases available")

        scored: list[_ScoreEntry] = []
        not_ready: list[str] = []

        for db in databases:
            summary = self._summaries.get(db)
            if summary is None:
                # Try to load schema and build summary on demand
                try:
                    schema = await self._cache.get_schema(db)
                    summary = DbSummary.from_schema(schema)
                    self._summaries[db] = summary
                except SchemaNotReadyError:
                    not_ready.append(db)
                    continue

            score = self._score(summary, keywords)
            if score > 0:
                scored.append((db, score))

        # If nothing scored and some are not ready, tell caller to retry
        if not scored and not_ready:
            raise SchemaNotReadyError(
                f"Database schemas still loading: {not_ready}",
                retry_after_ms=3000,
            )

        if not scored:
            if not_ready:
                raise SchemaNotReadyError(
                    f"Some databases not ready ({not_ready}), cannot infer",
                    retry_after_ms=3000,
                )
            raise DbInferNoMatchError("Query does not match any database")

        # Cross-database detection
        multi_hit = [(db, s) for db, s in scored if s > 0]
        if len(multi_hit) > 1:
            hit_dbs = [db for db, _ in multi_hit]
            if self._entity_spread_cross_db(keywords, hit_dbs):
                raise CrossDbUnsupportedError(
                    f"Query appears to span multiple databases: {hit_dbs}"
                )

        # Ambiguity detection
        scored.sort(key=lambda x: x[1], reverse=True)
        if len(scored) >= 2:
            top1, top2 = scored[0][1], scored[1][1]
            if top1 > 0 and (top1 - top2) / top1 < self.AMBIGUITY_THRESHOLD:
                raise DbInferAmbiguousError(
                    message=f"Ambiguous match: {scored[0][0]}, {scored[1][0]}",
                    candidates=[s[0] for s in scored[:3]],
                )

        return scored[0][0]

    def _extract_keywords(self, user_query: str) -> list[str]:
        """Extract searchable keywords from a natural language query.

        Simple tokenization: split on whitespace/punctuation, filter stop words,
        keep tokens with length >= 2 and alphanumeric content.
        """
        import re

        text = user_query.lower()
        # Split on non-alphanumeric (but preserve internal hyphens/underscores)
        tokens = re.split(r"[^\w\-]", text)
        keywords: list[str] = []
        for token in tokens:
            token = token.strip("-_")
            if len(token) >= 2 and token not in _STOP_WORDS:
                keywords.append(token)
        return keywords

    def _score(self, summary: DbSummary, keywords: list[str]) -> float:
        """Score a database summary against a list of keywords."""
        score = 0.0
        for kw in keywords:
            # Exact table name match
            if kw in summary.table_names:
                score += 10.0
                continue

            # Partial table name match
            for tname in summary.table_names:
                if kw in tname or tname in kw:
                    score += 5.0
                    break

            # Exact column name match
            if kw in summary.column_names:
                score += 3.0

            # Comment matches
            for comment in summary.table_comments:
                if kw in comment:
                    score += 2.0
                    break
            for comment in summary.column_comments:
                if kw in comment:
                    score += 2.0
                    break

        return score

    def _entity_spread_cross_db(
        self, keywords: list[str], dbs: list[str]
    ) -> bool:
        """Check if keywords are spread across different databases.

        Returns True if different keywords have their best match in different
        databases, suggesting a cross-database query.
        """
        best_db_per_kw: dict[str, str] = {}
        for kw in keywords:
            best_db: str | None = None
            best_score = 0.0
            for db in dbs:
                summary = self._summaries.get(db)
                if summary is None:
                    continue
                score = self._score(summary, [kw])
                if score > best_score:
                    best_score = score
                    best_db = db
            if best_db is not None:
                best_db_per_kw[kw] = best_db

        unique_best_dbs = set(best_db_per_kw.values())
        return len(unique_best_dbs) > 1
