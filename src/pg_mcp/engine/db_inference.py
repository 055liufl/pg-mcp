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


# Chinese (CJK) → English synonym map. Used to enrich keyword extraction so
# Chinese natural-language queries can match English schema entity names.
# Substring matching is used because the regex tokenizer treats consecutive
# CJK characters as a single token (e.g. "博客文章" stays one token).
_ZH_SYNONYMS: dict[str, tuple[str, ...]] = {
    # Blog / content
    "博客": ("blog", "post", "article"),
    "文章": ("post", "article"),
    "草稿": ("draft",),
    "已发布": ("published",),
    "评论": ("comment",),
    "标签": ("tag",),
    "作者": ("author", "user"),
    "审计": ("audit", "log"),
    "日志": ("log",),
    # E-commerce / sales
    "用户": ("user", "customer", "account"),
    "客户": ("customer", "client"),
    "订单": ("order",),
    "销售": ("sale", "sales", "revenue"),
    "营收": ("revenue", "sales"),
    "收入": ("revenue", "income"),
    "商品": ("product", "item"),
    "产品": ("product", "item"),
    "品牌": ("brand",),
    "品类": ("category",),
    "类目": ("category",),
    "库存": ("inventory", "stock"),
    "支付": ("payment",),
    "退款": ("refund",),
    "退货": ("return",),
    "优惠券": ("coupon",),
    "购物车": ("cart",),
    "地址": ("address", "shipping"),
    "邮箱": ("email",),
    "手机": ("phone", "mobile"),
    # Analytics / DW
    "渠道": ("channel",),
    "广告": ("ad", "advertisement", "campaign"),
    "投放": ("campaign", "ad"),
    "活动": ("campaign",),
    "邮件": ("email", "send"),
    "订阅": ("subscription",),
    "会话": ("session",),
    "事件": ("event",),
    "维度": ("dim", "dimension"),
    "事实": ("fact",),
    "国家": ("country", "region"),
    "地区": ("region", "country"),
    "门店": ("store",),
    "仓库": ("warehouse",),
    "活跃": ("active",),
    "留存": ("retention", "cohort"),
    "漏斗": ("funnel",),
    "转化": ("conversion",),
    "归因": ("attribution",),
    "终生价值": ("ltv", "lifetime"),
    "终生消费": ("lifetime",),
    "工单": ("ticket", "support"),
    "客服": ("support",),
}


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
            raise DbInferNoMatchError("查询中未找到可搜索的关键词")

        databases = self._cache.discovered_databases()
        if not databases:
            raise DbInferNoMatchError("没有可用的数据库")

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
                f"数据库 Schema 仍在加载中: {not_ready}",
                retry_after_ms=3000,
            )

        if not scored:
            if not_ready:
                raise SchemaNotReadyError(
                    f"部分数据库尚未就绪 ({not_ready})，无法推断",
                    retry_after_ms=3000,
                )
            raise DbInferNoMatchError("查询与任何数据库都不匹配")

        # Cross-database detection
        multi_hit = [(db, s) for db, s in scored if s > 0]
        if len(multi_hit) > 1:
            hit_dbs = [db for db, _ in multi_hit]
            if self._entity_spread_cross_db(keywords, hit_dbs):
                raise CrossDbUnsupportedError(
                    f"查询似乎涉及多个数据库: {hit_dbs}"
                )

        # Ambiguity detection
        scored.sort(key=lambda x: x[1], reverse=True)
        if len(scored) >= 2:
            top1, top2 = scored[0][1], scored[1][1]
            if top1 > 0 and (top1 - top2) / top1 < self.AMBIGUITY_THRESHOLD:
                raise DbInferAmbiguousError(
                    message=f"匹配不明确: {scored[0][0]}, {scored[1][0]}",
                    candidates=[s[0] for s in scored[:3]],
                )

        return scored[0][0]

    def _extract_keywords(self, user_query: str) -> list[str]:
        """Extract searchable keywords from a natural language query.

        Tokenization:
        - Splits on non-word characters (whitespace, punctuation).
        - Filters stop words and short tokens.
        - For CJK tokens, scans ``_ZH_SYNONYMS`` and appends English synonyms
          as substring matches so Chinese phrases can match English schema
          entity names.
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
                # Expand CJK substrings via synonym map.
                if any("\u4e00" <= c <= "\u9fff" for c in token):
                    for zh_key, en_words in _ZH_SYNONYMS.items():
                        if zh_key in token:
                            keywords.extend(en_words)
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
