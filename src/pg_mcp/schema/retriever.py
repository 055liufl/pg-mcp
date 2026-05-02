"""Schema retrieval with precomputed TableIndex for large schemas."""

from __future__ import annotations

import re
from dataclasses import dataclass

from pg_mcp.models.schema import (
    DatabaseSchema,
    ForeignKeyInfo,
    TableInfo,
)

# Simple stopwords for keyword extraction
_STOPWORDS: set[str] = {
    "the", "a", "an", "is", "are", "was", "were", "be", "been",
    "being", "have", "has", "had", "do", "does", "did", "will",
    "would", "could", "should", "may", "might", "must", "shall",
    "can", "need", "dare", "ought", "used", "to", "of", "in",
    "for", "on", "with", "at", "by", "from", "as", "into",
    "through", "during", "before", "after", "above", "below",
    "between", "under", "again", "further", "then", "once",
    "here", "there", "when", "where", "why", "how", "all",
    "each", "few", "more", "most", "other", "some", "such",
    "no", "nor", "not", "only", "own", "same", "so", "than",
    "too", "very", "just", "and", "but", "if", "or", "because",
    "until", "while", "what", "which", "who", "whom", "this",
    "that", "these", "those", "am", "it", "its", "itself",
    "they", "them", "their", "theirs", "themselves", "you",
    "your", "yours", "yourself", "yourselves", "he", "him",
    "his", "himself", "she", "her", "hers", "herself", "we",
    "us", "our", "ours", "ourselves", "i", "me", "my", "myself",
    "mine", "s", "t", "don", "doesn", "didn", "wasn",
    "weren", "won", "wouldn", "couldn", "shouldn", "isn", "aren",
    "hasn", "haven", "hadn", "needn", "mustn", "shan", "mightn",
}


# Chinese (CJK) → English synonym map.  Used by both ``DbInference``
# (database selection) and ``SchemaRetriever`` (relevant table ranking) so
# that Chinese natural-language queries can match English schema entity
# names.
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
    "金额": ("amount", "total"),
    "总额": ("total", "amount"),
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

TokenSet = set[str]


@dataclass(frozen=True)
class TableIndex:
    """Precomputed per-table retrieval index.

    Stores all searchable terms (table name, column names, comments)
    as a single lowercase token set for O(1) lookup during retrieval.
    """

    schema_name: str
    table_name: str
    all_terms: frozenset[str]


class SchemaRetriever:
    """Retrieves relevant schema subsets for large databases.

    For schemas with many tables, builds a precomputed ``TableIndex``
    per table and uses keyword matching to return only the most
    relevant tables for a given natural language query.

    The retriever owns a per-database cache of precomputed indices so
    they can be built once per schema load (via the cache observer
    hooks) and reused across requests.
    """

    def __init__(self, max_tables_for_full: int = 50) -> None:
        self._max_tables = max_tables_for_full
        self._indices_by_db: dict[str, list[TableIndex]] = {}

    def should_use_retrieval(self, schema: DatabaseSchema) -> bool:
        """Return ``True`` if the schema is large enough to warrant retrieval."""
        return schema.table_count() > self._max_tables

    def install_index(self, database: str, schema: DatabaseSchema) -> None:
        """Build and store the retrieval index for ``database``.

        Intended to be wired as a ``SchemaCache`` loaded hook so that
        retrieval indexes stay in sync with the canonical Redis copy.
        """
        self._indices_by_db[database] = self.build_index(schema)

    def invalidate_index(self, database: str) -> None:
        """Drop the cached retrieval index for ``database``.

        Intended to be wired as a ``SchemaCache`` invalidated hook.
        """
        self._indices_by_db.pop(database, None)

    def build_index(self, schema: DatabaseSchema) -> list[TableIndex]:
        """Build precomputed retrieval indices for all tables in a schema.

        Args:
            schema: Full database schema.

        Returns:
            List of ``TableIndex`` objects, one per table.
        """
        indices: list[TableIndex] = []
        for table in schema.tables:
            terms: set[str] = {
                table.table_name.lower(),
                table.schema_name.lower(),
            }
            for col in table.columns:
                terms.add(col.name.lower())
                if col.comment:
                    terms.update(self._tokenize(col.comment.lower()))
            if table.comment:
                terms.update(self._tokenize(table.comment.lower()))
            indices.append(
                TableIndex(
                    schema_name=table.schema_name,
                    table_name=table.table_name,
                    all_terms=frozenset(terms),
                )
            )
        return indices

    def retrieve(self, user_query: str, schema: DatabaseSchema) -> str:
        """Return a condensed schema context for the given user query.

        Extracts keywords from the query, scores each table by relevance
        using the precomputed index, and returns a formatted text
        containing the top-scoring tables and their related foreign keys.

        Args:
            user_query: Natural language query from the user.
            schema: Full database schema. Indices precomputed via
                :meth:`install_index` are reused; otherwise indices are
                built on demand.

        Returns:
            Formatted schema text suitable for LLM prompt context.
        """
        keywords = self._extract_keywords(user_query)

        # Prefer the precomputed per-database index; fall back to building
        # on demand for callers that did not install one.
        cached_indices = self._indices_by_db.get(schema.database)
        if cached_indices is not None and len(cached_indices) == len(
            schema.tables
        ):
            indices: list[TableIndex] = cached_indices
        else:
            indices = self.build_index(schema)

        # Score tables using precomputed index
        scored_tables: list[tuple[TableInfo, float]] = []
        for table, idx in zip(schema.tables, indices):
            score = self._score_by_index(idx, keywords)
            scored_tables.append((table, score))

        # Sort by score descending
        scored_tables.sort(key=lambda x: -x[1])

        # Take top N tables with positive scores, or fallback to first N
        top_n = 20
        top_tables = [t for t, s in scored_tables[:top_n] if s > 0]
        if not top_tables:
            top_tables = [t for t, _ in scored_tables[:top_n]]

        # Include related foreign keys
        related_fks = self._get_related_foreign_keys(top_tables, schema)

        return self._build_context(top_tables, related_fks, schema)

    def _extract_keywords(self, user_query: str) -> TokenSet:
        """Extract searchable keywords from a user query.

        CJK tokens are enriched with English synonyms so Chinese
        natural-language queries can match English schema entity names.
        """
        tokens = self._tokenize(user_query.lower())
        enriched: set[str] = set(tokens)
        for tok in tokens:
            # Only CJK tokens need synonym expansion.
            if not any("\u4e00" <= c <= "\u9fff" for c in tok):
                continue
            for zh_key, en_words in _ZH_SYNONYMS.items():
                if zh_key in tok:
                    enriched.update(en_words)
        return enriched

    def _tokenize(self, text: str) -> TokenSet:
        """Tokenize text into a set of lowercase tokens.

        Preserves:
        - English alphanumeric tokens (incl. underscores for identifiers)
        - CJK character sequences (e.g. "销售额", "滚动")

        Filters out common English stopwords and very short tokens.
        """
        lowered = text.lower()
        # English / numeric / underscore tokens
        en_tokens = set(re.findall(r"[a-z0-9_]+", lowered))
        # CJK sequences (Unicode ranges CJK Unified Ideographs +
        # CJK Unified Ideographs Extension A)
        cjk_tokens = set(re.findall(r"[\u4e00-\u9fff]+", lowered))
        tokens = en_tokens | cjk_tokens
        return {
            t
            for t in tokens
            if t not in _STOPWORDS and len(t) >= 2
        }

    def _score_by_index(
        self, index: TableIndex, keywords: TokenSet
    ) -> float:
        """Score a table index against query keywords.

        Scoring:
        - Table name exact match: +10
        - Table name partial match (keyword contained in name): +5
        - Column name or other term exact match: +3
        - Partial match in other terms: +1
        """
        score = 0.0
        table_full_name = f"{index.schema_name}.{index.table_name}".lower()

        for kw in keywords:
            # Table name exact match
            if kw == index.table_name.lower():
                score += 10.0
            elif kw in table_full_name:
                score += 5.0
            # Exact match in term set
            elif kw in index.all_terms:
                score += 3.0
            # Partial match: keyword is a substring of any term
            else:
                for term in index.all_terms:
                    if kw in term or term in kw:
                        score += 1.0
                        break

        return score

    def _get_related_foreign_keys(
        self,
        tables: list[TableInfo],
        schema: DatabaseSchema,
    ) -> list[ForeignKeyInfo]:
        """Get foreign keys related to the selected tables.

        Includes FKs where either the source or target table is in
        the selected table set.
        """
        table_ids = {
            f"{t.schema_name}.{t.table_name}" for t in tables
        }
        related: list[ForeignKeyInfo] = []
        for fk in schema.foreign_keys:
            source_id = f"{fk.source_schema}.{fk.source_table}"
            target_id = f"{fk.target_schema}.{fk.target_table}"
            if source_id in table_ids or target_id in table_ids:
                related.append(fk)
        return related

    def _build_context(
        self,
        tables: list[TableInfo],
        foreign_keys: list[ForeignKeyInfo],
        schema: DatabaseSchema,
    ) -> str:
        """Build a formatted schema context string for LLM prompting.

        Includes selected tables with their columns, related foreign
        keys, and enum types referenced by the selected tables.
        """
        lines: list[str] = []
        lines.append(f"-- Database: {schema.database}")
        lines.append(f"-- Showing {len(tables)} of {schema.table_count()} tables")
        lines.append("")

        for table in tables:
            lines.append(
                f"TABLE {table.schema_name}.{table.table_name}"
            )
            if table.comment:
                lines.append(f"  COMMENT: {table.comment}")
            for col in table.columns:
                pk_marker = " [PK]" if col.is_primary_key else ""
                null_marker = "" if col.nullable else " NOT NULL"
                default_str = f" DEFAULT {col.default}" if col.default else ""
                lines.append(
                    f"  {col.name} {col.type}{null_marker}{default_str}{pk_marker}"
                )
                if col.comment:
                    lines.append(f"    -- {col.comment}")
            lines.append("")

        if foreign_keys:
            lines.append("-- Foreign Keys")
            for fk in foreign_keys:
                lines.append(
                    f"-- {fk.source_schema}.{fk.source_table}"
                    f"({','.join(fk.source_columns)})"
                    f" -> {fk.target_schema}.{fk.target_table}"
                    f"({','.join(fk.target_columns)})"
                )
            lines.append("")

        # Include enum types that might be referenced by selected tables
        enum_types_used: set[str] = set()
        for table in tables:
            for col in table.columns:
                for enum in schema.enum_types:
                    if enum.type_name.lower() in col.type.lower():
                        enum_types_used.add(enum.type_name)

        if enum_types_used:
            lines.append("-- Enum Types")
            for enum in schema.enum_types:
                if enum.type_name in enum_types_used:
                    values = ", ".join(f"'{v}'" for v in enum.values)
                    lines.append(
                        f"-- {enum.schema_name}.{enum.type_name}: {values}"
                    )
            lines.append("")

        return "\n".join(lines)
