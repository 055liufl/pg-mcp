"""Log sanitization and PII masking utilities.

These functions ensure that sensitive data never leaks into logs:

- :func:`sanitize_sql` replaces string literals in SQL with ``'***'``.
- :func:`mask_pii` applies regex-based masking for emails and phone numbers.
"""

from __future__ import annotations

import re

# Replace SQL string literals (single-quoted) with a placeholder.
STRING_LITERAL_RE = re.compile(r"'[^']*'")

# Simple PII detection patterns.
EMAIL_RE = re.compile(r"[\w.-]+@[\w.-]+\.\w+")
PHONE_RE = re.compile(r"\b1[3-9]\d{9}\b")


def sanitize_sql(sql: str) -> str:
    """Replace all string literals in *sql* with ``'***'``.

    Args:
        sql: Raw SQL string that may contain sensitive literals.

    Returns:
        A sanitized copy of the SQL with literals redacted.
    """
    return STRING_LITERAL_RE.sub("'***'", sql)


def mask_pii(value: str) -> str:
    """Mask common PII patterns in *value*.

    Currently masks:
    - Email addresses -> ``***@***.***``
    - Chinese mobile numbers -> ``***PHONE***``

    Args:
        value: The raw string that may contain PII.

    Returns:
        A masked copy of the string.
    """
    value = EMAIL_RE.sub("***@***.***", value)
    value = PHONE_RE.sub("***PHONE***", value)
    return value
