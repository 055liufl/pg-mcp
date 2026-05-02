"""Unit tests for log sanitization and PII masking.

Covers:
- SQL string literal replacement
- PII masking (emails, phone numbers)
- Combined sanitization and masking
- Edge cases (no literals, no PII, empty strings)
"""

from __future__ import annotations

from pg_mcp.observability.sanitizer import mask_pii, sanitize_sql


class TestSanitizeSql:
    """Tests for SQL string literal sanitization."""

    def test_sanitize_sql_replaces_single_literal(self) -> None:
        sql = "SELECT * FROM users WHERE name = 'Alice'"
        result = sanitize_sql(sql)

        assert result == "SELECT * FROM users WHERE name = '***'"

    def test_sanitize_sql_replaces_multiple_literals(self) -> None:
        sql = "SELECT * FROM users WHERE name = 'Alice' AND status = 'active'"
        result = sanitize_sql(sql)

        assert "'***'" in result
        assert "Alice" not in result
        assert "active" not in result

    def test_sanitize_sql_preserves_non_literal_text(self) -> None:
        sql = "SELECT id, name FROM users"
        result = sanitize_sql(sql)

        assert result == sql

    def test_sanitize_sql_handles_empty_string(self) -> None:
        result = sanitize_sql("")

        assert result == ""

    def test_sanitize_sql_handles_string_with_no_literals(self) -> None:
        sql = "SELECT COUNT(*) FROM orders"
        result = sanitize_sql(sql)

        assert result == sql

    def test_sanitize_sql_handles_quoted_identifier(self) -> None:
        # Double-quoted identifiers should NOT be replaced
        sql = "SELECT * FROM \"Users\" WHERE name = 'Alice'"
        result = sanitize_sql(sql)

        assert '"Users"' in result
        assert "'***'" in result
        assert "Alice" not in result

    def test_sanitize_sql_handles_empty_literal(self) -> None:
        sql = "SELECT * FROM users WHERE name = ''"
        result = sanitize_sql(sql)

        assert result == "SELECT * FROM users WHERE name = '***'"

    def test_sanitize_sql_handles_literal_with_special_chars(self) -> None:
        sql = "SELECT * FROM users WHERE path = '/tmp/file.txt'"
        result = sanitize_sql(sql)

        assert "'***'" in result
        assert "/tmp/file.txt" not in result


class TestMaskPii:
    """Tests for PII masking."""

    def test_mask_pii_masks_email(self) -> None:
        text = "Contact alice@example.com for details"
        result = mask_pii(text)

        assert "alice@example.com" not in result
        assert "***@***.***" in result

    def test_mask_pii_masks_multiple_emails(self) -> None:
        text = "Emails: alice@example.com and bob@test.org"
        result = mask_pii(text)

        assert result.count("***@***.***") == 2

    def test_mask_pii_masks_chinese_phone(self) -> None:
        text = "Call 13800138000 for support"
        result = mask_pii(text)

        assert "13800138000" not in result
        assert "***PHONE***" in result

    def test_mask_pii_preserves_non_pii_text(self) -> None:
        text = "The quick brown fox jumps over 12345"
        result = mask_pii(text)

        assert result == text

    def test_mask_pii_handles_empty_string(self) -> None:
        result = mask_pii("")

        assert result == ""

    def test_mask_pii_does_not_mask_short_numbers(self) -> None:
        text = "Room 101, extension 1234"
        result = mask_pii(text)

        assert result == text

    def test_mask_pii_masks_email_in_sql_context(self) -> None:
        text = "SELECT * FROM users WHERE email = 'alice@example.com'"
        result = mask_pii(text)

        assert "alice@example.com" not in result


class TestCombined:
    """Tests for combined sanitization and masking workflows."""

    def test_sanitize_then_mask_pii(self) -> None:
        sql = "SELECT * FROM users WHERE email = 'alice@example.com'"
        sanitized = sanitize_sql(sql)
        masked = mask_pii(sanitized)

        # After sanitization, the email literal is already replaced
        assert "'***'" in masked
        assert "alice@example.com" not in masked

    def test_mask_pii_in_result_data(self) -> None:
        result_row = "User: John, Email: john@company.com, Phone: 13912345678"
        masked = mask_pii(result_row)

        assert "john@company.com" not in masked
        assert "13912345678" not in masked
        assert "***@***.***" in masked
        assert "***PHONE***" in masked
