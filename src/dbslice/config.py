import re
import unicodedata
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from dbslice.constants import DEFAULT_TRAVERSAL_DEPTH
from dbslice.models import VirtualForeignKey


class DatabaseType(Enum):
    """Supported database types."""

    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    SQLITE = "sqlite"


class TraversalDirection(Enum):
    """Direction for FK traversal."""

    UP = "up"  # Parents only (tables this one references)
    DOWN = "down"  # Children only (tables that reference this one)
    BOTH = "both"  # Both directions


class OutputFormat(Enum):
    """Output format options."""

    SQL = "sql"
    JSON = "json"
    CSV = "csv"


# SQL injection prevention: Dangerous keywords that should never appear in WHERE clauses
DANGEROUS_SQL_KEYWORDS = {
    "DROP",
    "DELETE",
    "TRUNCATE",
    "INSERT",
    "UPDATE",
    "ALTER",
    "CREATE",
    "RENAME",
    "GRANT",
    "REVOKE",
    # Transaction control (could be used for side effects)
    "COMMIT",
    "ROLLBACK",
    "SAVEPOINT",
    "EXECUTE",
    "EXEC",
    "CALL",
    # System commands (database-specific but dangerous)
    "SHUTDOWN",
    "COPY",
    "LOAD",
    # Data exfiltration via UNION
    "UNION",
    # Comment tricks to bypass filters
    "/*",
    "*/",
    "--",
    # Stacked queries (semicolon outside of quotes)
    # Note: semicolon is checked separately with context
}

# Dangerous PostgreSQL functions that should be blocked in WHERE clauses
DANGEROUS_PG_FUNCTIONS = {
    "pg_sleep",
    "pg_cancel_backend",
    "pg_terminate_backend",
    "pg_read_file",
    "pg_read_binary_file",
    "pg_ls_dir",
    "lo_import",
    "lo_export",
    "dblink",
    "dblink_exec",
}


def validate_where_clause(where_clause: str, seed_str: str = "") -> None:
    """
    Validate that a WHERE clause doesn't contain dangerous SQL keywords.

    This prevents SQL injection attacks by rejecting WHERE clauses that contain
    destructive or dangerous SQL operations. Only SELECT-like filtering operations
    are allowed.

    Args:
        where_clause: The WHERE clause to validate (without the WHERE keyword)
        seed_str: Original seed string for error reporting

    Raises:
        InsecureWhereClauseError: If dangerous keywords are detected
    """
    from dbslice.exceptions import InsecureWhereClauseError

    if not where_clause:
        return

    # Unicode normalization to prevent fullwidth character bypasses (e.g. ＤＲＯＰ -> DROP)
    where_clause_normalized = unicodedata.normalize("NFKC", where_clause)

    # Normalize: remove quoted strings to avoid false positives
    # This allows legitimate values like "status = 'DELETE'" to pass
    normalized = where_clause_normalized

    # Remove single-quoted strings (handles escaped quotes like O''Brien)
    normalized = re.sub(r"'(?:[^']*'')*[^']*'", "''", normalized)
    # Remove double-quoted identifiers/strings
    normalized = re.sub(r'"[^"]*"', '""', normalized)

    # Block PostgreSQL dollar-quoting ($$...$$ or $tag$...$tag$)
    if re.search(r"\$\$|\$[a-zA-Z_][a-zA-Z0-9_]*\$", normalized):
        raise InsecureWhereClauseError(seed_str or where_clause, "dollar quoting ($$)")

    # Block PostgreSQL escape strings (E'...')
    if re.search(r"\bE'", normalized, re.IGNORECASE):
        raise InsecureWhereClauseError(seed_str or where_clause, "escape string (E'...')")

    normalized_upper = normalized.upper()

    for keyword in DANGEROUS_SQL_KEYWORDS:
        # Use word boundary matching to avoid false positives
        # e.g., "dropbox_id" should not trigger "DROP"
        pattern = r"\b" + re.escape(keyword) + r"\b"
        if re.search(pattern, normalized_upper):
            raise InsecureWhereClauseError(seed_str or where_clause, keyword)

    normalized_lower = normalized.lower()
    for func_name in DANGEROUS_PG_FUNCTIONS:
        pattern = r"\b" + re.escape(func_name) + r"\s*\("
        if re.search(pattern, normalized_lower):
            raise InsecureWhereClauseError(seed_str or where_clause, func_name + "()")

    # Block subqueries (SELECT inside parentheses)
    if re.search(r"\(\s*SELECT\b", normalized_upper):
        raise InsecureWhereClauseError(seed_str or where_clause, "subquery (SELECT)")

    # Block type casts with :: (PostgreSQL-specific, can be used to smuggle data)
    if "::" in normalized:
        raise InsecureWhereClauseError(seed_str or where_clause, "type cast (::)")

    # Special check for semicolons (stacked queries)
    # Semicolons should not appear in WHERE clauses even in legitimate cases
    if ";" in normalized:
        raise InsecureWhereClauseError(seed_str or where_clause, ";")

    # Check for comment sequences that might be used to bypass validation
    if "--" in normalized or "/*" in normalized or "*/" in normalized:
        raise InsecureWhereClauseError(seed_str or where_clause, "comment sequence")


@dataclass
class SeedSpec:
    """Parsed seed specification."""

    table: str
    column: str | None  # None if using WHERE clause
    value: Any | None  # None if using WHERE clause
    where_clause: str | None  # Raw WHERE clause if provided

    @classmethod
    def parse(cls, seed_str: str) -> "SeedSpec":
        """
        Parse a seed string into a SeedSpec with comprehensive validation.

        Formats:
        - "table.column=value" -> simple equality
        - "table:WHERE_CLAUSE" -> raw WHERE clause

        Raises:
            InsecureWhereClauseError: If WHERE clause contains dangerous SQL keywords
            ValueError: If seed format is invalid or identifiers are unsafe
        """
        # Import validators here to avoid circular imports
        from dbslice.input_validators import (
            validate_column_name,
            validate_seed_value,
            validate_table_name,
        )

        if not seed_str or not seed_str.strip():
            raise ValueError("Seed specification cannot be empty")

        if ":" in seed_str and "=" not in seed_str.split(":")[0]:
            # Format: table:WHERE_CLAUSE
            parts = seed_str.split(":", 1)
            if len(parts) != 2:
                raise ValueError(f"Invalid seed format: {seed_str!r}. Use 'table:WHERE_CLAUSE'")

            table = parts[0].strip()
            where_clause = parts[1].strip()

            try:
                validate_table_name(table)
            except Exception as e:
                raise ValueError(f"Invalid seed table name: {e}")

            validate_where_clause(where_clause, seed_str)

            return cls(
                table=table,
                column=None,
                value=None,
                where_clause=where_clause,
            )
        elif "." in seed_str and "=" in seed_str:
            # Format: table.column=value
            try:
                eq_pos = seed_str.index("=")
                left = seed_str[:eq_pos]
                value = seed_str[eq_pos + 1 :].strip()

                dot_pos = left.rindex(".")
                table = left[:dot_pos].strip()
                column = left[dot_pos + 1 :].strip()
            except (ValueError, IndexError):
                raise ValueError(f"Invalid seed format: {seed_str!r}. Use 'table.column=value'")

            try:
                validate_table_name(table)
                validate_column_name(column)
            except Exception as e:
                raise ValueError(f"Invalid seed identifier: {e}")

            parsed_value: Any = value
            if value.isdigit():
                parsed_value = int(value)
            elif value.startswith("'") and value.endswith("'"):
                parsed_value = value[1:-1]
            elif value.startswith('"') and value.endswith('"'):
                parsed_value = value[1:-1]

            try:
                validate_seed_value(parsed_value)
            except Exception as e:
                raise ValueError(f"Invalid seed value: {e}")

            return cls(
                table=table,
                column=column,
                value=parsed_value,
                where_clause=None,
            )
        else:
            raise ValueError(
                f"Invalid seed format: {seed_str!r}. "
                "Use 'table.column=value' or 'table:WHERE_CLAUSE'"
            )

    def to_where_clause(self) -> tuple[str, tuple[Any, ...]]:
        """
        Convert to WHERE clause and parameters.

        Raises:
            InsecureWhereClauseError: If WHERE clause contains dangerous SQL keywords
        """
        if self.where_clause:
            # Re-validate in case object was constructed directly (not via parse)
            validate_where_clause(self.where_clause, f"{self.table}:{self.where_clause}")
            return (self.where_clause, ())
        else:
            return (f"{self.column} = %s", (self.value,))


@dataclass
class ExtractConfig:
    """Configuration for an extraction operation."""

    database_url: str
    seeds: list[SeedSpec]
    depth: int = DEFAULT_TRAVERSAL_DEPTH
    direction: TraversalDirection = TraversalDirection.BOTH
    output_format: OutputFormat = OutputFormat.SQL
    output_file: str | None = None
    anonymize: bool = False
    redact_fields: list[str] = field(default_factory=list)
    exclude_tables: set[str] = field(default_factory=set)
    passthrough_tables: set[str] = field(default_factory=set)
    verbose: bool = False
    dry_run: bool = False
    no_progress: bool = False
    validate: bool = True
    fail_on_validation_error: bool = False
    profile: bool = False  # Enable query profiling
    stream: bool = False  # Force streaming mode
    streaming_threshold: int = 50000  # Auto-enable streaming above this row count
    streaming_chunk_size: int = 1000  # Rows per chunk in streaming mode
    virtual_foreign_keys: list[VirtualForeignKey] = field(default_factory=list)
    schema: str | None = None  # PostgreSQL schema name (default: public)
