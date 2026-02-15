"""Input validation utilities for CLI arguments and user-provided parameters.

Validates database URLs, seed specifications, identifiers, file paths, and other
user inputs before they reach the extraction engine. Distinct from validation.py,
which validates extracted *data* for referential integrity post-extraction.
"""

import re
from pathlib import Path
from typing import Any

from dbslice.constants import (
    MAX_TRAVERSAL_DEPTH,
    MIN_TRAVERSAL_DEPTH,
)


class ValidationError(Exception):
    """Base exception for validation errors."""

    pass


class SeedValidationError(ValidationError):
    """Invalid seed specification."""

    def __init__(self, seed: str, reason: str):
        self.seed = seed
        self.reason = reason
        super().__init__(f"Invalid seed '{seed}': {reason}")


class DepthValidationError(ValidationError):
    """Invalid depth parameter."""

    def __init__(self, depth: int, reason: str):
        self.depth = depth
        self.reason = reason
        super().__init__(f"Invalid depth {depth}: {reason}")


class DatabaseURLValidationError(ValidationError):
    """Invalid database URL."""

    def __init__(self, url: str, reason: str):
        self.url = url
        self.reason = reason
        super().__init__(f"Invalid database URL: {reason}")


class IdentifierValidationError(ValidationError):
    """Invalid table or column name."""

    def __init__(self, identifier: str, identifier_type: str, reason: str):
        self.identifier = identifier
        self.identifier_type = identifier_type
        self.reason = reason
        super().__init__(f"Invalid {identifier_type} '{identifier}': {reason}")


class FilePathValidationError(ValidationError):
    """Invalid file path."""

    def __init__(self, path: str, reason: str):
        self.path = path
        self.reason = reason
        super().__init__(f"Invalid file path '{path}': {reason}")


# Allows: alphanumeric, underscore, and dollar sign (for some DBs)
# Must start with letter or underscore
IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_$]*$")

# Maximum lengths for identifiers (PostgreSQL limits)
MAX_IDENTIFIER_LENGTH = 63
MAX_WHERE_CLAUSE_LENGTH = 10000

DATABASE_URL_PATTERN = re.compile(
    r"^(postgres|postgresql|mysql|sqlite)://"  # scheme
    r"([^:@/]+(?::[^@/]*)?@)?"  # optional user:pass@
    r"([^:/]+)?"  # optional host
    r"(?::(\d+))?"  # optional :port
    r"(/[^?]*)"  # path (database name or file path)
    r"(\?.*)?$"  # optional query string
)

SQLITE_URL_PATTERN = re.compile(r"^sqlite:///(.+)$")


def validate_identifier(identifier: str, identifier_type: str = "identifier") -> None:
    """
    Validate a SQL identifier (table name, column name, etc.).

    Args:
        identifier: The identifier to validate
        identifier_type: Type of identifier for error messages (e.g., "table", "column")

    Raises:
        IdentifierValidationError: If the identifier is invalid

    Examples:
        >>> validate_identifier("users", "table")  # OK
        >>> validate_identifier("order_items", "table")  # OK
        >>> validate_identifier("user-name", "table")  # Raises error
        >>> validate_identifier("'; DROP TABLE users--", "table")  # Raises error
    """
    if not identifier:
        raise IdentifierValidationError(identifier, identifier_type, "Identifier cannot be empty")

    if len(identifier) > MAX_IDENTIFIER_LENGTH:
        raise IdentifierValidationError(
            identifier,
            identifier_type,
            f"Identifier too long (max {MAX_IDENTIFIER_LENGTH} characters)",
        )

    if not IDENTIFIER_PATTERN.match(identifier):
        raise IdentifierValidationError(
            identifier,
            identifier_type,
            "Identifier must start with a letter or underscore and contain only "
            "alphanumeric characters, underscores, or dollar signs",
        )

    dangerous_keywords = [
        "select",
        "drop",
        "delete",
        "insert",
        "update",
        "alter",
        "create",
        "truncate",
    ]
    if identifier.lower() in dangerous_keywords:
        raise IdentifierValidationError(
            identifier,
            identifier_type,
            f"Identifier cannot be a SQL keyword: {identifier.lower()}",
        )


def validate_table_name(table: str) -> None:
    """
    Validate a table name.

    Args:
        table: The table name to validate

    Raises:
        IdentifierValidationError: If the table name is invalid

    Examples:
        >>> validate_table_name("users")  # OK
        >>> validate_table_name("'; DROP TABLE")  # Raises error
    """
    validate_identifier(table, "table name")


def validate_column_name(column: str) -> None:
    """
    Validate a column name.

    Args:
        column: The column name to validate

    Raises:
        IdentifierValidationError: If the column name is invalid

    Examples:
        >>> validate_column_name("user_id")  # OK
        >>> validate_column_name("invalid-name")  # Raises error
    """
    validate_identifier(column, "column name")


def validate_where_clause(where_clause: str) -> None:
    """
    Validate a WHERE clause for basic safety.

    Note: This is basic validation. SQL injection protection should also
    be handled by using parameterized queries in the database layer.

    Args:
        where_clause: The WHERE clause to validate

    Raises:
        SeedValidationError: If the WHERE clause appears unsafe

    Examples:
        >>> validate_where_clause("status = 'active'")  # OK
        >>> validate_where_clause("id = 123 AND status = 'pending'")  # OK
    """
    if not where_clause or not where_clause.strip():
        raise SeedValidationError(where_clause, "WHERE clause cannot be empty")

    if len(where_clause) > MAX_WHERE_CLAUSE_LENGTH:
        raise SeedValidationError(
            where_clause,
            f"WHERE clause too long (max {MAX_WHERE_CLAUSE_LENGTH} characters)",
        )

    if ";" in where_clause:
        raise SeedValidationError(
            where_clause,
            "WHERE clause contains potentially dangerous SQL patterns (semicolon found)",
        )

    dangerous_patterns = [
        (r"\bdrop\s+table\b", "DROP TABLE"),
        (r"\bdelete\s+from\b", "DELETE FROM"),
        (r"\btruncate\b", "TRUNCATE"),
        (r"\balter\s+table\b", "ALTER TABLE"),
        (r"\bunion\s+select\b", "UNION SELECT"),
        (r"\bexec\s*\(", "EXEC"),
        (r"\bexecute\s*\(", "EXECUTE"),
        (r"--", "SQL comment"),
        (r"/\*", "SQL comment"),
    ]

    where_lower = where_clause.lower()
    for pattern, name in dangerous_patterns:
        if re.search(pattern, where_lower, re.IGNORECASE):
            raise SeedValidationError(
                where_clause, f"WHERE clause contains potentially dangerous SQL patterns ({name})"
            )


def validate_seed_value(value: Any) -> None:
    """
    Validate a seed value.

    Args:
        value: The seed value to validate

    Raises:
        SeedValidationError: If the value is invalid

    Examples:
        >>> validate_seed_value(123)  # OK
        >>> validate_seed_value("test@example.com")  # OK
        >>> validate_seed_value(None)  # Raises error
    """
    if value is None:
        raise SeedValidationError(str(value), "Seed value cannot be None")

    # Check if value is too long (for string values)
    if isinstance(value, str):
        if len(value) > 1000:
            raise SeedValidationError(value, "Seed value too long (max 1000 characters)")
        if not value.strip():
            raise SeedValidationError(value, "Seed value cannot be empty or whitespace")


def validate_depth(depth: int) -> None:
    """
    Validate traversal depth parameter.

    Args:
        depth: The depth to validate

    Raises:
        DepthValidationError: If depth is out of acceptable range

    Examples:
        >>> validate_depth(3)  # OK
        >>> validate_depth(10)  # OK
        >>> validate_depth(0)  # Raises error
        >>> validate_depth(100)  # Raises error
    """
    if not isinstance(depth, int):
        raise DepthValidationError(depth, f"Depth must be an integer, got {type(depth).__name__}")

    if depth < MIN_TRAVERSAL_DEPTH:
        raise DepthValidationError(
            depth,
            f"Depth must be at least {MIN_TRAVERSAL_DEPTH} (prevents unbounded traversal)",
        )

    if depth > MAX_TRAVERSAL_DEPTH:
        raise DepthValidationError(
            depth,
            f"Depth cannot exceed {MAX_TRAVERSAL_DEPTH} (prevents DoS attacks). "
            f"If you need deeper traversal, consider using multiple seeds.",
        )


def validate_database_url(url: str) -> None:
    """
    Validate database connection URL format before attempting connection.

    This provides early, user-friendly error messages for malformed URLs
    before expensive connection attempts.

    Args:
        url: Database connection URL to validate

    Raises:
        DatabaseURLValidationError: If URL format is invalid

    Examples:
        >>> validate_database_url("postgres://user:pass@localhost/db")  # OK
        >>> validate_database_url("mysql://localhost:3306/mydb")  # OK
        >>> validate_database_url("sqlite:///path/to/db.sqlite")  # OK
        >>> validate_database_url("invalid")  # Raises error
    """
    if not url or not url.strip():
        raise DatabaseURLValidationError(url, "Database URL cannot be empty")

    # Extract and validate scheme first (before pattern matching)
    try:
        scheme = url.split("://")[0].lower()
    except (IndexError, AttributeError):
        raise DatabaseURLValidationError(url, "Missing URL scheme")

    supported_schemes = ["postgres", "postgresql", "mysql", "sqlite"]
    if scheme not in supported_schemes:
        raise DatabaseURLValidationError(
            url,
            f"Unsupported database type '{scheme}'. Supported: {', '.join(supported_schemes)}",
        )

    if url.startswith("sqlite://"):
        if not SQLITE_URL_PATTERN.match(url):
            raise DatabaseURLValidationError(
                url,
                "SQLite URL must be in format: sqlite:///path/to/database.db\n"
                "Examples:\n"
                "  - sqlite:///./relative/path.db\n"
                "  - sqlite:////absolute/path.db\n"
                "  - sqlite:///:memory:",
            )
        return

    if not DATABASE_URL_PATTERN.match(url):
        raise DatabaseURLValidationError(
            url,
            "Database URL must be in format: scheme://[user:password@]host[:port]/database\n"
            "Examples:\n"
            "  - postgres://user:pass@localhost:5432/mydb\n"
            "  - postgresql://localhost/mydb\n"
            "  - mysql://user:pass@localhost:3306/mydb",
        )

    # For non-SQLite, check that database name is present
    if scheme in ["postgres", "postgresql", "mysql"]:
        try:
            parts = url.split("://", 1)
            if len(parts) < 2:
                raise DatabaseURLValidationError(
                    url, "Database name is required in URL path (e.g., postgres://localhost/mydb)"
                )

            after_scheme = parts[1]
            if "/" not in after_scheme:
                raise DatabaseURLValidationError(
                    url, "Database name is required in URL path (e.g., /mydb)"
                )

            path_part = after_scheme.split("/", 1)[1].split("?")[0] if "/" in after_scheme else ""

            if not path_part or path_part.strip() == "":
                raise DatabaseURLValidationError(
                    url, "Database name is required in URL path (e.g., /mydb)"
                )
        except IndexError:
            raise DatabaseURLValidationError(url, "Database name is required in URL path")


def validate_output_file_path(path: str | Path) -> None:
    """
    Validate output file path.

    Args:
        path: The file path to validate

    Raises:
        FilePathValidationError: If the path is invalid

    Examples:
        >>> validate_output_file_path("/tmp/output.sql")  # OK
        >>> validate_output_file_path("./subset.sql")  # OK
        >>> validate_output_file_path("/root/forbidden.sql")  # May raise error
    """
    if not path:
        raise FilePathValidationError(str(path), "File path cannot be empty")

    path_obj = Path(path)

    # Check if parent directory exists (or can be created)
    parent = path_obj.parent
    if parent != Path(".") and not parent.exists():
        raise FilePathValidationError(
            str(path),
            f"Parent directory does not exist: {parent}\n"
            "Please create the directory first or use an existing directory.",
        )

    # Check if parent is writable (if it exists)
    if parent.exists() and not parent.is_dir():
        raise FilePathValidationError(str(path), f"Parent path is not a directory: {parent}")

    # Check for dangerous paths (only if parent exists to avoid false positives)
    if parent.exists():
        try:
            resolved = path_obj.resolve()
            # Check if trying to write to system directories
            dangerous_paths = ["/bin", "/sbin", "/usr/bin", "/usr/sbin", "/etc", "/sys", "/proc"]
            for dangerous in dangerous_paths:
                if str(resolved).startswith(dangerous):
                    raise FilePathValidationError(
                        str(path),
                        f"Cannot write to system directory: {dangerous}\n"
                        "Please choose a different output location.",
                    )
        except (OSError, RuntimeError) as e:
            raise FilePathValidationError(str(path), f"Invalid path: {e}")


def validate_exclude_tables(tables: list[str]) -> None:
    """
    Validate a list of table names to exclude.

    Args:
        tables: List of table names to validate

    Raises:
        IdentifierValidationError: If any table name is invalid

    Examples:
        >>> validate_exclude_tables(["audit_log", "temp_data"])  # OK
        >>> validate_exclude_tables(["valid", "'; DROP TABLE"])  # Raises error
    """
    if not tables:
        return

    for table in tables:
        validate_table_name(table)


def validate_redact_fields(fields: list[str]) -> None:
    """
    Validate redact field specifications (format: table.column).

    Args:
        fields: List of field specifications to validate

    Raises:
        IdentifierValidationError: If any field specification is invalid
        ValidationError: If format is incorrect

    Examples:
        >>> validate_redact_fields(["users.email", "orders.notes"])  # OK
        >>> validate_redact_fields(["invalid"])  # Raises error
    """
    if not fields:
        return

    for field in fields:
        if "." not in field:
            raise ValidationError(f"Invalid redact field format '{field}': must be 'table.column'")

        parts = field.split(".")
        if len(parts) != 2:
            raise ValidationError(f"Invalid redact field format '{field}': must be 'table.column'")

        table, column = parts
        validate_table_name(table)
        validate_column_name(column)
