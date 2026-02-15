from dbslice.constants import MAX_SIMILAR_SUGGESTIONS

__all__ = [
    "DbsliceError",
    "ConnectionError",
    "InvalidSeedError",
    "InsecureWhereClauseError",
    "TableNotFoundError",
    "ColumnNotFoundError",
    "NoRowsFoundError",
    "CircularReferenceError",
    "UnsupportedDatabaseError",
    "InvalidURLError",
    "SchemaIntrospectionError",
    "ExtractionError",
]


class DbsliceError(Exception):
    """Base exception for all dbslice errors."""

    pass


class ConnectionError(DbsliceError):
    """Failed to connect to database."""

    def __init__(self, url: str, reason: str):
        self.url = url
        self.reason = reason
        masked_url = self._mask_password(url)
        super().__init__(f"Cannot connect to {masked_url}: {reason}")

    @staticmethod
    def _mask_password(url: str) -> str:
        """Mask password in database URL for safe display."""
        import re

        # Match password in URL: ://user:password@host
        # Use a greedy match for password up to the LAST @ before the host
        return re.sub(r"(://[^:]+:)(.+)(@[^@]+)$", r"\1****\3", url)


class InvalidSeedError(DbsliceError):
    """Seed query is invalid or malformed."""

    def __init__(self, seed: str, reason: str):
        self.seed = seed
        self.reason = reason
        super().__init__(f"Invalid seed '{seed}': {reason}")


class InsecureWhereClauseError(InvalidSeedError):
    """WHERE clause contains dangerous SQL keywords."""

    def __init__(self, seed: str, dangerous_keyword: str):
        self.dangerous_keyword = dangerous_keyword
        super().__init__(
            seed,
            f"WHERE clause contains dangerous keyword '{dangerous_keyword}'. "
            f"Only SELECT-like conditions are allowed (WHERE, AND, OR, NOT, IN, LIKE, etc.). "
            f"Destructive operations (DROP, DELETE, TRUNCATE, ALTER, etc.) are forbidden.",
        )


class TableNotFoundError(DbsliceError):
    """Referenced table does not exist in the database."""

    def __init__(self, table: str, available_tables: list[str] | None = None):
        self.table = table
        self.available_tables = available_tables
        msg = f"Table '{table}' not found in database"
        if available_tables:
            suggestions = self._find_similar(table, available_tables)
            if suggestions:
                msg += f". Did you mean: {', '.join(suggestions)}?"
        super().__init__(msg)

    @staticmethod
    def _find_similar(
        target: str, candidates: list[str], max_results: int = MAX_SIMILAR_SUGGESTIONS
    ) -> list[str]:
        """Find similar table names using simple substring matching."""
        target_lower = target.lower()
        similar = []
        for name in candidates:
            name_lower = name.lower()
            if target_lower in name_lower or name_lower in target_lower:
                similar.append(name)
            elif len(set(target_lower) & set(name_lower)) > len(target_lower) // 2:
                similar.append(name)
        return similar[:max_results]


class ColumnNotFoundError(DbsliceError):
    """Referenced column does not exist in the table."""

    def __init__(self, table: str, column: str, available_columns: list[str] | None = None):
        self.table = table
        self.column = column
        self.available_columns = available_columns
        msg = f"Column '{column}' not found in table '{table}'"
        if available_columns:
            msg += f". Available columns: {', '.join(available_columns[:10])}"
            if len(available_columns) > 10:
                msg += f" (and {len(available_columns) - 10} more)"
        super().__init__(msg)


class NoRowsFoundError(DbsliceError):
    """Seed query returned no rows."""

    def __init__(self, seed: str, table: str | None = None):
        self.seed = seed
        self.table = table
        msg = f"No rows found for seed: {seed}"
        if table:
            msg = f"No rows found in table '{table}' for seed: {seed}"
        super().__init__(msg)


class CircularReferenceError(DbsliceError):
    """Unbreakable circular reference detected."""

    def __init__(self, message: str):
        """
        Initialize with a detailed error message.

        Args:
            message: Complete error message describing the circular reference issue
        """
        super().__init__(message)


class UnsupportedDatabaseError(DbsliceError):
    """Database type is not supported."""

    def __init__(self, db_type: str):
        self.db_type = db_type
        super().__init__(
            f"Unsupported database type: '{db_type}'. Supported types: postgresql, mysql, sqlite"
        )


class InvalidURLError(DbsliceError):
    """Database URL is malformed."""

    def __init__(self, url: str, reason: str):
        self.url = url
        self.reason = reason
        super().__init__(f"Invalid database URL: {reason}")


class SchemaIntrospectionError(DbsliceError):
    """Failed to introspect database schema."""

    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(f"Failed to introspect schema: {reason}")


class ExtractionError(DbsliceError):
    """General extraction error."""

    def __init__(self, reason: str, table: str | None = None):
        self.reason = reason
        self.table = table
        msg = f"Extraction failed: {reason}"
        if table:
            msg = f"Extraction failed for table '{table}': {reason}"
        super().__init__(msg)
