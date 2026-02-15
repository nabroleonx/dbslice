from dataclasses import dataclass
from urllib.parse import parse_qs, unquote, urlparse

from dbslice.config import DatabaseType
from dbslice.constants import DEFAULT_MYSQL_PORT, DEFAULT_POSTGRESQL_PORT
from dbslice.exceptions import InvalidURLError, UnsupportedDatabaseError

SCHEME_TO_DB_TYPE: dict[str, DatabaseType] = {
    "postgres": DatabaseType.POSTGRESQL,
    "postgresql": DatabaseType.POSTGRESQL,
    "mysql": DatabaseType.MYSQL,
    "sqlite": DatabaseType.SQLITE,
}


@dataclass(repr=False)
class DatabaseConfig:
    """Parsed database connection configuration."""

    db_type: DatabaseType
    host: str | None
    port: int | None
    user: str | None
    password: str | None
    database: str
    options: dict[str, str]

    original_url: str

    def __repr__(self) -> str:
        masked_pw = "***" if self.password else None
        return (
            f"DatabaseConfig(db_type={self.db_type!r}, host={self.host!r}, "
            f"port={self.port!r}, user={self.user!r}, password={masked_pw!r}, "
            f"database={self.database!r}, options={self.options!r}, "
            f"original_url={self.masked_url!r})"
        )

    @property
    def masked_url(self) -> str:
        """Return the original URL with the password masked."""
        if not self.password:
            return self.original_url
        return self.original_url.replace(self.password, "***")

    def to_dsn(self) -> str:
        """Convert to a DSN string for the database driver."""
        if self.db_type == DatabaseType.SQLITE:
            return self.database

        parts = []
        if self.host:
            parts.append(f"host={self.host}")
        if self.port:
            parts.append(f"port={self.port}")
        if self.user:
            parts.append(f"user={self.user}")
        if self.password:
            parts.append(f"password={self.password}")
        if self.database:
            parts.append(f"dbname={self.database}")

        for key, value in self.options.items():
            parts.append(f"{key}={value}")

        return " ".join(parts)


def parse_database_url(url: str) -> DatabaseConfig:
    """
    Parse a database URL into a DatabaseConfig with comprehensive validation.

    Supported formats:
    - postgres://user:pass@host:port/dbname
    - postgresql://user:pass@host:port/dbname
    - mysql://user:pass@host:port/dbname
    - sqlite:///path/to/database.db
    - sqlite:///./relative/path.db

    Args:
        url: Database connection URL

    Returns:
        DatabaseConfig with parsed connection details

    Raises:
        InvalidURLError: If URL is malformed
        UnsupportedDatabaseError: If database type is not supported
    """
    # Perform early validation for better error messages
    from dbslice.input_validators import (
        DatabaseURLValidationError,
    )
    from dbslice.input_validators import (
        validate_database_url as validate_url_format,
    )

    try:
        validate_url_format(url)
    except DatabaseURLValidationError as e:
        # Check for empty URL
        if "cannot be empty" in e.reason.lower():
            raise InvalidURLError(url, "URL cannot be empty")
        # Check for missing scheme (no :// in URL) before unsupported type
        if "://" not in url:
            raise InvalidURLError(url, "Missing URL scheme (e.g., postgres://, mysql://)")
        # Check if this is an unsupported database type error
        if "Unsupported database type" in e.reason:
            # Extract the scheme from the reason message
            import re

            match = re.search(r"'([^']+)'", e.reason)
            scheme = match.group(1) if match else url
            raise UnsupportedDatabaseError(scheme)
        raise InvalidURLError(url, e.reason)

    if not url:
        raise InvalidURLError(url, "URL cannot be empty")

    try:
        parsed = urlparse(url)
    except Exception as e:
        raise InvalidURLError(url, f"Failed to parse URL: {e}")

    scheme = parsed.scheme.lower()
    if not scheme:
        raise InvalidURLError(url, "Missing URL scheme (e.g., postgres://, mysql://)")

    if scheme not in SCHEME_TO_DB_TYPE:
        raise UnsupportedDatabaseError(scheme)

    db_type = SCHEME_TO_DB_TYPE[scheme]

    # Handle SQLite specially
    if db_type == DatabaseType.SQLITE:
        return _parse_sqlite_url(url, parsed)

    # Parse standard database URL
    return _parse_standard_url(url, parsed, db_type)


def _parse_sqlite_url(url: str, parsed) -> DatabaseConfig:
    """Parse SQLite URL: sqlite:///path/to/db.sqlite"""
    # SQLite path is in the path component
    # sqlite:////var/data/test.db -> path = //var/data/test.db -> /var/data/test.db
    # sqlite:///./relative/path -> path = /./relative/path -> ./relative/path
    # sqlite:///:memory: -> path = /:memory: -> :memory:
    path = parsed.path

    if not path:
        raise InvalidURLError(url, "SQLite URL requires a path: sqlite:///path/to/db.sqlite")

    # Handle different path formats
    if path.startswith("/./"):
        # Relative path: /./relative/path -> ./relative/path
        database = path[1:]
    elif path.startswith("/:"):
        # Special paths like :memory: -> /:memory: -> :memory:
        database = path[1:]
    elif path.startswith("//"):
        # Absolute Unix path: //var/data -> /var/data
        database = path[1:]
    elif path.startswith("/"):
        # Already correct absolute path
        database = path
    else:
        database = path

    return DatabaseConfig(
        db_type=DatabaseType.SQLITE,
        host=None,
        port=None,
        user=None,
        password=None,
        database=database,
        options={},
        original_url=url,
    )


def _parse_standard_url(url: str, parsed, db_type: DatabaseType) -> DatabaseConfig:
    """Parse standard database URL (PostgreSQL, MySQL)."""
    user = unquote(parsed.username) if parsed.username else None
    password = unquote(parsed.password) if parsed.password else None

    host = parsed.hostname
    port = parsed.port

    if port is None:
        if db_type == DatabaseType.POSTGRESQL:
            port = DEFAULT_POSTGRESQL_PORT
        elif db_type == DatabaseType.MYSQL:
            port = DEFAULT_MYSQL_PORT

    database = parsed.path.lstrip("/") if parsed.path else ""
    if not database:
        raise InvalidURLError(url, "Database name is required")

    options: dict[str, str] = {}
    if parsed.query:
        query_params = parse_qs(parsed.query)
        for key, values in query_params.items():
            options[key] = values[0] if values else ""

    return DatabaseConfig(
        db_type=db_type,
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        options=options,
        original_url=url,
    )


def get_adapter_for_url(url: str):
    """
    Get the appropriate database adapter for a URL.

    Args:
        url: Database connection URL

    Returns:
        Instantiated DatabaseAdapter for the database type
    """
    config = parse_database_url(url)

    if config.db_type == DatabaseType.POSTGRESQL:
        from dbslice.adapters.postgresql import PostgreSQLAdapter

        return PostgreSQLAdapter()
    elif config.db_type == DatabaseType.MYSQL:
        # MySQL adapter not implemented yet
        raise UnsupportedDatabaseError("mysql (not yet implemented)")
    elif config.db_type == DatabaseType.SQLITE:
        # SQLite adapter not implemented yet
        raise UnsupportedDatabaseError("sqlite (not yet implemented)")
    else:
        raise UnsupportedDatabaseError(config.db_type.value)
