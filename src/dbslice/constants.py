DEFAULT_TRAVERSAL_DEPTH = 3
"""Default maximum depth for FK graph traversal."""

MIN_TRAVERSAL_DEPTH = 1
"""Minimum allowed traversal depth."""

MAX_TRAVERSAL_DEPTH = 10
"""Maximum allowed traversal depth (prevents DoS attacks)."""

DEFAULT_POSTGRESQL_PORT = 5432
"""Default port number for PostgreSQL connections."""

DEFAULT_MYSQL_PORT = 3306
"""Default port number for MySQL connections."""

MAX_SIMILAR_SUGGESTIONS = 3
"""Maximum number of similar suggestions to show in error messages."""

MAX_AVAILABLE_COLUMNS_DISPLAY = 10
"""Maximum number of columns to display in verbose output."""

DEFAULT_ANONYMIZATION_SEED = "dbslice_default_seed"
"""Default seed value for deterministic anonymization."""

DEFAULT_STREAMING_THRESHOLD = 50000
"""Auto-enable streaming mode above this row count."""

DEFAULT_STREAMING_CHUNK_SIZE = 1000
"""Default number of rows per chunk in streaming mode."""

DEFAULT_OUTPUT_FILE_MODE = 0o600
"""Secure default permissions for newly created output files."""

DEFAULT_MAX_SEED_ROWS = 10000
"""Default maximum number of rows a single seed query may return."""

SEED_ROW_WARNING_THRESHOLD = 1000
"""Warn when a seed query returns more than this many rows."""

DEFAULT_STATEMENT_TIMEOUT_MS = 0
"""Default PostgreSQL statement_timeout in milliseconds (0 = no timeout)."""
