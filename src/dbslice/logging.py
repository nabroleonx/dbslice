"""
Structured logging infrastructure for dbslice.

This module provides production-ready logging with structured JSON output,
contextual information, and proper log level handling.

Design principles:
- Structured logging for easy parsing (JSON format)
- Logs to stderr (stdout is for SQL output)
- Respects --verbose and --no-progress flags
- Includes rich context (table names, row counts, timing)
- Performance-aware (minimal overhead when logging is disabled)
"""

import json
import logging
import sys
import time
from contextlib import contextmanager
from enum import Enum
from typing import Any

_loggers: dict[str, logging.Logger] = {}


class LogLevel(Enum):
    """Log levels for dbslice operations."""

    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL


class StructuredFormatter(logging.Formatter):
    """
    JSON formatter for structured logging.

    Outputs logs as JSON with consistent fields:
    - timestamp: ISO 8601 timestamp
    - level: Log level name
    - logger: Logger name (module path)
    - message: Human-readable message
    - context: Additional structured data
    """

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_data = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        # Add extra context fields if present
        if hasattr(record, "context") and record.context:
            log_data["context"] = record.context

        return json.dumps(log_data)


class HumanReadableFormatter(logging.Formatter):
    """
    Human-readable formatter for verbose output.

    More readable format for development and debugging:
    [TIMESTAMP] LEVEL: message (context)
    """

    def format(self, record: logging.LogRecord) -> str:
        """Format log record for human reading."""
        timestamp = self.formatTime(record, self.datefmt)
        message = record.getMessage()

        # Add context if present
        context_str = ""
        if hasattr(record, "context") and record.context:
            context_parts = [f"{k}={v}" for k, v in record.context.items()]
            context_str = f" ({', '.join(context_parts)})"

        # Add exception if present
        exc_str = ""
        if record.exc_info:
            exc_str = "\n" + self.formatException(record.exc_info)

        return f"[{timestamp}] {record.levelname}: {message}{context_str}{exc_str}"


def setup_logging(
    verbose: bool = False,
    no_progress: bool = False,
    structured: bool = False,
) -> None:
    """
    Configure logging for dbslice.

    Args:
        verbose: Enable DEBUG level logging
        no_progress: Disable INFO level logs (only warnings and errors)
        structured: Use JSON structured format (default: human-readable)

    Log levels:
    - verbose=True: DEBUG and above
    - verbose=False, no_progress=False: INFO and above (default)
    - no_progress=True: WARNING and above (suppress progress logs)
    """
    # Determine log level
    if verbose:
        level = logging.DEBUG
    elif no_progress:
        level = logging.WARNING
    else:
        level = logging.INFO

    # Create handler for stderr
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(level)

    # Set formatter based on structured flag
    formatter: logging.Formatter
    if structured:
        formatter = StructuredFormatter(datefmt="%Y-%m-%d %H:%M:%S")
    else:
        formatter = HumanReadableFormatter(datefmt="%Y-%m-%d %H:%M:%S")

    handler.setFormatter(formatter)

    # Configure root logger for dbslice
    root_logger = logging.getLogger("dbslice")
    root_logger.setLevel(logging.DEBUG)  # Capture all, handler filters
    root_logger.handlers.clear()  # Remove any existing handlers
    root_logger.addHandler(handler)

    # Prevent propagation to avoid duplicate logs
    root_logger.propagate = False


def get_logger(name: str) -> "ContextLogger":
    """
    Get or create a logger for a module.

    Args:
        name: Logger name (typically __name__ of calling module)

    Returns:
        ContextLogger instance for the module
    """
    if name not in _loggers:
        logger = logging.getLogger(f"dbslice.{name}")
        _loggers[name] = logger

    return ContextLogger(_loggers[name])


class ContextLogger:
    """
    Logger wrapper that supports structured context.

    Provides convenient methods for logging with additional context data
    that will be included in structured logs.
    """

    def __init__(self, logger: logging.Logger):
        self._logger = logger
        self._context: dict[str, Any] = {}

    def _log(
        self, level: int, msg: str, context: dict[str, Any] | None = None, exc_info: Any = None
    ):
        """Internal method to log with context."""
        # Merge default context with call-specific context
        merged_context = {**self._context}
        if context:
            merged_context.update(context)

        # Create log record with context
        extra = {"context": merged_context} if merged_context else {}
        self._logger.log(level, msg, extra=extra, exc_info=exc_info)

    def debug(self, msg: str, **context):
        """Log DEBUG level message with optional context."""
        self._log(logging.DEBUG, msg, context)

    def info(self, msg: str, **context):
        """Log INFO level message with optional context."""
        self._log(logging.INFO, msg, context)

    def warning(self, msg: str, **context):
        """Log WARNING level message with optional context."""
        self._log(logging.WARNING, msg, context)

    def error(self, msg: str, exc_info: Any = None, **context):
        """Log ERROR level message with optional context and exception."""
        self._log(logging.ERROR, msg, context, exc_info=exc_info)

    def critical(self, msg: str, exc_info: Any = None, **context):
        """Log CRITICAL level message with optional context and exception."""
        self._log(logging.CRITICAL, msg, context, exc_info=exc_info)

    def with_context(self, **context) -> "ContextLogger":
        """
        Create a new logger with additional context.

        Useful for adding persistent context for a scope:

        Example:
            logger = get_logger(__name__)
            table_logger = logger.with_context(table="users")
            table_logger.info("Fetching rows")  # Includes table="users"
        """
        new_logger = ContextLogger(self._logger)
        new_logger._context = {**self._context, **context}
        return new_logger

    @contextmanager
    def timed_operation(self, operation: str, **context):
        """
        Context manager for timing operations.

        Logs operation start/end with timing information.

        Example:
            with logger.timed_operation("schema_introspection"):
                schema = adapter.get_schema()
        """
        start_time = time.time()
        self.debug(f"Starting {operation}", **context)

        try:
            yield
            elapsed = time.time() - start_time
            self.info(f"Completed {operation}", duration_ms=int(elapsed * 1000), **context)
        except Exception as e:
            elapsed = time.time() - start_time
            self.error(
                f"Failed {operation}",
                exc_info=True,
                duration_ms=int(elapsed * 1000),
                error=str(e),
                **context,
            )
            raise


def log_extraction_start(logger: ContextLogger, database_url: str, seeds: list):
    """Log extraction start with configuration."""
    from dbslice.utils.connection import parse_database_url

    config = parse_database_url(database_url)
    masked_url = database_url.replace(config.password, "***") if config.password else database_url

    logger.info(
        "Starting extraction",
        database=config.database,
        db_type=config.db_type.value,
        seed_count=len(seeds),
        url=masked_url,
    )


def log_extraction_complete(
    logger: ContextLogger, total_rows: int, table_count: int, duration_ms: int
):
    """Log extraction completion with statistics."""
    logger.info(
        "Extraction complete",
        total_rows=total_rows,
        table_count=table_count,
        duration_ms=duration_ms,
    )


def log_query_execution(
    logger: ContextLogger, query: str, params: tuple, row_count: int | None = None
):
    """Log SQL query execution for debugging."""
    query_preview = query[:200] + "..." if len(query) > 200 else query

    context = {
        "query_preview": query_preview,
        "param_count": len(params) if params else 0,
    }

    if row_count is not None:
        context["row_count"] = row_count

    logger.debug("Executing query", **context)


def log_table_processing(
    logger: ContextLogger,
    table: str,
    operation: str,
    row_count: int,
    current: int | None = None,
    total: int | None = None,
):
    """Log table processing operations."""
    context = {
        "table": table,
        "operation": operation,
        "row_count": row_count,
    }

    if current is not None and total is not None:
        context["progress"] = f"{current}/{total}"

    logger.info(f"Processing {table}", **context)
