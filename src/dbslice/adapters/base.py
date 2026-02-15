from abc import ABC, abstractmethod
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from dbslice.models import ForeignKey, SchemaGraph


class DatabaseAdapter(ABC):
    """
    Abstract base class for database adapters.

    Each adapter implements database-specific logic for:
    - Connection management
    - Schema introspection
    - Data fetching
    - Transaction control for snapshot consistency
    """

    @abstractmethod
    def connect(self, url: str) -> None:
        """
        Establish database connection.

        Args:
            url: Database connection URL

        Raises:
            ConnectionError: If connection fails
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """Close database connection."""
        pass

    @abstractmethod
    def get_schema(self, schema_name: str | None = None) -> SchemaGraph:
        """
        Introspect and return complete schema graph.

        Args:
            schema_name: Optional schema name (e.g., 'public' for PostgreSQL).
                        If None, uses the default schema.

        Returns:
            SchemaGraph with all tables and foreign key relationships

        Raises:
            SchemaIntrospectionError: If introspection fails
        """
        pass

    @abstractmethod
    def fetch_rows(
        self,
        table: str,
        where_clause: str,
        params: tuple[Any, ...],
    ) -> Iterator[dict[str, Any]]:
        """
        Fetch rows matching a WHERE condition.

        Args:
            table: Table name
            where_clause: SQL WHERE clause (without 'WHERE' keyword)
            params: Parameters for the WHERE clause

        Yields:
            dict mapping column names to values for each row
        """
        pass

    @abstractmethod
    def fetch_by_pk(
        self,
        table: str,
        pk_columns: tuple[str, ...],
        pk_values: set[tuple[Any, ...]],
    ) -> Iterator[dict[str, Any]]:
        """
        Fetch rows by primary key values.

        Args:
            table: Table name
            pk_columns: Names of primary key columns
            pk_values: Set of PK value tuples to fetch

        Yields:
            dict mapping column names to values for each row
        """
        pass

    def fetch_by_pk_chunked(
        self,
        table: str,
        pk_columns: tuple[str, ...],
        pk_values: set[tuple[Any, ...]],
        chunk_size: int = 1000,
    ) -> Iterator[list[dict[str, Any]]]:
        """
        Fetch rows by primary key values in chunks.

        This method is designed for streaming large datasets without loading
        all rows into memory. It yields batches of rows instead of individual rows.

        Args:
            table: Table name
            pk_columns: Names of primary key columns
            pk_values: Set of PK value tuples to fetch
            chunk_size: Number of rows per chunk (default: 1000)

        Yields:
            Lists of row dicts, each list containing up to chunk_size rows
        """
        # Default implementation: batch the individual row iterator
        chunk = []
        for row in self.fetch_by_pk(table, pk_columns, pk_values):
            chunk.append(row)
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []

        if chunk:
            yield chunk

    @abstractmethod
    def fetch_fk_values(
        self,
        table: str,
        fk: ForeignKey,
        source_pk_values: set[tuple[Any, ...]],
    ) -> set[tuple[Any, ...]]:
        """
        Fetch FK column values for rows identified by their PKs.

        Given a set of source table PKs, fetches the FK column values
        that can be used to find related records in the target table.

        Args:
            table: Source table name (should match fk.source_table)
            fk: Foreign key relationship
            source_pk_values: Set of source table PK tuples

        Returns:
            Set of FK value tuples (values in target table's PK columns)
        """
        pass

    @abstractmethod
    def fetch_referencing_pks(
        self,
        fk: ForeignKey,
        target_pk_values: set[tuple[Any, ...]],
    ) -> set[tuple[Any, ...]]:
        """
        Fetch PKs of rows that reference the given target PKs via an FK.

        This is the reverse of fetch_fk_values - given PKs in the target
        table, find all PKs in the source table that reference them.

        Args:
            fk: Foreign key relationship
            target_pk_values: Set of target table PK tuples

        Returns:
            Set of source table PK tuples for referencing rows
        """
        pass

    @abstractmethod
    def fetch_all_pks(
        self,
        table: str,
        pk_columns: tuple[str, ...],
    ) -> set[tuple[Any, ...]]:
        """
        Fetch ALL primary keys from a table.

        This is used for passthrough tables that should be included in full,
        regardless of FK relationships. Examples include:
        - Configuration tables (site settings, feature flags)
        - Lookup tables (countries, currencies, status codes)
        - Small reference tables (categories, tags)
        - Django system tables (content_type, site, migrations)

        Args:
            table: Table name
            pk_columns: Names of primary key columns

        Returns:
            Set of all PK value tuples in the table
        """
        pass

    @abstractmethod
    def get_table_pk_columns(self, table: str) -> tuple[str, ...]:
        """
        Get the primary key column names for a table.

        Args:
            table: Table name

        Returns:
            Tuple of column names forming the primary key
        """
        pass

    @abstractmethod
    def begin_snapshot(self) -> None:
        """
        Begin a snapshot transaction for consistent reads.

        This ensures all subsequent reads see a consistent view of the
        database, even if other transactions modify data.
        """
        pass

    @abstractmethod
    def end_snapshot(self) -> None:
        """
        End the snapshot transaction.

        Releases any locks or resources held for snapshot isolation.
        """
        pass

    @contextmanager
    def snapshot_transaction(self):
        """
        Context manager for consistent snapshot reads.

        Usage:
            with adapter.snapshot_transaction():
                rows = adapter.fetch_rows(...)
        """
        self.begin_snapshot()
        try:
            yield
        finally:
            self.end_snapshot()

    def __enter__(self):
        """Support using adapter as context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close connection when exiting context."""
        self.close()
        return False

    # Helper methods that can be overridden if needed

    def quote_identifier(self, name: str) -> str:
        """
        Quote an identifier (table or column name) for safe SQL.

        Default implementation uses double quotes (SQL standard).
        Override for database-specific quoting.
        """
        return f'"{name}"'

    def get_placeholder(self) -> str:
        """
        Get the parameter placeholder for this database.

        Default is %s (psycopg2 style). Override for others.
        """
        return "%s"
