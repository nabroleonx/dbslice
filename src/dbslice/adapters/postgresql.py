from collections.abc import Iterator
from typing import Any

import psycopg2
import psycopg2.extras

from dbslice.adapters.base import DatabaseAdapter
from dbslice.config import DatabaseType
from dbslice.exceptions import ConnectionError, ExtractionError, SchemaIntrospectionError
from dbslice.logging import get_logger
from dbslice.models import Column, ForeignKey, SchemaGraph, Table
from dbslice.utils.connection import parse_database_url

logger = get_logger(__name__)


class PostgreSQLAdapter(DatabaseAdapter):
    """PostgreSQL-specific database adapter."""

    # PostgreSQL has a parameter limit around 32767, but we'll be conservative
    # and use a smaller batch size to account for composite keys and safety margin
    DEFAULT_BATCH_SIZE = 1000

    def __init__(
        self,
        batch_size: int | None = None,
        profiler: Any = None,
        schema: str | None = None,
    ):
        self._conn: Any = None
        self._schema_name = schema or "public"
        self._schema_cache: SchemaGraph | None = None
        self.batch_size = batch_size or self.DEFAULT_BATCH_SIZE
        self.profiler = profiler

    def connect(self, url: str) -> None:
        """Establish PostgreSQL connection."""
        config = parse_database_url(url)

        if config.db_type != DatabaseType.POSTGRESQL:
            raise ConnectionError(url, f"Expected PostgreSQL URL, got {config.db_type.value}")

        logger.debug(
            "Connecting to PostgreSQL",
            host=config.host,
            port=config.port,
            database=config.database,
            user=config.user,
        )

        try:
            self._conn = psycopg2.connect(
                host=config.host,
                port=config.port,
                user=config.user,
                password=config.password,
                dbname=config.database,
                **{k: v for k, v in config.options.items()},
            )
            # Use autocommit for reads by default
            self._conn.autocommit = True

            # Set search_path so unqualified table names resolve to the target schema
            if self._schema_name != "public":
                with self._conn.cursor() as cur:
                    cur.execute("SET search_path TO %s, public", (self._schema_name,))
                logger.debug("search_path set", schema=self._schema_name)

            logger.info(
                "PostgreSQL connection established",
                database=config.database,
                schema=self._schema_name,
            )
        except psycopg2.Error as e:
            logger.error("PostgreSQL connection failed", error=str(e), exc_info=True)
            raise ConnectionError(url, str(e))

    def close(self) -> None:
        """Close PostgreSQL connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
            logger.debug("PostgreSQL connection closed")

    def get_schema(self, schema_name: str | None = None) -> SchemaGraph:
        """Introspect PostgreSQL schema."""
        if self._schema_cache is not None:
            logger.debug("Returning cached schema")
            return self._schema_cache

        schema = schema_name or self._schema_name
        logger.info("Starting schema introspection", schema=schema)

        try:
            tables = self._fetch_tables(schema)
            logger.debug("Tables fetched", count=len(tables))

            edges = self._fetch_foreign_keys(schema)
            logger.debug("Foreign keys fetched", count=len(edges))

            self._schema_cache = SchemaGraph(tables=tables, edges=edges)
            logger.info(
                "Schema introspection complete",
                schema=schema,
                table_count=len(tables),
                fk_count=len(edges),
            )
            return self._schema_cache
        except psycopg2.Error as e:
            logger.error("Schema introspection failed", error=str(e), exc_info=True)
            raise SchemaIntrospectionError(str(e))

    def _fetch_tables(self, schema: str) -> dict[str, Table]:
        """Fetch all tables with their columns and primary keys."""
        tables: dict[str, Table] = {}

        all_columns: dict[str, list[Column]] = {}
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    table_name,
                    column_name,
                    data_type,
                    is_nullable,
                    column_default
                FROM information_schema.columns
                WHERE table_schema = %s
                ORDER BY table_name, ordinal_position
                """,
                (schema,),
            )
            for row in cur.fetchall():
                table_name, col_name, data_type, is_nullable, default = row
                if table_name not in all_columns:
                    all_columns[table_name] = []
                all_columns[table_name].append(
                    Column(
                        name=col_name,
                        data_type=data_type,
                        nullable=is_nullable == "YES",
                        is_primary_key=False,
                        default=default,
                    )
                )

        all_pks: dict[str, list[str]] = {}
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT tc.table_name, kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY'
                  AND tc.table_schema = %s
                ORDER BY tc.table_name, kcu.ordinal_position
                """,
                (schema,),
            )
            for row in cur.fetchall():
                table_name, col_name = row
                if table_name not in all_pks:
                    all_pks[table_name] = []
                all_pks[table_name].append(col_name)

        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name
                """,
                (schema,),
            )
            table_names = [row[0] for row in cur.fetchall()]

        for table_name in table_names:
            columns = all_columns.get(table_name, [])
            pk_columns = tuple(all_pks.get(table_name, []))

            tables[table_name] = Table(
                name=table_name,
                schema=schema,
                columns=columns,
                primary_key=pk_columns,
                foreign_keys=[],  # Will be populated by _fetch_foreign_keys
            )

        return tables

    def _fetch_foreign_keys(self, schema: str) -> list[ForeignKey]:
        """Fetch all foreign key relationships.

        Uses pg_catalog instead of information_schema to correctly handle
        composite foreign keys. The information_schema approach produces a
        cross product between source and target columns for multi-column FKs.
        """
        fks: list[ForeignKey] = []

        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    c.conname AS constraint_name,
                    source_cls.relname AS source_table,
                    a_source.attname AS source_column,
                    target_cls.relname AS target_table,
                    a_target.attname AS target_column,
                    NOT a_source.attnotnull AS is_nullable
                FROM pg_constraint c
                JOIN pg_class source_cls ON c.conrelid = source_cls.oid
                JOIN pg_class target_cls ON c.confrelid = target_cls.oid
                JOIN pg_namespace ns ON source_cls.relnamespace = ns.oid
                CROSS JOIN LATERAL unnest(c.conkey, c.confkey)
                    WITH ORDINALITY AS u(source_attnum, target_attnum, ord)
                JOIN pg_attribute a_source
                    ON a_source.attrelid = c.conrelid
                    AND a_source.attnum = u.source_attnum
                JOIN pg_attribute a_target
                    ON a_target.attrelid = c.confrelid
                    AND a_target.attnum = u.target_attnum
                WHERE c.contype = 'f'
                  AND ns.nspname = %s
                ORDER BY c.conname, u.ord
                """,
                (schema,),
            )

            # Group by constraint name for multi-column FKs
            fk_data: dict[str, dict] = {}
            for row in cur.fetchall():
                constraint_name, source_table, source_col, target_table, target_col, is_nullable = (
                    row
                )

                if constraint_name not in fk_data:
                    fk_data[constraint_name] = {
                        "name": constraint_name,
                        "source_table": source_table,
                        "source_columns": [],
                        "target_table": target_table,
                        "target_columns": [],
                        "is_nullable": bool(is_nullable),
                    }

                fk_data[constraint_name]["source_columns"].append(source_col)
                fk_data[constraint_name]["target_columns"].append(target_col)

            for data in fk_data.values():
                fks.append(
                    ForeignKey(
                        name=data["name"],
                        source_table=data["source_table"],
                        source_columns=tuple(data["source_columns"]),
                        target_table=data["target_table"],
                        target_columns=tuple(data["target_columns"]),
                        is_nullable=data["is_nullable"],
                    )
                )

        return fks

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

        Raises:
            InsecureWhereClauseError: If WHERE clause contains dangerous SQL keywords
            ExtractionError: If query execution fails
        """
        # Defense-in-depth: validate WHERE clause even if it was validated earlier
        from dbslice.config import validate_where_clause

        validate_where_clause(where_clause, f"{table}:{where_clause}")

        try:
            query = f'SELECT * FROM "{table}" WHERE {where_clause}'
            logger.debug(
                "Executing fetch_rows query",
                table=table,
                where_clause=where_clause[:100],  # Truncate long clauses
            )

            if self.profiler:
                with self.profiler.track_query(
                    query, len(params), table=table, operation="fetch_rows"
                ) as tracker:
                    with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                        cur.execute(query, params)
                        row_count = 0
                        for row in cur:
                            row_count += 1
                            yield dict(row)
                        tracker.record_rows(row_count)
                        logger.debug("Fetched rows", table=table, row_count=row_count)
            else:
                with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(query, params)
                    row_count = 0
                    for row in cur:
                        row_count += 1
                        yield dict(row)
                    logger.debug("Fetched rows", table=table, row_count=row_count)
        except psycopg2.Error as e:
            logger.error(
                "Failed to fetch rows",
                table=table,
                error=str(e),
                exc_info=True,
            )
            raise ExtractionError(
                f"Failed to fetch rows from table '{table}': {e}", table=table
            ) from e

    def fetch_by_pk(
        self,
        table: str,
        pk_columns: tuple[str, ...],
        pk_values: set[tuple[Any, ...]],
    ) -> Iterator[dict[str, Any]]:
        """Fetch rows by primary key values with batching for large sets."""
        if not pk_values:
            return

        params_per_row = len(pk_columns)
        effective_batch_size = self.batch_size // max(params_per_row, 1)

        pk_values_list = list(pk_values)

        try:
            for batch_start in range(0, len(pk_values_list), effective_batch_size):
                batch_end = min(batch_start + effective_batch_size, len(pk_values_list))
                batch_pks = pk_values_list[batch_start:batch_end]

                if len(pk_columns) == 1:
                    col = pk_columns[0]
                    values = [v[0] for v in batch_pks]
                    placeholders = ", ".join(["%s"] * len(values))
                    query = f'SELECT * FROM "{table}" WHERE "{col}" IN ({placeholders})'
                    params = values
                else:
                    # Composite PK - use OR of AND conditions
                    conditions = []
                    params = []
                    for pk_tuple in batch_pks:
                        cond_parts = [f'"{col}" = %s' for col in pk_columns]
                        conditions.append(f"({' AND '.join(cond_parts)})")
                        params.extend(pk_tuple)
                    query = f'SELECT * FROM "{table}" WHERE {" OR ".join(conditions)}'

                if self.profiler:
                    with self.profiler.track_query(
                        query, len(params), table=table, operation="fetch_by_pk"
                    ) as tracker:
                        with self._conn.cursor(
                            cursor_factory=psycopg2.extras.RealDictCursor
                        ) as cur:
                            cur.execute(query, params)
                            row_count = 0
                            for row in cur:
                                row_count += 1
                                yield dict(row)
                            tracker.record_rows(row_count)
                else:
                    with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                        cur.execute(query, params)
                        for row in cur:
                            yield dict(row)
        except psycopg2.Error as e:
            raise ExtractionError(
                f"Failed to fetch rows by primary key from table '{table}': {e}", table=table
            ) from e

    def fetch_by_pk_chunked(
        self,
        table: str,
        pk_columns: tuple[str, ...],
        pk_values: set[tuple[Any, ...]],
        chunk_size: int = 1000,
    ) -> Iterator[list[dict[str, Any]]]:
        """
        Fetch rows by primary key values in chunks using server-side cursor.

        This optimized implementation uses PostgreSQL server-side cursors
        to stream large result sets without loading everything into memory.
        Large PK sets are batched to avoid exceeding PostgreSQL's parameter limit.

        Args:
            table: Table name
            pk_columns: Names of primary key columns
            pk_values: Set of PK value tuples to fetch
            chunk_size: Number of rows per chunk (default: 1000)

        Yields:
            Lists of row dicts, each list containing up to chunk_size rows
        """
        if not pk_values:
            return

        params_per_row = len(pk_columns)
        effective_batch_size = self.batch_size // max(params_per_row, 1)

        pk_values_list = list(pk_values)

        try:
            for batch_start in range(0, len(pk_values_list), effective_batch_size):
                batch_end = min(batch_start + effective_batch_size, len(pk_values_list))
                batch_pks = pk_values_list[batch_start:batch_end]

                if len(pk_columns) == 1:
                    col = pk_columns[0]
                    values = [v[0] for v in batch_pks]
                    placeholders = ", ".join(["%s"] * len(values))
                    query = f'SELECT * FROM "{table}" WHERE "{col}" IN ({placeholders})'
                    params = values
                else:
                    # Composite PK - use OR of AND conditions
                    conditions = []
                    params = []
                    for pk_tuple in batch_pks:
                        cond_parts = [f'"{col}" = %s' for col in pk_columns]
                        conditions.append(f"({' AND '.join(cond_parts)})")
                        params.extend(pk_tuple)
                    query = f'SELECT * FROM "{table}" WHERE {" OR ".join(conditions)}'

                # Named cursor enables server-side streaming to avoid loading entire result set into memory
                cursor_name = f"dbslice_stream_{table}_{batch_start}_{id(batch_pks)}"
                with self._conn.cursor(
                    name=cursor_name, cursor_factory=psycopg2.extras.RealDictCursor
                ) as cur:
                    cur.itersize = chunk_size
                    cur.execute(query, params)

                    while True:
                        rows = cur.fetchmany(chunk_size)
                        if not rows:
                            break
                        yield [dict(row) for row in rows]

        except psycopg2.Error as e:
            raise ExtractionError(
                f"Failed to fetch rows by primary key (chunked) from table '{table}': {e}",
                table=table,
            ) from e

    def fetch_fk_values(
        self,
        table: str,
        fk: ForeignKey,
        source_pk_values: set[tuple[Any, ...]],
    ) -> set[tuple[Any, ...]]:
        """
        Fetch FK column values for given source PKs with batching.

        This method implements query batching to avoid hitting PostgreSQL's
        parameter limit (~32K) when dealing with large PK sets.
        """
        if not source_pk_values:
            return set()

        schema = self.get_schema()
        source_table = schema.get_table(table)
        if not source_table:
            return set()

        pk_cols = source_table.primary_key
        fk_cols = fk.source_columns

        params_per_row = len(pk_cols)
        effective_batch_size = self.batch_size // max(params_per_row, 1)

        result: set[tuple[Any, ...]] = set()
        pk_values_list = list(source_pk_values)

        for batch_start in range(0, len(pk_values_list), effective_batch_size):
            batch_end = min(batch_start + effective_batch_size, len(pk_values_list))
            batch_pks = pk_values_list[batch_start:batch_end]

            if len(pk_cols) == 1:
                pk_col = pk_cols[0]
                pk_vals = [v[0] for v in batch_pks]
                placeholders = ", ".join(["%s"] * len(pk_vals))
                fk_select = ", ".join(f'"{c}"' for c in fk_cols)
                query = f'SELECT DISTINCT {fk_select} FROM "{table}" WHERE "{pk_col}" IN ({placeholders})'
                params = pk_vals
            else:
                conditions = []
                params = []
                for pk_tuple in batch_pks:
                    cond_parts = [f'"{col}" = %s' for col in pk_cols]
                    conditions.append(f"({' AND '.join(cond_parts)})")
                    params.extend(pk_tuple)
                fk_select = ", ".join(f'"{c}"' for c in fk_cols)
                query = (
                    f'SELECT DISTINCT {fk_select} FROM "{table}" WHERE {" OR ".join(conditions)}'
                )

            if self.profiler:
                with self.profiler.track_query(
                    query, len(params), table=table, operation="fetch_fk_values"
                ) as tracker:
                    with self._conn.cursor() as cur:
                        cur.execute(query, params)
                        rows = cur.fetchall()
                        for row in rows:
                            # Filter out NULL values (nullable FKs)
                            if None not in row:
                                result.add(row)
                        tracker.record_rows(len(rows))
            else:
                with self._conn.cursor() as cur:
                    cur.execute(query, params)
                    for row in cur.fetchall():
                        # Filter out NULL values (nullable FKs)
                        if None not in row:
                            result.add(row)

        logger.debug(
            "Fetched FK values with batching",
            table=table,
            fk=fk.name,
            input_pks=len(source_pk_values),
            output_fks=len(result),
            batches=((len(pk_values_list) - 1) // effective_batch_size) + 1
            if pk_values_list
            else 0,
        )

        return result

    def fetch_referencing_pks(
        self,
        fk: ForeignKey,
        target_pk_values: set[tuple[Any, ...]],
    ) -> set[tuple[Any, ...]]:
        """
        Fetch PKs of rows that reference the given target PKs with batching.

        This method implements query batching to avoid hitting PostgreSQL's
        parameter limit (~32K) when dealing with large PK sets.
        """
        if not target_pk_values:
            return set()

        source_table = fk.source_table
        fk_cols = fk.source_columns

        schema = self.get_schema()
        table_info = schema.get_table(source_table)
        if not table_info:
            return set()

        pk_cols = table_info.primary_key

        if not pk_cols:
            logger.debug(
                "Skipping table without primary key",
                table=source_table,
                fk=fk.name,
            )
            return set()

        params_per_row = len(fk_cols)
        effective_batch_size = self.batch_size // max(params_per_row, 1)

        result: set[tuple[Any, ...]] = set()
        pk_values_list = list(target_pk_values)

        pk_select = ", ".join(f'"{c}"' for c in pk_cols)

        for batch_start in range(0, len(pk_values_list), effective_batch_size):
            batch_end = min(batch_start + effective_batch_size, len(pk_values_list))
            batch_pks = pk_values_list[batch_start:batch_end]

            if len(fk_cols) == 1:
                fk_col = fk_cols[0]
                fk_vals = [v[0] for v in batch_pks]
                placeholders = ", ".join(["%s"] * len(fk_vals))
                query = f'SELECT DISTINCT {pk_select} FROM "{source_table}" WHERE "{fk_col}" IN ({placeholders})'
                params = fk_vals
            else:
                conditions = []
                params = []
                for fk_tuple in batch_pks:
                    cond_parts = [f'"{col}" = %s' for col in fk_cols]
                    conditions.append(f"({' AND '.join(cond_parts)})")
                    params.extend(fk_tuple)
                query = f'SELECT DISTINCT {pk_select} FROM "{source_table}" WHERE {" OR ".join(conditions)}'

            if self.profiler:
                with self.profiler.track_query(
                    query, len(params), table=source_table, operation="fetch_referencing_pks"
                ) as tracker:
                    with self._conn.cursor() as cur:
                        cur.execute(query, params)
                        rows = cur.fetchall()
                        for row in rows:
                            result.add(row)
                        tracker.record_rows(len(rows))
            else:
                with self._conn.cursor() as cur:
                    cur.execute(query, params)
                    for row in cur.fetchall():
                        result.add(row)

        logger.debug(
            "Fetched referencing PKs with batching",
            source_table=source_table,
            fk=fk.name,
            input_target_pks=len(target_pk_values),
            output_source_pks=len(result),
            batches=((len(pk_values_list) - 1) // effective_batch_size) + 1
            if pk_values_list
            else 0,
        )

        return result

    def fetch_all_pks(
        self,
        table: str,
        pk_columns: tuple[str, ...],
    ) -> set[tuple[Any, ...]]:
        """
        Fetch ALL primary keys from a table.

        This is used for passthrough tables that should be included in full,
        regardless of FK relationships.

        Args:
            table: Table name
            pk_columns: Names of primary key columns

        Returns:
            Set of all PK value tuples in the table
        """
        if not pk_columns:
            return set()

        try:
            pk_select = ", ".join(f'"{c}"' for c in pk_columns)
            query = f'SELECT {pk_select} FROM "{table}"'

            logger.debug(
                "Fetching all PKs for passthrough table",
                table=table,
                pk_columns=pk_columns,
            )

            if self.profiler:
                with self.profiler.track_query(
                    query, 0, table=table, operation="fetch_all_pks"
                ) as tracker:
                    with self._conn.cursor() as cur:
                        cur.execute(query)
                        rows = cur.fetchall()
                        result = set(rows)
                        tracker.record_rows(len(result))
                        logger.info(
                            "Fetched all PKs for passthrough table",
                            table=table,
                            count=len(result),
                        )
                        return result
            else:
                with self._conn.cursor() as cur:
                    cur.execute(query)
                    result = set(cur.fetchall())
                    logger.info(
                        "Fetched all PKs for passthrough table",
                        table=table,
                        count=len(result),
                    )
                    return result
        except psycopg2.Error as e:
            raise ExtractionError(
                f"Failed to fetch all PKs from passthrough table '{table}': {e}", table=table
            ) from e

    def get_table_pk_columns(self, table: str) -> tuple[str, ...]:
        """Get primary key column names for a table."""
        schema = self.get_schema()
        table_info = schema.get_table(table)
        if table_info:
            return table_info.primary_key
        return ()

    def begin_snapshot(self) -> None:
        """Begin a snapshot transaction with REPEATABLE READ isolation."""
        if self._conn:
            self._conn.autocommit = False
            with self._conn.cursor() as cur:
                cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")

    def end_snapshot(self) -> None:
        """End the snapshot transaction."""
        if self._conn:
            self._conn.rollback()  # Read-only, so rollback is fine
            self._conn.autocommit = True
