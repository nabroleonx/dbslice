import time
from typing import Any, TextIO

from dbslice.adapters.base import DatabaseAdapter
from dbslice.config import DatabaseType, ExtractConfig
from dbslice.core.engine import ExtractionResult, ProgressCallback
from dbslice.logging import get_logger
from dbslice.models import SchemaGraph, Table
from dbslice.output.sql import SQLGenerator
from dbslice.utils.anonymizer import DeterministicAnonymizer

logger = get_logger(__name__)


class StreamingExtractionEngine:
    """
    Streaming extraction engine that writes data directly to output file.

    This engine is designed for large datasets (>50K rows) where loading
    all data into memory would cause OOM errors. It processes tables one
    at a time in topological order, writing SQL directly to the output file
    as data is fetched.

    Key features:
    - Constant memory footprint regardless of dataset size
    - Supports anonymization during streaming
    - Handles circular dependencies with deferred updates
    - Progress tracking and logging
    """

    def __init__(
        self,
        config: ExtractConfig,
        adapter: DatabaseAdapter,
        schema: SchemaGraph,
        records: dict[str, set[tuple[Any, ...]]],
        insert_order: list[str],
        broken_fks: list[Any],
        deferred_updates: list[Any],
        db_type: DatabaseType,
        progress_callback: ProgressCallback | None = None,
        chunk_size: int = 1000,
    ):
        """
        Initialize streaming engine.

        Args:
            config: Extraction configuration
            adapter: Connected database adapter
            schema: Database schema
            records: Map of table -> set of PK tuples to extract
            insert_order: Topologically sorted table names
            broken_fks: Foreign keys broken to handle cycles
            deferred_updates: Deferred update statements for cycles
            db_type: Database type for SQL generation
            progress_callback: Optional progress callback
            chunk_size: Number of rows to fetch per chunk (default: 1000)
        """
        self.config = config
        self.adapter = adapter
        self.schema = schema
        self.records = records
        self.insert_order = insert_order
        self.broken_fks = broken_fks
        self.deferred_updates = deferred_updates
        self.db_type = db_type
        self.progress_callback = progress_callback
        self.chunk_size = chunk_size

        # Initialize anonymizer if needed
        self.anonymizer: DeterministicAnonymizer | None = None
        if config.anonymize or config.redact_fields:
            self.anonymizer = DeterministicAnonymizer()
            self.anonymizer.schema = schema
            if config.redact_fields:
                self.anonymizer.configure(config.redact_fields)

        self.sql_generator = SQLGenerator(
            db_type=db_type,
            include_transaction=True,
            include_truncate=False,
            disable_fk_checks=False,
        )

        self.stats: dict[str, int] = {}

    def _log(self, stage: str, message: str, current: int = 0, total: int = 0) -> None:
        """Send progress update to callback if configured."""
        if self.progress_callback:
            self.progress_callback(stage, message, current, total)

    def stream_to_file(self, output_file: str) -> ExtractionResult:
        """
        Stream extraction results directly to output file.

        This method processes tables one at a time in topological order,
        fetching data in chunks and writing SQL statements immediately.
        Memory usage stays bounded regardless of dataset size.

        Args:
            output_file: Path to output SQL file

        Returns:
            ExtractionResult with statistics (but empty tables dict)
        """
        start_time = time.time()
        total_tables = len([t for t in self.insert_order if t in self.records])

        logger.info(
            "Starting streaming extraction",
            output_file=output_file,
            table_count=total_tables,
            chunk_size=self.chunk_size,
        )

        self._log("stream", f"Streaming to {output_file}...")

        broken_fk_cols = self._build_broken_fk_map()

        try:
            with open(output_file, "w") as f:
                # Write header
                self._write_header(f, total_tables)

                current_table_idx = 0
                for table in self.insert_order:
                    if table not in self.records:
                        continue

                    current_table_idx += 1
                    pk_values = self.records[table]

                    if not pk_values:
                        continue

                    table_info = self.schema.get_table(table)
                    if not table_info:
                        continue

                    self._log(
                        "stream",
                        f"Streaming {table} ({len(pk_values)} rows)",
                        current_table_idx,
                        total_tables,
                    )

                    row_count = self._stream_table(
                        f, table, table_info, pk_values, broken_fk_cols.get(table)
                    )
                    self.stats[table] = row_count

                    logger.debug(
                        "Table streamed",
                        table=table,
                        row_count=row_count,
                    )

                if self.deferred_updates:
                    self._write_deferred_updates(f)

                self._write_footer(f)

        except Exception as e:
            logger.error(
                "Streaming extraction failed",
                error=str(e),
                output_file=output_file,
                exc_info=True,
            )
            raise

        # Log completion
        elapsed_ms = int((time.time() - start_time) * 1000)
        total_rows = sum(self.stats.values())
        logger.info(
            "Streaming extraction complete",
            total_rows=total_rows,
            table_count=len(self.stats),
            output_file=output_file,
            duration_ms=elapsed_ms,
        )

        self._log(
            "complete",
            f"Streamed {total_rows} rows from {len(self.stats)} tables to {output_file}",
        )

        # Return result (with empty tables since we streamed to file)
        return ExtractionResult(
            tables={},  # Empty - data was written to file
            insert_order=self.insert_order,
            stats=self.stats,
            traversal_path=[],
            has_cycles=len(self.broken_fks) > 0,
            broken_fks=self.broken_fks,
            deferred_updates=self.deferred_updates,
            cycle_infos=[],
        )

    def _stream_table(
        self,
        f: TextIO,
        table: str,
        table_info: Table,
        pk_values: set[tuple[Any, ...]],
        null_columns: set[str] | None,
    ) -> int:
        """
        Stream a single table's data to output file.

        Args:
            f: Output file handle
            table: Table name
            table_info: Table schema information
            pk_values: Set of PK tuples to fetch
            null_columns: Columns to set to NULL (for breaking cycles)

        Returns:
            Number of rows written
        """
        f.write(f"-- {table} ({len(pk_values)} rows)\n")

        pk_columns = table_info.primary_key
        row_count = 0

        with logger.timed_operation(
            "stream_table_data",
            table=table,
            row_count=len(pk_values),
        ):
            for chunk in self.adapter.fetch_by_pk_chunked(
                table, pk_columns, pk_values, self.chunk_size
            ):
                # Anonymize chunk if needed
                if self.anonymizer:
                    chunk = [self.anonymizer.anonymize_row(table, row) for row in chunk]

                for row in chunk:
                    insert_stmt = self.sql_generator._generate_insert(
                        table, row, table_info, null_columns
                    )
                    f.write(insert_stmt + "\n")
                    row_count += 1

        f.write("\n")
        return row_count

    def _write_header(self, f: TextIO, table_count: int) -> None:
        """Write SQL file header."""
        f.write("-- Generated by dbslice (streaming mode)\n")
        f.write(f"-- Tables: {table_count}\n")
        if self.broken_fks:
            f.write(f"-- Circular references detected: {len(self.broken_fks)} FK(s) broken\n")
        f.write("\n")
        f.write("BEGIN;\n")
        f.write("\n")

    def _write_footer(self, f: TextIO) -> None:
        """Write SQL file footer."""
        f.write("COMMIT;\n")

    def _write_deferred_updates(self, f: TextIO) -> None:
        """Write deferred UPDATE statements for broken FKs."""
        f.write("-- Restore circular foreign key references\n")
        for update in self.deferred_updates:
            update_stmt = self.sql_generator._generate_deferred_update(update, self.schema.tables)
            f.write(update_stmt + "\n")
        f.write("\n")

    def _build_broken_fk_map(self) -> dict[str, set[str]]:
        """
        Build a map of table -> set of FK column names that were broken.

        Returns:
            Dict mapping table name to set of FK column names
        """
        broken_fk_cols: dict[str, set[str]] = {}

        for fk in self.broken_fks:
            table = fk.source_table
            if table not in broken_fk_cols:
                broken_fk_cols[table] = set()

            for col in fk.source_columns:
                broken_fk_cols[table].add(col)

        return broken_fk_cols
