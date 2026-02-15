import time
from collections.abc import Callable
from dataclasses import dataclass, field
from graphlib import CycleError, TopologicalSorter
from typing import Any

from dbslice.adapters.base import DatabaseAdapter
from dbslice.config import (
    DatabaseType,
    ExtractConfig,
    SeedSpec,
    TraversalDirection,
)
from dbslice.constants import DEFAULT_TRAVERSAL_DEPTH
from dbslice.core.graph import GraphTraverser, TraversalConfig, TraversalResult
from dbslice.exceptions import (
    NoRowsFoundError,
    TableNotFoundError,
)
from dbslice.logging import get_logger
from dbslice.models import SchemaGraph
from dbslice.output.sql import SQLGenerator
from dbslice.utils.anonymizer import DeterministicAnonymizer
from dbslice.utils.connection import get_adapter_for_url, parse_database_url
from dbslice.validation import ExtractionValidator, ValidationResult

logger = get_logger(__name__)

# Type alias for progress callback functions.
#
# Progress callbacks are called during extraction to report status updates.
# Signature: (stage: str, message: str, current: int, total: int) -> None
#
# Args:
#     stage: Current extraction stage (e.g., "schema", "fetch", "sort")
#     message: Human-readable status message
#     current: Current item number (0 if not applicable)
#     total: Total items (0 if not applicable)
ProgressCallback = Callable[[str, str, int, int], None]


@dataclass
class ExtractedRow:
    """A single extracted record."""

    table: str
    pk_values: tuple[Any, ...]
    data: dict[str, Any]


@dataclass
class ExtractionResult:
    """
    Complete extraction result with data, metadata, and cycle information.

    Attributes:
        tables: Mapping of table names to lists of row data dictionaries
        insert_order: Topologically sorted table names for safe INSERT order
        stats: Row count statistics per table
        traversal_path: List of traversal steps for debugging (verbose mode)
        has_cycles: Whether circular FK dependencies were detected
        broken_fks: List of ForeignKey objects that were broken to resolve cycles
        deferred_updates: List of DeferredUpdate objects to restore broken FK values
        cycle_infos: List of CycleInfo objects describing detected cycles
        validation_result: Result of extraction validation (None if validation skipped)
    """

    tables: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    insert_order: list[str] = field(default_factory=list)
    stats: dict[str, int] = field(default_factory=dict)
    traversal_path: list[str] = field(default_factory=list)
    has_cycles: bool = False
    broken_fks: list[Any] = field(default_factory=list)  # list[ForeignKey]
    deferred_updates: list[Any] = field(default_factory=list)  # list[DeferredUpdate]
    cycle_infos: list[Any] = field(default_factory=list)  # list[CycleInfo]
    validation_result: ValidationResult | None = None
    profiler: Any = None  # Optional QueryProfiler

    def total_rows(self) -> int:
        """Get total number of extracted rows."""
        if self.tables:
            return sum(len(rows) for rows in self.tables.values())
        # Streaming/dry-run mode: tables is empty, use stats
        return sum(self.stats.values())

    def table_count(self) -> int:
        """Get number of tables with data."""
        if self.tables:
            return len(self.tables)
        # Streaming/dry-run mode: tables is empty, use stats
        return len(self.stats)


class ExtractionEngine:
    """
    Main extraction engine that orchestrates the extraction process.

    Flow:
    1. Connect to database
    2. Introspect schema
    3. Parse seeds and fetch seed rows
    4. Traverse FK graph to find related records
    5. Fetch all identified rows
    6. Topologically sort tables
    7. Return result for output generation
    """

    def __init__(
        self,
        config: ExtractConfig,
        progress_callback: ProgressCallback | None = None,
    ):
        self.config = config
        self.adapter: DatabaseAdapter | None = None
        self.schema: SchemaGraph | None = None
        self.progress_callback = progress_callback

        # Initialize anonymizer if needed (schema will be set after introspection)
        self.anonymizer: DeterministicAnonymizer | None = None
        if config.anonymize or config.redact_fields:
            self.anonymizer = DeterministicAnonymizer()
            if config.redact_fields:
                self.anonymizer.configure(config.redact_fields)

    def _log(self, stage: str, message: str, current: int = 0, total: int = 0) -> None:
        """Send progress update to callback if configured."""
        if self.progress_callback:
            self.progress_callback(stage, message, current, total)

    def extract(self) -> tuple[ExtractionResult, SchemaGraph]:
        """
        Perform the extraction.

        Returns:
            Tuple of (ExtractionResult, SchemaGraph) - schema is returned
            to avoid needing to reconnect for SQL generation.
        """
        start_time = time.time()

        db_config = parse_database_url(self.config.database_url)
        logger.info(
            "Starting extraction",
            database=db_config.database,
            db_type=db_config.db_type.value,
            seed_count=len(self.config.seeds),
            depth=self.config.depth,
            direction=self.config.direction.value,
        )

        profiler = None
        if self.config.profile:
            from dbslice.utils.profiling import QueryProfiler

            profiler = QueryProfiler()
            logger.info("Query profiling enabled")

        # Get appropriate adapter with profiler if available
        from dbslice.adapters.postgresql import PostgreSQLAdapter

        if db_config.db_type.value == "postgresql":
            self.adapter = PostgreSQLAdapter(profiler=profiler)
        else:
            self.adapter = get_adapter_for_url(self.config.database_url)

        try:
            with logger.timed_operation("database_connection", database=db_config.database):
                self.adapter.connect(self.config.database_url)
            self._log("connect", "Connected successfully")

            # Use snapshot transaction for consistent reads
            with self.adapter.snapshot_transaction():
                result = self._do_extract(db_config.db_type)

                # Log extraction summary
                elapsed_ms = int((time.time() - start_time) * 1000)
                logger.info(
                    "Extraction completed successfully",
                    total_rows=result.total_rows(),
                    table_count=result.table_count(),
                    duration_ms=elapsed_ms,
                    has_cycles=result.has_cycles,
                )

                if profiler:
                    result.profiler = profiler

                # Return schema along with result to avoid reconnection
                assert self.schema is not None
                return result, self.schema
        except Exception as e:
            elapsed_ms = int((time.time() - start_time) * 1000)
            logger.error(
                "Extraction failed",
                error=str(e),
                duration_ms=elapsed_ms,
                exc_info=True,
            )
            raise
        finally:
            if self.adapter:
                self.adapter.close()
                logger.debug("Database connection closed")

    def _do_extract(self, db_type: DatabaseType) -> ExtractionResult:
        """Perform extraction within a snapshot transaction."""
        assert self.adapter is not None

        self._log("schema", "Introspecting database schema...")
        with logger.timed_operation("schema_introspection"):
            self.schema = self.adapter.get_schema()

        for vfk in self.config.virtual_foreign_keys:
            self.schema.add_virtual_fk(vfk)

        logger.info(
            "Schema introspection complete",
            table_count=len(self.schema.tables),
            fk_count=len(self.schema.edges),
            virtual_fk_count=len(self.schema.virtual_edges),
        )
        self._log(
            "schema",
            f"Found {len(self.schema.tables)} tables, {len(self.schema.edges)} foreign keys"
            + (
                f", {len(self.schema.virtual_edges)} virtual foreign keys"
                if self.schema.virtual_edges
                else ""
            ),
        )

        if self.anonymizer:
            self.anonymizer.schema = self.schema
            logger.debug("Anonymizer configured with schema")

        all_records: dict[str, set[tuple[Any, ...]]] = {}
        all_paths: list[str] = []

        total_seeds = len(self.config.seeds)
        for i, seed in enumerate(self.config.seeds):
            seed_desc = (
                f"{seed.table}.{seed.column}={seed.value}"
                if seed.column
                else f"{seed.table}:{seed.where_clause}"
            )
            self._log("seed", f"Processing seed: {seed_desc}", i + 1, total_seeds)
            logger.debug("Processing seed", seed=seed_desc, index=i + 1, total=total_seeds)

            traversal_result = self._process_seed(seed)
            all_paths.extend(traversal_result.traversal_path)

            for table, pks in traversal_result.records.items():
                if table not in all_records:
                    all_records[table] = set()
                all_records[table].update(pks)

            logger.info(
                "Seed traversal complete",
                seed=seed_desc,
                records_found=traversal_result.total_records(),
                tables_affected=traversal_result.table_count(),
            )
            self._log(
                "seed",
                f"Found {traversal_result.total_records()} records across {traversal_result.table_count()} tables",
                i + 1,
                total_seeds,
            )

        self._log("sort", "Sorting tables by dependencies...")
        with logger.timed_operation("topological_sort", table_count=len(all_records)):
            insert_order, broken_fks, cycle_infos = self._topological_sort(set(all_records.keys()))

        if broken_fks:
            logger.warning(
                "Circular dependencies detected",
                broken_fk_count=len(broken_fks),
                cycle_count=len(cycle_infos),
            )

        # Dry-run mode: skip data fetch and return stats only
        if self.config.dry_run:
            dry_run_stats = {table: len(pks) for table, pks in all_records.items()}
            total_rows_estimate = sum(dry_run_stats.values())

            logger.info(
                "Dry-run complete",
                total_rows=total_rows_estimate,
                table_count=len(all_records),
            )
            self._log(
                "dry_run",
                f"Dry-run summary: {total_rows_estimate} rows across {len(all_records)} tables would be extracted",
            )

            for table in insert_order:
                if table in dry_run_stats:
                    self._log("dry_run", f"  {table}: {dry_run_stats[table]} rows")

            if all_paths:
                self._log("dry_run", "Traversal path:")
                for path_entry in all_paths:
                    self._log("dry_run", f"  {path_entry}")

            if cycle_infos:
                self._log("dry_run", f"Detected {len(cycle_infos)} circular dependency cycle(s)")

            return ExtractionResult(
                tables={},
                insert_order=insert_order,
                stats=dry_run_stats,
                traversal_path=all_paths,
                has_cycles=len(broken_fks) > 0,
                broken_fks=broken_fks,
                deferred_updates=[],
                cycle_infos=cycle_infos,
            )

        total_rows_estimate = sum(len(pks) for pks in all_records.values())
        use_streaming = self._should_use_streaming(total_rows_estimate)

        if use_streaming:
            if not self.config.output_file:
                raise ValueError("Streaming mode requires --out-file to be specified")

            logger.info(
                "Using streaming mode",
                estimated_rows=total_rows_estimate,
                threshold=self.config.streaming_threshold,
                output_file=self.config.output_file,
            )
            self._log(
                "stream",
                f"Using streaming mode for {total_rows_estimate} rows (threshold: {self.config.streaming_threshold})",
            )

            return self._do_streaming_extract(
                db_type, all_records, insert_order, broken_fks, cycle_infos, all_paths
            )

        logger.info(
            "Using in-memory mode",
            estimated_rows=total_rows_estimate,
            threshold=self.config.streaming_threshold,
        )

        # Fetch all row data
        self._log("fetch", f"Fetching data from {len(all_records)} tables...")
        logger.info("Starting data fetch phase", table_count=len(all_records))

        tables_data: dict[str, list[dict[str, Any]]] = {}
        stats: dict[str, int] = {}

        total_tables = len(all_records)
        for i, (table, pk_values) in enumerate(all_records.items()):
            if not pk_values:
                continue

            table_info = self.schema.get_table(table)
            if not table_info:
                continue

            self._log("fetch", f"Fetching {len(pk_values)} rows from {table}", i + 1, total_tables)

            pk_columns = table_info.primary_key
            with logger.timed_operation(
                "fetch_table_data",
                table=table,
                row_count=len(pk_values),
            ):
                rows = list(self.adapter.fetch_by_pk(table, pk_columns, pk_values))

            # Anonymize if enabled
            if self.anonymizer:
                with logger.timed_operation("anonymize_table_data", table=table):
                    rows = self._anonymize_table_data(table, rows)
                logger.debug("Table data anonymized", table=table, row_count=len(rows))

            tables_data[table] = rows
            stats[table] = len(rows)
            logger.debug("Table data fetched", table=table, row_count=len(rows))

        deferred_updates = []
        if broken_fks:
            from dbslice.core.cycles import build_deferred_updates

            self._log("cycles", f"Breaking {len(broken_fks)} circular reference(s)...")
            with logger.timed_operation("build_deferred_updates"):
                deferred_updates = build_deferred_updates(broken_fks, tables_data, self.schema)

            logger.info(
                "Circular references resolved",
                broken_fks=len(broken_fks),
                deferred_updates=len(deferred_updates),
            )
            self._log("cycles", f"Generated {len(deferred_updates)} deferred UPDATE(s)")

        total_rows = sum(len(rows) for rows in tables_data.values())
        self._log(
            "complete", f"Extraction complete: {total_rows} rows from {len(tables_data)} tables"
        )

        validation_result = None
        if self.config.validate:
            self._log("validate", "Validating extraction for referential integrity...")
            validator = ExtractionValidator(self.schema)

            with logger.timed_operation("validate_extraction"):
                validation_result = validator.validate(tables_data, broken_fks)

            if validation_result.is_valid:
                logger.info(
                    "Validation passed",
                    records_checked=validation_result.total_records_checked,
                    fk_checks=validation_result.total_fk_checks,
                )
                self._log("validate", "Validation passed: all FK references are intact")
            else:
                logger.warning(
                    "Validation failed",
                    orphaned_records=len(validation_result.orphaned_records),
                    records_checked=validation_result.total_records_checked,
                )
                self._log(
                    "validate",
                    f"Validation failed: {len(validation_result.orphaned_records)} orphaned record(s) found",
                )

                if self.config.fail_on_validation_error:
                    from dbslice.exceptions import ExtractionError

                    error_msg = (
                        f"Extraction validation failed: {len(validation_result.orphaned_records)} "
                        f"orphaned record(s) detected. These records have foreign key references to "
                        f"parent records that are not included in the extraction.\n\n"
                        f"{validation_result.format_report()}"
                    )
                    raise ExtractionError(error_msg)

        return ExtractionResult(
            tables=tables_data,
            insert_order=insert_order,
            stats=stats,
            traversal_path=all_paths,
            has_cycles=len(broken_fks) > 0,
            broken_fks=broken_fks,
            deferred_updates=deferred_updates,
            cycle_infos=cycle_infos,
            validation_result=validation_result,
        )

    def _anonymize_table_data(self, table: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Anonymize sensitive fields in extracted rows.

        This is called after data fetching but before SQL generation,
        ensuring anonymization happens once and consistently.

        Args:
            table: Table name
            rows: List of row dictionaries

        Returns:
            List of anonymized row dictionaries
        """
        if not self.anonymizer:
            return rows

        return [self.anonymizer.anonymize_row(table, row) for row in rows]

    def _process_seed(self, seed: SeedSpec) -> TraversalResult:
        """Process a single seed and return traversal result."""
        assert self.schema is not None
        assert self.adapter is not None

        if not self.schema.has_table(seed.table):
            raise TableNotFoundError(seed.table, self.schema.get_table_names())

        table_info = self.schema.get_table(seed.table)
        assert table_info is not None  # Guaranteed by has_table check above
        pk_columns = table_info.primary_key

        where_clause, params = seed.to_where_clause()
        seed_rows = list(self.adapter.fetch_rows(seed.table, where_clause, params))

        if not seed_rows:
            seed_str = (
                f"{seed.table}.{seed.column}={seed.value}"
                if seed.column
                else f"{seed.table}:{seed.where_clause}"
            )
            raise NoRowsFoundError(seed_str, seed.table)

        seed_pks: set[tuple[Any, ...]] = set()
        for row in seed_rows:
            pk_tuple = tuple(row[col] for col in pk_columns)
            seed_pks.add(pk_tuple)

        traversal_config = TraversalConfig(
            max_depth=self.config.depth,
            direction=self.config.direction,
            exclude_tables=self.config.exclude_tables,
            passthrough_tables=self.config.passthrough_tables,
        )

        traverser = GraphTraverser(self.schema, self.adapter)
        return traverser.traverse(seed.table, seed_pks, traversal_config)

    def _should_use_streaming(self, total_rows: int) -> bool:
        """
        Determine whether to use streaming mode based on configuration and row count.

        Args:
            total_rows: Estimated total number of rows to extract

        Returns:
            True if streaming mode should be used, False otherwise
        """
        # Force streaming if explicitly enabled
        if self.config.stream:
            logger.debug("Streaming enabled via --stream flag")
            return True

        # Auto-enable streaming if above threshold and output file is specified
        if total_rows >= self.config.streaming_threshold and self.config.output_file:
            logger.debug(
                "Auto-enabling streaming mode",
                total_rows=total_rows,
                threshold=self.config.streaming_threshold,
            )
            return True

        return False

    def _do_streaming_extract(
        self,
        db_type: DatabaseType,
        all_records: dict[str, set[tuple[Any, ...]]],
        insert_order: list[str],
        broken_fks: list[Any],
        cycle_infos: list[Any],
        all_paths: list[str],
    ) -> ExtractionResult:
        """
        Perform streaming extraction to file.

        This method writes extraction results directly to a file in chunks,
        avoiding loading the entire dataset into memory.
        Perform streaming extraction that writes directly to output file.

        This method is called when the dataset is large enough to warrant
        streaming mode (avoiding OOM errors).

        Args:
            db_type: Database type
            all_records: Map of table -> set of PK tuples to extract
            insert_order: Topologically sorted table names
            broken_fks: Foreign keys broken to handle cycles
            cycle_infos: Information about detected cycles
            all_paths: Traversal paths (for verbose mode)

        Returns:
            ExtractionResult with statistics (but empty tables dict)
        """
        assert self.schema is not None
        assert self.adapter is not None

        from dbslice.core.streaming import StreamingExtractionEngine

        deferred_updates = []
        if broken_fks:
            from dbslice.core.cycles import build_deferred_updates

            self._log("cycles", f"Breaking {len(broken_fks)} circular reference(s)...")

            # For streaming mode, we need to build deferred updates without having
            # all data in memory. We fetch the necessary data on-demand.
            with logger.timed_operation("build_deferred_updates_streaming"):
                temp_data = {}
                for fk in broken_fks:
                    table = fk.source_table
                    if table not in temp_data and table in all_records:
                        pk_values = all_records[table]
                        table_info = self.schema.get_table(table)
                        if table_info:
                            pk_columns = table_info.primary_key
                            rows = list(self.adapter.fetch_by_pk(table, pk_columns, pk_values))
                            temp_data[table] = rows

                deferred_updates = build_deferred_updates(broken_fks, temp_data, self.schema)

            logger.info(
                "Circular references resolved",
                broken_fks=len(broken_fks),
                deferred_updates=len(deferred_updates),
            )
            self._log("cycles", f"Generated {len(deferred_updates)} deferred UPDATE(s)")

        streaming_engine = StreamingExtractionEngine(
            config=self.config,
            adapter=self.adapter,
            schema=self.schema,
            records=all_records,
            insert_order=insert_order,
            broken_fks=broken_fks,
            deferred_updates=deferred_updates,
            db_type=db_type,
            progress_callback=self.progress_callback,
            chunk_size=self.config.streaming_chunk_size,
        )

        assert self.config.output_file is not None
        result = streaming_engine.stream_to_file(self.config.output_file)

        result.traversal_path = all_paths
        result.cycle_infos = cycle_infos

        return result

    def _topological_sort(self, tables: set[str]) -> tuple[list[str], list[Any], list[Any]]:
        """
        Topologically sort tables based on FK dependencies with cycle handling.

        Tables with no dependencies come first (parents before children).
        If cycles are detected, breaks them at nullable foreign keys.

        Returns:
            Tuple of (insert_order, broken_fks, cycle_infos)
        """
        assert self.schema is not None

        from dbslice.core.cycles import break_cycles_at_nullable_fks

        dependencies: dict[str, set[str]] = {t: set() for t in tables}

        for fk in self.schema.edges:
            if fk.source_table in tables and fk.target_table in tables:
                dependencies[fk.source_table].add(fk.target_table)

        ts = TopologicalSorter(dependencies)

        try:
            insert_order = list(ts.static_order())
            return insert_order, [], []
        except CycleError:
            try:
                fks_to_break, cycle_infos = break_cycles_at_nullable_fks(
                    self.schema, tables, dependencies
                )
            except ValueError as e:
                # No nullable FK found to break cycle
                from dbslice.exceptions import CircularReferenceError

                raise CircularReferenceError(str(e))

            modified_deps = {t: set(deps) for t, deps in dependencies.items()}
            for fk in fks_to_break:
                if fk.source_table in modified_deps:
                    modified_deps[fk.source_table].discard(fk.target_table)

            ts = TopologicalSorter(modified_deps)
            insert_order = list(ts.static_order())

            return insert_order, fks_to_break, cycle_infos


def extract_subset(
    database_url: str,
    seed: str,
    depth: int = DEFAULT_TRAVERSAL_DEPTH,
    direction: str = "up",
) -> str:
    """
    Convenience function to extract a database subset as SQL.

    Args:
        database_url: Database connection URL
        seed: Seed specification (e.g., "orders.id=12345")
        depth: Maximum FK traversal depth
        direction: Traversal direction ("up", "down", "both")

    Returns:
        SQL string with INSERT statements
    """
    seed_spec = SeedSpec.parse(seed)
    direction_enum = TraversalDirection(direction)
    config = ExtractConfig(
        database_url=database_url,
        seeds=[seed_spec],
        depth=depth,
        direction=direction_enum,
    )

    engine = ExtractionEngine(config)
    result, schema = engine.extract()

    db_config = parse_database_url(database_url)
    generator = SQLGenerator(db_type=db_config.db_type)
    return generator.generate(
        result.tables,
        result.insert_order,
        schema.tables,
    )
