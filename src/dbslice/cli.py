from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.status import Status

from dbslice import __version__
from dbslice.config import ExtractConfig, OutputFormat, SeedSpec, TraversalDirection
from dbslice.constants import DEFAULT_TRAVERSAL_DEPTH
from dbslice.core.engine import ExtractionEngine
from dbslice.exceptions import (
    CircularReferenceError,
    ConnectionError,
    DbsliceError,
    InvalidSeedError,
    NoRowsFoundError,
    TableNotFoundError,
)
from dbslice.input_validators import (
    ValidationError,
    validate_database_url,
    validate_depth,
    validate_exclude_tables,
    validate_output_file_path,
    validate_redact_fields,
)
from dbslice.logging import get_logger, setup_logging
from dbslice.output.csv_out import CSVGenerator
from dbslice.output.json_out import JSONGenerator
from dbslice.output.sql import SQLGenerator
from dbslice.utils.connection import parse_database_url

logger = get_logger(__name__)

app = typer.Typer(
    name="dbslice",
    help="Extract minimal, referentially-intact database subsets.",
    no_args_is_help=True,
)

console = Console(stderr=True)
# Use soft_wrap=True and large width to prevent line wrapping for data output
stdout_console = Console(soft_wrap=True, width=1000000)


def version_callback(value: bool):
    """Print version and exit."""
    if value:
        print(f"dbslice {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: Annotated[
        bool | None,
        typer.Option(
            "--version",
            "-V",
            help="Show version and exit.",
            callback=version_callback,
            is_eager=True,
        ),
    ] = None,
):
    """dbslice - Extract minimal database subsets."""
    pass


def create_progress_callback(status: Status | None, verbose: bool, console: Console):
    """
    Create a progress callback that updates Rich status display.

    Args:
        status: Optional Rich Status object to update during extraction
        verbose: Whether to show detailed progress messages
        console: Rich Console for verbose output

    Returns:
        Callback function with signature (stage, message, current, total) -> None
    """

    def callback(stage: str, message: str, current: int, total: int):
        if status:
            status.update(f"[bold blue]{message}[/bold blue]")
        if verbose:
            if current and total:
                console.print(f"  [dim][{current}/{total}] {message}[/dim]")
            else:
                console.print(f"  [dim]{message}[/dim]")

    return callback


def _parse_and_validate_seeds(
    seeds: list[str],
    console: Console,
) -> list[SeedSpec]:
    """
    Parse and validate seed specifications from CLI arguments.

    Args:
        seeds: Raw seed strings from CLI (e.g., ["orders.id=123", "users:email='test'"])
        console: Rich console for error output

    Returns:
        List of validated SeedSpec objects

    Raises:
        typer.Exit: If seeds are invalid or empty (exits with code 1)
    """
    parsed_seeds = []
    for s in seeds:
        try:
            parsed_seeds.append(SeedSpec.parse(s))
        except ValueError as e:
            raise InvalidSeedError(s, str(e))

    if not parsed_seeds:
        console.print("[red]Error:[/red] At least one --seed is required")
        raise typer.Exit(1)

    return parsed_seeds


def _parse_enum_parameters(
    direction: str,
    output: str,
    console: Console,
) -> tuple[TraversalDirection, OutputFormat]:
    """
    Parse and validate direction and output format CLI parameters.

    Args:
        direction: Direction string from CLI ("up", "down", or "both")
        output: Output format string from CLI ("sql", "json", or "csv")
        console: Rich console for error output

    Returns:
        Tuple of (TraversalDirection enum, OutputFormat enum)

    Raises:
        typer.Exit: If parameters are invalid (exits with code 1)
    """
    try:
        direction_enum = TraversalDirection(direction.lower())
    except ValueError:
        console.print(f"[red]Error:[/red] Invalid direction '{direction}'. Use: up, down, both")
        raise typer.Exit(1)

    try:
        output_format = OutputFormat(output.lower())
    except ValueError:
        console.print(f"[red]Error:[/red] Invalid output format '{output}'. Use: sql, json, csv")
        raise typer.Exit(1)

    return direction_enum, output_format


def _build_extract_config(
    database_url: str,
    seeds: list[SeedSpec],
    depth: int,
    direction: TraversalDirection,
    output_format: OutputFormat,
    out_file: Path | None,
    exclude: list[str] | None,
    passthrough: list[str] | None,
    anonymize: bool,
    redact: list[str] | None,
    verbose: bool,
    dry_run: bool,
    no_progress: bool,
    validate: bool,
    fail_on_validation_error: bool,
    profile: bool,
) -> ExtractConfig:
    """
    Build ExtractConfig from validated CLI parameters.

    Args:
        database_url: Database connection URL
        seeds: Validated seed specifications
        depth: Maximum FK traversal depth
        direction: Traversal direction enum
        output_format: Output format enum
        out_file: Optional output file path
        exclude: Optional list of tables to exclude
        passthrough: Optional list of tables to include in full
        anonymize: Enable automatic anonymization
        redact: Optional list of fields to redact
        verbose: Enable verbose output
        dry_run: Enable dry-run mode
        no_progress: Disable progress output
        validate: Enable extraction validation
        fail_on_validation_error: Stop on validation error
        profile: Enable query profiling

    Returns:
        Configured ExtractConfig object
    """
    return ExtractConfig(
        database_url=database_url,
        seeds=seeds,
        depth=depth,
        direction=direction,
        output_format=output_format,
        output_file=str(out_file) if out_file else None,
        exclude_tables=set(exclude) if exclude else set(),
        passthrough_tables=set(passthrough) if passthrough else set(),
        anonymize=anonymize,
        redact_fields=list(redact) if redact else [],
        verbose=verbose,
        dry_run=dry_run,
        no_progress=no_progress,
        validate=validate,
        fail_on_validation_error=fail_on_validation_error,
        profile=profile,
    )


def _show_extraction_settings(
    config: ExtractConfig,
    console: Console,
) -> None:
    """
    Display extraction settings in verbose mode.

    Args:
        config: Extraction configuration
        console: Rich console for output
    """
    console.print("\n[bold]Extraction Settings:[/bold]")
    console.print(f"  Direction: [cyan]{config.direction.value}[/cyan]")
    console.print(f"  Max Depth: [cyan]{config.depth}[/cyan]")
    console.print(f"  Seeds: [cyan]{len(config.seeds)}[/cyan]")
    for s in config.seeds:
        seed_desc = f"{s.table}.{s.column}={s.value}" if s.column else f"{s.table}:{s.where_clause}"
        console.print(f"    - {seed_desc}")
    if config.anonymize:
        console.print("  [yellow]Anonymization: ENABLED[/yellow]")
        if config.redact_fields:
            console.print("  Additional redacted fields:")
            for field in config.redact_fields:
                console.print(f"    - {field}")
    console.print()


def _execute_extraction(
    config: ExtractConfig,
    console: Console,
) -> tuple:
    """
    Execute the extraction with progress callback if needed.

    Args:
        config: Extraction configuration
        console: Rich console for progress output

    Returns:
        Tuple of (ExtractionResult, SchemaGraph, ExtractionEngine)
    """
    if config.no_progress:
        engine = ExtractionEngine(config)
        result, schema = engine.extract()
    else:
        with console.status("[bold blue]Connecting...[/bold blue]") as status:
            progress_cb = create_progress_callback(
                status if not config.verbose else None, config.verbose, console
            )
            engine = ExtractionEngine(config, progress_callback=progress_cb)
            result, schema = engine.extract()

    return result, schema, engine


def _show_extraction_summary(
    result,
    config: ExtractConfig,
    engine: ExtractionEngine,
    console: Console,
) -> None:
    """
    Display comprehensive extraction results summary.

    Args:
        result: Extraction result with tables and statistics
        config: Extraction configuration (for checking anonymize flag)
        engine: Extraction engine (for anonymizer statistics)
        console: Rich console for output
    """
    console.print()
    console.print("[bold green]Extraction Complete![/bold green]")
    console.print(
        f"  Total: [cyan]{result.total_rows()}[/cyan] rows from [cyan]{result.table_count()}[/cyan] tables"
    )

    if result.has_cycles:
        console.print()
        console.print("[yellow]⚠ Circular dependencies detected and resolved[/yellow]")
        console.print(f"  Broken FKs: [cyan]{len(result.broken_fks)}[/cyan]")
        console.print(f"  Deferred UPDATEs: [cyan]{len(result.deferred_updates)}[/cyan]")
        if config.verbose:
            for cycle_info in result.cycle_infos:
                console.print(f"  [dim]Cycle: {cycle_info}[/dim]")
            for fk in result.broken_fks:
                fk_desc = f"{fk.source_table}.{', '.join(fk.source_columns)} → {fk.target_table}"
                console.print(f"  [dim]Broken FK: {fk_desc}[/dim]")

    if result.validation_result:
        console.print()
        if result.validation_result.is_valid:
            console.print("[green]✓ Validation passed: All FK references intact[/green]")
            if config.verbose:
                console.print(
                    f"  Records checked: [cyan]{result.validation_result.total_records_checked}[/cyan]"
                )
                console.print(
                    f"  FK checks: [cyan]{result.validation_result.total_fk_checks}[/cyan]"
                )
        else:
            console.print("[red]✗ Validation failed: Orphaned records detected[/red]")
            console.print(
                f"  Orphaned records: [red]{len(result.validation_result.orphaned_records)}[/red]"
            )
            if config.verbose:
                console.print()
                console.print("[bold]Validation Report:[/bold]")
                console.print(result.validation_result.format_report())

    if config.anonymize:
        console.print()
        console.print("[yellow]ℹ Sensitive data anonymized[/yellow]")
        if config.verbose and engine.anonymizer:
            stats = engine.anonymizer.get_statistics()
            console.print(f"  Anonymized values cached: [cyan]{stats['cache_size']}[/cyan]")
            if stats["redact_fields_count"] > 0:
                console.print(
                    f"  Custom redact fields: [cyan]{stats['redact_fields_count']}[/cyan]"
                )

    console.print()
    console.print("[bold]Tables extracted:[/bold]")
    for table in result.insert_order:
        if table in result.stats:
            console.print(f"  [dim]{table}:[/dim] {result.stats[table]} rows")

    if config.verbose and result.traversal_path:
        console.print()
        console.print("[bold]Traversal path:[/bold]")
        for path in result.traversal_path:
            console.print(f"  [dim]{path}[/dim]")

    if config.profile and result.profiler:
        console.print()
        summary = result.profiler.get_summary()
        console.print(summary.format_summary(show_slowest=10))


def _generate_and_output_sql(
    result,
    schema,
    database_url: str,
    out_file: Path | None,
    no_progress: bool,
    console: Console,
    stdout_console: Console,
) -> None:
    """
    Generate SQL output and write to file or stdout.

    Args:
        result: Extraction result with tables and insert order
        schema: Database schema (used for table metadata)
        database_url: Database URL (parsed for db_type)
        out_file: Optional output file path
        no_progress: Whether progress output is disabled
        console: Rich console for progress/status messages
        stdout_console: Console for SQL output to stdout
    """
    db_config = parse_database_url(database_url)

    # Use schema from extraction (no reconnection needed)
    generator = SQLGenerator(db_type=db_config.db_type)
    sql_output = generator.generate(
        result.tables,
        result.insert_order,
        schema.tables,
        result.broken_fks,
        result.deferred_updates,
    )

    if out_file:
        out_file.write_text(sql_output)
        if not no_progress:
            console.print()
            console.print(
                f"[green]Wrote {result.total_rows()} rows to [bold]{out_file}[/bold][/green]"
            )
    else:
        if not no_progress:
            console.print()
            console.print("[dim]--- SQL Output ---[/dim]")
        stdout_console.print(sql_output)


def _generate_and_output_json(
    result,
    schema,
    out_file: Path | None,
    json_mode: str,
    json_pretty: bool,
    no_progress: bool,
    console: Console,
    stdout_console: Console,
) -> None:
    """
    Generate JSON output and write to file(s) or stdout.

    Args:
        result: Extraction result with tables and insert order
        schema: Database schema (used for table metadata)
        out_file: Optional output file/directory path
        json_mode: JSON output mode ("auto", "single", or "per-table")
        json_pretty: Enable pretty-printing
        no_progress: Whether progress output is disabled
        console: Rich console for progress/status messages
        stdout_console: Console for JSON output to stdout
    """
    if json_mode == "auto":
        if out_file and out_file.is_dir():
            mode = "per-table"
        else:
            mode = "single"
    else:
        mode = json_mode

    generator = JSONGenerator(mode=mode, pretty=json_pretty)
    json_output = generator.generate(
        result.tables,
        result.insert_order,
        schema.tables,
        result.broken_fks,
        result.deferred_updates,
    )

    if out_file:
        if mode == "single":
            assert isinstance(json_output, str)
            out_file.parent.mkdir(parents=True, exist_ok=True)
            out_file.write_text(json_output, encoding="utf-8")
            if not no_progress:
                console.print()
                console.print(
                    f"[green]Wrote {result.total_rows()} rows to [bold]{out_file}[/bold][/green]"
                )
        else:
            assert isinstance(json_output, dict)
            out_file.mkdir(parents=True, exist_ok=True)
            for table_name, table_json in json_output.items():
                table_file = out_file / f"{table_name}.json"
                table_file.write_text(table_json, encoding="utf-8")
            if not no_progress:
                console.print()
                console.print(
                    f"[green]Wrote {result.table_count()} tables ({result.total_rows()} rows) to [bold]{out_file}[/bold][/green]"
                )
    else:
        # Output to stdout (only single mode makes sense)
        if mode == "per-table":
            console.print(
                "[yellow]Warning:[/yellow] Per-table mode not supported for stdout, using single mode"
            )
            generator = JSONGenerator(mode="single", pretty=json_pretty)
            json_output = generator.generate(
                result.tables,
                result.insert_order,
                schema.tables,
                result.broken_fks,
                result.deferred_updates,
            )

        if not no_progress:
            console.print()
            console.print("[dim]--- JSON Output ---[/dim]")
        stdout_console.print(json_output)


def _generate_and_output_csv(
    result,
    schema,
    out_file: Path | None,
    csv_mode: str,
    csv_delimiter: str,
    no_progress: bool,
    console: Console,
    stdout_console: Console,
) -> None:
    """
    Generate CSV output and write to file(s) or stdout.

    Args:
        result: Extraction result with tables and insert order
        schema: Database schema (used for table metadata)
        out_file: Optional output file/directory path
        csv_mode: CSV output mode ("auto", "single", or "per-table")
        csv_delimiter: CSV field delimiter
        no_progress: Whether progress output is disabled
        console: Rich console for progress/status messages
        stdout_console: Console for CSV output to stdout
    """
    if csv_mode == "auto":
        if out_file and out_file.is_dir():
            mode = "per-table"
        else:
            mode = "single"
    else:
        mode = csv_mode

    generator = CSVGenerator(mode=mode, delimiter=csv_delimiter)
    csv_output = generator.generate(
        result.tables,
        result.insert_order,
        schema.tables,
        result.broken_fks,
        result.deferred_updates,
    )

    if out_file:
        if mode == "single":
            assert isinstance(csv_output, str)
            out_file.parent.mkdir(parents=True, exist_ok=True)
            out_file.write_text(csv_output, encoding="utf-8")
            if not no_progress:
                console.print()
                console.print(
                    f"[green]Wrote {result.total_rows()} rows to [bold]{out_file}[/bold][/green]"
                )
        else:
            assert isinstance(csv_output, dict)
            out_file.mkdir(parents=True, exist_ok=True)
            for table_name, table_csv in csv_output.items():
                table_file = out_file / f"{table_name}.csv"
                table_file.write_text(table_csv, encoding="utf-8")
            if not no_progress:
                console.print()
                console.print(
                    f"[green]Wrote {result.table_count()} tables ({result.total_rows()} rows) to [bold]{out_file}[/bold][/green]"
                )
    else:
        # Output to stdout (only single mode makes sense)
        if mode == "per-table":
            console.print(
                "[yellow]Warning:[/yellow] Per-table mode not supported for stdout, using single mode"
            )
            generator = CSVGenerator(mode="single", delimiter=csv_delimiter)
            csv_output = generator.generate(
                result.tables,
                result.insert_order,
                schema.tables,
                result.broken_fks,
                result.deferred_updates,
            )

        if not no_progress:
            console.print()
            console.print("[dim]--- CSV Output ---[/dim]")
        stdout_console.print(csv_output)


def _handle_output_format(
    output_format: OutputFormat,
    result,
    schema,
    database_url: str,
    out_file: Path | None,
    json_mode: str,
    json_pretty: bool,
    csv_mode: str,
    csv_delimiter: str,
    no_progress: bool,
    console: Console,
    stdout_console: Console,
) -> None:
    """
    Handle output generation based on configured format.

    Args:
        output_format: Desired output format (SQL, JSON, or CSV)
        result: Extraction result
        schema: Database schema
        database_url: Database connection URL
        out_file: Optional output file path
        json_mode: JSON output mode ("auto", "single", or "per-table")
        json_pretty: Enable JSON pretty-printing
        csv_mode: CSV output mode ("auto", "single", or "per-table")
        csv_delimiter: CSV field delimiter
        no_progress: Whether progress output is disabled
        console: Rich console for messages
        stdout_console: Console for data output

    Raises:
        typer.Exit: If format is not yet implemented (exits with code 1)
    """
    if output_format == OutputFormat.SQL:
        _generate_and_output_sql(
            result,
            schema,
            database_url,
            out_file,
            no_progress,
            console,
            stdout_console,
        )
    elif output_format == OutputFormat.JSON:
        _generate_and_output_json(
            result,
            schema,
            out_file,
            json_mode,
            json_pretty,
            no_progress,
            console,
            stdout_console,
        )
    elif output_format == OutputFormat.CSV:
        _generate_and_output_csv(
            result,
            schema,
            out_file,
            csv_mode,
            csv_delimiter,
            no_progress,
            console,
            stdout_console,
        )


@app.command()
def extract(
    database_url: Annotated[
        str | None,
        typer.Argument(
            help="Database connection URL (e.g., postgres://user:pass@host:5432/dbname)"
        ),
    ] = None,
    seed: Annotated[
        list[str] | None,
        typer.Option(
            "--seed",
            "-s",
            help="Seed record(s): 'table.column=value' or 'table:WHERE_CLAUSE'",
        ),
    ] = None,
    config: Annotated[
        Path | None,
        typer.Option(
            "--config",
            "-c",
            help="Path to YAML configuration file",
        ),
    ] = None,
    depth: Annotated[
        int,
        typer.Option(
            "--depth",
            "-d",
            help="Maximum FK traversal depth",
        ),
    ] = DEFAULT_TRAVERSAL_DEPTH,
    direction: Annotated[
        str,
        typer.Option(
            "--direction",
            help="Traversal direction: 'up' (parents), 'down' (children), 'both'",
        ),
    ] = "both",
    output: Annotated[
        str,
        typer.Option(
            "--output",
            "-o",
            help="Output format: sql, json, csv",
        ),
    ] = "sql",
    out_file: Annotated[
        Path | None,
        typer.Option(
            "--out-file",
            "-f",
            help="Write to file instead of stdout",
        ),
    ] = None,
    exclude: Annotated[
        list[str] | None,
        typer.Option(
            "--exclude",
            "-x",
            help="Tables to exclude from extraction",
        ),
    ] = None,
    passthrough: Annotated[
        list[str] | None,
        typer.Option(
            "--passthrough",
            "-p",
            help="Tables to include in full (all rows, regardless of FK relationships)",
        ),
    ] = None,
    verbose: Annotated[
        bool,
        typer.Option(
            "--verbose",
            "-v",
            help="Show detailed logs including traversal path",
        ),
    ] = False,
    anonymize: Annotated[
        bool,
        typer.Option(
            "--anonymize",
            "-a",
            help="Enable automatic anonymization of detected sensitive fields",
        ),
    ] = False,
    redact: Annotated[
        list[str] | None,
        typer.Option(
            "--redact",
            "-r",
            help="Additional fields to redact (format: table.column)",
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Show what would be extracted without fetching data",
        ),
    ] = False,
    no_progress: Annotated[
        bool,
        typer.Option(
            "--no-progress",
            help="Disable progress output (for piping)",
        ),
    ] = False,
    json_mode: Annotated[
        str,
        typer.Option(
            "--json-mode",
            help="JSON output mode: 'auto' (default), 'single' (one file), 'per-table' (separate files)",
        ),
    ] = "auto",
    json_pretty: Annotated[
        bool,
        typer.Option(
            "--json-pretty/--json-compact",
            help="Enable/disable JSON pretty-printing (default: enabled)",
        ),
    ] = True,
    csv_mode: Annotated[
        str,
        typer.Option(
            "--csv-mode",
            help="CSV output mode: 'auto' (default), 'single' (one file), 'per-table' (separate files)",
        ),
    ] = "auto",
    csv_delimiter: Annotated[
        str,
        typer.Option(
            "--csv-delimiter",
            help="CSV field delimiter (default: comma)",
        ),
    ] = ",",
    validate: Annotated[
        bool,
        typer.Option(
            "--validate/--no-validate",
            help="Validate extraction for referential integrity (default: enabled)",
        ),
    ] = True,
    fail_on_validation_error: Annotated[
        bool,
        typer.Option(
            "--fail-on-validation-error",
            help="Stop execution if validation finds issues (default: disabled, shows warning)",
        ),
    ] = False,
    profile: Annotated[
        bool,
        typer.Option(
            "--profile",
            help="Enable query profiling and show performance statistics",
        ),
    ] = False,
    stream: Annotated[
        bool,
        typer.Option(
            "--stream",
            help="Force streaming mode (write data directly to file without loading into memory)",
        ),
    ] = False,
    stream_threshold: Annotated[
        int,
        typer.Option(
            "--stream-threshold",
            help="Auto-enable streaming mode above this row count (default: 50000)",
        ),
    ] = 50000,
    stream_chunk_size: Annotated[
        int,
        typer.Option(
            "--stream-chunk-size",
            help="Number of rows to fetch per chunk in streaming mode (default: 1000)",
        ),
    ] = 1000,
):
    """
    Extract a database subset starting from seed record(s).

    Examples:

        # Extract by primary key (only parents/referenced records)
        dbslice extract postgres://localhost/myapp --seed "orders.id=12345"

        # Extract with children too (all related records)
        dbslice extract postgres://localhost/myapp --seed "orders.id=12345" --direction both

        # Extract with WHERE clause
        dbslice extract postgres://localhost/myapp --seed "orders:status='failed'"

        # Multiple seeds
        dbslice extract postgres://localhost/myapp -s "orders.id=1" -s "orders.id=2"

        # Save to file with verbose output
        dbslice extract postgres://localhost/myapp -s "users.id=1" -f subset.sql -v

        # Use configuration file
        dbslice extract --config dbslice.yaml --seed "orders.id=12345"

        # Use config file with CLI overrides (CLI args take precedence)
        dbslice extract --config dbslice.yaml --seed "users.id=1" --depth 5 --anonymize

        # Force streaming mode for large datasets (requires --out-file)
        dbslice extract postgres://localhost/myapp -s "orders:created_at > '2023-01-01'" -f large.sql --stream

        # Auto-enable streaming for 100K+ rows
        dbslice extract postgres://localhost/myapp -s "users:active=true" -f big.sql --stream-threshold 100000

        # Include passthrough tables (e.g., lookup/config tables)
        dbslice extract postgres://localhost/myapp -s "orders.id=1" --passthrough django_content_type --passthrough countries
    """
    try:
        setup_logging(verbose=verbose, no_progress=no_progress, structured=False)
        logger.debug("CLI command invoked", command="extract", depth=depth, direction=direction)

        loaded_config = None
        if config:
            try:
                from dbslice.config_file import ConfigFileError, load_config

                loaded_config = load_config(config)
                logger.info("Loaded configuration file", path=str(config))
                if verbose and not no_progress:
                    console.print(f"[dim]Loaded config from {config}[/dim]")
            except ConfigFileError as e:
                console.print(f"[red]Config Error:[/red] {e}")
                raise typer.Exit(1)

        if not database_url and (not loaded_config or not loaded_config.database.url):
            console.print(
                "[red]Error:[/red] Database URL is required. "
                "Provide it via DATABASE_URL argument or in config file under 'database.url'"
            )
            raise typer.Exit(1)

        if not database_url and loaded_config:
            database_url = loaded_config.database.url

        assert database_url is not None  # Guaranteed by the check above

        try:
            validate_database_url(database_url)
            validate_depth(depth)
            if out_file:
                validate_output_file_path(out_file)
            if exclude:
                validate_exclude_tables(exclude)
            if passthrough:
                validate_exclude_tables(passthrough)  # Same validation as exclude
            if redact:
                validate_redact_fields(redact)
            if json_mode not in ("auto", "single", "per-table"):
                raise ValueError(
                    f"Invalid json_mode: {json_mode}. Must be 'auto', 'single', or 'per-table'"
                )
            if csv_mode not in ("auto", "single", "per-table"):
                raise ValueError(
                    f"Invalid csv_mode: {csv_mode}. Must be 'auto', 'single', or 'per-table'"
                )
        except ValidationError as e:
            console.print(f"[red]Validation Error:[/red] {e}")
            raise typer.Exit(1)
        except ValueError as e:
            console.print(f"[red]Validation Error:[/red] {e}")
            raise typer.Exit(1)

        seed_specs = _parse_and_validate_seeds(seed or [], console)

        if loaded_config:
            direction_enum = TraversalDirection(direction) if direction != "both" else None
            output_format_enum = OutputFormat(output) if output != "sql" else None

            extract_config = loaded_config.to_extract_config(
                seeds=seed_specs,
                database_url=database_url,
                depth=depth if depth != DEFAULT_TRAVERSAL_DEPTH else None,
                direction=direction_enum,
                output_format=output_format_enum,
                output_file=str(out_file) if out_file else None,
                exclude=exclude,
                passthrough=passthrough,
                anonymize=anonymize if anonymize else None,
                redact=redact,
                verbose=verbose,
                dry_run=dry_run,
                no_progress=no_progress,
            )
            output_format = extract_config.output_format
        else:
            direction_enum, output_format = _parse_enum_parameters(direction, output, console)
            extract_config = _build_extract_config(
                database_url=database_url,
                seeds=seed_specs,
                depth=depth,
                direction=direction_enum,
                output_format=output_format,
                out_file=out_file,
                exclude=exclude,
                passthrough=passthrough,
                anonymize=anonymize,
                redact=redact,
                verbose=verbose,
                dry_run=dry_run,
                no_progress=no_progress,
                validate=validate,
                fail_on_validation_error=fail_on_validation_error,
                profile=profile,
            )
            extract_config.stream = stream
            extract_config.streaming_threshold = stream_threshold
            extract_config.streaming_chunk_size = stream_chunk_size

        if verbose and not no_progress:
            _show_extraction_settings(extract_config, console)

        result, schema, engine = _execute_extraction(extract_config, console)

        if not no_progress:
            _show_extraction_summary(result, extract_config, engine, console)

        _handle_output_format(
            output_format=output_format,
            result=result,
            schema=schema,
            database_url=database_url,
            out_file=out_file,
            json_mode=json_mode,
            json_pretty=json_pretty,
            csv_mode=csv_mode,
            csv_delimiter=csv_delimiter,
            no_progress=no_progress,
            console=console,
            stdout_console=stdout_console,
        )

    except ConnectionError as e:
        logger.error("Database connection failed", error=e.reason, exc_info=True)
        console.print(f"[red]Connection failed:[/red] {e.reason}")
        console.print(f"[dim]URL: {e._mask_password(e.url)}[/dim]")
        raise typer.Exit(1)

    except InvalidSeedError as e:
        logger.error("Invalid seed specification", error=str(e), exc_info=True)
        console.print(f"[red]Invalid seed:[/red] {e}")
        console.print("[dim]Format: table.column=value or table:WHERE_CLAUSE[/dim]")
        raise typer.Exit(1)

    except TableNotFoundError as e:
        logger.error("Table not found in schema", error=str(e), exc_info=True)
        console.print(f"[red]Table not found:[/red] {e}")
        raise typer.Exit(1)

    except NoRowsFoundError as e:
        logger.warning("No rows found for seed", error=str(e))
        console.print(f"[yellow]Warning:[/yellow] {e}")
        raise typer.Exit(1)

    except CircularReferenceError as e:
        logger.error("Circular reference detected", error=str(e), exc_info=True)
        console.print("[red]Circular Reference Error:[/red]")
        console.print(str(e))
        raise typer.Exit(1)

    except DbsliceError as e:
        logger.error("DbsliceError occurred", error=str(e), exc_info=True)
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)

    except (typer.Exit, SystemExit):
        raise

    except Exception as e:
        logger.critical("Unexpected error occurred", error=str(e), exc_info=True)
        console.print(f"[red]Unexpected error:[/red] {e}")
        if verbose:
            import traceback

            console.print(traceback.format_exc())
        raise typer.Exit(1)


@app.command()
def init(
    database_url: Annotated[
        str,
        typer.Argument(
            help="Database connection URL (e.g., postgres://user:pass@host:5432/dbname)"
        ),
    ],
    out_file: Annotated[
        Path,
        typer.Option(
            "--out-file",
            "-f",
            help="Output config file path",
        ),
    ] = Path("dbslice.yaml"),
    detect_sensitive: Annotated[
        bool,
        typer.Option(
            "--detect-sensitive",
            help="Auto-detect sensitive fields (email, phone, etc.)",
        ),
    ] = True,
):
    """
    Generate a configuration file from database schema.

    This command connects to the database, introspects the schema,
    and generates a YAML configuration file with sensible defaults.

    Examples:

        # Generate config with default filename (dbslice.yaml)
        dbslice init postgres://localhost/myapp

        # Generate config to a specific file
        dbslice init postgres://localhost/myapp -f config/production.yaml

        # Generate without sensitive field detection
        dbslice init postgres://localhost/myapp --no-detect-sensitive
    """
    try:
        try:
            validate_database_url(database_url)
            validate_output_file_path(out_file)
        except ValidationError as e:
            console.print(f"[red]Validation Error:[/red] {e}")
            raise typer.Exit(1)

        from dbslice.config_file import (
            AnonymizationConfig,
            DatabaseConfig,
            DbsliceConfig,
            ExtractionConfig,
            OutputConfig,
        )
        from dbslice.utils.connection import get_adapter_for_url

        with console.status("[bold blue]Connecting to database...[/bold blue]"):
            adapter = get_adapter_for_url(database_url)
            adapter.connect(database_url)

        try:
            with console.status("[bold blue]Introspecting schema...[/bold blue]"):
                schema = adapter.get_schema()

            console.print(
                f"[green]Found {len(schema.tables)} tables, {len(schema.edges)} foreign keys[/green]"
            )

            sensitive_fields = {}
            if detect_sensitive:
                console.print("[dim]Detecting sensitive fields...[/dim]")
                sensitive_fields = _detect_sensitive_fields(schema)
                if sensitive_fields:
                    console.print(
                        f"[yellow]Detected {len(sensitive_fields)} sensitive fields[/yellow]"
                    )

            config = DbsliceConfig(
                database=DatabaseConfig(url=database_url),
                extraction=ExtractionConfig(
                    default_depth=DEFAULT_TRAVERSAL_DEPTH,
                    direction="both",
                    exclude_tables=[],
                ),
                anonymization=AnonymizationConfig(
                    enabled=len(sensitive_fields) > 0,
                    fields=sensitive_fields,
                ),
                output=OutputConfig(
                    format="sql",
                    include_transaction=True,
                    include_drop_tables=False,
                ),
                tables={},
            )

            yaml_content = config.to_yaml(include_comments=True)
            out_file.write_text(yaml_content)

            console.print()
            console.print(f"[green]Configuration written to [bold]{out_file}[/bold][/green]")
            console.print()
            console.print("[bold]Next steps:[/bold]")
            console.print(f"  1. Review and edit [cyan]{out_file}[/cyan]")
            console.print(
                f"  2. Run extraction: [cyan]dbslice extract --config {out_file} --seed 'table.id=1'[/cyan]"
            )

        finally:
            adapter.close()

    except ConnectionError as e:
        console.print(f"[red]Connection failed:[/red] {e.reason}")
        raise typer.Exit(1)

    except DbsliceError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)

    except Exception as e:
        console.print(f"[red]Unexpected error:[/red] {e}")
        raise typer.Exit(1)


def _detect_sensitive_fields(schema) -> dict[str, str]:
    """
    Auto-detect sensitive fields in the schema.

    Returns:
        Dictionary mapping "table.column" to faker provider
    """
    sensitive_patterns = {
        # Email patterns
        "email": "email",
        "e_mail": "email",
        "email_address": "email",
        # Phone patterns
        "phone": "phone_number",
        "telephone": "phone_number",
        "mobile": "phone_number",
        "cell": "phone_number",
        "phone_number": "phone_number",
        # Name patterns
        "first_name": "first_name",
        "firstname": "first_name",
        "last_name": "last_name",
        "lastname": "last_name",
        "full_name": "name",
        "fullname": "name",
        # Address patterns
        "address": "address",
        "street": "street_address",
        "street_address": "street_address",
        "city": "city",
        "postal_code": "postcode",
        "postcode": "postcode",
        "zip_code": "postcode",
        "zipcode": "postcode",
        # Personal identifiers
        "ssn": "ssn",
        "social_security": "ssn",
        "passport": "passport_number",
        "passport_number": "passport_number",
        "driver_license": "license_plate",
        "credit_card": "credit_card_number",
        "card_number": "credit_card_number",
        # IP addresses
        "ip_address": "ipv4",
        "ip": "ipv4",
        "ipv4": "ipv4",
        "ipv6": "ipv6",
    }

    detected = {}
    for table_name, table in schema.tables.items():
        for column in table.columns:
            col_lower = column.name.lower()
            if col_lower in sensitive_patterns:
                detected[f"{table_name}.{column.name}"] = sensitive_patterns[col_lower]
            else:
                for pattern, provider in sensitive_patterns.items():
                    if pattern in col_lower:
                        detected[f"{table_name}.{column.name}"] = provider
                        break

    return detected


@app.command()
def inspect(
    database_url: Annotated[
        str,
        typer.Argument(help="Database connection URL"),
    ],
    table: Annotated[
        str | None,
        typer.Option(
            "--table",
            "-t",
            help="Show details for a specific table",
        ),
    ] = None,
):
    """
    Inspect database schema without extracting data.

    Shows tables, foreign keys, and detected sensitive fields.
    """
    try:
        try:
            validate_database_url(database_url)
            if table:
                from dbslice.input_validators import validate_table_name

                validate_table_name(table)
        except ValidationError as e:
            console.print(f"[red]Validation Error:[/red] {e}")
            raise typer.Exit(1)

        from dbslice.utils.connection import get_adapter_for_url

        with console.status("[bold blue]Connecting to database...[/bold blue]"):
            adapter = get_adapter_for_url(database_url)
            adapter.connect(database_url)

        try:
            with console.status("[bold blue]Introspecting schema...[/bold blue]"):
                schema = adapter.get_schema()

            if table:
                table_info = schema.get_table(table)
                if not table_info:
                    console.print(f"[red]Table '{table}' not found[/red]")
                    raise typer.Exit(1)

                console.print(f"\n[bold]{table}[/bold]")
                console.print(f"  Schema: {table_info.schema}")
                console.print(f"  Primary key: {', '.join(table_info.primary_key)}")
                console.print("\n  Columns:")
                for col in table_info.columns:
                    nullable = "NULL" if col.nullable else "NOT NULL"
                    pk = " [PK]" if col.name in table_info.primary_key else ""
                    console.print(f"    {col.name}: {col.data_type} {nullable}{pk}")

                parents = schema.get_parents(table)
                if parents:
                    console.print("\n  [bold]Foreign keys (references):[/bold]")
                    for parent_table, fk in parents:
                        nullable = "nullable" if fk.is_nullable else "required"
                        console.print(
                            f"    {', '.join(fk.source_columns)} -> [cyan]{parent_table}[/cyan].{', '.join(fk.target_columns)} ({nullable})"
                        )

                children = schema.get_children(table)
                if children:
                    console.print("\n  [bold]Referenced by:[/bold]")
                    for child_table, fk in children:
                        console.print(
                            f"    [cyan]{child_table}[/cyan].{', '.join(fk.source_columns)}"
                        )

            else:
                console.print(f"\n[bold]Tables ({len(schema.tables)})[/bold]")
                for name in sorted(schema.tables.keys()):
                    t = schema.tables[name]
                    pk_str = ", ".join(t.primary_key) if t.primary_key else "no PK"
                    console.print(f"  {name} ({pk_str})")

                console.print(f"\n[bold]Foreign Keys ({len(schema.edges)})[/bold]")
                for fk in schema.edges:
                    nullable = "nullable" if fk.is_nullable else "required"
                    src_cols = ", ".join(fk.source_columns)
                    tgt_cols = ", ".join(fk.target_columns)
                    console.print(
                        f"  {fk.source_table}.{src_cols} -> [cyan]{fk.target_table}[/cyan].{tgt_cols} ({nullable})"
                    )

                self_refs = [fk for fk in schema.edges if fk.is_self_referential]
                if self_refs:
                    console.print("\n[yellow]Self-references (potential cycles):[/yellow]")
                    for fk in self_refs:
                        console.print(f"  {fk.source_table}.{', '.join(fk.source_columns)}")

        finally:
            adapter.close()

    except ConnectionError as e:
        console.print(f"[red]Connection failed:[/red] {e.reason}")
        raise typer.Exit(1)

    except DbsliceError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)


@app.command()
def docs(
    port: Annotated[
        int,
        typer.Option(
            "--port",
            "-p",
            help="Port to serve documentation on",
            min=1024,
            max=65535,
        ),
    ] = 8000,
    build: Annotated[
        bool,
        typer.Option(
            "--build",
            "-b",
            help="Build static documentation instead of serving",
        ),
    ] = False,
):
    """
    Serve or build documentation locally.

    Requires mkdocs to be installed: pip install dbslice[docs]

    Examples:

        # Serve docs on default port (8000)
        dbslice docs

        # Serve on custom port
        dbslice docs --port 8080

        # Build static documentation
        dbslice docs --build
    """
    try:
        import mkdocs  # noqa: F401
    except ImportError:
        console.print("[red]Error:[/red] mkdocs is not installed.")
        console.print("")
        console.print("Install documentation dependencies with:")
        console.print("  [cyan]pip install dbslice[docs][/cyan]")
        console.print("")
        console.print("Or install mkdocs directly:")
        console.print("  [cyan]pip install mkdocs mkdocs-material[/cyan]")
        raise typer.Exit(1)

    import subprocess
    import sys

    possible_paths = [
        Path(__file__).parent.parent.parent.parent / "mkdocs.yml",  # Development
        Path.cwd() / "mkdocs.yml",  # Current directory
    ]

    mkdocs_config = None
    for path in possible_paths:
        if path.exists():
            mkdocs_config = path
            break

    if not mkdocs_config:
        console.print("[red]Error:[/red] mkdocs.yml not found.")
        console.print("")
        console.print("Make sure you're in the dbslice project directory,")
        console.print("or that mkdocs.yml exists in the current directory.")
        raise typer.Exit(1)

    config_dir = mkdocs_config.parent

    if build:
        console.print("[bold blue]Building documentation...[/bold blue]")
        result = subprocess.run(
            [sys.executable, "-m", "mkdocs", "build"],
            cwd=config_dir,
        )
        if result.returncode == 0:
            console.print(
                f"[green]Documentation built to [bold]{config_dir / 'site'}[/bold][/green]"
            )
        raise typer.Exit(result.returncode)
    else:
        console.print(f"[bold blue]Serving documentation on http://localhost:{port}[/bold blue]")
        console.print("[dim]Press Ctrl+C to stop[/dim]")
        console.print("")
        try:
            result = subprocess.run(
                [sys.executable, "-m", "mkdocs", "serve", "--dev-addr", f"localhost:{port}"],
                cwd=config_dir,
            )
            raise typer.Exit(result.returncode)
        except KeyboardInterrupt:
            console.print("\n[dim]Server stopped[/dim]")
            raise typer.Exit(0)


if __name__ == "__main__":
    app()
