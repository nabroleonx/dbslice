import csv
import io
import json
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID

from dbslice.models import Table


class CSVGenerator:
    """
    Generates RFC 4180 compliant CSV output from extracted data in two modes.

    Modes:
        single: One CSV file with all tables (includes table_name column)
        per-table: Separate CSV file for each table

    Features:
    - RFC 4180 compliant CSV formatting
    - Custom type handling (datetime, JSON, arrays, etc.)
    - Configurable delimiter, quoting, and line terminators
    - Proper escaping of special characters
    - NULL value handling
    """

    def __init__(
        self,
        mode: str = "single",
        delimiter: str = ",",
        quoting: int = csv.QUOTE_MINIMAL,
        line_terminator: str = "\n",
    ):
        """
        Initialize CSV generator with output configuration.

        Args:
            mode: Output mode - "single" or "per-table"
            delimiter: Field delimiter (default: comma)
            quoting: CSV quoting style from csv module constants
            line_terminator: Line ending (default: "\n")

        Raises:
            ValueError: If mode is not "single" or "per-table"
        """
        if mode not in ("single", "per-table"):
            raise ValueError(f"Invalid mode: {mode}. Must be 'single' or 'per-table'")

        self.mode = mode
        self.delimiter = delimiter
        self.quoting = quoting
        self.line_terminator = line_terminator

    def generate(
        self,
        tables_data: dict[str, list[dict[str, Any]]],
        insert_order: list[str],
        tables_schema: dict[str, Table],
        broken_fks: list[Any] | None = None,
        deferred_updates: list[Any] | None = None,
    ) -> str | dict[str, str]:
        """
        Generate CSV output from extracted data.

        Args:
            tables_data: Dict mapping table name to list of row dicts
            insert_order: Tables in topologically sorted order
            tables_schema: Dict mapping table name to Table schema
            broken_fks: List of ForeignKey objects that were broken (for cycles)
            deferred_updates: List of DeferredUpdate objects (for cycles)

        Returns:
            In "single" mode: CSV string with all data
            In "per-table" mode: Dict mapping table name to CSV string
        """
        broken_fks = broken_fks or []
        deferred_updates = deferred_updates or []

        if self.mode == "single":
            return self._generate_single(
                tables_data,
                insert_order,
                tables_schema,
            )
        else:
            return self._generate_per_table(
                tables_data,
                insert_order,
                tables_schema,
            )

    def _generate_single(
        self,
        tables_data: dict[str, list[dict[str, Any]]],
        insert_order: list[str],
        tables_schema: dict[str, Table],
    ) -> str:
        """
        Generate single CSV file with all tables.

        Format includes a table_name column as the first column to identify
        which table each row belongs to:

        table_name,column1,column2,...
        users,1,alice@example.com,...
        orders,1,100.50,...

        Args:
            tables_data: Table data to serialize
            insert_order: Topologically sorted table names
            tables_schema: Table schemas (not used directly in output)

        Returns:
            CSV string with all data
        """
        output = io.StringIO()
        writer = None

        all_columns: set[str] = set()
        for table_name, rows in tables_data.items():
            if rows:
                all_columns.update(rows[0].keys())

        sorted_columns = sorted(all_columns)
        fieldnames = ["table_name"] + sorted_columns

        for table_name in insert_order:
            if table_name not in tables_data:
                continue

            rows = tables_data[table_name]
            if not rows:
                continue

            if writer is None:
                writer = csv.DictWriter(
                    output,
                    fieldnames=fieldnames,
                    delimiter=self.delimiter,
                    quoting=self.quoting,  # type: ignore[arg-type]
                    lineterminator=self.line_terminator,
                )
                writer.writeheader()

            for row in rows:
                csv_row = {"table_name": table_name}
                for col, value in row.items():
                    csv_row[col] = self._format_value(value)
                # Fill missing columns with empty string
                for col in sorted_columns:
                    if col not in csv_row:
                        csv_row[col] = ""
                writer.writerow(csv_row)

        return output.getvalue()

    def _generate_per_table(
        self,
        tables_data: dict[str, list[dict[str, Any]]],
        insert_order: list[str],
        tables_schema: dict[str, Table],
    ) -> dict[str, str]:
        """
        Generate separate CSV string for each table.

        Format for each table:
        column1,column2,column3
        value1,value2,value3

        Args:
            tables_data: Table data to serialize
            insert_order: Topologically sorted table names (for consistent ordering)
            tables_schema: Table schemas (not used directly in output)

        Returns:
            Dict mapping table name to CSV string
        """
        result = {}

        for table_name, rows in tables_data.items():
            if not rows:
                result[table_name] = ""
                continue

            output = io.StringIO()

            fieldnames = list(rows[0].keys())

            writer = csv.DictWriter(
                output,
                fieldnames=fieldnames,
                delimiter=self.delimiter,
                quoting=self.quoting,  # type: ignore[arg-type]
                lineterminator=self.line_terminator,
            )
            writer.writeheader()

            for row in rows:
                csv_row = {col: self._format_value(value) for col, value in row.items()}
                writer.writerow(csv_row)

            result[table_name] = output.getvalue()

        return result

    def _format_value(self, value: Any) -> str:
        """
        Format a Python value as CSV field value.

        Type conversions:
        - None -> empty string (CSV convention for NULL)
        - bool -> "true"/"false"
        - datetime -> ISO 8601 string
        - date -> ISO 8601 date string
        - time -> ISO 8601 time string
        - timedelta -> total seconds as float
        - UUID -> string representation
        - bytes -> hex string
        - dict/list -> JSON string
        - Decimal/int/float -> string representation
        - Everything else -> str()

        Args:
            value: Python value to format

        Returns:
            String representation suitable for CSV
        """
        if value is None:
            # CSV convention: NULL is represented as empty field
            return ""

        if isinstance(value, bool):
            # Use lowercase for consistency with JSON
            return "true" if value else "false"

        if isinstance(value, datetime):
            return value.isoformat()

        if isinstance(value, date):
            return value.isoformat()

        if isinstance(value, time):
            return value.isoformat()

        if isinstance(value, timedelta):
            # Convert to total seconds for easy parsing
            return str(value.total_seconds())

        if isinstance(value, UUID):
            return str(value)

        if isinstance(value, bytes):
            return value.hex()

        if isinstance(value, dict | list):
            return json.dumps(value, default=str, ensure_ascii=False)

        if isinstance(value, Decimal | int | float):
            return str(value)

        return str(value)

    def write_to_file(
        self,
        output: str | dict[str, str],
        file_path: Path | str,
    ) -> None:
        """
        Write CSV output to file(s).

        Args:
            output: CSV output from generate()
            file_path: Output path (file for single mode, directory for per-table)

        Raises:
            ValueError: If mode/path combination is invalid
            OSError: If file operations fail
        """
        file_path = Path(file_path)

        if self.mode == "single":
            if not isinstance(output, str):
                raise ValueError("Single mode output must be a string")

            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text(output, encoding="utf-8")
        else:
            if not isinstance(output, dict):
                raise ValueError("Per-table mode output must be a dict")

            file_path.mkdir(parents=True, exist_ok=True)

            for table_name, csv_str in output.items():
                table_file = file_path / f"{table_name}.csv"
                table_file.write_text(csv_str, encoding="utf-8")


def generate_csv(
    tables_data: dict[str, list[dict[str, Any]]],
    insert_order: list[str],
    tables_schema: dict[str, Table],
    mode: str = "single",
    delimiter: str = ",",
) -> str | dict[str, str]:
    """
    Convenience function to generate CSV output.

    Args:
        tables_data: Dict mapping table name to list of row dicts
        insert_order: Tables in topologically sorted order
        tables_schema: Dict mapping table name to Table schema
        mode: Output mode - "single" or "per-table"
        delimiter: Field delimiter (default: comma)

    Returns:
        CSV string (single mode) or dict of CSV strings (per-table mode)
    """
    generator = CSVGenerator(mode=mode, delimiter=delimiter)
    return generator.generate(tables_data, insert_order, tables_schema)
