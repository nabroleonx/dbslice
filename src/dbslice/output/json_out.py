import json
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID

from dbslice.models import Table


class DatabaseTypeEncoder(json.JSONEncoder):
    """
    Custom JSON encoder that handles database-specific types.

    Supported type conversions:
    - datetime -> ISO 8601 string with timezone
    - date -> ISO 8601 date string (YYYY-MM-DD)
    - time -> ISO 8601 time string (HH:MM:SS[.ffffff])
    - timedelta -> total seconds (as float)
    - Decimal -> float
    - UUID -> string
    - bytes -> hex string
    - Any other non-serializable type -> string representation
    """

    def default(self, obj: Any) -> Any:
        """
        Convert non-serializable objects to JSON-compatible types.

        Args:
            obj: Object to convert

        Returns:
            JSON-serializable representation of the object
        """
        if isinstance(obj, datetime):
            return obj.isoformat()

        if isinstance(obj, date):
            return obj.isoformat()

        if isinstance(obj, time):
            return obj.isoformat()

        if isinstance(obj, timedelta):
            # Convert to total seconds for easy reconstruction
            return obj.total_seconds()

        if isinstance(obj, Decimal):
            # Convert to float for JSON compatibility
            return float(obj)

        if isinstance(obj, UUID):
            return str(obj)

        if isinstance(obj, bytes):
            return obj.hex()

        return super().default(obj)


class JSONGenerator:
    """
    Generates JSON output from extracted data in two modes.

    Modes:
        single: One JSON file with all tables and metadata
        per-table: Separate JSON file for each table

    Features:
    - Custom type handling (datetime, Decimal, UUID, bytes, etc.)
    - Pretty-print option for readability
    - Comprehensive metadata in single mode
    - Consistent output format across modes
    """

    def __init__(
        self,
        mode: str = "single",
        pretty: bool = True,
        indent: int = 2,
    ):
        """
        Initialize JSON generator with output configuration.

        Args:
            mode: Output mode - "single" or "per-table"
            pretty: Enable pretty-printing with indentation
            indent: Number of spaces for indentation (if pretty=True)

        Raises:
            ValueError: If mode is not "single" or "per-table"
        """
        if mode not in ("single", "per-table"):
            raise ValueError(f"Invalid mode: {mode}. Must be 'single' or 'per-table'")

        self.mode = mode
        self.pretty = pretty
        self.indent = indent if pretty else None

    def generate(
        self,
        tables_data: dict[str, list[dict[str, Any]]],
        insert_order: list[str],
        tables_schema: dict[str, Table],
        broken_fks: list[Any] | None = None,
        deferred_updates: list[Any] | None = None,
    ) -> str | dict[str, str]:
        """
        Generate JSON output from extracted data.

        Args:
            tables_data: Dict mapping table name to list of row dicts
            insert_order: Tables in topologically sorted order
            tables_schema: Dict mapping table name to Table schema
            broken_fks: List of ForeignKey objects that were broken (for cycles)
            deferred_updates: List of DeferredUpdate objects (for cycles)

        Returns:
            In "single" mode: JSON string with all data
            In "per-table" mode: Dict mapping table name to JSON string
        """
        broken_fks = broken_fks or []
        deferred_updates = deferred_updates or []

        if self.mode == "single":
            return self._generate_single(
                tables_data,
                insert_order,
                tables_schema,
                broken_fks,
                deferred_updates,
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
        broken_fks: list[Any],
        deferred_updates: list[Any],
    ) -> str:
        """
        Generate single JSON file with all tables and metadata.

        Format:
        {
            "metadata": {
                "generated_by": "dbslice",
                "table_count": N,
                "total_rows": M,
                "insert_order": [...],
                "has_cycles": bool,
                "broken_fks_count": X
            },
            "tables": {
                "table_name": [row1, row2, ...],
                ...
            }
        }

        Args:
            tables_data: Table data to serialize
            insert_order: Topologically sorted table names
            tables_schema: Table schemas (not included in output)
            broken_fks: Broken foreign keys for cycle handling
            deferred_updates: Deferred updates for cycle handling

        Returns:
            JSON string with all data and metadata
        """
        total_rows = sum(len(rows) for rows in tables_data.values())
        has_cycles = len(broken_fks) > 0 or len(deferred_updates) > 0

        metadata: dict[str, Any] = {
            "generated_by": "dbslice",
            "table_count": len(tables_data),
            "total_rows": total_rows,
            "insert_order": insert_order,
            "has_cycles": has_cycles,
        }

        if has_cycles:
            metadata["broken_fks_count"] = len(broken_fks)
            metadata["deferred_updates_count"] = len(deferred_updates)

        output: dict[str, Any] = {
            "metadata": metadata,
            "tables": tables_data,
        }

        return json.dumps(
            output,
            cls=DatabaseTypeEncoder,
            indent=self.indent,
            ensure_ascii=False,
        )

    def _generate_per_table(
        self,
        tables_data: dict[str, list[dict[str, Any]]],
        insert_order: list[str],
        tables_schema: dict[str, Table],
    ) -> dict[str, str]:
        """
        Generate separate JSON string for each table.

        Format for each table:
        {
            "table": "table_name",
            "row_count": N,
            "rows": [row1, row2, ...]
        }

        Args:
            tables_data: Table data to serialize
            insert_order: Topologically sorted table names (for metadata)
            tables_schema: Table schemas (not included in output)

        Returns:
            Dict mapping table name to JSON string
        """
        result = {}

        for table_name, rows in tables_data.items():
            table_output = {
                "table": table_name,
                "row_count": len(rows),
                "rows": rows,
            }

            result[table_name] = json.dumps(
                table_output,
                cls=DatabaseTypeEncoder,
                indent=self.indent,
                ensure_ascii=False,
            )

        return result

    def write_to_file(
        self,
        output: str | dict[str, str],
        file_path: Path | str,
    ) -> None:
        """
        Write JSON output to file(s).

        Args:
            output: JSON output from generate()
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

            for table_name, json_str in output.items():
                table_file = file_path / f"{table_name}.json"
                table_file.write_text(json_str, encoding="utf-8")


def generate_json(
    tables_data: dict[str, list[dict[str, Any]]],
    insert_order: list[str],
    tables_schema: dict[str, Table],
    mode: str = "single",
    pretty: bool = True,
) -> str | dict[str, str]:
    """
    Convenience function to generate JSON output.

    Args:
        tables_data: Dict mapping table name to list of row dicts
        insert_order: Tables in topologically sorted order
        tables_schema: Dict mapping table name to Table schema
        mode: Output mode - "single" or "per-table"
        pretty: Enable pretty-printing

    Returns:
        JSON string (single mode) or dict of JSON strings (per-table mode)
    """
    generator = JSONGenerator(mode=mode, pretty=pretty)
    return generator.generate(tables_data, insert_order, tables_schema)
