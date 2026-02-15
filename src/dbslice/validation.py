"""Post-extraction validation of referential integrity.

Checks that extracted data maintains FK relationships -- i.e., every foreign key
reference points to a record that is included in the extraction. Distinct from
input_validators.py, which validates user-provided CLI arguments and parameters.
"""

from dataclasses import dataclass, field
from typing import Any

from dbslice.logging import get_logger
from dbslice.models import ForeignKey, SchemaGraph

logger = get_logger(__name__)


@dataclass
class OrphanedRecord:
    """Represents a record with a missing parent reference."""

    table: str
    pk_values: tuple[Any, ...]
    fk_name: str
    fk_columns: tuple[str, ...]
    fk_values: tuple[Any, ...]
    parent_table: str
    parent_pk_columns: tuple[str, ...]

    def __str__(self) -> str:
        """Human-readable representation."""
        pk_str = ", ".join(f"{col}={val}" for col, val in zip(self._get_pk_cols(), self.pk_values))
        fk_str = ", ".join(f"{col}={val}" for col, val in zip(self.fk_columns, self.fk_values))
        return (
            f"{self.table}({pk_str}) -> "
            f"{self.parent_table}({fk_str}) via FK '{self.fk_name}' - parent not found"
        )

    def _get_pk_cols(self) -> tuple[str, ...]:
        """Get primary key column names (used for display)."""
        # This is a simplified version - in real usage the validator has access to schema
        return ("id",)


@dataclass
class ValidationResult:
    """
    Result of extraction validation.

    Contains information about any referential integrity issues found.
    """

    is_valid: bool = True
    orphaned_records: list[OrphanedRecord] = field(default_factory=list)
    broken_fks: list[ForeignKey] = field(default_factory=list)
    total_records_checked: int = 0
    total_fk_checks: int = 0

    def add_orphan(self, orphan: OrphanedRecord) -> None:
        """Add an orphaned record to the results."""
        self.orphaned_records.append(orphan)
        self.is_valid = False

    def format_report(self) -> str:
        """
        Format a human-readable validation report.

        Returns:
            Multi-line string with validation results
        """
        lines = []
        lines.append("=" * 80)
        lines.append("EXTRACTION VALIDATION REPORT")
        lines.append("=" * 80)
        lines.append("")

        lines.append(f"Records checked: {self.total_records_checked}")
        lines.append(f"Foreign key checks performed: {self.total_fk_checks}")
        lines.append("")

        if self.broken_fks:
            lines.append(f"Intentionally broken FKs (for cycles): {len(self.broken_fks)}")
            for fk in self.broken_fks:
                fk_desc = (
                    f"{fk.source_table}.{', '.join(fk.source_columns)} -> "
                    f"{fk.target_table}.{', '.join(fk.target_columns)}"
                )
                lines.append(f"  - {fk_desc} (FK: {fk.name})")
            lines.append("")

        if self.is_valid:
            lines.append("Status: VALID")
            lines.append("All foreign key references point to included records.")
        else:
            lines.append("Status: INVALID")
            lines.append(f"Found {len(self.orphaned_records)} orphaned record(s):")
            lines.append("")

            orphans_by_table: dict[str, list[OrphanedRecord]] = {}
            for orphan in self.orphaned_records:
                if orphan.table not in orphans_by_table:
                    orphans_by_table[orphan.table] = []
                orphans_by_table[orphan.table].append(orphan)

            for table, orphans in sorted(orphans_by_table.items()):
                lines.append(f"Table: {table} ({len(orphans)} orphaned)")
                for orphan in orphans:
                    lines.append(f"  - {orphan}")
                lines.append("")

        lines.append("=" * 80)
        return "\n".join(lines)


class ExtractionValidator:
    """
    Validates extracted data for referential integrity.

    Checks that all foreign key references point to records that are
    included in the extraction, preventing import failures.
    """

    def __init__(self, schema: SchemaGraph):
        """
        Initialize validator with schema.

        Args:
            schema: Database schema with tables and foreign keys
        """
        self.schema = schema
        logger.debug("ExtractionValidator initialized")

    def validate(
        self,
        tables: dict[str, list[dict[str, Any]]],
        broken_fks: list[ForeignKey] | None = None,
    ) -> ValidationResult:
        """
        Validate extracted data for referential integrity.

        Args:
            tables: Extracted data organized by table name
            broken_fks: List of FKs that were intentionally broken for cycles

        Returns:
            ValidationResult with detailed information about any issues found
        """
        logger.info(
            "Starting extraction validation",
            table_count=len(tables),
            broken_fk_count=len(broken_fks) if broken_fks else 0,
        )

        result = ValidationResult(broken_fks=broken_fks or [])
        broken_fk_set = set(broken_fks) if broken_fks else set()

        pk_index = self._build_pk_index(tables)
        logger.debug(
            "Built PK index",
            table_count=len(pk_index),
            total_pks=sum(len(pks) for pks in pk_index.values()),
        )

        # Validate each table's FK references
        for table_name, rows in tables.items():
            table_info = self.schema.get_table(table_name)
            if not table_info:
                logger.warning("Table not found in schema during validation", table=table_name)
                continue

            result.total_records_checked += len(rows)

            # Get all FKs for this table
            parents = self.schema.get_parents(table_name)

            for row in rows:
                pk_values = self._extract_pk_values(row, table_info.primary_key)

                # Check each FK relationship
                for parent_table, fk in parents:
                    # Skip broken FKs (intentional for cycle handling)
                    if fk in broken_fk_set:
                        logger.debug(
                            "Skipping validation for broken FK",
                            fk_name=fk.name,
                            source_table=fk.source_table,
                            target_table=fk.target_table,
                        )
                        continue

                    result.total_fk_checks += 1

                    # Extract FK values from the row
                    fk_values = self._extract_fk_values(row, fk.source_columns)

                    # Skip NULL FK values (nullable FKs are valid when NULL)
                    if any(v is None for v in fk_values):
                        logger.debug(
                            "Skipping NULL FK value",
                            table=table_name,
                            fk_name=fk.name,
                        )
                        continue

                    # Check if parent record exists in extraction
                    if not self._has_parent_record(parent_table, fk_values, pk_index):
                        orphan = OrphanedRecord(
                            table=table_name,
                            pk_values=pk_values,
                            fk_name=fk.name,
                            fk_columns=fk.source_columns,
                            fk_values=fk_values,
                            parent_table=parent_table,
                            parent_pk_columns=fk.target_columns,
                        )
                        result.add_orphan(orphan)
                        logger.warning(
                            "Orphaned record detected",
                            table=table_name,
                            pk_values=pk_values,
                            parent_table=parent_table,
                            fk_name=fk.name,
                            fk_values=fk_values,
                        )

        logger.info(
            "Validation complete",
            is_valid=result.is_valid,
            orphaned_count=len(result.orphaned_records),
            records_checked=result.total_records_checked,
            fk_checks=result.total_fk_checks,
        )

        return result

    def _build_pk_index(
        self,
        tables: dict[str, list[dict[str, Any]]],
    ) -> dict[str, set[tuple[Any, ...]]]:
        """
        Build an index of all primary keys for fast lookup.

        Args:
            tables: Extracted data organized by table name

        Returns:
            Dictionary mapping table name to set of PK tuples
        """
        index: dict[str, set[tuple[Any, ...]]] = {}

        for table_name, rows in tables.items():
            table_info = self.schema.get_table(table_name)
            if not table_info:
                continue

            pk_columns = table_info.primary_key
            pks = set()

            for row in rows:
                pk_tuple = self._extract_pk_values(row, pk_columns)
                pks.add(pk_tuple)

            index[table_name] = pks

        return index

    def _extract_pk_values(
        self,
        row: dict[str, Any],
        pk_columns: tuple[str, ...],
    ) -> tuple[Any, ...]:
        """
        Extract primary key values from a row.

        Args:
            row: Data row dictionary
            pk_columns: Primary key column names

        Returns:
            Tuple of PK values
        """
        return tuple(row[col] for col in pk_columns)

    def _extract_fk_values(
        self,
        row: dict[str, Any],
        fk_columns: tuple[str, ...],
    ) -> tuple[Any, ...]:
        """
        Extract foreign key values from a row.

        Args:
            row: Data row dictionary
            fk_columns: Foreign key column names

        Returns:
            Tuple of FK values
        """
        return tuple(row.get(col) for col in fk_columns)

    def _has_parent_record(
        self,
        parent_table: str,
        fk_values: tuple[Any, ...],
        pk_index: dict[str, set[tuple[Any, ...]]],
    ) -> bool:
        """
        Check if parent record exists in the extraction.

        Args:
            parent_table: Name of the parent table
            fk_values: FK values to look up
            pk_index: Index of all PKs in the extraction

        Returns:
            True if parent record exists, False otherwise
        """
        if parent_table not in pk_index:
            return False

        return fk_values in pk_index[parent_table]
