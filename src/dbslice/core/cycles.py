from dataclasses import dataclass
from typing import Any

from dbslice.models import ForeignKey, SchemaGraph


@dataclass
class CycleInfo:
    """Information about a detected cycle."""

    tables: list[str]  # Tables in the cycle (ordered)
    fks_in_cycle: list[ForeignKey]  # All FKs participating in the cycle

    def __str__(self) -> str:
        """Human-readable cycle representation."""
        return " → ".join(self.tables + [self.tables[0]])


@dataclass
class CycleBreak:
    """Represents a foreign key chosen to break a cycle."""

    fk: ForeignKey
    strategy: str  # "nullable", "deferred", or "disable"


@dataclass
class DeferredUpdate:
    """Represents an UPDATE needed to restore a broken FK after INSERT."""

    table: str
    pk_columns: tuple[str, ...]
    pk_values: tuple[Any, ...]
    fk_column: str
    fk_value: Any

    def format_where_clause(self) -> str:
        """Generate WHERE clause for UPDATE statement."""
        conditions = [f"{col} = {val!r}" for col, val in zip(self.pk_columns, self.pk_values)]
        return " AND ".join(conditions)


def find_cycles_dfs(dependencies: dict[str, set[str]]) -> list[list[str]]:
    """
    Find all cycles in a dependency graph using depth-first search.

    This uses a recursion stack to detect back edges, which indicate cycles.

    Args:
        dependencies: Map of table -> set of tables it depends on

    Returns:
        List of cycles, where each cycle is a list of table names
    """
    cycles = []
    visited = set()
    rec_stack: list[str] = []

    def dfs(node: str) -> None:
        """DFS helper that tracks recursion stack for cycle detection."""
        if node in rec_stack:
            cycle_start = rec_stack.index(node)
            cycle = rec_stack[cycle_start:]
            cycles.append(cycle)
            return

        if node in visited:
            return

        visited.add(node)
        rec_stack.append(node)

        for neighbor in dependencies.get(node, set()):
            dfs(neighbor)

        rec_stack.pop()

    for node in dependencies:
        if node not in visited:
            dfs(node)

    return cycles


def identify_cycle_fks(schema: SchemaGraph, cycle: list[str]) -> list[ForeignKey]:
    """
    Identify foreign keys that form edges in the cycle path.

    Only returns FKs that correspond to edges in the cycle path, not
    all FKs between tables in the cycle. For example, given cycle
    [A, B], the path is A -> B -> A, so only FKs forming edges
    (A, B) or (B, A) are returned, not self-referential FKs like (A, A).

    Args:
        schema: Database schema graph
        cycle: List of tables forming a cycle (ordered path)

    Returns:
        List of ForeignKey objects that are edges in the cycle path
    """
    # Build set of directed edges in the cycle path
    # cycle [A, B, C] means path A -> B -> C -> A
    cycle_edges: set[tuple[str, str]] = set()
    for i in range(len(cycle)):
        source = cycle[i]
        target = cycle[(i + 1) % len(cycle)]
        cycle_edges.add((source, target))

    cycle_fks = []
    for fk in schema.edges:
        if (fk.source_table, fk.target_table) in cycle_edges:
            cycle_fks.append(fk)

    return cycle_fks


def select_nullable_fk_to_break(
    cycle_fks: list[ForeignKey], cycle: list[str] | None = None
) -> ForeignKey | None:
    """
    Select the best nullable FK to break a cycle.

    Selection criteria (in priority order):
    1. For single-table (self-loop) cycles, prefer self-referential FKs
    2. For multi-table cycles, prefer inter-table FKs (self-referential FKs
       don't break inter-table cycles)
    3. Prefer single-column FKs (simpler)
    4. Return first nullable FK found

    Args:
        cycle_fks: List of foreign keys in the cycle
        cycle: Optional list of table names in the cycle (used to determine
            if self-referential FKs are relevant)

    Returns:
        The selected ForeignKey to break, or None if no nullable FK exists
    """
    nullable_fks = [fk for fk in cycle_fks if fk.is_nullable]

    if not nullable_fks:
        return None

    is_single_table_cycle = cycle is not None and len(cycle) == 1

    if is_single_table_cycle:
        # For self-loop cycles, self-referential FKs are the right choice
        self_ref_fks = [fk for fk in nullable_fks if fk.is_self_referential]
        if self_ref_fks:
            return self_ref_fks[0]
    else:
        # For multi-table cycles, filter OUT self-referential FKs since breaking
        # them does not resolve the inter-table cycle
        inter_table_fks = [fk for fk in nullable_fks if not fk.is_self_referential]
        if inter_table_fks:
            single_col_fks = [fk for fk in inter_table_fks if len(fk.source_columns) == 1]
            if single_col_fks:
                return single_col_fks[0]
            return inter_table_fks[0]

    single_col_fks = [fk for fk in nullable_fks if len(fk.source_columns) == 1]
    if single_col_fks:
        return single_col_fks[0]

    return nullable_fks[0]


def break_cycles_at_nullable_fks(
    schema: SchemaGraph,
    tables: set[str],
    dependencies: dict[str, set[str]],
) -> tuple[list[ForeignKey], list[CycleInfo]]:
    """
    Detect cycles and identify nullable FKs to break them.

    This is the main cycle-breaking algorithm that:
    1. Detects all cycles in the dependency graph
    2. For each cycle, finds nullable FKs that can be used to break it
    3. Returns the list of FKs to break and detected cycles

    Args:
        schema: Database schema graph
        tables: Set of tables being extracted
        dependencies: Current dependency graph (table -> dependencies)

    Returns:
        Tuple of (fks_to_break, detected_cycles)

    Raises:
        ValueError: If a cycle has no nullable FK to break
    """
    cycles = find_cycles_dfs(dependencies)

    if not cycles:
        return [], []

    fks_to_break: list[ForeignKey] = []
    fks_to_break_set: set[ForeignKey] = set()
    cycle_infos = []

    for cycle in cycles:
        cycle_fks = identify_cycle_fks(schema, cycle)
        nullable_fk = select_nullable_fk_to_break(cycle_fks, cycle)

        if nullable_fk is None:
            # No nullable FK - cannot break this cycle with our strategy
            cycle_str = " → ".join(cycle + [cycle[0]])

            fk_details = []
            for fk in cycle_fks:
                nullable_str = "nullable" if fk.is_nullable else "NOT NULL"
                fk_details.append(
                    f"  - {fk.source_table}.{', '.join(fk.source_columns)} → "
                    f"{fk.target_table}.{', '.join(fk.target_columns)} ({nullable_str})"
                )

            raise ValueError(
                f"Circular dependency detected with no nullable foreign key to break.\n\n"
                f"Cycle path: {cycle_str}\n\n"
                f"Foreign keys in cycle:\n" + "\n".join(fk_details) + "\n\n"
                f"To resolve this issue, you can:\n"
                f"  1. Make one of the foreign keys nullable in your database schema\n"
                f"  2. Use PostgreSQL's deferred constraints (if using PostgreSQL)\n"
                f"  3. Temporarily disable FK checks (use at your own risk)\n\n"
                f"Recommended: Make {cycle_fks[0].source_table}.{', '.join(cycle_fks[0].source_columns)} nullable."
            )

        # Avoid duplicate FK breaks when multiple cycles share the same FK
        if nullable_fk not in fks_to_break_set:
            fks_to_break.append(nullable_fk)
            fks_to_break_set.add(nullable_fk)
        cycle_infos.append(CycleInfo(tables=cycle, fks_in_cycle=cycle_fks))

    return fks_to_break, cycle_infos


def build_deferred_updates(
    fks_to_break: list[ForeignKey],
    tables_data: dict[str, list[dict[str, Any]]],
    schema: SchemaGraph,
) -> list[DeferredUpdate]:
    """
    Build UPDATE statements needed to restore broken FK values.

    For each row in tables with broken FKs, generates an UPDATE statement
    that will set the FK column to its actual value after all INSERTs complete.

    Args:
        fks_to_break: List of ForeignKey objects that were broken
        tables_data: Extracted row data (table -> list of row dicts)
        schema: Database schema graph

    Returns:
        List of DeferredUpdate objects describing the UPDATE statements needed
    """
    deferred_updates = []

    for fk in fks_to_break:
        source_table = fk.source_table
        fk_columns = fk.source_columns

        if source_table not in tables_data:
            continue

        table_info = schema.get_table(source_table)
        if not table_info:
            continue

        pk_columns = table_info.primary_key

        for row_data in tables_data[source_table]:
            for fk_col in fk_columns:
                if fk_col not in row_data:
                    continue

                fk_value = row_data[fk_col]

                # Skip if value is already NULL (no update needed)
                if fk_value is None:
                    continue

                pk_values = tuple(row_data[col] for col in pk_columns)

                deferred_updates.append(
                    DeferredUpdate(
                        table=source_table,
                        pk_columns=pk_columns,
                        pk_values=pk_values,
                        fk_column=fk_col,
                        fk_value=fk_value,
                    )
                )

    return deferred_updates
