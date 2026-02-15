from collections import deque
from dataclasses import dataclass, field
from typing import Any

from dbslice.adapters.base import DatabaseAdapter
from dbslice.config import TraversalDirection
from dbslice.constants import DEFAULT_TRAVERSAL_DEPTH
from dbslice.logging import get_logger
from dbslice.models import ForeignKey, SchemaGraph

logger = get_logger(__name__)


@dataclass
class TraversalConfig:
    """Configuration for graph traversal."""

    max_depth: int = DEFAULT_TRAVERSAL_DEPTH
    direction: TraversalDirection = TraversalDirection.BOTH
    exclude_tables: set[str] = field(default_factory=set)
    passthrough_tables: set[str] = field(default_factory=set)


@dataclass
class TraversalResult:
    """Result of graph traversal - records to extract organized by table."""

    # table_name -> set of PK tuples
    records: dict[str, set[tuple[Any, ...]]] = field(default_factory=dict)

    # For debugging: traversal path showing how records were discovered
    traversal_path: list[str] = field(default_factory=list)

    # Statistics
    tables_visited: set[str] = field(default_factory=set)

    def add_records(self, table: str, pk_values: set[tuple[Any, ...]]) -> set[tuple[Any, ...]]:
        """
        Add records for a table and return only the new ones.

        Returns the set of PK values that were actually new (not already tracked).
        """
        if table not in self.records:
            self.records[table] = set()

        new_pks = pk_values - self.records[table]
        self.records[table].update(new_pks)
        return new_pks

    def get_records(self, table: str) -> set[tuple[Any, ...]]:
        """Get all PK values for a table."""
        return self.records.get(table, set())

    def total_records(self) -> int:
        """Get total number of records across all tables."""
        return sum(len(pks) for pks in self.records.values())

    def table_count(self) -> int:
        """Get number of tables with records."""
        return len(self.records)


class GraphTraverser:
    """
    Traverses FK relationships to find all related records.

    Uses BFS to explore the schema graph starting from seed records,
    following foreign key relationships to discover dependent and
    depended-upon records.
    """

    def __init__(self, schema: SchemaGraph, adapter: DatabaseAdapter):
        self.schema = schema
        self.adapter = adapter

    def traverse(
        self,
        seed_table: str,
        seed_pks: set[tuple[Any, ...]],
        config: TraversalConfig,
    ) -> TraversalResult:
        """
        Perform BFS traversal from seed records following FK relationships.

        Args:
            seed_table: Table containing seed records
            seed_pks: Set of primary key tuples for seed records
            config: Traversal configuration

        Returns:
            TraversalResult containing all discovered records

        Direction behavior:
        - UP: Follow FKs to parent tables (ensure referential integrity)
        - DOWN: Follow FKs from child tables (include dependent data)
        - BOTH: Both directions (default)
        """
        logger.debug(
            "Starting graph traversal",
            seed_table=seed_table,
            seed_count=len(seed_pks),
            max_depth=config.max_depth,
            direction=config.direction.value,
        )

        result = TraversalResult()
        result.add_records(seed_table, seed_pks)
        result.tables_visited.add(seed_table)
        result.traversal_path.append(f"seed: {seed_table} ({len(seed_pks)} rows)")

        # Queue: (table, pk_set, depth, direction)
        queue: deque[tuple[str, set[tuple[Any, ...]], int, str]] = deque()

        if config.direction in (TraversalDirection.UP, TraversalDirection.BOTH):
            queue.append((seed_table, seed_pks, 0, "up"))

        if config.direction in (TraversalDirection.DOWN, TraversalDirection.BOTH):
            queue.append((seed_table, seed_pks, 0, "down"))

        # Track visited in each direction to avoid re-processing
        visited_up: dict[str, set[tuple[Any, ...]]] = {seed_table: seed_pks.copy()}
        visited_down: dict[str, set[tuple[Any, ...]]] = {seed_table: seed_pks.copy()}

        while queue:
            table, pks, depth, direction = queue.popleft()

            if depth >= config.max_depth and direction == "down":
                logger.debug(
                    "Max depth reached for downward traversal, skipping",
                    table=table,
                    depth=depth,
                    max_depth=config.max_depth,
                )
                continue

            if direction == "up":
                self._traverse_up(
                    table, pks, depth, config, result, visited_up, visited_down, queue
                )
            elif direction == "down":
                self._traverse_down(
                    table, pks, depth, config, result, visited_down, visited_up, queue
                )

        # Handle passthrough tables - include ALL rows regardless of FK relationships
        if config.passthrough_tables:
            logger.debug(
                "Processing passthrough tables",
                count=len(config.passthrough_tables),
            )
            self._process_passthrough_tables(
                config.passthrough_tables, result, config.exclude_tables
            )

        logger.info(
            "Graph traversal complete",
            total_records=result.total_records(),
            tables_visited=len(result.tables_visited),
            tables_affected=result.table_count(),
        )

        return result

    def _traverse_up(
        self,
        table: str,
        pks: set[tuple[Any, ...]],
        depth: int,
        config: TraversalConfig,
        result: TraversalResult,
        visited_up: dict[str, set[tuple[Any, ...]]],
        visited_down: dict[str, set[tuple[Any, ...]]],
        queue: deque,
    ) -> None:
        """Traverse upward to parent tables (tables this one references via FK)."""
        for parent_table, fk in self.schema.get_parents(table):
            # Skip excluded tables
            if parent_table in config.exclude_tables:
                logger.debug("Skipping excluded table", table=parent_table, direction="up")
                continue

            parent_pks = self._fetch_parent_pks(table, fk, pks)

            if not parent_pks:
                continue

            # Find new PKs not yet visited
            already_visited = visited_up.get(parent_table, set())
            new_pks = parent_pks - already_visited

            if not new_pks:
                continue

            visited_up.setdefault(parent_table, set()).update(new_pks)
            result.add_records(parent_table, new_pks)
            result.tables_visited.add(parent_table)

            fk_type = "virtual" if self.schema.is_virtual_fk(fk) else "up"
            result.traversal_path.append(
                f"{table} --({fk_type}:{fk.name})--> {parent_table} ({len(new_pks)} rows)"
            )

            logger.debug(
                "Traversed FK to parent",
                from_table=table,
                to_table=parent_table,
                fk_name=fk.name,
                is_virtual=self.schema.is_virtual_fk(fk),
                new_records=len(new_pks),
                depth=depth,
            )

            queue.append((parent_table, new_pks, depth + 1, "up"))

    def _traverse_down(
        self,
        table: str,
        pks: set[tuple[Any, ...]],
        depth: int,
        config: TraversalConfig,
        result: TraversalResult,
        visited_down: dict[str, set[tuple[Any, ...]]],
        visited_up: dict[str, set[tuple[Any, ...]]],
        queue: deque,
    ) -> None:
        """Traverse downward to child tables (tables that reference this one via FK)."""
        for child_table, fk in self.schema.get_children(table):
            # Skip excluded tables
            if child_table in config.exclude_tables:
                logger.debug("Skipping excluded table", table=child_table, direction="down")
                continue

            child_pks = self._fetch_child_pks(fk, pks)

            if not child_pks:
                continue

            # Find new PKs not yet visited
            already_visited = visited_down.get(child_table, set())
            new_pks = child_pks - already_visited

            if not new_pks:
                continue

            visited_down.setdefault(child_table, set()).update(new_pks)
            result.add_records(child_table, new_pks)
            result.tables_visited.add(child_table)

            fk_type = "virtual" if self.schema.is_virtual_fk(fk) else "down"
            result.traversal_path.append(
                f"{table} --({fk_type}:{fk.name})--> {child_table} ({len(new_pks)} rows)"
            )

            logger.debug(
                "Traversed FK to child",
                from_table=table,
                to_table=child_table,
                fk_name=fk.name,
                is_virtual=self.schema.is_virtual_fk(fk),
                new_records=len(new_pks),
                depth=depth,
            )

            queue.append((child_table, new_pks, depth + 1, "down"))

            # IMPORTANT: When going down, also traverse up from children
            # to ensure referential integrity for the child records.
            # This ensures all parents of child records are included.
            if child_table not in visited_up:
                visited_up[child_table] = set()
            new_for_up = new_pks - visited_up[child_table]
            if new_for_up:
                visited_up[child_table].update(new_for_up)
                logger.debug(
                    "Scheduling upward traversal from child for referential integrity",
                    table=child_table,
                    records=len(new_for_up),
                )
                queue.append((child_table, new_for_up, depth + 1, "up"))

    def _fetch_parent_pks(
        self,
        child_table: str,
        fk: ForeignKey,
        child_pks: set[tuple[Any, ...]],
    ) -> set[tuple[Any, ...]]:
        """
        Fetch PKs of parent records referenced by child records.

        Given child PKs, fetch the FK column values which correspond to
        parent PKs in the target table.
        """
        return self.adapter.fetch_fk_values(child_table, fk, child_pks)

    def _fetch_child_pks(
        self,
        fk: ForeignKey,
        parent_pks: set[tuple[Any, ...]],
    ) -> set[tuple[Any, ...]]:
        """
        Fetch PKs of child records that reference the given parent records.

        Given parent PKs, find all child records whose FK points to them.
        """
        return self.adapter.fetch_referencing_pks(fk, parent_pks)

    def _process_passthrough_tables(
        self,
        passthrough_tables: set[str],
        result: TraversalResult,
        exclude_tables: set[str],
    ) -> None:
        """
        Process passthrough tables - add ALL rows from these tables.

        Passthrough tables are included in full, regardless of FK relationships.
        Common use cases:
        - Configuration tables (site settings, feature flags)
        - Lookup tables (countries, currencies, status codes)
        - Small reference tables (categories, tags)
        - Django system tables (content_type, site, migrations)

        Args:
            passthrough_tables: Set of table names to include in full
            result: TraversalResult to update with passthrough table records
            exclude_tables: Set of table names to exclude (takes precedence over passthrough)
        """
        for table in passthrough_tables:
            if table in exclude_tables:
                logger.debug(
                    "Skipping passthrough table that is excluded",
                    table=table,
                )
                continue

            if not self.schema.has_table(table):
                logger.warning(
                    "Passthrough table not found in schema, skipping",
                    table=table,
                )
                continue

            table_info = self.schema.get_table(table)
            if not table_info:
                continue

            pk_columns = table_info.primary_key
            if not pk_columns:
                logger.warning(
                    "Passthrough table has no primary key, skipping",
                    table=table,
                )
                continue

            logger.debug("Fetching all rows from passthrough table", table=table)
            all_pks = self.adapter.fetch_all_pks(table, pk_columns)

            if not all_pks:
                logger.debug("Passthrough table is empty", table=table)
                continue

            new_pks = result.add_records(table, all_pks)
            result.tables_visited.add(table)
            result.traversal_path.append(
                f"passthrough: {table} ({len(all_pks)} rows total, {len(new_pks)} new)"
            )

            logger.info(
                "Processed passthrough table",
                table=table,
                total_rows=len(all_pks),
                new_rows=len(new_pks),
            )


def simple_traverse_up(
    adapter: DatabaseAdapter,
    schema: SchemaGraph,
    seed_table: str,
    seed_pks: set[tuple[Any, ...]],
    max_depth: int = DEFAULT_TRAVERSAL_DEPTH,
) -> TraversalResult:
    """
    Simple helper for upward-only traversal.

    This is useful for the MVP where we only need to ensure referential
    integrity by including all parent records.
    """
    config = TraversalConfig(
        max_depth=max_depth,
        direction=TraversalDirection.UP,
    )
    traverser = GraphTraverser(schema, adapter)
    return traverser.traverse(seed_table, seed_pks, config)
