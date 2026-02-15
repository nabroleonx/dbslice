import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any


@dataclass
class QueryStats:
    """Statistics for a single database query."""

    query: str
    params_count: int
    duration_ms: float
    rows_returned: int
    table: str | None = None
    operation: str | None = None  # "fetch_rows", "fetch_by_pk", "fetch_fk_values", etc.

    def __str__(self) -> str:
        """Format query stats for display."""
        table_str = f" [{self.table}]" if self.table else ""
        op_str = f" ({self.operation})" if self.operation else ""
        return (
            f"{self.duration_ms:.2f}ms{table_str}{op_str} - "
            f"{self.rows_returned} rows, {self.params_count} params - "
            f"{self.query[:100]}"
        )


class QueryProfiler:
    """
    Tracks and analyzes database query performance.

    Usage:
        profiler = QueryProfiler()

        # Track queries
        with profiler.track_query("SELECT * FROM users WHERE id = %s", table="users"):
            # Execute query
            rows = execute_query()
            profiler.record_rows(len(rows))

        # Get summary
        summary = profiler.get_summary()
    """

    def __init__(self):
        self.queries: list[QueryStats] = []
        self._current_query: dict[str, Any] | None = None
        self._start_time: float = 0.0
        self.enabled = True

    def start_query(
        self,
        query: str,
        params_count: int = 0,
        table: str | None = None,
        operation: str | None = None,
    ) -> None:
        """Start tracking a new query."""
        if not self.enabled:
            return

        self._start_time = time.perf_counter()
        self._current_query = {
            "query": query,
            "params_count": params_count,
            "table": table,
            "operation": operation,
            "rows_returned": 0,
        }

    def end_query(self, rows_returned: int = 0) -> None:
        """End tracking the current query."""
        if not self.enabled or self._current_query is None:
            return

        duration_ms = (time.perf_counter() - self._start_time) * 1000
        self._current_query["rows_returned"] = rows_returned

        stats = QueryStats(
            query=self._current_query["query"],
            params_count=self._current_query["params_count"],
            duration_ms=duration_ms,
            rows_returned=rows_returned,
            table=self._current_query["table"],
            operation=self._current_query["operation"],
        )
        self.queries.append(stats)
        self._current_query = None

    def track_query(
        self,
        query: str,
        params_count: int = 0,
        table: str | None = None,
        operation: str | None = None,
    ) -> "QueryTracker":
        """
        Context manager for tracking a query.

        Usage:
            with profiler.track_query("SELECT ...", table="users") as tracker:
                rows = execute_query()
                tracker.record_rows(len(rows))
        """
        return QueryTracker(self, query, params_count, table, operation)

    def get_summary(self) -> "ProfileSummary":
        """Get a summary of all tracked queries."""
        return ProfileSummary(self.queries)

    def reset(self) -> None:
        """Clear all tracked queries."""
        self.queries.clear()
        self._current_query = None

    def disable(self) -> None:
        """Disable query profiling."""
        self.enabled = False

    def enable(self) -> None:
        """Enable query profiling."""
        self.enabled = True


class QueryTracker:
    """Context manager for tracking a single query."""

    def __init__(
        self,
        profiler: QueryProfiler,
        query: str,
        params_count: int,
        table: str | None,
        operation: str | None,
    ):
        self.profiler = profiler
        self.query = query
        self.params_count = params_count
        self.table = table
        self.operation = operation
        self.rows_returned = 0

    def __enter__(self) -> "QueryTracker":
        self.profiler.start_query(self.query, self.params_count, self.table, self.operation)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.profiler.end_query(self.rows_returned)
        return False

    def record_rows(self, count: int) -> None:
        """Record the number of rows returned by the query."""
        self.rows_returned = count


@dataclass
class ProfileSummary:
    """Summary statistics for all tracked queries."""

    queries: list[QueryStats]

    def __post_init__(self):
        """Calculate summary statistics."""
        self.total_queries = len(self.queries)
        self.total_duration_ms = sum(q.duration_ms for q in self.queries)
        self.total_rows = sum(q.rows_returned for q in self.queries)
        self.avg_duration_ms = (
            self.total_duration_ms / self.total_queries if self.total_queries > 0 else 0
        )

        # Queries by table
        self._queries_by_table: dict[str, list[QueryStats]] = defaultdict(list)
        for q in self.queries:
            if q.table:
                self._queries_by_table[q.table].append(q)

        # Queries by operation
        self._queries_by_operation: dict[str, list[QueryStats]] = defaultdict(list)
        for q in self.queries:
            if q.operation:
                self._queries_by_operation[q.operation].append(q)

    def get_slowest_queries(self, n: int = 10) -> list[QueryStats]:
        """Get the N slowest queries."""
        return sorted(self.queries, key=lambda q: q.duration_ms, reverse=True)[:n]

    def get_queries_by_table(self, table: str) -> list[QueryStats]:
        """Get all queries for a specific table."""
        return self._queries_by_table.get(table, [])

    def get_table_stats(self) -> dict[str, dict[str, Any]]:
        """
        Get statistics grouped by table.

        Returns:
            Dict mapping table names to their query statistics
        """
        stats = {}
        for table, queries in self._queries_by_table.items():
            stats[table] = {
                "query_count": len(queries),
                "total_duration_ms": sum(q.duration_ms for q in queries),
                "total_rows": sum(q.rows_returned for q in queries),
                "avg_duration_ms": sum(q.duration_ms for q in queries) / len(queries),
            }
        return stats

    def get_operation_stats(self) -> dict[str, dict[str, Any]]:
        """
        Get statistics grouped by operation type.

        Returns:
            Dict mapping operation names to their query statistics
        """
        stats = {}
        for operation, queries in self._queries_by_operation.items():
            stats[operation] = {
                "query_count": len(queries),
                "total_duration_ms": sum(q.duration_ms for q in queries),
                "total_rows": sum(q.rows_returned for q in queries),
                "avg_duration_ms": sum(q.duration_ms for q in queries) / len(queries),
            }
        return stats

    def format_summary(self, show_slowest: int = 5) -> str:
        """
        Format a human-readable summary.

        Args:
            show_slowest: Number of slowest queries to display

        Returns:
            Formatted summary string
        """
        lines = []
        lines.append("=" * 80)
        lines.append("QUERY PERFORMANCE PROFILE")
        lines.append("=" * 80)
        lines.append("")

        # Overall stats
        lines.append("Overall Statistics:")
        lines.append(f"  Total queries:   {self.total_queries}")
        lines.append(f"  Total duration:  {self.total_duration_ms:.2f} ms")
        lines.append(f"  Total rows:      {self.total_rows}")
        lines.append(f"  Avg query time:  {self.avg_duration_ms:.2f} ms")
        lines.append("")

        # Queries by operation
        if self._queries_by_operation:
            lines.append("Queries by Operation:")
            op_stats = self.get_operation_stats()
            for op, stats in sorted(
                op_stats.items(), key=lambda x: x[1]["total_duration_ms"], reverse=True
            ):
                lines.append(
                    f"  {op:25s} {stats['query_count']:4d} queries  "
                    f"{stats['total_duration_ms']:8.2f} ms  "
                    f"{stats['total_rows']:6d} rows"
                )
            lines.append("")

        # Queries by table
        if self._queries_by_table:
            lines.append("Queries by Table:")
            table_stats = self.get_table_stats()
            for table, stats in sorted(
                table_stats.items(), key=lambda x: x[1]["total_duration_ms"], reverse=True
            ):
                lines.append(
                    f"  {table:25s} {stats['query_count']:4d} queries  "
                    f"{stats['total_duration_ms']:8.2f} ms  "
                    f"{stats['total_rows']:6d} rows"
                )
            lines.append("")

        # Slowest queries
        if show_slowest > 0 and self.queries:
            lines.append(f"Top {show_slowest} Slowest Queries:")
            for i, q in enumerate(self.get_slowest_queries(show_slowest), 1):
                lines.append(f"  {i}. {q}")
            lines.append("")

        lines.append("Performance Insights:")
        if self.total_queries > 100:
            lines.append(f"  ⚠ High query count ({self.total_queries}) - consider batching")
        if self.avg_duration_ms > 100:
            lines.append(f"  ⚠ High average query time ({self.avg_duration_ms:.2f}ms)")

        # Check for N+1 patterns
        op_stats = self.get_operation_stats()
        if "fetch_fk_values" in op_stats:
            fk_count = op_stats["fetch_fk_values"]["query_count"]
            if fk_count > 10:
                lines.append(f"  ⚠ Potential N+1 query pattern: {fk_count} FK fetch queries")

        if "fetch_referencing_pks" in op_stats:
            ref_count = op_stats["fetch_referencing_pks"]["query_count"]
            if ref_count > 10:
                lines.append(f"  ⚠ Potential N+1 query pattern: {ref_count} referencing PK queries")

        lines.append("=" * 80)

        return "\n".join(lines)
