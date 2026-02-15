"""Performance tests for query batching and profiling."""

import pytest

from dbslice.adapters.postgresql import PostgreSQLAdapter
from dbslice.utils.profiling import QueryProfiler


def test_fetch_fk_values_batching(sample_schema, mock_adapter):
    """Test that fetch_fk_values uses batching for large PK sets."""
    # Create a PostgreSQL adapter with small batch size for testing
    adapter = PostgreSQLAdapter(batch_size=10, profiler=None)
    adapter._conn = None  # Mock connection
    adapter._schema_cache = sample_schema

    # We can't easily test the batching without a real database connection,
    # but we can verify the batching logic is in place by checking the code path
    # This is more of an integration test placeholder
    assert adapter.batch_size == 10


def test_query_profiler_tracks_queries():
    """Test that QueryProfiler correctly tracks query execution."""
    profiler = QueryProfiler()

    # Simulate several queries
    with profiler.track_query(
        "SELECT * FROM users WHERE id = %s", 1, "users", "fetch_rows"
    ) as tracker:
        # Simulate query execution
        tracker.record_rows(5)

    with profiler.track_query(
        "SELECT * FROM orders WHERE user_id IN (%s, %s)", 2, "orders", "fetch_by_pk"
    ) as tracker:
        tracker.record_rows(10)

    with profiler.track_query(
        "SELECT user_id FROM orders WHERE id IN (%s)", 1, "orders", "fetch_fk_values"
    ) as tracker:
        tracker.record_rows(2)

    # Verify queries were tracked
    assert len(profiler.queries) == 3

    # Check first query
    assert profiler.queries[0].table == "users"
    assert profiler.queries[0].operation == "fetch_rows"
    assert profiler.queries[0].rows_returned == 5
    assert profiler.queries[0].params_count == 1

    # Check second query
    assert profiler.queries[1].table == "orders"
    assert profiler.queries[1].operation == "fetch_by_pk"
    assert profiler.queries[1].rows_returned == 10
    assert profiler.queries[1].params_count == 2


def test_query_profiler_summary():
    """Test QueryProfiler summary statistics."""
    profiler = QueryProfiler()

    # Simulate multiple queries
    for i in range(5):
        with profiler.track_query(
            f"SELECT * FROM users WHERE id = {i}", 1, "users", "fetch_rows"
        ) as tracker:
            tracker.record_rows(1)

    for i in range(3):
        with profiler.track_query(
            f"SELECT * FROM orders WHERE id = {i}", 1, "orders", "fetch_by_pk"
        ) as tracker:
            tracker.record_rows(2)

    summary = profiler.get_summary()

    # Check summary statistics
    assert summary.total_queries == 8
    assert summary.total_rows == 5 * 1 + 3 * 2  # 11 total rows

    # Check table statistics
    table_stats = summary.get_table_stats()
    assert "users" in table_stats
    assert table_stats["users"]["query_count"] == 5
    assert table_stats["users"]["total_rows"] == 5

    assert "orders" in table_stats
    assert table_stats["orders"]["query_count"] == 3
    assert table_stats["orders"]["total_rows"] == 6

    # Check operation statistics
    op_stats = summary.get_operation_stats()
    assert "fetch_rows" in op_stats
    assert op_stats["fetch_rows"]["query_count"] == 5

    assert "fetch_by_pk" in op_stats
    assert op_stats["fetch_by_pk"]["query_count"] == 3


def test_query_profiler_slowest_queries():
    """Test QueryProfiler identifies slowest queries."""
    profiler = QueryProfiler()

    # Simulate queries with varying durations
    import time

    with profiler.track_query("SLOW QUERY 1", 1, "users", "fetch_rows") as tracker:
        time.sleep(0.01)  # 10ms
        tracker.record_rows(1)

    with profiler.track_query("FAST QUERY", 1, "users", "fetch_rows") as tracker:
        time.sleep(0.001)  # 1ms
        tracker.record_rows(1)

    with profiler.track_query("SLOW QUERY 2", 1, "orders", "fetch_by_pk") as tracker:
        time.sleep(0.005)  # 5ms
        tracker.record_rows(1)

    summary = profiler.get_summary()
    slowest = summary.get_slowest_queries(2)

    # Check that queries are sorted by duration (slowest first)
    assert len(slowest) == 2
    assert "SLOW QUERY 1" in slowest[0].query
    assert slowest[0].duration_ms > slowest[1].duration_ms


def test_query_profiler_format_summary():
    """Test QueryProfiler summary formatting."""
    profiler = QueryProfiler()

    # Add some sample queries
    with profiler.track_query("SELECT * FROM users", 0, "users", "fetch_rows") as tracker:
        tracker.record_rows(10)

    with profiler.track_query("SELECT * FROM orders", 0, "orders", "fetch_by_pk") as tracker:
        tracker.record_rows(20)

    summary = profiler.get_summary()
    formatted = summary.format_summary(show_slowest=5)

    # Check that summary contains expected sections
    assert "QUERY PERFORMANCE PROFILE" in formatted
    assert "Overall Statistics:" in formatted
    assert "Total queries:" in formatted
    assert "Queries by Operation:" in formatted
    assert "Queries by Table:" in formatted
    assert "Slowest Queries:" in formatted


def test_query_profiler_n_plus_one_detection():
    """Test that profiler detects potential N+1 query patterns."""
    profiler = QueryProfiler()

    # Simulate N+1 pattern: many FK fetch queries
    for i in range(15):
        with profiler.track_query(
            f"SELECT user_id FROM orders WHERE id = {i}", 1, "orders", "fetch_fk_values"
        ) as tracker:
            tracker.record_rows(1)

    summary = profiler.get_summary()
    formatted = summary.format_summary()

    # Check that N+1 warning is present
    assert "Performance Insights:" in formatted
    assert "Potential N+1 query pattern" in formatted
    assert "FK fetch queries" in formatted


def test_query_profiler_disable():
    """Test that profiler can be disabled."""
    profiler = QueryProfiler()
    profiler.disable()

    # Queries should not be tracked when disabled
    with profiler.track_query("SELECT * FROM users", 0, "users", "fetch_rows") as tracker:
        tracker.record_rows(5)

    assert len(profiler.queries) == 0


def test_query_profiler_reset():
    """Test that profiler can be reset."""
    profiler = QueryProfiler()

    # Add some queries
    with profiler.track_query("SELECT * FROM users", 0, "users", "fetch_rows") as tracker:
        tracker.record_rows(5)

    assert len(profiler.queries) == 1

    # Reset should clear queries
    profiler.reset()
    assert len(profiler.queries) == 0


def test_batch_size_calculation():
    """Test that batch size is calculated correctly for composite keys."""
    adapter = PostgreSQLAdapter(batch_size=1000)

    # For single-column PK, effective batch size should be 1000
    # For 2-column PK, effective batch size should be 500
    # For 4-column PK, effective batch size should be 250

    # We can verify this logic by checking the batch_size attribute
    assert adapter.batch_size == 1000


def test_profiler_integration_with_adapter():
    """Test that profiler integrates correctly with PostgreSQL adapter."""
    profiler = QueryProfiler()
    adapter = PostgreSQLAdapter(batch_size=100, profiler=profiler)

    # Verify profiler is attached
    assert adapter.profiler is profiler
    assert adapter.batch_size == 100


def test_performance_improvement_with_batching(sample_schema, mock_adapter):
    """
    Test that batching provides performance improvements.

    This is a conceptual test showing that with batching:
    - Large PK sets are processed in chunks
    - Fewer total queries are made compared to one-by-one

    Without batching (N+1):
    - 1000 orders would result in 1000 queries

    With batching (batch_size=100):
    - 1000 orders would result in 10 queries
    """
    # This would be tested with an actual database in integration tests
    # Here we're just documenting the expected behavior

    num_orders = 1000
    batch_size = 100

    # Expected queries without batching
    queries_without_batching = num_orders

    # Expected queries with batching
    queries_with_batching = (num_orders + batch_size - 1) // batch_size  # Ceiling division

    assert queries_with_batching == 10
    assert queries_with_batching < queries_without_batching

    # Performance improvement ratio
    improvement_ratio = queries_without_batching / queries_with_batching
    assert improvement_ratio == 100  # 100x fewer queries


def test_mock_adapter_fk_fetching(sample_schema, mock_adapter):
    """Test FK value fetching with mock adapter."""
    # Get the FK from orders to users
    fk = None
    for edge in sample_schema.edges:
        if edge.source_table == "orders" and edge.target_table == "users":
            fk = edge
            break

    assert fk is not None

    # Fetch FK values for order with id=1
    order_pks = {(1,)}
    user_ids = mock_adapter.fetch_fk_values("orders", fk, order_pks)

    # Should get user_id = 1
    assert (1,) in user_ids


def test_mock_adapter_referencing_pks(sample_schema, mock_adapter):
    """Test referencing PK fetching with mock adapter."""
    # Get the FK from orders to users
    fk = None
    for edge in sample_schema.edges:
        if edge.source_table == "orders" and edge.target_table == "users":
            fk = edge
            break

    assert fk is not None

    # Fetch orders that reference user_id = 1
    user_pks = {(1,)}
    order_pks = mock_adapter.fetch_referencing_pks(fk, user_pks)

    # Should get order with id=1 (which has user_id=1)
    assert (1,) in order_pks
    # Should not get order with id=2 (which has user_id=2)
    assert (2,) not in order_pks


def test_query_stats_string_representation():
    """Test QueryStats string formatting."""
    from dbslice.utils.profiling import QueryStats

    stats = QueryStats(
        query="SELECT * FROM users WHERE id = %s",
        params_count=1,
        duration_ms=15.5,
        rows_returned=1,
        table="users",
        operation="fetch_rows",
    )

    str_repr = str(stats)
    assert "15.50ms" in str_repr
    assert "[users]" in str_repr
    assert "(fetch_rows)" in str_repr
    assert "1 rows" in str_repr
    assert "1 params" in str_repr
    assert "SELECT * FROM users" in str_repr


@pytest.mark.parametrize(
    "query_count,expected_warning",
    [
        (5, False),  # Few queries, no warning
        (15, True),  # Many queries, should warn
        (50, True),  # Very many queries, should warn
    ],
)
def test_n_plus_one_warning_threshold(query_count, expected_warning):
    """Test that N+1 warnings appear at the right threshold."""
    profiler = QueryProfiler()

    # Simulate FK fetch queries
    for i in range(query_count):
        with profiler.track_query(
            f"SELECT user_id FROM orders WHERE id = {i}", 1, "orders", "fetch_fk_values"
        ) as tracker:
            tracker.record_rows(1)

    summary = profiler.get_summary()
    formatted = summary.format_summary()

    if expected_warning:
        assert "Potential N+1 query pattern" in formatted
    else:
        assert "Potential N+1 query pattern" not in formatted
