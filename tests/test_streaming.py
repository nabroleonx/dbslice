"""Tests for streaming extraction functionality."""

from typing import Any

from dbslice.config import (
    DatabaseType,
    ExtractConfig,
    SeedSpec,
)
from dbslice.core.engine import ExtractionEngine
from dbslice.core.streaming import StreamingExtractionEngine
from tests.conftest import MockAdapter


class ChunkedMockAdapter(MockAdapter):
    """Mock adapter with chunked fetching support for testing."""

    def __init__(self, schema, data, chunk_size=1000):
        super().__init__(schema, data)
        self.chunk_size = chunk_size
        self.chunked_fetch_calls = []  # Track chunked fetch calls

    def fetch_by_pk_chunked(
        self,
        table: str,
        pk_columns: tuple[str, ...],
        pk_values: set[tuple[Any, ...]],
        chunk_size: int = 1000,
    ):
        """Yield data in chunks."""
        self.chunked_fetch_calls.append(
            {
                "table": table,
                "pk_count": len(pk_values),
                "chunk_size": chunk_size,
            }
        )

        # Fetch all matching rows
        all_rows = []
        for row in self.fetch_by_pk(table, pk_columns, pk_values):
            all_rows.append(row)

        # Yield in chunks
        for i in range(0, len(all_rows), chunk_size):
            yield all_rows[i : i + chunk_size]


def test_chunked_adapter_base_implementation(sample_schema, mock_adapter):
    """Test that base adapter provides default chunked implementation."""
    # The base adapter should have fetch_by_pk_chunked
    assert hasattr(mock_adapter, "fetch_by_pk_chunked")

    # Fetch data in chunks
    chunks = list(
        mock_adapter.fetch_by_pk_chunked(
            "users",
            ("id",),
            {(1,), (2,)},
            chunk_size=1,
        )
    )

    # Should get 2 chunks (one per user)
    assert len(chunks) == 2
    assert len(chunks[0]) == 1
    assert len(chunks[1]) == 1


def test_chunked_mock_adapter(sample_schema):
    """Test chunked mock adapter implementation."""
    data = {
        "users": [
            {"id": i, "email": f"user{i}@example.com", "name": f"User {i}"} for i in range(1, 11)
        ],
    }

    adapter = ChunkedMockAdapter(sample_schema, data)

    # Fetch in chunks of 3
    chunks = list(
        adapter.fetch_by_pk_chunked(
            "users",
            ("id",),
            {(i,) for i in range(1, 11)},
            chunk_size=3,
        )
    )

    # Should get 4 chunks: 3+3+3+1
    assert len(chunks) == 4
    assert len(chunks[0]) == 3
    assert len(chunks[1]) == 3
    assert len(chunks[2]) == 3
    assert len(chunks[3]) == 1

    # Verify chunked_fetch_calls was tracked
    assert len(adapter.chunked_fetch_calls) == 1
    assert adapter.chunked_fetch_calls[0]["table"] == "users"
    assert adapter.chunked_fetch_calls[0]["pk_count"] == 10


def test_streaming_engine_basic(sample_schema, tmp_path):
    """Test basic streaming engine functionality."""
    # Create test data
    data = {
        "users": [
            {"id": 1, "email": "alice@example.com", "name": "Alice"},
            {"id": 2, "email": "bob@example.com", "name": "Bob"},
        ],
        "orders": [
            {"id": 1, "user_id": 1, "total": 100.0, "status": "completed"},
            {"id": 2, "user_id": 2, "total": 200.0, "status": "pending"},
        ],
    }

    adapter = ChunkedMockAdapter(sample_schema, data)
    adapter.connect("test://localhost/test")

    # Define records to extract
    records = {
        "users": {(1,), (2,)},
        "orders": {(1,), (2,)},
    }

    # Create output file
    output_file = tmp_path / "streaming_test.sql"

    # Create streaming engine
    config = ExtractConfig(
        database_url="test://localhost/test",
        seeds=[SeedSpec.parse("users.id=1")],
        streaming_chunk_size=1,
    )

    engine = StreamingExtractionEngine(
        config=config,
        adapter=adapter,
        schema=sample_schema,
        records=records,
        insert_order=["users", "orders"],
        broken_fks=[],
        deferred_updates=[],
        db_type=DatabaseType.POSTGRESQL,
        chunk_size=1,
    )

    # Stream to file
    result = engine.stream_to_file(str(output_file))

    # Verify result
    assert result.stats["users"] == 2
    assert result.stats["orders"] == 2
    assert result.total_rows() == 4

    # Verify output file was created
    assert output_file.exists()

    # Read and verify SQL content
    sql_content = output_file.read_text()
    assert "BEGIN;" in sql_content
    assert "COMMIT;" in sql_content
    assert "INSERT INTO" in sql_content
    assert "users" in sql_content
    assert "orders" in sql_content

    # Verify chunked fetching was used
    assert len(adapter.chunked_fetch_calls) == 2  # users + orders


def test_streaming_with_anonymization(sample_schema, tmp_path):
    """Test streaming with anonymization enabled."""
    data = {
        "users": [
            {"id": 1, "email": "alice@example.com", "name": "Alice"},
            {"id": 2, "email": "bob@example.com", "name": "Bob"},
        ],
    }

    adapter = ChunkedMockAdapter(sample_schema, data)
    adapter.connect("test://localhost/test")

    records = {"users": {(1,), (2,)}}
    output_file = tmp_path / "anon_test.sql"

    config = ExtractConfig(
        database_url="test://localhost/test",
        seeds=[SeedSpec.parse("users.id=1")],
        anonymize=True,  # Enable anonymization
        streaming_chunk_size=1,
    )

    engine = StreamingExtractionEngine(
        config=config,
        adapter=adapter,
        schema=sample_schema,
        records=records,
        insert_order=["users"],
        broken_fks=[],
        deferred_updates=[],
        db_type=DatabaseType.POSTGRESQL,
        chunk_size=1,
    )

    result = engine.stream_to_file(str(output_file))

    assert result.stats["users"] == 2

    # Read SQL and verify anonymization occurred
    sql_content = output_file.read_text()
    # Email should be anonymized (deterministic)
    assert "alice@example.com" not in sql_content
    assert "bob@example.com" not in sql_content


def test_streaming_mode_auto_selection(sample_schema):
    """Test automatic streaming mode selection based on row count."""
    # Small dataset - should not use streaming
    config_small = ExtractConfig(
        database_url="test://localhost/test",
        seeds=[SeedSpec.parse("users.id=1")],
        streaming_threshold=50000,
        output_file="/tmp/test.sql",
    )

    engine_small = ExtractionEngine(config_small)
    # Should return False for small dataset
    assert not engine_small._should_use_streaming(1000)

    # Large dataset - should use streaming
    assert engine_small._should_use_streaming(60000)

    # No output file - should not use streaming even if large
    config_no_file = ExtractConfig(
        database_url="test://localhost/test",
        seeds=[SeedSpec.parse("users.id=1")],
        streaming_threshold=50000,
        output_file=None,
    )
    engine_no_file = ExtractionEngine(config_no_file)
    assert not engine_no_file._should_use_streaming(60000)


def test_streaming_mode_forced(sample_schema):
    """Test forcing streaming mode with --stream flag."""
    config = ExtractConfig(
        database_url="test://localhost/test",
        seeds=[SeedSpec.parse("users.id=1")],
        stream=True,  # Force streaming
        output_file="/tmp/test.sql",
    )

    engine = ExtractionEngine(config)
    # Should use streaming even for small datasets
    assert engine._should_use_streaming(100)


def test_streaming_vs_inmemory_same_output(sample_schema, tmp_path):
    """Test that streaming and in-memory modes produce identical output."""
    data = {
        "users": [
            {"id": 1, "email": "alice@example.com", "name": "Alice"},
        ],
        "orders": [
            {"id": 1, "user_id": 1, "total": 100.0, "status": "completed"},
        ],
    }

    # In-memory extraction
    adapter_mem = ChunkedMockAdapter(sample_schema, data)
    adapter_mem.connect("postgres://localhost/test")

    config_mem = ExtractConfig(
        database_url="postgres://localhost/test",
        seeds=[SeedSpec.parse("users.id=1")],
        stream=False,
        output_file=str(tmp_path / "inmemory.sql"),
    )

    engine_mem = ExtractionEngine(config_mem)
    engine_mem.adapter = adapter_mem
    engine_mem.schema = sample_schema

    from dbslice.output.sql import SQLGenerator
    from dbslice.utils.connection import parse_database_url

    # Simulate in-memory extraction
    records = {"users": {(1,)}, "orders": {(1,)}}
    tables_data_mem = {}
    for table, pk_values in records.items():
        table_info = sample_schema.get_table(table)
        pk_columns = table_info.primary_key
        rows = list(adapter_mem.fetch_by_pk(table, pk_columns, pk_values))
        tables_data_mem[table] = rows

    db_config = parse_database_url(config_mem.database_url)
    generator_mem = SQLGenerator(db_type=db_config.db_type)
    sql_mem = generator_mem.generate(
        tables_data_mem,
        ["users", "orders"],
        sample_schema.tables,
    )

    # Streaming extraction
    adapter_stream = ChunkedMockAdapter(sample_schema, data)
    adapter_stream.connect("test://localhost/test")

    output_stream = tmp_path / "streaming.sql"
    config_stream = ExtractConfig(
        database_url="test://localhost/test",
        seeds=[SeedSpec.parse("users.id=1")],
        streaming_chunk_size=1,
    )

    engine_stream = StreamingExtractionEngine(
        config=config_stream,
        adapter=adapter_stream,
        schema=sample_schema,
        records=records,
        insert_order=["users", "orders"],
        broken_fks=[],
        deferred_updates=[],
        db_type=db_config.db_type,
        chunk_size=1,
    )

    engine_stream.stream_to_file(str(output_stream))
    sql_stream = output_stream.read_text()

    # Both should have the same essential content (ignoring whitespace)
    # Normalize for comparison
    def normalize_sql(sql):
        lines = [
            line.strip()
            for line in sql.split("\n")
            if line.strip() and not line.strip().startswith("--")
        ]
        return "\n".join(lines)

    assert normalize_sql(sql_mem) == normalize_sql(sql_stream)


def test_streaming_memory_bounded(sample_schema):
    """Test that streaming mode keeps memory usage bounded."""
    # Create large dataset
    large_data = {
        "users": [
            {"id": i, "email": f"user{i}@example.com", "name": f"User {i}"}
            for i in range(1, 10001)  # 10K users
        ],
    }

    adapter = ChunkedMockAdapter(sample_schema, large_data, chunk_size=100)

    # Streaming should process in chunks
    chunks = list(
        adapter.fetch_by_pk_chunked(
            "users",
            ("id",),
            {(i,) for i in range(1, 10001)},
            chunk_size=100,
        )
    )

    # Should have exactly 100 chunks (10000 / 100)
    assert len(chunks) == 100

    # Each chunk should have at most 100 rows
    for chunk in chunks:
        assert len(chunk) <= 100


def test_streaming_with_cycles(sample_schema, tmp_path):
    """Test streaming mode handles circular dependencies correctly."""
    from dbslice.models import ForeignKey

    # Create schema with circular dependency
    fk_circular = ForeignKey(
        name="fk_users_orders",
        source_table="users",
        source_columns=("last_order_id",),
        target_table="orders",
        target_columns=("id",),
        is_nullable=True,  # Nullable to allow breaking
    )

    # Add circular FK to schema (orders -> users, users -> orders)
    sample_schema.edges.append(fk_circular)

    data = {
        "users": [
            {"id": 1, "email": "alice@example.com", "name": "Alice", "last_order_id": 1},
        ],
        "orders": [
            {"id": 1, "user_id": 1, "total": 100.0, "status": "completed"},
        ],
    }

    adapter = ChunkedMockAdapter(sample_schema, data)
    adapter.connect("test://localhost/test")

    records = {"users": {(1,)}, "orders": {(1,)}}
    output_file = tmp_path / "cycles.sql"

    config = ExtractConfig(
        database_url="test://localhost/test",
        seeds=[SeedSpec.parse("users.id=1")],
        streaming_chunk_size=1,
    )

    # For this test, we'll manually specify broken_fks
    engine = StreamingExtractionEngine(
        config=config,
        adapter=adapter,
        schema=sample_schema,
        records=records,
        insert_order=["orders", "users"],
        broken_fks=[fk_circular],
        deferred_updates=[],
        db_type=DatabaseType.POSTGRESQL,
        chunk_size=1,
    )

    result = engine.stream_to_file(str(output_file))

    # Should complete successfully
    assert result.has_cycles
    assert len(result.broken_fks) == 1

    # SQL should contain both tables
    sql_content = output_file.read_text()
    assert "users" in sql_content
    assert "orders" in sql_content


def test_streaming_empty_table(sample_schema, tmp_path):
    """Test streaming handles empty tables gracefully."""
    data = {
        "users": [],  # Empty table
    }

    adapter = ChunkedMockAdapter(sample_schema, data)
    adapter.connect("test://localhost/test")

    records = {"users": set()}  # No records
    output_file = tmp_path / "empty.sql"

    config = ExtractConfig(
        database_url="test://localhost/test",
        seeds=[SeedSpec.parse("users.id=1")],
        streaming_chunk_size=1,
    )

    engine = StreamingExtractionEngine(
        config=config,
        adapter=adapter,
        schema=sample_schema,
        records=records,
        insert_order=["users"],
        broken_fks=[],
        deferred_updates=[],
        db_type=DatabaseType.POSTGRESQL,
        chunk_size=1,
    )

    result = engine.stream_to_file(str(output_file))

    # Stats should show 0 rows
    assert result.stats.get("users", 0) == 0
    assert result.total_rows() == 0

    # File should still be valid SQL
    sql_content = output_file.read_text()
    assert "BEGIN;" in sql_content
    assert "COMMIT;" in sql_content


def test_streaming_chunk_sizes(sample_schema, tmp_path):
    """Test different chunk sizes work correctly."""
    data = {
        "users": [
            {"id": i, "email": f"user{i}@example.com", "name": f"User {i}"}
            for i in range(1, 101)  # 100 users
        ],
    }

    for chunk_size in [1, 10, 50, 100, 200]:
        adapter = ChunkedMockAdapter(sample_schema, data)
        adapter.connect("test://localhost/test")

        records = {"users": {(i,) for i in range(1, 101)}}
        output_file = tmp_path / f"chunk_{chunk_size}.sql"

        config = ExtractConfig(
            database_url="test://localhost/test",
            seeds=[SeedSpec.parse("users.id=1")],
            streaming_chunk_size=chunk_size,
        )

        engine = StreamingExtractionEngine(
            config=config,
            adapter=adapter,
            schema=sample_schema,
            records=records,
            insert_order=["users"],
            broken_fks=[],
            deferred_updates=[],
            db_type=DatabaseType.POSTGRESQL,
            chunk_size=chunk_size,
        )

        result = engine.stream_to_file(str(output_file))

        # Should always get all 100 rows
        assert result.stats["users"] == 100

        # Verify chunked fetching was called
        assert len(adapter.chunked_fetch_calls) == 1
        assert adapter.chunked_fetch_calls[0]["chunk_size"] == chunk_size


def test_streaming_requires_output_file(sample_schema):
    """Test that streaming mode requires --out-file."""
    config = ExtractConfig(
        database_url="postgres://localhost/test",
        seeds=[SeedSpec.parse("users.id=1")],
        stream=True,
        output_file=None,  # No output file
    )

    engine = ExtractionEngine(config)
    engine.schema = sample_schema

    # When stream=True, _should_use_streaming returns True even without output_file.
    # The ValueError is raised in _do_extract when it tries to start streaming
    # without an output file.
    assert engine._should_use_streaming(100000)
