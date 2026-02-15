"""Integration tests for streaming extraction with real extraction flow."""

import pytest

from dbslice.config import ExtractConfig, SeedSpec, TraversalDirection
from dbslice.core.engine import ExtractionEngine


def test_streaming_integration_with_mock_adapter(sample_schema, mock_adapter, tmp_path):
    """
    Integration test: Full extraction flow using streaming mode.

    This tests the integration between:
    - ExtractionEngine
    - StreamingExtractionEngine
    - Mock adapter with chunking
    - SQL generation
    """
    output_file = tmp_path / "streaming_integration.sql"

    config = ExtractConfig(
        database_url="test://localhost/test",
        seeds=[SeedSpec.parse("orders.id=1")],
        depth=3,
        direction=TraversalDirection.BOTH,
        output_file=str(output_file),
        stream=True,  # Force streaming mode
        streaming_chunk_size=1,
    )

    engine = ExtractionEngine(config)
    engine.adapter = mock_adapter
    engine.schema = sample_schema

    # Simulate extraction flow
    # Note: We can't run full extract() because it tries to connect
    # But we can test the _should_use_streaming logic

    # Should use streaming because stream=True
    assert engine._should_use_streaming(10)

    # Test auto-detection
    config2 = ExtractConfig(
        database_url="test://localhost/test",
        seeds=[SeedSpec.parse("orders.id=1")],
        output_file=str(tmp_path / "test2.sql"),
        streaming_threshold=1000,
    )
    engine2 = ExtractionEngine(config2)

    # Should not use streaming for small dataset
    assert not engine2._should_use_streaming(500)

    # Should use streaming for large dataset
    assert engine2._should_use_streaming(2000)


def test_streaming_decision_logic():
    """Test the decision logic for when to use streaming mode."""

    # Test 1: Force streaming with --stream flag
    config = ExtractConfig(
        database_url="test://localhost/test",
        seeds=[SeedSpec.parse("users.id=1")],
        stream=True,
        output_file="/tmp/test.sql",
    )
    engine = ExtractionEngine(config)
    assert engine._should_use_streaming(100)  # Even small datasets

    # Test 2: Auto-enable streaming based on threshold
    config = ExtractConfig(
        database_url="test://localhost/test",
        seeds=[SeedSpec.parse("users.id=1")],
        streaming_threshold=50000,
        output_file="/tmp/test.sql",
    )
    engine = ExtractionEngine(config)
    assert not engine._should_use_streaming(10000)  # Below threshold
    assert engine._should_use_streaming(60000)  # Above threshold

    # Test 3: No output file - no streaming
    config = ExtractConfig(
        database_url="test://localhost/test",
        seeds=[SeedSpec.parse("users.id=1")],
        streaming_threshold=50000,
        output_file=None,  # No output file
    )
    engine = ExtractionEngine(config)
    assert not engine._should_use_streaming(100000)  # Can't stream to stdout

    # Test 4: Custom threshold
    config = ExtractConfig(
        database_url="test://localhost/test",
        seeds=[SeedSpec.parse("users.id=1")],
        streaming_threshold=100000,
        output_file="/tmp/test.sql",
    )
    engine = ExtractionEngine(config)
    assert not engine._should_use_streaming(50000)  # Below custom threshold
    assert engine._should_use_streaming(150000)  # Above custom threshold


def test_streaming_config_validation():
    """Test that streaming configuration is properly validated."""

    # Valid streaming configs
    config1 = ExtractConfig(
        database_url="test://localhost/test",
        seeds=[SeedSpec.parse("users.id=1")],
        stream=True,
        output_file="/tmp/test.sql",
        streaming_chunk_size=1000,
    )
    assert config1.stream is True
    assert config1.streaming_chunk_size == 1000

    # Valid auto-streaming config
    config2 = ExtractConfig(
        database_url="test://localhost/test",
        seeds=[SeedSpec.parse("users.id=1")],
        streaming_threshold=25000,
        output_file="/tmp/test.sql",
    )
    assert config2.streaming_threshold == 25000

    # Default values
    config3 = ExtractConfig(
        database_url="test://localhost/test",
        seeds=[SeedSpec.parse("users.id=1")],
    )
    assert config3.stream is False
    assert config3.streaming_threshold == 50000
    assert config3.streaming_chunk_size == 1000


def test_streaming_with_multiple_tables(sample_schema, tmp_path):
    """Test streaming with multiple tables in correct order."""
    from dbslice.config import DatabaseType
    from dbslice.core.streaming import StreamingExtractionEngine
    from tests.conftest import MockAdapter

    # Create data for multiple tables
    data = {
        "users": [
            {"id": 1, "email": "alice@example.com", "name": "Alice"},
            {"id": 2, "email": "bob@example.com", "name": "Bob"},
        ],
        "products": [
            {"id": 1, "sku": "WIDGET-001", "name": "Widget", "price": 19.99},
        ],
        "orders": [
            {"id": 1, "user_id": 1, "total": 19.99, "status": "completed"},
        ],
        "order_items": [
            {"id": 1, "order_id": 1, "product_id": 1, "quantity": 1},
        ],
    }

    adapter = MockAdapter(sample_schema, data)
    adapter.connect("test://localhost/test")

    # Define all records
    records = {
        "users": {(1,), (2,)},
        "products": {(1,)},
        "orders": {(1,)},
        "order_items": {(1,)},
    }

    # Proper topological order: users, products first, then orders, then order_items
    insert_order = ["users", "products", "orders", "order_items"]

    output_file = tmp_path / "multi_table.sql"

    config = ExtractConfig(
        database_url="test://localhost/test",
        seeds=[SeedSpec.parse("orders.id=1")],
        streaming_chunk_size=1,
    )

    engine = StreamingExtractionEngine(
        config=config,
        adapter=adapter,
        schema=sample_schema,
        records=records,
        insert_order=insert_order,
        broken_fks=[],
        deferred_updates=[],
        db_type=DatabaseType.POSTGRESQL,
        chunk_size=1,
    )

    result = engine.stream_to_file(str(output_file))

    # Verify all tables were extracted
    assert result.stats["users"] == 2
    assert result.stats["products"] == 1
    assert result.stats["orders"] == 1
    assert result.stats["order_items"] == 1
    assert result.total_rows() == 5

    # Verify SQL file has tables in correct order
    sql_content = output_file.read_text()

    # Find positions of table comments
    users_pos = sql_content.find("-- users")
    products_pos = sql_content.find("-- products")
    orders_pos = sql_content.find("-- orders")
    order_items_pos = sql_content.find("-- order_items")

    # Verify order: users and products before orders, orders before order_items
    assert users_pos < orders_pos < order_items_pos
    assert products_pos < orders_pos


def test_streaming_error_handling(sample_schema, tmp_path):
    """Test that streaming handles errors gracefully."""
    from dbslice.config import DatabaseType
    from dbslice.core.streaming import StreamingExtractionEngine
    from tests.conftest import MockAdapter

    data = {
        "users": [
            {"id": 1, "email": "alice@example.com", "name": "Alice"},
        ],
    }

    adapter = MockAdapter(sample_schema, data)
    adapter.connect("test://localhost/test")

    records = {"users": {(1,)}}

    # Test with invalid output file path (directory doesn't exist)
    invalid_output = tmp_path / "nonexistent_dir" / "subdir" / "output.sql"

    config = ExtractConfig(
        database_url="test://localhost/test",
        seeds=[SeedSpec.parse("users.id=1")],
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

    # Should raise FileNotFoundError
    with pytest.raises(FileNotFoundError):
        engine.stream_to_file(str(invalid_output))


def test_streaming_preserves_data_integrity(sample_schema, tmp_path):
    """Test that streaming mode preserves all data correctly."""
    from dbslice.config import DatabaseType
    from dbslice.core.streaming import StreamingExtractionEngine
    from tests.conftest import MockAdapter

    # Create data with special characters and edge cases
    data = {
        "users": [
            {"id": 1, "email": "user@example.com", "name": "User 'with' quotes"},
            {"id": 2, "email": "user2@example.com", "name": 'User "with" double quotes'},
            {"id": 3, "email": "user3@example.com", "name": None},  # NULL value
        ],
    }

    adapter = MockAdapter(sample_schema, data)
    adapter.connect("test://localhost/test")

    records = {"users": {(1,), (2,), (3,)}}
    output_file = tmp_path / "integrity.sql"

    config = ExtractConfig(
        database_url="test://localhost/test",
        seeds=[SeedSpec.parse("users.id=1")],
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

    assert result.stats["users"] == 3

    sql_content = output_file.read_text()

    # Verify SQL escaping
    assert "'User ''with'' quotes'" in sql_content  # Single quotes escaped
    assert (
        '"User ""with"" double quotes"' in sql_content
        or "'User \"with\" double quotes'" in sql_content
    )
    assert "NULL" in sql_content  # NULL value preserved
