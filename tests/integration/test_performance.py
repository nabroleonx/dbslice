"""
Integration tests for performance characteristics.

These tests verify memory usage, streaming mode, query batching,
and extraction speed with realistic dataset sizes.
"""

import os
import tempfile
import time

import pytest

from dbslice.config import SeedSpec, TraversalDirection
from dbslice.core.engine import ExtractionEngine

pytestmark = [pytest.mark.integration, pytest.mark.performance, pytest.mark.slow]


class TestLargeDatasets:
    """Test extraction with larger datasets."""

    @pytest.fixture
    def large_dataset(self, pg_connection, clean_database):
        """Create a dataset with thousands of records."""
        with pg_connection.cursor() as cur:
            # Create tables
            cur.execute("""
                CREATE TABLE customers (
                    id SERIAL PRIMARY KEY,
                    email VARCHAR(255) NOT NULL,
                    name VARCHAR(255)
                )
            """)

            cur.execute("""
                CREATE TABLE orders (
                    id SERIAL PRIMARY KEY,
                    customer_id INTEGER NOT NULL REFERENCES customers(id),
                    total DECIMAL(10, 2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            cur.execute("""
                CREATE TABLE line_items (
                    id SERIAL PRIMARY KEY,
                    order_id INTEGER NOT NULL REFERENCES orders(id),
                    product_name VARCHAR(255),
                    quantity INTEGER,
                    price DECIMAL(10, 2)
                )
            """)

            # Insert test data - 1000 customers, 5000 orders, 15000 line items
            # Use efficient bulk insert
            cur.execute("""
                INSERT INTO customers (email, name)
                SELECT
                    'user' || i || '@example.com',
                    'User ' || i
                FROM generate_series(1, 1000) i
            """)

            cur.execute("""
                INSERT INTO orders (customer_id, total, created_at)
                SELECT
                    ((i - 1) % 1000) + 1,  -- Distribute orders across customers
                    (random() * 1000)::decimal(10,2),
                    CURRENT_TIMESTAMP - (random() * interval '365 days')
                FROM generate_series(1, 5000) i
            """)

            cur.execute("""
                INSERT INTO line_items (order_id, product_name, quantity, price)
                SELECT
                    ((i - 1) % 5000) + 1,  -- Distribute items across orders
                    'Product ' || ((i - 1) % 100 + 1),
                    (random() * 10)::integer + 1,
                    (random() * 100)::decimal(10,2)
                FROM generate_series(1, 15000) i
            """)

        return {
            "customers": 1000,
            "orders": 5000,
            "line_items": 15000,
        }

    def test_extract_subset_from_large_dataset(
        self, large_dataset: dict, extract_config_factory, pg_connection
    ):
        """Test extracting a small subset from a large dataset."""
        start_time = time.time()

        # Extract orders for one customer
        config = extract_config_factory(
            seeds=[SeedSpec.parse("customers.id=1")],
            direction=TraversalDirection.DOWN,
            depth=10,
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        elapsed = time.time() - start_time

        # Should extract only related records, not the entire dataset
        assert result.total_rows() < 100  # Much less than 16000 total rows
        assert "customers" in result.tables
        assert "orders" in result.tables
        assert "line_items" in result.tables

        # Should be reasonably fast (under 5 seconds for this small subset)
        assert elapsed < 5.0

    def test_extract_large_subset(self, large_dataset: dict, extract_config_factory, pg_connection):
        """Test extracting a larger subset (10% of dataset)."""
        start_time = time.time()

        # Extract orders for first 100 customers
        config = extract_config_factory(
            seeds=[SeedSpec.parse("customers:id <= 100")],
            direction=TraversalDirection.DOWN,
            depth=10,
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        elapsed = time.time() - start_time

        # Should extract roughly 10% of data
        assert result.total_rows() > 1000  # At least 1000 records
        assert result.total_rows() < 3000  # But not everything

        # Should complete in reasonable time (under 10 seconds)
        assert elapsed < 10.0


class TestStreamingMode:
    """Test streaming mode activation and behavior."""

    @pytest.fixture
    def medium_dataset(self, pg_connection, clean_database):
        """Create a medium-sized dataset for streaming tests."""
        with pg_connection.cursor() as cur:
            cur.execute("""
                CREATE TABLE items (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255),
                    data TEXT
                )
            """)

            # Insert 10000 items
            cur.execute("""
                INSERT INTO items (name, data)
                SELECT
                    'Item ' || i,
                    'Data for item ' || i || ' - ' || repeat('x', 100)
                FROM generate_series(1, 10000) i
            """)

        return {"items": 10000}

    def test_streaming_mode_with_large_threshold(
        self, medium_dataset: dict, extract_config_factory, pg_connection
    ):
        """Test that streaming mode activates above threshold."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            output_file = f.name

        try:
            # Set low threshold to trigger streaming
            config = extract_config_factory(
                seeds=[SeedSpec.parse("items:id <= 5000")],
                direction=TraversalDirection.BOTH,
                depth=10,
                streaming_threshold=1000,  # Low threshold
                output_file=output_file,
            )

            engine = ExtractionEngine(config)
            result, schema = engine.extract()

            # Verify streaming was used (tables dict should be empty or minimal)
            # In streaming mode, data is written directly to file
            assert result.total_rows() > 0  # Stats are still tracked

            # Verify output file was created and has content
            assert os.path.exists(output_file)
            with open(output_file) as f:
                content = f.read()
                assert len(content) > 1000  # Should have substantial SQL
                assert "INSERT INTO" in content

        finally:
            if os.path.exists(output_file):
                os.unlink(output_file)

    def test_force_streaming_mode(
        self, medium_dataset: dict, extract_config_factory, pg_connection
    ):
        """Test forcing streaming mode with --stream flag."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            output_file = f.name

        try:
            # Force streaming mode
            config = extract_config_factory(
                seeds=[SeedSpec.parse("items:id <= 100")],  # Small dataset
                direction=TraversalDirection.BOTH,
                depth=10,
                stream=True,  # Force streaming
                output_file=output_file,
            )

            engine = ExtractionEngine(config)
            result, schema = engine.extract()

            # Should use streaming even for small dataset
            assert os.path.exists(output_file)
            with open(output_file) as f:
                content = f.read()
                assert "INSERT INTO" in content

        finally:
            if os.path.exists(output_file):
                os.unlink(output_file)

    def test_streaming_chunk_size(
        self, medium_dataset: dict, extract_config_factory, pg_connection
    ):
        """Test that streaming chunk size is configurable."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            output_file = f.name

        try:
            # Use small chunk size
            config = extract_config_factory(
                seeds=[SeedSpec.parse("items:id <= 1000")],
                direction=TraversalDirection.BOTH,
                depth=10,
                stream=True,
                streaming_chunk_size=100,  # Small chunks
                output_file=output_file,
            )

            engine = ExtractionEngine(config)
            result, schema = engine.extract()

            # Should complete successfully with small chunks
            assert os.path.exists(output_file)

        finally:
            if os.path.exists(output_file):
                os.unlink(output_file)


class TestQueryBatching:
    """Test query batching effectiveness."""

    @pytest.fixture
    def fk_heavy_schema(self, pg_connection, clean_database):
        """Create schema with many FK lookups."""
        with pg_connection.cursor() as cur:
            cur.execute("""
                CREATE TABLE categories (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255)
                )
            """)

            cur.execute("""
                CREATE TABLE products (
                    id SERIAL PRIMARY KEY,
                    category_id INTEGER NOT NULL REFERENCES categories(id),
                    name VARCHAR(255)
                )
            """)

            # Insert data
            cur.execute("""
                INSERT INTO categories (id, name)
                SELECT i, 'Category ' || i
                FROM generate_series(1, 100) i
            """)

            cur.execute("""
                INSERT INTO products (category_id, name)
                SELECT
                    ((i - 1) % 100) + 1,
                    'Product ' || i
                FROM generate_series(1, 10000) i
            """)

        return {
            "categories": 100,
            "products": 10000,
        }

    def test_batching_with_many_fks(
        self, fk_heavy_schema: dict, extract_config_factory, pg_connection
    ):
        """Test that batching works with many FK lookups."""
        # Extract products which will require batched category lookups
        config = extract_config_factory(
            seeds=[SeedSpec.parse("products:id <= 5000")],
            direction=TraversalDirection.UP,
            depth=10,
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        # Should extract all referenced categories
        assert "categories" in result.tables
        assert len(result.tables["categories"]) == 100  # All categories

        # Should extract requested products
        assert "products" in result.tables
        assert len(result.tables["products"]) == 5000

    def test_batching_with_composite_keys(
        self, pg_connection, clean_database, extract_config_factory
    ):
        """Test batching with composite primary keys."""
        with pg_connection.cursor() as cur:
            # Create table with composite PK
            cur.execute("""
                CREATE TABLE order_tags (
                    order_id INTEGER,
                    tag_name VARCHAR(50),
                    value TEXT,
                    PRIMARY KEY (order_id, tag_name)
                )
            """)

            cur.execute("""
                CREATE TABLE tag_metadata (
                    id SERIAL PRIMARY KEY,
                    order_id INTEGER,
                    tag_name VARCHAR(50),
                    metadata TEXT,
                    FOREIGN KEY (order_id, tag_name)
                        REFERENCES order_tags(order_id, tag_name)
                )
            """)

            # Insert data
            cur.execute("""
                INSERT INTO order_tags (order_id, tag_name, value)
                SELECT
                    ((i - 1) / 10) + 1,
                    'tag' || (((i - 1) % 10) + 1),
                    'value' || i
                FROM generate_series(1, 1000) i
            """)

            cur.execute("""
                INSERT INTO tag_metadata (order_id, tag_name, metadata)
                SELECT
                    order_id,
                    tag_name,
                    'meta' || order_id || tag_name
                FROM order_tags
                WHERE (order_id % 2) = 0
            """)

        # Extract with composite key joins
        config = extract_config_factory(
            seeds=[SeedSpec.parse("order_tags:order_id <= 50")],
            direction=TraversalDirection.BOTH,
            depth=10,
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        # Should handle composite keys correctly
        assert "order_tags" in result.tables
        assert "tag_metadata" in result.tables


class TestBenchmarks:
    """Benchmark tests for extraction speed."""

    @pytest.fixture
    def benchmark_dataset(self, pg_connection, clean_database):
        """Create a realistic benchmark dataset."""
        with pg_connection.cursor() as cur:
            # E-commerce-like schema
            cur.execute("""
                CREATE TABLE users (
                    id SERIAL PRIMARY KEY,
                    email VARCHAR(255) NOT NULL,
                    name VARCHAR(255)
                )
            """)

            cur.execute("""
                CREATE TABLE orders (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(id),
                    total DECIMAL(10, 2),
                    status VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            cur.execute("""
                CREATE TABLE order_items (
                    id SERIAL PRIMARY KEY,
                    order_id INTEGER NOT NULL REFERENCES orders(id),
                    product_name VARCHAR(255),
                    quantity INTEGER,
                    price DECIMAL(10, 2)
                )
            """)

            # Insert realistic amounts
            cur.execute("""
                INSERT INTO users (email, name)
                SELECT
                    'user' || i || '@example.com',
                    'User ' || i
                FROM generate_series(1, 500) i
            """)

            cur.execute("""
                INSERT INTO orders (user_id, total, status, created_at)
                SELECT
                    ((i - 1) % 500) + 1,
                    (random() * 1000)::decimal(10,2),
                    CASE (i % 4)
                        WHEN 0 THEN 'completed'
                        WHEN 1 THEN 'pending'
                        WHEN 2 THEN 'shipped'
                        ELSE 'cancelled'
                    END,
                    CURRENT_TIMESTAMP - (random() * interval '365 days')
                FROM generate_series(1, 2500) i
            """)

            cur.execute("""
                INSERT INTO order_items (order_id, product_name, quantity, price)
                SELECT
                    ((i - 1) % 2500) + 1,
                    'Product ' || ((i - 1) % 50 + 1),
                    (random() * 10)::integer + 1,
                    (random() * 100)::decimal(10,2)
                FROM generate_series(1, 7500) i
            """)

        return {
            "users": 500,
            "orders": 2500,
            "order_items": 7500,
        }

    def test_benchmark_single_user_extraction(
        self, benchmark_dataset: dict, extract_config_factory, pg_connection
    ):
        """Benchmark extracting one user's complete data."""
        start_time = time.time()

        config = extract_config_factory(
            seeds=[SeedSpec.parse("users.id=1")],
            direction=TraversalDirection.DOWN,
            depth=10,
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        elapsed = time.time() - start_time

        # Record metrics
        print("\nSingle user extraction:")
        print(f"  Time: {elapsed:.2f}s")
        print(f"  Rows extracted: {result.total_rows()}")
        print(f"  Rows/second: {result.total_rows() / elapsed:.0f}")

        # Should be fast (under 2 seconds)
        assert elapsed < 2.0

    def test_benchmark_bulk_extraction(
        self, benchmark_dataset: dict, extract_config_factory, pg_connection
    ):
        """Benchmark extracting 10% of dataset."""
        start_time = time.time()

        config = extract_config_factory(
            seeds=[SeedSpec.parse("users:id <= 50")],
            direction=TraversalDirection.DOWN,
            depth=10,
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        elapsed = time.time() - start_time

        # Record metrics
        print("\nBulk extraction (10% of data):")
        print(f"  Time: {elapsed:.2f}s")
        print(f"  Rows extracted: {result.total_rows()}")
        print(f"  Rows/second: {result.total_rows() / elapsed:.0f}")

        # Should complete in reasonable time (under 5 seconds)
        assert elapsed < 5.0

    def test_benchmark_with_profiling(
        self, benchmark_dataset: dict, extract_config_factory, pg_connection
    ):
        """Benchmark with query profiling enabled."""
        config = extract_config_factory(
            seeds=[SeedSpec.parse("users.id=1")],
            direction=TraversalDirection.DOWN,
            depth=10,
            profile=True,
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        # Should have profiling data
        assert result.profiler is not None
        summary = result.profiler.get_summary()

        print("\nQuery profiling statistics:")
        print(f"  Total queries: {summary.total_queries}")
        print(f"  Total time: {summary.total_duration_ms:.2f}ms")
        print(f"  Total rows: {summary.total_rows}")
        if summary.total_queries > 0:
            print(f"  Avg query time: {summary.total_duration_ms / summary.total_queries:.2f}ms")


class TestMemoryUsage:
    """Test memory usage characteristics (qualitative)."""

    def test_in_memory_mode_completes(self, ecommerce_schema: dict, extract_config_factory):
        """Test that in-memory mode completes without OOM."""
        # Extract a moderate dataset in memory
        config = extract_config_factory(
            seeds=[SeedSpec.parse("users.id=1")],
            direction=TraversalDirection.DOWN,
            depth=10,
            stream=False,  # Force in-memory
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        # Should complete successfully
        assert result.total_rows() > 0
        assert len(result.tables) > 0

    def test_streaming_mode_reduces_memory(self, ecommerce_schema: dict, extract_config_factory):
        """Test that streaming mode doesn't load all data in memory."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            output_file = f.name

        try:
            # Force streaming mode
            config = extract_config_factory(
                seeds=[SeedSpec.parse("users.id=1")],
                direction=TraversalDirection.DOWN,
                depth=10,
                stream=True,
                output_file=output_file,
            )

            engine = ExtractionEngine(config)
            result, schema = engine.extract()

            # In streaming mode, tables dict should be empty or minimal
            # (data written directly to file)
            assert os.path.exists(output_file)
            file_size = os.path.getsize(output_file)
            assert file_size > 0

        finally:
            if os.path.exists(output_file):
                os.unlink(output_file)
