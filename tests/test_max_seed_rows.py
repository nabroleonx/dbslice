"""Tests for max_seed_rows safety limit on seed queries."""

from unittest.mock import patch

import pytest

from dbslice.config import ExtractConfig, SeedSpec
from dbslice.constants import DEFAULT_MAX_SEED_ROWS, SEED_ROW_WARNING_THRESHOLD
from dbslice.core.engine import ExtractionEngine
from dbslice.exceptions import ExtractionError
from dbslice.models import Column, SchemaGraph, Table
from tests.conftest import MockAdapter


def _make_schema_and_adapter(
    row_count: int,
) -> tuple[SchemaGraph, MockAdapter]:
    """Create a simple schema with a users table and the given number of rows."""
    users_table = Table(
        name="users",
        schema="public",
        columns=[
            Column(name="id", data_type="INTEGER", nullable=False, is_primary_key=True),
            Column(name="status", data_type="TEXT", nullable=True, is_primary_key=False),
        ],
        primary_key=("id",),
        foreign_keys=[],
    )
    schema = SchemaGraph(tables={"users": users_table}, edges=[])
    rows = [{"id": i, "status": "active"} for i in range(1, row_count + 1)]
    adapter = MockAdapter(schema, {"users": rows})
    return schema, adapter


class TestMaxSeedRowsDefault:
    """The default max_seed_rows value works correctly."""

    def test_default_value_is_set(self):
        config = ExtractConfig(
            database_url="postgresql://localhost/test",
            seeds=[SeedSpec.parse("users.id=1")],
        )
        assert config.max_seed_rows == DEFAULT_MAX_SEED_ROWS

    def test_seed_within_default_limit_succeeds(self):
        schema, adapter = _make_schema_and_adapter(5)
        config = ExtractConfig(
            database_url="postgresql://localhost/test",
            seeds=[SeedSpec.parse("users.id=1")],
            validate=False,
        )
        engine = ExtractionEngine(config)
        engine.schema = schema
        engine.adapter = adapter

        # Should not raise
        engine._process_seed(config.seeds[0])


class TestMaxSeedRowsExceedsLimit:
    """Seed queries exceeding the limit raise ExtractionError."""

    def test_exceeds_default_limit(self):
        row_count = DEFAULT_MAX_SEED_ROWS + 1
        schema, adapter = _make_schema_and_adapter(row_count)
        config = ExtractConfig(
            database_url="postgresql://localhost/test",
            seeds=[SeedSpec.parse("users.id=1")],
            validate=False,
        )
        engine = ExtractionEngine(config)
        engine.schema = schema
        engine.adapter = adapter

        with pytest.raises(ExtractionError, match="exceeding limit"):
            engine._process_seed(config.seeds[0])

    def test_exceeds_custom_limit(self):
        schema, adapter = _make_schema_and_adapter(6)
        config = ExtractConfig(
            database_url="postgresql://localhost/test",
            seeds=[SeedSpec.parse("users.id=1")],
            validate=False,
            max_seed_rows=5,
        )
        engine = ExtractionEngine(config)
        engine.schema = schema
        engine.adapter = adapter

        with pytest.raises(ExtractionError, match="exceeding limit of 5"):
            engine._process_seed(config.seeds[0])

    def test_exactly_at_limit_succeeds(self):
        schema, adapter = _make_schema_and_adapter(5)
        config = ExtractConfig(
            database_url="postgresql://localhost/test",
            seeds=[SeedSpec.parse("users.id=1")],
            validate=False,
            max_seed_rows=5,
        )
        engine = ExtractionEngine(config)
        engine.schema = schema
        engine.adapter = adapter

        # Should not raise -- exactly at limit, not over
        engine._process_seed(config.seeds[0])

    def test_error_message_includes_row_count_and_limit(self):
        schema, adapter = _make_schema_and_adapter(20)
        config = ExtractConfig(
            database_url="postgresql://localhost/test",
            seeds=[SeedSpec.parse("users.id=1")],
            validate=False,
            max_seed_rows=10,
        )
        engine = ExtractionEngine(config)
        engine.schema = schema
        engine.adapter = adapter

        with pytest.raises(ExtractionError, match=r"20 rows.*limit of 10"):
            engine._process_seed(config.seeds[0])

    def test_error_message_mentions_cli_flag(self):
        schema, adapter = _make_schema_and_adapter(20)
        config = ExtractConfig(
            database_url="postgresql://localhost/test",
            seeds=[SeedSpec.parse("users.id=1")],
            validate=False,
            max_seed_rows=10,
        )
        engine = ExtractionEngine(config)
        engine.schema = schema
        engine.adapter = adapter

        with pytest.raises(ExtractionError, match="--max-seed-rows"):
            engine._process_seed(config.seeds[0])


class TestMaxSeedRowsCustomConfig:
    """Custom max_seed_rows via config works correctly."""

    def test_custom_limit_via_config(self):
        config = ExtractConfig(
            database_url="postgresql://localhost/test",
            seeds=[SeedSpec.parse("users.id=1")],
            max_seed_rows=500,
        )
        assert config.max_seed_rows == 500

    def test_high_limit_allows_large_result(self):
        schema, adapter = _make_schema_and_adapter(50)
        config = ExtractConfig(
            database_url="postgresql://localhost/test",
            seeds=[SeedSpec.parse("users.id=1")],
            validate=False,
            max_seed_rows=100,
        )
        engine = ExtractionEngine(config)
        engine.schema = schema
        engine.adapter = adapter

        # 50 rows is under the 100 limit, should succeed
        engine._process_seed(config.seeds[0])


class TestSeedRowWarningThreshold:
    """Warning is emitted when seed rows exceed the warning threshold."""

    def test_warning_logged_above_threshold(self):
        row_count = SEED_ROW_WARNING_THRESHOLD + 1
        schema, adapter = _make_schema_and_adapter(row_count)
        config = ExtractConfig(
            database_url="postgresql://localhost/test",
            seeds=[SeedSpec.parse("users.id=1")],
            validate=False,
            max_seed_rows=row_count + 1,  # Don't hit the hard limit
        )
        engine = ExtractionEngine(config)
        engine.schema = schema
        engine.adapter = adapter

        with patch("dbslice.core.engine.logger") as mock_logger:
            engine._process_seed(config.seeds[0])
            mock_logger.warning.assert_called_once()
            call_args = mock_logger.warning.call_args
            assert "large number of rows" in call_args[0][0]

    def test_no_warning_below_threshold(self):
        row_count = SEED_ROW_WARNING_THRESHOLD
        schema, adapter = _make_schema_and_adapter(row_count)
        config = ExtractConfig(
            database_url="postgresql://localhost/test",
            seeds=[SeedSpec.parse("users.id=1")],
            validate=False,
            max_seed_rows=row_count + 1,
        )
        engine = ExtractionEngine(config)
        engine.schema = schema
        engine.adapter = adapter

        with patch("dbslice.core.engine.logger") as mock_logger:
            engine._process_seed(config.seeds[0])
            mock_logger.warning.assert_not_called()
