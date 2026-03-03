"""Safety tests for tables without primary keys."""

import pytest

from dbslice.adapters.postgresql import PostgreSQLAdapter
from dbslice.config import ExtractConfig, SeedSpec
from dbslice.core.engine import ExtractionEngine
from dbslice.exceptions import ExtractionError
from dbslice.models import Column, ForeignKey, SchemaGraph, Table
from tests.conftest import MockAdapter


class _NoCursorConnection:
    """Connection stub that fails if SQL execution is attempted."""

    def cursor(self, *args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("cursor() should not be called for PK-less guarded paths")


def test_seed_table_without_primary_key_fails_fast():
    schema = SchemaGraph(
        tables={
            "events": Table(
                name="events",
                schema="public",
                columns=[
                    Column(name="id", data_type="INTEGER", nullable=False, is_primary_key=False),
                    Column(
                        name="payload",
                        data_type="TEXT",
                        nullable=True,
                        is_primary_key=False,
                    ),
                ],
                primary_key=(),
                foreign_keys=[],
            )
        },
        edges=[],
    )
    adapter = MockAdapter(schema, {"events": [{"id": 1, "payload": "x"}]})
    config = ExtractConfig(
        database_url="postgresql://localhost/test",
        seeds=[SeedSpec.parse("events.id=1")],
        validate=False,
    )

    engine = ExtractionEngine(config)
    engine.schema = schema
    engine.adapter = adapter

    with pytest.raises(ExtractionError, match="Seed table has no primary key"):
        engine._process_seed(config.seeds[0])


def test_fetch_by_pk_returns_no_rows_when_pk_columns_empty():
    adapter = PostgreSQLAdapter()
    adapter._conn = _NoCursorConnection()

    rows = list(adapter.fetch_by_pk("events", (), {(1,)}))
    assert rows == []


def test_fetch_by_pk_chunked_returns_no_chunks_when_pk_columns_empty():
    adapter = PostgreSQLAdapter()
    adapter._conn = _NoCursorConnection()

    chunks = list(adapter.fetch_by_pk_chunked("events", (), {(1,)}, chunk_size=100))
    assert chunks == []


def test_fetch_fk_values_returns_empty_when_source_table_has_no_pk():
    adapter = PostgreSQLAdapter()
    adapter._conn = _NoCursorConnection()

    schema = SchemaGraph(
        tables={
            "events": Table(
                name="events",
                schema="public",
                columns=[
                    Column(name="event_id", data_type="INTEGER", nullable=False, is_primary_key=False),
                    Column(name="user_id", data_type="INTEGER", nullable=False, is_primary_key=False),
                ],
                primary_key=(),
                foreign_keys=[],
            ),
            "users": Table(
                name="users",
                schema="public",
                columns=[
                    Column(name="id", data_type="INTEGER", nullable=False, is_primary_key=True),
                ],
                primary_key=("id",),
                foreign_keys=[],
            ),
        },
        edges=[],
    )
    adapter.get_schema = lambda schema_name=None: schema  # type: ignore[method-assign]

    fk = ForeignKey(
        name="fk_events_users",
        source_table="events",
        source_columns=("user_id",),
        target_table="users",
        target_columns=("id",),
        is_nullable=False,
    )

    result = adapter.fetch_fk_values("events", fk, {(1,)})
    assert result == set()
