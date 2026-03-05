"""Tests for non-nullable cycle fallback behavior."""

import pytest

from dbslice.config import DatabaseType, ExtractConfig, OutputFormat, SeedSpec
from dbslice.core.engine import ExtractionEngine
from dbslice.exceptions import CircularReferenceError
from dbslice.models import Column, ForeignKey, SchemaGraph, Table


def _make_cycle_schema(*, deferrable: bool) -> SchemaGraph:
    table_a = Table(
        name="a",
        schema="public",
        columns=[
            Column(name="id", data_type="integer", nullable=False, is_primary_key=True),
            Column(name="b_id", data_type="integer", nullable=False, is_primary_key=False),
        ],
        primary_key=("id",),
        foreign_keys=[],
    )
    table_b = Table(
        name="b",
        schema="public",
        columns=[
            Column(name="id", data_type="integer", nullable=False, is_primary_key=True),
            Column(name="a_id", data_type="integer", nullable=False, is_primary_key=False),
        ],
        primary_key=("id",),
        foreign_keys=[],
    )

    fk_a_to_b = ForeignKey(
        name="fk_a_b",
        source_table="a",
        source_columns=("b_id",),
        target_table="b",
        target_columns=("id",),
        is_nullable=False,
        is_deferrable=deferrable,
    )
    fk_b_to_a = ForeignKey(
        name="fk_b_a",
        source_table="b",
        source_columns=("a_id",),
        target_table="a",
        target_columns=("id",),
        is_nullable=False,
        is_deferrable=deferrable,
    )

    table_a.foreign_keys.append(fk_a_to_b)
    table_b.foreign_keys.append(fk_b_to_a)

    return SchemaGraph(
        tables={"a": table_a, "b": table_b},
        edges=[fk_a_to_b, fk_b_to_a],
    )


def test_non_sql_format_uses_deterministic_cycle_fallback():
    config = ExtractConfig(
        database_url="postgresql://localhost/test",
        seeds=[SeedSpec.parse("a.id=1")],
        output_format=OutputFormat.JSON,
    )
    engine = ExtractionEngine(config)
    engine.schema = _make_cycle_schema(deferrable=False)

    insert_order, broken_fks, cycle_infos, used_deferred_cycle_strategy = engine._topological_sort(
        {"a", "b"},
        DatabaseType.POSTGRESQL,
    )

    assert insert_order == ["a", "b"]
    assert broken_fks == []
    assert len(cycle_infos) == 1
    assert used_deferred_cycle_strategy is False


def test_sql_disable_fk_checks_uses_deferred_cycle_fallback_when_deferrable():
    config = ExtractConfig(
        database_url="postgresql://localhost/test",
        seeds=[SeedSpec.parse("a.id=1")],
        output_format=OutputFormat.SQL,
        disable_fk_checks=True,
    )
    engine = ExtractionEngine(config)
    engine.schema = _make_cycle_schema(deferrable=True)

    insert_order, broken_fks, cycle_infos, used_deferred_cycle_strategy = engine._topological_sort(
        {"a", "b"},
        DatabaseType.POSTGRESQL,
    )

    assert insert_order == ["a", "b"]
    assert broken_fks == []
    assert len(cycle_infos) == 1
    assert used_deferred_cycle_strategy is True


def test_sql_disable_fk_checks_still_fails_for_non_deferrable_cycles():
    config = ExtractConfig(
        database_url="postgresql://localhost/test",
        seeds=[SeedSpec.parse("a.id=1")],
        output_format=OutputFormat.SQL,
        disable_fk_checks=True,
    )
    engine = ExtractionEngine(config)
    engine.schema = _make_cycle_schema(deferrable=False)

    with pytest.raises(CircularReferenceError, match="deferrable constraints"):
        engine._topological_sort({"a", "b"}, DatabaseType.POSTGRESQL)
