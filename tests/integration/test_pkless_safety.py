"""Integration tests for PK-less table safety hardening."""

import json
import subprocess

import pytest

pytestmark = pytest.mark.integration


def test_seed_table_without_primary_key_fails(
    pg_connection,
    clean_database,
    test_db_url: str,
):
    with pg_connection.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE audit_log (
                event_id INTEGER NOT NULL,
                message TEXT
            )
            """
        )
        cur.execute(
            """
            INSERT INTO audit_log (event_id, message) VALUES
            (1, 'login'),
            (2, 'logout')
            """
        )

    result = subprocess.run(
        [
            "python",
            "-m",
            "dbslice.cli",
            "extract",
            test_db_url,
            "--seed",
            "audit_log.event_id=1",
            "--no-progress",
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    combined = result.stdout + result.stderr
    assert "Seed table has no primary key" in combined


def test_non_seed_pk_less_table_is_skipped_safely(
    pg_connection,
    clean_database,
    test_db_url: str,
):
    with pg_connection.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE parent_lookup (
                code TEXT NOT NULL UNIQUE,
                label TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE child_events (
                id SERIAL PRIMARY KEY,
                parent_code TEXT REFERENCES parent_lookup(code),
                payload TEXT
            )
            """
        )
        cur.execute(
            """
            INSERT INTO parent_lookup (code, label) VALUES
            ('A', 'Alpha'),
            ('B', 'Beta')
            """
        )
        cur.execute(
            """
            INSERT INTO child_events (id, parent_code, payload) VALUES
            (1, 'A', 'first'),
            (2, 'B', 'second')
            """
        )

    result = subprocess.run(
        [
            "python",
            "-m",
            "dbslice.cli",
            "extract",
            test_db_url,
            "--seed",
            "child_events.id=1",
            "--output",
            "json",
            "--no-validate",
            "--no-progress",
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    tables = payload["tables"]
    assert "child_events" in tables
    # PK-less parent table should be skipped for data extraction. Depending on
    # output formatter behavior, it may be omitted or included as an empty list.
    assert tables.get("parent_lookup", []) == []
