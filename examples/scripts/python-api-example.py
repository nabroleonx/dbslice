#!/usr/bin/env python3
"""Example: Using dbslice as a Python library.

This script demonstrates how to use dbslice programmatically
for automated extraction workflows.

Usage:
    DATABASE_URL=postgres://localhost/myapp python python-api-example.py
    DATABASE_URL=postgres://localhost/myapp ORDER_ID=456 python python-api-example.py
"""

import os
from pathlib import Path

from dbslice.config import ExtractConfig, OutputFormat, SeedSpec, TraversalDirection
from dbslice.core.engine import ExtractionEngine
from dbslice.output.sql import SQLGenerator
from dbslice.utils.connection import parse_database_url


def extract_order_subset(database_url: str, order_id: int) -> str:
    """Extract an order and all related records.

    Args:
        database_url: Database connection URL
        order_id: The order ID to extract

    Returns:
        SQL statements as a string
    """
    # Configure extraction
    config = ExtractConfig(
        database_url=database_url,
        seeds=[SeedSpec(table="orders", column="id", value=str(order_id))],
        depth=3,
        direction=TraversalDirection.BOTH,
        output_format=OutputFormat.SQL,
        anonymize=True,  # Always anonymize for safety
    )

    # Run extraction
    engine = ExtractionEngine(config)
    result, schema = engine.extract()

    # Print summary
    print(f"Extracted {result.total_rows()} rows from {result.table_count()} tables:")
    for table in result.insert_order:
        if table in result.stats:
            print(f"  - {table}: {result.stats[table]} rows")

    # Generate SQL
    db_config = parse_database_url(database_url)
    generator = SQLGenerator(db_type=db_config.db_type)
    sql = generator.generate(
        result.tables,
        result.insert_order,
        schema.tables,
        result.broken_fks,
        result.deferred_updates,
    )

    return sql


def main():
    # Get database URL from environment
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("Error: DATABASE_URL environment variable is required")
        print("")
        print("Example:")
        print("  DATABASE_URL=postgres://localhost/myapp python python-api-example.py")
        return

    # Extract order
    order_id = int(os.environ.get("ORDER_ID", "123"))
    print(f"Extracting order {order_id}...")
    print("")

    sql = extract_order_subset(database_url, order_id)

    # Save to file
    output_path = Path(f"order_{order_id}_subset.sql")
    output_path.write_text(sql)
    print("")
    print(f"Saved to {output_path}")


if __name__ == "__main__":
    main()
