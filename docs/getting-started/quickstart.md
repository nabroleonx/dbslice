# Quick Start

This guide will have you extracting database subsets in under 5 minutes.

## Basic Extraction

Extract a single record and all its related data:

```bash
dbslice extract postgres://user:pass@localhost/mydb --seed "orders.id=12345"
```

This outputs SQL to stdout. Pipe it to a file:

```bash
dbslice extract postgres://localhost/mydb --seed "orders.id=12345" > subset.sql
```

## Understanding Seeds

Seeds tell dbslice where to start. Two formats are supported:

**Primary Key** — `table.column=value`

```bash
dbslice extract $DB_URL --seed "orders.id=12345"
dbslice extract $DB_URL --seed "users.email=john@example.com"
```

**WHERE Clause** — `table:WHERE_CLAUSE`

```bash
dbslice extract $DB_URL --seed "orders:status='failed'"
dbslice extract $DB_URL --seed "orders:created_at > '2024-01-01'"
```

## Multiple Seeds

Extract data for multiple records:

```bash
dbslice extract $DB_URL \
  --seed "orders.id=100" \
  --seed "orders.id=101" \
  --seed "orders.id=102"
```

## Control Traversal

### Depth

Limit how many FK hops to follow (default: 3):

```bash
# Only immediate relationships
dbslice extract $DB_URL --seed "orders.id=1" --depth 1

# Deep traversal
dbslice extract $DB_URL --seed "orders.id=1" --depth 5
```

### Direction

Control which relationships to follow:

```bash
# Parents only (referenced tables)
dbslice extract $DB_URL --seed "orders.id=1" --direction up

# Children only (tables that reference this)
dbslice extract $DB_URL --seed "orders.id=1" --direction down

# Both (default)
dbslice extract $DB_URL --seed "orders.id=1" --direction both
```

## Anonymize Sensitive Data

Automatically detect and anonymize PII:

```bash
dbslice extract $DB_URL --seed "users.id=1" --anonymize
```

Add specific fields to redact:

```bash
dbslice extract $DB_URL --seed "users.id=1" \
  --anonymize \
  --redact "audit_logs.ip_address"
```

## Output Formats

```bash
# SQL (default)
dbslice extract $DB_URL --seed "orders.id=1" --output sql

# JSON
dbslice extract $DB_URL --seed "orders.id=1" --output json

# CSV
dbslice extract $DB_URL --seed "orders.id=1" --output csv
```

## Save to File

```bash
dbslice extract $DB_URL --seed "orders.id=1" --out-file subset.sql
```

## Import to Local Database

```bash
# Extract
dbslice extract $PROD_DB --seed "orders.id=1" --anonymize > subset.sql

# Import to local
psql -d localdb < subset.sql
```

## Inspect Schema First

Before extracting, you can inspect your database schema:

```bash
dbslice inspect postgres://localhost/mydb
```

This shows tables, foreign keys, and detected sensitive fields.

## Using Config Files

For repeated extractions, use a config file:

```bash
# Generate config from database
dbslice init postgres://localhost/mydb

# Use config
dbslice extract --config dbslice.yaml --seed "orders.id=123"
```

## Next Steps

- [CLI Reference](../user-guide/cli-reference.md) - All options explained
- [Configuration](../user-guide/configuration.md) - YAML config files
- [Advanced Usage](../user-guide/advanced-usage.md) - Complex scenarios
