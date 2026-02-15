# dbslice

Extract minimal, referentially-intact database subsets for local development and debugging.

## The Problem

Copying an entire production database to your machine is infeasible. But reproducing a bug often requires having the exact data that caused it. **dbslice** solves this by extracting only the records you need, following foreign key relationships to ensure referential integrity.

## Quick Example

```bash
# Extract an order and all related records
dbslice extract postgres://localhost/myapp --seed "orders.id=12345" > subset.sql

# Import into local database
psql -d localdb < subset.sql
```

## Key Features

| Feature | Description |
|---------|-------------|
| :material-lightning-bolt: **Zero-config** | Introspects schema automatically, no data model file required |
| :material-console: **Single command** | Extract complete data subsets with one command |
| :material-shield-check: **Safe by default** | Auto-detects and anonymizes sensitive fields |
| :material-database: **Cross-database** | PostgreSQL, MySQL, SQLite with identical interface |
| :material-link-variant: **Virtual FKs** | Support for Django GenericForeignKeys and implicit relationships |

## Getting Started

| Step | Link | Description |
|------|------|-------------|
| 1 | [Installation](getting-started/installation.md) | Install dbslice and dependencies |
| 2 | [Quick Start](getting-started/quickstart.md) | Extract your first subset in 5 minutes |
| 3 | [CLI Reference](user-guide/cli-reference.md) | Learn all available options |
| 4 | [Configuration](user-guide/configuration.md) | Set up YAML config files |

## Use Cases

| Scenario | How dbslice helps |
|----------|-------------------|
| **Debug a production bug** | Extract the failing order and all related data |
| **Create test fixtures** | Extract representative data samples with anonymization |
| **Seed development database** | Get realistic data without the full database dump |
| **Data migration testing** | Extract subset to test migration scripts safely |

## How It Works

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Seed ID    │────▶│  FK Graph    │────▶│  Topo Sort   │
│ orders.id=1  │     │  Traversal   │     │  & Output    │
└──────────────┘     └──────────────┘     └──────────────┘
                            │
                     ┌──────┴──────┐
                     │             │
                 ┌───▼───┐   ┌─────▼─────┐
                 │ users │   │ products  │
                 └───────┘   └───────────┘
```

1. **Introspect** - Reads database schema to discover tables and FK relationships
2. **Traverse** - Starting from seed record(s), follows FK relationships via BFS
3. **Extract** - Fetches all identified records
4. **Sort** - Topologically sorts tables for correct INSERT order
5. **Output** - Generates SQL/JSON/CSV with proper escaping

## Documentation

- **[Installation](getting-started/installation.md)** - Install dbslice and dependencies
- **[Quick Start](getting-started/quickstart.md)** - Extract your first subset in 5 minutes
- **[CLI Reference](user-guide/cli-reference.md)** - All available commands and options
- **[Configuration](user-guide/configuration.md)** - YAML config file reference
- **[Advanced Usage](user-guide/advanced-usage.md)** - Anonymization, streaming, virtual FKs
- **[Best Practices](help/best-practices.md)** - Quick tips for effective use
