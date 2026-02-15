# Installation

## Requirements

- Python 3.10 or higher
- [uv](https://docs.astral.sh/uv/) package manager (recommended)

## Install from PyPI

```bash
# Install with uv (recommended)
uv add dbslice

# Try without installing
uvx dbslice --help

# Or with pip
pip install dbslice
```

## Verify Installation

```bash
# Check version
dbslice --version

# View help
dbslice --help
```

## Database Drivers

dbslice uses the following database drivers:

| Database | Driver | Status |
|----------|--------|--------|
| PostgreSQL | psycopg2-binary | Included |
| MySQL | mysql-connector-python | Planned (not yet implemented) |
| SQLite | sqlite3 (stdlib) | Planned (not yet implemented) |

## Development Setup

For contributing to dbslice:

```bash
# Clone repository
git clone https://github.com/nabroleonx/dbslice.git
cd dbslice

# Install in development mode
uv sync --dev

# Run tests
uv run pytest
```

## Environment Variables

You can set your database URL as an environment variable:

```bash
export DATABASE_URL="postgres://user:pass@localhost:5432/mydb"

# Then use without specifying URL
dbslice extract $DATABASE_URL --seed "orders.id=1"
```

## Next Steps

- [Quick Start Guide](quickstart.md) - Extract your first subset
- [CLI Reference](../user-guide/cli-reference.md) - All command options
