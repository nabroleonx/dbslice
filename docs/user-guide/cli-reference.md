# CLI Reference

Complete reference for the dbslice command-line interface.

## Table of Contents

- [Installation](#installation)
- [Commands](#commands)
  - [extract](#extract)
  - [init](#init)
  - [inspect](#inspect)
- [Global Options](#global-options)
- [Environment Variables](#environment-variables)
- [Exit Codes](#exit-codes)
- [Examples](#examples)
- [Shell Completion](#shell-completion)

---

## Installation

```bash
# Install dbslice
uv add dbslice

# Verify installation
dbslice --version
```

---

## Commands

### extract

Extract a database subset starting from seed record(s).

#### Synopsis

```bash
dbslice extract [OPTIONS] DATABASE_URL
```

#### Arguments

| Argument | Description |
|----------|-------------|
| `DATABASE_URL` | Database connection URL (e.g., `postgresql://user:pass@host:5432/dbname`) |

#### Options

##### Connection Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--schema` | TEXT | `public` | PostgreSQL schema name |
| `--config`, `-c` | PATH | - | Path to YAML configuration file |

##### Seed Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--seed`, `-s` | TEXT | *Required* | Seed record specification (repeatable) |

**Seed Formats:**
- `table.column=value` - Simple equality (e.g., `orders.id=12345`)
- `table:WHERE_CLAUSE` - Raw WHERE clause (e.g., `orders:status='failed'`)

##### Traversal Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--depth`, `-d` | INTEGER | `3` | Maximum FK traversal depth |
| `--direction` | TEXT | `both` | Traversal direction: `up`, `down`, or `both` |
| `--exclude`, `-x` | TEXT | - | Tables to exclude (repeatable) |

##### Output Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--output`, `-o` | TEXT | `sql` | Output format: `sql`, `json`, or `csv` |
| `--out-file`, `-f` | PATH | - | Write to file instead of stdout |
| `--json-mode` | TEXT | `auto` | JSON mode: `auto`, `single`, or `per-table` |
| `--json-pretty` / `--json-compact` | FLAG | Pretty | Enable/disable JSON pretty-printing |

##### Anonymization Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--anonymize`, `-a` | FLAG | `False` | Enable automatic anonymization of sensitive fields |
| `--redact`, `-r` | TEXT | - | Additional fields to redact (repeatable, format: `table.column`) |

##### Validation Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--validate` / `--no-validate` | FLAG | Enabled | Validate extraction for referential integrity |
| `--fail-on-validation-error` | FLAG | `False` | Stop execution if validation finds issues |

##### Performance Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--profile` | FLAG | `False` | Enable query profiling and show statistics |
| `--stream` | FLAG | `False` | Force streaming mode (requires `--out-file`) |
| `--stream-threshold` | INTEGER | `50000` | Auto-enable streaming above this row count |
| `--stream-chunk-size` | INTEGER | `1000` | Rows per chunk in streaming mode |

##### Display Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--verbose`, `-v` | FLAG | `False` | Show detailed logs including traversal path |
| `--no-progress` | FLAG | `False` | Disable progress output (for piping) |
| `--dry-run` | FLAG | `False` | Show what would be extracted without fetching data |

#### Examples

##### Basic Extraction

```bash
# Extract by primary key
dbslice extract postgresql://localhost/myapp --seed "orders.id=12345"

# Extract to file
dbslice extract postgresql://localhost/myapp -s "orders.id=12345" -f subset.sql

# With verbose output
dbslice extract postgresql://localhost/myapp -s "orders.id=12345" -f subset.sql -v
```

##### Multiple Seeds

```bash
# Multiple seeds (same table)
dbslice extract postgresql://localhost/myapp \
  -s "orders.id=12345" \
  -s "orders.id=67890"

# Multiple seeds (different tables)
dbslice extract postgresql://localhost/myapp \
  -s "orders.id=12345" \
  -s "users.email='test@example.com'"
```

##### WHERE Clause Seeds

```bash
# Simple condition
dbslice extract postgresql://localhost/myapp \
  -s "orders:status='failed'"

# Complex condition
dbslice extract postgresql://localhost/myapp \
  -s "orders:created_at >= '2023-01-01' AND status='pending'"

# Multiple conditions with AND/OR
dbslice extract postgresql://localhost/myapp \
  -s "users:age > 18 AND (country='US' OR country='CA')"
```

##### Traversal Direction

```bash
# Parents only (dependencies)
dbslice extract postgresql://localhost/myapp \
  -s "orders.id=12345" \
  --direction up

# Children only (referencing records)
dbslice extract postgresql://localhost/myapp \
  -s "users.id=42" \
  --direction down

# Both directions (default)
dbslice extract postgresql://localhost/myapp \
  -s "orders.id=12345" \
  --direction both
```

##### Depth Control

```bash
# Shallow extraction (depth=1)
dbslice extract postgresql://localhost/myapp \
  -s "orders.id=12345" \
  --depth 1

# Deep extraction (depth=10)
dbslice extract postgresql://localhost/myapp \
  -s "orders.id=12345" \
  --depth 10
```

##### Excluding Tables

```bash
# Exclude single table
dbslice extract postgresql://localhost/myapp \
  -s "orders.id=12345" \
  --exclude audit_logs

# Exclude multiple tables
dbslice extract postgresql://localhost/myapp \
  -s "orders.id=12345" \
  --exclude audit_logs \
  --exclude sessions \
  --exclude temp_data
```

##### Anonymization

```bash
# Auto-detect and anonymize sensitive fields
dbslice extract postgresql://localhost/myapp \
  -s "users.id=1" \
  --anonymize

# Anonymize with custom redactions
dbslice extract postgresql://localhost/myapp \
  -s "users.id=1" \
  --anonymize \
  --redact users.ssn \
  --redact payments.card_number \
  --redact customers.tax_id
```

##### JSON Output

```bash
# JSON to stdout
dbslice extract postgresql://localhost/myapp \
  -s "orders.id=12345" \
  --output json

# JSON to file (single file)
dbslice extract postgresql://localhost/myapp \
  -s "orders.id=12345" \
  --output json \
  --out-file subset.json

# JSON per table (directory)
dbslice extract postgresql://localhost/myapp \
  -s "orders.id=12345" \
  --output json \
  --json-mode per-table \
  --out-file output_dir/

# Compact JSON
dbslice extract postgresql://localhost/myapp \
  -s "orders.id=12345" \
  --output json \
  --json-compact
```

##### Streaming Large Datasets

```bash
# Force streaming mode
dbslice extract postgresql://localhost/myapp \
  -s "orders:created_at > '2020-01-01'" \
  --out-file large_subset.sql \
  --stream

# Auto-enable streaming at 100K rows
dbslice extract postgresql://localhost/myapp \
  -s "orders:created_at > '2020-01-01'" \
  --out-file large_subset.sql \
  --stream-threshold 100000

# Streaming with smaller chunks
dbslice extract postgresql://localhost/myapp \
  -s "orders:created_at > '2020-01-01'" \
  --out-file large_subset.sql \
  --stream \
  --stream-chunk-size 500
```

##### Query Profiling

```bash
# Enable profiling
dbslice extract postgresql://localhost/myapp \
  -s "orders.id=12345" \
  --profile \
  -v

# Profile with streaming
dbslice extract postgresql://localhost/myapp \
  -s "orders:created_at > '2020-01-01'" \
  --out-file large.sql \
  --stream \
  --profile
```

##### Validation

```bash
# Validate but continue on errors (default)
dbslice extract postgresql://localhost/myapp \
  -s "orders.id=12345" \
  --validate

# Fail on validation errors
dbslice extract postgresql://localhost/myapp \
  -s "orders.id=12345" \
  --validate \
  --fail-on-validation-error

# Skip validation
dbslice extract postgresql://localhost/myapp \
  -s "orders.id=12345" \
  --no-validate
```

##### Piping and Scripting

```bash
# Pipe to psql
dbslice extract postgresql://localhost/myapp \
  -s "orders.id=12345" \
  --no-progress | psql postgresql://localhost/test_db

# Pipe to gzip
dbslice extract postgresql://localhost/myapp \
  -s "orders.id=12345" \
  --no-progress | gzip > subset.sql.gz

# Dry run to preview
dbslice extract postgresql://localhost/myapp \
  -s "orders.id=12345" \
  --dry-run
```

---

### init

Generate a configuration file from database schema.

#### Synopsis

```bash
dbslice init [OPTIONS] DATABASE_URL
```

#### Arguments

| Argument | Description |
|----------|-------------|
| `DATABASE_URL` | Database connection URL |

#### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--out-file`, `-f` | PATH | `dbslice.yaml` | Output config file path |
| `--detect-sensitive` / `--no-detect-sensitive` | FLAG | Enabled | Auto-detect sensitive fields |
| `--schema` | TEXT | `public` | PostgreSQL schema name |

#### Examples

```bash
# Generate default config
dbslice init postgresql://localhost/myapp

# Generate to specific file
dbslice init postgresql://localhost/myapp -f config/production.yaml

# Generate without sensitive field detection
dbslice init postgresql://localhost/myapp --no-detect-sensitive

# Generate for remote database
dbslice init postgresql://user:pass@prod.example.com:5432/myapp \
  -f config/prod.yaml

# Generate config for a specific schema
dbslice init postgresql://localhost/myapp --schema myschema
```

#### Generated Config Structure

The `init` command generates a YAML configuration file with:
- Database connection details
- Default extraction settings
- Auto-detected sensitive fields (if enabled)
- Commented sections for easy customization

Example generated config:

```yaml
# dbslice configuration
version: "1.0"

database:
  url: postgresql://localhost/myapp

extraction:
  default_depth: 3
  direction: both
  exclude_tables: []

anonymization:
  enabled: true
  fields:
    users.email: email
    users.phone: phone_number
    users.ssn: ssn

output:
  format: sql
  include_transaction: true
  include_drop_tables: false
```

---

### inspect

Inspect database schema without extracting data.

#### Synopsis

```bash
dbslice inspect [OPTIONS] DATABASE_URL
```

#### Arguments

| Argument | Description |
|----------|-------------|
| `DATABASE_URL` | Database connection URL |

#### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--table`, `-t` | TEXT | - | Show details for a specific table |
| `--schema` | TEXT | `public` | PostgreSQL schema name |

#### Examples

##### Show All Tables

```bash
# List all tables and foreign keys
dbslice inspect postgresql://localhost/myapp

# Inspect a specific schema
dbslice inspect postgresql://localhost/myapp --schema myschema
```

Output:
```
Tables (15)
  users (id)
  orders (id)
  order_items (id)
  products (id)
  ...

Foreign Keys (23)
  orders.user_id -> users.id (required)
  order_items.order_id -> orders.id (required)
  order_items.product_id -> products.id (required)
  ...

Self-references (potential cycles):
  categories.parent_id
```

##### Inspect Specific Table

```bash
# Show details for one table
dbslice inspect postgresql://localhost/myapp --table orders
```

Output:
```
orders
  Schema: public
  Primary key: id

  Columns:
    id: integer NOT NULL [PK]
    user_id: integer NOT NULL
    status: character varying NULL
    total_amount: numeric(10,2) NULL
    created_at: timestamp with time zone NULL

  Foreign keys (references):
    user_id -> users.id (required)

  Referenced by:
    order_items.order_id
    payments.order_id
```

##### Inspect Multiple Tables

```bash
# Inspect multiple tables in sequence
for table in users orders products; do
  echo "=== $table ==="
  dbslice inspect postgresql://localhost/myapp -t $table
  echo
done
```

---

## Global Options

These options work with all commands:

| Option | Description |
|--------|-------------|
| `--version`, `-V` | Show version and exit |
| `--help` | Show help message and exit |

```bash
# Show version
dbslice --version

# Show help for command
dbslice extract --help
dbslice init --help
dbslice inspect --help
```

---

## Environment Variables

dbslice supports the following environment variables:

### Database Connection

| Variable | Description | Example |
|----------|-------------|---------|
| `DATABASE_URL` | Default database connection URL | `postgresql://localhost/myapp` |
| `PGHOST` | PostgreSQL host | `localhost` |
| `PGPORT` | PostgreSQL port | `5432` |
| `PGUSER` | PostgreSQL user | `myuser` |
| `PGPASSWORD` | PostgreSQL password | `mypassword` |
| `PGDATABASE` | PostgreSQL database | `mydb` |

### Extraction Configuration

| Variable | Description | Example |
|----------|-------------|---------|
| `DBSLICE_DEPTH` | Default traversal depth | `3` |
| `DBSLICE_DIRECTION` | Default traversal direction | `both` |
| `DBSLICE_OUTPUT_FORMAT` | Default output format | `sql` |

### Security

| Variable | Description | Example |
|----------|-------------|---------|
| `DBSLICE_ANONYMIZE` | Enable anonymization | `true` |
| `DBSLICE_REDACT_FIELDS` | Comma-separated redact fields | `users.ssn,payments.card` |

### Examples

```bash
# Set database URL
export DATABASE_URL="postgresql://localhost/myapp"
dbslice extract --seed "orders.id=12345"

# Set default depth
export DBSLICE_DEPTH=5
dbslice extract postgresql://localhost/myapp --seed "orders.id=12345"

# Enable anonymization by default
export DBSLICE_ANONYMIZE=true
export DBSLICE_REDACT_FIELDS="users.ssn,users.passport"
dbslice extract postgresql://localhost/myapp --seed "users.id=1"

# PostgreSQL-specific variables
export PGHOST=localhost
export PGPORT=5432
export PGUSER=myuser
export PGPASSWORD=mypassword
export PGDATABASE=mydb
dbslice extract --seed "orders.id=12345"
```

---

## Exit Codes

dbslice uses standard exit codes to indicate success or failure:

| Code | Meaning | Description |
|------|---------|-------------|
| `0` | Success | Extraction completed successfully |
| `1` | Error | Generic error occurred |
| `2` | Usage Error | Invalid command-line arguments |

### Exit Code Examples

```bash
# Check exit code
dbslice extract postgresql://localhost/myapp -s "orders.id=12345"
echo $?  # 0 = success, 1 = error

# Use in scripts
if dbslice extract postgresql://localhost/myapp -s "orders.id=12345" -f subset.sql; then
  echo "Extraction succeeded"
  psql postgresql://localhost/test_db < subset.sql
else
  echo "Extraction failed with code $?"
  exit 1
fi

# Exit on error in scripts
set -e
dbslice extract postgresql://localhost/myapp -s "orders.id=12345" -f subset.sql
# Script stops here if extraction fails
```

---

## Examples

### Complete Workflow Examples

#### Development Database Subset

```bash
# Extract subset from production for local development
dbslice extract \
  postgresql://prod.example.com/myapp \
  --seed "users:created_at >= '2023-01-01' AND status='active'" \
  --depth 3 \
  --anonymize \
  --redact users.ssn \
  --redact payments.card_number \
  --out-file dev_subset.sql \
  --verbose

# Load into local database
psql postgresql://localhost/myapp_dev < dev_subset.sql
```

#### Test Fixture Generation

```bash
# Generate test fixtures with known data
dbslice extract \
  postgresql://localhost/myapp \
  --seed "users.email='test@example.com'" \
  --seed "orders:status='test'" \
  --depth 5 \
  --anonymize \
  --out-file tests/fixtures/test_data.sql \
  --no-progress

# Use in tests
pytest --fixtures tests/fixtures/test_data.sql
```

#### Bug Reproduction

```bash
# Extract minimal dataset for bug reproduction
dbslice extract \
  postgresql://prod.example.com/myapp \
  --seed "orders.id=FAILING_ORDER_ID" \
  --direction both \
  --depth 10 \
  --anonymize \
  --out-file bug_reproduction.sql \
  --profile \
  --verbose

# Share with team
gzip bug_reproduction.sql
# bug_reproduction.sql.gz can be shared safely (anonymized)
```

#### Large Dataset Migration

```bash
# Extract large subset with streaming
dbslice extract \
  postgresql://source.example.com/myapp \
  --seed "orders:created_at >= '2023-01-01'" \
  --depth 3 \
  --out-file migration.sql \
  --stream \
  --stream-threshold 100000 \
  --stream-chunk-size 1000 \
  --profile \
  --verbose

# Shows memory-efficient processing of large datasets
```

#### CI/CD Integration

```bash
#!/bin/bash
# ci/generate_test_data.sh

set -e

echo "Generating test data subset..."

dbslice extract \
  "$PRODUCTION_DATABASE_URL" \
  --seed "users:is_test_user=true" \
  --depth 3 \
  --anonymize \
  --redact users.ssn \
  --redact payments.card_number \
  --out-file ci/test_data.sql \
  --no-progress \
  --fail-on-validation-error

echo "Loading test data..."
psql "$CI_DATABASE_URL" < ci/test_data.sql

echo "Test data ready!"
```

#### Schema Documentation

```bash
# Generate schema documentation
dbslice inspect postgresql://localhost/myapp > docs/schema.txt

# Inspect critical tables
for table in users orders payments; do
  echo "## $table" >> docs/schema.md
  dbslice inspect postgresql://localhost/myapp -t $table >> docs/schema.md
  echo >> docs/schema.md
done
```

---

## Shell Completion

### Bash

```bash
# Add to ~/.bashrc
eval "$(_DBSLICE_COMPLETE=bash_source dbslice)"
```

### Zsh

```zsh
# Add to ~/.zshrc
eval "$(_DBSLICE_COMPLETE=zsh_source dbslice)"
```

### Fish

```fish
# Add to ~/.config/fish/completions/dbslice.fish
eval (env _DBSLICE_COMPLETE=fish_source dbslice)
```

---

## See Also

- [Configuration Reference](configuration.md) -- YAML configuration
- [Advanced Usage](advanced-usage.md) -- Anonymization, streaming, virtual FKs
