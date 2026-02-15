# dbslice Examples

This directory contains example configurations and scripts for using dbslice.

## Configuration Examples

| File | Description |
|------|-------------|
| [configs/basic.yaml](configs/basic.yaml) | Basic configuration with common settings |
| [configs/with-anonymization.yaml](configs/with-anonymization.yaml) | Configuration with anonymization enabled |
| [configs/virtual-fks.yaml](configs/virtual-fks.yaml) | Using virtual foreign keys for implicit relationships |

## Scripts

| File | Description |
|------|-------------|
| [scripts/extract-subset.sh](scripts/extract-subset.sh) | Shell script for common extraction patterns |
| [scripts/python-api-example.py](scripts/python-api-example.py) | Using dbslice as a Python library |

## Quick Usage

### Using a config file

```bash
# Extract using a configuration file
dbslice extract --config examples/configs/basic.yaml --seed "orders.id=123"
```

### Running example scripts

```bash
# Make script executable
chmod +x examples/scripts/extract-subset.sh

# Run with your database URL
DATABASE_URL=postgres://localhost/myapp ./examples/scripts/extract-subset.sh
```

## Environment Variables

Most examples expect `DATABASE_URL` to be set:

```bash
export DATABASE_URL="postgres://user:pass@localhost:5432/mydb"
```
