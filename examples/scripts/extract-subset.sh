#!/bin/bash
# Example: Extract a database subset
#
# Usage:
#   DATABASE_URL=postgres://localhost/myapp ./extract-subset.sh
#
# Or with a specific seed:
#   DATABASE_URL=postgres://localhost/myapp SEED="orders.id=123" ./extract-subset.sh

set -e

# Configuration
DATABASE_URL="${DATABASE_URL:?DATABASE_URL environment variable is required}"
SEED="${SEED:-orders.id=1}"
OUTPUT_FILE="${OUTPUT_FILE:-subset.sql}"
DEPTH="${DEPTH:-3}"

echo "Extracting subset from database..."
echo "  Seed: $SEED"
echo "  Depth: $DEPTH"
echo "  Output: $OUTPUT_FILE"
echo ""

dbslice extract "$DATABASE_URL" \
  --seed "$SEED" \
  --depth "$DEPTH" \
  --anonymize \
  --out-file "$OUTPUT_FILE" \
  --verbose

echo ""
echo "Done! Import with:"
echo "  psql -d localdb < $OUTPUT_FILE"
