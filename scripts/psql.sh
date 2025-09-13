#!/usr/bin/env bash
# Open a psql session using environment variables
# Usage: ./scripts/psql.sh

# Load environment variables
source "$(dirname "$0")/env.sh"

# Connect to Postgres
psql "postgresql://$PGUSER:$PGPASSWORD@$PGHOST:$PGPORT/$PGDATABASE"