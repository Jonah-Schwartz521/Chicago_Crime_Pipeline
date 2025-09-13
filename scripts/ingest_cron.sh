#!/usr/bin/env bash
# Runs: ingest -> dbt (staging) -> QC gate
# Loads .env, uses project venv, prevents overlap, writes logs.

set -euo pipefail

PROJECT_ROOT="/Volumes/easystore/Projects/chicago-crime-pipeline"
VENV_BIN="$PROJECT_ROOT/.venv/bin"
LOG_DIR="$PROJECT_ROOT/logs"
LOCKFILE="$LOG_DIR/ingest.lock"

mkdir -p "$LOG_DIR"
LOGFILE="$LOG_DIR/ingest_$(date +%F).log"

# prevent overlapping runs
exec 9>"$LOCKFILE"
if ! flock -n 9; then
  echo "$(date -Is) [skipped] another run is in progress" | tee -a "$LOGFILE"
  exit 0
fi

{
  echo "================================================"
  echo "$(date -Is) [start] chicago-crime pipeline"
  echo "ROOT=$PROJECT_ROOT"
} >> "$LOGFILE"

cd "$PROJECT_ROOT"

# load env (does not print secrets)
source "$PROJECT_ROOT/scripts/env.sh" >> "$LOGFILE" 2>&1 || true

# sanity for required vars
for k in PGHOST PGPORT PGUSER PGPASSWORD PGDATABASE; do
  if [ -z "${!k:-}" ]; then
    echo "$(date -Is) [error] missing env var: $k" | tee -a "$LOGFILE"
    exit 1
  fi
done

PY="$VENV_BIN/python"
DBT="$VENV_BIN/dbt"

# 1) Ingest JSON into raw (idempotent upsert)
echo "$(date -Is) [step] ingest" >> "$LOGFILE"
$PY -u src/ingest.py >> "$LOGFILE" 2>&1

# 2) dbt staging + tests
echo "$(date -Is) [step] dbt run/test (staging)" >> "$LOGFILE"
$DBT run --select stg_chicago_crimes >> "$LOGFILE" 2>&1
$DBT test --select stg_chicago_crimes >> "$LOGFILE" 2>&1

# 3) QC gate
echo "$(date -Is) [step] qc_gate" >> "$LOGFILE"
$PY -u scripts/qc_gate.py >> "$LOGFILE" 2>&1

echo "$(date -Is) [done] pipeline finished OK" >> "$LOGFILE"