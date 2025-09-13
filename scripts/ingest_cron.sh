#!/usr/bin/env bash
# Runs: ingest -> dbt (staging) -> QC gate
# macOS-safe: uses mkdir lock (not flock) and BSD-compatible date.

set -euo pipefail

PROJECT_ROOT="/Volumes/easystore/Projects/chicago-crime-pipeline"
VENV_BIN="$PROJECT_ROOT/.venv/bin"
LOG_DIR="$PROJECT_ROOT/logs"
LOCK_DIR="$LOG_DIR/ingest.lock"   # directory lock (mkdir is atomic)

mkdir -p "$LOG_DIR"

# timestamp helper (UTC ISO8601)
ts() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

LOGFILE="$LOG_DIR/ingest_$(date +%F).log"

# ---- lock (no flock on macOS) ----
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "$(ts) [skipped] another run is in progress" | tee -a "$LOGFILE"
  exit 0
fi
trap 'rmdir "$LOCK_DIR" 2>/dev/null || rm -f "$LOCK_DIR"' EXIT

# ---- header ----
{
  echo "================================================"
  echo "$(ts) [start] chicago-crime pipeline"
  echo "ROOT=$PROJECT_ROOT"
} >> "$LOGFILE"

cd "$PROJECT_ROOT"

# ---- env ----
if [ -f "$PROJECT_ROOT/scripts/env.sh" ]; then
  # shellcheck disable=SC1091
  source "$PROJECT_ROOT/scripts/env.sh" >> "$LOGFILE" 2>&1 || true
fi

# sanity for required vars
MISSING=0
for k in PGHOST PGPORT PGUSER PGPASSWORD PGDATABASE; do
  if [ -z "${!k:-}" ]; then
    echo "$(ts) [error] missing env var: $k" | tee -a "$LOGFILE"
    MISSING=1
  fi
done
if [ "$MISSING" -ne 0 ]; then
  echo "$(ts) [abort] required env not set" | tee -a "$LOGFILE"
  exit 1
fi

PY="$VENV_BIN/python"
DBT="$VENV_BIN/dbt"

# ---- steps ----
ENTRY="src/ingest/ingest.py"
[ -f "$ENTRY" ] || { echo "$(ts) [error] missing $ENTRY" | tee -a "$LOGFILE"; exit 1; }

echo "$(ts) [step] ingest" >> "$LOGFILE"
$PY -u "$ENTRY" >> "$LOGFILE" 2>&1

echo "$(ts) [step] dbt build (staging)" >> "$LOGFILE"
$DBT build --project-dir ./dbt --profiles-dir ./dbt --select "path:models/staging/*" >> "$LOGFILE" 2>&1

echo "$(ts) [step] qc_gate" >> "$LOGFILE"
$PY -u scripts/qc_gate.py >> "$LOGFILE" 2>&1

echo "$(ts) [done] pipeline finished OK" >> "$LOGFILE"