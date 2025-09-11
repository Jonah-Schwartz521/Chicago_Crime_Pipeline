#!/usr/bin/env bash
set -euo pipefail
cd /Users/jonahschwartz/Code/chicago-crime-pipeline
source .venv/bin/activate
set -a; source .env; set +a
python ingest_smoketest.py >> logs/ingest.log 2>&1
