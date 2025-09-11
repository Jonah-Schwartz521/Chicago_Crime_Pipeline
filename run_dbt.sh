
#!/usr/bin/env bash

set -euo pipefail

cd /Users/jonahschwartz/Code/chicago-crime-pipeline/dbt

source ../.venv/bin/activate

set -a; source ../.env; set +a

../.venv/bin/dbt run >> ../logs/dbt.log 2>&1

