#!/usr/bin/env bash
# Load project .env into the current shell
# Usage: source scripts/env.sh

if [ -f .env ]; then
  set -a
  . ./.env
  set +a
  echo "[env] loaded from $(pwd)/.env"
else
  echo "[env] .env file not found in $(pwd)"
fi