#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULTS_DIR="$ROOT_DIR/results"
mkdir -p "$RESULTS_DIR"

RUNNER_IMAGE="benchmark-runner:latest"

run_one () {
  local deploy_dir="$1"   # folder name under deploy/
  local db_key="$2"       # --db value for runner

  echo "=============================="
  echo "Starting DB: $db_key (deploy/$deploy_dir)"
  echo "=============================="

  pushd "$ROOT_DIR/deploy/$deploy_dir" >/dev/null

  docker compose -p "$db_key" up -d

  echo "Waiting for DB startup..."
  sleep 8

  docker run --rm \
    --network=host \
    -v "$RESULTS_DIR:/app/results" \
    --env-file "$ROOT_DIR/deploy/$deploy_dir/.env" \
    "$RUNNER_IMAGE" \
    --db "$db_key" \
    --benchmark all \
    --runs 5 \
    --warmup 1 \
    --vehicle-id 46 \
    --out /app/results/results.csv

  docker compose -p "$db_key" down -v

  popd >/dev/null
  echo "Done: $db_key"
}

run_one "mssql-narrow"     "mssql_narrow"

echo "ALL DONE. Results in $RESULTS_DIR/results.csv"