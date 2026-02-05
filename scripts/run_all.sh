#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULTS_DIR="$ROOT_DIR/results"
mkdir -p "$RESULTS_DIR"

RUNNER_IMAGE="benchmark-runner:latest"

run_one () {
  local deploy_dir="$1"
  local db_key="$2"


  echo "=============================="
  echo "Starting DB: $db_key (deploy/$deploy_dir)"
  echo "=============================="

  pushd "$ROOT_DIR/deploy/$deploy_dir" >/dev/null

  docker compose -p "$db_key" up -d

  echo "Waiting for DB startup..."
  sleep 15
  echo "Done. Starting with benchmarks now."


  docker run --rm \
    --network=host \
    -v "$RESULTS_DIR:/app/results" \
    --env-file "$ROOT_DIR/deploy/$deploy_dir/.env" \
    "$RUNNER_IMAGE" \
    --db "$db_key" \
    --benchmark all \
    --runs 5 \
    --warmup 2 \
    --vehicle-id 46 \
    --start-ts 2025-04-01T00:00:00 \
    --end-ts 2025-10-31T00:00:00 \
    --vehicle-ids 46,47,48,49 \
    --start-insert-ts 2026-02-02T00:00:00

  docker compose -p "$db_key" down -v

  popd >/dev/null
  echo "Done: $db_key"
}

run_one "mssql-narrow"     "mssql_narrow"
run_one "mssql-wide"       "mssql_wide"
run_one "timescaledb"      "timescaledb"
run_one "questdb"          "questdb"
run_one "influxdb"         "influxdb"

echo "ALL DONE. Results in $RESULTS_DIR/results.csv"
