#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULTS_DIR="$ROOT_DIR/results"
mkdir -p "$RESULTS_DIR"

RUNNER_IMAGE="benchmark-runner:latest"

DBS=(
  "mssql_narrow"
  "mssql_wide"
)

run_one_db () {
  local db="$1"
  local deploy_dir="$ROOT_DIR/deploy/$db"

  echo "=============================="
  echo "Starting DB: $db"
  echo "=============================="

  pushd "$deploy_dir" >/dev/null

  docker compose -p "$db" up -d

  echo "Waiting for DB to become ready..."
  sleep 10

  # Run benchmarks
  docker run --rm \
    --network=host \
    -v "$RESULTS_DIR:/app/results" \
    --env-file "$deploy_dir/.env" \
    "$RUNNER_IMAGE" \
    --db "$db" \
    --benchmarks all \
    --runs 5 \
    --warmup 1

  # Stop DB
  docker compose -p "$db" down -v

  popd >/dev/null

  echo "Done: $db"
}

for db in "${DBS[@]}"; do
  run_one_db "$db"
done

echo "ALL DONE. Results in: $RESULTS_DIR"
