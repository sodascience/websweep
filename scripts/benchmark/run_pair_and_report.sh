#!/usr/bin/env bash
set -euo pipefail

SOURCE_FILE="/Users/garci061/pCloud_sync/backbone/sidn_sample/overview_urls.tsv"
BASE_DIR="/tmp/websweep_bench"
CSV_DIR="$BASE_DIR/csv_1h"
DUCKDB_DIR="$BASE_DIR/duckdb_1h"
REPORT_DIR="$BASE_DIR/report"

rm -rf "$CSV_DIR" "$DUCKDB_DIR" "$REPORT_DIR"
mkdir -p "$CSV_DIR" "$DUCKDB_DIR" "$REPORT_DIR"

echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] START csv run" >&2
UV_CACHE_DIR=/tmp/uv-cache-websweep uv run python scripts/benchmark/run_one_pass_timed.py \
  --source-file "$SOURCE_FILE" \
  --output-dir "$CSV_DIR" \
  --backend csv \
  --duration-seconds 3600 > "$CSV_DIR/run.log" 2>&1

echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] START duckdb run" >&2
UV_CACHE_DIR=/tmp/uv-cache-websweep uv run python scripts/benchmark/run_one_pass_timed.py \
  --source-file "$SOURCE_FILE" \
  --output-dir "$DUCKDB_DIR" \
  --backend duckdb \
  --duration-seconds 3600 > "$DUCKDB_DIR/run.log" 2>&1

echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] ANALYZE" >&2
UV_CACHE_DIR=/tmp/uv-cache-websweep uv run python scripts/benchmark/analyze_runs.py \
  --csv-run-dir "$CSV_DIR" \
  --duckdb-run-dir "$DUCKDB_DIR" \
  --output-json "$REPORT_DIR/backend_comparison.json" \
  --output-md "$REPORT_DIR/backend_comparison.md" > "$REPORT_DIR/analyze.log" 2>&1

echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] CONSOLIDATE" >&2
UV_CACHE_DIR=/tmp/uv-cache-websweep uv run python scripts/benchmark/finalize_runs.py \
  --run-dir "$CSV_DIR" \
  --run-dir "$DUCKDB_DIR" \
  --output-json "$REPORT_DIR/consolidation_check.json" > "$REPORT_DIR/consolidate.log" 2>&1

echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] DONE" >&2

touch "$BASE_DIR/pair_done.flag"
