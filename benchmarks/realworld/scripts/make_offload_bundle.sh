#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
DEFAULT_OUTPUT="/tmp/tracedb-offload-$(date -u +%Y%m%dT%H%M%SZ).tar.gz"
OUTPUT="${OFFLOAD_BUNDLE:-${DEFAULT_OUTPUT}}"

mkdir -p "$(dirname "${OUTPUT}")"

tar -czf "${OUTPUT}" \
  -C "${REPO_ROOT}" \
  --exclude='./target' \
  --exclude='./.git' \
  --exclude='./.env' \
  --exclude='./.env.local' \
  --exclude='./benchmarks/realworld/.env.local' \
  --exclude='./benchmarks/realworld/.cache' \
  --exclude='./benchmarks/realworld/.venv' \
  --exclude='./benchmarks/realworld/run-data' \
  --exclude='./benchmarks/realworld/report-bundles' \
  --exclude='./benchmarks/realworld/reports' \
  .

echo "${OUTPUT}"
