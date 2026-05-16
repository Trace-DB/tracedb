#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAB_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${LAB_ROOT}/../.." && pwd)"

set +u
source "${LAB_ROOT}/.env.local" 2>/dev/null || true
set -u

TRACEDB_URL="${TRACEDB_HTTP_URL:-${RAILWAY_TRACEDB_URL:-}}"
if [[ -z "${TRACEDB_URL}" ]]; then
  echo "set TRACEDB_HTTP_URL or RAILWAY_TRACEDB_URL to the Railway Gateway/Engine public URL" >&2
  exit 1
fi

PROFILE="${PROFILE:-smoke}"
DATASET="${DATASET:-generated}"
RECORDS="${RECORDS:-1000}"
RUN_ID="${RUN_ID:-railway-tracedb-$(date -u +%Y%m%dT%H%M%SZ)}"
OPENROUTER_MODE="${OPENROUTER_MODE:-auto}"
OPENROUTER_CAP="${OPENROUTER_CAP:-conservative}"
RERANK_MODEL="${RERANK_MODEL:-cohere/rerank-4-fast}"
EMBEDDING_DIMENSIONS="${EMBEDDING_DIMENSIONS:-1536}"
TRACEDB_HTTP_TIMEOUT_SECONDS="${TRACEDB_HTTP_TIMEOUT_SECONDS:-20}"
TRACEDB_HTTP_ADMIN_TIMEOUT_SECONDS="${TRACEDB_HTTP_ADMIN_TIMEOUT_SECONDS:-60}"
TRACEDB_HTTP_RETRIES="${TRACEDB_HTTP_RETRIES:-3}"
SCENARIOS="${SCENARIOS:-http_falsification}"
SURFACE="${SURFACE:-http,curl}"
TARGET="${TARGET:-tracedb}"
VENV_DIR="${LAB_ROOT}/.venv"
PYTHON="${VENV_DIR}/bin/python"

if [[ ! -d "${VENV_DIR}" ]]; then
  python3 -m venv "${VENV_DIR}"
fi
"${PYTHON}" -m pip install --upgrade pip >/dev/null
"${VENV_DIR}/bin/pip" install -r "${LAB_ROOT}/requirements.txt" >/dev/null

curl -fsS --retry 6 --retry-all-errors --connect-timeout 5 --max-time 20 \
  "${TRACEDB_URL%/}/ready" >/dev/null

(
  cd "${LAB_ROOT}"
  TRACEDB_HTTP_URL="${TRACEDB_URL%/}" \
  TRACEDB_HTTP_TIMEOUT_SECONDS="${TRACEDB_HTTP_TIMEOUT_SECONDS}" \
  TRACEDB_HTTP_ADMIN_TIMEOUT_SECONDS="${TRACEDB_HTTP_ADMIN_TIMEOUT_SECONDS}" \
  TRACEDB_HTTP_RETRIES="${TRACEDB_HTTP_RETRIES}" \
  TRACEDB_CLI="${REPO_ROOT}/target/release/tracedb" \
  "${PYTHON}" -m runner suite \
    --profile "${PROFILE}" \
    --dataset "${DATASET}" \
    --records "${RECORDS}" \
    --target "${TARGET}" \
    --surface "${SURFACE}" \
    --openrouter-mode "${OPENROUTER_MODE}" \
    --openrouter-cap "${OPENROUTER_CAP}" \
    --rerank-model "${RERANK_MODEL}" \
    --embedding-dimensions "${EMBEDDING_DIMENSIONS}" \
    --run-id "${RUN_ID}" \
    --reports-dir reports \
    --scenarios "${SCENARIOS}"
)

echo "suite report: ${LAB_ROOT}/reports/${RUN_ID}/suite.md"
