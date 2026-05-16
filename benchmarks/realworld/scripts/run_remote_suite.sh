#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAB_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${LAB_ROOT}/../.." && pwd)"

PROFILE="${PROFILE:-smoke}"
DATASET="${DATASET:-generated}"
RECORDS="${RECORDS:-1000}"
RUN_ID="${RUN_ID:-remote-$(date -u +%Y%m%dT%H%M%SZ)}"
OPENROUTER_MODE="${OPENROUTER_MODE:-required}"
OPENROUTER_CAP="${OPENROUTER_CAP:-conservative}"
RERANK_MODEL="${RERANK_MODEL:-cohere/rerank-4-fast}"
EMBEDDING_DIMENSIONS="${EMBEDDING_DIMENSIONS:-1536}"
TRACE_HOST="${TRACE_HOST:-127.0.0.1}"
TRACE_PORT="${TRACE_PORT:-18990}"
MIN_FREE_MB="${MIN_FREE_MB:-20000}"
KEEP_SERVICES="${KEEP_SERVICES:-0}"
REMOVE_VOLUMES="${REMOVE_VOLUMES:-0}"

VENV_DIR="${LAB_ROOT}/.venv"
RUN_DATA_DIR="${LAB_ROOT}/run-data/${RUN_ID}"
REPORT_BUNDLE_DIR="${LAB_ROOT}/report-bundles"
TRACE_DATA_DIR="${RUN_DATA_DIR}/tracedb"
TRACE_LOG="${RUN_DATA_DIR}/tracedb-engine.log"
COMPOSE_FILE="${LAB_ROOT}/docker-compose.yml"
PYTHON="${VENV_DIR}/bin/python"

free_mb() {
  df -Pm "${LAB_ROOT}" | awk 'NR == 2 {print $4}'
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

cleanup() {
  if [[ -n "${TRACE_PID:-}" ]] && kill -0 "${TRACE_PID}" >/dev/null 2>&1; then
    kill "${TRACE_PID}" >/dev/null 2>&1 || true
    wait "${TRACE_PID}" >/dev/null 2>&1 || true
  fi
  if [[ "${KEEP_SERVICES}" != "1" ]]; then
    if [[ "${REMOVE_VOLUMES}" == "1" ]]; then
      docker compose -f "${COMPOSE_FILE}" --profile lab down -v >/dev/null 2>&1 || true
    else
      docker compose -f "${COMPOSE_FILE}" --profile lab down >/dev/null 2>&1 || true
    fi
  fi
}
trap cleanup EXIT

main() {
  require_command docker
  require_command cargo
  require_command python3
  require_command curl
  require_command tar

  local available
  available="$(free_mb)"
  if [[ "${ALLOW_LOW_DISK:-0}" != "1" && "${available}" -lt "${MIN_FREE_MB}" ]]; then
    echo "refusing benchmark run: only ${available} MB free under ${LAB_ROOT}; need ${MIN_FREE_MB} MB" >&2
    echo "set ALLOW_LOW_DISK=1 only for tiny smoke runs" >&2
    exit 1
  fi

  mkdir -p "${RUN_DATA_DIR}" "${REPORT_BUNDLE_DIR}"

  if [[ ! -d "${VENV_DIR}" ]]; then
    python3 -m venv "${VENV_DIR}"
  fi
  "${PYTHON}" -m pip install --upgrade pip
  "${VENV_DIR}/bin/pip" install -r "${LAB_ROOT}/requirements.txt"

  if [[ "${OPENROUTER_MODE}" == "required" ]]; then
    set +u
    source "${LAB_ROOT}/.env.local" 2>/dev/null || true
    set -u
    if [[ -z "${OPENROUTER_API_KEY:-}" ]]; then
      echo "OPENROUTER_API_KEY is required; set it in the environment or benchmarks/realworld/.env.local" >&2
      exit 1
    fi
  fi

  cargo build --workspace --release

  docker compose -f "${COMPOSE_FILE}" --profile lab up -d \
    bench-postgres bench-pgvector bench-mongo bench-qdrant bench-opensearch

  "${REPO_ROOT}/target/release/tracedb" \
    --data "${TRACE_DATA_DIR}" \
    serve "${TRACE_HOST}:${TRACE_PORT}" >"${TRACE_LOG}" 2>&1 &
  TRACE_PID="$!"

  for _ in $(seq 1 120); do
    if curl -fsS "http://${TRACE_HOST}:${TRACE_PORT}/ready" >/dev/null 2>&1; then
      break
    fi
    sleep 0.5
  done
  curl -fsS "http://${TRACE_HOST}:${TRACE_PORT}/ready" >/dev/null

  (
    cd "${LAB_ROOT}"
    TRACEDB_HTTP_URL="http://${TRACE_HOST}:${TRACE_PORT}" \
    TRACEDB_CLI="${REPO_ROOT}/target/release/tracedb" \
    "${PYTHON}" -m runner suite \
      --profile "${PROFILE}" \
      --dataset "${DATASET}" \
      --records "${RECORDS}" \
      --target all \
      --surface sdk,cli,http,curl \
      --openrouter-mode "${OPENROUTER_MODE}" \
      --openrouter-cap "${OPENROUTER_CAP}" \
      --rerank-model "${RERANK_MODEL}" \
      --embedding-dimensions "${EMBEDDING_DIMENSIONS}" \
      --run-id "${RUN_ID}" \
      --reports-dir reports \
      --scenarios all
  )

  package_reports
}

package_reports() {
  local bundle="${REPORT_BUNDLE_DIR}/${RUN_ID}.tar.gz"
  local entries=()
  while IFS= read -r entry; do
    entries+=("${entry}")
  done < <(cd "${LAB_ROOT}/reports" && find . -maxdepth 1 -mindepth 1 -name "${RUN_ID}*" -type d | sed 's#^\./##' | sort)

  if [[ "${#entries[@]}" -eq 0 ]]; then
    echo "no reports found for ${RUN_ID}" >&2
    exit 1
  fi

  tar -czf "${bundle}" -C "${LAB_ROOT}/reports" "${entries[@]}"
  echo "suite report: ${LAB_ROOT}/reports/${RUN_ID}/suite.md"
  echo "report bundle: ${bundle}"
}

main "$@"
