#!/bin/sh
set -eu

usage() {
  echo "usage: $0 [base-url]" >&2
  echo "default base-url: http://127.0.0.1:18081" >&2
}

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
  usage
  exit 0
fi

base_url=${1:-http://127.0.0.1:18081}
base_url=${base_url%/}

if ! command -v curl >/dev/null 2>&1; then
  echo "curl is required" >&2
  exit 127
fi

check_endpoint() {
  path=$1
  url="${base_url}${path}"
  printf 'checking %s ... ' "$url"
  if curl -fsS --max-time 5 "$url" >/dev/null; then
    echo "ok"
  else
    echo "failed" >&2
    return 1
  fi
}

check_endpoint /v1/health
check_endpoint /v1/ready
