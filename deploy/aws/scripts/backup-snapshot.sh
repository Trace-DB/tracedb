#!/bin/sh
set -eu

usage() {
  echo "usage: $0 <target-backup-dir> [source-data-dir]" >&2
  echo "default source-data-dir: /data/tracedb" >&2
  echo "Creates a local tarball and checksum for ops evidence; this is not managed DR." >&2
}

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
  usage
  exit 0
fi

if [ $# -lt 1 ] || [ $# -gt 2 ]; then
  usage
  exit 64
fi

backup_dir=$1
source_dir=${2:-/data/tracedb}

if [ ! -d "$source_dir" ]; then
  echo "source data directory does not exist: $source_dir" >&2
  exit 1
fi

if ! command -v tar >/dev/null 2>&1; then
  echo "tar is required" >&2
  exit 127
fi

if command -v sha256sum >/dev/null 2>&1; then
  checksum_tool=sha256sum
elif command -v shasum >/dev/null 2>&1; then
  checksum_tool=shasum
else
  echo "sha256sum or shasum is required" >&2
  exit 127
fi

mkdir -p "$backup_dir"

timestamp=$(date -u '+%Y%m%dT%H%M%SZ')
archive_base="tracedb-data-${timestamp}.tar.gz"
archive="${backup_dir}/${archive_base}"
tmp_archive="${archive}.$$"

cleanup() {
  rm -f "$tmp_archive"
}
trap cleanup EXIT HUP INT TERM

# Archive the contents of the data directory so restore targets can be the empty
# /data/tracedb directory itself.
tar -C "$source_dir" -czf "$tmp_archive" .
mv "$tmp_archive" "$archive"

if [ "$checksum_tool" = "sha256sum" ]; then
  (cd "$backup_dir" && sha256sum "$archive_base") > "${archive}.sha256"
else
  (cd "$backup_dir" && shasum -a 256 "$archive_base") > "${archive}.sha256"
fi

printf 'created %s\n' "$archive"
printf 'created %s\n' "${archive}.sha256"
