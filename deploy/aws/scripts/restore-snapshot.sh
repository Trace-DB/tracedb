#!/bin/sh
set -eu

usage() {
  echo "usage: $0 <snapshot-tar.gz> <empty-target-data-dir>" >&2
  echo "Restores a local snapshot into an empty target directory. Stop TraceDB first." >&2
}

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
  usage
  exit 0
fi

if [ $# -ne 2 ]; then
  usage
  exit 64
fi

archive=$1
target_dir=$2
checksum_file="${archive}.sha256"

if [ ! -f "$archive" ]; then
  echo "snapshot archive does not exist: $archive" >&2
  exit 1
fi

if ! command -v tar >/dev/null 2>&1; then
  echo "tar is required" >&2
  exit 127
fi

if [ -e "$target_dir" ] && [ ! -d "$target_dir" ]; then
  echo "target exists but is not a directory: $target_dir" >&2
  exit 1
fi

mkdir -p "$target_dir"

if find "$target_dir" -mindepth 1 -maxdepth 1 | grep . >/dev/null 2>&1; then
  echo "target directory is not empty: $target_dir" >&2
  exit 1
fi

if [ -f "$checksum_file" ]; then
  if command -v sha256sum >/dev/null 2>&1; then
    archive_dir=$(dirname "$archive")
    checksum_base=$(basename "$checksum_file")
    (cd "$archive_dir" && sha256sum -c "$checksum_base")
  elif command -v shasum >/dev/null 2>&1; then
    expected=$(awk '{print $1}' "$checksum_file")
    actual=$(shasum -a 256 "$archive" | awk '{print $1}')
    if [ "$expected" != "$actual" ]; then
      echo "checksum mismatch for $archive" >&2
      exit 1
    fi
    printf '%s: OK\n' "$archive"
  else
    echo "checksum file exists but sha256sum or shasum is not available" >&2
    exit 127
  fi
else
  echo "warning: checksum file not found: $checksum_file" >&2
fi

# Extract archive contents into the empty target directory.
tar -C "$target_dir" -xzf "$archive"

printf 'restored %s into %s\n' "$archive" "$target_dir"
