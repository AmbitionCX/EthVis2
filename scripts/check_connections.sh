#!/usr/bin/env bash
# Read-only connectivity/authentication checks
set -euo pipefail
repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
if [[ ${1:-} == --help || $# -gt 1 ]]; then
  echo "Usage: $0 [path/to/.env]"
  echo "Checks Teku REST and SELECT 1 on PostgreSQL/ClickHouse (10s per service)."
  exit 0
fi
env_file=${1:-"$repo_dir/.env"}
if [[ ! -f $env_file ]]; then
  echo "Environment file not found: $env_file" >&2
  exit 1
fi
# Preserve the caller's relative .env path before changing directories.
env_file=$(cd -- "$(dirname -- "$env_file")" && pwd)/$(basename -- "$env_file")
if ! command -v go >/dev/null 2>&1 && [[ -f /home/chen/.g/env ]]; then
  source /home/chen/.g/env
fi
if ! command -v go >/dev/null 2>&1; then
  echo 'Go 1.27.3 is required; load your Go environment first.' >&2
  exit 1
fi
cd -- "$repo_dir"
# Reuse the collector's .env parser and DB driver; never source credentials as shell code.
exec go run ./cmd/ethvis-collector check-connections --env "$env_file"
