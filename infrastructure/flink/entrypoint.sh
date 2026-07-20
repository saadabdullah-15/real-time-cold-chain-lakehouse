#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" == "standalone-job" && -n "${FLINK_RESTORE_PATH:-}" ]]; then
  set -- "$@" --fromSavepoint "${FLINK_RESTORE_PATH}"
fi

exec /docker-entrypoint.sh "$@"
