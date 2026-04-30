#!/usr/bin/env bash
# Link SF1 and SF10 parquet files into ClickHouse user_files so that
# bench_sf.sh can reference them as file('sf1/trip.parquet', Parquet) etc.
#
# Usage:
#   ./scripts/link_bench_data.sh [sf1-dir] [sf10-dir]
#
# Defaults:
#   sf1-dir  = ../spatial-bench/sf1
#   sf10-dir = ../spatial-bench/sf10

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

SF1_SRC="${1:-${REPO_ROOT}/../spatial-bench/sf1}"
SF10_SRC="${2:-${REPO_ROOT}/../spatial-bench/sf10}"

USER_FILES="${REPO_ROOT}/tmp/data/user_files"

link_dataset() {
    local label="$1"
    local src="$2"
    local dst="${USER_FILES}/${label}"

    if [[ ! -d "${src}" ]]; then
        echo "WARNING: ${label} source dir '${src}' not found — skipping"
        return
    fi

    mkdir -p "${dst}"

    local linked=0
    for f in "${src}"/*.parquet; do
        [[ -e "${f}" ]] || continue
        ln -sf "${f}" "${dst}/$(basename "${f}")"
        (( linked++ )) || true
    done
    echo "Linked ${linked} parquet file(s): ${src} -> ${dst}"
}

link_dataset sf1  "${SF1_SRC}"
link_dataset sf10 "${SF10_SRC}"
