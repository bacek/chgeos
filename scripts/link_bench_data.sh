#!/usr/bin/env bash
# Link sedona-bench parquet files into ClickHouse user_files so that
# bench_sf.py can reference them as file('sf1/trip/*.parquet', Parquet) etc.
#
# Expected source layout — one directory per table, one or more files:
#   <sf-dir>/trip/trip.1.parquet
#   <sf-dir>/trip/trip.2.parquet
#   <sf-dir>/zone/zone.1.parquet
#   ...
#
# Files are symlinked into per-table dirs:
#   tmp/data/user_files/<sf>/<table>/<file>.parquet
#
# Usage:
#   ./scripts/link_bench_data.sh               # auto-discover: link every
#                                              # sf* dataset in the default root
#   ./scripts/link_bench_data.sh [sf-dir ...]  # explicit source dirs
#                                              # (label = basename of each dir)
#
# Default root for auto-discovery: ../sedona-bench/hf/v0.1.0
# (links sf1, sf10, sf100, ... — whatever exists there)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

ROOT="${REPO_ROOT}/../sedona-bench/hf/v0.1.0"

USER_FILES="${REPO_ROOT}/tmp/data/user_files"

link_dataset() {
    local src="$1"
    local label
    label="$(basename "${src}")"
    local dst="${USER_FILES}/${label}"

    if [[ ! -d "${src}" ]]; then
        echo "WARNING: source dir '${src}' not found — skipping"
        return
    fi

    local linked=0
    for table_dir in "${src}"/*/; do
        [[ -d "${table_dir}" ]] || continue
        local table
        table="$(basename "${table_dir}")"
        mkdir -p "${dst}/${table}"
        for f in "${table_dir}"*.parquet; do
            [[ -e "${f}" ]] || continue
            ln -sf "${f}" "${dst}/${table}/$(basename "${f}")"
            (( linked++ )) || true
        done
    done
    echo "Linked ${linked} parquet file(s): ${src} -> ${dst}"
}

if [[ $# -gt 0 ]]; then
    for d in "$@"; do
        link_dataset "$d"
    done
else
    for d in "${ROOT}"/sf*/; do
        [[ -d "${d}" ]] || continue
        link_dataset "$d"
    done
fi
