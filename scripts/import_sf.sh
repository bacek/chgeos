#!/usr/bin/env bash
# Import SF parquet files into native ClickHouse MergeTree tables.
#
# Usage:
#   ./scripts/import_sf.sh [path/to/clickhouse] [sf1|sf10] [--replace]
#
# Creates database sf1 or sf10 if it does not exist, then materializes each
# parquet file as a MergeTree table. Skips tables that already exist unless
# --replace is given, which drops and recreates them.
#
# Run scripts/link_bench_data.sh first to set up the user_files symlinks.

set -euo pipefail

CH="${1:-$(command -v clickhouse 2>/dev/null)}"
[[ -x "${CH}" ]] || { echo "ERROR: clickhouse binary not found; pass as first argument or put on PATH"; exit 1; }

SF="${2:-sf1}"
[[ "${SF}" == "sf1" || "${SF}" == "sf10" ]] || { echo "ERROR: scale factor must be sf1 or sf10, got '${SF}'"; exit 1; }

REPLACE=0
for arg in "${@:3}"; do
    [[ "${arg}" == "--replace" ]] && REPLACE=1
done

PORT="${CH_PORT:-19000}"

q() { "${CH}" client --port "${PORT}" -q "$1"; }

# Parquet files are sorted by their surrogate key column; match that in
# MergeTree so the physical layout is preserved after import.
declare -A ORDER_BY=(
    [trip]="t_tripkey"
    [zone]="z_zonekey"
    [building]="b_buildingkey"
    [customer]="c_custkey"
    [driver]="d_driverkey"
    [vehicle]="v_vehiclekey"
)

echo "Importing ${SF} into ClickHouse database '${SF}' ..."

q "CREATE DATABASE IF NOT EXISTS ${SF}"

for table in trip zone building customer driver vehicle; do
    key="${ORDER_BY[${table}]}"
    parquet="${SF}/${table}.parquet"

    exists=$(q "SELECT count() FROM system.tables WHERE database='${SF}' AND name='${table}'" 2>/dev/null)

    if [[ "${exists}" == "1" ]]; then
        if (( REPLACE )); then
            echo "  ${table}: dropping (--replace)"
            q "DROP TABLE ${SF}.${table}"
        else
            echo "  ${table}: already exists — skipping (use --replace to overwrite)"
            continue
        fi
    fi

    echo "  ${table}: importing from ${parquet} ..."
    q "CREATE TABLE ${SF}.${table}
       ENGINE = MergeTree()
       ORDER BY ${key}
       AS SELECT * FROM file('${parquet}', Parquet)"
    rows=$(q "SELECT count() FROM ${SF}.${table}")
    echo "  ${table}: ${rows} rows"
done

echo "Done."
