#!/usr/bin/env bash
# Quick Q11 timing comparison: fuse off vs fuse on.
set -euo pipefail

CH="${1:-../ClickHouse/build/programs/clickhouse}"
PORT="${CH_PORT:-19000}"
RUNS="${BENCH_RUNS:-5}"
TIMEOUT="${BENCH_TIMEOUT:-180}"
FUSE="${FUSE:-0}"

ZONE="file('zone.parquet', Parquet)"
TRIP="file('trip.parquet', Parquet)"

QUERY="SELECT count() AS cross_zone_trip_count
FROM ${ZONE} pickup_zone
JOIN ${TRIP} t            ON st_within(t.t_pickuploc,  pickup_zone.z_boundary)
JOIN ${ZONE} dropoff_zone ON st_within(t.t_dropoffloc, dropoff_zone.z_boundary)
WHERE pickup_zone.z_zonekey != dropoff_zone.z_zonekey
SETTINGS webassembly_udf_max_fuel=0,
         max_execution_time=${TIMEOUT},
         query_plan_fuse_spatial_joins=${FUSE}"

echo "=== Q11 fuse=${FUSE} ==="
times=()
rows="?"
for ((i=0; i<RUNS; i++)); do
    tmpf=$(mktemp); tmpe=$(mktemp); rc=0
    "${CH}" client --port "${PORT}" --time -q "${QUERY}" >"${tmpf}" 2>"${tmpe}" || rc=$?
    secs=$(grep -E '^[0-9]+(\.[0-9]+)?$' "${tmpe}" | tail -1)
    [[ -z "${secs}" ]] && secs="${TIMEOUT}"
    ms=$(awk "BEGIN {printf \"%d\", ${secs} * 1000 + 0.5}")
    if (( i == 0 )); then
        rows=$(tr -d '[:space:]' < "${tmpf}")
    fi
    if [[ ${rc} -ne 0 ]]; then
        echo "  run $((i+1)): ERROR (${ms}ms)"
        cat "${tmpe}" | head -3
    else
        echo "  run $((i+1)): ${ms}ms (rows=${rows})"
        times+=("${ms}")
    fi
    rm -f "${tmpf}" "${tmpe}"
done

if (( ${#times[@]} > 0 )); then
    sum=0; min=999999999; max=0
    for t in "${times[@]}"; do
        (( sum += t ))
        (( t < min )) && min=$t
        (( t > max )) && max=$t
    done
    avg=$(( sum / ${#times[@]} ))
    echo "  -> min=${min}ms  avg=${avg}ms  max=${max}ms  rows=${rows}  (n=${#times[@]})"
fi
