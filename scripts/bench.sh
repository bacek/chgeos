#!/usr/bin/env bash
# Benchmark suite for chgeos WASM UDFs.
#
# Usage:
#   ./scripts/bench.sh [path/to/clickhouse] [path/to/parquet]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

CH="${1:-$(command -v clickhouse 2>/dev/null)}"
[[ -x "${CH}" ]] || { echo "ERROR: clickhouse binary not found; pass as first argument or put on PATH"; exit 1; }

PARQUET="${2:?ERROR: parquet file required as second argument}"
PORT="${CH_PORT:-19000}"
FUEL="SETTINGS webassembly_udf_max_fuel=100000000000"

# Register bench-only functions (not in create.sql)
"${CH}" client --port "${PORT}" -q "
  CREATE OR REPLACE FUNCTION geos_bench_noop
    LANGUAGE WASM FROM 'chgeos' ARGUMENTS (a String) RETURNS UInt8 ABI BUFFERED_V1 DETERMINISTIC;
  CREATE OR REPLACE FUNCTION geos_bench_noop_rb
    LANGUAGE WASM FROM 'chgeos' ARGUMENTS (a String) RETURNS UInt8 ABI BUFFERED_V1 DETERMINISTIC;
  CREATE OR REPLACE FUNCTION geos_bench_wkb_parse
    LANGUAGE WASM FROM 'chgeos' ARGUMENTS (a String) RETURNS UInt8 ABI BUFFERED_V1 DETERMINISTIC;
  CREATE OR REPLACE FUNCTION geos_bench_envelope
    LANGUAGE WASM FROM 'chgeos' ARGUMENTS (a String) RETURNS UInt8 ABI BUFFERED_V1 DETERMINISTIC;
  CREATE OR REPLACE FUNCTION geos_bench_intersects_nobbox
    LANGUAGE WASM FROM 'chgeos' ARGUMENTS (a String, b String) RETURNS UInt8 ABI BUFFERED_V1 DETERMINISTIC;
" 2>/dev/null || true

RUNS="${BENCH_RUNS:-10}"

# Returns elapsed milliseconds for one query execution.
run_once() {
    local query="$1"
    result=$( { time "${CH}" client --port "${PORT}" -q "${query}" 2>/dev/null; } 2>&1 )
    echo "${result}" | grep real | awk '{
        split($2, t, /m|s/); print int((t[1]*60 + t[2])*1000+0.5)
    }'
}

run() {
    local label="$1"; local query="$2"
    local rows
    rows=$("${CH}" client --port "${PORT}" -q "${query/ ${FUEL}/}" 2>/dev/null | head -1)

    local times=() sum=0 min=999999 max=0
    for (( i=0; i<RUNS; i++ )); do
        local ms
        ms=$(run_once "${query}")
        times+=("${ms}")
        sum=$(( sum + ms ))
        (( ms < min )) && min=${ms}
        (( ms > max )) && max=${ms}
    done
    local avg=$(( sum / RUNS ))

    printf "%-52s rows=%-10s min=%4dms avg=%4dms max=%4dms\n" \
        "${label}" "${rows}" "${min}" "${avg}" "${max}"
}

echo ""
echo "Parquet: ${PARQUET}  (${RUNS} runs each)"
total=$("${CH}" client --port "${PORT}" -q "SELECT count() FROM file('${PARQUET}', Parquet, 'location String')" 2>/dev/null || echo '?')
echo "${total} rows"
echo ""
echo "Benchmark                                            Rows       min      avg      max"
echo "──────────────────────────────────────────────────────────────────────────────────────"

run "geos_bench_noop (MsgPack + WASM overhead)" \
    "SELECT count() FROM file('${PARQUET}', Parquet, 'location String') WHERE geos_bench_noop(location) ${FUEL}"

run "geos_bench_noop_rb (CH native Geometry → wkb())" \
    "SELECT count() FROM file('${PARQUET}', Parquet) WHERE geos_bench_noop_rb(wkb(location)) ${FUEL}"

run "geos_bench_wkb_parse (EWKB → GEOS parse)" \
    "SELECT count() FROM file('${PARQUET}', Parquet, 'location String') WHERE geos_bench_wkb_parse(location) ${FUEL}"

run "geos_bench_envelope (fast WKB bbox, no GEOS)" \
    "SELECT count() FROM file('${PARQUET}', Parquet, 'location String') WHERE geos_bench_envelope(location) ${FUEL}"

run "st_intersects_extent (bbox shortcut predicate)" \
    "SELECT count() FROM file('${PARQUET}', Parquet, 'location String') WHERE st_intersects_extent(location, st_geomfromtext('POLYGON ((13.0 52.3, 13.6 52.3, 13.6 52.7, 13.0 52.7, 13.0 52.3))')) ${FUEL}"

run "st_intersects (full GEOS, bbox shortcut active)" \
    "SELECT count() FROM file('${PARQUET}', Parquet, 'location String') WHERE st_intersects(location, st_geomfromtext('POLYGON ((13.0 52.3, 13.6 52.3, 13.6 52.7, 13.0 52.7, 13.0 52.3))')) ${FUEL}"

run "geos_bench_intersects_nobbox (full GEOS, no bbox)" \
    "SELECT count() FROM file('${PARQUET}', Parquet, 'location String') WHERE geos_bench_intersects_nobbox(location, st_geomfromtext('POLYGON ((13.0 52.3, 13.6 52.3, 13.6 52.7, 13.0 52.7, 13.0 52.3))')) ${FUEL}"

echo ""
