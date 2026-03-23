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

run() {
    local label="$1"; local query="$2"
    printf "%-52s " "${label}"
    result=$( { time "${CH}" client --port "${PORT}" -q "${query}" 2>/dev/null; } 2>&1 )
    rows=$(echo "${result}" | head -1)
    elapsed=$(echo "${result}" | grep real | awk '{print $2}')
    printf "rows=%-10s %s\n" "${rows}" "${elapsed}"
}

echo ""
echo "Parquet: ${PARQUET}"
total=$("${CH}" client --port "${PORT}" -q "SELECT count() FROM file('${PARQUET}', Parquet, 'location String')" 2>/dev/null || echo '?')
echo "${total} rows"
echo ""
echo "Benchmark                                            Rows       Time"
echo "─────────────────────────────────────────────────────────────────────"

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

echo ""
