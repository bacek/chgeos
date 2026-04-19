#!/usr/bin/env bash
# End-to-end tests for chgeos WASM UDFs via clickhouse-local.
#
# Usage:
#   ./clickhouse/test_e2e.sh [path/to/clickhouse] [path/to/chgeos.wasm]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ── Locate binaries ───────────────────────────────────────────────────────────

CLICKHOUSE_BIN="${1:-}"
if [[ -z "${CLICKHOUSE_BIN}" ]]; then
    for candidate in \
        "${REPO_ROOT}/../ClickHouse/build/programs/clickhouse" \
        "/data/bacek/src/ClickHouse/build/programs/clickhouse" \
        "$(command -v clickhouse 2>/dev/null || true)"; do
        [[ -x "${candidate}" ]] && { CLICKHOUSE_BIN="${candidate}"; break; }
    done
fi
[[ -x "${CLICKHOUSE_BIN}" ]] || { echo "ERROR: clickhouse binary not found"; exit 1; }

WASM_BIN="${2:-${REPO_ROOT}/build_wasm/chgeos.wasm}"
[[ -f "${WASM_BIN}" ]] || { echo "ERROR: WASM binary not found: ${WASM_BIN}"; exit 1; }

CONFIG="${SCRIPT_DIR}/config-test.xml"
CREATE_SQL="${SCRIPT_DIR}/create.sql"

# ── Temporary data directory ───────────────────────────────────────────────────

DATA_DIR="$(mktemp -d /tmp/chgeos-e2e-XXXXXX)"
trap 'rm -rf "${DATA_DIR}"' EXIT

mkdir -p "${DATA_DIR}/user_scripts/wasm" "${DATA_DIR}/user_defined"
cp "${WASM_BIN}" "${DATA_DIR}/user_scripts/wasm/chgeos.wasm"

CH() {
    "${CLICKHOUSE_BIN}" local \
        --config-file "${CONFIG}" \
        --path "${DATA_DIR}" \
        "$@"
}

# ── Build single multi-query test run ─────────────────────────────────────────
# create.sql + all test queries run in one clickhouse-local invocation.

declare -a NAMES=()
declare -a QUERIES=()
declare -a EXPECTED=()

t() { NAMES+=("$1"); QUERIES+=("$2"); EXPECTED+=("$3"); }

# Version / diagnostics
t "geos_version"       "SELECT geos_version() LIKE '3.%'"         "1"

# Geometry constructors / I/O
t "st_makepoint"       "SELECT st_astext(st_makepoint(1.0, 2.0))"  "POINT (1 2)"
t "st_geomfromtext"    "SELECT st_astext(st_geomfromtext('POINT (3 4)'))"  "POINT (3 4)"
t "st_asewkt_plain"    "SELECT st_asewkt(st_geomfromtext('POINT (0 0)'))"  "POINT (0 0)"

# Accessors
t "st_x"               "SELECT st_x(st_makepoint(1.0, 2.0))"       "1"
t "st_y"               "SELECT st_y(st_makepoint(1.0, 2.0))"       "2"
t "st_srid_default"    "SELECT st_srid(st_geomfromtext('POINT (0 0)'))"  "0"

# Measurements
t "st_area_1"          "SELECT st_area(st_geomfromtext('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'))"  "1"
t "st_area_6"          "SELECT st_area(st_geomfromtext('POLYGON ((0 0, 2 0, 2 3, 0 3, 0 0))'))"  "6"
t "st_distance"        "SELECT st_distance(st_makepoint(0.0, 0.0), st_makepoint(3.0, 4.0))"  "5"
t "st_length"          "SELECT st_length(st_geomfromtext('LINESTRING (0 0, 3 4)'))"  "5"
t "st_perimeter"       "SELECT st_perimeter(st_geomfromtext('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'))"  "4"

# Predicates
t "st_equals_same"     "SELECT st_equals(st_geomfromtext('POINT (1 2)'), st_geomfromtext('POINT (1 2)'))"  "1"
t "st_equals_diff"     "SELECT st_equals(st_geomfromtext('POINT (1 2)'), st_geomfromtext('POINT (2 1)'))"  "0"
t "st_intersects_yes"  "SELECT st_intersects(st_geomfromtext('LINESTRING (0 0, 2 2)'), st_geomfromtext('LINESTRING (0 2, 2 0)'))"  "1"
t "st_intersects_no"   "SELECT st_intersects(st_geomfromtext('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'), st_geomfromtext('POLYGON ((5 5, 6 5, 6 6, 5 6, 5 5))'))"  "0"
t "st_contains_yes"    "SELECT st_contains(st_geomfromtext('POLYGON ((0 0, 3 0, 3 3, 0 3, 0 0))'), st_geomfromtext('POINT (1 1)'))"  "1"
t "st_contains_no"     "SELECT st_contains(st_geomfromtext('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'), st_geomfromtext('POINT (2 2)'))"  "0"
t "st_within"          "SELECT st_within(st_geomfromtext('POINT (1 1)'), st_geomfromtext('POLYGON ((0 0, 3 0, 3 3, 0 3, 0 0))'))"  "1"
t "st_disjoint"        "SELECT st_disjoint(st_geomfromtext('POINT (0 0)'), st_geomfromtext('POINT (1 1)'))"  "1"
t "st_touches"         "SELECT st_touches(st_geomfromtext('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'), st_geomfromtext('POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0))'))"  "1"
t "st_crosses"         "SELECT st_crosses(st_geomfromtext('LINESTRING (0 0, 2 2)'), st_geomfromtext('LINESTRING (0 2, 2 0)'))"  "1"
t "st_overlaps_no"     "SELECT st_overlaps(st_geomfromtext('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'), st_geomfromtext('POLYGON ((2 2, 3 2, 3 3, 2 3, 2 2))'))"  "0"
t "st_isvalid_yes"     "SELECT st_isvalid(st_geomfromtext('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'))"  "1"
t "st_isvalid_no"      "SELECT st_isvalid(st_geomfromtext('POLYGON ((0 0, 2 2, 2 0, 0 2, 0 0))'))"  "0"
t "st_isempty"         "SELECT st_isempty(st_geomfromtext('GEOMETRYCOLLECTION EMPTY'))"  "1"

# Transforms
t "st_buffer_area"     "SELECT round(st_area(st_buffer(st_makepoint(0.0, 0.0), 1.0)), 1)"  "3.1"
t "st_centroid"        "SELECT st_astext(st_centroid(st_geomfromtext('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))')))"  "POINT (1 1)"
t "st_envelope"        "SELECT st_astext(st_envelope(st_geomfromtext('LINESTRING (0 0, 2 3)')))"  "POLYGON ((0 0, 2 0, 2 3, 0 3, 0 0))"
t "st_convexhull"      "SELECT round(st_area(st_convexhull(st_geomfromtext('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))'))), 1)"  "4"
t "st_reverse"         "SELECT st_astext(st_reverse(st_geomfromtext('LINESTRING (0 0, 1 1, 2 0)')))"  "LINESTRING (2 0, 1 1, 0 0)"
t "st_simplify"        "SELECT st_astext(st_simplify(st_geomfromtext('LINESTRING (0 0, 0.5 0.1, 1 0)'), 1.0))"  "LINESTRING (0 0, 1 0)"

# Overlay
t "st_intersection"    "SELECT round(st_area(st_intersection(st_geomfromtext('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))'), st_geomfromtext('POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))'))))"  "1"
t "st_union_area"      "SELECT round(st_area(st_union(st_geomfromtext('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'), st_geomfromtext('POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0))'))))"  "2"
t "st_difference"      "SELECT round(st_area(st_difference(st_geomfromtext('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))'), st_geomfromtext('POLYGON ((1 0, 2 0, 2 2, 1 2, 1 0))'))))"  "2"
t "st_symdiff_area"    "SELECT round(st_area(st_symdifference(st_geomfromtext('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))'), st_geomfromtext('POLYGON ((1 0, 3 0, 3 2, 1 2, 1 0))'))))"  "4"

# Aggregate — functions take Array(String); use groupArray() to collect rows
t "st_union_agg"        "SELECT round(st_area(st_union_agg(groupArray(geom)))) FROM (SELECT st_geomfromtext('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))') AS geom UNION ALL SELECT st_geomfromtext('POLYGON ((5 5, 6 5, 6 6, 5 6, 5 5))'))"  "2"
t "st_collect_agg"      "SELECT st_numgeometries(st_collect_agg(groupArray(geom))) FROM (SELECT st_geomfromtext('POINT (0 0)') AS geom UNION ALL SELECT st_geomfromtext('POINT (1 1)'))"  "2"
t "st_collect_agg_nodissolve" "SELECT round(st_area(st_collect_agg(groupArray(geom)))) FROM (SELECT st_geomfromtext('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))') AS geom UNION ALL SELECT st_geomfromtext('POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))'))"  "8"
t "st_extent_agg"       "SELECT round(st_area(st_extent_agg(groupArray(geom)))) FROM (SELECT st_geomfromtext('POINT (0 0)') AS geom UNION ALL SELECT st_geomfromtext('POINT (3 4)'))"  "12"
t "st_extent_agg_polys" "SELECT round(st_area(st_extent_agg(groupArray(geom)))) FROM (SELECT st_geomfromtext('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))') AS geom UNION ALL SELECT st_geomfromtext('POLYGON ((3 0, 5 0, 5 2, 3 2, 3 0))'))"  "10"
t "st_makeline_agg"     "SELECT st_astext(st_makeline_agg(groupArray(geom))) FROM (SELECT st_geomfromtext('POINT (0 0)') AS geom UNION ALL SELECT st_geomfromtext('POINT (1 0)') UNION ALL SELECT st_geomfromtext('POINT (1 1)'))"  "LINESTRING (0 0, 1 0, 1 1)"
t "st_convexhull_agg"   "SELECT round(st_area(st_convexhull_agg(groupArray(geom))), 1) FROM (SELECT st_geomfromtext('POINT (0 0)') AS geom UNION ALL SELECT st_geomfromtext('POINT (1 0)') UNION ALL SELECT st_geomfromtext('POINT (0 1)'))"  "0.5"

# COLUMNAR_V1 aggregate functions (COL_COMPLEX Array(String) input)
# The canonical names (st_union_agg etc.) now route through COLUMNAR_V1.
# Cross-check: COLUMNAR_V1 and _mp paths must agree.
t "col_vs_mp_union_agg" \
    "SELECT st_astext(st_union_agg(groupArray(wkb))) = st_astext(st_union_agg_mp(groupArray(wkb))) FROM (SELECT st_geomfromtext('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))') AS wkb UNION ALL SELECT st_geomfromtext('POLYGON ((2 2, 3 2, 3 3, 2 3, 2 2))'))" \
    "1"
t "col_extent_agg_bbox" \
    "SELECT st_astext(st_extent_agg(groupArray(wkb))) FROM (SELECT st_geomfromtext('POINT (1 2)') AS wkb UNION ALL SELECT st_geomfromtext('POINT (4 6)') UNION ALL SELECT st_geomfromtext('POINT (-1 0)'))" \
    "POLYGON ((-1 0, 4 0, 4 6, -1 6, -1 0))"
t "col_collect_agg_types" \
    "SELECT st_astext(st_collect_agg(groupArray(wkb))) FROM (SELECT st_geomfromtext('POINT (0 0)') AS wkb UNION ALL SELECT st_geomfromtext('POINT (1 1)'))" \
    "GEOMETRYCOLLECTION (POINT (0 0), POINT (1 1))"
t "col_union_agg_single" \
    "SELECT st_astext(st_union_agg(groupArray(wkb))) FROM (SELECT st_geomfromtext('POINT (3 7)') AS wkb)" \
    "POINT (3 7)"

# Multi-row batch
t "batch_area_sum"     "SELECT sum(st_area(st_geomfromtext(wkt))) FROM (SELECT 'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))' AS wkt UNION ALL SELECT 'POLYGON ((0 0, 2 0, 2 3, 0 3, 0 0))')"  "7"

# SRID round-trip — exercises the EWKB SRID strip/inject code path
t "st_setsrid_srid"    "SELECT st_srid(st_setsrid(st_makepoint(1.0, 2.0), 4326))"  "4326"
t "st_setsrid_ewkt"    "SELECT st_asewkt(st_setsrid(st_makepoint(1.0, 2.0), 4326))"  "SRID=4326;POINT (1 2)"
t "st_geomfromtext_srid" "SELECT st_asewkt(st_geomfromtext('SRID=4326;POINT (1 2)'))"  "SRID=4326;POINT (1 2)"
t "st_asewkt_no_srid"  "SELECT st_asewkt(st_makepoint(1.0, 2.0))"  "POINT (1 2)"

# I/O
t "st_geomfromwkb"     "SELECT st_astext(st_geomfromwkb(st_geomfromtext('POINT (1 2)')))"  "POINT (1 2)"
t "st_geomfromgeojson" "SELECT st_astext(st_geomfromgeojson('{\"type\":\"Point\",\"coordinates\":[1,2]}'))"  "POINT (1 2)"

# Accessors
t "st_npoints"         "SELECT st_npoints(st_geomfromtext('LINESTRING (0 0, 1 1, 2 0)'))"  "3"
t "st_numgeometries"   "SELECT st_numgeometries(st_geomfromtext('GEOMETRYCOLLECTION (POINT (0 0), POINT (1 1))'))"  "2"
t "st_geometrytype_pt" "SELECT st_geometrytype(st_makepoint(0.0, 0.0))"  "ST_Point"
t "st_geometrytype_ls" "SELECT st_geometrytype(st_geomfromtext('LINESTRING (0 0, 1 1)'))"  "ST_LineString"
t "st_dimension_pt"    "SELECT st_dimension(st_makepoint(0.0, 0.0))"  "0"
t "st_dimension_poly"  "SELECT st_dimension(st_geomfromtext('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'))"  "2"
t "st_isring_yes"      "SELECT st_isring(st_geomfromtext('LINESTRING (0 0, 1 0, 1 1, 0 1, 0 0)'))"  "1"
t "st_isring_no"       "SELECT st_isring(st_geomfromtext('LINESTRING (0 0, 1 0, 1 1)'))"  "0"
t "st_issimple"        "SELECT st_issimple(st_makepoint(0.0, 0.0))"  "1"
t "st_isvalidreason"   "SELECT st_isvalidreason(st_geomfromtext('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'))"  "Valid Geometry"

# Linestring point accessors (regression for M8 empty-guard fix)
t "st_startpoint"      "SELECT st_astext(st_startpoint(st_geomfromtext('LINESTRING (0 0, 1 1, 2 0)')))"  "POINT (0 0)"
t "st_endpoint"        "SELECT st_astext(st_endpoint(st_geomfromtext('LINESTRING (0 0, 1 1, 2 0)')))"  "POINT (2 0)"
t "st_pointn"          "SELECT st_astext(st_pointn(st_geomfromtext('LINESTRING (0 0, 1 1, 2 0)'), 2))"  "POINT (1 1)"

# Predicates
t "st_dwithin_yes"     "SELECT st_dwithin(st_geomfromtext('POINT (0 0)'), st_geomfromtext('POINT (3 4)'), 5.0)"  "1"
t "st_dwithin_no"      "SELECT st_dwithin(st_geomfromtext('POINT (0 0)'), st_geomfromtext('POINT (3 4)'), 4.9)"  "0"
t "st_intersects_ext_yes" "SELECT st_intersects_extent(st_geomfromtext('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))'), st_geomfromtext('POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))'))"  "1"
t "st_intersects_ext_no"  "SELECT st_intersects_extent(st_geomfromtext('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'), st_geomfromtext('POLYGON ((5 5, 6 5, 6 6, 5 6, 5 5))'))"  "0"
t "st_intersects_ext_rb_yes" "SELECT st_intersects_extent_rb(st_geomfromtext('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))'), st_geomfromtext('POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))'))"  "1"
t "st_intersects_ext_rb_no"  "SELECT st_intersects_extent_rb(st_geomfromtext('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'), st_geomfromtext('POLYGON ((5 5, 6 5, 6 6, 5 6, 5 5))'))"  "0"
t "st_intersects_ext_rb_touch" "SELECT st_intersects_extent_rb(st_geomfromtext('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'), st_geomfromtext('POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0))'))"  "1"
t "st_covers_interior" "SELECT st_covers(st_geomfromtext('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'), st_geomfromtext('POINT (5 5)'))"  "1"
t "st_covers_boundary" "SELECT st_covers(st_geomfromtext('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'), st_geomfromtext('POINT (5 0)'))"  "1"
t "st_coveredby"       "SELECT st_coveredby(st_geomfromtext('POINT (5 0)'), st_geomfromtext('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'))"  "1"
t "st_relate"          "SELECT st_relate(st_geomfromtext('POINT (0 0)'), st_geomfromtext('POINT (0 0)'))"  "0FFFFFFF2"

# Distance metrics
t "st_hausdorff"       "SELECT st_hausdorffdistance(st_geomfromtext('LINESTRING (0 0, 2 0)'), st_geomfromtext('LINESTRING (0 1, 2 1)'))"  "1"

# Geometry constructors
t "st_makeline"        "SELECT st_astext(st_makeline(st_makepoint(0.0, 0.0), st_makepoint(1.0, 1.0)))"  "LINESTRING (0 0, 1 1)"
t "st_makepolygon"     "SELECT st_geometrytype(st_makepolygon(st_geomfromtext('LINESTRING (0 0, 1 0, 1 1, 0 1, 0 0)')))"  "ST_Polygon"
t "st_collect"         "SELECT st_numgeometries(st_collect(st_makepoint(0.0, 0.0), st_makepoint(1.0, 1.0)))"  "2"

# Transforms
t "st_translate"       "SELECT st_astext(st_translate(st_makepoint(1.0, 2.0), 3.0, 4.0))"  "POINT (4 6)"
t "st_scale"           "SELECT st_astext(st_scale(st_makepoint(2.0, 3.0), 2.0, 3.0))"  "POINT (4 9)"
t "st_boundary"        "SELECT st_astext(st_boundary(st_geomfromtext('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))')))"  "LINESTRING (0 0, 1 0, 1 1, 0 1, 0 0)"
t "st_exteriorring"    "SELECT st_astext(st_exteriorring(st_geomfromtext('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))')))"  "LINESTRING (0 0, 1 0, 1 1, 0 1, 0 0)"
t "st_interiorpoint"   "SELECT st_within(st_interiorpoint(st_geomfromtext('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))')), st_geomfromtext('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'))"  "1"

# Processing
t "st_makevalid"       "SELECT st_isvalid(st_makevalid(st_geomfromtext('POLYGON ((0 0, 2 2, 2 0, 0 2, 0 0))')))"  "1"
t "st_subdivide"       "SELECT st_numgeometries(st_subdivide(st_geomfromtext('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))'), 5))"  "1"
t "st_unaryunion"      "SELECT round(st_area(st_unaryunion(st_geomfromtext('GEOMETRYCOLLECTION (POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0)), POLYGON ((2 0, 3 0, 3 1, 2 1, 2 0)))'))))"  "2"

# Clustering (regression for O(n²) → STRtree rewrite)
t "st_cluster_disjoint"    "SELECT st_numgeometries(st_clusterintersecting(st_geomfromtext('GEOMETRYCOLLECTION (POINT (0 0), POINT (10 10))')))"  "2"
t "st_cluster_intersects"  "SELECT st_numgeometries(st_clusterintersecting(st_geomfromtext('GEOMETRYCOLLECTION (POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0)), POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1)))')))"  "1"

# ── Run create.sql + all queries in one clickhouse-local call ─────────────────

MULTIQUERY="$(cat "${CREATE_SQL}")"$'\n'
for i in "${!NAMES[@]}"; do
    MULTIQUERY+="${QUERIES[$i]};"$'\n'
done

RESULTS="$(CH --multiquery --query "${MULTIQUERY}")"

# ── Compare results ────────────────────────────────────────────────────────────

PASS=0
FAIL=0
i=0
while IFS= read -r actual; do
    name="${NAMES[$i]}"
    expected="${EXPECTED[$i]}"
    if [[ "${actual}" == "${expected}" ]]; then
        echo "PASS  ${name}"
        (( PASS++ )) || true
    else
        echo "FAIL  ${name}"
        echo "      expected: ${expected}"
        echo "      got:      ${actual}"
        (( FAIL++ )) || true
    fi
    (( i++ )) || true
done <<< "${RESULTS}"

echo ""
echo "Results: ${PASS} passed, ${FAIL} failed"
[[ "${FAIL}" -eq 0 ]] || exit 1
