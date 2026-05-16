#!/usr/bin/env python3
"""End-to-end tests for chgeos WASM UDFs via clickhouse-local.

Usage:
    python3 clickhouse/test_e2e.py [path/to/clickhouse] [path/to/chgeos.wasm] [--wire-protocol col|mp|cb]

Environment:
    WIRE_PROTOCOL=col|mp|cb — wire format (default: col, bare names)
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory


# ---------------------------------------------------------------------------
# Parse create.sql to find registered function variants
# ---------------------------------------------------------------------------

def _parse_create_sql(sql_path: str) -> dict[str, list[str]]:
    """Return {bare_name: [variants]} from create.sql.

    Variants are the suffixes registered for each bare function name,
    e.g. {'st_x': ['', '_mp', '_col', '_cb'], 'st_knn': ['']}
    """
    text = Path(sql_path).read_text()
    # Match CREATE OR REPLACE FUNCTION lines (skip comments)
    pattern = re.compile(
        r'^CREATE\s+OR\s+REPLACE\s+FUNCTION\s+(st_\w+)'
        r'(?:\s+\n(?:\s+.*\n)*?\s+ABI\s+(\w+))?',
        re.MULTILINE,
    )
    # Simpler: match function name, then look for ABI line within next few lines
    lines = text.splitlines()
    funcs: dict[str, list[str]] = {}
    i = 0
    while i < len(lines):
        m = re.match(r'^CREATE\s+OR\s+REPLACE\s+FUNCTION\s+(st_\w+)', lines[i])
        if m:
            name = m.group(1)
            abi = None
            for j in range(i + 1, min(i + 6, len(lines))):
                abi_m = re.match(r'\s+ABI\s+(\w+)', lines[j])
                if abi_m:
                    abi = abi_m.group(1)
                    break
                if re.match(r'\s+(DETERMINISTIC|NOT)\b', lines[j]):
                    break
            if abi == 'COLUMNAR_V1':
                funcs.setdefault(name, []).append('')  # bare alias → _col
            else:
                funcs.setdefault(name, []).append('')  # no bare alias, only _mp
            # Also check for alias lines: CREATE OR REPLACE FUNCTION name AS ...
            for j in range(i + 1, min(i + 3, len(lines))):
                alias_pat = r'^CREATE\s+OR\s+REPLACE\s+FUNCTION\s+' + re.escape(name) + r'\s+AS\s+\(.*?\)\s*->\s*(st_\w+)'
                alias_m = re.match(alias_pat, lines[j])
                if alias_m:
                    target = alias_m.group(1)
                    suffix = target[len(name):] if target != name else ''
                    funcs[name].append(suffix)
        i += 1
    # Deduplicate
    for k in funcs:
        funcs[k] = sorted(set(funcs[k]))
    return funcs


# ---------------------------------------------------------------------------
# Suffix application (whitelist-based, like bench_sf.py)
# ---------------------------------------------------------------------------

# Functions that should never get a wire suffix (CH built-ins, geo types, etc.)
_SKIP = frozenset([
    'concat', 'toString', 'length', 'arraySort', 'round', 'sum',
    'groupArray', 'groupArrayAppending', 'UNION ALL', 'LIMIT',
    'SELECT', 'FROM', 'WHERE', 'AS', 'WITH',
])


def _apply_suffix(sql: str, suffix: str, whitelist: set[str]) -> str:
    """Append *suffix* to every whitelisted function name.

    Uses word-boundary matching so `st_collect` doesn't become
    `st_collect_mp_mp`.  Order is longest-first to prioritise longer names.
    """
    if not suffix:
        return sql
    for fn in sorted(whitelist, key=len, reverse=True):
        sql = re.sub(rf'\b{re.escape(fn)}\b', f'{fn}{suffix}', sql)
    return sql


# ---------------------------------------------------------------------------
# Test definitions
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Test:
    name: str
    query: str
    expected: str


def _tests() -> list[Test]:
    """All e2e test cases."""
    T = []

    def t(name: str, query: str, expected: str):
        T.append(Test(name, query, expected))

    # Version / diagnostics
    t("geos_version", "SELECT geos_version() LIKE '3.%'", "1")

    # Geometry constructors / I/O
    t("st_makepoint", "SELECT st_astext(st_makepoint(1.0, 2.0))", "POINT (1 2)")
    t("st_geomfromtext", "SELECT st_astext(st_geomfromtext('POINT (3 4)'))", "POINT (3 4)")
    t("st_asewkt_plain", "SELECT st_asewkt(st_geomfromtext('POINT (0 0)'))", "POINT (0 0)")

    # Accessors
    t("st_x", "SELECT st_x(st_makepoint(1.0, 2.0))", "1")
    t("st_y", "SELECT st_y(st_makepoint(1.0, 2.0))", "2")
    t("st_srid_default", "SELECT st_srid(st_geomfromtext('POINT (0 0)'))", "0")

    # Measurements
    t("st_area_1",
      "SELECT st_area(st_geomfromtext('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'))", "1")
    t("st_area_6",
      "SELECT st_area(st_geomfromtext('POLYGON ((0 0, 2 0, 2 3, 0 3, 0 0))'))", "6")
    t("st_distance",
      "SELECT st_distance(st_makepoint(0.0, 0.0), st_makepoint(3.0, 4.0))", "5")
    t("st_length",
      "SELECT st_length(st_geomfromtext('LINESTRING (0 0, 3 4)'))", "5")
    t("st_perimeter",
      "SELECT st_perimeter(st_geomfromtext('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'))", "4")

    # Predicates
    t("st_equals_same",
      "SELECT st_equals(st_geomfromtext('POINT (1 2)'), st_geomfromtext('POINT (1 2)'))", "1")
    t("st_equals_diff",
      "SELECT st_equals(st_geomfromtext('POINT (1 2)'), st_geomfromtext('POINT (2 1)'))", "0")
    t("st_intersects_yes",
      "SELECT st_intersects(st_geomfromtext('LINESTRING (0 0, 2 2)'), st_geomfromtext('LINESTRING (0 2, 2 0)'))", "1")
    t("st_intersects_no",
      "SELECT st_intersects(st_geomfromtext('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'), st_geomfromtext('POLYGON ((5 5, 6 5, 6 6, 5 6, 5 5))'))", "0")
    t("st_contains_yes",
      "SELECT st_contains(st_geomfromtext('POLYGON ((0 0, 3 0, 3 3, 0 3, 0 0))'), st_geomfromtext('POINT (1 1)'))", "1")
    t("st_contains_no",
      "SELECT st_contains(st_geomfromtext('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'), st_geomfromtext('POINT (2 2)'))", "0")
    t("st_within",
      "SELECT st_within(st_geomfromtext('POINT (1 1)'), st_geomfromtext('POLYGON ((0 0, 3 0, 3 3, 0 3, 0 0))'))", "1")
    t("st_disjoint",
      "SELECT st_disjoint(st_geomfromtext('POINT (0 0)'), st_geomfromtext('POINT (1 1)'))", "1")
    t("st_touches",
      "SELECT st_touches(st_geomfromtext('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'), st_geomfromtext('POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0))'))", "1")
    t("st_crosses",
      "SELECT st_crosses(st_geomfromtext('LINESTRING (0 0, 2 2)'), st_geomfromtext('LINESTRING (0 2, 2 0)'))", "1")
    t("st_overlaps_no",
      "SELECT st_overlaps(st_geomfromtext('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'), st_geomfromtext('POLYGON ((2 2, 3 2, 3 3, 2 3, 2 2))'))", "0")
    t("st_isvalid_yes",
      "SELECT st_isvalid(st_geomfromtext('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'))", "1")
    t("st_isvalid_no",
      "SELECT st_isvalid(st_geomfromtext('POLYGON ((0 0, 2 2, 2 0, 0 2, 0 0))'))", "0")
    t("st_isempty",
      "SELECT st_isempty(st_geomfromtext('GEOMETRYCOLLECTION EMPTY'))", "1")

    # Transforms
    t("st_buffer_area",
      "SELECT round(st_area(st_buffer(st_makepoint(0.0, 0.0), 1.0)), 1)", "3.1")
    t("st_centroid",
      "SELECT st_astext(st_centroid(st_geomfromtext('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))')))", "POINT (1 1)")
    t("st_envelope",
      "SELECT st_astext(st_envelope(st_geomfromtext('LINESTRING (0 0, 2 3)')))",
      "POLYGON ((0 0, 2 0, 2 3, 0 3, 0 0))")
    t("st_convexhull",
      "SELECT round(st_area(st_convexhull(st_geomfromtext('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))'))), 1)", "4")
    t("st_reverse",
      "SELECT st_astext(st_reverse(st_geomfromtext('LINESTRING (0 0, 1 1, 2 0)')))",
      "LINESTRING (2 0, 1 1, 0 0)")
    t("st_simplify",
      "SELECT st_astext(st_simplify(st_geomfromtext('LINESTRING (0 0, 0.5 0.1, 1 0)'), 1.0))",
      "LINESTRING (0 0, 1 0)")

    # Overlay
    t("st_intersection",
      "SELECT round(st_area(st_intersection(st_geomfromtext('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))'), st_geomfromtext('POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))'))))", "1")
    t("st_union_area",
      "SELECT round(st_area(st_union(st_geomfromtext('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'), st_geomfromtext('POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0))'))))", "2")
    t("st_difference",
      "SELECT round(st_area(st_difference(st_geomfromtext('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))'), st_geomfromtext('POLYGON ((1 0, 2 0, 2 2, 1 2, 1 0))'))))", "2")
    t("st_symdiff_area",
      "SELECT round(st_area(st_symdifference(st_geomfromtext('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))'), st_geomfromtext('POLYGON ((1 0, 3 0, 3 2, 1 2, 1 0))'))))", "4")

    # Aggregate — is_aggregate=1, CH accumulates rows per group automatically
    t("st_union_agg",
      "SELECT round(st_area(st_union_agg(geom))) FROM (SELECT st_geomfromtext('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))') AS geom UNION ALL SELECT st_geomfromtext('POLYGON ((5 5, 6 5, 6 6, 5 6, 5 5))'))", "2")
    t("st_collect_agg",
      "SELECT st_numgeometries(st_collect_agg(geom)) FROM (SELECT st_geomfromtext('POINT (0 0)') AS geom UNION ALL SELECT st_geomfromtext('POINT (1 1)'))", "2")
    t("st_collect_agg_nodissolve",
      "SELECT round(st_area(st_collect_agg(geom))) FROM (SELECT st_geomfromtext('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))') AS geom UNION ALL SELECT st_geomfromtext('POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))'))", "8")
    t("st_extent_agg",
      "SELECT round(st_area(st_extent_agg(geom))) FROM (SELECT st_geomfromtext('POINT (0 0)') AS geom UNION ALL SELECT st_geomfromtext('POINT (3 4)'))", "12")
    t("st_extent_agg_polys",
      "SELECT round(st_area(st_extent_agg(geom))) FROM (SELECT st_geomfromtext('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))') AS geom UNION ALL SELECT st_geomfromtext('POLYGON ((3 0, 5 0, 5 2, 3 2, 3 0))'))", "10")
    t("st_makeline_agg",
      "SELECT st_astext(st_makeline_agg(geom)) FROM (SELECT st_geomfromtext('POINT (0 0)') AS geom UNION ALL SELECT st_geomfromtext('POINT (1 0)') UNION ALL SELECT st_geomfromtext('POINT (1 1)'))", "LINESTRING (0 0, 1 0, 1 1)")
    t("st_convexhull_agg",
      "SELECT round(st_area(st_convexhull_agg(geom)), 1) FROM (SELECT st_geomfromtext('POINT (0 0)') AS geom UNION ALL SELECT st_geomfromtext('POINT (1 0)') UNION ALL SELECT st_geomfromtext('POINT (0 1)'))", "0.5")

    # st_knn: k-nearest-neighbour
    t("st_knn_basic",
      "WITH cands AS (SELECT groupArray(g) AS arr FROM (SELECT st_makepoint(0.0,0.0) AS g UNION ALL SELECT st_makepoint(3.0,4.0) UNION ALL SELECT st_makepoint(10.0,0.0))), r AS (SELECT st_knn(st_makepoint(1.0,0.0), (SELECT arr FROM cands), 2) AS res) SELECT concat(toString(length(res)), ',', toString(arraySort(x -> x.1, res)[1].1), ',', toString(round(arraySort(x -> x.1, res)[1].2, 1))) FROM r",
      "2,0,1")
    t("st_knn_k1",
      "WITH cands AS (SELECT groupArray(g) AS arr FROM (SELECT st_makepoint(5.0,5.0) AS g UNION ALL SELECT st_makepoint(0.0,0.0))) SELECT (st_knn(st_makepoint(1.0,1.0), (SELECT arr FROM cands), 1))[1].1",
      "1")
    t("st_knn_empty",
      "SELECT length(st_knn(st_makepoint(0.0,0.0), (SELECT groupArray(g) FROM (SELECT st_makepoint(0.0,0.0) AS g LIMIT 0)), 3))",
      "0")

    # Multi-row batch
    t("batch_area_sum",
      "SELECT sum(st_area(st_geomfromtext(wkt))) FROM (SELECT 'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))' AS wkt UNION ALL SELECT 'POLYGON ((0 0, 2 0, 2 3, 0 3, 0 0))')", "7")

    # SRID round-trip
    t("st_setsrid_srid", "SELECT st_srid(st_setsrid(st_makepoint(1.0, 2.0), 4326))", "4326")
    t("st_setsrid_ewkt", "SELECT st_asewkt(st_setsrid(st_makepoint(1.0, 2.0), 4326))", "SRID=4326;POINT (1 2)")
    t("st_geomfromtext_srid", "SELECT st_asewkt(st_geomfromtext('SRID=4326;POINT (1 2)'))", "SRID=4326;POINT (1 2)")
    t("st_asewkt_no_srid", "SELECT st_asewkt(st_makepoint(1.0, 2.0))", "POINT (1 2)")

    # I/O
    t("st_geomfromwkb", "SELECT st_astext(st_geomfromwkb(st_geomfromtext('POINT (1 2)')))", "POINT (1 2)")
    t("st_geomfromgeojson", "SELECT st_astext(st_geomfromgeojson('{\"type\":\"Point\",\"coordinates\":[1,2]}'))", "POINT (1 2)")

    # Accessors
    t("st_npoints", "SELECT st_npoints(st_geomfromtext('LINESTRING (0 0, 1 1, 2 0)'))", "3")
    t("st_numgeometries", "SELECT st_numgeometries(st_geomfromtext('GEOMETRYCOLLECTION (POINT (0 0), POINT (1 1))'))", "2")
    t("st_geometrytype_pt", "SELECT st_geometrytype(st_makepoint(0.0, 0.0))", "ST_Point")
    t("st_geometrytype_ls", "SELECT st_geometrytype(st_geomfromtext('LINESTRING (0 0, 1 1)'))", "ST_LineString")
    t("st_dimension_pt", "SELECT st_dimension(st_makepoint(0.0, 0.0))", "0")
    t("st_dimension_poly", "SELECT st_dimension(st_geomfromtext('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'))", "2")
    t("st_isring_yes", "SELECT st_isring(st_geomfromtext('LINESTRING (0 0, 1 0, 1 1, 0 1, 0 0)'))", "1")
    t("st_isring_no", "SELECT st_isring(st_geomfromtext('LINESTRING (0 0, 1 0, 1 1)'))", "0")
    t("st_issimple", "SELECT st_issimple(st_makepoint(0.0, 0.0))", "1")
    t("st_isvalidreason", "SELECT st_isvalidreason(st_geomfromtext('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'))", "Valid Geometry")

    # Linestring point accessors
    t("st_startpoint", "SELECT st_astext(st_startpoint(st_geomfromtext('LINESTRING (0 0, 1 1, 2 0)')))", "POINT (0 0)")
    t("st_endpoint", "SELECT st_astext(st_endpoint(st_geomfromtext('LINESTRING (0 0, 1 1, 2 0)')))", "POINT (2 0)")
    t("st_pointn", "SELECT st_astext(st_pointn(st_geomfromtext('LINESTRING (0 0, 1 1, 2 0)'), 2))", "POINT (1 1)")

    # Predicates
    t("st_dwithin_yes", "SELECT st_dwithin(st_geomfromtext('POINT (0 0)'), st_geomfromtext('POINT (3 4)'), 5.0)", "1")
    t("st_dwithin_no", "SELECT st_dwithin(st_geomfromtext('POINT (0 0)'), st_geomfromtext('POINT (3 4)'), 4.9)", "0")
    t("st_intersects_ext_yes",
      "SELECT st_intersects_extent(st_geomfromtext('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))'), st_geomfromtext('POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))'))", "1")
    t("st_intersects_ext_no",
      "SELECT st_intersects_extent(st_geomfromtext('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'), st_geomfromtext('POLYGON ((5 5, 6 5, 6 6, 5 6, 5 5))'))", "0")
    t("st_covers_interior",
      "SELECT st_covers(st_geomfromtext('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'), st_geomfromtext('POINT (5 5)'))", "1")
    t("st_covers_boundary",
      "SELECT st_covers(st_geomfromtext('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'), st_geomfromtext('POINT (5 0)'))", "1")
    t("st_coveredby",
      "SELECT st_coveredby(st_geomfromtext('POINT (5 0)'), st_geomfromtext('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'))", "1")
    t("st_relate", "SELECT st_relate(st_geomfromtext('POINT (0 0)'), st_geomfromtext('POINT (0 0)'))", "0FFFFFFF2")

    # Distance metrics
    t("st_hausdorff",
      "SELECT st_hausdorffdistance(st_geomfromtext('LINESTRING (0 0, 2 0)'), st_geomfromtext('LINESTRING (0 1, 2 1)'))", "1")

    # Geometry constructors
    t("st_makeline", "SELECT st_astext(st_makeline(st_makepoint(0.0, 0.0), st_makepoint(1.0, 1.0)))", "LINESTRING (0 0, 1 1)")
    t("st_makepolygon",
      "SELECT st_geometrytype(st_makepolygon(st_geomfromtext('LINESTRING (0 0, 1 0, 1 1, 0 1, 0 0)')))", "ST_Polygon")
    t("st_collect",
      "SELECT st_numgeometries(st_collect(st_makepoint(0.0, 0.0), st_makepoint(1.0, 1.0)))", "2")

    # Transforms
    t("st_translate", "SELECT st_astext(st_translate(st_makepoint(1.0, 2.0), 3.0, 4.0))", "POINT (4 6)")
    t("st_scale", "SELECT st_astext(st_scale(st_makepoint(2.0, 3.0), 2.0, 3.0))", "POINT (4 9)")
    t("st_boundary",
      "SELECT st_astext(st_boundary(st_geomfromtext('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))')))",
      "LINESTRING (0 0, 1 0, 1 1, 0 1, 0 0)")
    t("st_exteriorring",
      "SELECT st_astext(st_exteriorring(st_geomfromtext('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))')))",
      "LINESTRING (0 0, 1 0, 1 1, 0 1, 0 0)")
    t("st_interiorpoint",
      "SELECT st_within(st_interiorpoint(st_geomfromtext('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))')), st_geomfromtext('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'))", "1")

    # Processing
    t("st_makevalid",
      "SELECT st_isvalid(st_makevalid(st_geomfromtext('POLYGON ((0 0, 2 2, 2 0, 0 2, 0 0))')))", "1")
    t("st_subdivide",
      "SELECT st_numgeometries(st_subdivide(st_geomfromtext('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))'), 5))", "1")
    t("st_unaryunion",
      "SELECT round(st_area(st_unaryunion(st_geomfromtext('GEOMETRYCOLLECTION (POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0)), POLYGON ((2 0, 3 0, 3 1, 2 1, 2 0)))'))))", "2")

    # Clustering
    t("st_cluster_disjoint",
      "SELECT st_numgeometries(st_clusterintersecting(st_geomfromtext('GEOMETRYCOLLECTION (POINT (0 0), POINT (10 10))')))", "2")
    t("st_cluster_intersects",
      "SELECT st_numgeometries(st_clusterintersecting(st_geomfromtext('GEOMETRYCOLLECTION (POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0)), POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1)))')))", "1")

    # ── Native CH geometry type inputs (COL_VARIANT — Phase 3) ───────────
    t("native_point_x", "SELECT st_x((1.0, 2.0)::Point)", "1")
    t("native_point_y", "SELECT st_y((3.0, 5.0)::Point)", "5")
    t("native_linestring_length",
      "SELECT st_length([(0.0, 0.0), (3.0, 4.0)]::LineString)", "5")
    t("native_linestring_npoints",
      "SELECT st_npoints([(0.0, 0.0), (1.0, 0.0), (2.0, 0.0)]::LineString)", "3")
    t("native_polygon_area",
      "SELECT st_area([[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]]::Polygon)", "1")
    t("native_polygon_perimeter",
      "SELECT st_perimeter([[(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 2.0), (0.0, 0.0)]]::Polygon)", "8")
    t("native_multipolygon_numgeom",
      "SELECT st_numgeometries([[[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]], [[(5.0, 5.0), (6.0, 5.0), (6.0, 6.0), (5.0, 6.0), (5.0, 5.0)]]]::MultiPolygon)", "2")
    t("native_ring_npoints",
      "SELECT st_npoints([(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]::Ring)", "5")
    t("native_multilinestring_numgeom",
      "SELECT st_numgeometries([[(0.0, 0.0), (1.0, 1.0)], [(2.0, 2.0), (3.0, 3.0)]]::MultiLineString)", "2")
    t("native_pred_wkb_geo",
      "SELECT st_contains(st_geomfromtext('POLYGON ((0 0, 3 0, 3 3, 0 3, 0 0))'), (1.0, 1.0)::Point)", "1")
    t("native_pred_geo_wkb",
      "SELECT st_contains([[(0.0, 0.0), (3.0, 0.0), (3.0, 3.0), (0.0, 3.0), (0.0, 0.0)]]::Polygon, st_geomfromtext('POINT (1 1)'))", "1")
    t("native_pred_geo_geo",
      "SELECT st_contains([[(0.0, 0.0), (3.0, 0.0), (3.0, 3.0), (0.0, 3.0), (0.0, 0.0)]]::Polygon, (1.0, 1.0)::Point)", "1")
    t("native_multirow_area_sum",
      "SELECT sum(st_area(geo)) FROM (SELECT [[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]]::Polygon AS geo UNION ALL SELECT [[(0.0, 0.0), (2.0, 0.0), (2.0, 3.0), (0.0, 3.0), (0.0, 0.0)]]::Polygon)", "7")
    t("native_cross_check_area",
      "SELECT st_area([[(0.0, 0.0), (2.0, 0.0), (2.0, 3.0), (0.0, 3.0), (0.0, 0.0)]]::Polygon) = st_area(st_geomfromtext('POLYGON ((0 0, 2 0, 2 3, 0 3, 0 0))'))", "1")
    t("native_cross_check_length",
      "SELECT st_length([(0.0, 0.0), (3.0, 4.0)]::LineString) = st_length(st_geomfromtext('LINESTRING (0 0, 3 4)'))", "1")

    return T


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def locate_clickhouse() -> str:
    """Find the clickhouse binary."""
    for candidate in [
        os.environ.get("CLICKHOUSE_BIN"),
        str(Path(__file__).resolve().parent.parent.parent / "ClickHouse" / "build" / "programs" / "clickhouse"),
        "/data/bacek/src/ClickHouse/build/programs/clickhouse",
    ]:
        if candidate and os.access(candidate, os.X_OK):
            return candidate
    # Fallback: try PATH
    for path in os.environ.get("PATH", "").split(":"):
        full = os.path.join(path, "clickhouse")
        if os.access(full, os.X_OK):
            return full
    raise RuntimeError("clickhouse binary not found")


def main():
    parser = argparse.ArgumentParser(description="chgeos end-to-end tests")
    parser.add_argument("clickhouse", nargs="?", help="Path to clickhouse binary")
    parser.add_argument("wasm", nargs="?", help="Path to chgeos.wasm")
    parser.add_argument("--wire-protocol", default=os.environ.get("WIRE_PROTOCOL", "col"),
                        choices=["col", "mp", "cb"],
                        help="Wire format (default: col)")
    args = parser.parse_args()

    clickhouse = args.clickhouse or locate_clickhouse()
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent

    wasm_bin = args.wasm or str(repo_root / "build_wasm" / "chgeos.wasm")
    if not os.path.isfile(wasm_bin):
        print(f"ERROR: WASM binary not found: {wasm_bin}", file=sys.stderr)
        sys.exit(1)

    config = str(script_dir / "config-e2e.xml")
    create_sql = str(script_dir / "create.sql")

    # Parse create.sql to find registered variants
    registered = _parse_create_sql(create_sql)

    # Build whitelist: functions that have the requested suffix
    suffix = f"_{args.wire_protocol}" if args.wire_protocol != "col" else ""
    if suffix:
        whitelist = {name for name, variants in registered.items() if suffix in variants}
    else:
        whitelist = set(registered.keys())

    # Set up temp directory for wasm
    tmpdir = TemporaryDirectory()
    wasm_dest = str(Path(tmpdir.name) / "user_scripts" / "wasm" / "chgeos.wasm")
    os.makedirs(os.path.dirname(wasm_dest), exist_ok=True)
    Path(wasm_bin).read_bytes().__class__.__name__  # just to be sure
    import shutil
    shutil.copy2(wasm_bin, wasm_dest)

    def ch(*args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [clickhouse, "local", "--config-file", config,
             "--path", tmpdir.name] + list(args),
            capture_output=True, text=True,
        )

    # Build multi-query
    create_sql_content = Path(create_sql).read_text()
    queries = create_sql_content

    # Apply wire protocol suffix
    queries = _apply_suffix(queries, suffix, whitelist)

    # Add test queries
    tests = _tests()
    for test in tests:
        queries += test.query + ";"

    # Run all queries
    result = ch("--multiquery", "--query", queries)
    if result.returncode != 0:
        print(f"CH ERROR: {result.stderr}", file=sys.stderr)
        # Try to continue with partial results
        if not result.stdout.strip():
            sys.exit(1)

    results = result.stdout.strip().splitlines()

    # Compare
    passed = 0
    failed = 0
    for i, test in enumerate(tests):
        if i >= len(results):
            print(f"MISS  {test.name}  (no result from CH)")
            failed += 1
            continue
        actual = results[i].strip()
        if actual == test.expected:
            print(f"PASS  {test.name}")
            passed += 1
        else:
            print(f"FAIL  {test.name}")
            print(f"      expected: {test.expected}")
            print(f"      got:      {actual}")
            failed += 1

    print(f"\nResults: {passed} passed, {failed} failed")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
