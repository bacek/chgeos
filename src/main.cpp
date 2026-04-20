#include <geos/version.h>

#include "functions.hpp"
#include "functions/knn.hpp"
#include "rowbinary.hpp"
#include "msgpack.hpp"
#include "columnar.hpp"

extern "C" {

// ── Keep MsgPack ──────────────────────────────────────────────────────────────
// Bench functions: intentional MsgPack — they measure that specific overhead.
CH_UDF_FUNC(geos_bench_noop)
CH_UDF_FUNC(geos_bench_noop_rb)
CH_UDF_FUNC(geos_bench_wkb_parse)
CH_UDF_FUNC(geos_bench_envelope)
CH_UDF_FUNC(geos_bench_intersects_nobbox)
// Diagnostics
CH_UDF_FUNC(geos_log_test)
CH_UDF_FUNC(geos_test_exception)
// CH native type converters: RowBinary Tuple/Array layout differs from MsgPack
CH_UDF_FUNC(st_geomfromchpoint)
CH_UDF_FUNC(st_geomfromchlinestring)
CH_UDF_FUNC(st_geomfromchpolygon)
CH_UDF_FUNC(st_geomfromchmultipolygon)
// Pseudo-aggregate functions: Array(String) → Geometry, used with groupArray()
CH_UDF_RB_ONLY(st_union_agg)
CH_UDF_RB_ONLY(st_collect_agg)
CH_UDF_RB_ONLY(st_extent_agg)
CH_UDF_RB_ONLY(st_makeline_agg)
CH_UDF_RB_ONLY(st_convexhull_agg)
// COLUMNAR_V1 variants (COL_COMPLEX input: Array(String) via groupArray())
CH_UDF_COL(st_union_agg)
CH_UDF_COL(st_collect_agg)
CH_UDF_COL(st_extent_agg)
CH_UDF_COL(st_makeline_agg)
CH_UDF_COL(st_convexhull_agg)

// ── RowBinary — scalar functions ──────────────────────────────────────────────
// I/O
CH_UDF_RB_ONLY(geos_version)
CH_UDF_RB_ONLY(st_astext)
CH_UDF_RB_ONLY(st_asewkt)
CH_UDF_RB_ONLY(st_geomfromtext)
CH_UDF_RB_ONLY(st_geomfromgeojson)
CH_UDF_RB_ONLY(st_geomfromwkb)
// Constructors / geometry builders
CH_UDF_RB_ONLY(st_extent)
CH_UDF_RB_ONLY(st_envelope)
CH_UDF_RB_ONLY(st_expand)
CH_UDF_RB_ONLY(st_makebox2d)
CH_UDF_RB_ONLY(st_startpoint)
CH_UDF_RB_ONLY(st_endpoint)
CH_UDF_RB_ONLY(st_collect)
CH_UDF_RB_ONLY(st_makepoint)
CH_UDF_RB_ONLY(st_makepoint3d)
CH_UDF_RB_ONLY(st_makepolygon)
CH_UDF_RB_ONLY(st_makeline)
CH_UDF_RB_ONLY(st_convexhull)
CH_UDF_RB_ONLY(st_boundary)
CH_UDF_RB_ONLY(st_reverse)
CH_UDF_RB_ONLY(st_normalize)
CH_UDF_RB_ONLY(st_geometryn)
CH_UDF_RB_ONLY(st_symdifference)
CH_UDF_RB_ONLY(st_exteriorring)
CH_UDF_RB_ONLY(st_interiorringn)
CH_UDF_RB_ONLY(st_pointn)
CH_UDF_RB_ONLY(st_minimumboundingcircle)
CH_UDF_RB_ONLY(st_snap)
CH_UDF_RB_ONLY(st_offsetcurve)
CH_UDF_RB_ONLY(st_linmerge)
CH_UDF_RB_ONLY(st_polygonize)
CH_UDF_RB_ONLY(st_delaunaytriangles)
CH_UDF_RB_ONLY(st_voronoidiagram)
CH_UDF_RB_ONLY(st_closestpoint)
CH_UDF_RB_ONLY(st_shortestline)
CH_UDF_RB_ONLY(st_sharedpaths)
CH_UDF_RB_ONLY(st_node)
CH_UDF_RB_ONLY(st_addpoint)
CH_UDF_RB_ONLY(st_removepoint)
CH_UDF_RB_ONLY(st_setpoint)
// Accessors
CH_UDF_RB_ONLY(st_x)
CH_UDF_RB_ONLY(st_y)
CH_UDF_RB_ONLY(st_z)
CH_UDF_RB_ONLY(st_srid)
CH_UDF_RB_ONLY(st_npoints)
CH_UDF_RB_ONLY(st_area)
CH_UDF_RB_ONLY(st_centroid)
CH_UDF_RB_ONLY(st_length)
CH_UDF_RB_ONLY(st_perimeter)
CH_UDF_RB_ONLY(st_isvalid)
CH_UDF_RB_ONLY(st_isempty)
CH_UDF_RB_ONLY(st_issimple)
CH_UDF_RB_ONLY(st_isring)
CH_UDF_RB_ONLY(st_geometrytype)
CH_UDF_RB_ONLY(st_numgeometries)
CH_UDF_RB_ONLY(st_numinteriorrings)
CH_UDF_RB_ONLY(st_numpoints)
CH_UDF_RB_ONLY(st_dimension)
CH_UDF_RB_ONLY(st_interiorpoint)
CH_UDF_RB_ONLY(st_isvalidreason)
CH_UDF_RB_ONLY(st_nrings)
CH_UDF_RB_ONLY(st_hausdorffdistance)
CH_UDF_RB_ONLY(st_hausdorffdistance_densify)
CH_UDF_RB_ONLY(st_frechetdistance)
CH_UDF_RB_ONLY(st_frechetdistance_densify)
// Distance
CH_UDF_RB_ONLY(st_distance)
// Processing
CH_UDF_RB_ONLY(st_makevalid)
CH_UDF_RB_ONLY(st_buffer)
CH_UDF_RB_ONLY(st_buffer_params)
CH_UDF_RB_ONLY(st_simplify)
CH_UDF_RB_ONLY(st_segmentize)
CH_UDF_RB_ONLY(st_subdivide)
// Overlay
CH_UDF_RB_ONLY(st_union)
CH_UDF_RB_ONLY(st_intersection)
CH_UDF_RB_ONLY(st_difference)
CH_UDF_RB_ONLY(st_unaryunion)
CH_UDF_RB_ONLY(st_clusterintersecting)
// Transforms
CH_UDF_RB_ONLY(st_translate)
CH_UDF_RB_ONLY(st_scale)
CH_UDF_RB_ONLY(st_transform)
CH_UDF_RB_ONLY(st_transform_proj)
CH_UDF_RB_ONLY(st_setsrid)
// Relate
CH_UDF_RB_ONLY(st_relate)
CH_UDF_RB_ONLY(st_relate_pattern)

// ── RowBinary — predicates with bbox shortcut ─────────────────────────────────
CH_UDF_RB_BBOX2(st_contains,        bbox_op_contains,   false)
CH_UDF_RB_BBOX2(st_intersects,      bbox_op_intersects, false)
CH_UDF_RB_BBOX2(st_touches,         bbox_op_intersects, false)
CH_UDF_RB_BBOX2(st_within,          bbox_op_rcontains,  false)
CH_UDF_RB_BBOX2(st_crosses,         bbox_op_intersects, false)
CH_UDF_RB_BBOX2(st_overlaps,        bbox_op_intersects, false)
CH_UDF_RB_BBOX2(st_disjoint,        bbox_op_intersects, true)
CH_UDF_RB_BBOX2(st_equals,          bbox_op_intersects, false)
CH_UDF_RB_BBOX2(st_covers,          bbox_op_contains,   false)
CH_UDF_RB_BBOX2(st_coveredby,       bbox_op_rcontains,  false)
CH_UDF_RB_BBOX2(st_containsproperly,bbox_op_contains,   false)
// st_intersects_extent: plain name now RowBinary; _rb export kept for compat
CH_UDF_RB_ONLY(st_intersects_extent)
CH_UDF_RB_FUNC(st_intersects_extent)  // keeps st_intersects_extent_rb export
// st_dwithin: 3-arg, no bbox shortcut at predicate level
CH_UDF_RB_ONLY(st_dwithin)

// ── COLUMNAR_V1 — predicates ──────────────────────────────────────────────────
CH_UDF_COL_BBOX2_POINT(st_contains,   bbox_op_contains,   false)
CH_UDF_COL_BBOX2_POINT(st_intersects, bbox_op_intersects, false)
CH_UDF_COL_BBOX2(st_touches,         bbox_op_intersects, false)
CH_UDF_COL_BBOX2_POINT(st_within,    bbox_op_rcontains,  false)
CH_UDF_COL_BBOX2(st_crosses,         bbox_op_intersects, false)
CH_UDF_COL_BBOX2(st_overlaps,        bbox_op_intersects, false)
CH_UDF_COL_BBOX2(st_disjoint,        bbox_op_intersects, true)
CH_UDF_COL_BBOX2(st_equals,          bbox_op_intersects, false)
CH_UDF_COL_BBOX2_POINT(st_covers,    bbox_op_contains,   false)
CH_UDF_COL_BBOX2_POINT(st_coveredby, bbox_op_rcontains,  false)
CH_UDF_COL_BBOX2(st_containsproperly,bbox_op_contains,   false)
CH_UDF_COL_PRED3(st_dwithin)

// ── COLUMNAR_V1 — all other (types deduced from _impl) ──────────────────────
CH_UDF_COL(st_x)
CH_UDF_COL(st_y)
CH_UDF_COL(st_z)
CH_UDF_COL(st_area)
CH_UDF_COL(st_length)
CH_UDF_COL(st_perimeter)
CH_UDF_COL(st_distance)
CH_UDF_COL(st_hausdorffdistance)
CH_UDF_COL(st_frechetdistance)
CH_UDF_COL(st_isvalid)
CH_UDF_COL(st_isempty)
CH_UDF_COL(st_issimple)
CH_UDF_COL(st_isring)
CH_UDF_COL(st_srid)
CH_UDF_COL(st_npoints)
CH_UDF_COL(st_numpoints)
CH_UDF_COL(st_numgeometries)
CH_UDF_COL(st_numinteriorrings)
CH_UDF_COL(st_nrings)
CH_UDF_COL(st_dimension)
CH_UDF_COL(st_astext)
CH_UDF_COL(st_asewkt)
CH_UDF_COL(st_geometrytype)
CH_UDF_COL(st_isvalidreason)
CH_UDF_COL(st_relate)
CH_UDF_COL(st_intersection)
CH_UDF_COL(st_union)
CH_UDF_COL(st_difference)
CH_UDF_COL(st_makeline)
CH_UDF_COL(st_symdifference)
CH_UDF_COL(st_collect)
CH_UDF_COL(st_closestpoint)
CH_UDF_COL(st_shortestline)
CH_UDF_COL(st_sharedpaths)
CH_UDF_COL(st_convexhull)
CH_UDF_COL(st_envelope)
CH_UDF_COL(st_centroid)
CH_UDF_COL(st_makevalid)
CH_UDF_COL(st_boundary)
CH_UDF_COL(st_unaryunion)
CH_UDF_COL(st_reverse)
CH_UDF_COL(st_normalize)
CH_UDF_COL(st_node)
CH_UDF_COL(st_linmerge)
CH_UDF_COL(st_polygonize)
CH_UDF_COL(st_clusterintersecting)
CH_UDF_COL(st_extent)
CH_UDF_COL(st_interiorpoint)
CH_UDF_COL(st_expand)

// ── COLUMNAR_V1 — additional functions ───────────────────────────────────────
// Accessors (densify variants)
CH_UDF_COL(st_hausdorffdistance_densify)
CH_UDF_COL(st_frechetdistance_densify)
// Predicate (3 args: geom, geom, string_view)
CH_UDF_COL(st_relate_pattern)
// I/O
CH_UDF_COL(geos_version)
CH_UDF_COL(st_geomfromgeojson)
// Constructors
CH_UDF_COL(st_startpoint)
CH_UDF_COL(st_endpoint)
CH_UDF_COL(st_makebox2d)
CH_UDF_COL(st_geometryn)
CH_UDF_COL(st_exteriorring)
CH_UDF_COL(st_interiorringn)
CH_UDF_COL(st_pointn)
CH_UDF_COL(st_minimumboundingcircle)
CH_UDF_COL(st_snap)
CH_UDF_COL(st_offsetcurve)
CH_UDF_COL(st_delaunaytriangles)
CH_UDF_COL(st_voronoidiagram)
CH_UDF_COL(st_makepoint)
CH_UDF_COL(st_makepoint3d)
CH_UDF_COL(st_makepolygon)
CH_UDF_COL(st_addpoint)
CH_UDF_COL(st_removepoint)
CH_UDF_COL(st_setpoint)
// Processing
CH_UDF_COL(st_buffer)
CH_UDF_COL(st_buffer_params)
CH_UDF_COL(st_simplify)
CH_UDF_COL(st_segmentize)
CH_UDF_COL(st_subdivide)
// Transforms
CH_UDF_COL(st_translate)
CH_UDF_COL(st_scale)
CH_UDF_COL(st_setsrid)
CH_UDF_COL(st_transform)
CH_UDF_COL(st_transform_proj)

// ── st_knn: k-nearest-neighbour spatial query ─────────────────────────────────
// Manual export (not via CH_UDF_COL) because we need direct ColView access
// to detect when candidates is const → build STRtree once per batch.
//
// Signature: st_knn(query String, candidates Array(String), k UInt32)
//            → Array(Tuple(UInt64, Float64))
// Returns k (index, distance) pairs sorted by distance ascending.
// Index is 0-based into the candidates array.

__attribute__((export_name("st_knn")))
ch::raw_buffer* st_knn_col(ch::raw_buffer* ptr, uint32_t)
{
    using KVPair   = std::pair<uint64_t, double>;
    using KNNResult = std::vector<KVPair>;

    auto cb = ch::parse_columnar(ptr);
    uint32_t n     = cb.num_rows;
    ch::ColView col_q = cb.col(0);   // String (WKB)
    ch::ColView col_c = cb.col(1);   // Array(String) — COL_COMPLEX
    ch::ColView col_k = cb.col(2);   // UInt32

    uint32_t k = ch::col_get_fixed_widened<uint32_t>(col_k, 0);

    if (k == 0 || n == 0)
        return ch::write_complex_col<KNNResult>(n, [](uint32_t) -> KNNResult { return {}; });

    ch::raw_buffer* out = nullptr;
    try {
        if (col_c.is_const) {
            // Candidates same for every row → build STRtree once.
            auto wkbs = ch::col_get_complex_array<std::span<const uint8_t>>(col_c, 0);
            ch::KNNIndex index(wkbs);

            return ch::write_complex_col<KNNResult>(n, [&](uint32_t row) -> KNNResult {
                if (col_q.is_null(row)) return {};
                auto q = ch::read_wkb(col_q.get_bytes(row));
                return index.query(q.get(), k);
            });
        } else {
            // Candidates vary per row: brute-force.
            return ch::write_complex_col<KNNResult>(n, [&](uint32_t row) -> KNNResult {
                if (col_q.is_null(row)) return {};
                auto q   = ch::read_wkb(col_q.get_bytes(row));
                auto cands = ch::col_get_complex_array<std::span<const uint8_t>>(col_c, row);
                return ch::st_knn_brute(q.get(), cands, k);
            });
        }
    } catch (const std::exception& e) {
        if (out) clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(out));
        ch::panic(e.what());
    }
    __builtin_unreachable();
}

} // extern "C"
