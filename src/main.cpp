#include <geos/version.h>

#include "functions.hpp"
#include "rowbinary.hpp"
#include "udf.hpp"

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
// Aggregate functions: RowBinary Array(String) unpacking not implemented
CH_UDF_FUNC(st_union_agg)
CH_UDF_FUNC(st_collect_agg)
CH_UDF_FUNC(st_extent_agg)
CH_UDF_FUNC(st_makeline_agg)
CH_UDF_FUNC(st_convexhull_agg)

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

} // extern "C"
