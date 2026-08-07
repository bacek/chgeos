-- chgeos: PostGIS-compatible spatial functions backed by GEOS
-- All functions use ABI BUFFERED_V1.
-- Scalar functions use RowBinary serialization (lower overhead than MsgPack for
-- raw WKB blobs and fixed-width scalars).  Exceptions noted inline.
--
-- Load module first:
--   clickhouse client -q "INSERT INTO system.webassembly_modules (name, code) VALUES ('chgeos', file('/path/to/chgeos.wasm'))"

-- ---------------------------------------------------------------------------
-- Version / metadata
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION geos_version_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS () RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

-- ---------------------------------------------------------------------------
-- Diagnostics  (MsgPack — intentional)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION geos_log_test_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (level UInt32, msg String) RETURNS UInt32
ABI BUFFERED_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION geos_test_exception_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (msg String) RETURNS UInt32
ABI BUFFERED_V1
DETERMINISTIC;

-- ---------------------------------------------------------------------------
-- I/O
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION st_geomfromtext_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkt String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_geomfromgeojson_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geojson String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_geomfromwkb_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

-- CH native type converters  (MsgPack — Tuple/Array RowBinary layout not implemented)
CREATE OR REPLACE FUNCTION st_geomfromchpoint_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (pt Point) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_geomfromchlinestring_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (ls LineString) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_geomfromchpolygon_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (poly Polygon) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_geomfromchmultipolygon_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (mp MultiPolygon) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_astext_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_asewkt_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

-- ---------------------------------------------------------------------------
-- Accessors
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION st_x_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_y_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_srid_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Int32
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_setsrid_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, srid UInt32) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_npoints_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Int32
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_area_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_centroid_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

-- ---------------------------------------------------------------------------
-- Constructors
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION st_envelope_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_extent_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_expand_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, units Float64) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_makebox2d_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (low_left String, up_right String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_startpoint_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_endpoint_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_collect_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

-- ---------------------------------------------------------------------------
-- Predicates
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION st_contains_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1, serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_intersects_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1, serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_touches_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1, serialization_format = 'RowBinary';

-- st_dwithin: distance-based, bbox pruning is NOT safe (false negatives possible)
CREATE OR REPLACE FUNCTION st_dwithin_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String, dist Float64) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_intersects_extent_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1, serialization_format = 'RowBinary';
--
CREATE OR REPLACE FUNCTION st_intersects_extent_rb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary', is_spatial_predicate = 1;

CREATE OR REPLACE FUNCTION st_within_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1, serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_crosses_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1, serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_overlaps_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1, serialization_format = 'RowBinary';

-- st_disjoint: inverted bbox logic — pruning not safe
CREATE OR REPLACE FUNCTION st_disjoint_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_equals_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1, serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_covers_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1, serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_coveredby_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1, serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_containsproperly_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1, serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_relate_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_relate_pattern_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String, pattern String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_distance_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_length_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_isvalid_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_isempty_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_issimple_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_isring_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_geometrytype_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_numgeometries_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Int32
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_dimension_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Int32
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_interiorpoint_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_isvalidreason_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_numinteriorrings_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Int32
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_hausdorffdistance_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_hausdorffdistance_densify_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String, densify_frac Float64) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_frechetdistance_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_frechetdistance_densify_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String, densify_frac Float64) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_z_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_nrings_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Int32
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_perimeter_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_numpoints_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Int32
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_makepoint_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (x Float64, y Float64) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_makepoint3d_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (x Float64, y Float64, z Float64) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_makepolygon_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (shell String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_makeline_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_closestpoint_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_shortestline_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_sharedpaths_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_node_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_addpoint_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (line String, point String, pos Int32) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_removepoint_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (line String, pos Int32) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_setpoint_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (line String, pos Int32, point String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_convexhull_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_boundary_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_reverse_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_normalize_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_geometryn_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, n Int32) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_symdifference_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_exteriorring_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_interiorringn_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, n Int32) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_pointn_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, n Int32) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_minimumboundingcircle_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_snap_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String, tolerance Float64) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_offsetcurve_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, distance Float64) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_linmerge_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_polygonize_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_delaunaytriangles_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, tolerance Float64, only_edges Int32) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_voronoidiagram_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, tolerance Float64, only_edges Int32) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

-- ---------------------------------------------------------------------------
-- Overlay
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION st_union_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_intersection_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_difference_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_unaryunion_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

-- Pseudo-aggregate functions: take Array(String) of WKBs, used with groupArray().
CREATE OR REPLACE FUNCTION st_union_agg_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geoms Array(String)) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_collect_agg_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geoms Array(String)) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_extent_agg_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geoms Array(String)) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_makeline_agg_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geoms Array(String)) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_convexhull_agg_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geoms Array(String)) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_clusterintersecting_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

-- COLUMNAR_V1 variants of aggregate functions (COL_COMPLEX Array(String) input).
CREATE OR REPLACE FUNCTION st_union_agg
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geoms Array(String)) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC
SETTINGS is_aggregate = 1;

CREATE OR REPLACE FUNCTION st_collect_agg
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geoms Array(String)) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_extent_agg
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geoms Array(String)) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC
SETTINGS is_aggregate = 1;

CREATE OR REPLACE FUNCTION st_makeline_agg
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geoms Array(String)) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC
SETTINGS is_aggregate = 1;

CREATE OR REPLACE FUNCTION st_convexhull_agg
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geoms Array(String)) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC
SETTINGS is_aggregate = 1;

-- ---------------------------------------------------------------------------
-- Processing
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION st_buffer_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, radius Float64) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_buffer_params_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, radius Float64, params String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_simplify_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, tolerance Float64) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_makevalid_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_segmentize_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, max_length Float64) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_subdivide_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, max_vertices Int32) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

-- ---------------------------------------------------------------------------
-- Transforms
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION st_translate_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, dx Float64, dy Float64) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_scale_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, xf Float64, yf Float64) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_transform_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, srid UInt32) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_transform_proj_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, pipeline String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

-- ---------------------------------------------------------------------------
-- COLUMNAR_V1 — canonical names (no suffix)
-- ---------------------------------------------------------------------------

-- Predicates (2 geometry args) — COLUMNAR_V1, is_spatial_predicate = 1
CREATE OR REPLACE FUNCTION st_contains
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI COLUMNAR_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1;

CREATE OR REPLACE FUNCTION st_intersects
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI COLUMNAR_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1;

CREATE OR REPLACE FUNCTION st_touches
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI COLUMNAR_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1;

CREATE OR REPLACE FUNCTION st_within
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI COLUMNAR_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1;

CREATE OR REPLACE FUNCTION st_crosses
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI COLUMNAR_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1;

CREATE OR REPLACE FUNCTION st_overlaps
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI COLUMNAR_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1;

CREATE OR REPLACE FUNCTION st_equals
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI COLUMNAR_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1;

CREATE OR REPLACE FUNCTION st_covers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI COLUMNAR_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1;

CREATE OR REPLACE FUNCTION st_coveredby
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI COLUMNAR_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1;

CREATE OR REPLACE FUNCTION st_containsproperly
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI COLUMNAR_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1;

-- st_disjoint: inverted bbox logic — no spatial predicate flag
CREATE OR REPLACE FUNCTION st_disjoint
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_dwithin
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String, dist Float64) RETURNS UInt8
ABI COLUMNAR_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1, spatial_expand_arg = 2;

-- Scalar Float64 (1 geometry arg)
CREATE OR REPLACE FUNCTION st_x
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Float64
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_y
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Float64
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_z
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Float64
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_area
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Float64
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_length
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Float64
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_perimeter
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Float64
ABI COLUMNAR_V1
DETERMINISTIC;

-- Scalar Float64 (2 geometry args)
CREATE OR REPLACE FUNCTION st_distance
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS Float64
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_hausdorffdistance
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS Float64
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_frechetdistance
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS Float64
ABI COLUMNAR_V1
DETERMINISTIC;

-- Scalar Int32 (1 geometry arg)
CREATE OR REPLACE FUNCTION st_srid
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Int32
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_npoints
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Int32
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_numpoints
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Int32
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_numgeometries
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Int32
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_numinteriorrings
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Int32
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_nrings
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Int32
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_dimension
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Int32
ABI COLUMNAR_V1
DETERMINISTIC;

-- Scalar UInt8 (1 geometry arg)
CREATE OR REPLACE FUNCTION st_isvalid
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS UInt8
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_isempty
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS UInt8
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_issimple
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS UInt8
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_isring
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS UInt8
ABI COLUMNAR_V1
DETERMINISTIC;

-- String output (1 geometry arg)
CREATE OR REPLACE FUNCTION st_astext
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_asewkt
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_geometrytype
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_isvalidreason
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

-- String output (2 geometry args)
CREATE OR REPLACE FUNCTION st_relate
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

-- Geometry output (2 geometry args) → Nullable(String)
CREATE OR REPLACE FUNCTION st_intersection
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_union
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_difference
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_makeline
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_symdifference
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_collect
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_closestpoint
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_shortestline
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_sharedpaths
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

-- Geometry output (1 geometry arg) → Nullable(String)
CREATE OR REPLACE FUNCTION st_convexhull
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_envelope
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_centroid
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_makevalid
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_boundary
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_unaryunion
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_reverse
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_normalize
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_node
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_linmerge
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_polygonize
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_clusterintersecting
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_extent
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_interiorpoint
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_expand
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, units Float64) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

-- ---------------------------------------------------------------------------
-- COLUMNAR_V1 — additional functions
-- ---------------------------------------------------------------------------

-- Scalar Float64 (densify variants)
CREATE OR REPLACE FUNCTION st_hausdorffdistance_densify
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String, densify_frac Float64) RETURNS Float64
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_frechetdistance_densify
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String, densify_frac Float64) RETURNS Float64
ABI COLUMNAR_V1
DETERMINISTIC;

-- Predicate (3 args)
CREATE OR REPLACE FUNCTION st_relate_pattern
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String, pattern String) RETURNS UInt8
ABI COLUMNAR_V1
DETERMINISTIC;

-- I/O
CREATE OR REPLACE FUNCTION geos_version
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS () RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_geomfromgeojson
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geojson String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

-- Geometry constructors → Nullable(String)
CREATE OR REPLACE FUNCTION st_startpoint
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_endpoint
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_makebox2d
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (low_left String, up_right String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_geometryn
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, n Int32) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_exteriorring
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_interiorringn
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, n Int32) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_pointn
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, n Int32) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_minimumboundingcircle
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_snap
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String, tolerance Float64) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_offsetcurve
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, distance Float64) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_delaunaytriangles
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, tolerance Float64, only_edges Int32) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_voronoidiagram
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, tolerance Float64, only_edges Int32) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_makepoint
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (x Float64, y Float64) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_makepoint3d
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (x Float64, y Float64, z Float64) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_makepolygon
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (shell String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_addpoint
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (line String, point String, pos Int32) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_removepoint
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (line String, pos Int32) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_setpoint
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (line String, pos Int32, point String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

-- Processing → Nullable(String)
CREATE OR REPLACE FUNCTION st_buffer
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, radius Float64) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_buffer_params
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, radius Float64, params String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_simplify
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, tolerance Float64) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_segmentize
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, max_length Float64) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_subdivide
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, max_vertices Int32) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

-- Transforms → Nullable(String)
CREATE OR REPLACE FUNCTION st_translate
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, dx Float64, dy Float64) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_scale
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, xf Float64, yf Float64) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_setsrid
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, srid UInt32) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_transform
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, srid UInt32) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_transform_proj
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, pipeline String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

-- ---------------------------------------------------------------------------
-- st_knn: k-nearest-neighbour spatial query
-- ---------------------------------------------------------------------------
-- Returns Array(Tuple(UInt64, Float64)): k (0-based index, distance) pairs
-- from `candidates`, sorted by distance ascending.
-- When `candidates` is a const column (scalar subquery), a GEOS STRtree is
-- built once per batch and queried once per row.
--
-- Example:
--   WITH all_bldgs AS (SELECT groupArray(b_boundary) AS arr FROM building)
--   SELECT t_tripkey, st_knn(t_pickuploc, (SELECT arr FROM all_bldgs), 5)
--   FROM trip

CREATE OR REPLACE FUNCTION st_knn
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (query String, candidates Array(String), k UInt32) RETURNS Array(Tuple(UInt64, Float64))
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_knn_cb AS (a, b, c) -> st_knn(a, b, c);

-- ---------------------------------------------------------------------------
-- Remaining aliases — no COLUMNAR_V1 implementation, delegate to _mp
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION geos_log_test AS (a, b) -> geos_log_test_mp(a, b);
CREATE OR REPLACE FUNCTION geos_test_exception AS (a) -> geos_test_exception_mp(a);
CREATE OR REPLACE FUNCTION st_geomfromchlinestring AS (a) -> st_geomfromchlinestring_mp(a);
CREATE OR REPLACE FUNCTION st_geomfromchmultipolygon AS (a) -> st_geomfromchmultipolygon_mp(a);
CREATE OR REPLACE FUNCTION st_geomfromchpoint AS (a) -> st_geomfromchpoint_mp(a);
CREATE OR REPLACE FUNCTION st_geomfromchpolygon AS (a) -> st_geomfromchpolygon_mp(a);
CREATE OR REPLACE FUNCTION st_geomfromtext AS (a) -> st_geomfromtext_mp(a);
CREATE OR REPLACE FUNCTION st_geomfromwkb AS (a) -> st_geomfromwkb_mp(a);
CREATE OR REPLACE FUNCTION st_intersects_extent AS (a, b) -> st_intersects_extent_mp(a, b);

-- ---------------------------------------------------------------------------
-- Buffers wire format tests
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION st_x_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_y_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_area_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_contains_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

-- ---------------------------------------------------------------------------
-- Remaining Buffers wire format registrations (String types)
-- ---------------------------------------------------------------------------

-- Float64 (1 geometry arg)

CREATE OR REPLACE FUNCTION st_z_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_length_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_perimeter_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_distance_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_hausdorffdistance_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_frechetdistance_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_hausdorffdistance_densify_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String, densify_frac Float64) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_frechetdistance_densify_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String, densify_frac Float64) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_dwithin_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String, dist Float64) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary', is_spatial_predicate = 1, spatial_expand_arg = 2;

-- UInt8 (1 geometry arg)

CREATE OR REPLACE FUNCTION st_isvalid_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_isempty_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_issimple_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_isring_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

-- Int32 (1 geometry arg)

CREATE OR REPLACE FUNCTION st_srid_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS Int32
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_npoints_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS Int32
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_numpoints_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS Int32
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_numgeometries_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS Int32
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_numinteriorrings_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS Int32
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_nrings_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS Int32
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_dimension_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS Int32
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

-- String output (1 geometry arg)

CREATE OR REPLACE FUNCTION st_astext_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_asewkt_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_geometrytype_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_isvalidreason_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

-- UInt8 (2 geometry args)

CREATE OR REPLACE FUNCTION st_intersects_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary', is_spatial_predicate = 1;

CREATE OR REPLACE FUNCTION st_touches_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary', is_spatial_predicate = 1;

CREATE OR REPLACE FUNCTION st_within_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary', is_spatial_predicate = 1;

CREATE OR REPLACE FUNCTION st_crosses_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary', is_spatial_predicate = 1;

CREATE OR REPLACE FUNCTION st_overlaps_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary', is_spatial_predicate = 1;

CREATE OR REPLACE FUNCTION st_disjoint_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary', is_spatial_predicate = 1;

CREATE OR REPLACE FUNCTION st_equals_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary', is_spatial_predicate = 1;

CREATE OR REPLACE FUNCTION st_covers_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary', is_spatial_predicate = 1;

CREATE OR REPLACE FUNCTION st_coveredby_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary', is_spatial_predicate = 1;

CREATE OR REPLACE FUNCTION st_containsproperly_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary', is_spatial_predicate = 1;

-- String (2 geometry args) — relation output

CREATE OR REPLACE FUNCTION st_relate_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_relate_pattern_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String, pattern String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

-- Nullable(String) (2 geometry args) — geometry output

CREATE OR REPLACE FUNCTION st_intersection_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_union_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_difference_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_makeline_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_symdifference_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_collect_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_closestpoint_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_shortestline_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_sharedpaths_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

-- String output (1 geometry arg) — geometry-returning

CREATE OR REPLACE FUNCTION st_convexhull_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_envelope_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_centroid_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_makevalid_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_boundary_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_unaryunion_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_reverse_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_normalize_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_node_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_linmerge_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_polygonize_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_clusterintersecting_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_extent_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_interiorpoint_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_expand_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_minimumboundingcircle_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_snap_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_offsetcurve_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_delaunaytriangles_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_voronoidiagram_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_buffer_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_simplify_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_segmentize_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_subdivide_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_translate_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_scale_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

-- Other functions

CREATE OR REPLACE FUNCTION st_makepoint_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (x Float64, y Float64) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_makepoint3d_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (x Float64, y Float64, z Float64) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_makepolygon_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_addpoint_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_removepoint_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_setpoint_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_setsrid_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_transform_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_geomfromtext_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkt String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_geomfromwkb_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_transform_proj_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_buffer_params_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_geomfromgeojson_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (s String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_startpoint_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_endpoint_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_makebox2d_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_geometryn_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_exteriorring_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_interiorringn_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_pointn_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION geos_version_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (s String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';

CREATE OR REPLACE FUNCTION st_collect_agg_cb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geoms Array(String)) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'ColumnBinary';


-- ---------------------------------------------------------------------------
-- _col aliases for COLUMNAR_V1 (benchmark compat)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION st_x_col AS (a) -> st_x(a);
CREATE OR REPLACE FUNCTION st_y_col AS (a) -> st_y(a);
CREATE OR REPLACE FUNCTION st_area_col AS (a) -> st_area(a);
CREATE OR REPLACE FUNCTION st_distance_col AS (a, b) -> st_distance(a, b);
CREATE OR REPLACE FUNCTION st_dwithin_col AS (a, b, c) -> st_dwithin(a, b, c);
CREATE OR REPLACE FUNCTION st_intersects_col AS (a, b) -> st_intersects(a, b);
CREATE OR REPLACE FUNCTION st_within_col AS (a, b) -> st_within(a, b);
CREATE OR REPLACE FUNCTION st_length_col AS (a) -> st_length(a);
CREATE OR REPLACE FUNCTION st_makeline_col AS (a, b) -> st_makeline(a, b);
CREATE OR REPLACE FUNCTION st_convexhull_col AS (a) -> st_convexhull(a);
CREATE OR REPLACE FUNCTION st_intersection_col AS (a, b) -> st_intersection(a, b);

-- ---------------------------------------------------------------------------
-- Buffers wire format (_buffers suffix)
-- serialization_format = 'Buffers' — Native CH columnar (NativeWriter format)
-- ---------------------------------------------------------------------------

-- Predicates (2 geometry args)

CREATE OR REPLACE FUNCTION st_contains_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_intersects_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_touches_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_within_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_crosses_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_overlaps_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_disjoint_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_equals_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_covers_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_coveredby_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_containsproperly_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_dwithin_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String, dist Float64) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

-- Float64 (1 geometry arg)

CREATE OR REPLACE FUNCTION st_x_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_y_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_z_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_area_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_length_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_perimeter_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_distance_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_hausdorffdistance_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_frechetdistance_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_hausdorffdistance_densify_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String, densify_frac Float64) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_frechetdistance_densify_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String, densify_frac Float64) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

-- UInt8 (1 geometry arg)

CREATE OR REPLACE FUNCTION st_isvalid_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_isempty_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_issimple_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_isring_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

-- Int32 (1 geometry arg)

CREATE OR REPLACE FUNCTION st_srid_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS Int32
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_npoints_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS Int32
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_numpoints_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS Int32
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_numgeometries_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS Int32
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_numinteriorrings_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS Int32
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_nrings_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS Int32
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_dimension_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS Int32
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

-- String output (1 geometry arg)

CREATE OR REPLACE FUNCTION st_astext_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_asewkt_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_geometrytype_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_isvalidreason_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_relate_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

-- Geometry output (1 arg)

CREATE OR REPLACE FUNCTION st_centroid_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_envelope_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_convexhull_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_extent_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_startpoint_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_endpoint_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_interiorpoint_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_makevalid_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_boundary_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_unaryunion_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_reverse_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_normalize_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

-- Geometry output (2 args)

CREATE OR REPLACE FUNCTION st_makeline_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_intersection_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_union_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_difference_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_symdifference_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_collect_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_closestpoint_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_shortestline_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_makebox2d_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

-- Geometry output (geom + scalar)

CREATE OR REPLACE FUNCTION st_expand_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String, delta Float64) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_buffer_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String, radius Float64) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_simplify_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String, tolerance Float64) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';

CREATE OR REPLACE FUNCTION st_setsrid_buffers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geometry String, srid Int32) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'Buffers';
