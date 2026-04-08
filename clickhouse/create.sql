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
-- (RowBinary Array(String) unpacking not implemented — use MsgPack path.)
CREATE OR REPLACE FUNCTION st_union_agg_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geoms Array(String)) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_collect_agg_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geoms Array(String)) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_extent_agg_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geoms Array(String)) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_makeline_agg_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geoms Array(String)) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_convexhull_agg_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geoms Array(String)) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_clusterintersecting_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

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
-- COLUMNAR_V1 variants (_col suffix)
-- Constants passed once (not N×), no RowBinary serialization overhead.
-- Use these for hot paths / analytical queries.
-- ---------------------------------------------------------------------------

-- Predicates (2 geometry args)
CREATE OR REPLACE FUNCTION st_contains_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI COLUMNAR_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1;

CREATE OR REPLACE FUNCTION st_intersects_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI COLUMNAR_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1;

CREATE OR REPLACE FUNCTION st_touches_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI COLUMNAR_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1;

CREATE OR REPLACE FUNCTION st_within_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI COLUMNAR_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1;

CREATE OR REPLACE FUNCTION st_crosses_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI COLUMNAR_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1;

CREATE OR REPLACE FUNCTION st_overlaps_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI COLUMNAR_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1;

CREATE OR REPLACE FUNCTION st_equals_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI COLUMNAR_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1;

CREATE OR REPLACE FUNCTION st_covers_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI COLUMNAR_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1;

CREATE OR REPLACE FUNCTION st_coveredby_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI COLUMNAR_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1;

CREATE OR REPLACE FUNCTION st_containsproperly_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI COLUMNAR_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1;

-- st_disjoint: inverted bbox logic — pruning not safe
CREATE OR REPLACE FUNCTION st_disjoint_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI COLUMNAR_V1
DETERMINISTIC;

-- st_dwithin: distance-based, bbox pruning NOT safe
CREATE OR REPLACE FUNCTION st_dwithin_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String, dist Float64) RETURNS UInt8
ABI COLUMNAR_V1
DETERMINISTIC;

-- Predicates (1 geometry arg) → UInt8
CREATE OR REPLACE FUNCTION st_isvalid_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS UInt8
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_isempty_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS UInt8
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_issimple_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS UInt8
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_isring_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS UInt8
ABI COLUMNAR_V1
DETERMINISTIC;

-- Scalar Int32 (1 geometry arg)
CREATE OR REPLACE FUNCTION st_srid_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Int32
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_npoints_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Int32
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_numpoints_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Int32
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_numgeometries_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Int32
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_numinteriorrings_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Int32
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_nrings_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Int32
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_dimension_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Int32
ABI COLUMNAR_V1
DETERMINISTIC;

-- String output (1 geometry arg)
CREATE OR REPLACE FUNCTION st_astext_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_asewkt_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_geometrytype_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_isvalidreason_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

-- String output (2 geometry args)
CREATE OR REPLACE FUNCTION st_relate_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI COLUMNAR_V1
DETERMINISTIC;

-- Scalar Float64 (1 geometry arg)
CREATE OR REPLACE FUNCTION st_x_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Float64
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_y_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Float64
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_z_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Float64
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_area_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Float64
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_length_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Float64
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_perimeter_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Float64
ABI COLUMNAR_V1
DETERMINISTIC;

-- Scalar Float64 (2 geometry args)
CREATE OR REPLACE FUNCTION st_distance_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS Float64
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_hausdorffdistance_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS Float64
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_frechetdistance_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS Float64
ABI COLUMNAR_V1
DETERMINISTIC;

-- Geometry output (2 geometry args)
CREATE OR REPLACE FUNCTION st_intersection_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS Nullable(String)
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_union_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS Nullable(String)
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_difference_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS Nullable(String)
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_makeline_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS Nullable(String)
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_symdifference_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS Nullable(String)
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_collect_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS Nullable(String)
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_closestpoint_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS Nullable(String)
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_shortestline_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS Nullable(String)
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_sharedpaths_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS Nullable(String)
ABI COLUMNAR_V1
DETERMINISTIC;

-- Geometry output (1 geometry arg)
CREATE OR REPLACE FUNCTION st_convexhull_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Nullable(String)
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_envelope_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Nullable(String)
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_centroid_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Nullable(String)
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_makevalid_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Nullable(String)
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_boundary_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Nullable(String)
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_unaryunion_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Nullable(String)
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_reverse_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Nullable(String)
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_normalize_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Nullable(String)
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_node_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Nullable(String)
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_linmerge_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Nullable(String)
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_polygonize_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Nullable(String)
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_clusterintersecting_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Nullable(String)
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_extent_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Nullable(String)
ABI COLUMNAR_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_interiorpoint_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Nullable(String)
ABI COLUMNAR_V1
DETERMINISTIC;

-- ---------------------------------------------------------------------------
-- Canonical aliases (unsuffixed PostGIS-compatible names)
-- Points to _col (COLUMNAR_V1) when available, _mp otherwise.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION geos_log_test AS (a, b) -> geos_log_test_mp(a, b);
CREATE OR REPLACE FUNCTION geos_test_exception AS (a) -> geos_test_exception_mp(a);
CREATE OR REPLACE FUNCTION geos_version AS () -> geos_version_mp();
CREATE OR REPLACE FUNCTION st_addpoint AS (a, b, c) -> st_addpoint_mp(a, b, c);
CREATE OR REPLACE FUNCTION st_area AS (a) -> st_area_col(a);
CREATE OR REPLACE FUNCTION st_asewkt AS (a) -> st_asewkt_col(a);
CREATE OR REPLACE FUNCTION st_astext AS (a) -> st_astext_col(a);
CREATE OR REPLACE FUNCTION st_boundary AS (a) -> st_boundary_col(a);
CREATE OR REPLACE FUNCTION st_buffer AS (a, b) -> st_buffer_mp(a, b);
CREATE OR REPLACE FUNCTION st_buffer_params AS (a, b, c) -> st_buffer_params_mp(a, b, c);
CREATE OR REPLACE FUNCTION st_centroid AS (a) -> st_centroid_col(a);
CREATE OR REPLACE FUNCTION st_closestpoint AS (a, b) -> st_closestpoint_col(a, b);
CREATE OR REPLACE FUNCTION st_clusterintersecting AS (a) -> st_clusterintersecting_col(a);
CREATE OR REPLACE FUNCTION st_collect AS (a, b) -> st_collect_col(a, b);
CREATE OR REPLACE FUNCTION st_collect_agg AS (a) -> st_collect_agg_mp(a);
CREATE OR REPLACE FUNCTION st_contains AS (a, b) -> st_contains_col(a, b);
CREATE OR REPLACE FUNCTION st_containsproperly AS (a, b) -> st_containsproperly_col(a, b);
CREATE OR REPLACE FUNCTION st_convexhull AS (a) -> st_convexhull_col(a);
CREATE OR REPLACE FUNCTION st_convexhull_agg AS (a) -> st_convexhull_agg_mp(a);
CREATE OR REPLACE FUNCTION st_coveredby AS (a, b) -> st_coveredby_col(a, b);
CREATE OR REPLACE FUNCTION st_covers AS (a, b) -> st_covers_col(a, b);
CREATE OR REPLACE FUNCTION st_crosses AS (a, b) -> st_crosses_col(a, b);
CREATE OR REPLACE FUNCTION st_delaunaytriangles AS (a, b, c) -> st_delaunaytriangles_mp(a, b, c);
CREATE OR REPLACE FUNCTION st_difference AS (a, b) -> st_difference_col(a, b);
CREATE OR REPLACE FUNCTION st_dimension AS (a) -> st_dimension_col(a);
CREATE OR REPLACE FUNCTION st_disjoint AS (a, b) -> st_disjoint_col(a, b);
CREATE OR REPLACE FUNCTION st_distance AS (a, b) -> st_distance_col(a, b);
CREATE OR REPLACE FUNCTION st_dwithin AS (a, b, c) -> st_dwithin_col(a, b, c);
CREATE OR REPLACE FUNCTION st_endpoint AS (a) -> st_endpoint_mp(a);
CREATE OR REPLACE FUNCTION st_envelope AS (a) -> st_envelope_col(a);
CREATE OR REPLACE FUNCTION st_equals AS (a, b) -> st_equals_col(a, b);
CREATE OR REPLACE FUNCTION st_expand AS (a, b) -> st_expand_mp(a, b);
CREATE OR REPLACE FUNCTION st_extent AS (a) -> st_extent_col(a);
CREATE OR REPLACE FUNCTION st_extent_agg AS (a) -> st_extent_agg_mp(a);
CREATE OR REPLACE FUNCTION st_exteriorring AS (a) -> st_exteriorring_mp(a);
CREATE OR REPLACE FUNCTION st_frechetdistance AS (a, b) -> st_frechetdistance_col(a, b);
CREATE OR REPLACE FUNCTION st_frechetdistance_densify AS (a, b, c) -> st_frechetdistance_densify_mp(a, b, c);
CREATE OR REPLACE FUNCTION st_geometryn AS (a, b) -> st_geometryn_mp(a, b);
CREATE OR REPLACE FUNCTION st_geometrytype AS (a) -> st_geometrytype_col(a);
CREATE OR REPLACE FUNCTION st_geomfromchlinestring AS (a) -> st_geomfromchlinestring_mp(a);
CREATE OR REPLACE FUNCTION st_geomfromchmultipolygon AS (a) -> st_geomfromchmultipolygon_mp(a);
CREATE OR REPLACE FUNCTION st_geomfromchpoint AS (a) -> st_geomfromchpoint_mp(a);
CREATE OR REPLACE FUNCTION st_geomfromchpolygon AS (a) -> st_geomfromchpolygon_mp(a);
CREATE OR REPLACE FUNCTION st_geomfromgeojson AS (a) -> st_geomfromgeojson_mp(a);
CREATE OR REPLACE FUNCTION st_geomfromtext AS (a) -> st_geomfromtext_mp(a);
CREATE OR REPLACE FUNCTION st_geomfromwkb AS (a) -> st_geomfromwkb_mp(a);
CREATE OR REPLACE FUNCTION st_hausdorffdistance AS (a, b) -> st_hausdorffdistance_col(a, b);
CREATE OR REPLACE FUNCTION st_hausdorffdistance_densify AS (a, b, c) -> st_hausdorffdistance_densify_mp(a, b, c);
CREATE OR REPLACE FUNCTION st_interiorpoint AS (a) -> st_interiorpoint_col(a);
CREATE OR REPLACE FUNCTION st_interiorringn AS (a, b) -> st_interiorringn_mp(a, b);
CREATE OR REPLACE FUNCTION st_intersection AS (a, b) -> st_intersection_col(a, b);
CREATE OR REPLACE FUNCTION st_intersects AS (a, b) -> st_intersects_col(a, b);
CREATE OR REPLACE FUNCTION st_intersects_extent AS (a, b) -> st_intersects_extent_mp(a, b);
CREATE OR REPLACE FUNCTION st_isempty AS (a) -> st_isempty_col(a);
CREATE OR REPLACE FUNCTION st_isring AS (a) -> st_isring_col(a);
CREATE OR REPLACE FUNCTION st_issimple AS (a) -> st_issimple_col(a);
CREATE OR REPLACE FUNCTION st_isvalid AS (a) -> st_isvalid_col(a);
CREATE OR REPLACE FUNCTION st_isvalidreason AS (a) -> st_isvalidreason_col(a);
CREATE OR REPLACE FUNCTION st_length AS (a) -> st_length_col(a);
CREATE OR REPLACE FUNCTION st_linmerge AS (a) -> st_linmerge_col(a);
CREATE OR REPLACE FUNCTION st_makebox2d AS (a, b) -> st_makebox2d_mp(a, b);
CREATE OR REPLACE FUNCTION st_makeline AS (a, b) -> st_makeline_col(a, b);
CREATE OR REPLACE FUNCTION st_makeline_agg AS (a) -> st_makeline_agg_mp(a);
CREATE OR REPLACE FUNCTION st_makepoint AS (a, b) -> st_makepoint_mp(a, b);
CREATE OR REPLACE FUNCTION st_makepoint3d AS (a, b, c) -> st_makepoint3d_mp(a, b, c);
CREATE OR REPLACE FUNCTION st_makepolygon AS (a) -> st_makepolygon_mp(a);
CREATE OR REPLACE FUNCTION st_makevalid AS (a) -> st_makevalid_col(a);
CREATE OR REPLACE FUNCTION st_minimumboundingcircle AS (a) -> st_minimumboundingcircle_mp(a);
CREATE OR REPLACE FUNCTION st_node AS (a) -> st_node_col(a);
CREATE OR REPLACE FUNCTION st_normalize AS (a) -> st_normalize_col(a);
CREATE OR REPLACE FUNCTION st_npoints AS (a) -> st_npoints_col(a);
CREATE OR REPLACE FUNCTION st_nrings AS (a) -> st_nrings_col(a);
CREATE OR REPLACE FUNCTION st_numgeometries AS (a) -> st_numgeometries_col(a);
CREATE OR REPLACE FUNCTION st_numinteriorrings AS (a) -> st_numinteriorrings_col(a);
CREATE OR REPLACE FUNCTION st_numpoints AS (a) -> st_numpoints_col(a);
CREATE OR REPLACE FUNCTION st_offsetcurve AS (a, b) -> st_offsetcurve_mp(a, b);
CREATE OR REPLACE FUNCTION st_overlaps AS (a, b) -> st_overlaps_col(a, b);
CREATE OR REPLACE FUNCTION st_perimeter AS (a) -> st_perimeter_col(a);
CREATE OR REPLACE FUNCTION st_pointn AS (a, b) -> st_pointn_mp(a, b);
CREATE OR REPLACE FUNCTION st_polygonize AS (a) -> st_polygonize_col(a);
CREATE OR REPLACE FUNCTION st_relate AS (a, b) -> st_relate_col(a, b);
CREATE OR REPLACE FUNCTION st_relate_pattern AS (a, b, c) -> st_relate_pattern_mp(a, b, c);
CREATE OR REPLACE FUNCTION st_removepoint AS (a, b) -> st_removepoint_mp(a, b);
CREATE OR REPLACE FUNCTION st_reverse AS (a) -> st_reverse_col(a);
CREATE OR REPLACE FUNCTION st_scale AS (a, b, c) -> st_scale_mp(a, b, c);
CREATE OR REPLACE FUNCTION st_segmentize AS (a, b) -> st_segmentize_mp(a, b);
CREATE OR REPLACE FUNCTION st_setpoint AS (a, b, c) -> st_setpoint_mp(a, b, c);
CREATE OR REPLACE FUNCTION st_setsrid AS (a, b) -> st_setsrid_mp(a, b);
CREATE OR REPLACE FUNCTION st_sharedpaths AS (a, b) -> st_sharedpaths_col(a, b);
CREATE OR REPLACE FUNCTION st_shortestline AS (a, b) -> st_shortestline_col(a, b);
CREATE OR REPLACE FUNCTION st_simplify AS (a, b) -> st_simplify_mp(a, b);
CREATE OR REPLACE FUNCTION st_snap AS (a, b, c) -> st_snap_mp(a, b, c);
CREATE OR REPLACE FUNCTION st_srid AS (a) -> st_srid_col(a);
CREATE OR REPLACE FUNCTION st_startpoint AS (a) -> st_startpoint_mp(a);
CREATE OR REPLACE FUNCTION st_subdivide AS (a, b) -> st_subdivide_mp(a, b);
CREATE OR REPLACE FUNCTION st_symdifference AS (a, b) -> st_symdifference_col(a, b);
CREATE OR REPLACE FUNCTION st_touches AS (a, b) -> st_touches_col(a, b);
CREATE OR REPLACE FUNCTION st_transform AS (a, b) -> st_transform_mp(a, b);
CREATE OR REPLACE FUNCTION st_transform_proj AS (a, b) -> st_transform_proj_mp(a, b);
CREATE OR REPLACE FUNCTION st_translate AS (a, b, c) -> st_translate_mp(a, b, c);
CREATE OR REPLACE FUNCTION st_unaryunion AS (a) -> st_unaryunion_col(a);
CREATE OR REPLACE FUNCTION st_union AS (a, b) -> st_union_col(a, b);
CREATE OR REPLACE FUNCTION st_union_agg AS (a) -> st_union_agg_mp(a);
CREATE OR REPLACE FUNCTION st_voronoidiagram AS (a, b, c) -> st_voronoidiagram_mp(a, b, c);
CREATE OR REPLACE FUNCTION st_within AS (a, b) -> st_within_col(a, b);
CREATE OR REPLACE FUNCTION st_x AS (a) -> st_x_col(a);
CREATE OR REPLACE FUNCTION st_y AS (a) -> st_y_col(a);
CREATE OR REPLACE FUNCTION st_z AS (a) -> st_z_col(a);
