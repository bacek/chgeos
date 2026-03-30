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

CREATE OR REPLACE FUNCTION geos_version
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS () RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

-- ---------------------------------------------------------------------------
-- Diagnostics  (MsgPack — intentional)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION geos_log_test
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (level UInt32, msg String) RETURNS UInt32
ABI BUFFERED_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION geos_test_exception
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (msg String) RETURNS UInt32
ABI BUFFERED_V1
DETERMINISTIC;

-- ---------------------------------------------------------------------------
-- I/O
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION st_geomfromtext
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkt String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_geomfromgeojson
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (geojson String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_geomfromwkb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

-- CH native type converters  (MsgPack — Tuple/Array RowBinary layout not implemented)
CREATE OR REPLACE FUNCTION st_geomfromchpoint
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (pt Point) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_geomfromchlinestring
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (ls LineString) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_geomfromchpolygon
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (poly Polygon) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_geomfromchmultipolygon
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (mp MultiPolygon) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC;

CREATE OR REPLACE FUNCTION st_astext
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_asewkt
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

-- ---------------------------------------------------------------------------
-- Accessors
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION st_x
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_y
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_srid
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Int32
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_setsrid
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, srid UInt32) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_npoints
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Int32
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_area
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_centroid
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

-- ---------------------------------------------------------------------------
-- Constructors
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION st_envelope
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_extent
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_expand
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, units Float64) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_makebox2d
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (low_left String, up_right String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_startpoint
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_endpoint
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_collect
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

-- ---------------------------------------------------------------------------
-- Predicates
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION st_contains
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1, serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_intersects
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1, serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_touches
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1, serialization_format = 'RowBinary';

-- st_dwithin: distance-based, bbox pruning is NOT safe (false negatives possible)
CREATE OR REPLACE FUNCTION st_dwithin
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String, dist Float64) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_intersects_extent
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1, serialization_format = 'RowBinary';

-- Same function, RowBinary serialization — kept for backwards compatibility.
CREATE OR REPLACE FUNCTION st_intersects_extent_rb
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary', is_spatial_predicate = 1;

CREATE OR REPLACE FUNCTION st_within
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1, serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_crosses
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1, serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_overlaps
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1, serialization_format = 'RowBinary';

-- st_disjoint: inverted bbox logic — pruning not safe
CREATE OR REPLACE FUNCTION st_disjoint
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_equals
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1, serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_covers
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1, serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_coveredby
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1, serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_containsproperly
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1, serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_relate
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_relate_pattern
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String, pattern String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_distance
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_length
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_isvalid
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_isempty
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_issimple
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_isring
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_geometrytype
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_numgeometries
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Int32
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_dimension
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Int32
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_interiorpoint
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_isvalidreason
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_numinteriorrings
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Int32
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_hausdorffdistance
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_hausdorffdistance_densify
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String, densify_frac Float64) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_frechetdistance
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_frechetdistance_densify
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String, densify_frac Float64) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_z
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_nrings
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Int32
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_perimeter
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Float64
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_numpoints
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS Int32
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_makepoint
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (x Float64, y Float64) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_makepoint3d
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (x Float64, y Float64, z Float64) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_makepolygon
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (shell String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_makeline
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_closestpoint
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_shortestline
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_sharedpaths
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_node
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_addpoint
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (line String, point String, pos Int32) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_removepoint
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (line String, pos Int32) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_setpoint
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (line String, pos Int32, point String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_convexhull
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_boundary
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_reverse
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_normalize
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_geometryn
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, n Int32) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_symdifference
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_exteriorring
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_interiorringn
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, n Int32) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_pointn
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, n Int32) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_minimumboundingcircle
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_snap
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String, tolerance Float64) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_offsetcurve
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, distance Float64) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_linmerge
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_polygonize
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_delaunaytriangles
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, tolerance Float64, only_edges Int32) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_voronoidiagram
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, tolerance Float64, only_edges Int32) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

-- ---------------------------------------------------------------------------
-- Overlay
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION st_union
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_intersection
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_difference
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_unaryunion
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

-- Aggregate functions  (MsgPack — RowBinary Array(String) unpacking not implemented)
CREATE OR REPLACE FUNCTION st_union_agg
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS is_aggregate = 1;

CREATE OR REPLACE FUNCTION st_collect_agg
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS is_aggregate = 1;

CREATE OR REPLACE FUNCTION st_extent_agg
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS is_aggregate = 1;

CREATE OR REPLACE FUNCTION st_makeline_agg
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS is_aggregate = 1;

CREATE OR REPLACE FUNCTION st_convexhull_agg
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS is_aggregate = 1;

CREATE OR REPLACE FUNCTION st_clusterintersecting
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

-- ---------------------------------------------------------------------------
-- Processing
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION st_buffer
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, radius Float64) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_buffer_params
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, radius Float64, params String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_simplify
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, tolerance Float64) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_makevalid
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_segmentize
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, max_length Float64) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_subdivide
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, max_vertices Int32) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

-- ---------------------------------------------------------------------------
-- Transforms
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION st_translate
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, dx Float64, dy Float64) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_scale
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, xf Float64, yf Float64) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_transform
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, srid Int32) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';

CREATE OR REPLACE FUNCTION st_transform_proj
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String, pipeline String) RETURNS String
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS serialization_format = 'RowBinary';
