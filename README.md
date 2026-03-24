# chgeos

PostGIS-compatible spatial functions for ClickHouse, delivered as a WebAssembly UDF module powered by [GEOS](https://libgeos.org/) 3.12+.

## Disclaimer

This is my pet-project with `-Ofun` mentality.
* Just a hobby, won't be big and professional.
* I'm testing how far I can push Claude.
* I'm (re-)learning low-level optimization I haven't done in 15+ years.
* I'm extracting useful pieces out of this project into upstream projects. For example non-copy `std::span` handing in `msgpack23` and exceptions support in `wasmtime`.
* This is nowhere near any useful application. For many reasons. Especially because CH<->UDF interaction is very limited. Basically it's one way street at the moment and any "spatial aware" query engine, which can use Parquet file metadata will be faster. Much faster. Order of magnitude faster.

Having said that, I'm not saying it will never be useful. 

## Motivation

ClickHouse is fast. If you need to crunch billions of rows, it's the right tool. But the moment you ask "can it do spatial analytics?" the answer is: technically yes, practically no.

You get `Point`, H3, S2. What you don't get is anything resembling a real spatial function library. No `ST_Area`. No `ST_Buffer`. No `ST_Intersects`. No `ST_Within`. The entire PostGIS vocabulary — functions that GIS engineers have been using since 2001 — is absent. If your data has geometry and you want to do anything non-trivial, the current answer is: export to PostGIS, do the work there, import back. Congratulations, you just negated the reason you chose ClickHouse.

The concrete use case that started this: querying geospatial data stored in Parquet files via Apache Iceberg. Large geometry datasets in a lakehouse, ClickHouse as the query engine, geometry encoded as WKB blobs in Parquet `BYTE_ARRAY` columns — industry standard, works everywhere. ClickHouse reads the WKB fine. It just can't do anything interesting with it.

### Why not a core PR?

The obvious move is a PR to ClickHouse adding GEOS as a dependency. This is also the move that will consume six months and probably fail:

- GEOS is LGPL; ClickHouse's licensing situation makes bundling it uncomfortable ([tracked issue](https://github.com/ClickHouse/ClickHouse/issues/80186))
- Adding sixty spatial functions wrapped around a new external dependency is not a quick review
- You need it working now, not when the stars align over the issue tracker

### Why WASM?

ClickHouse's experimental WASM UDF engine (wasmtime) lets you compile a `.wasm` module, drop it in, and call your functions as if they were built-in. GEOS compiles to WASM via Emscripten. The result is a self-contained binary with no system dependencies, no ClickHouse internals touched, no LGPL contamination of the host binary. You ship a file and write `CREATE FUNCTION` statements.

With chgeos, querying geometry from an Iceberg table looks exactly like PostGIS — same function names, same semantics, no translation guide:

```sql
SELECT region_name, st_area(geometry) AS area
FROM iceberg('s3://my-bucket/regions/')
WHERE st_intersects(geometry, st_geomfromtext('POLYGON((...))'))
ORDER BY area DESC;
```

## How it works

**Wire format:** Geometries are `String` columns containing raw EWKB bytes — the same format PostGIS, GeoParquet, and most spatial tools use. No conversion needed at the database boundary.

**ABI:** Every UDF uses `BUFFERED_V1` — ClickHouse passes an entire column as a MsgPack buffer in one call, the module processes all rows in a tight loop, returns results. WASM boundary overhead is constant per query, not per row.

**Template machinery:** A single `impl_wrapper<Ret, Args...>` template handles all MsgPack unpacking and packing. Adding a function is two lines: the C++ impl and a macro invocation. No hand-written marshaling.

**Bbox short-circuit:** Binary predicates (`ST_Intersects`, `ST_Contains`, `ST_Within`, etc.) extract bounding boxes directly from raw WKB bytes — no GEOS parse, no heap allocation — and return early when boxes don't overlap. GEOS is only invoked when the bbox check passes.

> **Note:** ClickHouse WASM UDFs are experimental and not available on ClickHouse Cloud.
> Active upstream PRs this project depends on or tracks:
> - [#99373](https://github.com/ClickHouse/ClickHouse/pull/99373) — WASM UDF ABI improvements (BUFFERED_V1 stability)
> - [#100005](https://github.com/ClickHouse/ClickHouse/pull/100005) — `DETERMINISTIC` keyword support for WASM UDFs

## Functions

> **WKB** columns are stored as `String` containing raw WKB bytes.

### Metadata

| Function | Arguments | Returns | Description |
|---|---|---|---|
| `geos_version` | — | `String` | GEOS version string |

### I/O

| Function | Arguments | Returns | Description |
|---|---|---|---|
| `st_geomfromtext` | `wkt String` | `WKB` | Parse WKT → WKB |
| `st_geomfromwkb` | `wkb String` | `WKB` | Validate and round-trip WKB |
| `st_geomfromgeojson` | `geojson String` | `WKB` | Parse GeoJSON → WKB |
| `st_geomfromchpoint` | `pt Point` | `WKB` | ClickHouse `Point` → WKB |
| `st_geomfromchlinestring` | `ls LineString` | `WKB` | ClickHouse `LineString` → WKB |
| `st_geomfromchpolygon` | `poly Polygon` | `WKB` | ClickHouse `Polygon` → WKB |
| `st_geomfromchmultipolygon` | `mp MultiPolygon` | `WKB` | ClickHouse `MultiPolygon` → WKB |
| `st_astext` | `wkb String` | `String` | WKB → WKT |
| `st_asewkt` | `wkb String` | `String` | WKB → EWKT (includes SRID) |

### Accessors

| Function | Arguments | Returns | Description |
|---|---|---|---|
| `st_x` | `wkb String` | `Float64` | X coordinate of a Point |
| `st_y` | `wkb String` | `Float64` | Y coordinate of a Point |
| `st_z` | `wkb String` | `Float64` | Z coordinate of a Point |
| `st_srid` | `wkb String` | `Int32` | SRID |
| `st_setsrid` | `wkb String, srid UInt32` | `WKB` | Set SRID |
| `st_npoints` | `wkb String` | `Int32` | Number of vertices |
| `st_numpoints` | `wkb String` | `Int32` | Number of points (LineString) |
| `st_numgeometries` | `wkb String` | `Int32` | Number of sub-geometries |
| `st_numinteriorrings` | `wkb String` | `Int32` | Number of interior rings (Polygon) |
| `st_nrings` | `wkb String` | `Int32` | Total number of rings |
| `st_dimension` | `wkb String` | `Int32` | Geometry dimension |
| `st_geometrytype` | `wkb String` | `String` | Geometry type name |
| `st_area` | `wkb String` | `Float64` | Area |
| `st_length` | `wkb String` | `Float64` | Length |
| `st_perimeter` | `wkb String` | `Float64` | Perimeter |
| `st_isvalid` | `wkb String` | `UInt8` | Validity check |
| `st_isvalidreason` | `wkb String` | `String` | Reason for invalidity |
| `st_isempty` | `wkb String` | `UInt8` | Empty check |
| `st_issimple` | `wkb String` | `UInt8` | Simplicity check |
| `st_isring` | `wkb String` | `UInt8` | Ring check |

### Constructors

| Function | Arguments | Returns | Description |
|---|---|---|---|
| `st_makepoint` | `x Float64, y Float64` | `WKB` | Create a 2D Point |
| `st_makepoint3d` | `x Float64, y Float64, z Float64` | `WKB` | Create a 3D Point |
| `st_makeline` | `a String, b String` | `WKB` | Create a LineString from two points |
| `st_makepolygon` | `shell String` | `WKB` | Create a Polygon from a ring |
| `st_makebox2d` | `low_left String, up_right String` | `WKB` | Bounding box from two corner points |
| `st_collect` | `a String, b String` | `WKB` | Collect into GeometryCollection |
| `st_envelope` | `wkb String` | `WKB` | Bounding box polygon |
| `st_extent` | `wkb String` | `WKB` | Alias for `st_envelope` |
| `st_expand` | `wkb String, units Float64` | `WKB` | Expand bounding box by distance |
| `st_centroid` | `wkb String` | `WKB` | Centroid |
| `st_interiorpoint` | `wkb String` | `WKB` | A point guaranteed to be inside |
| `st_startpoint` | `wkb String` | `WKB` | First point of a LineString |
| `st_endpoint` | `wkb String` | `WKB` | Last point of a LineString |
| `st_pointn` | `wkb String, n Int32` | `WKB` | N-th point of a LineString |
| `st_geometryn` | `wkb String, n Int32` | `WKB` | N-th sub-geometry |
| `st_exteriorring` | `wkb String` | `WKB` | Exterior ring of a Polygon |
| `st_interiorringn` | `wkb String, n Int32` | `WKB` | N-th interior ring of a Polygon |
| `st_boundary` | `wkb String` | `WKB` | Boundary |
| `st_convexhull` | `wkb String` | `WKB` | Convex hull |
| `st_minimumboundingcircle` | `wkb String` | `WKB` | Minimum bounding circle |
| `st_closestpoint` | `a String, b String` | `WKB` | Closest point on A to B |
| `st_shortestline` | `a String, b String` | `WKB` | Shortest line between A and B |
| `st_sharedpaths` | `a String, b String` | `WKB` | Shared paths between two lineal geometries |
| `st_addpoint` | `line String, point String, pos Int32` | `WKB` | Add point to LineString at position |
| `st_removepoint` | `line String, pos Int32` | `WKB` | Remove point from LineString |
| `st_setpoint` | `line String, pos Int32, point String` | `WKB` | Replace point in LineString |

### Predicates

| Function | Arguments | Returns | Description |
|---|---|---|---|
| `st_contains` | `a String, b String` | `UInt8` | A contains B |
| `st_containsproperly` | `a String, b String` | `UInt8` | A properly contains B |
| `st_covers` | `a String, b String` | `UInt8` | A covers B |
| `st_coveredby` | `a String, b String` | `UInt8` | A is covered by B |
| `st_crosses` | `a String, b String` | `UInt8` | A and B cross |
| `st_disjoint` | `a String, b String` | `UInt8` | A and B are disjoint |
| `st_dwithin` | `a String, b String, dist Float64` | `UInt8` | A and B are within distance |
| `st_equals` | `a String, b String` | `UInt8` | A and B are geometrically equal |
| `st_intersects` | `a String, b String` | `UInt8` | A and B intersect |
| `st_intersects_extent` | `a String, b String` | `UInt8` | Bounding boxes intersect (fast check) |
| `st_overlaps` | `a String, b String` | `UInt8` | A and B overlap |
| `st_touches` | `a String, b String` | `UInt8` | A and B touch |
| `st_within` | `a String, b String` | `UInt8` | A is within B |
| `st_relate` | `a String, b String` | `String` | DE-9IM relation matrix |
| `st_relate_pattern` | `a String, b String, pattern String` | `UInt8` | Match DE-9IM pattern |
| `st_distance` | `a String, b String` | `Float64` | Minimum distance |
| `st_hausdorffdistance` | `a String, b String` | `Float64` | Hausdorff distance |
| `st_hausdorffdistance_densify` | `a String, b String, densify_frac Float64` | `Float64` | Hausdorff distance with densification |
| `st_frechetdistance` | `a String, b String` | `Float64` | Fréchet distance |
| `st_frechetdistance_densify` | `a String, b String, densify_frac Float64` | `Float64` | Fréchet distance with densification |

### Overlay

| Function | Arguments | Returns | Description |
|---|---|---|---|
| `st_union` | `a String, b String` | `WKB` | Union |
| `st_union_agg` | `wkb_array Array(String)` | `WKB` | Union of an array of geometries |
| `st_intersection` | `a String, b String` | `WKB` | Intersection |
| `st_difference` | `a String, b String` | `WKB` | A minus B |
| `st_symdifference` | `a String, b String` | `WKB` | Symmetric difference |
| `st_unaryunion` | `wkb String` | `WKB` | Union all sub-geometries |
| `st_clusterintersecting` | `wkb String` | `WKB` | Cluster intersecting geometries |

### Processing

| Function | Arguments | Returns | Description |
|---|---|---|---|
| `st_buffer` | `wkb String, radius Float64` | `WKB` | Buffer by radius |
| `st_buffer_params` | `wkb String, radius Float64, params String` | `WKB` | Buffer with style options (see below) |
| `st_simplify` | `wkb String, tolerance Float64` | `WKB` | Douglas-Peucker simplification |
| `st_makevalid` | `wkb String` | `WKB` | Repair invalid geometry |
| `st_normalize` | `wkb String` | `WKB` | Normalize geometry |
| `st_reverse` | `wkb String` | `WKB` | Reverse vertex order |
| `st_node` | `wkb String` | `WKB` | Add nodes at intersections |
| `st_segmentize` | `wkb String, max_length Float64` | `WKB` | Densify by max segment length |
| `st_subdivide` | `wkb String, max_vertices Int32` | `WKB` | Subdivide into smaller parts |
| `st_snap` | `a String, b String, tolerance Float64` | `WKB` | Snap A to B within tolerance |
| `st_offsetcurve` | `wkb String, distance Float64` | `WKB` | Offset curve |
| `st_linmerge` | `wkb String` | `WKB` | Merge line segments |
| `st_polygonize` | `wkb String` | `WKB` | Polygonize a set of lines |
| `st_delaunaytriangles` | `wkb String, tolerance Float64, only_edges Int32` | `WKB` | Delaunay triangulation |
| `st_voronoidiagram` | `wkb String, tolerance Float64, only_edges Int32` | `WKB` | Voronoi diagram |

### Transforms

| Function | Arguments | Returns | Description |
|---|---|---|---|
| `st_translate` | `wkb String, dx Float64, dy Float64` | `WKB` | Translate by offset |
| `st_scale` | `wkb String, xf Float64, yf Float64` | `WKB` | Scale by factor |
| `st_transform` | `wkb String, srid Int32` | `WKB` | CRS transform (PROJ 9 not linked — see Limitations) |
| `st_transform_proj` | `wkb String, pipeline String` | `WKB` | PROJ pipeline transform (PROJ 9 not linked — see Limitations) |

### `st_buffer_params` options

Space-separated `key=value` tokens:

| Key | Values |
|---|---|
| `endcap` | `round` (default), `flat`, `butt`, `square` |
| `join` | `round` (default), `mitre`, `miter`, `bevel` |
| `mitre_limit` / `miter_limit` | float |
| `quad_segs` | integer (default 8) |
| `side` | `left`, `right` |

### Benchmarking

These functions are exported from the WASM module for micro-benchmarking call overhead and parsing cost. They are not in `create.sql` — register them manually if needed.

| Function | Arguments | Returns | Description |
|---|---|---|---|
| `geos_bench_noop` | `wkb String` | `UInt8` | MsgPack round-trip + WASM call overhead only, no parsing |
| `geos_bench_noop_rb` | `wkb String` | `UInt8` | Same via CH-native `wkb()` serialization path |
| `geos_bench_wkb_parse` | `wkb String` | `UInt8` | EWKB → GEOS parse cost only |
| `geos_bench_envelope` | `wkb String` | `UInt8` | Fast WKB bbox extraction without GEOS allocation |

## Building

### Requirements

- [Emscripten](https://emscripten.org/) (emsdk), `emcmake` in `PATH`
- CMake ≥ 3.10

### Build the WASM module

```bash
emcmake cmake -S . -B build_wasm -DCMAKE_BUILD_TYPE=Release
cmake --build build_wasm --target chgeos
# Output: build_wasm/chgeos.wasm
```

### Build and run unit tests (native)

No Emscripten needed — tests compile and run natively with a standard C++ toolchain.

```bash
cmake -S . -B build_native -DCMAKE_BUILD_TYPE=Debug
cmake --build build_native
cd build_native && ctest --output-on-failure
```

### Run end-to-end tests (ClickHouse)

Requires a ClickHouse binary with WASM UDF support and a built `chgeos.wasm`.

```bash
./clickhouse/test_e2e.sh [/path/to/clickhouse] [/path/to/chgeos.wasm]
```

Defaults: looks for ClickHouse at `../ClickHouse/build/programs/clickhouse` relative to the repo root, and the WASM module at `build_wasm/chgeos.wasm`.

## ClickHouse setup

### Enable the feature

```xml
<!-- config.xml -->
<clickhouse>
    <allow_experimental_webassembly_udf>true</allow_experimental_webassembly_udf>
    <webassembly_udf_engine>wasmtime</webassembly_udf_engine>
</clickhouse>
```

### Load the module

```sql
INSERT INTO system.webassembly_modules (name, code)
SELECT 'chgeos', base64Decode('{base64 of chgeos.wasm}');
```

Or from a file via `clickhouse-client`:

```bash
clickhouse-client --query "
  INSERT INTO system.webassembly_modules (name, code)
  SELECT 'chgeos', readFile('/path/to/chgeos.wasm')"
```

### Register functions

Each function must be registered individually. All use `ABI BUFFERED_V1` and MsgPack serialization:

```sql
CREATE OR REPLACE FUNCTION st_geomfromtext
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkt String) RETURNS String
ABI BUFFERED_V1;

CREATE OR REPLACE FUNCTION st_astext
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (wkb String) RETURNS String
ABI BUFFERED_V1;

CREATE OR REPLACE FUNCTION st_intersects
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1;

CREATE OR REPLACE FUNCTION st_distance
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS Float64
ABI BUFFERED_V1;

-- ... (one CREATE FUNCTION per function)
```

### Usage examples

**Basic accessors and predicates:**

```sql
WITH
    st_geomfromtext('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') AS poly,
    st_geomfromtext('POINT (5 5)') AS pt
SELECT
    st_astext(st_centroid(poly))  AS centroid,   -- 'POINT (5 5)'
    st_area(poly)                 AS area,        -- 100
    st_contains(poly, pt)         AS contains,    -- 1
    st_distance(poly, pt)         AS distance;    -- 0
```

**Filter rows from a Parquet file by spatial intersection:**

```sql
SELECT count()
FROM file('locations.parquet', Parquet, 'location String')
WHERE st_intersects(
    location,
    st_geomfromtext('POLYGON ((13.0 52.3, 13.6 52.3, 13.6 52.7, 13.0 52.7, 13.0 52.3))')
);
```

**Spatial join via bounding-box pre-filter + exact predicate:**

```sql
SELECT a.id, b.id
FROM points  AS a
JOIN regions AS b ON st_intersects_extent(a.geom, b.geom)
                  AND st_within(a.geom, b.geom);
```

**I/O: parse GeoJSON, round-trip through WKB, export EWKT:**

```sql
SELECT
    st_asewkt(st_setsrid(st_geomfromgeojson('{"type":"Point","coordinates":[13.4,52.5]}'), 4326));
-- 'SRID=4326;POINT (13.4 52.5)'
```

**Geometry processing: buffer, repair, subdivide:**

```sql
SELECT
    st_astext(st_buffer(st_makepoint(0, 0), 1.0))      AS circle,
    st_isvalid(st_makevalid(st_geomfromtext(bad_wkt)))  AS fixed,
    st_numgeometries(st_subdivide(large_poly, 256))     AS chunks
FROM my_table;
```

**Aggregate union over an array** (modelled after DuckDB's `ST_Union_Agg`; since ClickHouse WASM UDFs are scalar-only, pass an array built with `groupArray`):

```sql
SELECT
    region_id,
    st_astext(st_union_agg(groupArray(geom))) AS merged
FROM my_table
GROUP BY region_id;
```

**DE-9IM relation:**

```sql
SELECT st_relate(
    st_geomfromtext('LINESTRING (0 0, 2 2)'),
    st_geomfromtext('LINESTRING (0 2, 2 0)')
);  -- '0F1FF0102' (crossing lines)
```

## Architecture

```
src/
├── main.cpp              # WASM exports: geos_version + CH_UDF_FUNC calls
├── udf.hpp               # impl_wrapper (msgpack row loop) + CH_UDF_FUNC macro
├── functions.hpp         # Aggregator — includes all functions/* headers
├── functions/
│   ├── accessors.hpp     # st_x, st_y, st_centroid, st_area, st_npoints, st_srid
│   ├── predicates.hpp    # st_contains, st_intersects, st_touches, st_dwithin, ...
│   ├── constructors.hpp  # st_geomfromtext/wkb, st_extent, st_makebox2d, ...
│   ├── transforms.hpp    # st_translate, st_scale, st_transform
│   ├── overlay.hpp       # st_union, st_intersection, st_difference, ...
│   ├── processing.hpp    # st_buffer, st_simplify, st_subdivide, ...
│   └── io.hpp            # st_astext, st_aswkb, geos_version_str
├── geom/
│   ├── wkb.hpp           # read_wkb, write_wkb, read_wkt
│   ├── filters.hpp       # TranslateFilter, ScaleFilter
│   └── subdivide.hpp     # subdivide_recursive
├── clickhouse.hpp        # Host ABI: clickhouse_log, clickhouse_throw
├── mem.hpp               # RawBuffer, clickhouse_create/destroy_buffer
└── io.hpp                # VectorWriteBuf (streambuf over std::vector)
```

**Wire format:** Each UDF receives a `RawBuffer*` containing MsgPack-serialized argument columns and a row count. `impl_wrapper` unpacks rows, calls the `_impl` function, and packs results back into a new `RawBuffer`. Geometry is passed as raw WKB bytes (`std::span<const uint8_t>`).

## Limitations

- **No native ClickHouse geometry type integration.** ClickHouse's built-in `Point`, `LineString`, `Polygon`, `MultiPolygon` types have their own internal representation. You cannot pass them directly to `ST_*` functions. Conversion helpers (`ST_GeomFromCHPoint`, `ST_GeomFromCHLineString`, etc.) are provided, but every geometry column must be in WKB (`String`) for chgeos to operate on it.

- **No PROJ 9 / accurate CRS reprojection.** `ST_Transform` and `ST_TransformProj` exist in the DDL but PROJ is not linked into the WASM module. Datum-shift-aware reprojection (e.g. EPSG:4326 → EPSG:3857 with grid files, NAD27 → NAD83) requires PROJ 9 with datum grids, which cannot run inside the WASM sandbox. Reproject data before loading it into ClickHouse if accurate CRS conversion is needed.

- **Planar geometry only.** All calculations are Cartesian — `ST_Distance`, `ST_Area`, `ST_Length` work in the coordinate units of the geometry, not meters on the sphere. Use an appropriate projected CRS (e.g. UTM) for metric results.

- **No native aggregate functions.** WASM UDFs in ClickHouse are scalar only. `ST_Union_Agg` works around this by accepting an `Array(String)` rather than a true `GROUP BY` aggregate, but this requires materializing the geometry array in memory first.

- **Geometry parsed on every call.** WKB is re-parsed from bytes for each row invocation — there is no persistent geometry cache across rows or queries.

- **Experimental ClickHouse feature.** `allow_experimental_webassembly_udf` is not production-ready and not available on ClickHouse Cloud. The UDF API may change between ClickHouse releases.

## Dependencies

| Library | Version | Role |
|---|---|---|
| [GEOS](https://libgeos.org/) | ≥ 3.12 | Geometry engine |
| [msgpack23](https://github.com/rwindegger/msgpack23) | ≥ 3.1 | MsgPack serialization with zero-copy span support |
| [googletest](https://github.com/google/googletest) | ≥ 1.15 | Unit tests (native build only) |
