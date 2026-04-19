# chgeos

PostGIS-compatible spatial functions for ClickHouse, delivered as a WebAssembly UDF module powered by [GEOS](https://libgeos.org/) 3.12+.

## Disclaimer

This is my pet-project with `-Ofun` mentality.
* Just a hobby, won't be big and professional.
* I'm testing how far I can push Claude.
* I'm (re-)learning low-level optimization I haven't done in 15+ years.
* I'm extracting useful pieces out of this project into upstream projects. For example non-copy `std::span` handling in `msgpack23` and exceptions support in `wasmtime`. Also, general improvements of WASM UDF support in ClickHouse. Watch this space ;)
* ~~This is nowhere near any useful application. For many reasons. Especially because CH<->UDF interaction is very limited. Basically it's a one-way street at the moment and any "spatial aware" query engine that can use Parquet file metadata will be faster. Much faster. Order of magnitude faster.~~ See [CH_CHANGES.md](CH_CHANGES.md) and [BENCHMARK.md](BENCHMARK.md) 

Having said that, I'm not saying it will never be useful.

## Motivation

ClickHouse is fast. If you need to crunch billions of rows, it's the right tool. But the moment you ask "can it do spatial analytics?" the answer is: technically yes, practically no.

ClickHouse has native geometry types (`Point`, `Polygon`, `MultiPolygon`, ...) and some spatial functions under names like `polygonsIntersectCartesian`, `areaCartesian`, `polygonsWithinCartesian`. It also has H3 and S2 index support. What it doesn't have is:

- **WKB / GeoParquet compatibility.** Native CH geometry types have their own internal representation. Real-world geometry data — GeoParquet, PostGIS, GDAL, anything — is encoded as WKB. You can't pass a WKB blob to `polygonsIntersectCartesian`.
- **PostGIS-compatible names.** Every GIS engineer knows `ST_Intersects`, `ST_Buffer`, `ST_Within`. ClickHouse's equivalents are named differently, require type conversion, and are documented separately.
- **The full GEOS function set.** `ST_Buffer`, `ST_Simplify`, `ST_Centroid`, `ST_MakeValid`, etc. — none of these exist in ClickHouse.

The concrete use case that started this: querying geospatial data stored in Parquet files via Apache Iceberg. Large geometry datasets in a lakehouse, ClickHouse as the query engine, geometry encoded as WKB blobs in Parquet `BYTE_ARRAY` columns — industry standard, works everywhere.

ClickHouse reads the WKB fine. But a bunch of functions are missing. And I really wanted to use them to make some particular DB quack in awe. I failed, btw.

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

## In-flight ClickHouse changes

chgeos depends on ClickHouse patches that are not yet merged upstream, all on the [`bacek/wasm`](https://github.com/bacek/ClickHouse/tree/bacek/wasm) branch. The changes span four areas: WASM runtime extensions (UDAFs, `DETERMINISTIC` constant folding, dynamic block splitting), a columnar call ABI (COLUMNAR_V1), a spatial predicate join engine (SpatialRTreeJoin with R-tree indexing and query rewriting), and spatial pruning at the storage layer (GeoParquet row-group/page pruning, Iceberg manifest pruning, MergeTree skip index). See [CH_CHANGES.md](CH_CHANGES.md) for a detailed write-up.

## How it works

**Wire format:** Geometries are `String` columns containing raw EWKB bytes — the same format PostGIS, GeoParquet, and most spatial tools use. No conversion needed at the database boundary.

**Three ABIs:** Functions are registered under three wire formats. The fastest path (COLUMNAR_V1) is the default for all functions that support it:
- **COLUMNAR_V1** (`ABI COLUMNAR_V1`) — one call for all N rows; ClickHouse sends columns, not rows. Constant columns (e.g. a filter polygon) are sent once, not N times. Exported as `name_col`.
- **RowBinary** (`ABI BUFFERED_V1`, `serialization_format = 'RowBinary'`) — one call per batch with typed binary encoding. Exported as `name_mp`.
- **MsgPack** (`ABI BUFFERED_V1`) — original path, used for aggregates and CH native type converters. Also exported as `name_mp`.

Canonical PostGIS-compatible function names (`st_contains`, `st_distance`, etc.) are SQL aliases that route to `_col` when a columnar variant exists, or `_mp` otherwise.

**Template machinery:** A single `columnar_impl_wrapper<Ret, Args...>` template deduces all argument and return types from the `_impl` function pointer. Adding a new columnar function is two lines: the C++ impl and a `CH_UDF_COL(name)` macro invocation.

**Bbox short-circuit:** Binary predicates (`ST_Intersects`, `ST_Contains`, `ST_Within`, etc.) extract bounding boxes directly from raw WKB bytes — no GEOS parse, no heap allocation — and return early when boxes don't overlap. GEOS is only invoked when the bbox check passes.

**PreparedGeometry:** When a geometry column is constant across all rows (e.g. a filter polygon in a WHERE clause), the columnar wrapper parses the WKB once, builds a GEOS `PreparedGeometry` (STR-tree spatial index), and reuses it for all N rows. This accelerates all 11 binary predicates and `ST_DWithin`.

> **Note:** ClickHouse WASM UDFs are experimental and not available on ClickHouse Cloud. The patches chgeos requires are tracked in the [In-flight ClickHouse changes](#in-flight-clickhouse-changes) section above.

## Functions

PostGIS-compatible names and semantics throughout. The complete DDL is in [`clickhouse/create.sql`](clickhouse/create.sql).

Two additions beyond the standard PostGIS set:

- **`st_intersects_extent`** — bounding-box-only intersection check with no GEOS parse. Use as a fast pre-filter in joins before applying a precise predicate.
- **`st_knn`** — k-nearest-neighbour query: given a probe geometry and an array of candidate geometries, returns the `k` closest (index, distance) pairs. When the candidate array is constant, the GEOS STRtree index is built once per batch.

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
clickhouse client -q "INSERT INTO system.webassembly_modules (name, code) VALUES ('chgeos', file('/path/to/chgeos.wasm'))"
```

### Register functions

All function DDL is in `clickhouse/create.sql`. Load them all at once:

```bash
clickhouse client --multiquery < clickhouse/create.sql
```

Each function is registered under up to three names:

```sql
-- COLUMNAR_V1 (fastest path, preferred for analytical queries)
CREATE OR REPLACE FUNCTION st_intersects_col
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI COLUMNAR_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1;

-- RowBinary / MsgPack (fallback, used for aggregates and CH-native type converters)
CREATE OR REPLACE FUNCTION st_intersects_mp
LANGUAGE WASM FROM 'chgeos'
ARGUMENTS (a String, b String) RETURNS UInt8
ABI BUFFERED_V1
DETERMINISTIC
SETTINGS is_spatial_predicate = 1, serialization_format = 'RowBinary';

-- Canonical PostGIS-compatible alias (routes to _col when available, _mp otherwise)
CREATE OR REPLACE FUNCTION st_intersects AS (a, b) -> st_intersects_col(a, b);
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

**Aggregate functions** — true `GROUP BY` aggregates, same as PostGIS/DuckDB:

```sql
-- Dissolving union per region
SELECT region_id, st_astext(st_union_agg(geom)) AS merged
FROM my_table
GROUP BY region_id;

-- Bounding box per category
SELECT category, st_astext(st_extent_agg(geom)) AS bbox
FROM my_table
GROUP BY category;

-- Collect all geometries per group (no dissolve)
SELECT region_id, st_numgeometries(st_collect_agg(geom)) AS count
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

## Limitations

- **No native ClickHouse geometry type integration.** ClickHouse's built-in `Point`, `LineString`, `Polygon`, `MultiPolygon` types have their own internal representation. You cannot pass them directly to `ST_*` functions. Conversion helpers (`ST_GeomFromCHPoint`, `ST_GeomFromCHLineString`, etc.) are provided, but every geometry column must be in WKB (`String`) for chgeos to operate on it.

- **No PROJ 9 / accurate CRS reprojection.** `ST_Transform` and `ST_TransformProj` exist in the DDL but PROJ is not linked into the WASM module. Datum-shift-aware reprojection (e.g. EPSG:4326 → EPSG:3857 with grid files, NAD27 → NAD83) requires PROJ 9 with datum grids, which cannot run inside the WASM sandbox. Reproject data before loading it into ClickHouse if accurate CRS conversion is needed.

- **Planar geometry only.** All calculations are Cartesian — `ST_Distance`, `ST_Area`, `ST_Length` work in the coordinate units of the geometry, not meters on the sphere. Use an appropriate projected CRS (e.g. UTM) for metric results.

- **Geometry parsed per row (with exceptions).** WKB is re-parsed from bytes for each row. The exception is constant geometry columns in COLUMNAR_V1: when one argument is constant (e.g. a filter polygon), PreparedGeometry builds a spatial index once and reuses it for all rows.

- **Experimental ClickHouse feature.** `allow_experimental_webassembly_udf` is not production-ready and not available on ClickHouse Cloud. The UDF API may change between ClickHouse releases.

## Dependencies

| Library | Version | Role |
|---|---|---|
| [GEOS](https://libgeos.org/) | ≥ 3.12 | Geometry engine |
| [msgpack23](https://github.com/rwindegger/msgpack23) | ≥ 3.1 | MsgPack serialization with zero-copy span support |
| [googletest](https://github.com/google/googletest) | ≥ 1.15 | Unit tests (native build only) |
