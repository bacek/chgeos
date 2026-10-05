# chgeos

chgeos adds PostGIS-compatible spatial functions to ClickHouse. It is a WebAssembly (WASM)
user-defined function module that uses [GEOS](https://libgeos.org/) 3.12 or later.

## Disclaimer

This is a hobby project, written for fun.

* It is a hobby. It will not become big or professional.
* I use it to test how far I can push Claude.
* I use it to relearn low-level optimization that I have not done for more than 15 years.
* I move useful parts of it into upstream projects. Examples are non-copy `std::span` support
  in `msgpack23`, exception support in `wasmtime`, and general improvements to WASM UDF support
  in ClickHouse.
* ~~This is nowhere near any useful application. For many reasons. Especially because CH<->UDF interaction is very limited. Basically it's a one-way street at the moment and any "spatial aware" query engine that can use Parquet file metadata will be faster. Much faster. Order of magnitude faster.~~ See [CH_CHANGES.md](CH_CHANGES.md) and [BENCHMARK.md](BENCHMARK.md).

I do not say that it will never be useful.

## Status (2026-10-05)

The project is mostly complete for its current scope.

On the [SpatialBench](https://github.com/apache/sedona-spatialbench) suite, chgeos is the
fastest engine on 10 of 12 queries at SF1, with one tie. At SF10 it is the fastest on 9 of 12
queries, with two ties. The other engines are DuckDB 1.5.6, SedonaDB 0.4.1 and PyCanopy 0.4.1.

chgeos loses two queries. PyCanopy is faster on SF1 Q11 (cross-zone trips). DuckDB is faster on
SF10 Q7 (detour ratio), because DuckDB reads points directly from the Parquet bytes, but chgeos
parses each value. All 12 queries return the correct answers at both scales. See
[BENCHMARK.md](BENCHMARK.md).

The remaining work is mostly outside this repository:

- Parquet read speed. ClickHouse reads Parquet 1.6 to 2.7 times slower than DuckDB on a plain
  scan. chgeos still wins the queries that scan the most data, but the read speed limits how
  fast they can be.
- The WASM boundary. Code in WASM runs several times slower per row than the same code compiled
  for the CPU. To close this gap, wasmtime and the ClickHouse UDF support must change.
- Upstreaming. The spatial join and the WASM UDF changes are in a ClickHouse fork (see
  [CH_CHANGES.md](CH_CHANGES.md)). Other users can use them only after they are merged into
  ClickHouse.

## Motivation

ClickHouse is fast. If you must process billions of rows, it is the correct tool. But spatial
analysis in ClickHouse is possible in theory and difficult in practice.

ClickHouse has native geometry types (`Point`, `Polygon`, `MultiPolygon`, and others). It has
some spatial functions, with names such as `polygonsIntersectCartesian`, `areaCartesian` and
`polygonsWithinCartesian`. It also supports the H3 and S2 indexes. It does not have these
features:

- WKB and GeoParquet compatibility. WKB (Well-Known Binary) is the standard binary format for
  geometry. The native ClickHouse geometry types use their own internal format. Real geometry
  data from GeoParquet, PostGIS, GDAL and other tools is in WKB. You cannot give a WKB value to
  `polygonsIntersectCartesian`.
- PostGIS-compatible names. GIS engineers know `ST_Intersects`, `ST_Buffer` and `ST_Within`.
  The ClickHouse functions have different names, need type conversion, and have separate
  documentation.
- The full GEOS function set. ClickHouse has no `ST_Buffer`, `ST_Simplify`, `ST_Centroid` or
  `ST_MakeValid`.

The use case that started this project is a query on geometry data in Parquet files in Apache
Iceberg. The data is a large set of geometries in a lakehouse. ClickHouse is the query engine.
The geometry is WKB in Parquet `BYTE_ARRAY` columns, which is the industry standard.

ClickHouse reads the WKB correctly, but many functions are missing. I wanted to use them to
make a particular database quack in awe. I did not succeed.

### Why not a pull request to ClickHouse?

The obvious approach is a pull request that adds GEOS to ClickHouse as a dependency. This
approach would take about six months and would probably fail:

- GEOS uses the LGPL license. The ClickHouse license situation makes it difficult to include
  GEOS ([tracked issue](https://github.com/ClickHouse/ClickHouse/issues/80186)).
- A review of sixty spatial functions that wrap a new external dependency is slow.
- You need the functions now.

### Why WASM?

The experimental WASM UDF engine in ClickHouse (wasmtime) loads a compiled `.wasm` module. You
can then call its functions as if they were built in. Emscripten compiles GEOS to WASM. The
result is one binary file with no system dependencies. It does not change the ClickHouse
internals, and the LGPL code does not go into the ClickHouse binary. You copy one file and run
`CREATE FUNCTION` statements.

With chgeos, a query on geometry in an Iceberg table looks the same as a PostGIS query. The
function names and semantics are the same:

```sql
SELECT region_name, st_area(geometry) AS area
FROM iceberg('s3://my-bucket/regions/')
WHERE st_intersects(geometry, st_geomfromtext('POLYGON((...))'))
ORDER BY area DESC;
```

## ClickHouse changes in progress

chgeos needs ClickHouse patches that are not yet merged upstream. All of them are on the
[`bacek/wasm`](https://github.com/bacek/ClickHouse/tree/bacek/wasm) branch. The patches cover
four areas:

- WASM runtime extensions: aggregate UDFs (UDAFs), constant folding for `DETERMINISTIC`
  functions, and dynamic block splitting.
- A columnar call interface (COLUMNAR_V1).
- A join engine for spatial predicates (SpatialRTreeJoin), with an R-tree index and query
  rewriting.
- Spatial pruning at the storage layer: GeoParquet row-group and page pruning, Iceberg manifest
  pruning, and a MergeTree skip index.

[CH_CHANGES.md](CH_CHANGES.md) describes the patches in detail.

## How it works

Geometries are `String` columns that contain raw EWKB bytes. EWKB is WKB with an optional SRID.
PostGIS, GeoParquet and most spatial tools use this format, so no conversion is necessary at
the database boundary.

chgeos registers each function for up to three call interfaces (ABIs). COLUMNAR_V1 is the
fastest, and chgeos uses it by default for every function that supports it:

- COLUMNAR_V1 (`ABI COLUMNAR_V1`) makes one call for all N rows. ClickHouse sends columns, not
  rows. It sends a constant column, for example a filter polygon, one time and not N times. The
  exported name is `name_col`.
- RowBinary (`ABI BUFFERED_V1`, `serialization_format = 'RowBinary'`) makes one call per batch,
  with a typed binary encoding. The exported name is `name_mp`.
- MsgPack (`ABI BUFFERED_V1`) is the original interface. Aggregates and the converters for
  native ClickHouse types use it. The exported name is also `name_mp`.

The standard PostGIS names (`st_contains`, `st_distance` and the others) are SQL aliases. An
alias calls the `_col` function if it exists, and the `_mp` function if not.

One template, `columnar_impl_wrapper<Ret, Args...>`, gets the argument and return types from
the `_impl` function pointer. To add a columnar function, you write two lines: the C++
implementation and a `CH_UDF_COL(name)` macro call.

The binary predicates (`ST_Intersects`, `ST_Contains`, `ST_Within` and the others) first read
the bounding boxes directly from the WKB bytes. This step does not parse the geometry and does
not allocate memory. If the boxes do not overlap, the predicate returns at once. GEOS runs only
when the boxes overlap.

A geometry column can be constant for all rows, for example a filter polygon in a `WHERE`
clause. In that case the columnar wrapper parses the WKB one time and builds a GEOS
`PreparedGeometry`, which is a geometry with a spatial index (an STR tree). It then uses this
prepared geometry for all N rows. This makes all 11 binary predicates and `ST_DWithin` faster.

> Note: ClickHouse WASM UDFs are experimental, and ClickHouse Cloud does not support them. The
> section [ClickHouse changes in progress](#clickhouse-changes-in-progress) lists the patches
> that chgeos needs.

## Functions

The function names and semantics are the same as in PostGIS. The full DDL is in
[`clickhouse/create.sql`](clickhouse/create.sql).

chgeos adds two functions that PostGIS does not have:

- `st_intersects_extent` checks only whether two bounding boxes intersect, and does not parse
  the geometry. Use it as a fast filter in a join before a precise predicate.
- `st_knn` finds the k nearest neighbours. It takes a probe geometry and an array of candidate
  geometries, and returns the `k` closest (index, distance) pairs. If the candidate array is
  constant, chgeos builds the index one time and uses it for all batches.

## Benchmarks

The benchmark script (`scripts/bench_sf.py`) runs the 12 SpatialBench queries on ClickHouse
with chgeos. It reads Parquet files or native MergeTree tables.

### Usage

```bash
# Full run (all 12 queries, 5 runs each)
python3 scripts/bench_sf.py --ch ../ClickHouse/build/programs/clickhouse --sf sf1

# Single query
python3 scripts/bench_sf.py --ch ../ClickHouse/build/programs/clickhouse --sf sf1 --query Q7

# Native MergeTree tables (run scripts/import_sf.sh first)
python3 scripts/bench_sf.py --ch ../ClickHouse/build/programs/clickhouse --sf sf1 --native

# JSON output (JSON Lines, one BenchmarkSuite per line)
python3 scripts/bench_sf.py --ch ../ClickHouse/build/programs/clickhouse --sf sf1 --json --output sf1_results.json
```

### Arguments

| Flag | Default | Description |
|------|---------|-------------|
| `--ch` | `clickhouse` on PATH | Path to ClickHouse binary |
| `--sf` | `sf1` | Scale factor — `sf1` (6M rows) or `sf10` (60M rows) |
| `--native` | off | Read from native MergeTree tables instead of Parquet |
| `--runs` | 5 | Number of runs per query (averaged) |
| `--timeout` | 120 | Per-query timeout in seconds |
| `--settings` | — | Extra CH settings, e.g. `"query_max_memory_usage=100000000000"` |
| `--query` | all | Run only this query (e.g. `Q1`, `Q7`) |
| `--queries` | all | Comma-separated queries (e.g. `Q1,Q7`) |
| `--json` | off | Write results to JSON Lines file |
| `--output` | `<sf>/benchmark_results.json` | Output file path for `--json` |

### JSON output format

If you set `--json`, the script writes one JSON line per run of the query set. The format is
the same as the [SedonaDB `BenchmarkSuite`](https://github.com/Location3/spatialbench) format:

```json
{"engine": "chgeos", "version": "14b0f95", "scale_factor": 1.0,
 "timestamp": "2026-05-06T12:00:00+00:00", "total_time": 45.23,
 "results": [
   {"query": "Q1", "time_seconds": 0.11, "row_count": 258, "status": "success", "error_message": null},
   {"query": "Q7", "time_seconds": 4.80, "row_count": 6000000, "status": "success", "error_message": null},
   {"query": "Q8", "time_seconds": 18.50, "row_count": null, "status": "timeout", "error_message": "Timeout after 120s"}
 ]}
```

Fields:

- `engine` is always `"chgeos"`.
- `version` is the short git SHA of the chgeos commit.
- `scale_factor` is `1.0` or `10.0`.
- `total_time` is the sum of all successful `time_seconds` values, in seconds.
- `results[].time_seconds` is the average time across runs, in seconds, rounded to two decimal
  places.
- `results[].status` is `"success"`, `"error"` or `"timeout"`.

## Building

### Requirements

- [Emscripten](https://emscripten.org/) (emsdk), with `emcmake` in `PATH`
- CMake 3.10 or later

### Build the WASM module

```bash
emcmake cmake -S . -B build_wasm -DCMAKE_BUILD_TYPE=Release
cmake --build build_wasm --target chgeos
# Output: build_wasm/chgeos.wasm
```

### Build and run the unit tests (native)

The unit tests do not need Emscripten. They compile and run with a standard C++ toolchain.

```bash
cmake -S . -B build_native -DCMAKE_BUILD_TYPE=Debug
cmake --build build_native
cd build_native && ctest --output-on-failure
```

### Run the end-to-end tests (ClickHouse)

The end-to-end tests need a ClickHouse binary with WASM UDF support and a built `chgeos.wasm`.

```bash
./clickhouse/test_e2e.sh [/path/to/clickhouse] [/path/to/chgeos.wasm]
```

By default, the script looks for ClickHouse at `../ClickHouse/build/programs/clickhouse`,
relative to the repository root. It looks for the WASM module at `build_wasm/chgeos.wasm`.

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

You can also load it from a file with `clickhouse-client`:

```bash
clickhouse client -q "INSERT INTO system.webassembly_modules (name, code) VALUES ('chgeos', file('/path/to/chgeos.wasm'))"
```

### Register the functions

The DDL for all functions is in `clickhouse/create.sql`. To load all of them, run:

```bash
clickhouse client --multiquery < clickhouse/create.sql
```

chgeos registers each function under up to three names:

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

Basic accessors and predicates:

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

Filter the rows of a Parquet file with a spatial intersection:

```sql
SELECT count()
FROM file('locations.parquet', Parquet, 'location String')
WHERE st_intersects(
    location,
    st_geomfromtext('POLYGON ((13.0 52.3, 13.6 52.3, 13.6 52.7, 13.0 52.7, 13.0 52.3))')
);
```

Spatial join with a bounding-box filter and then an exact predicate:

```sql
SELECT a.id, b.id
FROM points  AS a
JOIN regions AS b ON st_intersects_extent(a.geom, b.geom)
                  AND st_within(a.geom, b.geom);
```

Input and output. Parse GeoJSON, convert through WKB, and write EWKT:

```sql
SELECT
    st_asewkt(st_setsrid(st_geomfromgeojson('{"type":"Point","coordinates":[13.4,52.5]}'), 4326));
-- 'SRID=4326;POINT (13.4 52.5)'
```

Geometry processing. Buffer, repair and subdivide:

```sql
SELECT
    st_astext(st_buffer(st_makepoint(0, 0), 1.0))      AS circle,
    st_isvalid(st_makevalid(st_geomfromtext(bad_wkt)))  AS fixed,
    st_numgeometries(st_subdivide(large_poly, 256))     AS chunks
FROM my_table;
```

Aggregate functions. These are true `GROUP BY` aggregates, as in PostGIS and DuckDB:

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

DE-9IM relation:

```sql
SELECT st_relate(
    st_geomfromtext('LINESTRING (0 0, 2 2)'),
    st_geomfromtext('LINESTRING (0 2, 2 0)')
);  -- '0F1FF0102' (crossing lines)
```

## Limitations

- No integration with the native ClickHouse geometry types. The built-in `Point`, `LineString`,
  `Polygon` and `MultiPolygon` types use their own internal format. You cannot give them
  directly to the `ST_*` functions. chgeos has conversion functions (`ST_GeomFromCHPoint`,
  `ST_GeomFromCHLineString` and others), but every geometry column must be WKB in a `String`
  column before chgeos can use it.
- No PROJ 9 and no accurate coordinate system (CRS) conversion. `ST_Transform` and
  `ST_TransformProj` are in the DDL, but the WASM module does not include PROJ. A conversion
  that shifts the datum (for example EPSG:4326 to EPSG:3857 with grid files, or NAD27 to NAD83)
  needs PROJ 9 with datum grids, and these cannot run in the WASM sandbox. If you need an
  accurate CRS conversion, convert the data before you load it into ClickHouse.
- Planar geometry only. All calculations are Cartesian. `ST_Distance`, `ST_Area` and
  `ST_Length` use the coordinate units of the geometry, not meters on the sphere. For results
  in meters, use a projected CRS, for example UTM.
- Geometry is parsed for each row, with one exception. chgeos parses the WKB bytes again for
  each row. If one argument is a constant geometry column in COLUMNAR_V1 (for example a filter
  polygon), chgeos builds a `PreparedGeometry` with a spatial index one time and uses it for
  all rows.
- Experimental ClickHouse feature. `allow_experimental_webassembly_udf` is not ready for
  production, and ClickHouse Cloud does not support it. The UDF API can change between
  ClickHouse releases.

## Dependencies

| Library | Version | Role |
|---|---|---|
| [GEOS](https://libgeos.org/) | ≥ 3.12 | Geometry engine |
| [msgpack23](https://github.com/rwindegger/msgpack23) | ≥ 3.1 | MsgPack serialization with zero-copy span support |
| [googletest](https://github.com/google/googletest) | ≥ 1.15 | Unit tests (native build only) |
